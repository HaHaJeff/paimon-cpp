/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/realtime/primary_key_realtime_store.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/realtime/arrow_realtime_store_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<arrow::Field> FieldWithId(const std::string& name,
                                          const std::shared_ptr<arrow::DataType>& type,
                                          int32_t field_id, bool nullable = true) {
    return DataField::ConvertDataFieldToArrowField(
               DataField(field_id, arrow::field(name, type, nullable)))
        ->WithNullable(nullable);
}

std::shared_ptr<arrow::Schema> PreparedSchema() {
    return arrow::schema(
        {DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
         DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
             ->WithNullable(false),
         DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
         DataField::ConvertDataFieldToArrowField(DataField(0, arrow::field("id", arrow::int64()))),
         DataField::ConvertDataFieldToArrowField(
             DataField(1, arrow::field("value", arrow::utf8())))});
}

std::shared_ptr<arrow::Schema> NestedPreparedSchema() {
    return arrow::schema(
        {DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
         DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
             ->WithNullable(false),
         DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
         DataField::ConvertDataFieldToArrowField(DataField(0, arrow::field("id", arrow::int64()))),
         DataField::ConvertDataFieldToArrowField(DataField(
             1,
             arrow::field("value",
                          arrow::struct_({arrow::field("name", arrow::utf8()),
                                          arrow::field("items", arrow::list(arrow::int32()))}))))});
}

std::unique_ptr<RecordBatch> MakeBatch(const std::string& json) {
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(PreparedSchema()->fields()), json)
            .ValueOrDie();
    auto c_array = std::make_unique<ArrowArray>();
    EXPECT_TRUE(arrow::ExportArray(*array, c_array.get()).ok());
    return RecordBatchBuilder(c_array.get()).Finish().value();
}

std::unique_ptr<RecordBatch> MakeBatch(const std::shared_ptr<arrow::Schema>& schema,
                                       const std::string& json) {
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json)
            .ValueOrDie();
    auto c_array = std::make_unique<ArrowArray>();
    EXPECT_TRUE(arrow::ExportArray(*array, c_array.get()).ok());
    return RecordBatchBuilder(c_array.get()).Finish().value();
}

std::unique_ptr<RecordBatch> MakeSlicedBatch(const std::shared_ptr<arrow::Schema>& schema,
                                             const std::string& json, int64_t offset,
                                             int64_t length) {
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json)
            .ValueOrDie()
            ->Slice(offset, length);
    auto c_array = std::make_unique<ArrowArray>();
    EXPECT_TRUE(arrow::ExportArray(*array, c_array.get()).ok());
    return RecordBatchBuilder(c_array.get()).Finish().value();
}

void AssertOffsetsZero(const ArrowArray* array) {
    ASSERT_NE(nullptr, array);
    ASSERT_EQ(0, array->offset);
    for (int64_t child = 0; child < array->n_children; ++child) {
        AssertOffsetsZero(array->children[child]);
    }
    if (array->dictionary) {
        AssertOffsetsZero(array->dictionary);
    }
}

Result<std::string> ReadJson(const std::vector<std::unique_ptr<BatchReader>>& readers) {
    std::vector<std::shared_ptr<arrow::Array>> batches;
    for (const std::unique_ptr<BatchReader>& reader : readers) {
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
            if (BatchReader::IsEofBatch(batch)) {
                break;
            }
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> array,
                arrow::ImportArray(batch.first.get(), batch.second.get()));
            batches.push_back(std::move(array));
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> result,
                                      arrow::Concatenate(batches));
    return result->ToString();
}

class TestingMemoryPool final : public MemoryPool {
 public:
    void* Malloc(uint64_t size, uint64_t alignment) override {
        ++allocation_count;
        if (reject_allocations) {
            throw std::bad_alloc();
        }
        return delegate_->Malloc(size, alignment);
    }

    void* Realloc(void* pointer, size_t old_size, size_t new_size, uint64_t alignment) override {
        ++allocation_count;
        if (reject_allocations) {
            throw std::bad_alloc();
        }
        return delegate_->Realloc(pointer, old_size, new_size, alignment);
    }

    void Free(void* pointer, uint64_t size) override {
        delegate_->Free(pointer, size);
    }

    void Free(void* pointer, uint64_t size, uint64_t alignment) override {
        delegate_->Free(pointer, size, alignment);
    }

    uint64_t CurrentUsage() const override {
        return delegate_->CurrentUsage();
    }

    uint64_t MaxMemoryUsage() const override {
        return delegate_->MaxMemoryUsage();
    }

    bool reject_allocations = false;
    int64_t allocation_count = 0;

 private:
    std::unique_ptr<MemoryPool> delegate_ = GetMemoryPool();
};

TEST(PrimaryKeyRealtimeStoreTest, TestWriteAndSealValidation) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(PreparedSchema(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_FALSE(segment.has_value());
    ASSERT_NOK_WITH_MSG(store->Write(RealtimeWriteBatch{nullptr, OffsetRange(0, 0)}),
                        "write batch is null");
    ASSERT_NOK_WITH_MSG(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 1, 0, 1, "one"]])"), OffsetRange(0, 0)}),
        "offset range does not match batch row count");

    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeBatch(R"([[0, 1, 0, 1, "one"], [0, 2, 1, 2, "two"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 3, 2, 3, "three"]])"), OffsetRange(2, 3)}));

    ASSERT_OK_AND_ASSIGN(segment, store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(OffsetRange(0, 3), segment.value()->GetOffsetRange());
    ASSERT_GT(store->GetMemoryUsage(), 0);
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 4, 3, 4, "four"]])"), OffsetRange(3, 4)}));
}

TEST(PrimaryKeyRealtimeStoreTest, TestBadTransportPrefix) {
    const std::shared_ptr<arrow::Schema> valid = PreparedSchema();
    std::vector<arrow::FieldVector> invalid_fields;

    arrow::FieldVector wrong_type = valid->fields();
    wrong_type[0] = DataField::ConvertDataFieldToArrowField(
                        DataField(SpecialFields::ValueKind().Id(),
                                  arrow::field("_VALUE_KIND", arrow::int32(), false)))
                        ->WithNullable(false);
    invalid_fields.push_back(std::move(wrong_type));

    arrow::FieldVector nullable_sequence = valid->fields();
    nullable_sequence[1] = nullable_sequence[1]->WithNullable(true);
    invalid_fields.push_back(std::move(nullable_sequence));

    arrow::FieldVector wrong_offset_id = valid->fields();
    wrong_offset_id[2] = DataField::ConvertDataFieldToArrowField(
                             DataField(99, arrow::field("_REALTIME_OFFSET", arrow::int64(), false)))
                             ->WithNullable(false);
    invalid_fields.push_back(std::move(wrong_offset_id));

    for (const arrow::FieldVector& fields : invalid_fields) {
        ASSERT_NOK_WITH_MSG(
            PrimaryKeyRealtimeStore::Create(arrow::schema(fields), GetDefaultPool()),
            "prepared schema field");
    }
}

TEST(PrimaryKeyRealtimeStoreTest, TestCommitReaderPerStoredBatch) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(PreparedSchema(), GetDefaultPool()));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeBatch(R"([[1, 6, 1, 1, "before"], [0, 5, 0, 3, "three"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[2, 7, 2, 2, "after"]])"), OffsetRange(2, 3)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateCommitReaders(segment.value()));
    ASSERT_EQ(2, readers.size());
    ASSERT_OK_AND_ASSIGN(std::string actual, ReadJson(readers));
    ASSERT_EQ(
        "-- is_valid: all not null\n-- child 0 type: int8\n  [\n    1,\n    0,\n    2\n  ]\n-- "
        "child 1 type: int64\n  [\n    6,\n    5,\n    7\n  ]\n-- child 2 type: int64\n  [\n    "
        "1,\n    0,\n    2\n  ]\n-- child 3 type: int64\n  [\n    1,\n    3,\n    2\n  ]\n-- child "
        "4 type: string\n  [\n    \"before\",\n    \"three\",\n    \"after\"\n  ]",
        actual);
    for (const std::unique_ptr<BatchReader>& reader : readers) {
        reader->Close();
    }
}

void AssertSlicedBatch(BatchReader* reader) {
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    ASSERT_EQ(2, batch.first->length);
    AssertOffsetsZero(batch.first.get());
    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> array = std::move(import_result).ValueOrDie();
    std::shared_ptr<arrow::StructArray> values = checked_pointer_cast<arrow::StructArray>(array);
    ASSERT_EQ(2, checked_pointer_cast<arrow::Int64Array>(values->field(3))->Value(0));
    ASSERT_EQ(3, checked_pointer_cast<arrow::Int64Array>(values->field(3))->Value(1));
    std::shared_ptr<arrow::StructArray> nested =
        checked_pointer_cast<arrow::StructArray>(values->field(4));
    ASSERT_EQ("two", checked_pointer_cast<arrow::StringArray>(nested->field(0))->GetString(0));
    ASSERT_EQ("three", checked_pointer_cast<arrow::StringArray>(nested->field(0))->GetString(1));
    std::shared_ptr<arrow::ListArray> items =
        checked_pointer_cast<arrow::ListArray>(nested->field(1));
    std::shared_ptr<arrow::Int32Array> first_items =
        checked_pointer_cast<arrow::Int32Array>(items->value_slice(0));
    ASSERT_EQ(3, first_items->Value(0));
    ASSERT_EQ(4, first_items->Value(1));
    std::shared_ptr<arrow::Int32Array> second_items =
        checked_pointer_cast<arrow::Int32Array>(items->value_slice(1));
    ASSERT_EQ(5, second_items->Value(0));
    ASSERT_EQ(6, second_items->Value(1));
    ASSERT_OK_AND_ASSIGN(batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
}

TEST(PrimaryKeyRealtimeStoreTest, TestSlicedReadersExportZeroOffsets) {
    std::shared_ptr<arrow::Schema> schema = NestedPreparedSchema();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(schema, GetDefaultPool()));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeSlicedBatch(
            schema,
            R"([[0, 1, 0, 1, ["one", [1, 2]]], [0, 2, 1, 2, ["two", [3, 4]]], [0, 3, 2, 3, ["three", [5, 6]]], [0, 4, 3, 4, ["four", [7, 8]]]])",
            1, 2),
        OffsetRange(0, 2)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());
    AssertSlicedBatch(readers[0].get());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(readers, store->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_EQ(1, readers.size());
    AssertSlicedBatch(readers[0].get());
}

TEST(PrimaryKeyRealtimeStoreTest, TestCloseUnreadBatchReaders) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(PreparedSchema(), GetDefaultPool()));
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 10, 0, 1, "a"]])"), OffsetRange(0, 1)}));
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 20, 1, 2, "b"]])"), OffsetRange(1, 2)}));
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 30, 2, 3, "c"]])"), OffsetRange(2, 3)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateCommitReaders(segment.value()));
    ASSERT_EQ(3, readers.size());
    for (const std::unique_ptr<BatchReader>& reader : readers) {
        reader->Close();
    }
}

TEST(PrimaryKeyRealtimeStoreTest, TestReclaimKeepsReadView) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(PreparedSchema(), GetDefaultPool()));
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 0, 4, 1, "one"]])"), OffsetRange(4, 5)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    ASSERT_OK(store->AdvanceCommittedOffset(5));
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(4, 5)), view->GetOffsetRange());
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryReaderPerStoredBatch) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(PreparedSchema(), GetDefaultPool()));
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 1, 0, 2, "two"]])"), OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 2, 1, 1, "one"]])"), OffsetRange(1, 2)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*PreparedSchema(), c_schema.get()).ok());
    RealtimeQueryContext context{/*read_schema=*/c_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_EQ(2, readers.size());
    ASSERT_OK_AND_ASSIGN(std::string actual, ReadJson(readers));
    ASSERT_NE(std::string::npos, actual.find("\"one\""));
    ASSERT_NE(std::string::npos, actual.find("\"two\""));
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryReaderProjectsTopLevelFieldsById) {
    const std::shared_ptr<arrow::Schema> stored_schema = PreparedSchema();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(stored_schema, GetDefaultPool()));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeSlicedBatch(stored_schema,
                        R"([[0, 1, 0, 6, "six"], [0, 2, 1, 7, "seven"], [0, 3, 2, 8, "eight"]])", 1,
                        1),
        OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    arrow::FieldVector requested_fields(stored_schema->fields().begin(),
                                        stored_schema->fields().begin() + 3);
    requested_fields.push_back(FieldWithId("renamed_value", arrow::utf8(), 1));
    requested_fields.push_back(FieldWithId("added", arrow::int32(), 2));
    std::shared_ptr<arrow::Schema> requested_schema = arrow::schema(std::move(requested_fields));
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*requested_schema, c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> array = std::move(import_result).ValueOrDie();
    ASSERT_TRUE(array->type()->Equals(arrow::struct_(requested_schema->fields())));
    std::shared_ptr<arrow::StructArray> projected = checked_pointer_cast<arrow::StructArray>(array);
    ASSERT_EQ(5, projected->num_fields());
    ASSERT_EQ("seven", checked_pointer_cast<arrow::StringArray>(projected->field(3))->GetString(0));
    ASSERT_TRUE(projected->field(4)->IsNull(0));
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryReaderRejectsMissingNonNullableTopLevelField) {
    const std::shared_ptr<arrow::Schema> stored_schema = PreparedSchema();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(stored_schema, GetDefaultPool()));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 1, 0, 7, "seven"]])"), OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    arrow::FieldVector requested_fields = stored_schema->fields();
    requested_fields.push_back(
        FieldWithId("required_added", arrow::int32(), 2, /*nullable=*/false));
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(
        arrow::ExportSchema(*arrow::schema(std::move(requested_fields)), c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_NOK_WITH_MSG(store->CreateQueryReaders(view, /*offset_begin=*/0, context),
                        "requested non-nullable field 'required_added' with id 2 is absent");
}

TEST(PrimaryKeyRealtimeStoreTest, TestQuerySchemaAlignmentUsesCallerPool) {
    const std::shared_ptr<arrow::Schema> stored_schema = PreparedSchema();
    std::shared_ptr<TestingMemoryPool> pool = std::make_shared<TestingMemoryPool>();
    auto write_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*stored_schema, write_schema.get()).ok());
    RealtimeStoreCreateRequest request{std::move(write_schema),
                                       /*options=*/{}, pool, RealtimeStoreMode::PRIMARY_KEY};
    ArrowRealtimeStoreFactory factory;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeStore> store, factory.Create(std::move(request)));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 1, 0, 7, "seven"]])"), OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    arrow::FieldVector requested_fields = stored_schema->fields();
    requested_fields.push_back(FieldWithId("added", arrow::int32(), 2));
    std::shared_ptr<arrow::Schema> requested_schema = arrow::schema(std::move(requested_fields));
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*requested_schema, c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};

    std::vector<arrow::FieldVector> zero_copy_schemas;
    zero_copy_schemas.push_back(stored_schema->fields());
    arrow::FieldVector reordered_fields(stored_schema->fields().begin(),
                                        stored_schema->fields().begin() + 3);
    reordered_fields.push_back(FieldWithId("renamed_value", arrow::utf8(), 1));
    reordered_fields.push_back(FieldWithId("renamed_id", arrow::int64(), 0));
    zero_copy_schemas.push_back(std::move(reordered_fields));
    pool->reject_allocations = true;
    for (const arrow::FieldVector& fields : zero_copy_schemas) {
        auto zero_copy_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(fields), zero_copy_schema.get()).ok());
        RealtimeQueryContext zero_copy_context{zero_copy_schema.get(), /*predicate=*/nullptr,
                                               /*enable_predicate_pushdown=*/false};
        const int64_t allocations_before_query = pool->allocation_count;
        ASSERT_OK_AND_ASSIGN(
            std::vector<std::unique_ptr<BatchReader>> readers,
            store->CreateQueryReaders(view, /*offset_begin=*/0, zero_copy_context));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
        ASSERT_TRUE(arrow::ImportArray(batch.first.get(), batch.second.get()).ok());
        ASSERT_EQ(allocations_before_query, pool->allocation_count);
    }

    const int64_t allocations_before_query = pool->allocation_count;
    ASSERT_NOK_WITH_MSG(store->CreateQueryReaders(view, /*offset_begin=*/0, context),
                        "Out of memory");
    ASSERT_GT(pool->allocation_count, allocations_before_query);
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryAlignmentPoolOutlivesStoreReaderAndExport) {
    const std::shared_ptr<arrow::Schema> stored_schema = PreparedSchema();
    std::shared_ptr<TestingMemoryPool> pool = std::make_shared<TestingMemoryPool>();
    std::weak_ptr<TestingMemoryPool> pool_lifetime = pool;
    auto write_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*stored_schema, write_schema.get()).ok());
    RealtimeStoreCreateRequest request{std::move(write_schema),
                                       /*options=*/{}, pool, RealtimeStoreMode::PRIMARY_KEY};
    ArrowRealtimeStoreFactory factory;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeStore> store, factory.Create(std::move(request)));
    request.memory_pool.reset();
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 1, 0, 7, "seven"]])"), OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    arrow::FieldVector requested_fields = stored_schema->fields();
    requested_fields.push_back(FieldWithId("added", arrow::int32(), 2));
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(
        arrow::ExportSchema(*arrow::schema(std::move(requested_fields)), c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_EQ(1, readers.size());
    ASSERT_GT(pool->allocation_count, 0);

    view.reset();
    store.reset();
    pool.reset();
    ASSERT_FALSE(pool_lifetime.expired());

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> imported = std::move(import_result).ValueOrDie();
    readers.clear();
    ASSERT_FALSE(pool_lifetime.expired());
    imported.reset();
    ASSERT_TRUE(pool_lifetime.expired());
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryReaderAlignsNestedFieldsById) {
    const std::shared_ptr<arrow::Field> stored_profile_a =
        FieldWithId("profile_a", arrow::int32(), 30);
    const std::shared_ptr<arrow::Field> stored_a = FieldWithId("a", arrow::int32(), 10);
    const std::shared_ptr<arrow::Field> stored_b = FieldWithId("b", arrow::int32(), 11);
    const std::shared_ptr<arrow::Field> stored_x = FieldWithId("x", arrow::int32(), 20);
    const std::shared_ptr<arrow::Field> stored_y = FieldWithId("y", arrow::int32(), 21);
    arrow::FieldVector stored_fields = {
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
            ->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
        FieldWithId("id", arrow::int64(), 0),
        FieldWithId("profile", arrow::struct_({stored_profile_a}), 1),
        FieldWithId("items", arrow::list(arrow::struct_({stored_a, stored_b})), 2),
        FieldWithId("attrs", arrow::map(arrow::utf8(), arrow::struct_({stored_x, stored_y})), 3)};
    std::shared_ptr<arrow::Schema> stored_schema = arrow::schema(std::move(stored_fields));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(stored_schema, GetDefaultPool()));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeSlicedBatch(
            stored_schema,
            R"([[0, 1, 0, 6, [5], [[1, 2]], [["before", [3, 4]]]], [0, 2, 1, 7, [50], [[100, 200], null], [["k1", [7, 8]], ["k2", null]]], [0, 3, 2, 8, [500], [[9, 10]], [["after", [11, 12]]]]])",
            1, 1),
        OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    const std::shared_ptr<arrow::Field> requested_profile_missing =
        FieldWithId("added_profile", arrow::int32(), 31);
    const std::shared_ptr<arrow::Field> requested_profile_a =
        FieldWithId("renamed_profile_a", arrow::int32(), 30);
    const std::shared_ptr<arrow::Field> requested_b = FieldWithId("renamed_b", arrow::int32(), 11);
    const std::shared_ptr<arrow::Field> requested_a = FieldWithId("renamed_a", arrow::int32(), 10);
    const std::shared_ptr<arrow::Field> requested_item_missing =
        FieldWithId("added_item", arrow::int32(), 12);
    const std::shared_ptr<arrow::Field> requested_y = FieldWithId("renamed_y", arrow::int32(), 21);
    const std::shared_ptr<arrow::Field> requested_x = FieldWithId("renamed_x", arrow::int32(), 20);
    const std::shared_ptr<arrow::Field> requested_attr_missing =
        FieldWithId("added_attr", arrow::int32(), 22);
    arrow::FieldVector requested_fields(stored_schema->fields().begin(),
                                        stored_schema->fields().begin() + 3);
    requested_fields.push_back(FieldWithId(
        "renamed_profile", arrow::struct_({requested_profile_missing, requested_profile_a}), 1));
    requested_fields.push_back(FieldWithId(
        "renamed_items",
        arrow::list(arrow::struct_({requested_b, requested_item_missing, requested_a})), 2));
    requested_fields.push_back(
        FieldWithId("renamed_attrs",
                    arrow::map(arrow::utf8(),
                               arrow::struct_({requested_y, requested_attr_missing, requested_x})),
                    3));
    std::shared_ptr<arrow::Schema> requested_schema = arrow::schema(std::move(requested_fields));
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*requested_schema, c_schema.get()).ok());
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> array = std::move(import_result).ValueOrDie();
    ASSERT_TRUE(array->type()->Equals(arrow::struct_(requested_schema->fields())));
    std::shared_ptr<arrow::StructArray> projected = checked_pointer_cast<arrow::StructArray>(array);
    const std::shared_ptr<arrow::StructArray> profile =
        checked_pointer_cast<arrow::StructArray>(projected->field(3));
    ASSERT_TRUE(profile->field(0)->IsNull(0));
    ASSERT_EQ(50, checked_pointer_cast<arrow::Int32Array>(profile->field(1))->Value(0));
    const std::shared_ptr<arrow::ListArray> items =
        checked_pointer_cast<arrow::ListArray>(projected->field(4));
    const std::shared_ptr<arrow::StructArray> item_values =
        checked_pointer_cast<arrow::StructArray>(items->value_slice(0));
    ASSERT_EQ(200, checked_pointer_cast<arrow::Int32Array>(item_values->field(0))->Value(0));
    ASSERT_TRUE(item_values->field(1)->IsNull(0));
    ASSERT_EQ(100, checked_pointer_cast<arrow::Int32Array>(item_values->field(2))->Value(0));
    ASSERT_TRUE(item_values->IsNull(1));

    const std::shared_ptr<arrow::MapArray> attrs =
        checked_pointer_cast<arrow::MapArray>(projected->field(5));
    const int64_t attr_offset = attrs->value_offset(0);
    const int64_t attr_length = attrs->value_length(0);
    const std::shared_ptr<arrow::StringArray> attr_keys =
        checked_pointer_cast<arrow::StringArray>(attrs->keys()->Slice(attr_offset, attr_length));
    ASSERT_EQ("k1", attr_keys->GetString(0));
    const std::shared_ptr<arrow::StructArray> attr_values =
        checked_pointer_cast<arrow::StructArray>(attrs->items()->Slice(attr_offset, attr_length));
    ASSERT_EQ(8, checked_pointer_cast<arrow::Int32Array>(attr_values->field(0))->Value(0));
    ASSERT_TRUE(attr_values->field(1)->IsNull(0));
    ASSERT_EQ(7, checked_pointer_cast<arrow::Int32Array>(attr_values->field(2))->Value(0));
    ASSERT_TRUE(attr_values->IsNull(1));
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryReaderRejectsMissingNonNullableNestedFields) {
    const std::shared_ptr<arrow::Field> stored_profile_a =
        FieldWithId("profile_a", arrow::int32(), 30);
    const std::shared_ptr<arrow::Field> stored_item_a = FieldWithId("item_a", arrow::int32(), 10);
    const std::shared_ptr<arrow::Field> stored_attr_a = FieldWithId("attr_a", arrow::int32(), 20);
    arrow::FieldVector stored_fields = {
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
            ->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
        FieldWithId("profile", arrow::struct_({stored_profile_a}), 1),
        FieldWithId("items", arrow::list(arrow::struct_({stored_item_a})), 2),
        FieldWithId("attrs", arrow::map(arrow::utf8(), arrow::struct_({stored_attr_a})), 3)};
    const std::shared_ptr<arrow::Schema> stored_schema = arrow::schema(std::move(stored_fields));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(stored_schema, GetDefaultPool()));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(stored_schema, R"([[0, 1, 0, [5], [[10]], [["key", [20]]]]])"),
                           OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    const std::shared_ptr<arrow::Field> required =
        FieldWithId("required_nested", arrow::int32(), 99, /*nullable=*/false);
    std::vector<arrow::FieldVector> requested_schemas;
    arrow::FieldVector struct_fields = stored_schema->fields();
    struct_fields[3] = FieldWithId("profile", arrow::struct_({stored_profile_a, required}), 1);
    requested_schemas.push_back(std::move(struct_fields));
    arrow::FieldVector list_fields = stored_schema->fields();
    list_fields[4] =
        FieldWithId("items", arrow::list(arrow::struct_({stored_item_a, required})), 2);
    requested_schemas.push_back(std::move(list_fields));
    arrow::FieldVector map_fields = stored_schema->fields();
    map_fields[5] = FieldWithId(
        "attrs", arrow::map(arrow::utf8(), arrow::struct_({stored_attr_a, required})), 3);
    requested_schemas.push_back(std::move(map_fields));

    for (const arrow::FieldVector& fields : requested_schemas) {
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(fields), c_schema.get()).ok());
        RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr,
                                     /*enable_predicate_pushdown=*/false};
        ASSERT_NOK_WITH_MSG(store->CreateQueryReaders(view, /*offset_begin=*/0, context),
                            "requested non-nullable field 'required_nested' with id 99 is absent");
    }
}

}  // namespace
}  // namespace paimon::test

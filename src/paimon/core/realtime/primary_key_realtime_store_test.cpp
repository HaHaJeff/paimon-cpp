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

#include <memory>
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
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<arrow::Field> FieldWithId(const std::string& name,
                                          const std::shared_ptr<arrow::DataType>& type,
                                          int32_t field_id) {
    return DataField::ConvertDataFieldToArrowField(DataField(field_id, arrow::field(name, type)));
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

std::shared_ptr<TableSchema> PkSchema(
    const std::shared_ptr<arrow::DataType>& key_type = arrow::int64(),
    const std::map<std::string, std::string>& options = {}) {
    return TableSchema::Create(
               /*schema_id=*/0,
               arrow::schema({arrow::field("id", key_type), arrow::field("value", arrow::utf8())}),
               /*partition_keys=*/{}, /*primary_keys=*/{"id"}, options)
        .value();
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

TEST(PrimaryKeyRealtimeStoreOptionsTest, TestSupportedOptions) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({{Options::BUCKET, "1"}}));
    ASSERT_OK(PrimaryKeyRealtimeStore::ValidateOptions(options, *PkSchema()));
}

TEST(PrimaryKeyRealtimeStoreOptionsTest, TestUnsupportedOptions) {
    const std::string sequence_group =
        std::string(Options::FIELDS_PREFIX) + ".value." + Options::SEQUENCE_GROUP;
    const std::vector<std::map<std::string, std::string>> unsupported_options = {
        {{Options::BUCKET, "0"}},
        {{Options::BUCKET, "1"}, {Options::MERGE_ENGINE, "partial-update"}},
        {{Options::BUCKET, "1"}, {Options::DATA_EVOLUTION_ENABLED, "true"}},
        {{Options::BUCKET, "1"}, {sequence_group, "seq"}},
        {{Options::BUCKET, "1"}, {Options::SEQUENCE_FIELD, "seq"}},
        {{Options::BUCKET, "1"}, {Options::FORCE_LOOKUP, "true"}},
        {{Options::BUCKET, "1"}, {Options::DELETION_VECTORS_ENABLED, "true"}},
        {{Options::BUCKET, "1"}, {Options::CHANGELOG_PRODUCER, "input"}},
    };
    for (const std::map<std::string, std::string>& option_map : unsupported_options) {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(option_map));
        ASSERT_NOK(PrimaryKeyRealtimeStore::ValidateOptions(options, *PkSchema()));
    }
}

TEST(PrimaryKeyRealtimeStoreOptionsTest, TestRejectsFloatingPrimaryKeys) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({{Options::BUCKET, "1"}}));
    ASSERT_NOK_WITH_MSG(
        PrimaryKeyRealtimeStore::ValidateOptions(options, *PkSchema(arrow::float32())),
        "FLOAT or DOUBLE primary keys");
    ASSERT_NOK_WITH_MSG(
        PrimaryKeyRealtimeStore::ValidateOptions(options, *PkSchema(arrow::float64())),
        "FLOAT or DOUBLE primary keys");
}

TEST(PrimaryKeyRealtimeStoreOptionsTest, TestRejectsEnabledGlobalIndex) {
    const std::map<std::string, std::string> option_map = {{Options::BUCKET, "1"},
                                                           {Options::PK_BTREE_INDEX_COLUMNS, "id"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(option_map));
    ASSERT_NOK_WITH_MSG(
        PrimaryKeyRealtimeStore::ValidateOptions(options, *PkSchema(arrow::int64(), option_map)),
        "does not support global indexes");
}

TEST(PrimaryKeyRealtimeStoreTest, TestWriteAndSealValidation) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(PreparedSchema()));
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
        ASSERT_NOK_WITH_MSG(PrimaryKeyRealtimeStore::Create(arrow::schema(fields)),
                            "prepared schema field");
    }
}

TEST(PrimaryKeyRealtimeStoreTest, TestCommitReaderPerStoredBatch) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(PreparedSchema()));
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

TEST(PrimaryKeyRealtimeStoreTest, TestCommitReaderExportsZeroOffsets) {
    std::shared_ptr<arrow::Schema> schema = NestedPreparedSchema();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(schema));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeBatch(schema, R"([[0, 1, 0, 1, ["one", [1, 2]]], [0, 2, 1, 2, ["two", [3, 4]]]])"),
        OffsetRange(0, 2)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    ASSERT_EQ(2, batch.first->length);
    AssertOffsetsZero(batch.first.get());
    ASSERT_TRUE(arrow::ImportArray(batch.first.get(), batch.second.get()).ok());
    ASSERT_OK_AND_ASSIGN(batch, readers[0]->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
}

TEST(PrimaryKeyRealtimeStoreTest, TestCloseUnreadBatchReaders) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(PreparedSchema()));
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
                         PrimaryKeyRealtimeStore::Create(PreparedSchema()));
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
                         PrimaryKeyRealtimeStore::Create(PreparedSchema()));
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
                         PrimaryKeyRealtimeStore::Create(stored_schema));
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

TEST(PrimaryKeyRealtimeStoreTest, TestQueryReaderAlignsNestedFieldsById) {
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
        FieldWithId("items", arrow::list(arrow::struct_({stored_a, stored_b})), 1),
        FieldWithId("attrs", arrow::map(arrow::utf8(), arrow::struct_({stored_x, stored_y})), 2)};
    std::shared_ptr<arrow::Schema> stored_schema = arrow::schema(std::move(stored_fields));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(stored_schema));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeSlicedBatch(
            stored_schema,
            R"([[0, 1, 0, 6, [[1, 2]], [["before", [3, 4]]]], [0, 2, 1, 7, [[100, 200], null], [["k1", [7, 8]], ["k2", null]]], [0, 3, 2, 8, [[9, 10]], [["after", [11, 12]]]]])",
            1, 1),
        OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

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
        "renamed_items",
        arrow::list(arrow::struct_({requested_b, requested_item_missing, requested_a})), 1));
    requested_fields.push_back(
        FieldWithId("renamed_attrs",
                    arrow::map(arrow::utf8(),
                               arrow::struct_({requested_y, requested_attr_missing, requested_x})),
                    2));
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
    const std::shared_ptr<arrow::ListArray> items =
        checked_pointer_cast<arrow::ListArray>(projected->field(3));
    const std::shared_ptr<arrow::StructArray> item_values =
        checked_pointer_cast<arrow::StructArray>(items->value_slice(0));
    ASSERT_EQ(200, checked_pointer_cast<arrow::Int32Array>(item_values->field(0))->Value(0));
    ASSERT_TRUE(item_values->field(1)->IsNull(0));
    ASSERT_EQ(100, checked_pointer_cast<arrow::Int32Array>(item_values->field(2))->Value(0));
    ASSERT_TRUE(item_values->IsNull(1));

    const std::shared_ptr<arrow::MapArray> attrs =
        checked_pointer_cast<arrow::MapArray>(projected->field(4));
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

}  // namespace
}  // namespace paimon::test

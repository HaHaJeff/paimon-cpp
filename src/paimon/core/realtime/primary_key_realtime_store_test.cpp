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
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/realtime/realtime_fields.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<arrow::Schema> PreparedSchema() {
    return arrow::schema(
        {DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
         DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
             ->WithNullable(false),
         DataField::ConvertDataFieldToArrowField(RealtimeOffsetField())->WithNullable(false),
         DataField::ConvertDataFieldToArrowField(DataField(0, arrow::field("id", arrow::int64()))),
         DataField::ConvertDataFieldToArrowField(
             DataField(1, arrow::field("value", arrow::utf8())))});
}

std::shared_ptr<arrow::Schema> NestedPreparedSchema() {
    return arrow::schema(
        {DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
         DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
             ->WithNullable(false),
         DataField::ConvertDataFieldToArrowField(RealtimeOffsetField())->WithNullable(false),
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
    ASSERT_OK(ValidatePrimaryKeyRealtimeOptions(options, *PkSchema()));
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
        ASSERT_NOK(ValidatePrimaryKeyRealtimeOptions(options, *PkSchema()));
    }
}

TEST(PrimaryKeyRealtimeStoreOptionsTest, TestRejectsFloatingPrimaryKeys) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({{Options::BUCKET, "1"}}));
    ASSERT_NOK_WITH_MSG(ValidatePrimaryKeyRealtimeOptions(options, *PkSchema(arrow::float32())),
                        "FLOAT or DOUBLE primary keys");
    ASSERT_NOK_WITH_MSG(ValidatePrimaryKeyRealtimeOptions(options, *PkSchema(arrow::float64())),
                        "FLOAT or DOUBLE primary keys");
}

TEST(PrimaryKeyRealtimeStoreOptionsTest, TestRejectsEnabledGlobalIndex) {
    const std::map<std::string, std::string> option_map = {{Options::BUCKET, "1"},
                                                           {Options::PK_BTREE_INDEX_COLUMNS, "id"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(option_map));
    ASSERT_NOK_WITH_MSG(
        ValidatePrimaryKeyRealtimeOptions(options, *PkSchema(arrow::int64(), option_map)),
        "does not support global indexes");
}

TEST(PrimaryKeyRealtimeStoreTest, TestWriteAndSealValidation) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<PrimaryKeyRealtimeStore> store,
        PrimaryKeyRealtimeStore::Create(PreparedSchema(), {"id"}, GetDefaultPool()));
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
    ASSERT_NOK_WITH_MSG(store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 3, 3, 3, "three"]])"),
                                                        OffsetRange(3, 4)}),
                        "offset ranges must be contiguous");
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 3, 2, 3, "three"]])"), OffsetRange(2, 3)}));

    ASSERT_OK_AND_ASSIGN(segment, store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(OffsetRange(0, 3), segment.value()->GetOffsetRange());
    ASSERT_GT(store->GetMemoryUsage(), 0);
    ASSERT_NOK_WITH_MSG(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 4, 4, 4, "four"]])"), OffsetRange(4, 5)}),
        "offset ranges must be contiguous");
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 4, 3, 4, "four"]])"), OffsetRange(3, 4)}));
}

TEST(PrimaryKeyRealtimeStoreTest, TestCommitBatches) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<PrimaryKeyRealtimeStore> store,
        PrimaryKeyRealtimeStore::Create(PreparedSchema(), {"id"}, GetDefaultPool()));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeBatch(R"([[1, 6, 1, 1, "before"], [0, 5, 0, 3, "three"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[2, 7, 2, 2, "after"]])"), OffsetRange(2, 3)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(std::string actual, ReadJson(readers));
    ASSERT_EQ(
        "-- is_valid: all not null\n-- child 0 type: int8\n  [\n    1,\n    2,\n    0\n  ]\n-- "
        "child 1 type: int64\n  [\n    6,\n    7,\n    5\n  ]\n-- child 2 type: int64\n  [\n    "
        "1,\n    2,\n    0\n  ]\n-- child 3 type: int64\n  [\n    1,\n    2,\n    3\n  ]\n-- child "
        "4 type: string\n  [\n    \"before\",\n    \"after\",\n    \"three\"\n  ]",
        actual);
    readers[0]->Close();
    readers[0]->Close();
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, readers[0]->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
}

TEST(PrimaryKeyRealtimeStoreTest, TestCommitReaderExportsZeroOffsets) {
    std::shared_ptr<arrow::Schema> schema = NestedPreparedSchema();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         PrimaryKeyRealtimeStore::Create(schema, {"id"}, GetDefaultPool()));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeBatch(schema, R"([[0, 1, 0, 1, ["one", [1, 2]]], [0, 2, 1, 2, ["two", [3, 4]]]])"),
        OffsetRange(0, 2)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());
    for (int32_t row = 0; row < 2; ++row) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        AssertOffsetsZero(batch.first.get());
        ASSERT_TRUE(arrow::ImportArray(batch.first.get(), batch.second.get()).ok());
    }
}

TEST(PrimaryKeyRealtimeStoreTest, TestReclaimKeepsReadView) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<PrimaryKeyRealtimeStore> store,
        PrimaryKeyRealtimeStore::Create(PreparedSchema(), {"id"}, GetDefaultPool()));
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 0, 4, 1, "one"]])"), OffsetRange(4, 5)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    ASSERT_OK(store->AdvanceCommittedOffset(5));
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(4, 5)), view->GetOffsetRange());
}

TEST(PrimaryKeyRealtimeStoreTest, TestQueryReaderCardinalityIsConstant) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<PrimaryKeyRealtimeStore> store,
        PrimaryKeyRealtimeStore::Create(PreparedSchema(), {"id"}, GetDefaultPool()));
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 1, 0, 2, "two"]])"), OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 2, 1, 1, "one"]])"), OffsetRange(1, 2)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    RealtimeQueryContext context{/*read_schema=*/nullptr, /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(std::string actual, ReadJson(readers));
    ASSERT_NE(std::string::npos, actual.find("\"one\""));
    ASSERT_NE(std::string::npos, actual.find("\"two\""));
}

}  // namespace
}  // namespace paimon::test

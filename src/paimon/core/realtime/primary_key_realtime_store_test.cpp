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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/core/realtime/primary_key_realtime_store.h"

#include <cstdint>
#include <limits>
#include <map>
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
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(PrimaryKeyRealtimeStoreOptionsTest, TestSupportedOptions) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({{Options::BUCKET, "1"}}));
    ASSERT_OK(ValidatePrimaryKeyRealtimeOptions(options));
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
        ASSERT_NOK(ValidatePrimaryKeyRealtimeOptions(options));
    }
}

class PrimaryKeyRealtimeStoreTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = std::shared_ptr<MemoryPool>(GetMemoryPool());
        schema_ = arrow::schema(
            {arrow::field("id", arrow::int64()), arrow::field("value", arrow::utf8())});
        ASSERT_OK_AND_ASSIGN(store_, CreateStore(schema_, {"id"}, /*restore_max_sequence=*/4));
    }

    Result<std::shared_ptr<PrimaryKeyRealtimeStore>> CreateStore(
        const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::string>& primary_keys,
        int64_t restore_max_sequence) const {
        std::vector<DataField> key_fields;
        key_fields.reserve(primary_keys.size());
        for (const std::string& primary_key : primary_keys) {
            const int32_t index = schema->GetFieldIndex(primary_key);
            key_fields.emplace_back(index, schema->field(index));
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                               FieldsComparator::Create(key_fields,
                                                        /*is_ascending_order=*/true));
        auto merge_factory = []() {
            auto merge_function =
                std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
            return std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
        };
        return PrimaryKeyRealtimeStore::Create(schema, primary_keys, key_comparator, merge_factory,
                                               restore_max_sequence,
                                               /*read_batch_size=*/2, pool_);
    }

    std::unique_ptr<RecordBatch> MakeBatch(
        const std::string& json, const std::vector<RecordBatch::RowKind>& row_kinds = {},
        const std::shared_ptr<arrow::Schema>& schema = nullptr) const {
        const std::shared_ptr<arrow::Schema>& batch_schema = schema ? schema : schema_;
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(batch_schema->fields()), json)
                .ValueOrDie();
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        RecordBatchBuilder builder(&c_array);
        builder.SetRowKinds(row_kinds);
        return builder.Finish().value();
    }

    std::unique_ptr<ArrowSchema> MakeReadSchema(const arrow::FieldVector& fields) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*arrow::schema(fields), c_schema.get()).ok());
        return c_schema;
    }

    void AssertReaderOutput(const std::vector<std::unique_ptr<BatchReader>>& readers,
                            const std::shared_ptr<arrow::DataType>& type,
                            const std::string& json) const {
        std::vector<std::shared_ptr<arrow::Array>> batches;
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            while (true) {
                ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
                if (BatchReader::IsEofBatch(batch)) {
                    break;
                }
                arrow::Result<std::shared_ptr<arrow::Array>> imported =
                    arrow::ImportArray(batch.first.get(), batch.second.get());
                ASSERT_TRUE(imported.ok()) << imported.status().ToString();
                batches.push_back(std::move(imported).ValueOrDie());
            }
        }
        ASSERT_FALSE(batches.empty());
        arrow::Result<std::shared_ptr<arrow::Array>> concatenated = arrow::Concatenate(batches);
        ASSERT_TRUE(concatenated.ok()) << concatenated.status().ToString();
        std::shared_ptr<arrow::Array> actual = std::move(concatenated).ValueOrDie();
        std::shared_ptr<arrow::Array> expected =
            arrow::ipc::internal::json::ArrayFromJSON(type, json).ValueOrDie();
        ASSERT_TRUE(actual->Equals(*expected))
            << "expected: " << expected->ToString() << ", actual: " << actual->ToString();
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            reader->Close();
        }
    }

    std::shared_ptr<arrow::DataType> CommitType() const {
        return arrow::struct_({
            DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind()),
            schema_->field(0),
            schema_->field(1),
        });
    }

    std::shared_ptr<arrow::DataType> QueryType() const {
        return arrow::struct_({
            DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind()),
            DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()),
            schema_->field(0),
            schema_->field(1),
        });
    }

    arrow::FieldVector FullQueryFields(
        const std::shared_ptr<arrow::Schema>& schema = nullptr) const {
        const std::shared_ptr<arrow::Schema>& query_schema = schema ? schema : schema_;
        arrow::FieldVector fields = {
            DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())};
        fields.insert(fields.end(), query_schema->fields().begin(), query_schema->fields().end());
        return fields;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<PrimaryKeyRealtimeStore> store_;
};

TEST_F(PrimaryKeyRealtimeStoreTest, TestWriteAndSeal) {
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_FALSE(segment.has_value());
    ASSERT_NOK_WITH_MSG(store_->Write(RealtimeWriteBatch{nullptr, OffsetRange(0, 0)}),
                        "write batch is null");
    ASSERT_NOK_WITH_MSG(
        store_->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "a"], [2, "b"]])"), OffsetRange(0, 0)}),
        "offset range does not match batch row count");

    ASSERT_OK(
        store_->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "a"], [2, "b"]])"), OffsetRange(0, 2)}));
    ASSERT_NOK_WITH_MSG(
        store_->Write(RealtimeWriteBatch{MakeBatch(R"([[4, "d"]])"), OffsetRange(3, 4)}),
        "offset ranges must be contiguous");
    ASSERT_OK(store_->Write(RealtimeWriteBatch{MakeBatch(R"([[3, "c"]])"), OffsetRange(2, 3)}));

    ASSERT_OK_AND_ASSIGN(segment, store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(OffsetRange(0, 3), segment.value()->GetOffsetRange());
    ASSERT_GT(store_->GetMemoryUsage(), 0);

    struct ValidationCase {
        int64_t restore_max_sequence;
        std::string error;
    };
    const std::vector<ValidationCase> cases = {
        {-2, "restore max sequence number must be at least -1"},
        {std::numeric_limits<int64_t>::max(), "sequence number has reached INT64_MAX"},
    };
    for (const ValidationCase& test_case : cases) {
        ASSERT_NOK_WITH_MSG(CreateStore(schema_, {"id"}, test_case.restore_max_sequence),
                            test_case.error);
    }
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestCommitBatches) {
    ASSERT_OK(store_->Write(RealtimeWriteBatch{
        MakeBatch(R"([[3, "three"], [1, "before"]])",
                  {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::UPDATE_BEFORE}),
        OffsetRange(0, 2)}));
    ASSERT_OK(store_->Write(RealtimeWriteBatch{
        MakeBatch(R"([[2, "after"]])", {RecordBatch::RowKind::UPDATE_AFTER}), OffsetRange(2, 3)}));
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[4, "deleted"], [0, "zero"]])",
                                     {RecordBatch::RowKind::DELETE, RecordBatch::RowKind::INSERT}),
                           OffsetRange(3, 5)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateCommitReaders(segment.value()));
    AssertReaderOutput(readers, CommitType(),
                       R"([[0, 3, "three"], [1, 1, "before"], [2, 2, "after"],
                            [3, 4, "deleted"], [0, 0, "zero"]])");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(FullQueryFields());
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(readers, store_->CreateQueryReaders(view, /*offset_begin=*/0, context));
    AssertReaderOutput(readers, QueryType(),
                       R"([[0, 9, 0, "zero"], [1, 6, 1, "before"], [2, 7, 2, "after"],
                            [0, 5, 3, "three"], [3, 8, 4, "deleted"]])");
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestMutationMerge) {
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[1, "old"], [2, "two"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store_->Write(RealtimeWriteBatch{
        MakeBatch(R"([[1, "new"], [2, "gone"]])",
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE}),
        OffsetRange(2, 4)}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(FullQueryFields());
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateQueryReaders(view, /*offset_begin=*/0, context));
    AssertReaderOutput(readers, QueryType(), R"([[2, 7, 1, "new"], [3, 8, 2, "gone"]])");
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestReadViewLifecycle) {
    ASSERT_OK(store_->Write(RealtimeWriteBatch{MakeBatch(R"([[10, "a"], [11, "b"], [12, "c"]])"),
                                               OffsetRange(10, 13)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(10, 13)), view->GetOffsetRange());

    ASSERT_OK(store_->AdvanceCommittedOffset(13));
    ASSERT_EQ(0, store_->GetMemoryUsage());
    ASSERT_OK(
        store_->Write(RealtimeWriteBatch{MakeBatch(R"([[13, "later"]])"), OffsetRange(13, 14)}));

    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(FullQueryFields());
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateQueryReaders(view, /*offset_begin=*/11, context));
    AssertReaderOutput(readers, QueryType(), R"([[0, 6, 11, "b"], [0, 7, 12, "c"]])");

    std::unique_ptr<ArrowSchema> empty_schema = MakeReadSchema(FullQueryFields());
    context.read_schema = empty_schema.get();
    ASSERT_OK_AND_ASSIGN(readers, store_->CreateQueryReaders(view, /*offset_begin=*/13, context));
    ASSERT_TRUE(readers.empty());
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestQueryKeyRange) {
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[5, "five"], [1, "one"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store_->SealForCommit());
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[9, "nine"], [7, "seven"]])"), OffsetRange(2, 4)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(FullQueryFields());
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_EQ(2, readers.size());
    const std::vector<std::pair<int64_t, int64_t>> key_ranges = {{1, 5}, {7, 9}};
    for (size_t i = 0; i < readers.size(); ++i) {
        auto* range = dynamic_cast<PrimaryKeyRangeProvider*>(readers[i].get());
        ASSERT_NE(nullptr, range);
        ASSERT_EQ(key_ranges[i].first, range->GetMinKey()->GetLong(0));
        ASSERT_EQ(key_ranges[i].second, range->GetMaxKey()->GetLong(0));
    }
    AssertReaderOutput(readers, QueryType(),
                       R"([[0, 6, 1, "one"], [0, 5, 5, "five"], [0, 8, 7, "seven"],
                            [0, 7, 9, "nine"]])");

    ASSERT_OK(store_->AdvanceCommittedOffset(2));
    ASSERT_OK_AND_ASSIGN(view, store_->AcquireReadView());
    read_schema = MakeReadSchema(FullQueryFields());
    context.read_schema = read_schema.get();
    ASSERT_OK_AND_ASSIGN(readers, store_->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_EQ(1, readers.size());
    auto* range = dynamic_cast<PrimaryKeyRangeProvider*>(readers[0].get());
    ASSERT_NE(nullptr, range);
    ASSERT_EQ(7, range->GetMinKey()->GetLong(0));
    ASSERT_EQ(9, range->GetMaxKey()->GetLong(0));
    AssertReaderOutput(readers, QueryType(), R"([[0, 8, 7, "seven"], [0, 7, 9, "nine"]])");
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestSequenceExhaustion) {
    const int64_t max_sequence = std::numeric_limits<int64_t>::max();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         CreateStore(schema_, {"id"}, max_sequence - 3));
    ASSERT_OK(store->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "kept"]])"), OffsetRange(10, 11)}));
    ASSERT_NOK_WITH_MSG(
        store->Write(RealtimeWriteBatch{
            MakeBatch(R"([[7, "rejected-a"], [8, "rejected-b"], [9, "rejected-c"]])"),
            OffsetRange(11, 14)}),
        "sequence range exceeds INT64_MAX");
    ASSERT_OK(
        store->Write(RealtimeWriteBatch{MakeBatch(R"([[2, "also-kept"]])"), OffsetRange(11, 12)}));

    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(OffsetRange(10, 12), segment.value()->GetOffsetRange());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(10, 12)), view->GetOffsetRange());
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(FullQueryFields());
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, /*offset_begin=*/10, context));
    AssertReaderOutput(readers, QueryType(),
                       R"([[0, 9223372036854775805, 1, "kept"],
                            [0, 9223372036854775806, 2, "also-kept"]])");
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestQueryProjection) {
    ASSERT_OK(
        store_->Write(RealtimeWriteBatch{MakeBatch(R"([[2, "b"], [1, "a"]])"), OffsetRange(0, 2)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    const std::shared_ptr<arrow::Field> value_kind =
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind());
    const std::shared_ptr<arrow::Field> sequence =
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber());
    struct ProjectionCase {
        arrow::FieldVector requested;
        std::shared_ptr<arrow::DataType> expected_type;
        std::string expected_json;
    };
    const std::vector<ProjectionCase> cases = {
        {{schema_->field(1), value_kind, sequence, schema_->field(0)},
         arrow::struct_({value_kind, schema_->field(1), sequence, schema_->field(0)}),
         R"([[0, "a", 6, 1], [0, "b", 5, 2]])"},
        {{schema_->field(0), value_kind},
         arrow::struct_({value_kind, schema_->field(0)}),
         R"([[0, 1], [0, 2]])"},
    };
    for (const ProjectionCase& test_case : cases) {
        std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(test_case.requested);
        RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                     /*enable_predicate_pushdown=*/false};
        ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                             store_->CreateQueryReaders(view, /*offset_begin=*/0, context));
        AssertReaderOutput(readers, test_case.expected_type, test_case.expected_json);
    }

    std::unique_ptr<ArrowSchema> read_schema =
        MakeReadSchema({arrow::field("unknown", arrow::int64())});
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_NOK_WITH_MSG(store_->CreateQueryReaders(view, /*offset_begin=*/0, context),
                        "query field is missing from write schema: unknown");
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestQueryProjectionMatchesRenamedFieldsById) {
    const std::shared_ptr<arrow::Field> id =
        DataField::ConvertDataFieldToArrowField(DataField(0, arrow::field("id", arrow::int64())));
    const std::shared_ptr<arrow::Field> value =
        DataField::ConvertDataFieldToArrowField(DataField(1, arrow::field("value", arrow::utf8())));
    const std::shared_ptr<arrow::Schema> write_schema = arrow::schema({id, value});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         CreateStore(write_schema, {"id"}, /*restore_max_sequence=*/4));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[1, "kept"]])", {}, write_schema), OffsetRange(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    const std::shared_ptr<arrow::Field> renamed_value = DataField::ConvertDataFieldToArrowField(
        DataField(1, arrow::field("renamed", arrow::utf8())));
    const std::shared_ptr<arrow::Field> renamed_id = DataField::ConvertDataFieldToArrowField(
        DataField(0, arrow::field("renamed_id", arrow::int64())));
    const std::shared_ptr<arrow::Field> replaced =
        DataField::ConvertDataFieldToArrowField(DataField(2, arrow::field("value", arrow::utf8())));
    const std::shared_ptr<arrow::Field> replaced_id =
        DataField::ConvertDataFieldToArrowField(DataField(4, arrow::field("id", arrow::int64())));
    const std::shared_ptr<arrow::Field> added =
        DataField::ConvertDataFieldToArrowField(DataField(3, arrow::field("added", arrow::utf8())));
    std::unique_ptr<ArrowSchema> read_schema =
        MakeReadSchema({renamed_value, renamed_id, replaced, replaced_id, added});
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, /*offset_begin=*/0, context));
    const std::shared_ptr<arrow::DataType> result_type =
        arrow::struct_({DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind()),
                        renamed_value, renamed_id, replaced, replaced_id, added});
    AssertReaderOutput(readers, result_type, R"([[0, "kept", 1, null, null, null]])");
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestNestedProjection) {
    const std::shared_ptr<arrow::Field> id =
        DataField::ConvertDataFieldToArrowField(DataField(0, arrow::field("id", arrow::int64())));
    const std::shared_ptr<arrow::Field> a =
        DataField::ConvertDataFieldToArrowField(DataField(10, arrow::field("a", arrow::int64())));
    const std::shared_ptr<arrow::Field> b =
        DataField::ConvertDataFieldToArrowField(DataField(11, arrow::field("b", arrow::int64())));
    const std::shared_ptr<arrow::Field> payload = DataField::ConvertDataFieldToArrowField(
        DataField(1, arrow::field("payload", arrow::struct_({a, b}))));
    const std::shared_ptr<arrow::Schema> nested_schema = arrow::schema({id, payload});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         CreateStore(nested_schema, {"id"}, /*restore_max_sequence=*/4));
    ASSERT_OK(store->Write(RealtimeWriteBatch{
        MakeBatch(R"([[2, [200, 2000]], [1, [100, null]], [3, [300, 3000]]])", {}, nested_schema),
        OffsetRange(0, 3)}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    const std::shared_ptr<arrow::Field> projected_payload = payload->WithType(arrow::struct_({b}));
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema({projected_payload});
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, /*offset_begin=*/0, context));
    const std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind()), projected_payload});
    AssertReaderOutput(readers, result_type, R"([[0, [null]], [0, [2000]], [0, [3000]]])");
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestCompositeKeyClipping) {
    std::shared_ptr<arrow::Schema> composite_schema =
        arrow::schema({arrow::field("id", arrow::int64()), arrow::field("region", arrow::utf8()),
                       arrow::field("value", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyRealtimeStore> store,
                         CreateStore(composite_schema, {"id", "region"},
                                     /*restore_max_sequence=*/4));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[9, "z", "clipped"], [2, "b", "two-b"], [1, "c", "one-c"],
                       [2, "a", "two-a"]])",
                                     {}, composite_schema),
                           OffsetRange(20, 24)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());
    const std::shared_ptr<arrow::Field> sequence =
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber());
    std::unique_ptr<ArrowSchema> read_schema =
        MakeReadSchema({sequence, composite_schema->field(0), composite_schema->field(2)});
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, /*offset_begin=*/21, context));
    ASSERT_EQ(1, readers.size());
    auto* range = dynamic_cast<PrimaryKeyRangeProvider*>(readers[0].get());
    ASSERT_NE(nullptr, range);
    ASSERT_EQ(1, range->GetMinKey()->GetLong(0));
    ASSERT_EQ("c", range->GetMinKey()->GetString(1).ToString());
    ASSERT_EQ(2, range->GetMaxKey()->GetLong(0));
    ASSERT_EQ("b", range->GetMaxKey()->GetString(1).ToString());
    std::shared_ptr<arrow::DataType> query_type =
        arrow::struct_({DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind()),
                        sequence, composite_schema->field(0), composite_schema->field(2)});
    AssertReaderOutput(readers, query_type,
                       R"([[0, 7, 1, "one-c"], [0, 8, 2, "two-a"],
                            [0, 6, 2, "two-b"]])");
}

}  // namespace paimon::test

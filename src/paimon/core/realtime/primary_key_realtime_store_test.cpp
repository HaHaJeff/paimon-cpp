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

#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class PrimaryKeyRealtimeStoreTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = std::shared_ptr<MemoryPool>(GetMemoryPool());
        schema_ = arrow::schema(
            {arrow::field("id", arrow::int64()), arrow::field("value", arrow::utf8())});
        ASSERT_OK_AND_ASSIGN(key_comparator_,
                             FieldsComparator::Create({DataField(0, schema_->field(0))},
                                                      /*is_ascending_order=*/true));
        auto merge_factory = []() {
            auto merge_function =
                std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
            return std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
        };
        ASSERT_OK_AND_ASSIGN(
            store_, PrimaryKeyRealtimeStore::Create(schema_, {"id"}, key_comparator_, merge_factory,
                                                    /*restore_max_sequence_number=*/4,
                                                    /*read_batch_size=*/1024, pool_));
    }

    std::unique_ptr<RecordBatch> MakeBatch(
        const std::string& json, const std::vector<RecordBatch::RowKind>& row_kinds = {}) const {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json)
                .ValueOrDie();
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        RecordBatchBuilder builder(&c_array);
        builder.SetRowKinds(row_kinds);
        return builder.Finish().value();
    }

    std::unique_ptr<ArrowSchema> MakeReadSchema(bool include_sequence) const {
        arrow::FieldVector fields;
        if (include_sequence) {
            fields.push_back(
                DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()));
        }
        fields.insert(fields.end(), schema_->fields().begin(), schema_->fields().end());
        auto c_schema = std::make_unique<ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*arrow::schema(fields), c_schema.get()).ok());
        return c_schema;
    }

    void AssertReaderOutput(BatchReader* reader, const std::shared_ptr<arrow::DataType>& type,
                            const std::string& json) const {
        ASSERT_NE(nullptr, reader);
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        arrow::Result<std::shared_ptr<arrow::Array>> imported_result =
            arrow::ImportArray(batch.first.get(), batch.second.get());
        ASSERT_TRUE(imported_result.ok()) << imported_result.status().ToString();
        std::shared_ptr<arrow::Array> actual = std::move(imported_result).ValueOrDie();
        std::shared_ptr<arrow::Array> expected =
            arrow::ipc::internal::json::ArrayFromJSON(type, json).ValueOrDie();
        ASSERT_TRUE(actual->Equals(*expected))
            << "expected: " << expected->ToString() << ", actual: " << actual->ToString();

        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, reader->NextBatch());
        ASSERT_TRUE(BatchReader::IsEofBatch(eof));
        reader->Close();
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

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<FieldsComparator> key_comparator_;
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

    auto merge_factory = []() {
        auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        return std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
    };
    ASSERT_NOK_WITH_MSG(PrimaryKeyRealtimeStore::Create(
                            schema_, {"id"}, key_comparator_, merge_factory,
                            /*restore_max_sequence_number=*/-2, /*read_batch_size=*/1024, pool_),
                        "restore max sequence number must be at least -1");
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestCommitReaderPreservesMutations) {
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[2, "old"], [1, "one"], [2, "new"]])",
                                     {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                                      RecordBatch::RowKind::UPDATE_AFTER}),
                           OffsetRange(0, 3)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());
    AssertReaderOutput(readers[0].get(), CommitType(),
                       R"([[0, 2, "old"], [0, 1, "one"], [2, 2, "new"]])");
}

TEST_F(PrimaryKeyRealtimeStoreTest, TestMutationMerge) {
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[1, "old"], [2, "two"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store_->Write(RealtimeWriteBatch{
        MakeBatch(R"([[1, "new"], [2, "gone"]])",
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE}),
        OffsetRange(2, 4)}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(/*include_sequence=*/true);
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_EQ(1, readers.size());
    AssertReaderOutput(readers[0].get(), QueryType(), R"([[2, 7, 1, "new"], [3, 8, 2, "gone"]])");
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

    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(/*include_sequence=*/true);
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateQueryReaders(view, /*offset_begin=*/11, context));
    ASSERT_EQ(1, readers.size());
    AssertReaderOutput(readers[0].get(), QueryType(), R"([[0, 6, 11, "b"], [0, 7, 12, "c"]])");

    std::unique_ptr<ArrowSchema> empty_schema = MakeReadSchema(/*include_sequence=*/true);
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
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(/*include_sequence=*/true);
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                 /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateQueryReaders(view, /*offset_begin=*/0, context));
    ASSERT_EQ(2, readers.size());
    auto* first_range = dynamic_cast<PrimaryKeyRangeProvider*>(readers[0].get());
    auto* second_range = dynamic_cast<PrimaryKeyRangeProvider*>(readers[1].get());
    ASSERT_NE(nullptr, first_range);
    ASSERT_NE(nullptr, second_range);
    ASSERT_EQ(1, first_range->GetMinKey()->GetLong(0));
    ASSERT_EQ(5, first_range->GetMaxKey()->GetLong(0));
    ASSERT_EQ(7, second_range->GetMinKey()->GetLong(0));
    ASSERT_EQ(9, second_range->GetMaxKey()->GetLong(0));
}

}  // namespace paimon::test

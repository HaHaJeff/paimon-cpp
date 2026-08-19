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

#include "paimon/core/realtime/primary_key_mem_indexer.h"

#include <atomic>
#include <functional>
#include <future>
#include <memory>
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
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class ForeignSegment : public RealtimeSegmentHandle {
 public:
    Range GetOffsetRange() const override {
        return Range(0, 0);
    }
};

class ForeignReadView : public MemReadView {
 public:
    std::optional<Range> GetOffsetRange() const override {
        return Range(0, 0);
    }
};

}  // namespace

class PrimaryKeyMemIndexerTest : public testing::Test {
 public:
    void SetUp() override {
        test_dir_ = UniqueTestDirectory::Create();
        pool_ = std::shared_ptr<MemoryPool>(GetMemoryPool());
        schema_ = arrow::schema(
            {arrow::field("id", arrow::int64()), arrow::field("value", arrow::utf8())});
        ASSERT_OK_AND_ASSIGN(key_comparator_,
                             FieldsComparator::Create({DataField(0, schema_->field(0))},
                                                      /*is_ascending_order=*/true));
        merge_function_wrapper_factory_ = []() {
            auto merge_function =
                std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
            return std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
        };
        io_manager_ = std::make_shared<IOManager>(test_dir_->Str(), test_dir_->GetFileSystem());
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::WRITE_BUFFER_SPILLABLE, "true"}},
                                                  test_dir_->GetFileSystem()));
        ASSERT_OK_AND_ASSIGN(indexer_,
                             PrimaryKeyMemIndexer::Create(
                                 schema_, {"id"}, key_comparator_, merge_function_wrapper_factory_,
                                 /*restore_max_seq_number=*/-1, options, io_manager_,
                                 /*enable_multi_thread_spill=*/false, pool_));
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

    std::unique_ptr<ArrowSchema> MakeReadSchema() const {
        auto c_schema = std::make_unique<ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema_, c_schema.get()).ok());
        return c_schema;
    }

 protected:
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::function<std::shared_ptr<MergeFunctionWrapper<KeyValue>>()>
        merge_function_wrapper_factory_;
    std::shared_ptr<IOManager> io_manager_;
    std::shared_ptr<PrimaryKeyMemIndexer> indexer_;
};

TEST_F(PrimaryKeyMemIndexerTest, TestWriteAndSeal) {
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> empty_segment,
                         indexer_->SealForCommit());
    ASSERT_FALSE(empty_segment.has_value());

    ASSERT_NOK_WITH_MSG(indexer_->Write(RealtimeWriteBatch{nullptr, Range(0, 0)}),
                        "write batch is null");
    ASSERT_NOK_WITH_MSG(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[0, "a"], [1, "b"]])"), Range(0, 0)}),
        "offset range does not match batch row count");

    ASSERT_OK(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[0, "a"], [1, "b"]])"), Range(0, 1)}));
    ASSERT_NOK_WITH_MSG(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[3, "d"], [4, "e"]])"), Range(3, 4)}),
        "offset ranges must be contiguous");
    ASSERT_OK(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[2, "c"], [3, "d"]])"), Range(2, 3)}));

    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         indexer_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(Range(0, 3), segment.value()->GetOffsetRange());
    ASSERT_OK_AND_ASSIGN(empty_segment, indexer_->SealForCommit());
    ASSERT_FALSE(empty_segment.has_value());

    ASSERT_GT(indexer_->GetMemoryUsage(), 0);
}

TEST_F(PrimaryKeyMemIndexerTest, TestForeignHandles) {
    ASSERT_OK(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "one"], [2, "two"]])"), Range(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         indexer_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> commit_readers,
                         indexer_->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, commit_readers.size());
    commit_readers[0]->Close();

    ASSERT_NOK_WITH_MSG(indexer_->CreateCommitReaders(std::make_shared<ForeignSegment>()),
                        "segment was not created by the PK mem indexer");

    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema();
    MemQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                            /*enable_predicate_pushdown=*/false};
    ASSERT_NOK_WITH_MSG(indexer_->CreateQueryReaders(std::make_shared<ForeignReadView>(),
                                                     /*offset_lower_exclusive=*/-1, context),
                        "read view was not created by the PK mem indexer");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer_->AcquireReadView());
    context.read_schema = nullptr;
    ASSERT_NOK_WITH_MSG(indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, context),
                        "PK mem query read schema is null");
}

TEST_F(PrimaryKeyMemIndexerTest, TestCommitReadersPreserveSortedMutations) {
    ASSERT_OK(indexer_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[2, "old"], [1, "one"], [2, "new"]])",
                                     {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                                      RecordBatch::RowKind::UPDATE_AFTER}),
                           Range(0, 2)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         indexer_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         indexer_->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    arrow::Result<std::shared_ptr<arrow::Array>> imported_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(imported_result.ok()) << imported_result.status().ToString();
    std::shared_ptr<arrow::Array> imported = std::move(imported_result).ValueOrDie();
    ASSERT_EQ(arrow::Type::STRUCT, imported->type_id());
    std::shared_ptr<arrow::StructArray> values = checked_pointer_cast<arrow::StructArray>(imported);
    ASSERT_EQ(2, values->length());
    std::shared_ptr<arrow::StructType> value_type =
        checked_pointer_cast<arrow::StructType>(values->type());
    ASSERT_EQ(SpecialFields::SequenceNumber().Name(), value_type->field(0)->name());
    ASSERT_EQ(SpecialFields::ValueKind().Name(), value_type->field(1)->name());
    ASSERT_EQ("id", value_type->field(2)->name());
    ASSERT_EQ("value", value_type->field(3)->name());
    std::shared_ptr<arrow::Int64Array> sequences =
        checked_pointer_cast<arrow::Int64Array>(values->field(0));
    std::shared_ptr<arrow::Int8Array> row_kinds =
        checked_pointer_cast<arrow::Int8Array>(values->field(1));
    std::shared_ptr<arrow::Int64Array> ids =
        checked_pointer_cast<arrow::Int64Array>(values->field(2));
    std::shared_ptr<arrow::StringArray> strings =
        checked_pointer_cast<arrow::StringArray>(values->field(3));
    ASSERT_EQ(1, ids->Value(0));
    ASSERT_EQ(1, sequences->Value(0));
    ASSERT_EQ(static_cast<int8_t>(RecordBatch::RowKind::INSERT), row_kinds->Value(0));
    ASSERT_EQ("one", strings->GetString(0));
    ASSERT_EQ(2, ids->Value(1));
    ASSERT_EQ(2, sequences->Value(1));
    ASSERT_EQ(static_cast<int8_t>(RecordBatch::RowKind::UPDATE_AFTER), row_kinds->Value(1));
    ASSERT_EQ("new", strings->GetString(1));
    ASSERT_OK_AND_ASSIGN(batch, readers[0]->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
    readers[0]->Close();
}

TEST_F(PrimaryKeyMemIndexerTest, TestMemoryPoolLifecycle) {
    ASSERT_EQ(0, pool_->CurrentUsage());
    ASSERT_OK(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "one"], [2, "two"]])"), Range(0, 1)}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer_->AcquireReadView());
    const uint64_t baseline = pool_->CurrentUsage();
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema();
    MemQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                            /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<BatchReader>> readers,
        indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, context));
    ASSERT_EQ(1, readers.size());
    std::shared_ptr<arrow::Array> imported;
    {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        ASSERT_GT(pool_->CurrentUsage(), baseline);
        arrow::Result<std::shared_ptr<arrow::Array>> imported_result =
            arrow::ImportArray(batch.first.get(), batch.second.get());
        ASSERT_TRUE(imported_result.ok()) << imported_result.status();
        imported = std::move(imported_result).ValueOrDie();
        std::shared_ptr<arrow::StructArray> values =
            std::dynamic_pointer_cast<arrow::StructArray>(imported);
        ASSERT_TRUE(values);
    }
    readers[0]->Close();
    readers.clear();
    imported.reset();
    ASSERT_EQ(baseline, pool_->CurrentUsage());
}

TEST_F(PrimaryKeyMemIndexerTest, TestQueryReaderWithoutSequence) {
    ASSERT_OK(indexer_->Write(RealtimeWriteBatch{
        MakeBatch(R"([[1, "old"], [1, "new"], [2, "two"]])",
                  {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::UPDATE_AFTER,
                   RecordBatch::RowKind::INSERT}),
        Range(0, 2)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer_->AcquireReadView());
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema();
    MemQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                            /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<BatchReader>> readers,
        indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, context));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    arrow::Result<std::shared_ptr<arrow::Array>> imported_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(imported_result.ok()) << imported_result.status();
    std::shared_ptr<arrow::Array> imported = std::move(imported_result).ValueOrDie();
    std::shared_ptr<arrow::StructArray> values =
        std::dynamic_pointer_cast<arrow::StructArray>(imported);
    ASSERT_TRUE(values);
    ASSERT_EQ(2, values->length());
    ASSERT_EQ(3, values->num_fields());
    ASSERT_EQ(SpecialFields::ValueKind().Name(), values->type()->field(0)->name());
    ASSERT_EQ("id", values->type()->field(1)->name());
    ASSERT_EQ("value", values->type()->field(2)->name());
    std::shared_ptr<arrow::Int64Array> ids =
        std::dynamic_pointer_cast<arrow::Int64Array>(values->field(1));
    std::shared_ptr<arrow::StringArray> payloads =
        std::dynamic_pointer_cast<arrow::StringArray>(values->field(2));
    ASSERT_TRUE(ids);
    ASSERT_TRUE(payloads);
    ASSERT_EQ(1, ids->Value(0));
    ASSERT_EQ("new", payloads->GetString(0));
    ASSERT_EQ(2, ids->Value(1));
    ASSERT_EQ("two", payloads->GetString(1));
    readers[0]->Close();
}

TEST_F(PrimaryKeyMemIndexerTest, TestAcquireReadViewDoesNotRotateBuilding) {
    int32_t factory_calls = 0;
    auto factory = [&factory_calls]() {
        ++factory_calls;
        auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        return std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
    };
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SPILLABLE, "true"}},
                                              test_dir_->GetFileSystem()));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<PrimaryKeyMemIndexer> indexer,
        PrimaryKeyMemIndexer::Create(schema_, {"id"}, key_comparator_, factory,
                                     /*restore_max_seq_number=*/-1, options, io_manager_,
                                     /*enable_multi_thread_spill=*/false, pool_));
    ASSERT_OK(indexer->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "one"]])"), Range(0, 0)}));
    ASSERT_EQ(1, factory_calls);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer->AcquireReadView());
    ASSERT_EQ(2, factory_calls);
    ASSERT_EQ(std::optional<Range>(Range(0, 0)), view->GetOffsetRange());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> second_view, indexer->AcquireReadView());
    ASSERT_EQ(3, factory_calls);
    ASSERT_EQ(std::optional<Range>(Range(0, 0)), second_view->GetOffsetRange());
}

TEST_F(PrimaryKeyMemIndexerTest, TestReadViewIsConsumedOnce) {
    ASSERT_OK(indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "one"]])"), Range(0, 0)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer_->AcquireReadView());
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema();
    MemQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                            /*enable_predicate_pushdown=*/false};

    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<BatchReader>> readers,
        indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, context));
    ASSERT_EQ(1, readers.size());
    read_schema = MakeReadSchema();
    context.read_schema = read_schema.get();
    ASSERT_NOK_WITH_MSG(indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, context),
                        "PK memory read view has already been consumed");
    readers[0]->Close();
}

TEST_F(PrimaryKeyMemIndexerTest, TestSpillReadViewSurvivesLaterWrites) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SPILLABLE, "true"},
                                               {Options::WRITE_BUFFER_SIZE, "1"},
                                               {Options::LOCAL_SORT_MAX_NUM_FILE_HANDLES, "2"}},
                                              test_dir_->GetFileSystem()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PrimaryKeyMemIndexer> indexer,
                         PrimaryKeyMemIndexer::Create(
                             schema_, {"id"}, key_comparator_, merge_function_wrapper_factory_,
                             /*restore_max_seq_number=*/-1, options, io_manager_,
                             /*enable_multi_thread_spill=*/false, pool_));
    ASSERT_OK(indexer->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "one"]])"), Range(0, 0)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer->AcquireReadView());

    ASSERT_OK(indexer->Write(RealtimeWriteBatch{MakeBatch(R"([[2, "two"]])"), Range(1, 1)}));
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema();
    MemQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                            /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         indexer->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, context));
    ASSERT_EQ(1, readers.size());

    indexer.reset();
    io_manager_.reset();
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    std::shared_ptr<arrow::Array> values =
        arrow::ImportArray(batch.first.get(), batch.second.get()).ValueOrDie();
    ASSERT_EQ(1, values->length());
    readers[0]->Close();
}

TEST_F(PrimaryKeyMemIndexerTest, TestBuildingReadViewIsStableAndClipped) {
    ASSERT_OK(indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "one"]])"), Range(0, 0)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> first_view, indexer_->AcquireReadView());
    ASSERT_OK(indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[2, "two"]])"), Range(1, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> second_view, indexer_->AcquireReadView());

    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema();
    MemQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                            /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<BatchReader>> first_readers,
        indexer_->CreateQueryReaders(first_view, /*offset_lower_exclusive=*/-1, context));
    ASSERT_EQ(1, first_readers.size());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first_batch, first_readers[0]->NextBatch());
    std::shared_ptr<arrow::Array> first_values =
        arrow::ImportArray(first_batch.first.get(), first_batch.second.get()).ValueOrDie();
    ASSERT_EQ(1, first_values->length());
    first_readers[0]->Close();

    read_schema = MakeReadSchema();
    context.read_schema = read_schema.get();
    ASSERT_NOK_WITH_MSG(
        indexer_->CreateQueryReaders(second_view, /*offset_lower_exclusive=*/0, context),
        "committed offset splits a PK memory segment");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> covered_view, indexer_->AcquireReadView());
    read_schema = MakeReadSchema();
    context.read_schema = read_schema.get();
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<BatchReader>> covered_readers,
        indexer_->CreateQueryReaders(covered_view, /*offset_lower_exclusive=*/1, context));
    ASSERT_TRUE(covered_readers.empty());
}

TEST_F(PrimaryKeyMemIndexerTest, TestQueryReadersExposeKeyRange) {
    ASSERT_OK(indexer_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[5, "five"], [1, "one"], [9, "nine"]])"), Range(0, 2)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer_->AcquireReadView());
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema();
    MemQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                            /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<BatchReader>> readers,
        indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, context));
    ASSERT_EQ(1, readers.size());
    auto* range_provider = dynamic_cast<PrimaryKeyRangeProvider*>(readers[0].get());
    ASSERT_NE(nullptr, range_provider);
    ASSERT_EQ(1, range_provider->GetMinKey()->GetLong(0));
    ASSERT_EQ(9, range_provider->GetMaxKey()->GetLong(0));
    readers[0]->Close();
}

TEST_F(PrimaryKeyMemIndexerTest, TestConcurrentReaderReleaseAndWrite) {
    ASSERT_OK(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "one"], [2, "two"]])"), Range(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer_->AcquireReadView());

    std::atomic<bool> reader_ready = false;
    std::atomic<bool> release_reader = false;
    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema();
    std::future<Status> reader = std::async(
        std::launch::async, [this, view = std::move(view), read_schema = std::move(read_schema),
                             &reader_ready, &release_reader]() mutable {
            MemQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                                    /*enable_predicate_pushdown=*/false};
            PAIMON_ASSIGN_OR_RAISE(
                std::vector<std::unique_ptr<BatchReader>> readers,
                indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, context));
            view.reset();
            reader_ready.store(true);
            while (!release_reader.load()) {
            }
            for (const std::unique_ptr<BatchReader>& batch_reader : readers) {
                batch_reader->Close();
            }
            return Status::OK();
        });

    while (!reader_ready.load()) {
        static_cast<void>(indexer_->GetMemoryUsage());
    }
    ASSERT_OK(indexer_->AdvanceCommittedOffset(/*committed_offset=*/1));
    release_reader.store(true);
    ASSERT_OK(indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[3, "three"]])"), Range(2, 2)}));
    ASSERT_OK(reader.get());
}

}  // namespace paimon::test

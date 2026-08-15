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
        auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        merge_function_wrapper_ =
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
        io_manager_ = std::make_shared<IOManager>(test_dir_->Str(), test_dir_->GetFileSystem());
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::WRITE_BUFFER_SPILLABLE, "true"}},
                                                  test_dir_->GetFileSystem()));
        ASSERT_OK_AND_ASSIGN(
            indexer_,
            PrimaryKeyMemIndexer::Create(schema_, {"id"}, key_comparator_, merge_function_wrapper_,
                                         /*restore_max_seq_number=*/-1, options, io_manager_,
                                         /*enable_multi_thread_spill=*/false, pool_));
    }

    std::unique_ptr<RecordBatch> MakeBatch(const std::string& json) const {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json)
                .ValueOrDie();
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        return RecordBatchBuilder(&c_array).Finish().value();
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
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
    std::shared_ptr<IOManager> io_manager_;
    std::shared_ptr<PrimaryKeyMemIndexer> indexer_;
};

TEST_F(PrimaryKeyMemIndexerTest, TestMemoryPoolLifecycle) {
    ASSERT_EQ(0, pool_->CurrentUsage());
    ASSERT_OK(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[1, "one"], [2, "two"]])"), Range(0, 1)}));
    ASSERT_GT(pool_->MaxMemoryUsage(), 0);

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
        std::shared_ptr<arrow::StructType> struct_type =
            std::static_pointer_cast<arrow::StructType>(values->type());
        ASSERT_EQ(3, values->num_fields());
        ASSERT_EQ(SpecialFields::ValueKind().Name(), struct_type->field(0)->name());
        ASSERT_EQ("id", struct_type->field(1)->name());
        ASSERT_EQ("value", struct_type->field(2)->name());
    }
    readers[0]->Close();
    readers.clear();
    imported.reset();
    ASSERT_EQ(baseline, pool_->CurrentUsage());
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

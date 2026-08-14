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

#include "paimon/realtime/realtime_context.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/core/realtime/realtime_primary_key_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class TestingReadView : public MemReadView {
 public:
    explicit TestingReadView(const std::optional<Range>& offset_range)
        : offset_range_(offset_range) {}

    std::optional<Range> GetOffsetRange() const override {
        return offset_range_;
    }

 private:
    std::optional<Range> offset_range_;
};

class TestingMemIndexer : public MemIndexer {
 public:
    Status Write(RealtimeWriteBatch&&) override {
        return Status::OK();
    }

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override {
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>&) override {
        return std::vector<std::unique_ptr<BatchReader>>();
    }

    Result<std::shared_ptr<MemReadView>> AcquireReadView() override {
        ++acquire_count;
        return std::make_shared<TestingReadView>(offset_range);
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<MemReadView>&, int64_t, const MemQueryContext&) override {
        return std::vector<std::unique_ptr<BatchReader>>();
    }

    Status AdvanceCommittedOffset(int64_t committed_offset) override {
        ++advance_count;
        if (fail_next_advance) {
            fail_next_advance = false;
            return Status::Invalid("injected committed offset failure");
        }
        committed_offsets.push_back(committed_offset);
        return Status::OK();
    }

    uint64_t GetMemoryUsage() const override {
        return 0;
    }

    int32_t acquire_count = 0;
    int32_t advance_count = 0;
    bool fail_next_advance = false;
    std::optional<Range> offset_range;
    std::vector<int64_t> committed_offsets;
};

class TestingMemIndexerFactory : public MemIndexerFactory {
 public:
    Result<std::shared_ptr<MemIndexer>> Create(std::unique_ptr<ArrowSchema> write_schema,
                                               const std::map<std::string, std::string>&,
                                               const std::shared_ptr<MemoryPool>&) override {
        if (!write_schema || !write_schema->release) {
            return Status::Invalid("testing write schema is null");
        }
        ArrowSchemaRelease(write_schema.get());
        auto indexer = std::make_shared<TestingMemIndexer>();
        indexers.push_back(indexer);
        return indexer;
    }

    std::vector<std::shared_ptr<TestingMemIndexer>> indexers;
};

std::unique_ptr<ArrowSchema> MakeWriteSchema() {
    auto c_schema = std::make_unique<ArrowSchema>();
    EXPECT_TRUE(
        arrow::ExportSchema(*arrow::schema({arrow::field("id", arrow::int64())}), c_schema.get())
            .ok());
    return c_schema;
}

TEST(RealtimeContextTest, TestReusesIndexerAndCapturesRegisteredViews) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();

    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState first_state,
                         context->GetOrCreateMemIndexer({{"dt", "2026-08-02"}}, 0,
                                                        MakeWriteSchema(), {{"k", "v"}}, pool));
    ASSERT_EQ(0, first_state.initial_offset);
    ASSERT_OK_AND_ASSIGN(
        RealtimeMemIndexerState first_again_state,
        context->GetOrCreateMemIndexer({{"dt", "2026-08-02"}}, 0, MakeWriteSchema(), {}, pool));
    ASSERT_EQ(first_state.indexer, first_again_state.indexer);
    ASSERT_EQ(0, first_again_state.initial_offset);
    ASSERT_EQ(1, factory->indexers.size());
    ASSERT_EQ(1, factory->indexers[0]->acquire_count);

    ASSERT_OK_AND_ASSIGN(
        RealtimeMemIndexerState second_state,
        context->GetOrCreateMemIndexer({{"dt", "2026-08-02"}}, 1, MakeWriteSchema(), {}, pool));
    ASSERT_OK_AND_ASSIGN(
        RealtimeMemIndexerState third_state,
        context->GetOrCreateMemIndexer({{"dt", "2026-08-03"}}, 0, MakeWriteSchema(), {}, pool));
    ASSERT_NE(first_state.indexer, second_state.indexer);
    ASSERT_NE(first_state.indexer, third_state.indexer);
    ASSERT_EQ(3, factory->indexers.size());

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context->AcquireReadViews());
    ASSERT_EQ(3, views.size());
    const RealtimePartitionBucket expected_partition_bucket({{"dt", "2026-08-02"}}, 0);
    ASSERT_EQ(expected_partition_bucket, views[0].partition_bucket);
    ASSERT_EQ(first_state.indexer, views[0].indexer);
    ASSERT_TRUE(views[0].read_view);
    ASSERT_EQ(2, factory->indexers[0]->acquire_count);
    ASSERT_EQ(1, factory->indexers[1]->acquire_count);
    ASSERT_EQ(1, factory->indexers[2]->acquire_count);
}

TEST(RealtimeContextTest, TestGenericCreatorIsLazyAndPreservesInitialOffset) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context, RealtimeContext::Create());
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};
    const RealtimePartitionBucket partition_bucket(partition, /*bucket=*/3);
    ASSERT_OK(context->AdvanceCommittedProgress(5, {{partition_bucket, /*offset=*/7}}));

    auto testing_indexer = std::make_shared<TestingMemIndexer>();
    int32_t creator_count = 0;
    RealtimeContext::MemIndexerCreator creator = [&]() -> Result<std::shared_ptr<MemIndexer>> {
        ++creator_count;
        return std::shared_ptr<MemIndexer>(testing_indexer);
    };
    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState first_state,
                         context->GetOrCreateMemIndexer(partition, 3, creator));
    ASSERT_EQ(testing_indexer, first_state.indexer);
    ASSERT_EQ(8, first_state.initial_offset);
    ASSERT_EQ(1, creator_count);

    testing_indexer->offset_range = Range(8, 12);
    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState reused_state,
                         context->GetOrCreateMemIndexer(partition, 3, creator));
    ASSERT_EQ(first_state.indexer, reused_state.indexer);
    ASSERT_EQ(13, reused_state.initial_offset);
    ASSERT_EQ(1, creator_count);
    ASSERT_EQ(1, testing_indexer->acquire_count);
}

TEST(RealtimeContextTest, TestPropagatesCreatorFailure) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context, RealtimeContext::Create());
    RealtimeContext::MemIndexerCreator creator = []() -> Result<std::shared_ptr<MemIndexer>> {
        return Status::IOError("creator failed");
    };

    Result<RealtimeMemIndexerState> result =
        context->GetOrCreateMemIndexer(/*partition=*/{}, /*bucket=*/0, creator);
    ASSERT_TRUE(result.status().IsIOError());
    ASSERT_NOK_WITH_MSG(result, "creator failed");
}

TEST(RealtimeContextTest, TestRejectsNullCreatedIndexer) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context, RealtimeContext::Create());
    RealtimeContext::MemIndexerCreator null_creator = []() -> Result<std::shared_ptr<MemIndexer>> {
        return std::shared_ptr<MemIndexer>();
    };
    ASSERT_NOK_WITH_MSG(
        context->GetOrCreateMemIndexer(/*partition=*/{}, /*bucket=*/0, null_creator),
        "mem indexer creator returned null");

    auto testing_indexer = std::make_shared<TestingMemIndexer>();
    RealtimeContext::MemIndexerCreator valid_creator = [&]() {
        return Result<std::shared_ptr<MemIndexer>>(testing_indexer);
    };
    ASSERT_OK_AND_ASSIGN(
        RealtimeMemIndexerState state,
        context->GetOrCreateMemIndexer(/*partition=*/{}, /*bucket=*/0, valid_creator));
    ASSERT_EQ(testing_indexer, state.indexer);
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context->AcquireReadViews());
    ASSERT_EQ(1, views.size());
    ASSERT_EQ(testing_indexer, views[0].indexer);
}

TEST(RealtimeContextTest, TestPrimaryKeyWriterRejectsReusedCustomIndexer) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context, RealtimeContext::Create());
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};
    RealtimeContext::MemIndexerCreator creator = []() -> Result<std::shared_ptr<MemIndexer>> {
        return std::shared_ptr<MemIndexer>(std::make_shared<TestingMemIndexer>());
    };
    ASSERT_OK(context->GetOrCreateMemIndexer(partition, 3, creator));

    ASSERT_NOK_WITH_MSG(
        RealtimePrimaryKeyWriter::Create(
            partition, 3, MakeWriteSchema(), /*trimmed_primary_keys=*/{"id"}, context,
            /*merge_tree_writer=*/nullptr, /*options=*/{}, GetDefaultPool(),
            /*file_system=*/nullptr, /*temp_directory=*/"", /*enable_multi_thread_spill=*/false),
        "registered PK mem indexer is not PrimaryKeyMemIndexer");
}

TEST(RealtimeContextTest, TestCommittedProgressIsMonotonicAndSelective) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};

    ASSERT_OK(context->GetOrCreateMemIndexer(partition, 0, MakeWriteSchema(), {}, pool));
    ASSERT_OK(context->GetOrCreateMemIndexer(partition, 1, MakeWriteSchema(), {}, pool));
    ASSERT_EQ(2, factory->indexers.size());

    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(-1, {}),
                        "snapshot id must not be negative");
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(
                            4, {{RealtimePartitionBucket(partition, /*bucket=*/-1), /*offset=*/3}}),
                        "invalid partition-bucket committed offset");
    ASSERT_TRUE(factory->indexers[0]->committed_offsets.empty());
    ASSERT_TRUE(factory->indexers[1]->committed_offsets.empty());

    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {{RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/7},
            {RealtimePartitionBucket({{"dt", "unknown"}}, /*bucket=*/0), /*offset=*/9}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_TRUE(factory->indexers[1]->committed_offsets.empty());

    ASSERT_OK_AND_ASSIGN(
        RealtimeMemIndexerState restored_state,
        context->GetOrCreateMemIndexer({{"dt", "unknown"}}, 0, MakeWriteSchema(), {}, pool));
    ASSERT_EQ(10, restored_state.initial_offset);

    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {{RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/10}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(4, {}),
                        "committed snapshot cannot move backwards");

    ASSERT_OK(context->AdvanceCommittedProgress(
        6, {{RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/7},
            {RealtimePartitionBucket(partition, /*bucket=*/1), /*offset=*/8},
            {RealtimePartitionBucket({{"dt", "unknown"}}, /*bucket=*/0), /*offset=*/9}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_EQ(std::vector<int64_t>({8}), factory->indexers[1]->committed_offsets);
}

TEST(RealtimeContextTest, TestRetriesOnlyIncompleteReclamation) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};

    ASSERT_OK(context->GetOrCreateMemIndexer(partition, 0, MakeWriteSchema(), {}, pool));
    ASSERT_OK(context->GetOrCreateMemIndexer(partition, 1, MakeWriteSchema(), {}, pool));
    ASSERT_OK(context->GetOrCreateMemIndexer(partition, 2, MakeWriteSchema(), {}, pool));
    ASSERT_EQ(3, factory->indexers.size());
    factory->indexers[1]->fail_next_advance = true;

    const RealtimeOffsetMap committed_offsets = {
        {RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/7},
        {RealtimePartitionBucket(partition, /*bucket=*/1), /*offset=*/8},
        {RealtimePartitionBucket(partition, /*bucket=*/2), /*offset=*/9}};
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(5, committed_offsets),
                        "injected committed offset failure");
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_TRUE(factory->indexers[1]->committed_offsets.empty());
    ASSERT_EQ(std::vector<int64_t>({9}), factory->indexers[2]->committed_offsets);

    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState failed_indexer_state,
                         context->GetOrCreateMemIndexer(partition, 1, MakeWriteSchema(), {}, pool));
    ASSERT_EQ(9, failed_indexer_state.initial_offset);

    ASSERT_OK(context->AdvanceCommittedProgress(5, committed_offsets));
    ASSERT_EQ(1, factory->indexers[0]->advance_count);
    ASSERT_EQ(2, factory->indexers[1]->advance_count);
    ASSERT_EQ(1, factory->indexers[2]->advance_count);
    ASSERT_EQ(std::vector<int64_t>({8}), factory->indexers[1]->committed_offsets);
}

TEST(RealtimeContextTest, TestRejectsNullFactory) {
    ASSERT_NOK_WITH_MSG(RealtimeContext::Create(/*factory=*/nullptr),
                        "mem indexer factory is null");
}

}  // namespace
}  // namespace paimon::test

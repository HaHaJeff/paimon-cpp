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
#include "paimon/memory/memory_pool.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class TestingReadView : public MemReadView {
 public:
    std::optional<Range> GetOffsetRange() const override {
        return std::nullopt;
    }
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
        return std::make_shared<TestingReadView>();
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<MemReadView>&, int64_t, const MemQueryContext&) override {
        return std::vector<std::unique_ptr<BatchReader>>();
    }

    Status AdvanceCommittedOffset(int64_t committed_offset) override {
        committed_offsets.push_back(committed_offset);
        return Status::OK();
    }

    uint64_t GetMemoryUsage() const override {
        return 0;
    }

    Status Close() override {
        return Status::OK();
    }

    int32_t acquire_count = 0;
    std::vector<int64_t> committed_offsets;
};

class TestingMemIndexerFactory : public MemIndexerFactory {
 public:
    Result<std::shared_ptr<MemIndexer>> Create(ArrowSchema* write_schema,
                                               const std::map<std::string, std::string>&,
                                               const std::shared_ptr<MemoryPool>&) override {
        if (!write_schema || !write_schema->release) {
            return Status::Invalid("testing write schema is null");
        }
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

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemIndexer> first,
                         context->GetOrCreateMemIndexer({{"dt", "2026-08-02"}}, 0,
                                                        MakeWriteSchema(), {{"k", "v"}}, pool));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<MemIndexer> first_again,
        context->GetOrCreateMemIndexer({{"dt", "2026-08-02"}}, 0, MakeWriteSchema(), {}, pool));
    ASSERT_EQ(first, first_again);
    ASSERT_EQ(1, factory->indexers.size());

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<MemIndexer> second,
        context->GetOrCreateMemIndexer({{"dt", "2026-08-02"}}, 1, MakeWriteSchema(), {}, pool));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<MemIndexer> third,
        context->GetOrCreateMemIndexer({{"dt", "2026-08-03"}}, 0, MakeWriteSchema(), {}, pool));
    ASSERT_NE(first, second);
    ASSERT_NE(first, third);
    ASSERT_EQ(3, factory->indexers.size());

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context->AcquireReadViews());
    ASSERT_EQ(3, views.size());
    const std::map<std::string, std::string> expected_partition = {{"dt", "2026-08-02"}};
    ASSERT_EQ(expected_partition, views[0].partition);
    ASSERT_EQ(0, views[0].bucket);
    ASSERT_EQ(first, views[0].indexer);
    ASSERT_TRUE(views[0].read_view);
    ASSERT_EQ(1, factory->indexers[0]->acquire_count);
    ASSERT_EQ(1, factory->indexers[1]->acquire_count);
    ASSERT_EQ(1, factory->indexers[2]->acquire_count);
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
    ASSERT_NOK_WITH_MSG(
        context->AdvanceCommittedProgress(
            4, {RealtimePartitionBucketOffset{partition, /*bucket=*/-1, /*offset=*/3}}),
        "invalid partition-bucket committed offset");
    ASSERT_TRUE(factory->indexers[0]->committed_offsets.empty());
    ASSERT_TRUE(factory->indexers[1]->committed_offsets.empty());

    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {RealtimePartitionBucketOffset{partition, /*bucket=*/0, /*offset=*/7},
            RealtimePartitionBucketOffset{{{"dt", "unknown"}},
                                          /*bucket=*/0,
                                          /*offset=*/9}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_TRUE(factory->indexers[1]->committed_offsets.empty());

    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {RealtimePartitionBucketOffset{partition, /*bucket=*/0, /*offset=*/10}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(4, {}),
                        "committed snapshot cannot move backwards");

    ASSERT_OK(context->AdvanceCommittedProgress(
        6, {RealtimePartitionBucketOffset{partition, /*bucket=*/1, /*offset=*/8}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_EQ(std::vector<int64_t>({8}), factory->indexers[1]->committed_offsets);
}

TEST(RealtimeContextTest, TestRejectsNullFactory) {
    ASSERT_NOK_WITH_MSG(RealtimeContext::Create(/*factory=*/nullptr),
                        "mem indexer factory is null");
}

}  // namespace
}  // namespace paimon::test

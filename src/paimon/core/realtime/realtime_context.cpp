/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/realtime/realtime_context.h"

#include <map>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "paimon/arrow/abi.h"
#include "paimon/core/realtime/partition_bucket.h"
#include "paimon/macros.h"
#include "paimon/realtime/arrow_mem_indexer_factory.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/status.h"

namespace paimon {

class RealtimeContext::Impl {
 public:
    explicit Impl(const std::shared_ptr<MemIndexerFactory>& factory) : factory_(factory) {}

    Result<std::shared_ptr<MemIndexer>> GetOrCreateMemIndexer(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        std::unique_ptr<ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool) {
        std::lock_guard<std::mutex> lock(mutex_);
        const PartitionBucket key(partition, bucket);
        auto iter = indexers_.find(key);
        if (iter != indexers_.end()) {
            if (write_schema && write_schema->release) {
                write_schema->release(write_schema.get());
            }
            return iter->second;
        }
        Result<std::shared_ptr<MemIndexer>> indexer_result =
            factory_->Create(write_schema.get(), options, memory_pool);
        if (write_schema && write_schema->release) {
            write_schema->release(write_schema.get());
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemIndexer> indexer, std::move(indexer_result));
        indexers_.emplace(key, indexer);
        return indexer;
    }

    Result<std::vector<RealtimePartitionBucketView>> AcquireReadViews() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<RealtimePartitionBucketView> result;
        result.reserve(indexers_.size());
        for (const auto& [partition_bucket, indexer] : indexers_) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemReadView> read_view,
                                   indexer->AcquireReadView());
            result.push_back(RealtimePartitionBucketView{partition_bucket.partition,
                                                         partition_bucket.bucket, indexer,
                                                         std::move(read_view)});
        }
        return result;
    }

    Status AdvanceCommittedProgress(
        int64_t snapshot_id, const std::vector<RealtimePartitionBucketOffset>& committed_offsets) {
        if (snapshot_id < 0) {
            return Status::Invalid("real-time refresh snapshot id must not be negative");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (last_refreshed_snapshot_id_ && snapshot_id < last_refreshed_snapshot_id_.value()) {
            return Status::Invalid("real-time committed snapshot cannot move backwards");
        }
        if (last_refreshed_snapshot_id_ && snapshot_id == last_refreshed_snapshot_id_.value()) {
            return Status::OK();
        }
        for (const RealtimePartitionBucketOffset& committed : committed_offsets) {
            if (committed.bucket < 0 || committed.offset < 0) {
                return Status::Invalid("invalid partition-bucket committed offset");
            }
            auto iter = indexers_.find(PartitionBucket(committed.partition, committed.bucket));
            if (iter != indexers_.end()) {
                PAIMON_RETURN_NOT_OK(iter->second->Reclaim(committed.offset));
            }
        }
        last_refreshed_snapshot_id_ = snapshot_id;
        return Status::OK();
    }

 private:
    std::shared_ptr<MemIndexerFactory> factory_;
    std::mutex mutex_;
    std::map<PartitionBucket, std::shared_ptr<MemIndexer>> indexers_;
    std::optional<int64_t> last_refreshed_snapshot_id_;
};

Result<std::shared_ptr<RealtimeContext>> RealtimeContext::Create() {
    return Create(std::make_shared<ArrowMemIndexerFactory>());
}

Result<std::shared_ptr<RealtimeContext>> RealtimeContext::Create(
    const std::shared_ptr<MemIndexerFactory>& factory) {
    if (!factory) {
        return Status::Invalid("mem indexer factory is null");
    }
    return std::shared_ptr<RealtimeContext>(new RealtimeContext(std::make_unique<Impl>(factory)));
}

RealtimeContext::RealtimeContext(std::unique_ptr<Impl>&& impl) : impl_(std::move(impl)) {}

RealtimeContext::~RealtimeContext() = default;

Result<std::shared_ptr<MemIndexer>> RealtimeContext::GetOrCreateMemIndexer(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    std::unique_ptr<ArrowSchema> write_schema, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    return impl_->GetOrCreateMemIndexer(partition, bucket, std::move(write_schema), options,
                                        memory_pool);
}

Result<std::vector<RealtimePartitionBucketView>> RealtimeContext::AcquireReadViews() {
    return impl_->AcquireReadViews();
}

Status RealtimeContext::AdvanceCommittedProgress(
    int64_t snapshot_id, const std::vector<RealtimePartitionBucketOffset>& committed_offsets) {
    return impl_->AdvanceCommittedProgress(snapshot_id, committed_offsets);
}

}  // namespace paimon

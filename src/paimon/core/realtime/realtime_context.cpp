/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/realtime/realtime_context.h"

#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/c/helpers.h"
#include "paimon/arrow/abi.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/macros.h"
#include "paimon/realtime/arrow_mem_indexer_factory.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/realtime/primary_key_mem_indexer_factory.h"
#include "paimon/status.h"

namespace paimon {

class RealtimeContext::Impl {
 public:
    virtual ~Impl() = default;

    template <typename CreateIndexer>
    Result<RealtimeMemIndexerState> GetOrCreate(const std::map<std::string, std::string>& partition,
                                                int32_t bucket,
                                                std::unique_ptr<ArrowSchema> write_schema,
                                                CreateIndexer&& create_indexer) {
        ScopeGuard schema_guard([&write_schema]() {
            if (write_schema) {
                ArrowSchemaRelease(write_schema.get());
            }
        });
        std::lock_guard<std::mutex> progress_lock(progress_mutex_);
        std::lock_guard<std::mutex> registry_lock(mutex_);
        const RealtimePartitionBucket key(partition, bucket);
        int64_t initial_offset = 0;
        auto offset_iter = committed_offsets_.find(key);
        if (offset_iter != committed_offsets_.end()) {
            if (offset_iter->second == std::numeric_limits<int64_t>::max()) {
                return Status::Invalid("real-time offset has reached INT64_MAX");
            }
            initial_offset = offset_iter->second + 1;
        }
        auto iter = indexers_.find(key);
        if (iter != indexers_.end()) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemReadView> read_view,
                                   iter->second->AcquireReadView());
            if (!read_view) {
                return Status::Invalid("mem indexer returned a null read view");
            }
            const std::optional<Range> memory_range = read_view->GetOffsetRange();
            if (memory_range) {
                if (memory_range->to == std::numeric_limits<int64_t>::max()) {
                    return Status::Invalid("real-time offset has reached INT64_MAX");
                }
                // A reused indexer may retain sealed-but-uncommitted rows beyond the committed
                // offset. Continue after all retained rows instead of restarting across a seal.
                if (memory_range->to >= initial_offset) {
                    initial_offset = memory_range->to + 1;
                }
            }
            return RealtimeMemIndexerState{iter->second, initial_offset};
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemIndexer> indexer,
                               create_indexer(std::move(write_schema)));
        if (!indexer) {
            return Status::Invalid("mem indexer factory returned null");
        }
        indexers_.emplace(key, indexer);
        if (offset_iter != committed_offsets_.end()) {
            reclaimed_offsets_.emplace(key, offset_iter->second);
        }
        return RealtimeMemIndexerState{std::move(indexer), initial_offset};
    }

 public:
    Result<std::vector<RealtimePartitionBucketView>> AcquireReadViews() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<RealtimePartitionBucketView> result;
        result.reserve(indexers_.size());
        for (const auto& [partition_bucket, indexer] : indexers_) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemReadView> read_view,
                                   indexer->AcquireReadView());
            result.push_back(
                RealtimePartitionBucketView{partition_bucket, indexer, std::move(read_view)});
        }
        return result;
    }

    Status AdvanceCommittedProgress(int64_t snapshot_id,
                                    const RealtimeOffsetMap& committed_offsets) {
        if (snapshot_id < 0) {
            return Status::Invalid("real-time refresh snapshot id must not be negative");
        }
        std::lock_guard<std::mutex> progress_lock(progress_mutex_);
        if (last_refreshed_snapshot_id_ && snapshot_id < last_refreshed_snapshot_id_.value()) {
            return Status::Invalid("real-time committed snapshot cannot move backwards");
        }
        if (!last_refreshed_snapshot_id_ || snapshot_id > last_refreshed_snapshot_id_.value()) {
            for (const auto& [partition_bucket, committed_offset] : committed_offsets) {
                if (partition_bucket.bucket < 0 || committed_offset < 0) {
                    return Status::Invalid("invalid partition-bucket committed offset");
                }
                auto previous_iter = committed_offsets_.find(partition_bucket);
                if (previous_iter != committed_offsets_.end()) {
                    if (committed_offset < previous_iter->second) {
                        return Status::Invalid(
                            "real-time partition-bucket committed offset cannot move backwards");
                    }
                }
            }
            committed_offsets_ = committed_offsets;
            last_refreshed_snapshot_id_ = snapshot_id;
        }

        std::vector<std::tuple<RealtimePartitionBucket, std::shared_ptr<MemIndexer>, int64_t>>
            notifications;
        {
            std::lock_guard<std::mutex> registry_lock(mutex_);
            for (const auto& [partition_bucket, committed_offset] : committed_offsets_) {
                auto reclaimed_iter = reclaimed_offsets_.find(partition_bucket);
                if (reclaimed_iter != reclaimed_offsets_.end() &&
                    reclaimed_iter->second >= committed_offset) {
                    continue;
                }
                auto indexer_iter = indexers_.find(partition_bucket);
                if (indexer_iter != indexers_.end()) {
                    notifications.emplace_back(partition_bucket, indexer_iter->second,
                                               committed_offset);
                }
            }
        }
        // Reclaim independent indexers on a best-effort basis, then report the first error.
        Status first_error = Status::OK();
        for (const auto& [partition_bucket, indexer, committed_offset] : notifications) {
            Status status = indexer->AdvanceCommittedOffset(committed_offset);
            if (status.ok()) {
                reclaimed_offsets_[partition_bucket] = committed_offset;
            } else if (first_error.ok()) {
                first_error = std::move(status);
            }
        }
        return first_error;
    }

 private:
    std::mutex mutex_;
    std::mutex progress_mutex_;
    std::map<RealtimePartitionBucket, std::shared_ptr<MemIndexer>> indexers_;
    RealtimeOffsetMap committed_offsets_;
    RealtimeOffsetMap reclaimed_offsets_;
    std::optional<int64_t> last_refreshed_snapshot_id_;
};

template <typename Factory>
class RealtimeContext::FactoryImpl final : public RealtimeContext::Impl {
 public:
    explicit FactoryImpl(std::shared_ptr<Factory> factory) : factory_(std::move(factory)) {}

    const std::shared_ptr<Factory>& GetFactory() const {
        return factory_;
    }

 private:
    std::shared_ptr<Factory> factory_;
};

Result<std::shared_ptr<RealtimeContext>> RealtimeContext::Create() {
    return std::shared_ptr<RealtimeContext>(new RealtimeContext(std::make_unique<Impl>()));
}

Result<std::shared_ptr<RealtimeContext>> RealtimeContext::Create(
    const std::shared_ptr<MemIndexerFactory>& factory) {
    if (!factory) {
        return Status::Invalid("mem indexer factory is null");
    }
    return std::shared_ptr<RealtimeContext>(
        new RealtimeContext(std::make_unique<FactoryImpl<MemIndexerFactory>>(factory)));
}

Result<std::shared_ptr<RealtimeContext>> RealtimeContext::CreatePrimaryKey(
    const std::shared_ptr<PrimaryKeyMemIndexerFactory>& factory) {
    if (!factory) {
        return Status::Invalid("primary-key mem indexer factory is null");
    }
    return std::shared_ptr<RealtimeContext>(
        new RealtimeContext(std::make_unique<FactoryImpl<PrimaryKeyMemIndexerFactory>>(factory)));
}

RealtimeContext::RealtimeContext(std::unique_ptr<Impl>&& impl) : impl_(std::move(impl)) {}

RealtimeContext::~RealtimeContext() = default;

Result<RealtimeMemIndexerState> RealtimeContext::GetOrCreateMemIndexer(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    std::unique_ptr<ArrowSchema> write_schema, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (dynamic_cast<FactoryImpl<PrimaryKeyMemIndexerFactory>*>(impl_.get())) {
        return Status::Invalid("primary-key mem indexer factory cannot create append indexers");
    }
    FactoryImpl<MemIndexerFactory>* configured_factory =
        dynamic_cast<FactoryImpl<MemIndexerFactory>*>(impl_.get());
    return impl_->GetOrCreate(
        partition, bucket, std::move(write_schema),
        [&options, &memory_pool, configured_factory](std::unique_ptr<ArrowSchema> schema) {
            if (configured_factory) {
                return configured_factory->GetFactory()->Create(std::move(schema), options,
                                                                memory_pool);
            }
            ArrowMemIndexerFactory factory;
            return factory.Create(std::move(schema), options, memory_pool);
        });
}

Result<RealtimeMemIndexerState> RealtimeContext::GetOrCreatePrimaryKeyMemIndexer(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    std::unique_ptr<ArrowSchema> write_schema, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<PrimaryKeyMemIndexerFactory>& factory,
    int64_t restore_max_sequence_number) {
    if (dynamic_cast<FactoryImpl<MemIndexerFactory>*>(impl_.get())) {
        return Status::Invalid("generic mem indexer factory cannot create primary-key indexers");
    }
    FactoryImpl<PrimaryKeyMemIndexerFactory>* configured_factory =
        dynamic_cast<FactoryImpl<PrimaryKeyMemIndexerFactory>*>(impl_.get());
    const std::shared_ptr<PrimaryKeyMemIndexerFactory>& effective_factory =
        configured_factory ? configured_factory->GetFactory() : factory;
    const PrimaryKeyMemIndexerCreationContext creation_context{partition, bucket,
                                                               restore_max_sequence_number};
    return impl_->GetOrCreate(partition, bucket, std::move(write_schema),
                              [&options, &memory_pool, &effective_factory,
                               &creation_context](std::unique_ptr<ArrowSchema> schema) {
                                  return effective_factory->Create(std::move(schema), options,
                                                                   memory_pool, creation_context);
                              });
}

Result<std::vector<RealtimePartitionBucketView>> RealtimeContext::AcquireReadViews() {
    return impl_->AcquireReadViews();
}

Status RealtimeContext::AdvanceCommittedProgress(int64_t snapshot_id,
                                                 const RealtimeOffsetMap& committed_offsets) {
    return impl_->AdvanceCommittedProgress(snapshot_id, committed_offsets);
}

}  // namespace paimon

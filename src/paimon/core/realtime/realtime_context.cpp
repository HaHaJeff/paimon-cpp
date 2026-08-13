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

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/arrow/abi.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/realtime/primary_key_mem_indexer.h"
#include "paimon/macros.h"
#include "paimon/realtime/arrow_mem_indexer_factory.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/status.h"

namespace paimon {
namespace {

enum class MemIndexerKind { APPEND, PRIMARY_KEY };

struct RegisteredMemIndexer {
    std::shared_ptr<MemIndexer> indexer;
    MemIndexerKind kind;
};

Result<std::shared_ptr<PrimaryKeyMemIndexer>> CreatePrimaryKeyMemIndexer(
    std::unique_ptr<ArrowSchema> write_schema, const std::vector<std::string>& trimmed_primary_keys,
    const std::map<std::string, std::string>& options, const PrimaryKeyMemIndexerContext& context) {
    if (!write_schema || !write_schema->release) {
        return Status::Invalid("mem indexer write schema is null");
    }
    ScopeGuard schema_guard([schema = write_schema.get()]() { ArrowSchemaRelease(schema); });
    if (!context.memory_pool) {
        return Status::Invalid("mem indexer memory pool is null");
    }
    if (!context.file_system) {
        return Status::Invalid("mem indexer file system is null");
    }
    if (context.temp_directory.empty()) {
        return Status::Invalid("mem indexer temporary directory is empty");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> imported_schema,
                                      arrow::ImportSchema(write_schema.get()));
    std::vector<DataField> key_fields;
    key_fields.reserve(trimmed_primary_keys.size());
    for (int32_t i = 0; i < static_cast<int32_t>(trimmed_primary_keys.size()); ++i) {
        std::shared_ptr<arrow::Field> field =
            imported_schema->GetFieldByName(trimmed_primary_keys[i]);
        if (!field) {
            return Status::Invalid("primary key ", trimmed_primary_keys[i],
                                   " is missing from write schema");
        }
        key_fields.emplace_back(i, field);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(key_fields, /*is_ascending_order=*/true));
    auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
    auto merge_wrapper = std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options, context.file_system));
    auto io_manager = std::make_shared<IOManager>(context.temp_directory, context.file_system);
    return PrimaryKeyMemIndexer::Create(imported_schema, trimmed_primary_keys, key_comparator,
                                        merge_wrapper, core_options, io_manager,
                                        context.enable_multi_thread_spill, context.memory_pool);
}

}  // namespace

class RealtimeContext::Impl {
 public:
    explicit Impl(const std::shared_ptr<MemIndexerFactory>& factory) : factory_(factory) {}

    Result<RealtimeMemIndexerState> GetOrCreateMemIndexer(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        std::unique_ptr<ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool) {
        return GetOrCreateMemIndexer(partition, bucket, std::move(write_schema), {}, options,
                                     memory_pool, nullptr, MemIndexerKind::APPEND);
    }

    Result<RealtimePrimaryKeyMemIndexerState> GetOrCreatePrimaryKeyMemIndexer(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        std::unique_ptr<ArrowSchema> write_schema,
        const std::vector<std::string>& trimmed_primary_keys,
        const std::map<std::string, std::string>& options,
        const PrimaryKeyMemIndexerContext& context) {
        PAIMON_ASSIGN_OR_RAISE(
            RealtimeMemIndexerState state,
            GetOrCreateMemIndexer(partition, bucket, std::move(write_schema), trimmed_primary_keys,
                                  options, context.memory_pool, &context,
                                  MemIndexerKind::PRIMARY_KEY));
        std::shared_ptr<PrimaryKeyMemIndexer> indexer =
            std::dynamic_pointer_cast<PrimaryKeyMemIndexer>(state.indexer);
        if (!indexer) {
            return Status::Invalid("registered primary-key mem indexer has an invalid type");
        }
        return RealtimePrimaryKeyMemIndexerState{std::move(indexer), state.initial_offset};
    }

 private:
    Result<RealtimeMemIndexerState> GetOrCreateMemIndexer(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        std::unique_ptr<ArrowSchema> write_schema,
        const std::vector<std::string>& trimmed_primary_keys,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const PrimaryKeyMemIndexerContext* primary_key_context, MemIndexerKind kind) {
        std::lock_guard<std::mutex> progress_lock(progress_mutex_);
        std::lock_guard<std::mutex> registry_lock(mutex_);
        const RealtimePartitionBucket key(partition, bucket);
        int64_t initial_offset = 0;
        auto offset_iter = committed_offsets_.find(key);
        if (offset_iter != committed_offsets_.end()) {
            if (offset_iter->second == std::numeric_limits<int64_t>::max()) {
                if (write_schema) {
                    ArrowSchemaRelease(write_schema.get());
                }
                return Status::Invalid("real-time offset has reached INT64_MAX");
            }
            initial_offset = offset_iter->second + 1;
        }
        auto iter = indexers_.find(key);
        if (iter != indexers_.end()) {
            if (write_schema) {
                ArrowSchemaRelease(write_schema.get());
            }
            if (iter->second.kind != kind) {
                return Status::Invalid(
                    "partition-bucket already uses an incompatible mem indexer kind");
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemReadView> read_view,
                                   iter->second.indexer->AcquireReadView());
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
            return RealtimeMemIndexerState{iter->second.indexer, initial_offset};
        }
        std::shared_ptr<MemIndexer> indexer;
        if (kind == MemIndexerKind::PRIMARY_KEY) {
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<PrimaryKeyMemIndexer> primary_key_indexer,
                CreatePrimaryKeyMemIndexer(std::move(write_schema), trimmed_primary_keys, options,
                                           *primary_key_context));
            indexer = std::move(primary_key_indexer);
        } else {
            PAIMON_ASSIGN_OR_RAISE(indexer,
                                   factory_->Create(std::move(write_schema), options, memory_pool));
        }
        indexers_.emplace(key, RegisteredMemIndexer{indexer, kind});
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
        for (const auto& [partition_bucket, registered] : indexers_) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemReadView> read_view,
                                   registered.indexer->AcquireReadView());
            result.push_back(RealtimePartitionBucketView{partition_bucket, registered.indexer,
                                                         std::move(read_view)});
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
                    notifications.emplace_back(partition_bucket, indexer_iter->second.indexer,
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
    std::shared_ptr<MemIndexerFactory> factory_;
    std::mutex mutex_;
    std::mutex progress_mutex_;
    std::map<RealtimePartitionBucket, RegisteredMemIndexer> indexers_;
    RealtimeOffsetMap committed_offsets_;
    RealtimeOffsetMap reclaimed_offsets_;
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

Result<RealtimeMemIndexerState> RealtimeContext::GetOrCreateMemIndexer(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    std::unique_ptr<ArrowSchema> write_schema, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    return impl_->GetOrCreateMemIndexer(partition, bucket, std::move(write_schema), options,
                                        memory_pool);
}

Result<RealtimePrimaryKeyMemIndexerState> RealtimeContext::GetOrCreatePrimaryKeyMemIndexer(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    std::unique_ptr<ArrowSchema> write_schema, const std::vector<std::string>& trimmed_primary_keys,
    const std::map<std::string, std::string>& options, const PrimaryKeyMemIndexerContext& context) {
    return impl_->GetOrCreatePrimaryKeyMemIndexer(partition, bucket, std::move(write_schema),
                                                  trimmed_primary_keys, options, context);
}

Result<std::vector<RealtimePartitionBucketView>> RealtimeContext::AcquireReadViews() {
    return impl_->AcquireReadViews();
}

Status RealtimeContext::AdvanceCommittedProgress(int64_t snapshot_id,
                                                 const RealtimeOffsetMap& committed_offsets) {
    return impl_->AdvanceCommittedProgress(snapshot_id, committed_offsets);
}

}  // namespace paimon

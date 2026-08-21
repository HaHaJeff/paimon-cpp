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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "paimon/core/utils/batch_writer.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/realtime/realtime_store.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class MemoryPool;
class MergeTreeWriter;
class RealtimeContextImpl;
struct RealtimeStoreState;

/// Primary-key real-time writer backed by an in-memory mutation indexer.
class RealtimePrimaryKeyWriter final : public BatchWriter {
 public:
    static Result<std::shared_ptr<RealtimePrimaryKeyWriter>> Create(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::shared_ptr<RealtimeContextImpl>& realtime_context,
        const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
        const std::shared_ptr<MemoryPool>& memory_pool, const RealtimeStoreState& store_state);

    Status Write(std::unique_ptr<RecordBatch>&& batch) override;
    Result<CommitIncrement> PrepareCommit(bool wait_compaction) override;
    Status Compact(bool full_compaction) override;
    uint64_t GetMemoryUsage() const override;
    Status FlushMemory() override;
    Result<bool> CompactNotCompleted() override;
    Status Sync() override;
    Status Close() override;
    std::shared_ptr<Metrics> GetMetrics() const override;

 private:
    RealtimePrimaryKeyWriter(const std::shared_ptr<RealtimeStore>& realtime_store,
                             const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
                             const std::shared_ptr<RealtimeContextImpl>& realtime_context,
                             const RealtimePartitionBucket& partition_bucket,
                             const std::shared_ptr<arrow::Schema>& write_schema,
                             int64_t next_offset, const std::shared_ptr<MemoryPool>& memory_pool);

    Status FlushSegment(const std::shared_ptr<RealtimeSegmentHandle>& segment);

    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<RealtimeStore> realtime_store_;
    std::shared_ptr<MergeTreeWriter> merge_tree_writer_;
    std::shared_ptr<RealtimeContextImpl> realtime_context_;
    RealtimePartitionBucket partition_bucket_;
    std::shared_ptr<arrow::Schema> write_schema_;
    int64_t next_offset_;
    std::mutex realtime_store_mutex_;
    std::mutex prepare_mutex_;
};

}  // namespace paimon

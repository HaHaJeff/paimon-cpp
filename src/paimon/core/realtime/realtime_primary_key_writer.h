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
#include <vector>

#include "paimon/core/utils/batch_writer.h"
#include "paimon/realtime/mem_indexer.h"

struct ArrowSchema;

namespace arrow {
class Schema;
}

namespace paimon {

class FileSystem;
class MemoryPool;
class MergeTreeWriter;
class RealtimeContext;

class RealtimePrimaryKeyWriter final : public BatchWriter {
 public:
    static Result<std::shared_ptr<RealtimePrimaryKeyWriter>> Create(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        std::unique_ptr<::ArrowSchema> write_schema,
        const std::vector<std::string>& trimmed_primary_keys,
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<FileSystem>& file_system, const std::string& temp_directory,
        bool enable_multi_thread_spill, int64_t restore_max_seq_number);

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
    RealtimePrimaryKeyWriter(const std::shared_ptr<MemIndexer>& mem_indexer,
                             const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
                             const std::shared_ptr<arrow::Schema>& key_schema,
                             const std::shared_ptr<arrow::Schema>& value_schema,
                             const std::shared_ptr<MemoryPool>& memory_pool, int64_t next_offset);

    Status FlushSegment(const std::shared_ptr<RealtimeSegmentHandle>& segment);

    std::shared_ptr<MemIndexer> mem_indexer_;
    std::shared_ptr<MergeTreeWriter> merge_tree_writer_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<MemoryPool> memory_pool_;
    int64_t next_offset_;
    std::mutex mem_indexer_mutex_;
    std::mutex prepare_mutex_;
};

}  // namespace paimon

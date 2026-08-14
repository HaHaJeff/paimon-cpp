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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/realtime/mem_indexer.h"

namespace arrow {
class Schema;
}

namespace paimon {

class CoreOptions;
class FieldsComparator;
class IOManager;
class KeyValue;
class MemoryPool;
template <typename T>
class MergeFunctionWrapper;

/// Built-in memory indexer selected explicitly for primary-key tables.
///
/// Building and immutable segments use independent spillable write buffers. Read views and
/// readers pin their segments, so committed-offset reclamation cannot remove live spill files.
class PrimaryKeyMemIndexer : public MemIndexer {
 public:
    ~PrimaryKeyMemIndexer() override;
    static Result<std::shared_ptr<PrimaryKeyMemIndexer>> Create(
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::vector<std::string>& trimmed_primary_keys,
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
        int64_t restore_max_seq_number, const CoreOptions& options,
        const std::shared_ptr<IOManager>& io_manager, bool enable_multi_thread_spill,
        const std::shared_ptr<MemoryPool>& memory_pool);

    Status Write(RealtimeWriteBatch&& write_batch) override;
    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override;
    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) override;
    Result<std::shared_ptr<MemReadView>> AcquireReadView() override;
    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<MemReadView>& view, int64_t offset_lower_exclusive,
        const MemQueryContext& context) override;
    Result<std::optional<Range>> GetPrimaryKeyQuerySequenceRange(
        const std::shared_ptr<MemReadView>& view, int64_t offset_lower_exclusive);
    Result<std::vector<std::unique_ptr<BatchReader>>> CreatePrimaryKeyQueryReaders(
        const std::shared_ptr<MemReadView>& view, int64_t offset_lower_exclusive,
        const MemQueryContext& context);
    Status AdvanceCommittedOffset(int64_t committed_offset) override;
    uint64_t GetMemoryUsage() const override;

 private:
    class Impl;
    explicit PrimaryKeyMemIndexer(std::unique_ptr<Impl>&& impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon

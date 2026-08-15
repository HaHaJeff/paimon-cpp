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

#include "paimon/core/realtime/realtime_primary_key_writer.h"

#include <limits>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/key_value_batch_record_reader.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/macros.h"
#include "paimon/realtime/realtime_context.h"

namespace paimon {

Result<std::shared_ptr<RealtimePrimaryKeyWriter>> RealtimePrimaryKeyWriter::Create(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    std::unique_ptr<::ArrowSchema> write_schema,
    const std::vector<std::string>& trimmed_primary_keys,
    const std::shared_ptr<RealtimeContext>& realtime_context,
    const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<FileSystem>& file_system,
    const std::string& temp_directory, bool enable_multi_thread_spill,
    int64_t restore_max_seq_number) {
    if (!realtime_context) {
        return Status::Invalid("PK real-time context is null");
    }
    if (!write_schema || !write_schema->release) {
        return Status::Invalid("PK real-time write schema is null");
    }
    ScopeGuard schema_guard([schema = write_schema.get()]() { ArrowSchemaRelease(schema); });
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> value_schema,
                                      arrow::ImportSchema(write_schema.get()));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*value_schema, write_schema.get()));
    arrow::FieldVector key_fields;
    key_fields.reserve(trimmed_primary_keys.size());
    for (const std::string& primary_key : trimmed_primary_keys) {
        std::shared_ptr<arrow::Field> key_field = value_schema->GetFieldByName(primary_key);
        if (!key_field) {
            return Status::Invalid("primary key ", primary_key, " is missing from write schema");
        }
        key_fields.push_back(std::move(key_field));
    }
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema(std::move(key_fields));
    MemIndexerCreateRequest request{
        std::move(write_schema),
        options,
        memory_pool,
        partition,
        bucket,
        PrimaryKeyMemIndexerCreateConfig{trimmed_primary_keys, restore_max_seq_number, file_system,
                                         temp_directory, enable_multi_thread_spill}};
    schema_guard.Release();
    PAIMON_ASSIGN_OR_RAISE(RealtimeMemIndexerState indexer_state,
                           realtime_context->GetOrCreateMemIndexer(std::move(request)));
    return std::shared_ptr<RealtimePrimaryKeyWriter>(
        new RealtimePrimaryKeyWriter(indexer_state.indexer, merge_tree_writer, key_schema,
                                     value_schema, memory_pool, indexer_state.initial_offset));
}

RealtimePrimaryKeyWriter::RealtimePrimaryKeyWriter(
    const std::shared_ptr<MemIndexer>& mem_indexer,
    const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool, int64_t next_offset)
    : mem_indexer_(mem_indexer),
      merge_tree_writer_(merge_tree_writer),
      key_schema_(key_schema),
      value_schema_(value_schema),
      memory_pool_(memory_pool),
      next_offset_(next_offset) {}

Status RealtimePrimaryKeyWriter::Write(std::unique_ptr<RecordBatch>&& batch) {
    const int64_t row_count = batch->GetData()->length;
    if (row_count == 0) {
        return Status::OK();
    }
    // INT64_MAX is reserved as the exhausted next-offset sentinel.
    if (row_count > std::numeric_limits<int64_t>::max() - next_offset_) {
        return Status::Invalid("real-time offset range exceeds INT64_MAX");
    }
    const Range range(next_offset_, next_offset_ + row_count - 1);
    PAIMON_RETURN_NOT_OK(mem_indexer_->Write(RealtimeWriteBatch{std::move(batch), range}));
    next_offset_ += row_count;
    return Status::OK();
}

Result<CommitIncrement> RealtimePrimaryKeyWriter::PrepareCommit(bool wait_compaction) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                           mem_indexer_->SealForCommit());
    if (segment) {
        PAIMON_RETURN_NOT_OK(FlushSegment(segment.value()));
    }
    PAIMON_ASSIGN_OR_RAISE(CommitIncrement increment,
                           merge_tree_writer_->PrepareCommit(wait_compaction));
    if (segment) {
        increment.SetRealtimeOffsetRange(segment.value()->GetOffsetRange());
    }
    return increment;
}

Status RealtimePrimaryKeyWriter::FlushSegment(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                           mem_indexer_->CreateCommitReaders(segment));
    std::vector<std::unique_ptr<KeyValueRecordReader>> sorted_readers;
    sorted_readers.reserve(readers.size());
    for (std::unique_ptr<BatchReader>& reader : readers) {
        sorted_readers.push_back(std::make_unique<KeyValueBatchRecordReader>(
            std::move(reader), key_schema_, value_schema_, memory_pool_));
    }
    return merge_tree_writer_->WriteSortedReaders(std::move(sorted_readers));
}

Status RealtimePrimaryKeyWriter::Compact(bool) {
    return Status::Invalid("PK real-time write does not support explicit compaction");
}

uint64_t RealtimePrimaryKeyWriter::GetMemoryUsage() const {
    return mem_indexer_->GetMemoryUsage();
}

Status RealtimePrimaryKeyWriter::FlushMemory() {
    return Status::OK();
}

Result<bool> RealtimePrimaryKeyWriter::CompactNotCompleted() {
    return merge_tree_writer_->CompactNotCompleted();
}

Status RealtimePrimaryKeyWriter::Sync() {
    return merge_tree_writer_->Sync();
}

Status RealtimePrimaryKeyWriter::Close() {
    return merge_tree_writer_->Close();
}

std::shared_ptr<Metrics> RealtimePrimaryKeyWriter::GetMetrics() const {
    return merge_tree_writer_->GetMetrics();
}

}  // namespace paimon

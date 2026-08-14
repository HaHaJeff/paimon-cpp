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
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/realtime/primary_key_mem_indexer.h"
#include "paimon/core/realtime/primary_key_mem_indexer_factory.h"
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
    auto factory = std::make_shared<PrimaryKeyMemIndexerFactory>(
        trimmed_primary_keys, restore_max_seq_number, file_system, temp_directory,
        enable_multi_thread_spill);
    PAIMON_ASSIGN_OR_RAISE(
        RealtimeMemIndexerState indexer_state,
        realtime_context->GetOrCreateMemIndexer(partition, bucket, std::move(write_schema), options,
                                                memory_pool, factory));
    std::shared_ptr<PrimaryKeyMemIndexer> indexer =
        std::dynamic_pointer_cast<PrimaryKeyMemIndexer>(indexer_state.indexer);
    if (!indexer) {
        return Status::Invalid("registered PK mem indexer is not PrimaryKeyMemIndexer");
    }
    return std::shared_ptr<RealtimePrimaryKeyWriter>(
        new RealtimePrimaryKeyWriter(indexer, merge_tree_writer, indexer_state.initial_offset));
}

RealtimePrimaryKeyWriter::RealtimePrimaryKeyWriter(
    const std::shared_ptr<PrimaryKeyMemIndexer>& mem_indexer,
    const std::shared_ptr<MergeTreeWriter>& merge_tree_writer, int64_t next_offset)
    : mem_indexer_(mem_indexer), merge_tree_writer_(merge_tree_writer), next_offset_(next_offset) {}

Status RealtimePrimaryKeyWriter::Write(std::unique_ptr<RecordBatch>&& batch) {
    const int64_t row_count = batch->GetData()->length;
    if (row_count == 0) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> lock(mem_indexer_mutex_);
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
    std::lock_guard<std::mutex> lock(prepare_mutex_);
    std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment;
    {
        std::lock_guard<std::mutex> mem_indexer_lock(mem_indexer_mutex_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<RealtimeSegmentHandle>> sealed_segment,
                               mem_indexer_->SealForCommit());
        segment = std::move(sealed_segment);
    }
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

Result<std::unique_ptr<RecordBatch>> RealtimePrimaryKeyWriter::ToMutationRecordBatch(
    BatchReader::ReadBatch&& batch) {
    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    std::shared_ptr<arrow::StructArray> struct_array =
        std::dynamic_pointer_cast<arrow::StructArray>(imported);
    if (!struct_array) {
        return Status::Invalid("PK mem indexer commit batch is not a StructArray");
    }
    std::shared_ptr<arrow::Array> value_kind =
        struct_array->GetFieldByName(SpecialFields::ValueKind().Name());
    std::shared_ptr<arrow::Int8Array> kind_array =
        std::dynamic_pointer_cast<arrow::Int8Array>(value_kind);
    if (!kind_array) {
        return Status::Invalid("PK mem indexer commit batch is missing _VALUE_KIND");
    }
    std::vector<RecordBatch::RowKind> row_kinds;
    row_kinds.reserve(static_cast<size_t>(kind_array->length()));
    for (int64_t index = 0; index < kind_array->length(); ++index) {
        if (kind_array->IsNull(index)) {
            return Status::Invalid("PK mem indexer commit reader returned a null _VALUE_KIND");
        }
        PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                               RowKind::FromByteValue(kind_array->Value(index)));
        row_kinds.push_back(static_cast<RecordBatch::RowKind>(row_kind->ToByteValue()));
    }
    PAIMON_ASSIGN_OR_RAISE(struct_array, ArrowUtils::RemoveFieldFromStructArray(
                                             struct_array, SpecialFields::ValueKind().Name()));
    auto output = std::make_unique<ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, output.get()));
    RecordBatchBuilder builder(output.get());
    builder.SetRowKinds(row_kinds);
    return builder.Finish();
}

Status RealtimePrimaryKeyWriter::FlushSegment(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                           mem_indexer_->CreateCommitReaders(segment));
    ScopeGuard reader_guard([&readers]() {
        for (const std::unique_ptr<BatchReader>& commit_reader : readers) {
            commit_reader->Close();
        }
    });
    for (const std::unique_ptr<BatchReader>& commit_reader : readers) {
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, commit_reader->NextBatch());
            if (BatchReader::IsEofBatch(batch)) {
                break;
            }
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> mutation_batch,
                                   ToMutationRecordBatch(std::move(batch)));
            if (mutation_batch->GetData()->length == 0) {
                continue;
            }
            PAIMON_RETURN_NOT_OK(merge_tree_writer_->Write(std::move(mutation_batch)));
        }
    }
    return Status::OK();
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

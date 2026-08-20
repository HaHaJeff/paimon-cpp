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
#include <optional>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/macros.h"
#include "paimon/realtime/realtime_context.h"

namespace paimon {

Result<std::shared_ptr<RealtimePrimaryKeyWriter>> RealtimePrimaryKeyWriter::Create(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<RealtimeContextImpl>& realtime_context,
    const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
    const std::shared_ptr<MemoryPool>& memory_pool, const RealtimeStoreState& store_state) {
    return std::shared_ptr<RealtimePrimaryKeyWriter>(
        new RealtimePrimaryKeyWriter(store_state.store, merge_tree_writer, realtime_context,
                                     RealtimePartitionBucket(partition, bucket), write_schema,
                                     store_state.initial_offset, memory_pool));
}

RealtimePrimaryKeyWriter::RealtimePrimaryKeyWriter(
    const std::shared_ptr<RealtimeStore>& realtime_store,
    const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
    const std::shared_ptr<RealtimeContextImpl>& realtime_context,
    const RealtimePartitionBucket& partition_bucket,
    const std::shared_ptr<arrow::Schema>& write_schema, int64_t next_offset,
    const std::shared_ptr<MemoryPool>& memory_pool)
    : memory_pool_(memory_pool),
      realtime_store_(realtime_store),
      merge_tree_writer_(merge_tree_writer),
      realtime_context_(realtime_context),
      partition_bucket_(partition_bucket),
      write_schema_(write_schema),
      next_offset_(next_offset) {}

Status RealtimePrimaryKeyWriter::Write(std::unique_ptr<RecordBatch>&& batch) {
    if (!batch || !batch->GetData()) {
        return Status::Invalid("PK real-time write batch is null");
    }
    const int64_t row_count = batch->GetData()->length;
    if (row_count == 0) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    if (row_count > std::numeric_limits<int64_t>::max() - next_offset_) {
        return Status::Invalid("real-time offset range exceeds INT64_MAX");
    }
    const OffsetRange range(next_offset_, next_offset_ + row_count);
    PAIMON_RETURN_NOT_OK(realtime_store_->Write(RealtimeWriteBatch{std::move(batch), range}));
    next_offset_ += row_count;
    return Status::OK();
}

Result<CommitIncrement> RealtimePrimaryKeyWriter::PrepareCommit(bool wait_compaction) {
    std::lock_guard<std::mutex> lock(prepare_mutex_);
    std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment;
    {
        std::lock_guard<std::mutex> realtime_store_lock(realtime_store_mutex_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<RealtimeSegmentHandle>> sealed_segment,
                               realtime_store_->SealForCommit());
        segment = std::move(sealed_segment);
    }
    if (segment) {
        PAIMON_RETURN_NOT_OK(FlushSegment(segment.value()));
    }
    PAIMON_ASSIGN_OR_RAISE(CommitIncrement increment,
                           merge_tree_writer_->PrepareCommit(wait_compaction));
    if (segment) {
        const std::vector<std::shared_ptr<DataFileMeta>>& new_files =
            increment.GetNewFilesIncrement().NewFiles();
        if (!new_files.empty()) {
            realtime_context_->AdvanceMaterializedMaxSequenceNumber(
                partition_bucket_, DataFileMeta::GetMaxSequenceNumber(new_files));
        }
        increment.SetRealtimeOffsetRange(segment.value()->GetOffsetRange());
    }
    return increment;
}

Status RealtimePrimaryKeyWriter::FlushSegment(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                           realtime_store_->CreateCommitReaders(segment));
    ScopeGuard readers_guard([&readers]() {
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            if (reader) {
                reader->Close();
            }
        }
    });
    for (const std::unique_ptr<BatchReader>& reader : readers) {
        if (!reader) {
            return Status::Invalid("PK real-time store returned a null commit reader");
        }
    }
    ConcatBatchReader reader(std::move(readers), memory_pool_);
    ScopeGuard reader_guard([&reader]() { reader.Close(); });
    const OffsetRange offset_range = segment->GetOffsetRange();
    int64_t emitted_rows = 0;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader.NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (!imported || imported->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("PK real-time store commit reader returned a non-StructArray");
        }
        std::shared_ptr<arrow::StructArray> struct_array =
            checked_pointer_cast<arrow::StructArray>(imported);
        std::shared_ptr<arrow::Array> value_kind =
            struct_array->GetFieldByName(SpecialFields::ValueKind().Name());
        if (!value_kind || value_kind->type_id() != arrow::Type::INT8) {
            return Status::Invalid(
                "PK real-time store commit reader must return an INT8 _VALUE_KIND field");
        }
        std::shared_ptr<arrow::Int8Array> encoded_row_kinds =
            checked_pointer_cast<arrow::Int8Array>(value_kind);
        std::vector<RecordBatch::RowKind> row_kinds;
        row_kinds.reserve(static_cast<size_t>(encoded_row_kinds->length()));
        for (int64_t i = 0; i < encoded_row_kinds->length(); ++i) {
            if (encoded_row_kinds->IsNull(i)) {
                return Status::Invalid("PK real-time store commit reader returned a null row kind");
            }
            PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                                   RowKind::FromByteValue(encoded_row_kinds->Value(i)));
            row_kinds.push_back(static_cast<RecordBatch::RowKind>(row_kind->ToByteValue()));
        }
        PAIMON_ASSIGN_OR_RAISE(struct_array, ArrowUtils::RemoveFieldFromStructArray(
                                                 struct_array, SpecialFields::ValueKind().Name()));
        if (!struct_array->type()->Equals(arrow::struct_(write_schema_->fields()))) {
            return Status::Invalid(
                "PK real-time store commit reader schema does not match table write schema");
        }
        const int64_t row_count = struct_array->length();
        if (row_count > offset_range.Count() - emitted_rows) {
            return Status::Invalid(
                "PK real-time store commit readers returned more rows than the sealed offset "
                "range");
        }
        emitted_rows += row_count;
        if (row_count == 0) {
            continue;
        }
        auto output = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, output.get()));
        RecordBatchBuilder builder(output.get());
        builder.SetRowKinds(row_kinds);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> record_batch, builder.Finish());
        PAIMON_RETURN_NOT_OK(merge_tree_writer_->Write(std::move(record_batch)));
    }
    if (emitted_rows != offset_range.Count()) {
        return Status::Invalid(
            "PK real-time store commit readers returned fewer rows than the sealed offset range");
    }
    return Status::OK();
}

Status RealtimePrimaryKeyWriter::Compact(bool) {
    return Status::Invalid("PK real-time write does not support explicit compaction");
}

uint64_t RealtimePrimaryKeyWriter::GetMemoryUsage() const {
    return realtime_store_->GetMemoryUsage();
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

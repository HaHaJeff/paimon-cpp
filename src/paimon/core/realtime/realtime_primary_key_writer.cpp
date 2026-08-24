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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/realtime/realtime_primary_key_writer.h"

#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/merged_key_value_record_reader.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/realtime/prepared_key_value_reader.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_fields.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/macros.h"

namespace paimon {

namespace {

struct PreparedArrayPrivateData {
    void (*release)(ArrowArray*);
    void* private_data;
    std::shared_ptr<arrow::MemoryPool> arrow_pool;
};

void ReleasePreparedArray(ArrowArray* array) {
    auto* data = static_cast<PreparedArrayPrivateData*>(array->private_data);
    array->release = data->release;
    array->private_data = data->private_data;
    array->release(array);
    delete data;
}

Status RetainPreparedArrayPool(ArrowArray* array,
                               const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (!array || !array->release || !arrow_pool) {
        return Status::Invalid("cannot retain prepared batch memory pool");
    }
    array->private_data =
        new PreparedArrayPrivateData{array->release, array->private_data, arrow_pool};
    array->release = ReleasePreparedArray;
    return Status::OK();
}

Result<std::shared_ptr<arrow::StructArray>> PrepareBatch(
    std::unique_ptr<RecordBatch>&& batch, const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::vector<std::string>& trimmed_primary_keys, int64_t first_sequence_number,
    int64_t first_offset, arrow::MemoryPool* arrow_pool) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> input,
        arrow::ImportArray(batch->GetData(), arrow::struct_(write_schema->fields())));
    if (!input || input->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("PK real-time write data is not a StructArray");
    }
    std::shared_ptr<arrow::StructArray> values = checked_pointer_cast<arrow::StructArray>(input);
    const int64_t count = values->length();
    arrow::Int8Builder kinds(arrow_pool);
    arrow::Int64Builder sequences(arrow_pool);
    arrow::Int64Builder offsets(arrow_pool);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(kinds.Reserve(count));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(sequences.Reserve(count));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets.Reserve(count));
    const std::vector<RecordBatch::RowKind>& row_kinds = batch->GetRowKind();
    for (int64_t row = 0; row < count; ++row) {
        const RecordBatch::RowKind kind =
            row_kinds.empty() ? RecordBatch::RowKind::INSERT : row_kinds[row];
        kinds.UnsafeAppend(static_cast<int8_t>(kind));
        sequences.UnsafeAppend(first_sequence_number + row);
        offsets.UnsafeAppend(first_offset + row);
    }
    std::shared_ptr<arrow::Array> kind_array;
    std::shared_ptr<arrow::Array> sequence_array;
    std::shared_ptr<arrow::Array> offset_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(kinds.Finish(&kind_array));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(sequences.Finish(&sequence_array));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets.Finish(&offset_array));
    arrow::ArrayVector columns = {std::move(kind_array), std::move(sequence_array),
                                  std::move(offset_array)};
    columns.insert(columns.end(), values->fields().begin(), values->fields().end());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> prepared,
        arrow::StructArray::Make(std::move(columns), prepared_schema->fields()));

    std::vector<arrow::compute::SortKey> sort_keys;
    sort_keys.reserve(trimmed_primary_keys.size() + 1);
    for (const std::string& key : trimmed_primary_keys) {
        sort_keys.emplace_back(key, arrow::compute::SortOrder::Ascending);
    }
    sort_keys.emplace_back(SpecialFields::SequenceNumber().Name(),
                           arrow::compute::SortOrder::Ascending);
    arrow::compute::ExecContext context(arrow_pool);
    arrow::compute::SortOptions options(sort_keys, arrow::compute::NullPlacement::AtStart);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow::Datum indices,
        arrow::compute::SortIndices(arrow::Datum(prepared), options, &context));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow::Datum sorted,
        arrow::compute::Take(arrow::Datum(prepared), indices,
                             arrow::compute::TakeOptions::NoBoundsCheck(), &context));
    std::shared_ptr<arrow::Array> sorted_array = sorted.make_array();
    if (!sorted_array || sorted_array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("PK real-time sorted batch is not a StructArray");
    }
    return checked_pointer_cast<arrow::StructArray>(std::move(sorted_array));
}

}  // namespace

Result<std::shared_ptr<RealtimePrimaryKeyWriter>> RealtimePrimaryKeyWriter::Create(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::vector<std::string>& trimmed_primary_keys,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<RealtimeContextImpl>& realtime_context,
    const RealtimeStoreState& store_state, int64_t restored_max_sequence_number,
    const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!store_state.store || !merge_tree_writer || !write_schema || !key_comparator ||
        !realtime_context || !memory_pool) {
        return Status::Invalid("PK real-time writer received a null dependency");
    }
    if (trimmed_primary_keys.empty()) {
        return Status::Invalid("PK real-time writer requires at least one primary key");
    }
    if (restored_max_sequence_number < -1 ||
        restored_max_sequence_number == std::numeric_limits<int64_t>::max()) {
        return Status::Invalid("PK restored sequence number is invalid");
    }
    arrow::FieldVector key_fields;
    key_fields.reserve(trimmed_primary_keys.size());
    for (const std::string& key : trimmed_primary_keys) {
        std::shared_ptr<arrow::Field> field = write_schema->GetFieldByName(key);
        if (!field) {
            return Status::Invalid("PK field is missing from write schema: ", key);
        }
        key_fields.push_back(std::move(field));
    }
    arrow::FieldVector prepared_fields = {
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
            ->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(RealtimeOffsetField())->WithNullable(false)};
    prepared_fields.insert(prepared_fields.end(), write_schema->fields().begin(),
                           write_schema->fields().end());
    const RealtimePartitionBucket partition_bucket(partition, bucket);
    const int64_t initial_max_sequence_number =
        realtime_context->AdvanceMaterializedMaxSequenceNumber(partition_bucket,
                                                               restored_max_sequence_number);
    return std::shared_ptr<RealtimePrimaryKeyWriter>(new RealtimePrimaryKeyWriter(
        store_state.store, merge_tree_writer, realtime_context, partition_bucket, write_schema,
        arrow::schema(std::move(prepared_fields)), arrow::schema(std::move(key_fields)),
        trimmed_primary_keys, key_comparator, store_state.initial_offset,
        initial_max_sequence_number, memory_pool));
}

RealtimePrimaryKeyWriter::RealtimePrimaryKeyWriter(
    const std::shared_ptr<RealtimeStore>& realtime_store,
    const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
    const std::shared_ptr<RealtimeContextImpl>& realtime_context,
    const RealtimePartitionBucket& partition_bucket,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::vector<std::string>& trimmed_primary_keys,
    const std::shared_ptr<FieldsComparator>& key_comparator, int64_t next_offset,
    int64_t last_sequence_number, const std::shared_ptr<MemoryPool>& memory_pool)
    : memory_pool_(memory_pool),
      arrow_pool_(GetArrowPool(memory_pool)),
      realtime_store_(realtime_store),
      merge_tree_writer_(merge_tree_writer),
      realtime_context_(realtime_context),
      partition_bucket_(partition_bucket),
      write_schema_(write_schema),
      prepared_schema_(prepared_schema),
      key_schema_(key_schema),
      trimmed_primary_keys_(trimmed_primary_keys),
      key_comparator_(key_comparator),
      next_offset_(next_offset),
      last_sequence_number_(last_sequence_number) {}

Status RealtimePrimaryKeyWriter::Write(std::unique_ptr<RecordBatch>&& batch) {
    if (!batch || !batch->GetData()) {
        return Status::Invalid("PK real-time write batch is null");
    }
    const int64_t count = batch->GetData()->length;
    if (count == 0) {
        return Status::OK();
    }
    const std::vector<RecordBatch::RowKind>& row_kinds = batch->GetRowKind();
    if (!row_kinds.empty() && static_cast<int64_t>(row_kinds.size()) != count) {
        return Status::Invalid("PK real-time row-kind count does not match batch row count");
    }
    for (RecordBatch::RowKind row_kind : row_kinds) {
        PAIMON_ASSIGN_OR_RAISE(const RowKind* validated,
                               RowKind::FromByteValue(static_cast<int8_t>(row_kind)));
        static_cast<void>(validated);
    }
    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    if (count > std::numeric_limits<int64_t>::max() - next_offset_) {
        return Status::Invalid("real-time offset range exceeds INT64_MAX");
    }
    // Reserve INT64_MAX as the exhausted sequence-number sentinel.
    if (last_sequence_number_ >= std::numeric_limits<int64_t>::max() - count) {
        return Status::Invalid("PK sequence range exceeds INT64_MAX");
    }
    const int64_t first_sequence = last_sequence_number_ + 1;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::StructArray> prepared,
        PrepareBatch(std::move(batch), write_schema_, prepared_schema_, trimmed_primary_keys_,
                     first_sequence, next_offset_, arrow_pool_.get()));
    auto output = std::make_unique<ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*prepared, output.get()));
    PAIMON_RETURN_NOT_OK(RetainPreparedArrayPool(output.get(), arrow_pool_));
    RecordBatchBuilder builder(output.get());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> prepared_batch, builder.Finish());
    PAIMON_RETURN_NOT_OK(realtime_store_->Write(RealtimeWriteBatch{
        std::move(prepared_batch), OffsetRange(next_offset_, next_offset_ + count)}));
    next_offset_ += count;
    last_sequence_number_ += count;
    realtime_context_->AdvanceMaterializedMaxSequenceNumber(partition_bucket_,
                                                            last_sequence_number_);
    return Status::OK();
}

Result<CommitIncrement> RealtimePrimaryKeyWriter::PrepareCommit(bool wait_compaction) {
    std::lock_guard<std::mutex> prepare_lock(prepare_mutex_);
    std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment;
    {
        std::lock_guard<std::mutex> store_lock(realtime_store_mutex_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<RealtimeSegmentHandle>> sealed,
                               realtime_store_->SealForCommit());
        segment = std::move(sealed);
    }
    if (segment && !segment.value()) {
        return Status::Invalid("PK real-time store sealed a null segment");
    }
    std::optional<OffsetRange> sealed_range;
    int64_t expected_raw_row_count = 0;
    if (segment) {
        sealed_range = segment.value()->GetOffsetRange();
        if (sealed_range->begin < 0 || sealed_range->end < sealed_range->begin ||
            __builtin_sub_overflow(sealed_range->end, sealed_range->begin,
                                   &expected_raw_row_count)) {
            return Status::Invalid("PK real-time store returned an invalid sealed offset range");
        }
        PAIMON_RETURN_NOT_OK(FlushSegment(segment.value(), expected_raw_row_count));
    }
    PAIMON_ASSIGN_OR_RAISE(CommitIncrement increment,
                           merge_tree_writer_->PrepareCommit(wait_compaction));
    if (segment) {
        increment.SetRealtimeOffsetRange(sealed_range.value());
    }
    return increment;
}

Status RealtimePrimaryKeyWriter::FlushSegment(const std::shared_ptr<RealtimeSegmentHandle>& segment,
                                              int64_t expected_raw_row_count) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                           realtime_store_->CreateCommitReaders(segment));
    ScopeGuard readers_guard([&readers]() {
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            if (reader) {
                reader->Close();
            }
        }
    });
    int64_t raw_row_count = 0;
    std::vector<std::unique_ptr<KeyValueRecordReader>> sorted_readers;
    sorted_readers.reserve(readers.size());
    for (std::unique_ptr<BatchReader>& reader : readers) {
        if (!reader) {
            return Status::Invalid("PK real-time store returned a null commit reader");
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<KeyValueRecordReader> prepared_reader,
            AdaptPreparedBatchReader(std::move(reader), prepared_schema_, std::nullopt, key_schema_,
                                     write_schema_, memory_pool_, &raw_row_count));
        auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        sorted_readers.push_back(std::make_unique<MergedKeyValueRecordReader>(
            std::move(prepared_reader), key_comparator_,
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function))));
    }
    readers_guard.Release();
    PAIMON_RETURN_NOT_OK(merge_tree_writer_->WriteSortedReaders(std::move(sorted_readers)));
    if (raw_row_count != expected_raw_row_count) {
        return Status::Invalid("PK real-time store commit readers did not cover the sealed range");
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

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

#include "paimon/core/realtime/primary_key_mem_indexer.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/io/key_value_projection_consumer.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/spill_channel_manager.h"
#include "paimon/core/mergetree/spill_reader.h"
#include "paimon/core/mergetree/spill_writer.h"
#include "paimon/core/mergetree/write_buffer.h"
#include "paimon/macros.h"

namespace paimon {
namespace {

class Segment;

class CommitSpillFile {
 public:
    CommitSpillFile(const FileIOChannel::ID& channel_id,
                    const std::shared_ptr<SpillChannelManager>& manager)
        : channel_id_(channel_id), manager_(manager) {}

    ~CommitSpillFile() {
        [[maybe_unused]] Status status = manager_->DeleteChannel(channel_id_);
    }

    FileIOChannel::ID channel_id_;
    std::shared_ptr<SpillChannelManager> manager_;
};

class SegmentBatchReader : public BatchReader {
 public:
    static Result<std::unique_ptr<SegmentBatchReader>> Create(
        const std::shared_ptr<Segment>& segment,
        std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
        const std::shared_ptr<arrow::Schema>& output_schema, const std::vector<int32_t>& projection,
        int32_t batch_size, const std::shared_ptr<MemoryPool>& pool);

    Result<ReadBatch> NextBatch() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override;

 private:
    SegmentBatchReader(const std::shared_ptr<Segment>& segment,
                       std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
                       std::unique_ptr<RowToArrowArrayConverter<KeyValue, ReadBatch>>&& converter,
                       int32_t batch_size)
        : segment_(segment),
          readers_(std::move(readers)),
          converter_(std::move(converter)),
          batch_size_(batch_size),
          metrics_(std::make_shared<MetricsImpl>()) {}

    std::shared_ptr<Segment> segment_;
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers_;
    std::unique_ptr<KeyValueRecordReader::Iterator> iterator_;
    std::unique_ptr<RowToArrowArrayConverter<KeyValue, ReadBatch>> converter_;
    size_t next_reader_ = 0;
    KeyValueRecordReader* current_reader_ = nullptr;
    int32_t batch_size_;
    std::shared_ptr<Metrics> metrics_;
};

class Segment {
 public:
    Segment(const Range& offset_range, const Range& sequence_range,
            std::unique_ptr<WriteBuffer>&& buffer,
            const std::shared_ptr<CommitSpillFile>& commit_file)
        : offset_range_(offset_range),
          sequence_range_(sequence_range),
          buffer_(std::move(buffer)),
          commit_file_(commit_file) {}

    Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateReaders() {
        std::lock_guard<std::mutex> lock(mutex_);
        return buffer_->CreateReaders();
    }

    const std::shared_ptr<CommitSpillFile>& GetCommitFile() const {
        return commit_file_;
    }

    uint64_t GetMemoryUsage() const {
        return buffer_->GetMemoryUsage();
    }

    Range offset_range_;
    Range sequence_range_;
    bool prepared_ = false;

 private:
    std::unique_ptr<WriteBuffer> buffer_;
    std::shared_ptr<CommitSpillFile> commit_file_;
    std::mutex mutex_;
};

Result<std::unique_ptr<SegmentBatchReader>> SegmentBatchReader::Create(
    const std::shared_ptr<Segment>& segment,
    std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
    const std::shared_ptr<arrow::Schema>& output_schema, const std::vector<int32_t>& projection,
    int32_t batch_size, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<KeyValueProjectionConsumer> converter,
                           KeyValueProjectionConsumer::Create(output_schema, projection, pool));
    return std::unique_ptr<SegmentBatchReader>(
        new SegmentBatchReader(segment, std::move(readers), std::move(converter), batch_size));
}

Result<BatchReader::ReadBatch> SegmentBatchReader::NextBatch() {
    std::vector<KeyValue> values;
    values.reserve(static_cast<size_t>(batch_size_));
    while (static_cast<int32_t>(values.size()) < batch_size_) {
        if (!iterator_) {
            while (current_reader_ || next_reader_ < readers_.size()) {
                if (!current_reader_) {
                    current_reader_ = readers_[next_reader_++].get();
                }
                PAIMON_ASSIGN_OR_RAISE(iterator_, current_reader_->NextBatch());
                if (iterator_) {
                    break;
                }
                current_reader_ = nullptr;
            }
            if (!iterator_) {
                break;
            }
        }
        PAIMON_ASSIGN_OR_RAISE(bool has_next, iterator_->HasNext());
        if (!has_next) {
            iterator_.reset();
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(KeyValue value, iterator_->Next());
        values.push_back(std::move(value));
    }
    if (values.empty()) {
        return MakeEofBatch();
    }
    return converter_->NextBatch(values);
}

void SegmentBatchReader::Close() {
    iterator_.reset();
    current_reader_ = nullptr;
    for (const std::unique_ptr<KeyValueRecordReader>& reader : readers_) {
        reader->Close();
    }
    readers_.clear();
    converter_->CleanUp();
    segment_.reset();
}

class SegmentHandle : public RealtimeSegmentHandle {
 public:
    explicit SegmentHandle(std::vector<std::shared_ptr<Segment>>&& segments)
        : segments_(std::move(segments)),
          offset_range_(segments_.front()->offset_range_.from, segments_.back()->offset_range_.to) {
    }

    Range GetOffsetRange() const override {
        return offset_range_;
    }

    std::vector<std::shared_ptr<Segment>> segments_;

 private:
    Range offset_range_;
};

class ReadView : public MemReadView {
 public:
    explicit ReadView(std::vector<std::shared_ptr<Segment>> segments)
        : segments_(std::move(segments)) {
        if (!segments_.empty()) {
            offset_range_ =
                Range(segments_.front()->offset_range_.from, segments_.back()->offset_range_.to);
        }
    }

    std::optional<Range> GetOffsetRange() const override {
        return offset_range_;
    }

    std::vector<std::shared_ptr<Segment>> segments_;

 private:
    std::optional<Range> offset_range_;
};

}  // namespace

class PrimaryKeyMemIndexer::Impl {
 public:
    Impl(const std::shared_ptr<arrow::Schema>& write_schema,
         std::vector<std::string> trimmed_primary_keys,
         const std::shared_ptr<FieldsComparator>& key_comparator,
         const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
         int64_t next_sequence_number, const CoreOptions& options,
         const std::shared_ptr<IOManager>& io_manager, bool enable_multi_thread_spill,
         const std::shared_ptr<MemoryPool>& memory_pool)
        : write_schema_(write_schema),
          trimmed_primary_keys_(std::move(trimmed_primary_keys)),
          key_comparator_(key_comparator),
          merge_function_wrapper_(merge_function_wrapper),
          options_(options),
          io_manager_(io_manager),
          enable_multi_thread_spill_(enable_multi_thread_spill),
          memory_pool_(memory_pool),
          next_sequence_number_(next_sequence_number) {
        arrow::FieldVector key_fields;
        key_fields.reserve(trimmed_primary_keys_.size());
        for (const std::string& key : trimmed_primary_keys_) {
            key_fields.push_back(write_schema_->GetFieldByName(key));
        }
        key_schema_ = arrow::schema(std::move(key_fields));
    }

    Result<std::unique_ptr<WriteBuffer>> CreateBuffer() const {
        return WriteBuffer::Create(next_sequence_number_ - 1, write_schema_, trimmed_primary_keys_,
                                   /*user_defined_sequence_fields=*/{}, key_comparator_,
                                   /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_,
                                   options_, io_manager_, enable_multi_thread_spill_, memory_pool_);
    }

    Status EnsureBuilding() {
        if (!building_) {
            PAIMON_ASSIGN_OR_RAISE(building_, CreateBuffer());
        }
        return Status::OK();
    }

    Status RotateBuilding() {
        if (!building_ || building_->IsEmpty()) {
            return Status::OK();
        }
        PAIMON_RETURN_NOT_OK(commit_writer_->Close());
        auto commit_file = std::make_shared<CommitSpillFile>(commit_writer_->GetChannelId(),
                                                             commit_channel_manager_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteBuffer> replacement, CreateBuffer());
        segments_.push_back(std::make_shared<Segment>(building_offset_range_.value(),
                                                      building_sequence_range_.value(),
                                                      std::move(building_), commit_file));
        building_ = std::move(replacement);
        commit_writer_.reset();
        building_offset_range_.reset();
        building_sequence_range_.reset();
        return Status::OK();
    }

    Status Write(RealtimeWriteBatch&& write_batch) {
        if (!write_batch.batch) {
            return Status::Invalid("PK real-time write batch is null");
        }
        const int64_t row_count = write_batch.batch->GetData()->length;
        if (row_count <= 0 || write_batch.offset_range.from < 0 ||
            write_batch.offset_range.to < write_batch.offset_range.from ||
            write_batch.offset_range.to - write_batch.offset_range.from != row_count - 1) {
            return Status::Invalid("PK real-time offset range does not match batch row count");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (last_offset_ && write_batch.offset_range.from != last_offset_.value() + 1) {
            return Status::Invalid("PK real-time offset ranges must be contiguous");
        }
        if (row_count > std::numeric_limits<int64_t>::max() - next_sequence_number_) {
            return Status::Invalid("PK sequence range exceeds INT64_MAX");
        }
        PAIMON_RETURN_NOT_OK(EnsureBuilding());
        const Range sequence_range(next_sequence_number_, next_sequence_number_ + row_count - 1);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> imported,
            arrow::ImportArray(write_batch.batch->GetData(),
                               arrow::struct_(write_schema_->fields())));
        std::shared_ptr<arrow::StructArray> values =
            std::dynamic_pointer_cast<arrow::StructArray>(imported);
        if (!values) {
            return Status::Invalid("PK real-time write data is not a StructArray");
        }
        std::vector<RecordBatch::RowKind> row_kinds = write_batch.batch->GetRowKind();
        if (!row_kinds.empty() && static_cast<int64_t>(row_kinds.size()) != row_count) {
            return Status::Invalid("PK real-time row-kind count does not match row count");
        }
        if (!commit_writer_) {
            PAIMON_RETURN_NOT_OK(CreateCommitWriter());
        }
        arrow::Int64Builder sequence_builder;
        arrow::Int8Builder kind_builder;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(sequence_builder.Reserve(row_count));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(kind_builder.Reserve(row_count));
        for (int64_t i = 0; i < row_count; ++i) {
            sequence_builder.UnsafeAppend(sequence_range.from + i);
            kind_builder.UnsafeAppend(row_kinds.empty()
                                          ? static_cast<int8_t>(RecordBatch::RowKind::INSERT)
                                          : static_cast<int8_t>(row_kinds[static_cast<size_t>(i)]));
        }
        std::shared_ptr<arrow::Array> sequences;
        std::shared_ptr<arrow::Array> kinds;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(sequence_builder.Finish(&sequences));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(kind_builder.Finish(&kinds));
        arrow::ArrayVector commit_columns = {sequences, kinds};
        commit_columns.insert(commit_columns.end(), values->fields().begin(),
                              values->fields().end());
        PAIMON_RETURN_NOT_OK(commit_writer_->WriteBatch(
            arrow::RecordBatch::Make(commit_schema_, row_count, commit_columns)));
        auto c_array = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*values, c_array.get()));
        RecordBatchBuilder batch_builder(c_array.get());
        batch_builder.SetRowKinds(row_kinds);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> buffer_batch, batch_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(bool can_accept_more, building_->Write(std::move(buffer_batch)));
        if (!building_offset_range_) {
            building_offset_range_ = write_batch.offset_range;
            building_sequence_range_ = sequence_range;
        } else {
            building_offset_range_ =
                Range(building_offset_range_->from, write_batch.offset_range.to);
            building_sequence_range_ = Range(building_sequence_range_->from, sequence_range.to);
        }
        last_offset_ = write_batch.offset_range.to;
        next_sequence_number_ += row_count;
        if (!can_accept_more) {
            PAIMON_RETURN_NOT_OK(RotateBuilding());
        }
        return Status::OK();
    }

    Status CreateCommitWriter() {
        if (!commit_channel_manager_) {
            commit_channel_manager_ = std::make_shared<SpillChannelManager>(
                options_.GetFileSystem(), options_.GetLocalSortMaxNumFileHandles());
            PAIMON_ASSIGN_OR_RAISE(commit_channel_enumerator_,
                                   io_manager_->CreateChannelEnumerator());
        }
        const CompressOptions& compression = options_.GetSpillCompressOptions();
        PAIMON_ASSIGN_OR_RAISE(
            commit_writer_, SpillWriter::Create(options_.GetFileSystem(), commit_schema_,
                                                commit_channel_enumerator_, commit_channel_manager_,
                                                compression.compress, compression.zstd_level,
                                                enable_multi_thread_spill_, memory_pool_));
        return Status::OK();
    }

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() {
        std::lock_guard<std::mutex> lock(mutex_);
        PAIMON_RETURN_NOT_OK(RotateBuilding());
        std::vector<std::shared_ptr<Segment>> selected;
        for (const std::shared_ptr<Segment>& segment : segments_) {
            if (!segment->prepared_) {
                segment->prepared_ = true;
                selected.push_back(segment);
            }
        }
        if (selected.empty()) {
            return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
        }
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(
            std::make_shared<SegmentHandle>(std::move(selected)));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& handle) {
        std::shared_ptr<SegmentHandle> typed = std::dynamic_pointer_cast<SegmentHandle>(handle);
        if (!typed) {
            return Status::Invalid("segment was not created by the PK mem indexer");
        }
        arrow::FieldVector fields = {
            DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())};
        fields.insert(fields.end(), write_schema_->fields().begin(), write_schema_->fields().end());
        std::vector<int32_t> projection = {KeyValueProjectionConsumer::kValueKindProjection};
        for (int32_t i = 0; i < write_schema_->num_fields(); ++i) {
            projection.push_back(i);
        }
        return CreateReaders(typed->segments_, arrow::schema(fields), projection);
    }

    Result<std::shared_ptr<MemReadView>> AcquireReadView() {
        std::lock_guard<std::mutex> lock(mutex_);
        PAIMON_RETURN_NOT_OK(RotateBuilding());
        return std::shared_ptr<MemReadView>(new ReadView(segments_));
    }

    Result<std::vector<std::shared_ptr<Segment>>> SelectSegments(
        const std::shared_ptr<MemReadView>& view, int64_t lower) const {
        std::shared_ptr<ReadView> typed = std::dynamic_pointer_cast<ReadView>(view);
        if (!typed) {
            return Status::Invalid("read view was not created by the PK mem indexer");
        }
        std::vector<std::shared_ptr<Segment>> selected;
        for (const std::shared_ptr<Segment>& segment : typed->segments_) {
            if (segment->offset_range_.to <= lower) {
                continue;
            }
            if (segment->offset_range_.from <= lower) {
                return Status::Invalid("committed offset splits a PK memory segment");
            }
            selected.push_back(segment);
        }
        return selected;
    }

    Result<std::optional<Range>> GetPrimaryKeyQuerySequenceRange(
        const std::shared_ptr<MemReadView>& view, int64_t lower) const {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Segment>> selected,
                               SelectSegments(view, lower));
        if (selected.empty()) {
            return std::optional<Range>();
        }
        return std::optional<Range>(
            Range(selected.front()->sequence_range_.from, selected.back()->sequence_range_.to));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreatePrimaryKeyQueryReaders(
        const std::shared_ptr<MemReadView>& view, int64_t lower, const MemQueryContext& context) {
        if (!context.read_schema || !context.read_schema->release) {
            return Status::Invalid("PK mem query read schema is null");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> requested,
                                          arrow::ImportSchema(context.read_schema));
        arrow::FieldVector value_fields;
        std::vector<int32_t> projection = {KeyValueProjectionConsumer::kSequenceNumberProjection,
                                           KeyValueProjectionConsumer::kValueKindProjection};
        for (const std::shared_ptr<arrow::Field>& field : requested->fields()) {
            if (field->name() == SpecialFields::SequenceNumber().Name() ||
                field->name() == SpecialFields::ValueKind().Name()) {
                continue;
            }
            int32_t index = write_schema_->GetFieldIndex(field->name());
            if (index < 0) {
                return Status::Invalid("PK mem query field is missing from write schema: ",
                                       field->name());
            }
            value_fields.push_back(field);
            projection.push_back(index);
        }
        arrow::FieldVector output_fields = {
            DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()),
            DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())};
        output_fields.insert(output_fields.end(), value_fields.begin(), value_fields.end());
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Segment>> selected,
                               SelectSegments(view, lower));
        return CreateReaders(selected, arrow::schema(output_fields), projection);
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateReaders(
        const std::vector<std::shared_ptr<Segment>>& segments,
        const std::shared_ptr<arrow::Schema>& schema, const std::vector<int32_t>& projection) {
        std::vector<std::unique_ptr<BatchReader>> result;
        result.reserve(segments.size());
        for (const std::shared_ptr<Segment>& segment : segments) {
            std::vector<std::unique_ptr<KeyValueRecordReader>> readers;
            if (projection.front() == KeyValueProjectionConsumer::kValueKindProjection) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::unique_ptr<SpillReader> commit_reader,
                    SpillReader::Create(options_.GetFileSystem(), key_schema_, write_schema_,
                                        enable_multi_thread_spill_,
                                        segment->GetCommitFile()->channel_id_, memory_pool_));
                readers.push_back(std::move(commit_reader));
            } else {
                PAIMON_ASSIGN_OR_RAISE(readers, segment->CreateReaders());
            }
            for (std::unique_ptr<KeyValueRecordReader>& key_value_reader : readers) {
                std::vector<std::unique_ptr<KeyValueRecordReader>> single_reader;
                single_reader.push_back(std::move(key_value_reader));
                PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SegmentBatchReader> reader,
                                       SegmentBatchReader::Create(
                                           segment, std::move(single_reader), schema, projection,
                                           options_.GetReadBatchSize(), memory_pool_));
                result.push_back(std::move(reader));
            }
        }
        return result;
    }

    Status AdvanceCommittedOffset(int64_t committed_offset) {
        std::lock_guard<std::mutex> lock(mutex_);
        segments_.erase(std::remove_if(segments_.begin(), segments_.end(),
                                       [committed_offset](const std::shared_ptr<Segment>& segment) {
                                           return segment->offset_range_.to <= committed_offset;
                                       }),
                        segments_.end());
        return Status::OK();
    }

    uint64_t GetMemoryUsage() const {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t result = building_ ? building_->GetMemoryUsage() : 0;
        for (const std::shared_ptr<Segment>& segment : segments_) {
            result += segment->GetMemoryUsage();
        }
        return result;
    }

    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<arrow::Schema> key_schema_ = arrow::schema({});
    std::shared_ptr<arrow::Schema> commit_schema_ =
        SpecialFields::CompleteSequenceAndValueKindField(write_schema_);
    std::vector<std::string> trimmed_primary_keys_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
    CoreOptions options_;
    std::shared_ptr<IOManager> io_manager_;
    bool enable_multi_thread_spill_;
    std::shared_ptr<MemoryPool> memory_pool_;
    mutable std::mutex mutex_;
    std::unique_ptr<WriteBuffer> building_;
    std::shared_ptr<SpillChannelManager> commit_channel_manager_;
    std::shared_ptr<FileIOChannel::Enumerator> commit_channel_enumerator_;
    std::unique_ptr<SpillWriter> commit_writer_;
    std::optional<Range> building_offset_range_;
    std::optional<Range> building_sequence_range_;
    std::vector<std::shared_ptr<Segment>> segments_;
    std::optional<int64_t> last_offset_;
    int64_t next_sequence_number_;
};

Result<std::shared_ptr<PrimaryKeyMemIndexer>> PrimaryKeyMemIndexer::Create(
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::vector<std::string>& trimmed_primary_keys,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
    int64_t restore_max_seq_number, const CoreOptions& options,
    const std::shared_ptr<IOManager>& io_manager, bool enable_multi_thread_spill,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (restore_max_seq_number == std::numeric_limits<int64_t>::max()) {
        return Status::Invalid("PK sequence number has reached INT64_MAX");
    }
    if (trimmed_primary_keys.empty() || !key_comparator || !merge_function_wrapper) {
        return Status::Invalid("PK mem indexer requires primary keys and merge helpers");
    }
    if (!options.GetWriteBufferSpillable() || !io_manager) {
        return Status::Invalid("PK real-time memory indexer requires spill and a temp directory");
    }
    for (const std::string& key : trimmed_primary_keys) {
        if (write_schema->GetFieldIndex(key) < 0) {
            return Status::Invalid("primary key ", key, " is missing from write schema");
        }
    }
    auto impl = std::make_unique<Impl>(write_schema, trimmed_primary_keys, key_comparator,
                                       merge_function_wrapper, restore_max_seq_number + 1, options,
                                       io_manager, enable_multi_thread_spill, memory_pool);
    return std::shared_ptr<PrimaryKeyMemIndexer>(new PrimaryKeyMemIndexer(std::move(impl)));
}

PrimaryKeyMemIndexer::PrimaryKeyMemIndexer(std::unique_ptr<Impl>&& impl) : impl_(std::move(impl)) {}

PrimaryKeyMemIndexer::~PrimaryKeyMemIndexer() = default;

Status PrimaryKeyMemIndexer::Write(RealtimeWriteBatch&& batch) {
    return impl_->Write(std::move(batch));
}

Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>>
PrimaryKeyMemIndexer::SealForCommit() {
    return impl_->SealForCommit();
}

Result<std::vector<std::unique_ptr<BatchReader>>> PrimaryKeyMemIndexer::CreateCommitReaders(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    return impl_->CreateCommitReaders(segment);
}

Result<std::shared_ptr<MemReadView>> PrimaryKeyMemIndexer::AcquireReadView() {
    return impl_->AcquireReadView();
}

Result<std::vector<std::unique_ptr<BatchReader>>> PrimaryKeyMemIndexer::CreateQueryReaders(
    const std::shared_ptr<MemReadView>&, int64_t, const MemQueryContext&) {
    return Status::Invalid("PK mem indexer queries require primary-key merge readers");
}

Result<std::optional<Range>> PrimaryKeyMemIndexer::GetPrimaryKeyQuerySequenceRange(
    const std::shared_ptr<MemReadView>& view, int64_t lower) {
    return impl_->GetPrimaryKeyQuerySequenceRange(view, lower);
}

Result<std::vector<std::unique_ptr<BatchReader>>>
PrimaryKeyMemIndexer::CreatePrimaryKeyQueryReaders(const std::shared_ptr<MemReadView>& view,
                                                   int64_t lower, const MemQueryContext& context) {
    return impl_->CreatePrimaryKeyQueryReaders(view, lower, context);
}

Status PrimaryKeyMemIndexer::AdvanceCommittedOffset(int64_t committed_offset) {
    return impl_->AdvanceCommittedOffset(committed_offset);
}

uint64_t PrimaryKeyMemIndexer::GetMemoryUsage() const {
    return impl_->GetMemoryUsage();
}

}  // namespace paimon

/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/realtime/arrow_mem_indexer.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/macros.h"

namespace paimon {
namespace {

uint64_t GetArrayMemoryUsage(const std::shared_ptr<arrow::ArrayData>& data) {
    uint64_t result = 0;
    for (const std::shared_ptr<arrow::Buffer>& buffer : data->buffers) {
        if (buffer) {
            result += static_cast<uint64_t>(buffer->size());
        }
    }
    for (const std::shared_ptr<arrow::ArrayData>& child : data->child_data) {
        result += GetArrayMemoryUsage(child);
    }
    if (data->dictionary) {
        result += GetArrayMemoryUsage(data->dictionary);
    }
    return result;
}

}  // namespace

class ArrowMemIndexer::Segment : public RealtimeSegmentHandle {
 public:
    Segment(const Range& offset_range, std::vector<StoredBatch>&& batches)
        : offset_range_(offset_range), batches_(std::move(batches)) {}

    Range GetOffsetRange() const override {
        return offset_range_;
    }

    const std::vector<StoredBatch>& GetBatches() const {
        return batches_;
    }

 private:
    Range offset_range_;
    std::vector<StoredBatch> batches_;
};

class ArrowMemIndexer::CommitBatchReader : public BatchReader {
 public:
    CommitBatchReader(const std::shared_ptr<Segment>& segment,
                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : segment_(segment), arrow_pool_(arrow_pool), metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (!segment_ || next_batch_ >= segment_->GetBatches().size()) {
            return MakeEofBatch();
        }
        const StoredBatch& stored = segment_->GetBatches()[next_batch_++];
        int64_t row_count = stored.data->length();

        arrow::Int8Builder row_kind_builder(arrow_pool_.get());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(row_kind_builder.Reserve(row_count));
        if (stored.row_kinds.empty()) {
            for (int64_t i = 0; i < row_count; ++i) {
                row_kind_builder.UnsafeAppend(static_cast<int8_t>(RecordBatch::RowKind::INSERT));
            }
        } else {
            for (RecordBatch::RowKind row_kind : stored.row_kinds) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    row_kind_builder.Append(static_cast<int8_t>(row_kind)));
            }
        }
        std::shared_ptr<arrow::Array> row_kind_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(row_kind_builder.Finish(&row_kind_array));

        arrow::ArrayVector fields = {row_kind_array};
        fields.insert(fields.end(), stored.data->fields().begin(), stored.data->fields().end());
        arrow::FieldVector schema_fields = {
            DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())};
        const arrow::FieldVector& data_fields = stored.data->struct_type()->fields();
        schema_fields.insert(schema_fields.end(), data_fields.begin(), data_fields.end());

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> result,
            arrow::StructArray::Make(fields, schema_fields, stored.data->null_bitmap(),
                                     stored.data->null_count(), stored.data->offset()));
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*result, c_array.get(), c_schema.get()));
        return ReadBatch(std::move(c_array), std::move(c_schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        segment_.reset();
    }

 private:
    std::shared_ptr<Segment> segment_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;
    size_t next_batch_ = 0;
};

ArrowMemIndexer::ArrowMemIndexer(const std::shared_ptr<arrow::Schema>& write_schema,
                                 const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : write_schema_(write_schema), arrow_pool_(arrow_pool) {}

Status ArrowMemIndexer::Write(RealtimeWriteBatch&& write_batch) {
    if (!write_batch.batch) {
        return Status::Invalid("real-time write batch is null");
    }
    int64_t row_count = write_batch.batch->GetData()->length;
    if (write_batch.offset_range.Count() != row_count) {
        return Status::Invalid("real-time offset range does not match batch row count");
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> data,
        arrow::ImportArray(write_batch.batch->GetData(), arrow::struct_(write_schema_->fields())));
    std::shared_ptr<arrow::StructArray> struct_array =
        std::dynamic_pointer_cast<arrow::StructArray>(data);
    if (!struct_array) {
        return Status::Invalid("real-time write data is not a StructArray");
    }

    if (closed_) {
        return Status::Invalid("mem indexer is closed");
    }
    if (building_range_ && write_batch.offset_range.from != building_range_->to + 1) {
        return Status::Invalid("real-time offset ranges must be contiguous");
    }
    building_memory_usage_ += GetArrayMemoryUsage(struct_array->data());
    building_batches_.push_back(
        StoredBatch{std::move(struct_array), write_batch.batch->GetRowKind()});
    if (!building_range_) {
        building_range_ = write_batch.offset_range;
    } else {
        building_range_ = Range(building_range_->from, write_batch.offset_range.to);
    }
    return Status::OK();
}

Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> ArrowMemIndexer::SealForCommit() {
    if (closed_) {
        return Status::Invalid("mem indexer is closed");
    }
    if (building_batches_.empty()) {
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
    }
    auto segment = std::make_shared<Segment>(building_range_.value(), std::move(building_batches_));
    building_batches_.clear();
    building_range_.reset();
    building_memory_usage_ = 0;
    return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(std::move(segment));
}

Result<std::vector<std::unique_ptr<BatchReader>>> ArrowMemIndexer::CreateCommitReaders(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    std::shared_ptr<Segment> arrow_segment = std::dynamic_pointer_cast<Segment>(segment);
    if (!arrow_segment) {
        return Status::Invalid("segment was not created by the Arrow mem indexer");
    }
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.push_back(std::make_unique<CommitBatchReader>(arrow_segment, arrow_pool_));
    return readers;
}

uint64_t ArrowMemIndexer::GetMemoryUsage() const {
    return building_memory_usage_;
}

Status ArrowMemIndexer::Close() {
    building_batches_.clear();
    building_memory_usage_ = 0;
    building_range_.reset();
    closed_ = true;
    return Status::OK();
}

}  // namespace paimon

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

#include "paimon/core/realtime/primary_key_realtime_store.h"

#include <mutex>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/core_options.h"
#include "paimon/macros.h"

namespace paimon {

Status ValidatePrimaryKeyRealtimeOptions(const CoreOptions& options) {
    if (options.GetBucket() <= 0) {
        return Status::NotImplemented("PK realtime v1 requires fixed buckets");
    }
    if (options.GetMergeEngine() != MergeEngine::DEDUPLICATE) {
        return Status::NotImplemented("PK realtime v1 supports only the DEDUPLICATE merge engine");
    }
    if (options.DataEvolutionEnabled()) {
        return Status::NotImplemented("PK realtime v1 does not support data evolution");
    }
    if (!options.GetFieldsSequenceGroups().empty()) {
        return Status::NotImplemented("PK realtime v1 does not support sequence groups");
    }
    if (options.IgnoreDelete() || options.PartialUpdateRemoveRecordOnDelete() ||
        options.AggregationRemoveRecordOnDelete() ||
        !options.GetPartialUpdateRemoveRecordOnSequenceGroup().empty()) {
        return Status::NotImplemented("PK realtime v1 requires default delete behavior");
    }
    if (!options.GetSequenceField().empty()) {
        return Status::NotImplemented("PK realtime v1 does not support sequence.field");
    }
    if (!options.SequenceFieldSortOrderIsAscending()) {
        return Status::NotImplemented(
            "PK realtime v1 supports only ascending sequence.field.sort-order");
    }
    if (options.NeedLookup() || options.DeletionVectorsEnabled() ||
        options.GetChangelogProducer() != ChangelogProducer::NONE) {
        return Status::NotImplemented("PK realtime v1 does not support lookup or early MOR");
    }
    return Status::OK();
}

namespace {

uint64_t GetArrayMemoryUsage(const std::shared_ptr<arrow::ArrayData>& data) {
    uint64_t total = 0;
    for (const std::shared_ptr<arrow::Buffer>& buffer : data->buffers) {
        if (buffer) {
            total += static_cast<uint64_t>(buffer->size());
        }
    }
    for (const std::shared_ptr<arrow::ArrayData>& child : data->child_data) {
        total += GetArrayMemoryUsage(child);
    }
    if (data->dictionary) {
        total += GetArrayMemoryUsage(data->dictionary);
    }
    return total;
}

struct StoredBatch {
    std::shared_ptr<arrow::StructArray> data;
    OffsetRange offset_range;
    uint64_t memory_usage;
};

class Segment final : public RealtimeSegmentHandle {
 public:
    Segment(const OffsetRange& range, std::vector<StoredBatch>&& batches)
        : range_(range), batches_(std::move(batches)) {}

    OffsetRange GetOffsetRange() const override {
        return range_;
    }
    const std::vector<StoredBatch>& Batches() const {
        return batches_;
    }

 private:
    OffsetRange range_;
    std::vector<StoredBatch> batches_;
};

class ReadView final : public RealtimeReadView {
 public:
    explicit ReadView(std::vector<std::shared_ptr<Segment>>&& segments)
        : segments_(std::move(segments)) {
        if (!segments_.empty()) {
            range_ = OffsetRange(segments_.front()->GetOffsetRange().begin,
                                 segments_.back()->GetOffsetRange().end);
        }
    }

    std::optional<OffsetRange> GetOffsetRange() const override {
        return range_;
    }
    const std::vector<std::shared_ptr<Segment>>& Segments() const {
        return segments_;
    }

 private:
    std::vector<std::shared_ptr<Segment>> segments_;
    std::optional<OffsetRange> range_;
};

class RawBatchReader final : public BatchReader {
 public:
    RawBatchReader(std::vector<StoredBatch> batches)
        : batches_(std::move(batches)), metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (next_ == batches_.size()) {
            return MakeEofBatch();
        }
        const std::shared_ptr<arrow::StructArray>& batch = batches_[next_++].data;
        auto array = std::make_unique<ArrowArray>();
        auto schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*batch, array.get(), schema.get()));
        return ReadBatch(std::move(array), std::move(schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }
    void Close() override {
        batches_.clear();
    }

 private:
    std::vector<StoredBatch> batches_;
    size_t next_ = 0;
    std::shared_ptr<Metrics> metrics_;
};

}  // namespace

class PrimaryKeyRealtimeStore::Impl {
 public:
    explicit Impl(std::shared_ptr<arrow::Schema> prepared_schema)
        : prepared_schema_(std::move(prepared_schema)) {}

    Status Write(RealtimeWriteBatch&& write_batch) {
        if (!write_batch.batch || !write_batch.batch->GetData()) {
            return Status::Invalid("PK real-time write batch is null");
        }
        const int64_t row_count = write_batch.batch->GetData()->length;
        if (write_batch.offset_range.begin < 0 || write_batch.offset_range.Count() != row_count ||
            row_count <= 0) {
            return Status::Invalid("PK real-time offset range does not match batch row count");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(write_batch.batch->GetData(),
                               arrow::struct_(prepared_schema_->fields())));
        if (!array || array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("PK real-time prepared batch is not a StructArray");
        }
        std::shared_ptr<arrow::StructArray> prepared =
            checked_pointer_cast<arrow::StructArray>(array);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(prepared->ValidateFull());
        std::lock_guard<std::mutex> lock(mutex_);
        if (last_offset_ && write_batch.offset_range.begin != last_offset_.value()) {
            return Status::Invalid("PK real-time offset ranges must be contiguous");
        }
        building_.push_back(
            StoredBatch{prepared, write_batch.offset_range, GetArrayMemoryUsage(prepared->data())});
        building_memory_usage_ += building_.back().memory_usage;
        last_offset_ = write_batch.offset_range.end;
        return Status::OK();
    }

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (building_.empty()) {
            return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
        }
        OffsetRange range(building_.front().offset_range.begin, building_.back().offset_range.end);
        std::shared_ptr<Segment> segment = std::make_shared<Segment>(range, std::move(building_));
        sealed_.push_back(segment);
        building_.clear();
        building_memory_usage_ = 0;
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(std::move(segment));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& handle) {
        std::shared_ptr<Segment> segment = std::dynamic_pointer_cast<Segment>(handle);
        if (!segment) {
            return Status::Invalid("segment was not created by the PK real-time store");
        }
        std::vector<std::unique_ptr<BatchReader>> readers;
        readers.reserve(segment->Batches().size());
        for (const StoredBatch& batch : segment->Batches()) {
            readers.push_back(std::make_unique<RawBatchReader>(std::vector<StoredBatch>{batch}));
        }
        return readers;
    }

    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::shared_ptr<Segment>> segments = sealed_;
        if (!building_.empty()) {
            OffsetRange range(building_.front().offset_range.begin,
                              building_.back().offset_range.end);
            segments.push_back(
                std::make_shared<Segment>(range, std::vector<StoredBatch>(building_)));
        }
        return std::shared_ptr<RealtimeReadView>(new ReadView(std::move(segments)));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view, int64_t, const RealtimeQueryContext&) {
        std::shared_ptr<ReadView> typed = std::dynamic_pointer_cast<ReadView>(view);
        if (!typed) {
            return Status::Invalid("read view was not created by the PK real-time store");
        }
        std::vector<std::unique_ptr<BatchReader>> readers;
        size_t batch_count = 0;
        for (const std::shared_ptr<Segment>& segment : typed->Segments()) {
            batch_count += segment->Batches().size();
        }
        readers.reserve(batch_count);
        for (const std::shared_ptr<Segment>& segment : typed->Segments()) {
            for (const StoredBatch& batch : segment->Batches()) {
                readers.push_back(
                    std::make_unique<RawBatchReader>(std::vector<StoredBatch>{batch}));
            }
        }
        return readers;
    }

    Status AdvanceCommittedOffset(int64_t committed_end) {
        std::lock_guard<std::mutex> lock(mutex_);
        while (!sealed_.empty() && sealed_.front()->GetOffsetRange().end <= committed_end) {
            sealed_.erase(sealed_.begin());
        }
        return Status::OK();
    }

    uint64_t GetMemoryUsage() const {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t total = building_memory_usage_;
        for (const std::shared_ptr<Segment>& segment : sealed_) {
            for (const StoredBatch& batch : segment->Batches()) {
                total += batch.memory_usage;
            }
        }
        return total;
    }

 private:
    std::shared_ptr<arrow::Schema> prepared_schema_;
    mutable std::mutex mutex_;
    std::vector<StoredBatch> building_;
    std::vector<std::shared_ptr<Segment>> sealed_;
    uint64_t building_memory_usage_ = 0;
    std::optional<int64_t> last_offset_;
};

PrimaryKeyRealtimeStore::PrimaryKeyRealtimeStore(std::unique_ptr<Impl>&& impl)
    : impl_(std::move(impl)) {}
PrimaryKeyRealtimeStore::~PrimaryKeyRealtimeStore() = default;

Result<std::shared_ptr<PrimaryKeyRealtimeStore>> PrimaryKeyRealtimeStore::Create(
    const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!prepared_schema || !memory_pool) {
        return Status::Invalid("PK prepared schema or memory pool is null");
    }
    return std::shared_ptr<PrimaryKeyRealtimeStore>(
        new PrimaryKeyRealtimeStore(std::make_unique<Impl>(prepared_schema)));
}
Status PrimaryKeyRealtimeStore::Write(RealtimeWriteBatch&& batch) {
    return impl_->Write(std::move(batch));
}
Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>>
PrimaryKeyRealtimeStore::SealForCommit() {
    return impl_->SealForCommit();
}
Result<std::vector<std::unique_ptr<BatchReader>>> PrimaryKeyRealtimeStore::CreateCommitReaders(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    return impl_->CreateCommitReaders(segment);
}
Result<std::shared_ptr<RealtimeReadView>> PrimaryKeyRealtimeStore::AcquireReadView() {
    return impl_->AcquireReadView();
}
Result<std::vector<std::unique_ptr<BatchReader>>> PrimaryKeyRealtimeStore::CreateQueryReaders(
    const std::shared_ptr<RealtimeReadView>& view, int64_t offset,
    const RealtimeQueryContext& context) {
    return impl_->CreateQueryReaders(view, offset, context);
}
Status PrimaryKeyRealtimeStore::AdvanceCommittedOffset(int64_t offset) {
    return impl_->AdvanceCommittedOffset(offset);
}
uint64_t PrimaryKeyRealtimeStore::GetMemoryUsage() const {
    return impl_->GetMemoryUsage();
}

}  // namespace paimon

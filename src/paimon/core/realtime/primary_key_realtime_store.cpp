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

#include <cstddef>
#include <mutex>
#include <optional>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/api.h"
#include "paimon/common/data/columnar/columnar_batch_context.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/realtime/prepared_key_value_reader.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/macros.h"

namespace paimon {

Status ValidatePrimaryKeyRealtimeOptions(const CoreOptions& options, const TableSchema& schema) {
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
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> primary_key_fields,
                           schema.TrimmedPrimaryKeyFields());
    for (const DataField& field : primary_key_fields) {
        if (field.Type()->id() == arrow::Type::FLOAT || field.Type()->id() == arrow::Type::DOUBLE) {
            return Status::NotImplemented(
                "PK realtime v1 does not support FLOAT or DOUBLE primary keys");
        }
    }
    if (options.GlobalIndexEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexDefinitions definitions,
                               PrimaryKeyIndexDefinitions::Create(schema));
        if (!definitions.Definitions().empty()) {
            return Status::NotImplemented("PK realtime v1 does not support global indexes");
        }
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
    RawBatchReader(std::vector<StoredBatch> batches, std::vector<int32_t> key_field_indexes,
                   const std::shared_ptr<FieldsComparator>& key_comparator,
                   const std::shared_ptr<MemoryPool>& memory_pool)
        : batches_(std::move(batches)),
          positions_(batches_.size(), 0),
          key_field_indexes_(std::move(key_field_indexes)),
          key_comparator_(key_comparator),
          memory_pool_(memory_pool),
          arrow_pool_(GetArrowPool(memory_pool)),
          heap_(SourceGreater{this}),
          metrics_(std::make_shared<MetricsImpl>()) {
        key_contexts_.reserve(batches_.size());
        sequence_arrays_.reserve(batches_.size());
        for (size_t i = 0; i < batches_.size(); ++i) {
            const StoredBatch& batch = batches_[i];
            arrow::ArrayVector key_arrays;
            key_arrays.reserve(key_field_indexes_.size());
            for (int32_t field_index : key_field_indexes_) {
                key_arrays.push_back(batch.data->field(field_index));
            }
            key_contexts_.push_back(
                std::make_shared<ColumnarBatchContext>(key_arrays, memory_pool_));
            sequence_arrays_.push_back(
                checked_pointer_cast<arrow::Int64Array>(batch.data->field(1)));
            if (batch.data->length() > 0) {
                heap_.push(i);
            }
        }
    }

    Result<ReadBatch> NextBatch() override {
        if (heap_.empty()) {
            return MakeEofBatch();
        }

        struct SelectedRow {
            size_t selected_source;
            int64_t source_ordinal;
        };
        struct SelectedSource {
            size_t source;
            std::vector<int64_t> rows;
            int64_t base = -1;
        };
        std::vector<SelectedRow> selected_rows;
        selected_rows.reserve(kOutputBatchSize);
        std::vector<SelectedSource> selected_sources;
        std::unordered_map<size_t, size_t> selected_source_indexes;
        while (!heap_.empty() && selected_rows.size() < kOutputBatchSize) {
            const size_t source = heap_.top();
            heap_.pop();
            auto [source_it, inserted] =
                selected_source_indexes.emplace(source, selected_sources.size());
            if (inserted) {
                selected_sources.push_back(SelectedSource{source, {}});
            }
            SelectedSource& selected_source = selected_sources[source_it->second];
            selected_rows.push_back(
                SelectedRow{source_it->second, static_cast<int64_t>(selected_source.rows.size())});
            selected_source.rows.push_back(positions_[source]++);
            if (positions_[source] < batches_[source].data->length()) {
                heap_.push(source);
            }
        }

        arrow::compute::ExecContext context(arrow_pool_.get());
        arrow::ArrayVector grouped_batches;
        int64_t grouped_row_count = 0;
        for (SelectedSource& selected_source : selected_sources) {
            arrow::Int64Builder source_index_builder(arrow_pool_.get());
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                source_index_builder.AppendValues(selected_source.rows));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> source_indices,
                                              source_index_builder.Finish());
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                arrow::Datum source_batch,
                arrow::compute::Take(arrow::Datum(batches_[selected_source.source].data),
                                     arrow::Datum(source_indices),
                                     arrow::compute::TakeOptions::NoBoundsCheck(), &context));
            selected_source.base = grouped_row_count;
            grouped_row_count += static_cast<int64_t>(selected_source.rows.size());
            grouped_batches.push_back(source_batch.make_array());
        }

        std::shared_ptr<arrow::Array> batch;
        if (grouped_batches.size() == 1) {
            batch = std::move(grouped_batches[0]);
        } else {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> grouped,
                arrow::Concatenate(grouped_batches, arrow_pool_.get()));
            arrow::Int64Builder order_builder(arrow_pool_.get());
            PAIMON_RETURN_NOT_OK_FROM_ARROW(order_builder.Reserve(selected_rows.size()));
            for (const SelectedRow& selected : selected_rows) {
                order_builder.UnsafeAppend(selected_sources[selected.selected_source].base +
                                           selected.source_ordinal);
            }
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> order,
                                              order_builder.Finish());
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                arrow::Datum reordered,
                arrow::compute::Take(arrow::Datum(grouped), arrow::Datum(order),
                                     arrow::compute::TakeOptions::NoBoundsCheck(), &context));
            batch = reordered.make_array();
        }
        auto array = std::make_unique<ArrowArray>();
        auto schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*batch, array.get(), schema.get()));
        return ReadBatch(std::move(array), std::move(schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }
    void Close() override {
        while (!heap_.empty()) {
            heap_.pop();
        }
        batches_.clear();
        positions_.clear();
        key_contexts_.clear();
        sequence_arrays_.clear();
    }

 private:
    static constexpr size_t kOutputBatchSize = 1024;

    bool Less(size_t left, size_t right) const {
        ColumnarRowRef left_key(key_contexts_[left], positions_[left]);
        ColumnarRowRef right_key(key_contexts_[right], positions_[right]);
        const int32_t key_comparison = key_comparator_->CompareTo(left_key, right_key);
        if (key_comparison != 0) {
            return key_comparison < 0;
        }
        const int64_t left_sequence = sequence_arrays_[left]->Value(positions_[left]);
        const int64_t right_sequence = sequence_arrays_[right]->Value(positions_[right]);
        if (left_sequence != right_sequence) {
            return left_sequence < right_sequence;
        }
        return left < right;
    }

    struct SourceGreater {
        RawBatchReader* reader;

        bool operator()(size_t left, size_t right) const {
            return reader->Less(right, left);
        }
    };

    std::vector<StoredBatch> batches_;
    std::vector<int64_t> positions_;
    std::vector<int32_t> key_field_indexes_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::vector<std::shared_ptr<ColumnarBatchContext>> key_contexts_;
    std::vector<std::shared_ptr<arrow::Int64Array>> sequence_arrays_;
    std::priority_queue<size_t, std::vector<size_t>, SourceGreater> heap_;
    std::shared_ptr<Metrics> metrics_;
};

}  // namespace

class PrimaryKeyRealtimeStore::Impl {
 public:
    Impl(std::shared_ptr<arrow::Schema> prepared_schema, std::vector<int32_t> key_field_indexes,
         const std::shared_ptr<FieldsComparator>& key_comparator,
         const std::shared_ptr<MemoryPool>& memory_pool)
        : prepared_schema_(std::move(prepared_schema)),
          key_field_indexes_(std::move(key_field_indexes)),
          key_comparator_(key_comparator),
          memory_pool_(memory_pool) {}

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
        if (!segment->Batches().empty()) {
            readers.push_back(std::make_unique<RawBatchReader>(
                segment->Batches(), key_field_indexes_, key_comparator_, memory_pool_));
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
        std::vector<StoredBatch> batches;
        for (const std::shared_ptr<Segment>& segment : typed->Segments()) {
            batches.insert(batches.end(), segment->Batches().begin(), segment->Batches().end());
        }
        if (!batches.empty()) {
            readers.push_back(std::make_unique<RawBatchReader>(
                std::move(batches), key_field_indexes_, key_comparator_, memory_pool_));
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
    std::vector<int32_t> key_field_indexes_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MemoryPool> memory_pool_;
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
    const std::vector<std::string>& trimmed_primary_keys,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    PAIMON_RETURN_NOT_OK(ValidatePreparedTransportSchema(prepared_schema));
    if (trimmed_primary_keys.empty() || !memory_pool) {
        return Status::Invalid("PK primary keys are empty or memory pool is null");
    }
    std::vector<int32_t> key_field_indexes;
    std::vector<DataField> key_fields;
    key_field_indexes.reserve(trimmed_primary_keys.size());
    key_fields.reserve(trimmed_primary_keys.size());
    for (const std::string& key : trimmed_primary_keys) {
        const int32_t field_index = prepared_schema->GetFieldIndex(key);
        if (field_index < 3) {
            return Status::Invalid("PK field is missing from prepared schema: ", key);
        }
        key_field_indexes.push_back(field_index);
        PAIMON_ASSIGN_OR_RAISE(DataField field, DataField::ConvertArrowFieldToDataField(
                                                    prepared_schema->field(field_index)));
        key_fields.push_back(std::move(field));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(key_fields, /*is_ascending_order=*/true));
    return std::shared_ptr<PrimaryKeyRealtimeStore>(new PrimaryKeyRealtimeStore(
        std::make_unique<Impl>(prepared_schema, key_field_indexes, key_comparator, memory_pool)));
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

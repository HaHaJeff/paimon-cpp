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

#include "paimon/core/realtime/primary_key_realtime_store.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/key_value_in_memory_record_reader.h"
#include "paimon/core/io/key_value_projection_consumer.h"
#include "paimon/core/io/key_value_projection_reader.h"
#include "paimon/core/io/merged_key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/sort_merge_reader_with_loser_tree.h"
#include "paimon/core/utils/nested_projection_utils.h"
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

struct StoredBatch {
    std::shared_ptr<arrow::StructArray> data;
    std::vector<RecordBatch::RowKind> row_kinds;
    OffsetRange offset_range;
    int64_t first_sequence_number;
    uint64_t memory_usage;
};
using BatchGroup = std::vector<std::shared_ptr<const StoredBatch>>;

class Segment final : public RealtimeSegmentHandle {
 public:
    Segment(const OffsetRange& offset_range,
            std::vector<std::shared_ptr<const StoredBatch>>&& batches)
        : offset_range_(offset_range), batches_(std::move(batches)) {}

    OffsetRange GetOffsetRange() const override {
        return offset_range_;
    }

    const std::vector<std::shared_ptr<const StoredBatch>>& Batches() const {
        return batches_;
    }

    uint64_t GetMemoryUsage() const {
        uint64_t result = 0;
        for (const std::shared_ptr<const StoredBatch>& batch : batches_) {
            result += batch->memory_usage;
        }
        return result;
    }

 private:
    OffsetRange offset_range_;
    std::vector<std::shared_ptr<const StoredBatch>> batches_;
};

class PrimaryKeyRealtimeReadView final : public RealtimeReadView {
 public:
    explicit PrimaryKeyRealtimeReadView(std::vector<BatchGroup>&& groups)
        : groups_(std::move(groups)) {
        if (!groups_.empty()) {
            offset_range_ = OffsetRange(groups_.front().front()->offset_range.begin,
                                        groups_.back().back()->offset_range.end);
        }
    }

    std::optional<OffsetRange> GetOffsetRange() const override {
        return offset_range_;
    }

    const std::vector<BatchGroup>& Groups() const {
        return groups_;
    }

 private:
    std::vector<BatchGroup> groups_;
    std::optional<OffsetRange> offset_range_;
};

class CommitBatchReader final : public BatchReader {
 public:
    CommitBatchReader(const std::shared_ptr<Segment>& segment,
                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : segment_(segment), arrow_pool_(arrow_pool), metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (!segment_ || next_batch_ >= static_cast<int32_t>(segment_->Batches().size())) {
            return MakeEofBatch();
        }
        const std::shared_ptr<const StoredBatch>& stored = segment_->Batches()[next_batch_++];
        const int64_t row_count = stored->data->length();
        arrow::Int8Builder row_kind_builder(arrow_pool_.get());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(row_kind_builder.Reserve(row_count));
        if (stored->row_kinds.empty()) {
            for (int64_t i = 0; i < row_count; ++i) {
                row_kind_builder.UnsafeAppend(static_cast<int8_t>(RecordBatch::RowKind::INSERT));
            }
        } else {
            for (RecordBatch::RowKind row_kind : stored->row_kinds) {
                row_kind_builder.UnsafeAppend(static_cast<int8_t>(row_kind));
            }
        }
        std::shared_ptr<arrow::Array> row_kind_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(row_kind_builder.Finish(&row_kind_array));
        arrow::ArrayVector arrays = {std::move(row_kind_array)};
        arrays.insert(arrays.end(), stored->data->fields().begin(), stored->data->fields().end());
        arrow::FieldVector fields = {
            DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())};
        const arrow::FieldVector& value_fields = stored->data->struct_type()->fields();
        fields.insert(fields.end(), value_fields.begin(), value_fields.end());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> output,
                                          arrow::StructArray::Make(arrays, fields));
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*output, c_array.get(), c_schema.get()));
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
    int32_t next_batch_ = 0;
};

class KeyRangeBatchReader final : public BatchReader, public PrimaryKeyRangeProvider {
 public:
    KeyRangeBatchReader(std::unique_ptr<BatchReader>&& reader,
                        const std::shared_ptr<InternalRow>& min_key,
                        const std::shared_ptr<InternalRow>& max_key)
        : reader_(std::move(reader)), min_key_(min_key), max_key_(max_key) {}

    Result<ReadBatch> NextBatch() override {
        return reader_->NextBatch();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

    std::shared_ptr<InternalRow> GetMinKey() const override {
        return min_key_;
    }

    std::shared_ptr<InternalRow> GetMaxKey() const override {
        return max_key_;
    }

 private:
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<InternalRow> min_key_;
    std::shared_ptr<InternalRow> max_key_;
};

}  // namespace

class PrimaryKeyRealtimeStore::Impl {
 public:
    Impl(const std::shared_ptr<arrow::Schema>& write_schema, std::vector<std::string> primary_keys,
         const std::shared_ptr<FieldsComparator>& key_comparator,
         const std::function<std::shared_ptr<MergeFunctionWrapper<KeyValue>>()>&
             merge_function_wrapper_factory,
         int64_t next_sequence_number, int32_t read_batch_size,
         const std::shared_ptr<MemoryPool>& memory_pool)
        : write_schema_(write_schema),
          primary_keys_(std::move(primary_keys)),
          key_comparator_(key_comparator),
          merge_function_wrapper_factory_(merge_function_wrapper_factory),
          next_sequence_number_(next_sequence_number),
          read_batch_size_(read_batch_size),
          memory_pool_(memory_pool),
          arrow_pool_(GetArrowPool(memory_pool)) {}

    Result<std::shared_ptr<InternalRow>> CopyKey(const InternalRow& key) const {
        auto result = std::make_shared<BinaryRow>(static_cast<int32_t>(primary_keys_.size()));
        BinaryRowWriter writer(result.get(), /*initial_size=*/128, memory_pool_.get());
        writer.Reset();
        for (int32_t index = 0; index < static_cast<int32_t>(primary_keys_.size()); ++index) {
            std::shared_ptr<arrow::Field> field =
                write_schema_->GetFieldByName(primary_keys_[index]);
            PAIMON_ASSIGN_OR_RAISE(InternalRow::FieldGetterFunc getter,
                                   InternalRow::CreateFieldGetter(index, field->type(),
                                                                  /*use_view=*/true));
            PAIMON_ASSIGN_OR_RAISE(BinaryRowWriter::FieldSetterFunc setter,
                                   BinaryRowWriter::CreateFieldSetter(index, field->type()));
            setter(getter(key), &writer);
        }
        writer.Complete();
        return std::static_pointer_cast<InternalRow>(result);
    }

    Result<std::pair<std::shared_ptr<InternalRow>, std::shared_ptr<InternalRow>>> GetKeyRange(
        const std::shared_ptr<arrow::StructArray>& values) const {
        arrow::ArrayVector key_arrays;
        key_arrays.reserve(primary_keys_.size());
        for (const std::string& primary_key : primary_keys_) {
            std::shared_ptr<arrow::Array> key_array = values->GetFieldByName(primary_key);
            if (!key_array) {
                return Status::Invalid("primary key is missing from PK query batch: ", primary_key);
            }
            key_arrays.push_back(std::move(key_array));
        }
        auto context = std::make_shared<ColumnarBatchContext>(key_arrays, memory_pool_);
        int64_t min_row = 0;
        int64_t max_row = 0;
        for (int64_t row = 1; row < values->length(); ++row) {
            ColumnarRowRef current(context, row);
            ColumnarRowRef min_key(context, min_row);
            ColumnarRowRef max_key(context, max_row);
            if (key_comparator_->CompareTo(current, min_key) < 0) {
                min_row = row;
            }
            if (key_comparator_->CompareTo(current, max_key) > 0) {
                max_row = row;
            }
        }
        ColumnarRowRef min_key(context, min_row);
        ColumnarRowRef max_key(context, max_row);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalRow> copied_min, CopyKey(min_key));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalRow> copied_max, CopyKey(max_key));
        return std::make_pair(std::move(copied_min), std::move(copied_max));
    }

    Status Write(RealtimeWriteBatch&& write_batch) {
        if (!write_batch.batch || !write_batch.batch->GetData()) {
            return Status::Invalid("PK real-time write batch is null");
        }
        const int64_t row_count = write_batch.batch->GetData()->length;
        if (row_count <= 0 || write_batch.offset_range.begin < 0 ||
            write_batch.offset_range.Count() != row_count) {
            return Status::Invalid("PK real-time offset range does not match batch row count");
        }
        const std::vector<RecordBatch::RowKind>& row_kinds = write_batch.batch->GetRowKind();
        if (!row_kinds.empty() && static_cast<int64_t>(row_kinds.size()) != row_count) {
            return Status::Invalid("PK real-time row-kind count does not match batch row count");
        }
        for (RecordBatch::RowKind row_kind : row_kinds) {
            PAIMON_ASSIGN_OR_RAISE(const RowKind* validated,
                                   RowKind::FromByteValue(static_cast<int8_t>(row_kind)));
            static_cast<void>(validated);
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> imported,
            arrow::ImportArray(write_batch.batch->GetData(),
                               arrow::struct_(write_schema_->fields())));
        if (!imported || imported->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("PK real-time write data is not a StructArray");
        }
        std::shared_ptr<arrow::StructArray> values =
            checked_pointer_cast<arrow::StructArray>(imported);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(values->ValidateFull());

        std::lock_guard<std::mutex> lock(mutex_);
        if (last_offset_ && write_batch.offset_range.begin != last_offset_.value()) {
            return Status::Invalid("PK real-time offset ranges must be contiguous");
        }
        if (row_count > std::numeric_limits<int64_t>::max() - next_sequence_number_) {
            return Status::Invalid("PK sequence range exceeds INT64_MAX");
        }
        auto stored = std::make_shared<const StoredBatch>(
            StoredBatch{std::move(values), row_kinds, write_batch.offset_range,
                        next_sequence_number_, GetArrayMemoryUsage(imported->data())});
        building_batches_.push_back(std::move(stored));
        building_memory_usage_ += building_batches_.back()->memory_usage;
        last_offset_ = write_batch.offset_range.end;
        next_sequence_number_ += row_count;
        return Status::OK();
    }

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (building_batches_.empty()) {
            return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
        }
        const OffsetRange range(building_batches_.front()->offset_range.begin,
                                building_batches_.back()->offset_range.end);
        auto segment = std::make_shared<Segment>(range, std::move(building_batches_));
        sealed_segments_.push_back(segment);
        building_batches_.clear();
        building_memory_usage_ = 0;
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(std::move(segment));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) {
        std::shared_ptr<Segment> typed = std::dynamic_pointer_cast<Segment>(segment);
        if (!typed) {
            return Status::Invalid("segment was not created by the PK real-time store");
        }
        std::vector<std::unique_ptr<BatchReader>> result;
        result.push_back(std::make_unique<CommitBatchReader>(typed, arrow_pool_));
        return result;
    }

    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<BatchGroup> groups;
        groups.reserve(sealed_segments_.size() + (building_batches_.empty() ? 0 : 1));
        for (const std::shared_ptr<Segment>& segment : sealed_segments_) {
            groups.push_back(segment->Batches());
        }
        if (!building_batches_.empty()) {
            groups.push_back(building_batches_);
        }
        return std::shared_ptr<RealtimeReadView>(new PrimaryKeyRealtimeReadView(std::move(groups)));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view, int64_t lower,
        const RealtimeQueryContext& context) {
        std::shared_ptr<PrimaryKeyRealtimeReadView> typed =
            std::dynamic_pointer_cast<PrimaryKeyRealtimeReadView>(view);
        if (!typed) {
            return Status::Invalid("read view was not created by the PK real-time store");
        }
        if (!context.read_schema || !context.read_schema->release) {
            return Status::Invalid("PK real-time query read schema is null");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> requested,
                                          arrow::ImportSchema(context.read_schema));
        arrow::FieldVector output_fields = {
            DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())};
        arrow::FieldVector aligned_value_fields = write_schema_->fields();
        std::vector<int32_t> projection = {KeyValueProjectionConsumer::kValueKindProjection};
        for (const std::shared_ptr<arrow::Field>& field : requested->fields()) {
            if (field->name() == SpecialFields::ValueKind().Name()) {
                continue;
            }
            output_fields.push_back(field);
            if (field->name() == SpecialFields::SequenceNumber().Name()) {
                projection.push_back(KeyValueProjectionConsumer::kSequenceNumberProjection);
                continue;
            }
            int32_t index = write_schema_->GetFieldIndex(field->name());
            if (index < 0) {
                Result<int32_t> field_id = NestedProjectionUtils::GetPaimonFieldId(field);
                if (!field_id.ok()) {
                    return Status::Invalid(
                        "PK real-time query field is missing from write schema: ", field->name());
                }
                index = static_cast<int32_t>(aligned_value_fields.size());
                aligned_value_fields.push_back(field);
            } else {
                aligned_value_fields[index] = field;
            }
            projection.push_back(index);
        }
        const std::shared_ptr<arrow::DataType> aligned_value_type =
            arrow::struct_(aligned_value_fields);

        std::vector<std::unique_ptr<BatchReader>> result;
        for (const BatchGroup& group : typed->Groups()) {
            std::vector<std::unique_ptr<KeyValueRecordReader>> batch_readers;
            std::shared_ptr<InternalRow> min_key;
            std::shared_ptr<InternalRow> max_key;
            for (const std::shared_ptr<const StoredBatch>& batch : group) {
                if (batch->offset_range.end <= lower) {
                    continue;
                }
                const int64_t offset = std::max<int64_t>(0, lower - batch->offset_range.begin);
                const int64_t length = batch->data->length() - offset;
                std::shared_ptr<arrow::Array> sliced = batch->data->Slice(offset, length);
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> aligned,
                                       NestedProjectionUtils::AlignArrayToReadType(
                                           sliced, aligned_value_type, arrow_pool_.get()));
                if (!aligned || aligned->type_id() != arrow::Type::STRUCT) {
                    return Status::Invalid(
                        "PK real-time query projection did not produce a "
                        "StructArray");
                }
                std::shared_ptr<arrow::StructArray> selected =
                    checked_pointer_cast<arrow::StructArray>(aligned);
                using KeyRange =
                    std::pair<std::shared_ptr<InternalRow>, std::shared_ptr<InternalRow>>;
                PAIMON_ASSIGN_OR_RAISE(KeyRange key_range, GetKeyRange(selected));
                if (!min_key || key_comparator_->CompareTo(*key_range.first, *min_key) < 0) {
                    min_key = key_range.first;
                }
                if (!max_key || key_comparator_->CompareTo(*key_range.second, *max_key) > 0) {
                    max_key = key_range.second;
                }
                std::vector<RecordBatch::RowKind> selected_kinds;
                if (!batch->row_kinds.empty()) {
                    selected_kinds.assign(batch->row_kinds.begin() + offset,
                                          batch->row_kinds.end());
                }
                std::unique_ptr<KeyValueRecordReader> reader =
                    std::make_unique<KeyValueInMemoryRecordReader>(
                        batch->first_sequence_number + offset, selected, selected_kinds,
                        primary_keys_, /*user_defined_sequence_fields=*/std::vector<std::string>(),
                        /*sequence_fields_ascending=*/true, key_comparator_, memory_pool_);
                std::shared_ptr<MergeFunctionWrapper<KeyValue>> batch_merge =
                    merge_function_wrapper_factory_();
                if (!batch_merge) {
                    return Status::Invalid("merge function wrapper factory returned null");
                }
                batch_readers.push_back(std::make_unique<MergedKeyValueRecordReader>(
                    std::move(reader), key_comparator_, batch_merge));
            }
            if (batch_readers.empty()) {
                continue;
            }
            std::shared_ptr<MergeFunctionWrapper<KeyValue>> group_merge =
                merge_function_wrapper_factory_();
            if (!group_merge) {
                return Status::Invalid("merge function wrapper factory returned null");
            }
            auto merged = std::make_unique<SortMergeReaderWithLoserTree>(
                std::move(batch_readers), key_comparator_,
                /*user_defined_seq_comparator=*/nullptr, group_merge);
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<KeyValueProjectionReader> projected,
                KeyValueProjectionReader::Create(std::move(merged), arrow::schema(output_fields),
                                                 projection, read_batch_size_, memory_pool_));
            result.push_back(
                std::make_unique<KeyRangeBatchReader>(std::move(projected), min_key, max_key));
        }
        return result;
    }

    Status AdvanceCommittedOffset(int64_t committed_end_offset) {
        std::lock_guard<std::mutex> lock(mutex_);
        sealed_segments_.erase(
            std::remove_if(sealed_segments_.begin(), sealed_segments_.end(),
                           [committed_end_offset](const std::shared_ptr<Segment>& segment) {
                               return segment->GetOffsetRange().end <= committed_end_offset;
                           }),
            sealed_segments_.end());
        return Status::OK();
    }

    uint64_t GetMemoryUsage() const {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t result = building_memory_usage_;
        for (const std::shared_ptr<Segment>& segment : sealed_segments_) {
            result += segment->GetMemoryUsage();
        }
        return result;
    }

 private:
    std::shared_ptr<arrow::Schema> write_schema_;
    std::vector<std::string> primary_keys_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::function<std::shared_ptr<MergeFunctionWrapper<KeyValue>>()>
        merge_function_wrapper_factory_;
    int64_t next_sequence_number_;
    int32_t read_batch_size_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    mutable std::mutex mutex_;
    std::vector<std::shared_ptr<const StoredBatch>> building_batches_;
    std::vector<std::shared_ptr<Segment>> sealed_segments_;
    uint64_t building_memory_usage_ = 0;
    std::optional<int64_t> last_offset_;
};

Result<std::shared_ptr<PrimaryKeyRealtimeStore>> PrimaryKeyRealtimeStore::Create(
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::vector<std::string>& primary_keys,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::function<std::shared_ptr<MergeFunctionWrapper<KeyValue>>()>&
        merge_function_wrapper_factory,
    int64_t restore_max_sequence_number, int32_t read_batch_size,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!write_schema || primary_keys.empty() || !key_comparator ||
        !merge_function_wrapper_factory || !memory_pool || read_batch_size <= 0) {
        return Status::Invalid("PK real-time store requires schema, keys, merge helpers, and pool");
    }
    if (restore_max_sequence_number < -1) {
        return Status::Invalid("PK restore max sequence number must be at least -1");
    }
    if (restore_max_sequence_number == std::numeric_limits<int64_t>::max()) {
        return Status::Invalid("PK sequence number has reached INT64_MAX");
    }
    for (const std::string& key : primary_keys) {
        if (write_schema->GetFieldIndex(key) < 0) {
            return Status::Invalid("primary key ", key, " is missing from write schema");
        }
    }
    auto impl = std::make_unique<Impl>(
        write_schema, primary_keys, key_comparator, merge_function_wrapper_factory,
        restore_max_sequence_number + 1, read_batch_size, memory_pool);
    return std::shared_ptr<PrimaryKeyRealtimeStore>(new PrimaryKeyRealtimeStore(std::move(impl)));
}

PrimaryKeyRealtimeStore::PrimaryKeyRealtimeStore(std::unique_ptr<Impl>&& impl)
    : impl_(std::move(impl)) {}

PrimaryKeyRealtimeStore::~PrimaryKeyRealtimeStore() = default;

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
    const std::shared_ptr<RealtimeReadView>& view, int64_t offset_begin,
    const RealtimeQueryContext& context) {
    return impl_->CreateQueryReaders(view, offset_begin, context);
}

Status PrimaryKeyRealtimeStore::AdvanceCommittedOffset(int64_t committed_offset) {
    return impl_->AdvanceCommittedOffset(committed_offset);
}

uint64_t PrimaryKeyRealtimeStore::GetMemoryUsage() const {
    return impl_->GetMemoryUsage();
}

}  // namespace paimon

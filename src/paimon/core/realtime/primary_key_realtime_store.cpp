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
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/realtime/arrow_array_pool_holder.h"
#include "paimon/core/realtime/prepared_key_value_reader.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

namespace {

struct AlignedArray {
    std::shared_ptr<arrow::Array> array;
    bool uses_arrow_pool;
};

Result<AlignedArray> AlignArrayByPaimonIds(const std::shared_ptr<arrow::Array>& array,
                                           const std::shared_ptr<arrow::DataType>& read_type,
                                           arrow::MemoryPool* pool);

bool TypesExactlyEqual(const std::shared_ptr<arrow::DataType>& data_type,
                       const std::shared_ptr<arrow::DataType>& read_type) {
    if (!data_type->Equals(read_type) || data_type->num_fields() != read_type->num_fields()) {
        return false;
    }
    for (int32_t i = 0; i < data_type->num_fields(); ++i) {
        if (!data_type->field(i)->Equals(read_type->field(i), /*check_metadata=*/true) ||
            !TypesExactlyEqual(data_type->field(i)->type(), read_type->field(i)->type())) {
            return false;
        }
    }
    return true;
}

Result<AlignedArray> AlignStructArrayByPaimonIds(
    const std::shared_ptr<arrow::StructArray>& array,
    const std::shared_ptr<arrow::StructType>& read_type, arrow::MemoryPool* pool) {
    const std::shared_ptr<arrow::StructType> data_type =
        checked_pointer_cast<arrow::StructType>(array->type());
    std::unordered_map<int32_t, int32_t> data_field_indexes;
    data_field_indexes.reserve(data_type->num_fields());
    for (int32_t i = 0; i < data_type->num_fields(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(int32_t field_id,
                               NestedProjectionUtils::GetPaimonFieldId(data_type->field(i)));
        if (!data_field_indexes.emplace(field_id, i).second) {
            return Status::Invalid(fmt::format("duplicate field id {} in stored schema", field_id));
        }
    }

    std::unordered_map<int32_t, bool> requested_field_ids;
    requested_field_ids.reserve(read_type->num_fields());
    std::vector<std::shared_ptr<arrow::ArrayData>> children;
    children.reserve(read_type->num_fields());
    bool uses_arrow_pool = false;
    for (const std::shared_ptr<arrow::Field>& read_field : read_type->fields()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t field_id,
                               NestedProjectionUtils::GetPaimonFieldId(read_field));
        if (!requested_field_ids.emplace(field_id, true).second) {
            return Status::Invalid(
                fmt::format("duplicate field id {} in requested schema", field_id));
        }
        const auto data_iter = data_field_indexes.find(field_id);
        if (data_iter == data_field_indexes.end()) {
            if (!read_field->nullable()) {
                return Status::Invalid(fmt::format(
                    "requested non-nullable field '{}' with id {} is absent from stored schema",
                    read_field->name(), field_id));
            }
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> null_child,
                arrow::MakeArrayOfNull(read_field->type(), array->offset() + array->length(),
                                       pool));
            children.push_back(null_child->data());
            uses_arrow_pool = true;
            continue;
        }
        std::shared_ptr<arrow::Array> child =
            arrow::MakeArray(array->data()->child_data[data_iter->second]);
        PAIMON_ASSIGN_OR_RAISE(AlignedArray aligned_child,
                               AlignArrayByPaimonIds(child, read_field->type(), pool));
        children.push_back(aligned_child.array->data());
        uses_arrow_pool = uses_arrow_pool || aligned_child.uses_arrow_pool;
    }

    std::shared_ptr<arrow::ArrayData> aligned = array->data()->Copy();
    aligned->type = read_type;
    aligned->child_data = std::move(children);
    return AlignedArray{arrow::MakeArray(std::move(aligned)), uses_arrow_pool};
}

Result<AlignedArray> AlignArrayByPaimonIds(const std::shared_ptr<arrow::Array>& array,
                                           const std::shared_ptr<arrow::DataType>& read_type,
                                           arrow::MemoryPool* pool) {
    if (TypesExactlyEqual(array->type(), read_type)) {
        return AlignedArray{array, false};
    }
    if (array->type_id() != read_type->id()) {
        return Status::Invalid(fmt::format("stored value type {} does not match requested type {}",
                                           array->type()->ToString(), read_type->ToString()));
    }
    switch (read_type->id()) {
        case arrow::Type::STRUCT:
            return AlignStructArrayByPaimonIds(checked_pointer_cast<arrow::StructArray>(array),
                                               checked_pointer_cast<arrow::StructType>(read_type),
                                               pool);
        case arrow::Type::LIST: {
            std::shared_ptr<arrow::Array> values =
                checked_pointer_cast<arrow::ListArray>(array)->values();
            PAIMON_ASSIGN_OR_RAISE(
                AlignedArray aligned_values,
                AlignArrayByPaimonIds(values, read_type->field(0)->type(), pool));
            std::shared_ptr<arrow::ArrayData> aligned = array->data()->Copy();
            aligned->type = read_type;
            aligned->child_data = {aligned_values.array->data()};
            return AlignedArray{arrow::MakeArray(std::move(aligned)),
                                aligned_values.uses_arrow_pool};
        }
        case arrow::Type::MAP: {
            const std::shared_ptr<arrow::MapArray> map =
                checked_pointer_cast<arrow::MapArray>(array);
            const std::shared_ptr<arrow::MapType> map_type =
                checked_pointer_cast<arrow::MapType>(read_type);
            std::shared_ptr<arrow::Array> keys = map->keys();
            PAIMON_ASSIGN_OR_RAISE(AlignedArray aligned_keys,
                                   AlignArrayByPaimonIds(keys, map_type->key_type(), pool));
            std::shared_ptr<arrow::Array> items = map->items();
            PAIMON_ASSIGN_OR_RAISE(AlignedArray aligned_items,
                                   AlignArrayByPaimonIds(items, map_type->item_type(), pool));
            std::shared_ptr<arrow::ArrayData> entries = array->data()->child_data[0]->Copy();
            entries->type = arrow::struct_({map_type->key_field(), map_type->item_field()});
            entries->child_data = {aligned_keys.array->data(), aligned_items.array->data()};
            std::shared_ptr<arrow::ArrayData> aligned = array->data()->Copy();
            aligned->type = read_type;
            aligned->child_data = {std::move(entries)};
            return AlignedArray{arrow::MakeArray(std::move(aligned)),
                                aligned_keys.uses_arrow_pool || aligned_items.uses_arrow_pool};
        }
        default:
            return Status::Invalid(
                fmt::format("stored leaf type {} does not match requested type {}",
                            array->type()->ToString(), read_type->ToString()));
    }
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

class StoredBatchReader final : public BatchReader {
 public:
    explicit StoredBatchReader(const StoredBatch& batch,
                               std::shared_ptr<arrow::MemoryPool> arrow_pool = nullptr)
        : arrow_pool_(std::move(arrow_pool)),
          data_(batch.data),
          metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (!data_) {
            return MakeEofBatch();
        }
        auto array = std::make_unique<ArrowArray>();
        auto schema = std::make_unique<ArrowSchema>();
        ScopeGuard export_guard([array_ptr = array.get(), schema_ptr = schema.get()]() {
            ArrowArrayRelease(array_ptr);
            ArrowSchemaRelease(schema_ptr);
        });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*data_, array.get(), schema.get()));
        if (arrow_pool_) {
            PAIMON_RETURN_NOT_OK(RetainArrowArrayMemoryPool(array.get(), arrow_pool_));
        }
        data_.reset();
        arrow_pool_.reset();
        export_guard.Release();
        return ReadBatch(std::move(array), std::move(schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }
    void Close() override {
        data_.reset();
        arrow_pool_.reset();
    }

 private:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<arrow::StructArray> data_;
    std::shared_ptr<Metrics> metrics_;
};

}  // namespace

class PrimaryKeyRealtimeStore::Impl {
 public:
    Impl(std::shared_ptr<arrow::Schema> prepared_schema, std::shared_ptr<MemoryPool> memory_pool,
         std::shared_ptr<arrow::MemoryPool> arrow_pool)
        : prepared_schema_(std::move(prepared_schema)),
          memory_pool_(std::move(memory_pool)),
          arrow_pool_(std::move(arrow_pool)) {}

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
        std::lock_guard<std::mutex> lock(mutex_);
        building_.push_back(StoredBatch{prepared, write_batch.offset_range,
                                        ArrowUtils::GetArrayMemoryUsage(prepared->data())});
        building_memory_usage_ += building_.back().memory_usage;
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
            readers.push_back(std::make_unique<StoredBatchReader>(batch));
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
        const std::shared_ptr<RealtimeReadView>& view, int64_t,
        const RealtimeQueryContext& context) {
        std::shared_ptr<ReadView> typed = std::dynamic_pointer_cast<ReadView>(view);
        if (!typed) {
            return Status::Invalid("read view was not created by the PK real-time store");
        }
        if (context.read_schema == nullptr || context.read_schema->release == nullptr) {
            return Status::Invalid("PK real-time query read schema is null");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                          arrow::ImportSchema(context.read_schema));
        PAIMON_RETURN_NOT_OK(PreparedKeyValueReaderFactory::ValidateTransportSchema(read_schema));
        std::vector<std::unique_ptr<BatchReader>> readers;
        for (const std::shared_ptr<Segment>& segment : typed->Segments()) {
            for (const StoredBatch& batch : segment->Batches()) {
                PAIMON_ASSIGN_OR_RAISE(
                    AlignedArray projected,
                    AlignArrayByPaimonIds(batch.data, arrow::struct_(read_schema->fields()),
                                          arrow_pool_.get()));
                StoredBatch query_batch{checked_pointer_cast<arrow::StructArray>(projected.array),
                                        batch.offset_range, /*memory_usage=*/0};
                readers.push_back(std::make_unique<StoredBatchReader>(
                    query_batch, projected.uses_arrow_pool ? arrow_pool_ : nullptr));
            }
        }
        return readers;
    }

    Status AdvanceCommittedOffset(int64_t committed_end_offset) {
        std::lock_guard<std::mutex> lock(mutex_);
        while (!sealed_.empty() && sealed_.front()->GetOffsetRange().end <= committed_end_offset) {
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
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    mutable std::mutex mutex_;
    std::vector<StoredBatch> building_;
    std::vector<std::shared_ptr<Segment>> sealed_;
    uint64_t building_memory_usage_ = 0;
};

PrimaryKeyRealtimeStore::PrimaryKeyRealtimeStore(std::unique_ptr<Impl>&& impl)
    : impl_(std::move(impl)) {}
PrimaryKeyRealtimeStore::~PrimaryKeyRealtimeStore() = default;

Result<std::shared_ptr<PrimaryKeyRealtimeStore>> PrimaryKeyRealtimeStore::Create(
    const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    PAIMON_RETURN_NOT_OK(PreparedKeyValueReaderFactory::ValidateTransportSchema(prepared_schema));
    if (!memory_pool) {
        return Status::Invalid("PK real-time store memory pool is null");
    }
    std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(memory_pool);
    return std::shared_ptr<PrimaryKeyRealtimeStore>(new PrimaryKeyRealtimeStore(
        std::make_unique<Impl>(prepared_schema, memory_pool, std::move(arrow_pool))));
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
Status PrimaryKeyRealtimeStore::AdvanceCommittedOffset(int64_t committed_end_offset) {
    return impl_->AdvanceCommittedOffset(committed_end_offset);
}
uint64_t PrimaryKeyRealtimeStore::GetMemoryUsage() const {
    return impl_->GetMemoryUsage();
}

}  // namespace paimon

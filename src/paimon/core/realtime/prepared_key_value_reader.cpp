/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/realtime/prepared_key_value_reader.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/api.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "fmt/format.h"
#include "paimon/common/data/columnar/columnar_batch_context.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/realtime/realtime_fields.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/macros.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/status.h"

namespace paimon {

namespace {

constexpr int32_t kValueKindIndex = 0;
constexpr int32_t kSequenceNumberIndex = 1;
constexpr int32_t kRealtimeOffsetIndex = 2;
constexpr int32_t kPreparedValueStartIndex = 3;

Result<std::shared_ptr<arrow::Array>> AlignArrayByPaimonIds(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& read_type);

class RealtimeOffsetCoverage {
 public:
    static Result<std::shared_ptr<RealtimeOffsetCoverage>> Create(
        const OffsetRange& sealed_offsets, size_t reader_count,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
        if (sealed_offsets.begin < 0 || sealed_offsets.end < sealed_offsets.begin) {
            return Status::Invalid("PK real-time store returned an invalid sealed offset range");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Buffer> seen_offsets,
            arrow::AllocateEmptyBitmap(sealed_offsets.Count(), arrow_pool.get()));
        return std::shared_ptr<RealtimeOffsetCoverage>(new RealtimeOffsetCoverage(
            sealed_offsets, reader_count, std::move(seen_offsets), arrow_pool));
    }

    Status Add(const arrow::Int64Array& offsets) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (int64_t row = 0; row < offsets.length(); ++row) {
            const int64_t offset = offsets.Value(row);
            if (offset < sealed_offsets_.begin || offset >= sealed_offsets_.end) {
                return Status::Invalid(
                    "PK real-time store commit reader offset is outside the sealed range");
            }
            const int64_t index = offset - sealed_offsets_.begin;
            if (arrow::bit_util::GetBit(seen_offsets_->data(), index)) {
                return Status::Invalid(
                    "PK real-time store commit readers contain duplicate REALTIME_OFFSET");
            }
            arrow::bit_util::SetBit(seen_offsets_->mutable_data(), index);
            ++seen_count_;
        }
        return Status::OK();
    }

    Status FinishReader() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++finished_reader_count_;
        if (finished_reader_count_ == reader_count_ && seen_count_ != sealed_offsets_.Count()) {
            return Status::Invalid(
                "PK real-time store commit readers did not cover the sealed range");
        }
        return Status::OK();
    }

 private:
    RealtimeOffsetCoverage(const OffsetRange& sealed_offsets, size_t reader_count,
                           std::shared_ptr<arrow::Buffer> seen_offsets,
                           const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : sealed_offsets_(sealed_offsets),
          reader_count_(reader_count),
          arrow_pool_(arrow_pool),
          seen_offsets_(std::move(seen_offsets)) {}

    OffsetRange sealed_offsets_;
    size_t reader_count_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<arrow::Buffer> seen_offsets_;
    int64_t seen_count_ = 0;
    size_t finished_reader_count_ = 0;
    std::mutex mutex_;
};

Status CheckPreparedField(const std::shared_ptr<arrow::Schema>& schema, int32_t field_idx,
                          const DataField& expected_field) {
    if (schema->num_fields() <= field_idx) {
        return Status::Invalid(fmt::format("prepared schema missing transport field {} at index {}",
                                           expected_field.Name(), field_idx));
    }
    const std::shared_ptr<arrow::Field>& field = schema->field(field_idx);
    PAIMON_ASSIGN_OR_RAISE(int32_t field_id, NestedProjectionUtils::GetPaimonFieldId(field));
    if (field->name() != expected_field.Name() || !field->type()->Equals(*expected_field.Type()) ||
        field->nullable() || field_id != expected_field.Id()) {
        return Status::Invalid(fmt::format(
            "prepared schema field {} must be non-null {}:{} with field id {}, got {}:{} "
            "nullable={} field id {}",
            field_idx, expected_field.Name(), expected_field.Type()->ToString(),
            expected_field.Id(), field->name(), field->type()->ToString(), field->nullable(),
            field_id));
    }
    return Status::OK();
}

Result<int32_t> FindFieldIndexByPaimonId(const arrow::FieldVector& fields, int32_t field_id) {
    std::optional<int32_t> matching_index;
    for (int32_t i = 0; i < static_cast<int32_t>(fields.size()); ++i) {
        PAIMON_ASSIGN_OR_RAISE(int32_t candidate_id,
                               NestedProjectionUtils::GetPaimonFieldId(fields[i]));
        if (candidate_id == field_id) {
            if (matching_index.has_value()) {
                return Status::Invalid(
                    fmt::format("duplicate field id {} in prepared schema", field_id));
            }
            matching_index = i;
        }
    }
    if (matching_index.has_value()) {
        return matching_index.value();
    }
    return Status::Invalid(fmt::format("cannot find field id {} in prepared schema", field_id));
}

Status ValidateProjectionType(const std::shared_ptr<arrow::DataType>& prepared_type,
                              const std::shared_ptr<arrow::DataType>& query_type) {
    if (prepared_type->id() != query_type->id()) {
        return Status::Invalid(fmt::format("prepared value type {} does not match query type {}",
                                           prepared_type->ToString(), query_type->ToString()));
    }
    switch (query_type->id()) {
        case arrow::Type::STRUCT: {
            const arrow::FieldVector& prepared_fields = prepared_type->fields();
            for (const std::shared_ptr<arrow::Field>& query_field : query_type->fields()) {
                PAIMON_ASSIGN_OR_RAISE(int32_t query_id,
                                       NestedProjectionUtils::GetPaimonFieldId(query_field));
                PAIMON_ASSIGN_OR_RAISE(int32_t prepared_idx,
                                       FindFieldIndexByPaimonId(prepared_fields, query_id));
                PAIMON_RETURN_NOT_OK(ValidateProjectionType(prepared_fields[prepared_idx]->type(),
                                                            query_field->type()));
            }
            return Status::OK();
        }
        case arrow::Type::LIST:
            return ValidateProjectionType(prepared_type->field(0)->type(),
                                          query_type->field(0)->type());
        case arrow::Type::MAP: {
            const std::shared_ptr<arrow::MapType> prepared_map =
                checked_pointer_cast<arrow::MapType>(prepared_type);
            const std::shared_ptr<arrow::MapType> query_map =
                checked_pointer_cast<arrow::MapType>(query_type);
            PAIMON_RETURN_NOT_OK(
                ValidateProjectionType(prepared_map->key_type(), query_map->key_type()));
            return ValidateProjectionType(prepared_map->item_type(), query_map->item_type());
        }
        default:
            if (!prepared_type->Equals(*query_type)) {
                return Status::Invalid(
                    fmt::format("prepared leaf type {} does not match query type {}",
                                prepared_type->ToString(), query_type->ToString()));
            }
            return Status::OK();
    }
}

Status ValidateProjectionSchema(const std::shared_ptr<arrow::Schema>& prepared_schema,
                                const std::shared_ptr<arrow::Schema>& query_schema) {
    arrow::FieldVector prepared_value_fields(
        prepared_schema->fields().begin() + kPreparedValueStartIndex,
        prepared_schema->fields().end());
    for (const std::shared_ptr<arrow::Field>& query_field : query_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t query_id,
                               NestedProjectionUtils::GetPaimonFieldId(query_field));
        PAIMON_ASSIGN_OR_RAISE(int32_t prepared_idx,
                               FindFieldIndexByPaimonId(prepared_value_fields, query_id));
        PAIMON_RETURN_NOT_OK(ValidateProjectionType(prepared_value_fields[prepared_idx]->type(),
                                                    query_field->type()));
    }
    return Status::OK();
}

Status ValidateExactCommitSchema(const std::shared_ptr<arrow::Schema>& prepared_schema,
                                 const std::shared_ptr<arrow::Schema>& value_schema) {
    if (prepared_schema->num_fields() != value_schema->num_fields() + kPreparedValueStartIndex) {
        return Status::Invalid("commit requires the exact prepared writer schema");
    }
    for (int32_t i = 0; i < value_schema->num_fields(); ++i) {
        if (!prepared_schema->field(i + kPreparedValueStartIndex)
                 ->Equals(value_schema->field(i), true)) {
            return Status::Invalid("commit requires the exact prepared writer schema");
        }
    }
    return Status::OK();
}

Status ValidatePreparedSchema(const std::shared_ptr<arrow::Schema>& prepared_schema) {
    if (!prepared_schema || prepared_schema->num_fields() < kPreparedValueStartIndex) {
        return Status::Invalid("prepared schema must contain realtime transport fields");
    }
    PAIMON_RETURN_NOT_OK(
        CheckPreparedField(prepared_schema, kValueKindIndex, SpecialFields::ValueKind()));
    PAIMON_RETURN_NOT_OK(
        CheckPreparedField(prepared_schema, kSequenceNumberIndex, SpecialFields::SequenceNumber()));
    PAIMON_RETURN_NOT_OK(
        CheckPreparedField(prepared_schema, kRealtimeOffsetIndex, RealtimeOffsetField()));
    return Status::OK();
}

Result<std::shared_ptr<arrow::Array>> AlignStructArrayByPaimonIds(
    const std::shared_ptr<arrow::StructArray>& array,
    const std::shared_ptr<arrow::StructType>& read_type) {
    const std::shared_ptr<arrow::StructType> data_type =
        checked_pointer_cast<arrow::StructType>(array->type());
    std::unordered_map<int32_t, int32_t> data_field_id_to_idx;
    data_field_id_to_idx.reserve(data_type->num_fields());
    for (int32_t i = 0; i < data_type->num_fields(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(int32_t field_id,
                               NestedProjectionUtils::GetPaimonFieldId(data_type->field(i)));
        if (!data_field_id_to_idx.emplace(field_id, i).second) {
            return Status::Invalid(
                fmt::format("duplicate field id {} in prepared value struct", field_id));
        }
    }

    arrow::ArrayVector aligned_arrays;
    aligned_arrays.reserve(read_type->num_fields());
    for (const std::shared_ptr<arrow::Field>& read_field : read_type->fields()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t read_field_id,
                               NestedProjectionUtils::GetPaimonFieldId(read_field));
        auto data_iter = data_field_id_to_idx.find(read_field_id);
        if (data_iter == data_field_id_to_idx.end()) {
            return Status::Invalid(
                fmt::format("cannot find field id {} in prepared value struct", read_field_id));
        }
        std::shared_ptr<arrow::Array> child =
            arrow::MakeArray(array->data()->child_data[data_iter->second]);
        PAIMON_ASSIGN_OR_RAISE(child, AlignArrayByPaimonIds(child, read_field->type()));
        aligned_arrays.push_back(std::move(child));
    }

    std::shared_ptr<arrow::ArrayData> aligned_data = array->data()->Copy();
    aligned_data->type = read_type;
    aligned_data->child_data.clear();
    aligned_data->child_data.reserve(aligned_arrays.size());
    for (const std::shared_ptr<arrow::Array>& aligned_array : aligned_arrays) {
        aligned_data->child_data.push_back(aligned_array->data());
    }
    return arrow::MakeArray(std::move(aligned_data));
}

Result<std::shared_ptr<arrow::Array>> AlignListArrayByPaimonIds(
    const std::shared_ptr<arrow::ListArray>& array,
    const std::shared_ptr<arrow::ListType>& read_type) {
    std::shared_ptr<arrow::Array> values = array->values();
    PAIMON_ASSIGN_OR_RAISE(values, AlignArrayByPaimonIds(values, read_type->value_type()));
    std::shared_ptr<arrow::ArrayData> new_data = array->data()->Copy();
    new_data->type = read_type;
    new_data->child_data = {values->data()};
    return arrow::MakeArray(new_data);
}

Result<std::shared_ptr<arrow::Array>> AlignMapArrayByPaimonIds(
    const std::shared_ptr<arrow::MapArray>& array,
    const std::shared_ptr<arrow::MapType>& read_type) {
    std::shared_ptr<arrow::Array> keys = array->keys();
    PAIMON_ASSIGN_OR_RAISE(keys, AlignArrayByPaimonIds(keys, read_type->key_type()));
    std::shared_ptr<arrow::Array> items = array->items();
    PAIMON_ASSIGN_OR_RAISE(items, AlignArrayByPaimonIds(items, read_type->item_type()));

    const std::shared_ptr<arrow::ArrayData>& entries_data = array->data()->child_data[0];
    std::shared_ptr<arrow::ArrayData> new_entries = entries_data->Copy();
    new_entries->type = arrow::struct_({read_type->key_field(), read_type->item_field()});
    new_entries->child_data = {keys->data(), items->data()};

    std::shared_ptr<arrow::ArrayData> new_data = array->data()->Copy();
    new_data->type = read_type;
    new_data->child_data = {std::move(new_entries)};
    return arrow::MakeArray(new_data);
}

Result<std::shared_ptr<arrow::Array>> AlignArrayByPaimonIds(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& read_type) {
    if (array->type()->id() != read_type->id()) {
        return Status::Invalid(fmt::format("prepared value type {} does not match query type {}",
                                           array->type()->ToString(), read_type->ToString()));
    }
    switch (read_type->id()) {
        case arrow::Type::STRUCT:
            return AlignStructArrayByPaimonIds(checked_pointer_cast<arrow::StructArray>(array),
                                               checked_pointer_cast<arrow::StructType>(read_type));
        case arrow::Type::LIST:
            return AlignListArrayByPaimonIds(checked_pointer_cast<arrow::ListArray>(array),
                                             checked_pointer_cast<arrow::ListType>(read_type));
        case arrow::Type::MAP:
            return AlignMapArrayByPaimonIds(checked_pointer_cast<arrow::MapArray>(array),
                                            checked_pointer_cast<arrow::MapType>(read_type));
        default:
            if (!array->type()->Equals(*read_type)) {
                return Status::Invalid(
                    fmt::format("prepared leaf type {} does not match query type {}",
                                array->type()->ToString(), read_type->ToString()));
            }
            return array;
    }
}

Result<arrow::ArrayVector> ProjectFieldsByPaimonIds(
    const std::shared_ptr<arrow::StructArray>& data_batch,
    const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::shared_ptr<arrow::Schema>& query_schema) {
    std::unordered_map<int32_t, int32_t> prepared_field_id_to_idx;
    prepared_field_id_to_idx.reserve(prepared_schema->num_fields());
    for (int32_t i = kPreparedValueStartIndex; i < prepared_schema->num_fields(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(int32_t field_id,
                               NestedProjectionUtils::GetPaimonFieldId(prepared_schema->field(i)));
        if (!prepared_field_id_to_idx.emplace(field_id, i).second) {
            return Status::Invalid(
                fmt::format("duplicate field id {} in prepared schema", field_id));
        }
    }

    arrow::ArrayVector result;
    result.reserve(query_schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& query_field : query_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t query_field_id,
                               NestedProjectionUtils::GetPaimonFieldId(query_field));
        auto prepared_iter = prepared_field_id_to_idx.find(query_field_id);
        if (prepared_iter == prepared_field_id_to_idx.end()) {
            return Status::Invalid(
                fmt::format("cannot find field id {} in prepared schema", query_field_id));
        }
        std::shared_ptr<arrow::Array> field_array = data_batch->field(prepared_iter->second);
        PAIMON_ASSIGN_OR_RAISE(field_array,
                               AlignArrayByPaimonIds(field_array, query_field->type()));
        result.push_back(std::move(field_array));
    }
    return result;
}

Result<std::shared_ptr<arrow::StructArray>> ApplyOffsetFilter(
    const std::shared_ptr<arrow::StructArray>& data_batch,
    const std::shared_ptr<arrow::NumericArray<arrow::Int64Type>>& offset_array,
    const std::optional<OffsetRange>& visible_offsets, arrow::MemoryPool* arrow_pool) {
    if (!visible_offsets.has_value()) {
        return data_batch;
    }

    arrow::BooleanBuilder filter_builder(arrow_pool);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(filter_builder.Reserve(offset_array->length()));
    int64_t visible_row_count = 0;
    for (int64_t i = 0; i < offset_array->length(); ++i) {
        int64_t offset = offset_array->Value(i);
        bool visible = offset >= visible_offsets->begin && offset < visible_offsets->end;
        filter_builder.UnsafeAppend(visible);
        visible_row_count += visible;
    }
    if (visible_row_count == 0) {
        return std::shared_ptr<arrow::StructArray>();
    }
    if (visible_row_count == data_batch->length()) {
        return data_batch;
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> filter,
                                      filter_builder.Finish());
    arrow::compute::ExecContext exec_context(arrow_pool);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow::Datum filtered,
        arrow::compute::Filter(data_batch, filter, arrow::compute::FilterOptions::Defaults(),
                               &exec_context));
    return checked_pointer_cast<arrow::StructArray>(filtered.make_array());
}

class PreparedKeyValueReader final : public KeyValueRecordReader {
 public:
    PreparedKeyValueReader(std::unique_ptr<BatchReader>&& reader,
                           const std::shared_ptr<arrow::Schema>& prepared_schema,
                           const std::optional<OffsetRange>& visible_offsets,
                           const std::shared_ptr<arrow::Schema>& key_schema,
                           const std::shared_ptr<arrow::Schema>& value_schema,
                           const std::shared_ptr<FieldsComparator>& key_comparator,
                           const std::shared_ptr<MemoryPool>& pool,
                           const std::shared_ptr<RealtimeOffsetCoverage>& offset_coverage)
        : reader_(std::move(reader)),
          prepared_schema_(prepared_schema),
          visible_offsets_(visible_offsets),
          key_schema_(key_schema),
          value_schema_(value_schema),
          key_comparator_(key_comparator),
          pool_(pool),
          arrow_pool_(GetArrowPool(pool)),
          offset_coverage_(offset_coverage) {}

    ~PreparedKeyValueReader() override {
        Close();
    }

    class Iterator final : public KeyValueRecordReader::Iterator {
     public:
        explicit Iterator(PreparedKeyValueReader* reader) : reader_(reader) {}

        Result<bool> HasNext() const override {
            return cursor_ < reader_->row_kind_array_->length();
        }

        Result<KeyValue> Next() override {
            if (cursor_ >= reader_->row_kind_array_->length()) {
                return Status::Invalid("No more prepared key values in current iterator");
            }
            std::shared_ptr<InternalRow> key =
                std::make_shared<ColumnarRowRef>(reader_->key_ctx_, cursor_);
            auto value = std::make_unique<ColumnarRowRef>(reader_->value_ctx_, cursor_);
            PAIMON_ASSIGN_OR_RAISE(
                const RowKind* row_kind,
                RowKind::FromByteValue(reader_->row_kind_array_->Value(cursor_)));
            int64_t sequence_number = reader_->sequence_number_array_->Value(cursor_);
            ++cursor_;
            return KeyValue(row_kind, sequence_number, KeyValue::UNKNOWN_LEVEL, std::move(key),
                            std::move(value));
        }

     private:
        PreparedKeyValueReader* reader_;
        int64_t cursor_ = 0;
    };

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override {
        Result<std::unique_ptr<KeyValueRecordReader::Iterator>> result = NextBatchImpl();
        if (!result.ok()) {
            Close();
        }
        return result;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        if (closed_) {
            return;
        }
        closed_ = true;
        ResetBatchState();
        reader_->Close();
    }

 private:
    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatchImpl() {
        while (true) {
            ResetBatchState();
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader_->NextBatch());
            if (BatchReader::IsEofBatch(batch)) {
                if (offset_coverage_ && !offset_coverage_finished_) {
                    offset_coverage_finished_ = true;
                    PAIMON_RETURN_NOT_OK(offset_coverage_->FinishReader());
                }
                return std::unique_ptr<KeyValueRecordReader::Iterator>();
            }
            auto& [c_array, c_schema] = batch;
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                              arrow::ImportArray(c_array.get(), c_schema.get()));
            if (!arrow_array || arrow_array->type_id() != arrow::Type::STRUCT) {
                return Status::Invalid("cannot cast prepared batch to StructArray");
            }
            std::shared_ptr<arrow::StructArray> data_batch =
                checked_pointer_cast<arrow::StructArray>(arrow_array);
            PAIMON_RETURN_NOT_OK(ValidatePreparedBatch(data_batch));
            PAIMON_RETURN_NOT_OK(ValidateOrdering(data_batch));

            std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> offset_array =
                checked_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(
                    data_batch->field(kRealtimeOffsetIndex));
            if (offset_coverage_) {
                PAIMON_RETURN_NOT_OK(offset_coverage_->Add(*offset_array));
            }
            PAIMON_ASSIGN_OR_RAISE(
                data_batch,
                ApplyOffsetFilter(data_batch, offset_array, visible_offsets_, arrow_pool_.get()));
            if (!data_batch) {
                continue;
            }

            row_kind_array_ = checked_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(
                data_batch->field(kValueKindIndex));
            sequence_number_array_ = checked_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(
                data_batch->field(kSequenceNumberIndex));
            PAIMON_ASSIGN_OR_RAISE(
                arrow::ArrayVector key_fields,
                ProjectFieldsByPaimonIds(data_batch, prepared_schema_, key_schema_));
            PAIMON_ASSIGN_OR_RAISE(
                arrow::ArrayVector value_fields,
                ProjectFieldsByPaimonIds(data_batch, prepared_schema_, value_schema_));
            key_ctx_ = std::make_shared<ColumnarBatchContext>(key_fields, pool_);
            value_ctx_ = std::make_shared<ColumnarBatchContext>(value_fields, pool_);
            ArrowUtils::TraverseArray(data_batch);
            return std::make_unique<Iterator>(this);
        }
    }

    Status ValidatePreparedBatch(const std::shared_ptr<arrow::StructArray>& data_batch) const {
        if (data_batch->num_fields() != prepared_schema_->num_fields()) {
            return Status::Invalid(fmt::format(
                "prepared batch field count {} does not match prepared schema field count {}",
                data_batch->num_fields(), prepared_schema_->num_fields()));
        }
        const arrow::FieldVector& batch_fields = data_batch->type()->fields();
        for (int32_t i = 0; i < data_batch->num_fields(); ++i) {
            if (!batch_fields[i]->Equals(prepared_schema_->field(i), true)) {
                return Status::Invalid(fmt::format(
                    "prepared batch field {} does not match declared prepared schema", i));
            }
        }
        if (!data_batch->field(kValueKindIndex) ||
            data_batch->field(kValueKindIndex)->type_id() != arrow::Type::INT8) {
            return Status::Invalid("cannot cast VALUE_KIND column to int8 arrow array");
        }
        if (!data_batch->field(kSequenceNumberIndex) ||
            data_batch->field(kSequenceNumberIndex)->type_id() != arrow::Type::INT64) {
            return Status::Invalid("cannot cast SEQUENCE_NUMBER column to int64 arrow array");
        }
        if (!data_batch->field(kRealtimeOffsetIndex) ||
            data_batch->field(kRealtimeOffsetIndex)->type_id() != arrow::Type::INT64) {
            return Status::Invalid("cannot cast REALTIME_OFFSET column to int64 arrow array");
        }
        if (data_batch->field(kValueKindIndex)->null_count() != 0 ||
            data_batch->field(kSequenceNumberIndex)->null_count() != 0 ||
            data_batch->field(kRealtimeOffsetIndex)->null_count() != 0) {
            return Status::Invalid("prepared transport columns must not contain nulls");
        }
        return Status::OK();
    }

    Status ValidateOrdering(const std::shared_ptr<arrow::StructArray>& data_batch) {
        if (data_batch->length() == 0) {
            return Status::OK();
        }
        PAIMON_ASSIGN_OR_RAISE(arrow::ArrayVector key_fields,
                               ProjectFieldsByPaimonIds(data_batch, prepared_schema_, key_schema_));
        std::shared_ptr<ColumnarBatchContext> key_context =
            std::make_shared<ColumnarBatchContext>(key_fields, pool_);
        std::shared_ptr<arrow::Int64Array> sequences =
            checked_pointer_cast<arrow::Int64Array>(data_batch->field(kSequenceNumberIndex));
        for (int64_t row = 0; row < data_batch->length(); ++row) {
            ColumnarRowRef current_key(key_context, row);
            if (previous_key_context_) {
                ColumnarRowRef previous_key(previous_key_context_, previous_key_row_);
                const int32_t key_comparison =
                    key_comparator_->CompareTo(previous_key, current_key);
                if (key_comparison > 0 ||
                    (key_comparison == 0 && previous_sequence_ > sequences->Value(row))) {
                    return Status::Invalid(
                        "PK real-time plugin reader is not globally sorted by primary key and "
                        "sequence number");
                }
            }
            previous_key_context_ = key_context;
            previous_key_row_ = row;
            previous_sequence_ = sequences->Value(row);
        }
        return Status::OK();
    }

    void ResetBatchState() {
        key_ctx_.reset();
        value_ctx_.reset();
        row_kind_array_.reset();
        sequence_number_array_.reset();
    }

 private:
    bool closed_ = false;
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<arrow::Schema> prepared_schema_;
    std::optional<OffsetRange> visible_offsets_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<RealtimeOffsetCoverage> offset_coverage_;
    bool offset_coverage_finished_ = false;
    std::shared_ptr<ColumnarBatchContext> key_ctx_;
    std::shared_ptr<ColumnarBatchContext> value_ctx_;
    std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> row_kind_array_;
    std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> sequence_number_array_;
    std::shared_ptr<ColumnarBatchContext> previous_key_context_;
    int64_t previous_key_row_ = 0;
    int64_t previous_sequence_ = 0;
};

}  // namespace

namespace {

Result<std::unique_ptr<KeyValueRecordReader>> AdaptPreparedBatchReaderImpl(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::optional<OffsetRange>& visible_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<RealtimeOffsetCoverage>& offset_coverage) {
    std::unique_ptr<BatchReader> owned_reader = std::move(reader);
    if (!owned_reader) {
        return Status::Invalid("prepared batch reader cannot be null");
    }
    ScopeGuard close_guard([&owned_reader]() -> void { owned_reader->Close(); });
    PAIMON_RETURN_NOT_OK(ValidatePreparedSchema(prepared_schema));
    if (!key_schema) {
        return Status::Invalid("prepared key schema cannot be null");
    }
    if (!value_schema) {
        return Status::Invalid("prepared value schema cannot be null");
    }
    if (!key_comparator) {
        return Status::Invalid("prepared key comparator cannot be null");
    }
    if (!memory_pool) {
        return Status::Invalid("prepared reader memory pool cannot be null");
    }
    PAIMON_RETURN_NOT_OK(ValidateProjectionSchema(prepared_schema, key_schema));
    PAIMON_RETURN_NOT_OK(ValidateProjectionSchema(prepared_schema, value_schema));
    if (!visible_offsets.has_value()) {
        PAIMON_RETURN_NOT_OK(ValidateExactCommitSchema(prepared_schema, value_schema));
    }
    std::unique_ptr<KeyValueRecordReader> result(new PreparedKeyValueReader(
        std::move(owned_reader), prepared_schema, visible_offsets, key_schema, value_schema,
        key_comparator, memory_pool, offset_coverage));
    close_guard.Release();
    return result;
}

}  // namespace

Result<std::unique_ptr<KeyValueRecordReader>> AdaptPreparedBatchReader(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::optional<OffsetRange>& visible_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    return AdaptPreparedBatchReaderImpl(std::move(reader), prepared_schema, visible_offsets,
                                        key_schema, value_schema, key_comparator, memory_pool,
                                        /*offset_coverage=*/nullptr);
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> AdaptPreparedCommitBatchReaders(
    std::vector<std::unique_ptr<BatchReader>>&& readers,
    const std::shared_ptr<arrow::Schema>& prepared_schema, const OffsetRange& sealed_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!memory_pool) {
        return Status::Invalid("prepared reader memory pool cannot be null");
    }
    std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(memory_pool);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<RealtimeOffsetCoverage> offset_coverage,
        RealtimeOffsetCoverage::Create(sealed_offsets, readers.size(), arrow_pool));
    ScopeGuard readers_guard([&readers]() {
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            if (reader) {
                reader->Close();
            }
        }
    });
    std::vector<std::unique_ptr<KeyValueRecordReader>> adapted_readers;
    ScopeGuard adapted_readers_guard([&adapted_readers]() {
        for (const std::unique_ptr<KeyValueRecordReader>& reader : adapted_readers) {
            reader->Close();
        }
    });
    adapted_readers.reserve(readers.size());
    for (std::unique_ptr<BatchReader>& reader : readers) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<KeyValueRecordReader> adapted_reader,
                               AdaptPreparedBatchReaderImpl(
                                   std::move(reader), prepared_schema, std::nullopt, key_schema,
                                   value_schema, key_comparator, memory_pool, offset_coverage));
        adapted_readers.push_back(std::move(adapted_reader));
    }
    readers_guard.Release();
    adapted_readers_guard.Release();
    return adapted_readers;
}

Result<std::unique_ptr<KeyValueRecordReader>> AdaptPreparedBatchReader(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::optional<OffsetRange>& visible_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!key_schema) {
        return Status::Invalid("prepared key schema cannot be null");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> key_fields,
                           DataField::ConvertArrowSchemaToDataFields(key_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(key_fields, /*is_ascending_order=*/true));
    return AdaptPreparedBatchReader(std::move(reader), prepared_schema, visible_offsets, key_schema,
                                    value_schema, key_comparator, memory_pool);
}

}  // namespace paimon

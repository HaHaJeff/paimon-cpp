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
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/columnar/columnar_batch_context.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/macros.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

namespace {

template <typename Reader>
void CloseReaders(const std::vector<std::unique_ptr<Reader>>& readers) {
    for (const std::unique_ptr<Reader>& reader : readers) {
        if (reader) {
            reader->Close();
        }
    }
}

class RealtimeOffsetCoverage {
 public:
    static Result<std::shared_ptr<RealtimeOffsetCoverage>> Create(const OffsetRange& sealed_offsets,
                                                                  size_t reader_count) {
        if (sealed_offsets.begin < 0 || sealed_offsets.end < sealed_offsets.begin) {
            return Status::Invalid("PK real-time store returned an invalid sealed offset range");
        }
        return std::shared_ptr<RealtimeOffsetCoverage>(
            new RealtimeOffsetCoverage(sealed_offsets, reader_count));
    }

    Status Add(const arrow::Int64Array& offsets) {
        for (int64_t row = 0; row < offsets.length(); ++row) {
            const int64_t offset = offsets.Value(row);
            if (offset < sealed_offsets_.begin || offset >= sealed_offsets_.end) {
                return Status::Invalid(
                    "PK real-time store commit reader offset is outside the sealed range");
            }
            if (!seen_offsets_.CheckedAdd(offset)) {
                return Status::Invalid(
                    "PK real-time store commit readers did not cover the sealed range");
            }
        }
        return Status::OK();
    }

    Status FinishReader() {
        ++finished_reader_count_;
        if (finished_reader_count_ == reader_count_ &&
            seen_offsets_.Cardinality() != sealed_offsets_.Count()) {
            return Status::Invalid(
                "PK real-time store commit readers did not cover the sealed range");
        }
        return Status::OK();
    }

 private:
    RealtimeOffsetCoverage(const OffsetRange& sealed_offsets, size_t reader_count)
        : sealed_offsets_(sealed_offsets), reader_count_(reader_count) {}

    OffsetRange sealed_offsets_;
    size_t reader_count_;
    RoaringBitmap64 seen_offsets_;
    size_t finished_reader_count_ = 0;
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

Result<std::vector<int32_t>> ResolveFieldIndexes(
    const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::unordered_map<int32_t, int32_t>& field_indexes,
    const std::shared_ptr<arrow::Schema>& row_schema) {
    std::vector<int32_t> result;
    result.reserve(row_schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& row_field : row_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t field_id,
                               NestedProjectionUtils::GetPaimonFieldId(row_field));
        auto field_index = field_indexes.find(field_id);
        if (field_index == field_indexes.end()) {
            return Status::Invalid(
                fmt::format("cannot find field id {} in prepared schema", field_id));
        }
        const std::shared_ptr<arrow::Field>& prepared_field =
            prepared_schema->field(field_index->second);
        if (!prepared_field->type()->Equals(row_field->type())) {
            return Status::Invalid(fmt::format(
                "prepared field id {} type {} does not match row "
                "type {}",
                field_id, prepared_field->type()->ToString(), row_field->type()->ToString()));
        }
        result.push_back(field_index->second);
    }
    return result;
}

Status ValidateExactCommitSchema(const std::shared_ptr<arrow::Schema>& prepared_schema,
                                 const std::shared_ptr<arrow::Schema>& value_schema) {
    if (prepared_schema->num_fields() !=
        value_schema->num_fields() + SpecialFields::kPreparedKeyValueValueStartIndex) {
        return Status::Invalid("commit requires the exact prepared writer schema");
    }
    for (int32_t i = 0; i < value_schema->num_fields(); ++i) {
        if (!prepared_schema->field(i + SpecialFields::kPreparedKeyValueValueStartIndex)
                 ->Equals(value_schema->field(i), true)) {
            return Status::Invalid("commit requires the exact prepared writer schema");
        }
    }
    return Status::OK();
}

class PreparedReaderPlan {
 public:
    static Result<std::shared_ptr<const PreparedReaderPlan>> Create(
        const std::shared_ptr<arrow::Schema>& prepared_schema,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::shared_ptr<MemoryPool>& memory_pool, bool exact_commit_schema) {
        PAIMON_RETURN_NOT_OK(
            PreparedKeyValueReaderFactory::ValidateTransportSchema(prepared_schema));
        if (!key_schema) {
            return Status::Invalid("prepared key schema cannot be null");
        }
        if (!value_schema) {
            return Status::Invalid("prepared value schema cannot be null");
        }
        if (!memory_pool) {
            return Status::Invalid("prepared reader memory pool cannot be null");
        }
        if (exact_commit_schema) {
            PAIMON_RETURN_NOT_OK(ValidateExactCommitSchema(prepared_schema, value_schema));
        }
        std::unordered_map<int32_t, int32_t> field_indexes;
        field_indexes.reserve(prepared_schema->num_fields() -
                              SpecialFields::kPreparedKeyValueValueStartIndex);
        for (int32_t i = SpecialFields::kPreparedKeyValueValueStartIndex;
             i < prepared_schema->num_fields(); ++i) {
            PAIMON_ASSIGN_OR_RAISE(int32_t field_id, NestedProjectionUtils::GetPaimonFieldId(
                                                         prepared_schema->field(i)));
            if (!field_indexes.emplace(field_id, i).second) {
                return Status::Invalid(
                    fmt::format("duplicate field id {} in prepared schema", field_id));
            }
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> key_field_indexes,
                               ResolveFieldIndexes(prepared_schema, field_indexes, key_schema));
        PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> value_field_indexes,
                               ResolveFieldIndexes(prepared_schema, field_indexes, value_schema));
        return std::shared_ptr<const PreparedReaderPlan>(new PreparedReaderPlan(
            prepared_schema, std::move(key_field_indexes), std::move(value_field_indexes)));
    }

    const std::shared_ptr<arrow::Schema>& PreparedSchema() const {
        return prepared_schema_;
    }

    const std::vector<int32_t>& KeyFieldIndexes() const {
        return key_field_indexes_;
    }

    const std::vector<int32_t>& ValueFieldIndexes() const {
        return value_field_indexes_;
    }

 private:
    PreparedReaderPlan(const std::shared_ptr<arrow::Schema>& schema,
                       std::vector<int32_t>&& key_indexes, std::vector<int32_t>&& value_indexes)
        : prepared_schema_(schema),
          key_field_indexes_(std::move(key_indexes)),
          value_field_indexes_(std::move(value_indexes)) {}

    const std::shared_ptr<arrow::Schema> prepared_schema_;
    const std::vector<int32_t> key_field_indexes_;
    const std::vector<int32_t> value_field_indexes_;
};

class PreparedKeyValueReader final : public KeyValueRecordReader {
 public:
    PreparedKeyValueReader(std::unique_ptr<BatchReader>&& reader,
                           const std::shared_ptr<const PreparedReaderPlan>& plan,
                           const std::optional<OffsetRange>& visible_offsets,
                           const std::shared_ptr<MemoryPool>& pool,
                           const std::shared_ptr<RealtimeOffsetCoverage>& offset_coverage)
        : reader_(std::move(reader)),
          plan_(plan),
          visible_offsets_(visible_offsets),
          pool_(pool),
          offset_coverage_(offset_coverage) {}

    ~PreparedKeyValueReader() override {
        Close();
    }

    class Iterator final : public KeyValueRecordReader::Iterator {
     public:
        explicit Iterator(PreparedKeyValueReader* reader) : reader_(reader) {}

        Result<bool> HasNext() const override {
            return cursor_ < reader_->RowCount();
        }

        Result<KeyValue> Next() override {
            if (cursor_ >= reader_->RowCount()) {
                return Status::Invalid("No more prepared key values in current iterator");
            }
            const int64_t row = reader_->RowAt(cursor_);
            std::shared_ptr<InternalRow> key =
                std::make_shared<ColumnarRowRef>(reader_->key_ctx_, row);
            auto value = std::make_unique<ColumnarRowRef>(reader_->value_ctx_, row);
            PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                                   RowKind::FromByteValue(reader_->row_kind_array_->Value(row)));
            int64_t sequence_number = reader_->sequence_number_array_->Value(row);
            ++cursor_;
            return KeyValue(row_kind, sequence_number, KeyValue::UNKNOWN_LEVEL, std::move(key),
                            std::move(value));
        }

     private:
        PreparedKeyValueReader* reader_;
        int64_t cursor_ = 0;
    };

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override {
        if (first_error_.has_value()) {
            return first_error_.value();
        }
        Result<std::unique_ptr<KeyValueRecordReader::Iterator>> result = NextBatchImpl();
        if (!result.ok()) {
            first_error_ = result.status();
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
            BatchReader::ReadBatchWithBitmap batch_with_bitmap;
            if (visible_offsets_.has_value()) {
                PAIMON_ASSIGN_OR_RAISE(batch_with_bitmap, reader_->NextBatchWithBitmap());
            } else {
                PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader_->NextBatch());
                batch_with_bitmap.first = std::move(batch);
            }
            if (BatchReader::IsEofBatch(batch_with_bitmap)) {
                if (offset_coverage_ && !offset_coverage_finished_) {
                    offset_coverage_finished_ = true;
                    PAIMON_RETURN_NOT_OK(offset_coverage_->FinishReader());
                }
                return std::unique_ptr<KeyValueRecordReader::Iterator>();
            }
            auto& [batch, selection] = batch_with_bitmap;
            auto& [c_array, c_schema] = batch;
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                              arrow::ImportArray(c_array.get(), c_schema.get()));
            if (!arrow_array || arrow_array->type_id() != arrow::Type::STRUCT) {
                return Status::Invalid("cannot cast prepared batch to StructArray");
            }
            std::shared_ptr<arrow::StructArray> data_batch =
                checked_pointer_cast<arrow::StructArray>(arrow_array);
            PAIMON_RETURN_NOT_OK(ValidatePreparedBatch(data_batch));

            std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> offset_array =
                checked_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(
                    data_batch->field(SpecialFields::kPreparedKeyValueRealtimeOffsetIndex));
            if (offset_coverage_) {
                PAIMON_RETURN_NOT_OK(offset_coverage_->Add(*offset_array));
            }

            row_kind_array_ = checked_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(
                data_batch->field(SpecialFields::kPreparedKeyValueValueKindIndex));
            sequence_number_array_ = checked_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(
                data_batch->field(SpecialFields::kPreparedKeyValueSequenceNumberIndex));
            arrow::ArrayVector key_fields;
            key_fields.reserve(plan_->KeyFieldIndexes().size());
            for (int32_t index : plan_->KeyFieldIndexes()) {
                key_fields.push_back(data_batch->field(index));
            }
            arrow::ArrayVector value_fields;
            value_fields.reserve(plan_->ValueFieldIndexes().size());
            for (int32_t index : plan_->ValueFieldIndexes()) {
                value_fields.push_back(data_batch->field(index));
            }
            key_ctx_ = std::make_shared<ColumnarBatchContext>(key_fields, pool_);
            value_ctx_ = std::make_shared<ColumnarBatchContext>(value_fields, pool_);
            PAIMON_ASSIGN_OR_RAISE(bool has_selected_rows,
                                   SelectRows(*offset_array, std::move(selection)));
            if (!has_selected_rows) {
                continue;
            }
            ArrowUtils::TraverseArray(data_batch);
            return std::make_unique<Iterator>(this);
        }
    }

    Status ValidatePreparedBatch(const std::shared_ptr<arrow::StructArray>& data_batch) const {
        if (data_batch->num_fields() != plan_->PreparedSchema()->num_fields()) {
            return Status::Invalid(fmt::format(
                "prepared batch field count {} does not match prepared schema field count {}",
                data_batch->num_fields(), plan_->PreparedSchema()->num_fields()));
        }
        const arrow::FieldVector& batch_fields = data_batch->type()->fields();
        for (int32_t i = 0; i < data_batch->num_fields(); ++i) {
            if (!batch_fields[i]->Equals(plan_->PreparedSchema()->field(i), true)) {
                return Status::Invalid(fmt::format(
                    "prepared batch field {} does not match declared prepared schema", i));
            }
        }
        if (data_batch->field(SpecialFields::kPreparedKeyValueValueKindIndex)->null_count() != 0 ||
            data_batch->field(SpecialFields::kPreparedKeyValueSequenceNumberIndex)->null_count() !=
                0 ||
            data_batch->field(SpecialFields::kPreparedKeyValueRealtimeOffsetIndex)->null_count() !=
                0) {
            return Status::Invalid("prepared transport columns must not contain nulls");
        }
        return Status::OK();
    }

    Result<bool> SelectRows(const arrow::Int64Array& offsets, RoaringBitmap32&& selection) {
        for (auto iter = selection.Begin(); iter != selection.End(); ++iter) {
            const int32_t row = *iter;
            if (row < 0 || row >= offsets.length()) {
                return Status::Invalid(
                    fmt::format("selected row id {} is out of bounds for prepared batch length {}",
                                row, offsets.length()));
            }
        }
        if (!visible_offsets_.has_value()) {
            selected_rows_.reserve(offsets.length());
            for (int64_t row = 0; row < offsets.length(); ++row) {
                selected_rows_.push_back(row);
            }
            return true;
        }
        for (auto iter = selection.Begin(); iter != selection.End(); ++iter) {
            const int32_t row = *iter;
            const int64_t offset = offsets.Value(row);
            if (offset >= visible_offsets_->begin && offset < visible_offsets_->end) {
                selected_rows_.push_back(row);
            }
        }
        return !selected_rows_.empty();
    }

    int64_t RowCount() const {
        return static_cast<int64_t>(selected_rows_.size());
    }

    int64_t RowAt(int64_t ordinal) const {
        return selected_rows_[ordinal];
    }

    void ResetBatchState() {
        key_ctx_.reset();
        value_ctx_.reset();
        row_kind_array_.reset();
        sequence_number_array_.reset();
        selected_rows_.clear();
    }

 private:
    bool closed_ = false;
    std::optional<Status> first_error_;
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<const PreparedReaderPlan> plan_;
    std::optional<OffsetRange> visible_offsets_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<RealtimeOffsetCoverage> offset_coverage_;
    bool offset_coverage_finished_ = false;
    std::shared_ptr<ColumnarBatchContext> key_ctx_;
    std::shared_ptr<ColumnarBatchContext> value_ctx_;
    std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> row_kind_array_;
    std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> sequence_number_array_;
    std::vector<int64_t> selected_rows_;
};

}  // namespace

Status PreparedKeyValueReaderFactory::ValidateTransportSchema(
    const std::shared_ptr<arrow::Schema>& prepared_schema) {
    if (!prepared_schema ||
        prepared_schema->num_fields() < SpecialFields::kPreparedKeyValueValueStartIndex) {
        return Status::Invalid("prepared schema must contain realtime transport fields");
    }
    PAIMON_RETURN_NOT_OK(CheckPreparedField(prepared_schema,
                                            SpecialFields::kPreparedKeyValueValueKindIndex,
                                            SpecialFields::ValueKind()));
    PAIMON_RETURN_NOT_OK(CheckPreparedField(prepared_schema,
                                            SpecialFields::kPreparedKeyValueSequenceNumberIndex,
                                            SpecialFields::SequenceNumber()));
    PAIMON_RETURN_NOT_OK(CheckPreparedField(prepared_schema,
                                            SpecialFields::kPreparedKeyValueRealtimeOffsetIndex,
                                            SpecialFields::RealtimeOffset()));
    return Status::OK();
}

namespace {

Result<std::unique_ptr<KeyValueRecordReader>> AdaptPreparedBatchReaderImpl(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<const PreparedReaderPlan>& plan,
    const std::optional<OffsetRange>& visible_offsets,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<RealtimeOffsetCoverage>& offset_coverage) {
    std::unique_ptr<BatchReader> owned_reader = std::move(reader);
    if (!owned_reader) {
        return Status::Invalid("prepared batch reader cannot be null");
    }
    ScopeGuard close_guard([&owned_reader]() -> void { owned_reader->Close(); });
    if (visible_offsets.has_value() && visible_offsets->begin > visible_offsets->end) {
        return Status::Invalid("prepared visible offset range begin exceeds end");
    }
    std::unique_ptr<KeyValueRecordReader> result = std::make_unique<PreparedKeyValueReader>(
        std::move(owned_reader), plan, visible_offsets, memory_pool, offset_coverage);
    close_guard.Release();
    return result;
}

}  // namespace

Result<std::unique_ptr<KeyValueRecordReader>> PreparedKeyValueReaderFactory::Create(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::optional<OffsetRange>& visible_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    std::unique_ptr<BatchReader> owned_reader = std::move(reader);
    ScopeGuard reader_guard([&owned_reader]() {
        if (owned_reader) {
            owned_reader->Close();
        }
    });
    if (visible_offsets.has_value() && visible_offsets->begin > visible_offsets->end) {
        return Status::Invalid("prepared visible offset range begin exceeds end");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<const PreparedReaderPlan> plan,
        PreparedReaderPlan::Create(prepared_schema, key_schema, value_schema, memory_pool,
                                   /*exact_commit_schema=*/!visible_offsets.has_value()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<KeyValueRecordReader> result,
        AdaptPreparedBatchReaderImpl(std::move(owned_reader), plan, visible_offsets, memory_pool,
                                     /*offset_coverage=*/nullptr));
    reader_guard.Release();
    return result;
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>>
PreparedKeyValueReaderFactory::CreateForQuery(std::vector<std::unique_ptr<BatchReader>>&& readers,
                                              const std::shared_ptr<arrow::Schema>& prepared_schema,
                                              const OffsetRange& visible_offsets,
                                              const std::shared_ptr<arrow::Schema>& key_schema,
                                              const std::shared_ptr<arrow::Schema>& value_schema,
                                              const std::shared_ptr<MemoryPool>& memory_pool) {
    std::vector<std::unique_ptr<KeyValueRecordReader>> adapted_readers;
    ScopeGuard readers_guard([&readers, &adapted_readers]() {
        CloseReaders(readers);
        CloseReaders(adapted_readers);
    });
    if (visible_offsets.begin > visible_offsets.end) {
        return Status::Invalid("prepared visible offset range begin exceeds end");
    }
    for (const std::unique_ptr<BatchReader>& reader : readers) {
        if (!reader) {
            return Status::Invalid("PK real-time store returned a null query reader");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<const PreparedReaderPlan> plan,
        PreparedReaderPlan::Create(prepared_schema, key_schema, value_schema, memory_pool,
                                   /*exact_commit_schema=*/false));
    adapted_readers.reserve(readers.size());
    for (std::unique_ptr<BatchReader>& reader : readers) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<KeyValueRecordReader> adapted_reader,
            AdaptPreparedBatchReaderImpl(std::move(reader), plan, visible_offsets, memory_pool,
                                         /*offset_coverage=*/nullptr));
        adapted_readers.push_back(std::move(adapted_reader));
    }
    readers_guard.Release();
    return adapted_readers;
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>>
PreparedKeyValueReaderFactory::CreateForCommit(
    std::vector<std::unique_ptr<BatchReader>>&& readers,
    const std::shared_ptr<arrow::Schema>& prepared_schema, const OffsetRange& sealed_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    std::vector<std::unique_ptr<KeyValueRecordReader>> adapted_readers;
    ScopeGuard readers_guard([&readers, &adapted_readers]() {
        CloseReaders(readers);
        CloseReaders(adapted_readers);
    });
    for (const std::unique_ptr<BatchReader>& reader : readers) {
        if (!reader) {
            return Status::Invalid("PK real-time store returned a null commit reader");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<const PreparedReaderPlan> plan,
        PreparedReaderPlan::Create(prepared_schema, key_schema, value_schema, memory_pool,
                                   /*exact_commit_schema=*/true));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeOffsetCoverage> offset_coverage,
                           RealtimeOffsetCoverage::Create(sealed_offsets, readers.size()));
    adapted_readers.reserve(readers.size());
    for (std::unique_ptr<BatchReader>& reader : readers) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<KeyValueRecordReader> adapted_reader,
                               AdaptPreparedBatchReaderImpl(std::move(reader), plan, std::nullopt,
                                                            memory_pool, offset_coverage));
        adapted_readers.push_back(std::move(adapted_reader));
    }
    readers_guard.Release();
    return adapted_readers;
}

}  // namespace paimon

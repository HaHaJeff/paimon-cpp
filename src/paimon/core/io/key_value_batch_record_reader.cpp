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

#include "paimon/core/io/key_value_batch_record_reader.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/key_value.h"
#include "paimon/reader/batch_reader.h"

namespace paimon {

class KeyValueBatchRecordReader::Iterator : public KeyValueRecordReader::Iterator {
 public:
    explicit Iterator(KeyValueBatchRecordReader* reader) : reader_(reader) {}

    Result<bool> HasNext() const override {
        return cursor_ < reader_->values_->length();
    }

    Result<KeyValue> Next() override {
        if (reader_->sequences_->IsNull(cursor_) || reader_->row_kinds_->IsNull(cursor_)) {
            return Status::Invalid("PK merge metadata must not be null");
        }
        PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                               RowKind::FromByteValue(reader_->row_kinds_->Value(cursor_)));
        int64_t sequence = reader_->sequences_->Value(cursor_) + reader_->sequence_shift_;
        std::shared_ptr<InternalRow> key =
            std::make_shared<ColumnarRowRef>(reader_->key_context_, cursor_);
        auto value = std::make_unique<ColumnarRowRef>(reader_->value_context_, cursor_++);
        return KeyValue(row_kind, sequence, KeyValue::UNKNOWN_LEVEL, std::move(key),
                        std::move(value));
    }

 private:
    KeyValueBatchRecordReader* reader_;
    int64_t cursor_ = 0;
};

KeyValueBatchRecordReader::KeyValueBatchRecordReader(
    std::unique_ptr<BatchReader>&& reader, int64_t sequence_shift,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema, const std::shared_ptr<MemoryPool>& pool)
    : reader_(std::move(reader)),
      sequence_shift_(sequence_shift),
      key_schema_(key_schema),
      value_schema_(value_schema),
      pool_(pool) {}

Result<std::unique_ptr<KeyValueRecordReader::Iterator>> KeyValueBatchRecordReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader_->NextBatch());
    if (BatchReader::IsEofBatch(batch)) {
        return std::unique_ptr<KeyValueRecordReader::Iterator>();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                      arrow::ImportArray(batch.first.get(), batch.second.get()));
    std::shared_ptr<arrow::StructArray> input =
        std::dynamic_pointer_cast<arrow::StructArray>(imported);
    if (!input) {
        return Status::Invalid("PK merge input is not a StructArray");
    }
    sequences_ = std::dynamic_pointer_cast<arrow::Int64Array>(
        input->GetFieldByName(SpecialFields::SequenceNumber().Name()));
    row_kinds_ = std::dynamic_pointer_cast<arrow::Int8Array>(
        input->GetFieldByName(SpecialFields::ValueKind().Name()));
    if (!sequences_ || !row_kinds_) {
        return Status::Invalid("PK merge input is missing sequence or value-kind metadata");
    }
    PAIMON_ASSIGN_OR_RAISE(input, ArrowUtils::RemoveFieldFromStructArray(
                                      input, SpecialFields::SequenceNumber().Name()));
    PAIMON_ASSIGN_OR_RAISE(
        values_, ArrowUtils::RemoveFieldFromStructArray(input, SpecialFields::ValueKind().Name()));
    if (!ArrowUtils::EqualsIgnoreNullable(values_->type(),
                                          arrow::struct_(value_schema_->fields()))) {
        return Status::Invalid("PK merge input value schema does not match the table read schema");
    }
    arrow::ArrayVector key_fields;
    for (const std::shared_ptr<arrow::Field>& field : key_schema_->fields()) {
        std::shared_ptr<arrow::Array> key = values_->GetFieldByName(field->name());
        if (!key) {
            return Status::Invalid("PK merge input is missing key field ", field->name());
        }
        key_fields.push_back(std::move(key));
    }
    key_context_ = std::make_shared<ColumnarBatchContext>(key_fields, pool_);
    value_context_ = std::make_shared<ColumnarBatchContext>(values_->fields(), pool_);
    return std::make_unique<Iterator>(this);
}

std::shared_ptr<Metrics> KeyValueBatchRecordReader::GetReaderMetrics() const {
    return reader_->GetReaderMetrics();
}

void KeyValueBatchRecordReader::Close() {
    values_.reset();
    sequences_.reset();
    row_kinds_.reset();
    key_context_.reset();
    value_context_.reset();
    reader_->Close();
}

}  // namespace paimon

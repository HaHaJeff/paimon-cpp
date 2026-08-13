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

#pragma once

#include <cstdint>
#include <memory>

#include "arrow/type_fwd.h"
#include "paimon/core/io/key_value_record_reader.h"

namespace arrow {
class StructArray;
}  // namespace arrow

namespace paimon {

class BatchReader;
class MemoryPool;
struct ColumnarBatchContext;

/// Adapts sorted Arrow batches with PK merge metadata to a KeyValueRecordReader.
class KeyValueBatchRecordReader : public KeyValueRecordReader {
 public:
    KeyValueBatchRecordReader(std::unique_ptr<BatchReader>&& reader, int64_t sequence_shift,
                              const std::shared_ptr<arrow::Schema>& key_schema,
                              const std::shared_ptr<arrow::Schema>& value_schema,
                              const std::shared_ptr<MemoryPool>& pool);

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;

 private:
    class Iterator;

    std::unique_ptr<BatchReader> reader_;
    int64_t sequence_shift_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::StructArray> values_;
    std::shared_ptr<arrow::Int64Array> sequences_;
    std::shared_ptr<arrow::Int8Array> row_kinds_;
    std::shared_ptr<ColumnarBatchContext> key_context_;
    std::shared_ptr<ColumnarBatchContext> value_context_;
};

}  // namespace paimon

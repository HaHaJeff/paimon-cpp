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

#include "paimon/core/realtime/primary_key_merge_input.h"

#include <limits>
#include <optional>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/key_value_batch_record_reader.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/realtime/primary_key_mem_indexer.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/realtime/mem_indexer.h"

namespace paimon {

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> PrimaryKeyMergeInput::Create(
    const std::shared_ptr<RealtimeSplit>& split, const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<InternalReadContext>& context, const std::shared_ptr<MemoryPool>& pool) {
    std::vector<std::shared_ptr<DataFileMeta>> disk_files;
    for (const std::shared_ptr<Split>& disk_split : split->DiskSplits()) {
        std::shared_ptr<DataSplitImpl> data_split =
            std::dynamic_pointer_cast<DataSplitImpl>(disk_split);
        if (!data_split) {
            return Status::Invalid("PK real-time disk split is not a data split");
        }
        disk_files.insert(disk_files.end(), data_split->DataFiles().begin(),
                          data_split->DataFiles().end());
    }
    const int64_t disk_max_sequence = DataFileMeta::GetMaxSequenceNumber(disk_files);
    arrow::FieldVector memory_fields = value_schema->fields();
    for (const std::shared_ptr<arrow::Field>& key_field : key_schema->fields()) {
        if (value_schema->GetFieldIndex(key_field->name()) < 0) {
            memory_fields.push_back(key_field);
        }
    }
    std::shared_ptr<arrow::Schema> memory_schema = arrow::schema(std::move(memory_fields));
    auto c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*memory_schema, c_schema.get()));
    ScopeGuard schema_guard([schema = c_schema.get()]() { ArrowSchemaRelease(schema); });
    MemQueryContext query_context{c_schema.get(), context->GetPredicate(), false};
    std::shared_ptr<PrimaryKeyMemIndexer> indexer =
        std::dynamic_pointer_cast<PrimaryKeyMemIndexer>(split->Indexer());
    if (!indexer) {
        return Status::Invalid("PK real-time split does not contain a primary-key mem indexer");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<Range> sequence_range,
        indexer->GetPrimaryKeyQuerySequenceRange(split->ReadView(), split->CommittedOffset()));
    if (!sequence_range) {
        return std::vector<std::unique_ptr<KeyValueRecordReader>>();
    }
    if (sequence_range->from < 0 || sequence_range->to < sequence_range->from ||
        disk_max_sequence == std::numeric_limits<int64_t>::max()) {
        return Status::Invalid("PK real-time query sequence range is invalid");
    }
    const int64_t target_first = disk_max_sequence + 1;
    if (sequence_range->to - sequence_range->from >
        std::numeric_limits<int64_t>::max() - target_first) {
        return Status::Invalid("PK real-time query sequence range exceeds INT64_MAX");
    }
    const int64_t shift = target_first - sequence_range->from;
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> batch_readers,
                           indexer->CreatePrimaryKeyQueryReaders(
                               split->ReadView(), split->CommittedOffset(), query_context));
    std::vector<std::unique_ptr<KeyValueRecordReader>> result;
    result.reserve(batch_readers.size());
    for (std::unique_ptr<BatchReader>& reader : batch_readers) {
        if (!reader) {
            return Status::Invalid("PK mem indexer returned a null merge reader");
        }
        result.push_back(std::make_unique<KeyValueBatchRecordReader>(
            std::move(reader), shift, key_schema, memory_schema, pool));
    }
    return result;
}

}  // namespace paimon

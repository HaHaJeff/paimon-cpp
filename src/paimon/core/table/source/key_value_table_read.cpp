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

#include "paimon/core/table/source/key_value_table_read.h"

#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/key_value_batch_record_reader.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/operation/merge_file_split_read.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/realtime/primary_key_mem_indexer.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/pk_count_reader.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/status.h"

namespace paimon {
class DataSplit;
class Executor;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;

namespace {

Result<std::vector<AdditionalKeyValueReader>> CreatePrimaryKeyMemoryReaders(
    const std::shared_ptr<RealtimeSplit>& split, const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    arrow::FieldVector memory_fields = value_schema->fields();
    for (const std::shared_ptr<arrow::Field>& key_field : key_schema->fields()) {
        if (value_schema->GetFieldIndex(key_field->name()) < 0) {
            memory_fields.push_back(key_field);
        }
    }
    std::shared_ptr<arrow::Schema> memory_schema = arrow::schema(std::move(memory_fields));
    arrow::FieldVector requested_fields = {
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())};
    requested_fields.insert(requested_fields.end(), memory_schema->fields().begin(),
                            memory_schema->fields().end());
    auto c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportSchema(*arrow::schema(std::move(requested_fields)), c_schema.get()));
    ScopeGuard schema_guard([schema = c_schema.get()]() { ArrowSchemaRelease(schema); });
    MemQueryContext query_context{c_schema.get(), context->GetPredicate(), false};
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> batch_readers,
                           split->Indexer()->CreateQueryReaders(
                               split->ReadView(), split->CommittedOffset(), query_context));
    std::vector<AdditionalKeyValueReader> result;
    result.reserve(batch_readers.size());
    for (std::unique_ptr<BatchReader>& reader : batch_readers) {
        std::shared_ptr<InternalRow> min_key;
        std::shared_ptr<InternalRow> max_key;
        if (auto* range_provider = dynamic_cast<PrimaryKeyRangeProvider*>(reader.get())) {
            min_key = range_provider->GetMinKey();
            max_key = range_provider->GetMaxKey();
        }
        auto key_value_reader = std::make_unique<KeyValueBatchRecordReader>(
            std::move(reader), key_schema, memory_schema, memory_pool);
        result.push_back(AdditionalKeyValueReader{std::move(key_value_reader), min_key, max_key});
    }
    return result;
}

}  // namespace

KeyValueTableRead::KeyValueTableRead(std::vector<std::unique_ptr<SplitRead>>&& split_reads,
                                     std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
                                     const std::shared_ptr<FileStorePathFactory>& path_factory,
                                     const std::shared_ptr<InternalReadContext>& context,
                                     const std::shared_ptr<MemoryPool>& memory_pool,
                                     const std::shared_ptr<Executor>& executor)
    : TableRead(memory_pool),
      split_reads_(std::move(split_reads)),
      merge_file_split_read_(std::move(merge_file_split_read)),
      path_factory_(path_factory),
      context_(context),
      executor_(executor) {}

KeyValueTableRead::~KeyValueTableRead() = default;

Result<std::unique_ptr<TableRead>> KeyValueTableRead::Create(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor) {
    auto raw_file_split_read =
        std::make_unique<RawFileSplitRead>(path_factory, context, memory_pool, executor);
    std::vector<std::unique_ptr<SplitRead>> split_reads;
    split_reads.emplace_back(std::move(raw_file_split_read));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<MergeFileSplitRead> merge_file_split_read,
        MergeFileSplitRead::Create(path_factory, context, memory_pool, executor));

    return std::unique_ptr<TableRead>(
        new KeyValueTableRead(std::move(split_reads), std::move(merge_file_split_read),
                              path_factory, context, memory_pool, executor));
}

void KeyValueTableRead::ForceKeepDelete(bool force_keep_delete) {
    force_keep_delete_ = force_keep_delete;
    merge_file_split_read_->ForceKeepDelete(force_keep_delete);
}

Result<std::unique_ptr<BatchReader>> KeyValueTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    std::shared_ptr<RealtimeSplit> realtime_split = std::dynamic_pointer_cast<RealtimeSplit>(split);
    if (realtime_split) {
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<AdditionalKeyValueReader> memory_readers,
            CreatePrimaryKeyMemoryReaders(realtime_split, merge_file_split_read_->GetKeySchema(),
                                          merge_file_split_read_->GetValueSchema(), context_,
                                          GetMemoryPool()));
        return merge_file_split_read_->CreateReader(realtime_split->DiskSplits(),
                                                    std::move(memory_readers));
    }
    auto data_split = std::dynamic_pointer_cast<DataSplit>(split);
    if (!data_split) {
        return Status::Invalid("split cannot be casted to DataSplit");
    }
    for (const auto& read : split_reads_) {
        PAIMON_ASSIGN_OR_RAISE(bool matched, read->Match(data_split, force_keep_delete_));
        if (matched) {
            return read->CreateReader(data_split);
        }
    }
    PAIMON_ASSIGN_OR_RAISE(bool matched,
                           merge_file_split_read_->Match(data_split, force_keep_delete_));
    if (matched) {
        return merge_file_split_read_->CreateReader(data_split);
    }
    return Status::Invalid("create reader failed, not read match with data split.");
}

Result<std::unique_ptr<CountReader>> KeyValueTableRead::CreateCountReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    for (const std::shared_ptr<Split>& split : splits) {
        if (std::dynamic_pointer_cast<RealtimeSplit>(split)) {
            return Status::NotImplemented(
                "CreateCountReader does not support process-local real-time splits");
        }
    }
    if (context_->GetPredicate() != nullptr) {
        return Status::NotImplemented(
            "CreateCountReader with predicate pushdown is not supported yet");
    }

    if (force_keep_delete_) {
        return Status::NotImplemented("CreateCountReader with force_keep_delete is not supported");
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<PKCountReader> pk_count_reader,
        PKCountReader::Create(splits, path_factory_, context_, GetMemoryPool(), executor_));

    return pk_count_reader;
}

}  // namespace paimon

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

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/key_value.h"
#include "paimon/core/operation/merge_file_split_read.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/realtime/primary_key_realtime_store.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_reader.h"
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
struct ColumnarBatchContext;

namespace {

class QueryBatchKeyValueReader final : public KeyValueRecordReader {
 public:
    QueryBatchKeyValueReader(std::unique_ptr<BatchReader>&& reader,
                             const std::shared_ptr<arrow::Schema>& key_schema,
                             const std::shared_ptr<arrow::Schema>& value_schema,
                             const std::shared_ptr<MemoryPool>& pool)
        : reader_(std::move(reader)),
          key_schema_(key_schema),
          value_schema_(value_schema),
          pool_(pool) {}

    ~QueryBatchKeyValueReader() override {
        Close();
    }

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;

 private:
    class Iterator;

    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::StructArray> values_;
    std::shared_ptr<arrow::Int64Array> sequences_;
    std::shared_ptr<arrow::Int8Array> row_kinds_;
    std::shared_ptr<ColumnarBatchContext> key_context_;
    std::shared_ptr<ColumnarBatchContext> value_context_;
    bool closed_ = false;
};

class QueryBatchKeyValueReader::Iterator final : public KeyValueRecordReader::Iterator {
 public:
    explicit Iterator(QueryBatchKeyValueReader* reader) : reader_(reader) {}

    Result<bool> HasNext() const override {
        return cursor_ < reader_->values_->length();
    }

    Result<KeyValue> Next() override {
        if (reader_->sequences_->IsNull(cursor_) || reader_->row_kinds_->IsNull(cursor_)) {
            return Status::Invalid("PK merge metadata must not be null");
        }
        PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                               RowKind::FromByteValue(reader_->row_kinds_->Value(cursor_)));
        const int64_t sequence = reader_->sequences_->Value(cursor_);
        std::shared_ptr<InternalRow> key =
            std::make_shared<ColumnarRowRef>(reader_->key_context_, cursor_);
        auto value = std::make_unique<ColumnarRowRef>(reader_->value_context_, cursor_++);
        return KeyValue(row_kind, sequence, KeyValue::UNKNOWN_LEVEL, std::move(key),
                        std::move(value));
    }

 private:
    QueryBatchKeyValueReader* reader_;
    int64_t cursor_ = 0;
};

Result<std::unique_ptr<KeyValueRecordReader::Iterator>> QueryBatchKeyValueReader::NextBatch() {
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
    key_fields.reserve(key_schema_->num_fields());
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

std::shared_ptr<Metrics> QueryBatchKeyValueReader::GetReaderMetrics() const {
    return reader_->GetReaderMetrics();
}

void QueryBatchKeyValueReader::Close() {
    if (closed_) {
        return;
    }
    closed_ = true;
    values_.reset();
    sequences_.reset();
    row_kinds_.reset();
    key_context_.reset();
    value_context_.reset();
    if (reader_) {
        reader_->Close();
    }
}

Result<std::vector<AdditionalKeyValueReader>> CreateMemoryReaders(
    const std::shared_ptr<RealtimeSplit>& split, const RealtimePartitionBucketView& memory,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    arrow::FieldVector requested_fields = {
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())};
    requested_fields.insert(requested_fields.end(), value_schema->fields().begin(),
                            value_schema->fields().end());
    auto c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportSchema(*arrow::schema(requested_fields), c_schema.get()));
    ScopeGuard schema_guard([schema = c_schema.get()]() { ArrowSchemaRelease(schema); });
    RealtimeQueryContext query_context{c_schema.get(), /*predicate=*/nullptr,
                                       /*enable_predicate_pushdown=*/false};
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> batch_readers,
                           memory.store->CreateQueryReaders(
                               memory.read_view, split->CommittedEndOffset(), query_context));
    ScopeGuard reader_guard([&batch_readers]() {
        for (const std::unique_ptr<BatchReader>& reader : batch_readers) {
            if (reader) {
                reader->Close();
            }
        }
    });
    if (batch_readers.empty()) {
        return Status::Invalid("PK real-time store returned no query readers for active memory");
    }
    std::vector<AdditionalKeyValueReader> result;
    result.reserve(batch_readers.size());
    for (std::unique_ptr<BatchReader>& reader : batch_readers) {
        if (!reader) {
            return Status::Invalid("PK real-time store returned a null query reader");
        }
        std::shared_ptr<InternalRow> min_key;
        std::shared_ptr<InternalRow> max_key;
        if (auto* provider = dynamic_cast<PrimaryKeyRangeProvider*>(reader.get())) {
            min_key = provider->GetMinKey();
            max_key = provider->GetMaxKey();
        }
        result.push_back(
            AdditionalKeyValueReader{std::make_unique<QueryBatchKeyValueReader>(
                                         std::move(reader), key_schema, value_schema, memory_pool),
                                     std::move(min_key), std::move(max_key)});
    }
    return result;
}

}  // namespace

KeyValueTableRead::KeyValueTableRead(std::vector<std::unique_ptr<SplitRead>>&& split_reads,
                                     const std::shared_ptr<FileStorePathFactory>& path_factory,
                                     const std::shared_ptr<InternalReadContext>& context,
                                     const std::shared_ptr<MemoryPool>& memory_pool,
                                     const std::shared_ptr<Executor>& executor)
    : TableRead(memory_pool),
      split_reads_(std::move(split_reads)),
      path_factory_(path_factory),
      context_(context),
      executor_(executor) {}

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
    split_reads.emplace_back(std::move(merge_file_split_read));

    return std::unique_ptr<TableRead>(new KeyValueTableRead(std::move(split_reads), path_factory,
                                                            context, memory_pool, executor));
}

void KeyValueTableRead::ForceKeepDelete(bool force_keep_delete) {
    force_keep_delete_ = force_keep_delete;
    for (const auto& read : split_reads_) {
        auto* merge_read = dynamic_cast<MergeFileSplitRead*>(read.get());
        if (merge_read != nullptr) {
            merge_read->ForceKeepDelete(force_keep_delete);
        }
    }
}

Result<std::unique_ptr<BatchReader>> KeyValueTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    std::shared_ptr<RealtimeSplit> realtime_split = std::dynamic_pointer_cast<RealtimeSplit>(split);
    if (realtime_split) {
        return CreateRealtimeReader(realtime_split, /*release_ticket=*/true);
    }

    std::shared_ptr<Split> dispatch_split = split;
    if (auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(indexed_split->Validate());
        if (!indexed_split->Scores().empty()) {
            // TODO(wangyong9999): Propagate indexed scores through the primary-key
            // physical-position read path.
            return Status::NotImplemented(
                "Primary-key reads do not support scored indexed splits yet.");
        }
        // Primary-key indexed splits carry physical positions and are routed independently
        // of the inner split's raw-convertible marker, matching Java's dedicated provider.
        const std::shared_ptr<DataSplit>& inner_split = indexed_split->GetDataSplit();
        if (!force_keep_delete_) {
            bool has_raw_reader = false;
            for (const auto& read : split_reads_) {
                if (dynamic_cast<RawFileSplitRead*>(read.get()) != nullptr) {
                    has_raw_reader = true;
                    PAIMON_ASSIGN_OR_RAISE(bool matched,
                                           read->Match(indexed_split, /*force_keep_delete=*/false));
                    if (matched) {
                        return read->CreateReader(indexed_split);
                    }
                    // A manually supplied or deserialized indexed split can still reference
                    // legacy files. Preserve merge semantics when raw-read safety is uncertain.
                    dispatch_split = inner_split;
                    break;
                }
            }
            if (!has_raw_reader) {
                return Status::Invalid(
                    "create reader failed, primary-key indexed split has no raw reader.");
            }
        } else {
            // Keeping delete rows is incompatible with physical-position pruning. Reading the
            // inner split through the normal merge path preserves correctness.
            dispatch_split = inner_split;
        }
    }
    auto data_split = std::dynamic_pointer_cast<DataSplit>(dispatch_split);
    if (!data_split) {
        return Status::Invalid("split cannot be casted to DataSplit");
    }
    for (const auto& read : split_reads_) {
        PAIMON_ASSIGN_OR_RAISE(bool matched, read->Match(data_split, force_keep_delete_));
        if (matched) {
            return read->CreateReader(data_split);
        }
    }
    return Status::Invalid("create reader failed, not read match with data split.");
}

Result<std::unique_ptr<BatchReader>> KeyValueTableRead::CreateReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.reserve(splits.size());
    std::vector<std::shared_ptr<RealtimeSplit>> realtime_splits;
    for (const std::shared_ptr<Split>& split : splits) {
        std::shared_ptr<RealtimeSplit> realtime_split =
            std::dynamic_pointer_cast<RealtimeSplit>(split);
        if (realtime_split) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                                   CreateRealtimeReader(realtime_split, /*release_ticket=*/false));
            readers.push_back(std::move(reader));
            realtime_splits.push_back(std::move(realtime_split));
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, CreateReader(split));
            readers.push_back(std::move(reader));
        }
    }

    std::unique_ptr<BatchReader> result =
        std::make_unique<ConcatBatchReader>(std::move(readers), GetMemoryPool());
    if (!realtime_splits.empty()) {
        const std::shared_ptr<RealtimeContext> realtime_context = context_->GetRealtimeContext();
        if (!realtime_context) {
            return Status::Invalid("reading a real-time split requires a real-time context");
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                               RealtimeContextImpl::Cast(realtime_context));
        for (const std::shared_ptr<RealtimeSplit>& realtime_split : realtime_splits) {
            PAIMON_RETURN_NOT_OK(
                realtime_context_impl->ReleaseReadView(realtime_split->OpaqueTicket()));
        }
    }
    return result;
}

Result<std::unique_ptr<BatchReader>> KeyValueTableRead::CreateRealtimeReader(
    const std::shared_ptr<RealtimeSplit>& realtime_split, bool release_ticket) {
    if (realtime_split->Version() != RealtimeSplit::kCurrentVersion) {
        return Status::Invalid("unsupported real-time split version");
    }
    const std::shared_ptr<RealtimeContext> realtime_context = context_->GetRealtimeContext();
    if (!realtime_context) {
        return Status::Invalid("reading a real-time split requires a real-time context");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                           RealtimeContextImpl::Cast(realtime_context));
    PAIMON_ASSIGN_OR_RAISE(RealtimePartitionBucketView memory,
                           realtime_context_impl->ResolveReadView(realtime_split->OpaqueTicket()));
    const RealtimePartitionBucket expected_partition_bucket(realtime_split->Partition(),
                                                            realtime_split->Bucket());
    if (memory.partition_bucket != expected_partition_bucket) {
        return Status::Invalid("real-time read-view ticket belongs to another partition-bucket");
    }
    const std::optional<OffsetRange> memory_range = memory.read_view->GetOffsetRange();
    if (!memory_range || memory_range->end != realtime_split->MemoryEndOffset()) {
        return Status::Invalid("real-time read-view ticket does not match the split offset range");
    }
    for (const std::unique_ptr<SplitRead>& read : split_reads_) {
        auto* merge_read = dynamic_cast<MergeFileSplitRead*>(read.get());
        if (merge_read) {
            PAIMON_ASSIGN_OR_RAISE(
                std::vector<AdditionalKeyValueReader> memory_readers,
                CreateMemoryReaders(realtime_split, memory, merge_read->GetKeySchema(),
                                    merge_read->GetValueSchema(), context_, GetMemoryPool()));
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                                   merge_read->CreateRealtimeReader(realtime_split->DiskSplits(),
                                                                    std::move(memory_readers)));
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeReader> realtime_reader,
                                   RealtimeReader::Create(memory.read_view, std::move(reader)));
            if (release_ticket) {
                PAIMON_RETURN_NOT_OK(
                    realtime_context_impl->ReleaseReadView(realtime_split->OpaqueTicket()));
            }
            return std::unique_ptr<BatchReader>(std::move(realtime_reader));
        }
    }
    return Status::Invalid("create reader failed, merge file split read not found");
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

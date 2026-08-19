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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/realtime/arrow_mem_indexer_factory.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class DelegatingMemIndexer : public MemIndexer {
 public:
    explicit DelegatingMemIndexer(std::shared_ptr<MemIndexer> delegate)
        : delegate_(std::move(delegate)) {}

    Status Write(RealtimeWriteBatch&& batch) override {
        return delegate_->Write(std::move(batch));
    }

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override {
        return delegate_->SealForCommit();
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) override {
        return delegate_->CreateCommitReaders(segment);
    }

    Result<std::shared_ptr<MemReadView>> AcquireReadView() override {
        return delegate_->AcquireReadView();
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<MemReadView>& view, int64_t offset_lower_exclusive,
        const MemQueryContext& context) override {
        return delegate_->CreateQueryReaders(view, offset_lower_exclusive, context);
    }

    Status AdvanceCommittedOffset(int64_t committed_offset) override {
        return delegate_->AdvanceCommittedOffset(committed_offset);
    }

    uint64_t GetMemoryUsage() const override {
        return delegate_->GetMemoryUsage();
    }

 private:
    std::shared_ptr<MemIndexer> delegate_;
};

class DelegatingMemIndexerFactory : public MemIndexerFactory {
 public:
    struct CapturedRequest {
        std::map<std::string, std::string> partition;
        int32_t bucket;
        MemIndexerCreateConfig mode_config;
    };

    Result<std::shared_ptr<MemIndexer>> Create(MemIndexerCreateRequest&& request) override {
        ++create_count;
        requests.push_back(CapturedRequest{request.partition, request.bucket, request.mode_config});
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemIndexer> delegate,
                               delegate_.Create(std::move(request)));
        return std::shared_ptr<MemIndexer>(
            std::make_shared<DelegatingMemIndexer>(std::move(delegate)));
    }

    int32_t create_count = 0;
    std::vector<CapturedRequest> requests;

 private:
    ArrowMemIndexerFactory delegate_;
};

class ConcurrentTestState {
 public:
    void WaitForStart() {
        ready_threads_.fetch_add(1, std::memory_order_release);
        while (!start_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    void StartWhenReady(int32_t worker_count) {
        while (ready_threads_.load(std::memory_order_acquire) < worker_count) {
            std::this_thread::yield();
        }
        start_.store(true, std::memory_order_release);
    }

    void RecordError(const Status& status) {
        RecordError(status.ToString());
    }

    void RecordError(std::string error) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            errors_.push_back(std::move(error));
        }
        stop_.store(true, std::memory_order_release);
        progress_cv.notify_all();
        snapshot_cv.notify_all();
    }

    bool RecordErrorIfNotOk(const Status& status) {
        if (status.ok()) {
            return false;
        }
        RecordError(status);
        return true;
    }

    template <typename T>
    bool RecordErrorIfNotOk(const Result<T>& result) {
        if (result.ok()) {
            return false;
        }
        RecordError(result.status());
        return true;
    }

    bool ShouldStop() const {
        return stop_.load(std::memory_order_acquire);
    }

    const std::vector<std::string>& Errors() const {
        return errors_;
    }

    std::mutex mutex;
    std::condition_variable progress_cv;
    std::condition_variable snapshot_cv;

 private:
    std::atomic<bool> start_{false};
    std::atomic<bool> stop_{false};
    std::atomic<int32_t> ready_threads_{0};
    std::vector<std::string> errors_;
};

class RealtimeWriteInteTest : public ::testing::Test {
 protected:
    using Row = std::tuple<int64_t, std::string, std::string>;

    struct CollectedReadResult {
        std::unique_ptr<BatchReader> reader;
        std::shared_ptr<arrow::ChunkedArray> data;
    };

    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create("local");
        ASSERT_NE(nullptr, dir_);
        table_path_ = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
        fields_ = {arrow::field("id", arrow::int64()), arrow::field("payload", arrow::utf8()),
                   arrow::field("pt", arrow::utf8())};
        schema_ = arrow::schema(fields_);
        options_ = {
            {Options::MANIFEST_FORMAT, "orc"}, {Options::FILE_FORMAT, "orc"},
            {Options::FILE_SYSTEM, "local"},   {Options::BUCKET, "1"},
            {Options::BUCKET_KEY, "id"},       {Options::TARGET_FILE_SIZE, "1048576"},
        };
    }

    void TearDown() override {
        dir_.reset();
    }

    void CreateTable(const std::vector<std::string>& partition_keys) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*schema_, c_schema.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog,
                             Catalog::Create(dir_->Str(), options_));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), c_schema.get(), partition_keys,
                                       /*primary_keys=*/{}, options_,
                                       /*ignore_if_exists=*/false));
    }

    void CreatePkTable(const std::vector<std::string>& partition_keys = {}) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*schema_, c_schema.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog,
                             Catalog::Create(dir_->Str(), options_));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        const std::vector<std::string> primary_keys = partition_keys.empty()
                                                          ? std::vector<std::string>{"id"}
                                                          : std::vector<std::string>{"id", "pt"};
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), c_schema.get(), partition_keys,
                                       primary_keys, options_, /*ignore_if_exists=*/false));
    }

    Result<std::unique_ptr<FileStoreWrite>> CreateRealtimeWriter(
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::vector<std::string>& write_schema = {},
        int32_t write_buffer_spill_thread_number = 0) const {
        WriteContextBuilder builder(table_path_, commit_user_);
        builder.SetOptions(options_)
            .WithStreamingMode(true)
            .WithRealtimeContext(realtime_context)
            .WithTempDirectory(PathUtil::JoinPath(dir_->Str(), "spill"));
        if (!write_schema.empty()) {
            builder.WithWriteSchema(write_schema);
        }
        if (write_buffer_spill_thread_number > 0) {
            builder.SetWriteBufferSpillThreadNumber(write_buffer_spill_thread_number);
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> context, builder.Finish());
        return FileStoreWrite::Create(std::move(context));
    }

    Result<std::unique_ptr<FileStoreWrite>> CreateRealtimeWriter() const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContext> realtime_context,
                               RealtimeContext::Create());
        return CreateRealtimeWriter(realtime_context);
    }

    Result<std::unique_ptr<RecordBatch>> MakeBatch(const std::vector<Row>& rows,
                                                   bool partitioned) const {
        return MakeBatch(rows, partitioned, /*bucket=*/0);
    }

    Result<std::unique_ptr<RecordBatch>> MakeBatch(const std::vector<Row>& rows, bool partitioned,
                                                   int32_t bucket) const {
        return MakeBatch(rows, partitioned, bucket, {});
    }

    Result<std::unique_ptr<RecordBatch>> MakeBatch(
        const std::vector<Row>& rows, bool partitioned, int32_t bucket,
        const std::vector<RecordBatch::RowKind>& row_kinds) const {
        if (rows.empty()) {
            return Status::Invalid("cannot create an empty test batch");
        }
        const std::string& partition = std::get<2>(rows.front());
        std::string json = "[";
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto& [id, payload, pt] = rows[i];
            if (pt != partition) {
                return Status::Invalid("one test batch must contain only one partition");
            }
            if (i > 0) {
                json += ",";
            }
            json += "[" + std::to_string(id) + ",\"" + payload + "\",\"" + pt + "\"]";
        }
        json += "]";

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), json));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        RecordBatchBuilder builder(&c_array);
        builder.SetRowKinds(row_kinds);
        if (partitioned) {
            builder.SetPartition({{"pt", partition}});
        }
        return builder.SetBucket(bucket).Finish();
    }

    static std::vector<Row> MakeRows(int64_t first_id, int64_t count,
                                     const std::string& partition) {
        std::vector<Row> rows;
        rows.reserve(count);
        for (int64_t i = 0; i < count; ++i) {
            int64_t id = first_id + i;
            rows.emplace_back(id, "value-" + std::to_string(id), partition);
        }
        return rows;
    }

    Result<int64_t> Commit(const std::vector<RealtimeCommitProgress>& realtime_commits,
                           int64_t commit_identifier) const {
        CommitContextBuilder builder(table_path_, commit_user_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->CommitWithProgress(realtime_commits, commit_identifier,
                                          /*watermark=*/std::nullopt);
    }

    Status Commit(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                  int64_t commit_identifier = BATCH_WRITE_COMMIT_IDENTIFIER) const {
        CommitContextBuilder builder(table_path_, commit_user_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->Commit(commit_messages, commit_identifier);
    }

    static std::vector<std::shared_ptr<DataFileMeta>> NewFiles(
        const std::vector<RealtimeCommitProgress>& progress) {
        std::vector<std::shared_ptr<DataFileMeta>> files;
        for (const RealtimeCommitProgress& item : progress) {
            std::shared_ptr<CommitMessageImpl> message =
                std::dynamic_pointer_cast<CommitMessageImpl>(item.commit_message);
            if (!message) {
                continue;
            }
            const std::vector<std::shared_ptr<DataFileMeta>>& new_files =
                message->GetNewFilesIncrement().NewFiles();
            files.insert(files.end(), new_files.begin(), new_files.end());
        }
        return files;
    }

    Result<std::shared_ptr<Plan>> CreatePlan(
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<Predicate>& predicate) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan,
                               CreateScan(realtime_context, predicate));
        return scan->CreatePlan();
    }

    Result<std::unique_ptr<TableScan>> CreateScan(
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<Predicate>& predicate) const {
        ScanContextBuilder scan_builder(table_path_);
        if (realtime_context) {
            scan_builder.WithRealtimeContext(realtime_context);
        }
        scan_builder.SetPredicate(predicate).WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context,
                               scan_builder.SetOptions(options_).Finish());
        return TableScan::Create(std::move(scan_context));
    }

    Result<CollectedReadResult> ReadPlan(const std::shared_ptr<Plan>& plan,
                                         const std::vector<std::string>& read_fields,
                                         const std::shared_ptr<Predicate>& predicate,
                                         bool enable_predicate_filter) const {
        ReadContextBuilder read_builder(table_path_);
        read_builder.SetOptions(options_)
            .SetReadFieldNames(read_fields)
            .SetPredicate(predicate)
            .EnablePredicateFilter(enable_predicate_filter)
            .WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                               TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                               table_read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> result,
                               ReadResultCollector::CollectResult(reader.get()));
        return CollectedReadResult{std::move(reader), std::move(result)};
    }

    Result<std::vector<Row>> ReadRows(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan,
                               CreatePlan(realtime_context, /*predicate=*/nullptr));
        return ReadRows(plan);
    }

    Result<std::vector<Row>> ReadRows(const std::shared_ptr<Plan>& plan) const {
        PAIMON_ASSIGN_OR_RAISE(CollectedReadResult read_result,
                               ReadPlan(plan, {"id", "payload", "pt"}, /*predicate=*/nullptr,
                                        /*enable_predicate_filter=*/false));
        const std::shared_ptr<arrow::ChunkedArray>& result = read_result.data;

        std::vector<Row> rows;
        if (!result) {
            return rows;
        }
        for (const std::shared_ptr<arrow::Array>& chunk : result->chunks()) {
            std::shared_ptr<arrow::StructArray> data =
                std::dynamic_pointer_cast<arrow::StructArray>(chunk);
            if (!data || data->num_fields() != 4) {
                return Status::Invalid("unexpected real-time test read schema");
            }
            std::shared_ptr<arrow::Int8Array> row_kinds =
                std::dynamic_pointer_cast<arrow::Int8Array>(data->field(0));
            std::shared_ptr<arrow::Int64Array> ids =
                std::dynamic_pointer_cast<arrow::Int64Array>(data->field(1));
            std::shared_ptr<arrow::StringArray> payloads =
                std::dynamic_pointer_cast<arrow::StringArray>(data->field(2));
            std::shared_ptr<arrow::StringArray> partitions =
                std::dynamic_pointer_cast<arrow::StringArray>(data->field(3));
            if (!row_kinds || !ids || !payloads || !partitions) {
                return Status::Invalid("unexpected real-time test read field type");
            }
            for (int64_t i = 0; i < data->length(); ++i) {
                if (row_kinds->IsNull(i) ||
                    row_kinds->Value(i) != static_cast<int8_t>(RecordBatch::RowKind::INSERT) ||
                    ids->IsNull(i) || payloads->IsNull(i) || partitions->IsNull(i)) {
                    return Status::Invalid("unexpected null or row kind in real-time test result");
                }
                rows.emplace_back(ids->Value(i), payloads->GetString(i), partitions->GetString(i));
            }
        }
        return rows;
    }

    Result<std::vector<Row>> ReadRows() const {
        return ReadRows(std::shared_ptr<RealtimeContext>());
    }

    Result<uint64_t> GetRealtimeMemoryUsage(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::vector<RealtimePartitionBucketView> views,
                               realtime_context->AcquireReadViews());
        uint64_t memory_usage = 0;
        for (const RealtimePartitionBucketView& view : views) {
            memory_usage += view.indexer->GetMemoryUsage();
        }
        return memory_usage;
    }

    void VerifyPkSpillWriteRead(int32_t write_buffer_spill_thread_number) {
        options_[Options::WRITE_BUFFER_SIZE] = "1";
        options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
        options_[Options::WRITE_BUFFER_SPILL_MAX_DISK_SIZE] = "1b";
        CreatePkTable();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                             RealtimeContext::Create());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                             CreateRealtimeWriter(realtime_context, /*write_schema=*/{},
                                                  write_buffer_spill_thread_number));

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                             MakeBatch({Row{1, "old-1", "p0"}, Row{2, "keep", "p0"}},
                                       /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(disk_batch)));
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_progress,
                             writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
        ASSERT_EQ(1, disk_progress.size());
        ASSERT_EQ(Range(0, 1), disk_progress[0].offset_range);
        ASSERT_OK(Commit(disk_progress, /*commit_identifier=*/0));

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> memory_batch,
            MakeBatch({Row{1, "new-1", "p0"}, Row{3, "deleted", "p0"}, Row{4, "new-4", "p0"}},
                      /*partitioned=*/false, /*bucket=*/0,
                      {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE,
                       RecordBatch::RowKind::INSERT}));
        ASSERT_OK(writer->Write(std::move(memory_batch)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> pinned_plan,
                             CreatePlan(realtime_context, /*predicate=*/nullptr));
        std::vector<Row> expected = {{1, "new-1", "p0"}, {2, "keep", "p0"}, {4, "new-4", "p0"}};
        ASSERT_OK_AND_ASSIGN(std::vector<Row> memory_rows, ReadRows(realtime_context));
        ASSERT_EQ(expected, memory_rows);

        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> memory_progress,
                             writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
        ASSERT_EQ(1, memory_progress.size());
        ASSERT_EQ(Range(2, 4), memory_progress[0].offset_range);
        ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot,
                             Commit(memory_progress, /*commit_identifier=*/1));
        ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot));

        ASSERT_OK_AND_ASSIGN(std::vector<Row> pinned_rows, ReadRows(pinned_plan));
        ASSERT_EQ(expected, pinned_rows);
        ASSERT_OK_AND_ASSIGN(std::vector<Row> refreshed_rows, ReadRows(realtime_context));
        ASSERT_EQ(expected, refreshed_rows);
        ASSERT_OK(writer->Close());

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> restart_context,
                             RealtimeContext::Create());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> restart_writer,
                             CreateRealtimeWriter(restart_context, /*write_schema=*/{},
                                                  write_buffer_spill_thread_number));
        ASSERT_OK_AND_ASSIGN(std::vector<Row> reopened_rows, ReadRows(restart_context));
        ASSERT_EQ(expected, reopened_rows);
        ASSERT_OK(restart_writer->Close());
        ASSERT_OK_AND_ASSIGN(std::vector<Row> disk_rows, ReadRows());
        ASSERT_EQ(expected, disk_rows);
    }

    Result<std::vector<int64_t>> ReadPkQuerySequences(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::vector<RealtimePartitionBucketView> views,
                               realtime_context->AcquireReadViews());
        if (views.size() != 1) {
            return Status::Invalid("expected one PK real-time read view");
        }
        auto read_schema = std::make_unique<ArrowSchema>();
        arrow::FieldVector requested_fields = {
            DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())};
        requested_fields.insert(requested_fields.end(), schema_->fields().begin(),
                                schema_->fields().end());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportSchema(*arrow::schema(std::move(requested_fields)), read_schema.get()));
        ScopeGuard schema_guard([schema = read_schema.get()]() { ArrowSchemaRelease(schema); });
        MemQueryContext query_context{read_schema.get(), /*predicate=*/nullptr,
                                      /*enable_predicate_pushdown=*/false};
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::unique_ptr<BatchReader>> readers,
            views[0].indexer->CreateQueryReaders(views[0].read_view,
                                                 /*offset_lower_exclusive=*/-1, query_context));

        std::vector<int64_t> sequences;
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            while (true) {
                PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
                if (BatchReader::IsEofBatch(batch)) {
                    break;
                }
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> imported,
                    arrow::ImportArray(batch.first.get(), batch.second.get()));
                std::shared_ptr<arrow::StructArray> values =
                    std::dynamic_pointer_cast<arrow::StructArray>(imported);
                if (!values) {
                    return Status::Invalid("PK query reader did not return a StructArray");
                }
                std::shared_ptr<arrow::Int64Array> sequence_array =
                    std::dynamic_pointer_cast<arrow::Int64Array>(
                        values->GetFieldByName(SpecialFields::SequenceNumber().Name()));
                if (!sequence_array) {
                    return Status::Invalid("PK query reader did not return sequence numbers");
                }
                for (int64_t i = 0; i < sequence_array->length(); ++i) {
                    sequences.push_back(sequence_array->Value(i));
                }
            }
            reader->Close();
        }
        return sequences;
    }

    static Status ValidateReadPrefix(const std::vector<Row>& rows, int64_t total_rows) {
        std::vector<bool> seen(static_cast<size_t>(total_rows), false);
        int64_t max_id = -1;
        for (const Row& row : rows) {
            const auto& [id, payload, partition] = row;
            if (id < 0 || id >= total_rows) {
                return Status::Invalid("real-time read id is out of range");
            }
            if (seen[static_cast<size_t>(id)]) {
                return Status::Invalid("real-time read contains duplicate ids");
            }
            if (payload != "value-" + std::to_string(id) || partition != "p0") {
                return Status::Invalid("real-time read row does not match its id");
            }
            seen[static_cast<size_t>(id)] = true;
            max_id = std::max(max_id, id);
        }
        for (int64_t id = 0; id <= max_id; ++id) {
            if (!seen[static_cast<size_t>(id)]) {
                return Status::Invalid("real-time read contains an id gap");
            }
        }
        return Status::OK();
    }

    Result<RealtimeOffsetMap> ReadCommittedOffsets() const {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_));
        SnapshotManager snapshot_manager(options.GetFileSystem(), table_path_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, snapshot_manager.LatestSnapshot());
        return RealtimeCommitProperties::ReadOffsets(snapshot, options.GetFileSystem());
    }

    void FinalizeCommitAndCheck(FileStoreWrite* writer,
                                std::vector<RealtimeCommitProgress> realtime_commits,
                                int64_t prepare_identifier, std::vector<Row> expected_rows) const {
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> final_commits,
                             writer->PrepareCommitWithProgress(prepare_identifier));
        realtime_commits.insert(realtime_commits.end(),
                                std::make_move_iterator(final_commits.begin()),
                                std::make_move_iterator(final_commits.end()));
        ASSERT_OK(Commit(realtime_commits, prepare_identifier));
        ASSERT_OK(writer->Close());

        ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
        ASSERT_EQ(expected_rows, actual_rows);
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string table_path_;
    std::string commit_user_ = "realtime_commit_user";
    arrow::FieldVector fields_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(RealtimeWriteInteTest, TestAppendCommitAndRead) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/10, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    FinalizeCommitAndCheck(writer.get(), /*realtime_commits=*/{}, /*prepare_identifier=*/0, rows);
}

TEST_F(RealtimeWriteInteTest, TestRollingFilesPreserveProgress) {
    options_[Options::TARGET_FILE_ROW_NUM] = "10";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());

    std::vector<Row> expected_rows;
    constexpr int64_t kBatchCount = 3;
    constexpr int64_t kRowsPerBatch = 10;
    for (int64_t batch_index = 0; batch_index < kBatchCount; ++batch_index) {
        std::vector<Row> rows =
            MakeRows(batch_index * kRowsPerBatch, kRowsPerBatch, /*partition=*/"p0");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        expected_rows.insert(expected_rows.end(), rows.begin(), rows.end());
    }

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, commits.size());
    ASSERT_EQ(Range(0, kBatchCount * kRowsPerBatch - 1), commits[0].offset_range);
    std::shared_ptr<CommitMessageImpl> commit_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(commits[0].commit_message);
    ASSERT_NE(nullptr, commit_message);
    ASSERT_EQ(3, commit_message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_OK(Commit(commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
    ASSERT_EQ(expected_rows, actual_rows);
}

TEST_F(RealtimeWriteInteTest, TestCommitOrdersPreparedOffsetRanges) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());

    std::vector<Row> first_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, commits.size());
    ASSERT_EQ(Range(0, 2), commits[0].offset_range);

    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, second_commits.size());
    ASSERT_EQ(Range(3, 4), second_commits[0].offset_range);

    commits.push_back(std::move(second_commits[0]));
    std::reverse(commits.begin(), commits.end());
    ASSERT_OK(Commit(commits, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(4, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK(writer->Close());

    std::vector<Row> expected_rows = first_rows;
    expected_rows.insert(expected_rows.end(), second_rows.begin(), second_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
    ASSERT_EQ(expected_rows, actual_rows);
}

TEST_F(RealtimeWriteInteTest, TestReadMemoryBeforePrepareCommit) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/10, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(rows, actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestCloseWriterKeepsContextReadable) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(rows, actual_rows);
}

TEST_F(RealtimeWriteInteTest, TestPinnedPlanRemainsReadableAfterWriterClose) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(plan));
    ASSERT_EQ(rows, actual_rows);
}

TEST_F(RealtimeWriteInteTest, TestCloseWriterAllowsContextReuseByLaterWriter) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> first_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(first_writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, commits.size());
    ASSERT_EQ(Range(0, 2), commits[0].offset_range);
    ASSERT_OK(first_writer->Close());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(second_writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, second_commits.size());
    ASSERT_EQ(Range(3, 4), second_commits[0].offset_range);

    commits.push_back(std::move(second_commits[0]));
    ASSERT_OK(Commit(commits, /*commit_identifier=*/1));
    std::vector<Row> expected_rows = first_rows;
    expected_rows.insert(expected_rows.end(), second_rows.begin(), second_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, actual_rows);
    ASSERT_OK(second_writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkContextReuse) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = {{1, "one", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(first_writer->Write(std::move(batch)));
    ASSERT_OK(first_writer->Close());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(rows, actual_rows);

    ASSERT_NOK_WITH_MSG(CreateRealtimeWriter(realtime_context),
                        "already bound to a primary-key writer");
}

TEST_F(RealtimeWriteInteTest, TestReadCommittedDiskAndBuildingMemory) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, disk_commits.size());
    ASSERT_EQ(Range(0, 2), disk_commits[0].offset_range);

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> expected_rows = disk_rows;
    expected_rows.insert(expected_rows.end(), memory_rows.begin(), memory_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestProjectionAndPredicateForMemoryAndDisk) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::shared_ptr<Predicate> scan_predicate =
        PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT,
                                      Literal(static_cast<int64_t>(1)));
    std::shared_ptr<Predicate> read_predicate =
        PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"id", FieldType::BIGINT,
                                      Literal(static_cast<int64_t>(1)));
    const std::vector<std::string> read_fields = {"payload", "id"};
    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("payload", arrow::utf8()),
         arrow::field("id", arrow::int64())});

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> memory_plan,
                         CreatePlan(realtime_context, scan_predicate));
    ASSERT_OK_AND_ASSIGN(
        CollectedReadResult memory_result,
        ReadPlan(memory_plan, read_fields, read_predicate, /*enable_predicate_filter=*/true));
    std::shared_ptr<arrow::Array> expected_memory =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, "value-2", 2]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, memory_result.data);
    ASSERT_TRUE(
        std::make_shared<arrow::ChunkedArray>(expected_memory)->Equals(*memory_result.data));

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));
    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> union_plan,
                         CreatePlan(realtime_context, scan_predicate));
    ASSERT_OK_AND_ASSIGN(
        CollectedReadResult union_result,
        ReadPlan(union_plan, read_fields, read_predicate, /*enable_predicate_filter=*/true));
    std::shared_ptr<arrow::Array> expected_union =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, "value-2", 2],
            [0, "value-3", 3],
            [0, "value-4", 4],
            [0, "value-5", 5]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, union_result.data);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected_union)->Equals(*union_result.data));
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestDiskPredicatePushdownWithoutMemoryFiltering) {
    options_[Options::FILE_FORMAT] = "parquet";
    options_[Options::WRITE_BATCH_SIZE] = "1";
    options_["parquet.page.size"] = "1";
    options_["parquet.enable-dictionary"] = "false";
    options_["parquet.write.enable-page-index"] = "true";
    options_["parquet.read.enable-page-index-filter"] = "true";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT,
                                Literal(static_cast<int64_t>(1)));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, CreatePlan(realtime_context, predicate));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult result,
                         ReadPlan(plan, {"id", "payload", "pt"}, predicate,
                                  /*enable_predicate_filter=*/false));
    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("id", arrow::int64()),
         arrow::field("payload", arrow::utf8()), arrow::field("pt", arrow::utf8())});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, 1, "value-1", "p0"],
            [0, 3, "value-3", "p0"],
            [0, 4, "value-4", "p0"],
            [0, 5, "value-5", "p0"]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, result.data);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result.data))
        << result.data->ToString();
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestRefreshCommittedSnapshotReclaimsMemory) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/10, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, disk_commits.size());
    ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot_id,
                         Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/10, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    std::vector<Row> expected_rows = disk_rows;
    expected_rows.insert(expected_rows.end(), memory_rows.begin(), memory_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> read1, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, read1);
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_before_refresh,
                         GetRealtimeMemoryUsage(realtime_context));

    ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> read2, ReadRows(realtime_context));
    ASSERT_EQ(read1, read2);
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_after_refresh,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_LT(memory_usage_after_refresh, memory_usage_before_refresh);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPlanPinsMemoryAcrossRefresh) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot_id,
                         Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> pinned_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));

    ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot_id));

    std::vector<Row> expected_rows = disk_rows;
    expected_rows.insert(expected_rows.end(), memory_rows.begin(), memory_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> pinned_rows, ReadRows(pinned_plan));
    ASSERT_EQ(expected_rows, pinned_rows);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> refreshed_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> refreshed_rows, ReadRows(refreshed_plan));
    ASSERT_EQ(pinned_rows, refreshed_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestRepeatedCommitReadAndRefresh) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    constexpr int64_t kRoundCount = 3;
    constexpr int64_t kRowsPerRound = 4;
    std::vector<Row> expected_rows;
    for (int64_t round = 0; round < kRoundCount; ++round) {
        std::vector<Row> rows = MakeRows(round * kRowsPerRound, kRowsPerRound, /*partition=*/"p0");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                             writer->PrepareCommitWithProgress(/*commit_identifier=*/round));
        ASSERT_EQ(1, commits.size());
        ASSERT_EQ(Range(round * kRowsPerRound, (round + 1) * kRowsPerRound - 1),
                  commits[0].offset_range);
        ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot_id,
                             Commit(commits, /*commit_identifier=*/round));
        expected_rows.insert(expected_rows.end(), rows.begin(), rows.end());

        ASSERT_OK_AND_ASSIGN(std::vector<Row> read_before_refresh, ReadRows(realtime_context));
        ASSERT_EQ(expected_rows, read_before_refresh);
        ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_before_refresh,
                             GetRealtimeMemoryUsage(realtime_context));
        ASSERT_GT(memory_usage_before_refresh, 0);

        ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot_id));

        ASSERT_OK_AND_ASSIGN(std::vector<Row> read_after_refresh, ReadRows(realtime_context));
        ASSERT_EQ(read_before_refresh, read_after_refresh);
        ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_after_refresh,
                             GetRealtimeMemoryUsage(realtime_context));
        ASSERT_EQ(0, memory_usage_after_refresh);
    }
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestConcurrentWritePrepareCommitReadAndRefresh) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    constexpr int32_t kPrepareThreadCount = 1;
    constexpr int32_t kReadThreadCount = 4;
    constexpr int64_t kBatchCount = 12;
    constexpr int64_t kRowsPerBatch = 2;
    constexpr int64_t kTotalRows = kBatchCount * kRowsPerBatch;

    std::atomic<bool> writer_done{false};
    std::atomic<bool> prepare_done{false};
    std::atomic<bool> commit_done{false};
    std::atomic<bool> refresh_done{false};
    std::atomic<int64_t> next_prepare_identifier{0};
    std::atomic<int32_t> commit_count{0};
    std::atomic<int32_t> refresh_count{0};
    ConcurrentTestState state;
    std::map<int64_t, RealtimeCommitProgress> pending_commits;
    std::deque<int64_t> pending_snapshot_ids;
    std::vector<int32_t> prepare_call_counts(kPrepareThreadCount, 0);
    std::vector<int32_t> read_call_counts(kReadThreadCount, 0);

    auto enqueue_prepared_commits = [&](std::vector<RealtimeCommitProgress>&& commits) {
        std::string error;
        {
            std::lock_guard<std::mutex> lock(state.mutex);
            for (RealtimeCommitProgress& commit : commits) {
                int64_t offset_from = commit.offset_range.from;
                if (!pending_commits.emplace(offset_from, std::move(commit)).second) {
                    error = "duplicate prepared real-time offset range";
                    break;
                }
            }
        }
        if (!error.empty()) {
            state.RecordError(error);
        }
        state.progress_cv.notify_all();
    };

    std::thread write_thread([&]() {
        state.WaitForStart();
        for (int64_t batch_index = 0; batch_index < kBatchCount && !state.ShouldStop();
             ++batch_index) {
            std::vector<Row> rows =
                MakeRows(batch_index * kRowsPerBatch, kRowsPerBatch, /*partition=*/"p0");
            Result<std::unique_ptr<RecordBatch>> batch_result =
                MakeBatch(rows, /*partitioned=*/false);
            if (state.RecordErrorIfNotOk(batch_result)) {
                break;
            }
            Status status = writer->Write(std::move(batch_result).value());
            if (state.RecordErrorIfNotOk(status)) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            // Pause midway until one refresh completes to guarantee write and refresh overlap.
            if (batch_index + 1 == kBatchCount / 2) {
                auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
                while (refresh_count.load(std::memory_order_acquire) == 0 && !state.ShouldStop() &&
                       std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                if (refresh_count.load(std::memory_order_acquire) == 0 && !state.ShouldStop()) {
                    state.RecordError("timed out waiting for a refresh while writing");
                    break;
                }
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> prepare_threads;
    prepare_threads.reserve(kPrepareThreadCount);
    for (int32_t thread_index = 0; thread_index < kPrepareThreadCount; ++thread_index) {
        prepare_threads.emplace_back([&, thread_index]() {
            state.WaitForStart();
            do {
                int64_t identifier = next_prepare_identifier.fetch_add(1);
                Result<std::vector<RealtimeCommitProgress>> result =
                    writer->PrepareCommitWithProgress(identifier);
                ++prepare_call_counts[thread_index];
                if (state.RecordErrorIfNotOk(result)) {
                    break;
                }
                enqueue_prepared_commits(std::move(result).value());
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } while (!writer_done.load(std::memory_order_acquire) && !state.ShouldStop());
        });
    }

    std::thread commit_thread([&]() {
        state.WaitForStart();
        int64_t next_offset = 0;
        int64_t commit_identifier = 0;
        while (!state.ShouldStop()) {
            std::optional<RealtimeCommitProgress> next_commit;
            {
                std::unique_lock<std::mutex> lock(state.mutex);
                state.progress_cv.wait(lock, [&]() {
                    return state.ShouldStop() || pending_commits.count(next_offset) > 0 ||
                           prepare_done.load(std::memory_order_acquire);
                });
                if (state.ShouldStop()) {
                    break;
                }
                auto iter = pending_commits.find(next_offset);
                if (iter == pending_commits.end()) {
                    if (prepare_done.load(std::memory_order_acquire)) {
                        if (!pending_commits.empty()) {
                            lock.unlock();
                            state.RecordError("prepared real-time offset ranges contain a gap");
                        }
                        break;
                    }
                    continue;
                }
                next_commit = std::move(iter->second);
                pending_commits.erase(iter);
            }

            std::vector<RealtimeCommitProgress> commits;
            commits.push_back(std::move(next_commit).value());
            int64_t committed_offset = commits[0].offset_range.to;
            Result<int64_t> commit_result = Commit(commits, commit_identifier++);
            if (state.RecordErrorIfNotOk(commit_result)) {
                break;
            }
            next_offset = committed_offset + 1;
            {
                std::lock_guard<std::mutex> lock(state.mutex);
                pending_snapshot_ids.push_back(std::move(commit_result).value());
            }
            ++commit_count;
            state.snapshot_cv.notify_all();
        }
        commit_done.store(true, std::memory_order_release);
        state.snapshot_cv.notify_all();
    });

    std::thread refresh_thread([&]() {
        state.WaitForStart();
        while (!state.ShouldStop()) {
            std::optional<int64_t> snapshot_id;
            {
                std::unique_lock<std::mutex> lock(state.mutex);
                state.snapshot_cv.wait(lock, [&]() {
                    return state.ShouldStop() || !pending_snapshot_ids.empty() ||
                           commit_done.load(std::memory_order_acquire);
                });
                if (state.ShouldStop()) {
                    break;
                }
                if (pending_snapshot_ids.empty()) {
                    if (commit_done.load(std::memory_order_acquire)) {
                        break;
                    }
                    continue;
                }
                snapshot_id = pending_snapshot_ids.front();
                pending_snapshot_ids.pop_front();
            }
            Status status = writer->RefreshCommittedSnapshot(snapshot_id.value());
            if (state.RecordErrorIfNotOk(status)) {
                break;
            }
            ++refresh_count;
        }
        refresh_done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> read_threads;
    read_threads.reserve(kReadThreadCount);
    for (int32_t thread_index = 0; thread_index < kReadThreadCount; ++thread_index) {
        read_threads.emplace_back([&, thread_index]() {
            state.WaitForStart();
            while (!refresh_done.load(std::memory_order_acquire) && !state.ShouldStop()) {
                Result<std::vector<Row>> result = ReadRows(realtime_context);
                ++read_call_counts[thread_index];
                if (state.RecordErrorIfNotOk(result)) {
                    break;
                }
                Status status = ValidateReadPrefix(result.value(), kTotalRows);
                if (state.RecordErrorIfNotOk(status)) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }

    constexpr int32_t kWorkerCount = 1 + kPrepareThreadCount + 1 + 1 + kReadThreadCount;
    state.StartWhenReady(kWorkerCount);

    write_thread.join();
    for (std::thread& thread : prepare_threads) {
        thread.join();
    }
    if (!state.ShouldStop()) {
        Result<std::vector<RealtimeCommitProgress>> final_result =
            writer->PrepareCommitWithProgress(next_prepare_identifier.fetch_add(1));
        if (!state.RecordErrorIfNotOk(final_result)) {
            enqueue_prepared_commits(std::move(final_result).value());
        }
    }
    prepare_done.store(true, std::memory_order_release);
    state.progress_cv.notify_all();

    commit_thread.join();
    refresh_thread.join();
    for (std::thread& thread : read_threads) {
        thread.join();
    }

    ASSERT_TRUE(state.Errors().empty()) << (state.Errors().empty() ? "" : state.Errors().front());
    for (int32_t call_count : prepare_call_counts) {
        ASSERT_GT(call_count, 0);
    }
    for (int32_t call_count : read_call_counts) {
        ASSERT_GT(call_count, 0);
    }
    ASSERT_GE(commit_count.load(), 2);
    ASSERT_GE(refresh_count.load(), 2);
    ASSERT_OK_AND_ASSIGN(std::vector<Row> final_rows, ReadRows(realtime_context));
    ASSERT_EQ(kTotalRows, static_cast<int64_t>(final_rows.size()));
    ASSERT_OK(ValidateReadPrefix(final_rows, kTotalRows));
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(kTotalRows - 1,
              committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage, GetRealtimeMemoryUsage(realtime_context));
    ASSERT_EQ(0, memory_usage);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestMultiplePartitions) {
    CreateTable(/*partition_keys=*/{"pt"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<std::vector<Row>> disk_rows;
    for (int64_t partition_index = 0; partition_index < 2; ++partition_index) {
        std::string partition = "p" + std::to_string(partition_index);
        std::vector<Row> rows = MakeRows(partition_index * 10, /*count=*/10, partition);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/true));
        ASSERT_OK(writer->Write(std::move(batch)));
        disk_rows.push_back(std::move(rows));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(2, disk_commits.size());
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> p0_memory_rows = MakeRows(/*first_id=*/20, /*count=*/5, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p0_memory_batch,
                         MakeBatch(p0_memory_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p0_memory_batch)));
    std::vector<Row> p2_memory_rows = MakeRows(/*first_id=*/30, /*count=*/5, /*partition=*/"p2");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p2_memory_batch,
                         MakeBatch(p2_memory_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p2_memory_batch)));

    // p0 has disk and memory rows, p1 is disk-only, and p2 is memory-only.
    std::vector<Row> expected_rows = disk_rows[0];
    expected_rows.insert(expected_rows.end(), p0_memory_rows.begin(), p0_memory_rows.end());
    expected_rows.insert(expected_rows.end(), disk_rows[1].begin(), disk_rows[1].end());
    expected_rows.insert(expected_rows.end(), p2_memory_rows.begin(), p2_memory_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, actual_rows);

    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(2, committed_offsets.size());
    for (int64_t partition_index = 0; partition_index < 2; ++partition_index) {
        RealtimePartitionBucket partition_bucket({{"pt", "p" + std::to_string(partition_index)}},
                                                 /*bucket=*/0);
        ASSERT_EQ(9, committed_offsets.at(partition_bucket));
    }
    ASSERT_EQ(committed_offsets.end(),
              committed_offsets.find(RealtimePartitionBucket({{"pt", "p2"}}, /*bucket=*/0)));
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestMultipleBucketsRestoreIndependentOffsets) {
    options_[Options::BUCKET] = "2";
    CreateTable(/*partition_keys=*/{});

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer, CreateRealtimeWriter());
    std::vector<Row> bucket0_disk_rows = MakeRows(/*first_id=*/0, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> bucket0_disk_batch,
                         MakeBatch(bucket0_disk_rows, /*partitioned=*/false, /*bucket=*/0));
    ASSERT_OK(first_writer->Write(std::move(bucket0_disk_batch)));
    std::vector<Row> bucket1_disk_rows = MakeRows(/*first_id=*/10, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> bucket1_disk_batch,
                         MakeBatch(bucket1_disk_rows, /*partitioned=*/false, /*bucket=*/1));
    ASSERT_OK(first_writer->Write(std::move(bucket1_disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_commits,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(2, first_commits.size());
    ASSERT_OK(Commit(first_commits, /*commit_identifier=*/0));
    ASSERT_OK(first_writer->Close());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> bucket0_memory_rows =
        MakeRows(/*first_id=*/2, /*count=*/1, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> bucket0_memory_batch,
                         MakeBatch(bucket0_memory_rows, /*partitioned=*/false, /*bucket=*/0));
    ASSERT_OK(second_writer->Write(std::move(bucket0_memory_batch)));
    std::vector<Row> bucket1_memory_rows =
        MakeRows(/*first_id=*/13, /*count=*/1, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> bucket1_memory_batch,
                         MakeBatch(bucket1_memory_rows, /*partitioned=*/false, /*bucket=*/1));
    ASSERT_OK(second_writer->Write(std::move(bucket1_memory_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(2, second_commits.size());
    std::map<int32_t, Range> prepared_ranges;
    for (const RealtimeCommitProgress& commit : second_commits) {
        prepared_ranges.emplace(commit.partition_bucket.bucket, commit.offset_range);
    }
    ASSERT_EQ(Range(2, 2), prepared_ranges.at(0));
    ASSERT_EQ(Range(3, 3), prepared_ranges.at(1));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    std::vector<Row> expected_rows = bucket0_disk_rows;
    expected_rows.insert(expected_rows.end(), bucket0_memory_rows.begin(),
                         bucket0_memory_rows.end());
    expected_rows.insert(expected_rows.end(), bucket1_disk_rows.begin(), bucket1_disk_rows.end());
    expected_rows.insert(expected_rows.end(), bucket1_memory_rows.begin(),
                         bucket1_memory_rows.end());
    std::sort(expected_rows.begin(), expected_rows.end());
    std::sort(actual_rows.begin(), actual_rows.end());
    ASSERT_EQ(expected_rows, actual_rows);

    ASSERT_OK(Commit(second_commits, /*commit_identifier=*/1));
    ASSERT_OK(second_writer->Close());
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(2, committed_offsets.size());
    ASSERT_EQ(2, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_EQ(3, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/1)));
}

TEST_F(RealtimeWriteInteTest, TestRestoreOffsetFromCommittedSnapshot) {
    CreateTable(/*partition_keys=*/{});

    std::vector<Row> first_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer, CreateRealtimeWriter());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(first_writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_commits,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, first_commits.size());
    ASSERT_EQ(Range(0, 2), first_commits[0].offset_range);
    ASSERT_OK(Commit(first_commits, /*commit_identifier=*/0));
    ASSERT_OK(first_writer->Close());

    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer, CreateRealtimeWriter());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(second_writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, second_commits.size());
    ASSERT_EQ(Range(3, 4), second_commits[0].offset_range);

    std::vector<Row> expected_rows = MakeRows(/*first_id=*/0, /*count=*/5, /*partition=*/"p0");
    FinalizeCommitAndCheck(second_writer.get(), std::move(second_commits),
                           /*prepare_identifier=*/1, std::move(expected_rows));

    RealtimePartitionBucket partition_bucket(/*partition=*/{}, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap second_committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(4, second_committed_offsets.at(partition_bucket));
}

TEST_F(RealtimeWriteInteTest, TestPkRealtimeConcurrency) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    options_[Options::NUM_SORTED_RUNS_COMPACTION_TRIGGER] = "1";
    options_[Options::COMMIT_FORCE_COMPACT] = "true";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    int64_t next_row_id = 0;
    int64_t next_commit_identifier = 0;
    for (int32_t i = 0; i < 2; ++i) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(MakeRows(next_row_id++, /*count=*/1, /*partition=*/"p0"),
                                       /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                             writer->PrepareCommitWithProgress(next_commit_identifier));
        ASSERT_EQ(1, progress.size());
        std::shared_ptr<CommitMessageImpl> message =
            std::dynamic_pointer_cast<CommitMessageImpl>(progress[0].commit_message);
        ASSERT_NE(nullptr, message);
        ASSERT_TRUE(message->GetCompactIncrement().IsEmpty());
        ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(progress, next_commit_identifier++));
        ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));
    }

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(MakeRows(next_row_id++, /*count=*/1, /*partition=*/"p0"),
                                   /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    WriteContextBuilder compact_builder(table_path_, commit_user_);
    compact_builder.SetOptions(options_).WithStreamingMode(true).WithTempDirectory(
        PathUtil::JoinPath(dir_->Str(), "compact-spill"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> compact_context, compact_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> compact_writer,
                         FileStoreWrite::Create(std::move(compact_context)));
    ASSERT_OK(compact_writer->Compact(/*partition=*/{}, /*bucket=*/0,
                                      /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> compact_messages,
        compact_writer->PrepareCommit(/*wait_compaction=*/true, next_commit_identifier));
    ASSERT_EQ(1, compact_messages.size());
    std::shared_ptr<CommitMessageImpl> compact_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(compact_messages[0]);
    ASSERT_NE(nullptr, compact_message);
    ASSERT_TRUE(compact_message->GetNewFilesIncrement().IsEmpty());
    ASSERT_EQ(2, compact_message->GetCompactIncrement().CompactBefore().size());
    ASSERT_FALSE(compact_message->GetCompactIncrement().CompactAfter().empty());
    ASSERT_OK(compact_writer->Close());
    ASSERT_OK(Commit(compact_messages, next_commit_identifier++));

    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options_));
    SnapshotManager snapshot_manager(core_options.GetFileSystem(), table_path_);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> compact_snapshot,
                         snapshot_manager.LatestSnapshot());
    ASSERT_TRUE(compact_snapshot);
    ASSERT_EQ(Snapshot::CommitKind::Compact(), compact_snapshot->GetCommitKind());
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap compact_offsets, ReadCommittedOffsets());
    ASSERT_EQ(1, compact_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK(writer->RefreshCommittedSnapshot(compact_snapshot->Id()));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows_after_compaction, ReadRows(realtime_context));
    ASSERT_OK(ValidateReadPrefix(rows_after_compaction, next_row_id));
    ASSERT_EQ(next_row_id, static_cast<int64_t>(rows_after_compaction.size()));

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> memory_progress,
                         writer->PrepareCommitWithProgress(next_commit_identifier));
    ASSERT_EQ(1, memory_progress.size());
    ASSERT_EQ(Range(2, 2), memory_progress[0].offset_range);
    std::shared_ptr<CommitMessageImpl> memory_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(memory_progress[0].commit_message);
    ASSERT_NE(nullptr, memory_message);
    ASSERT_TRUE(memory_message->GetCompactIncrement().IsEmpty());
    ASSERT_OK_AND_ASSIGN(int64_t memory_snapshot,
                         Commit(memory_progress, next_commit_identifier++));
    ASSERT_OK(writer->RefreshCommittedSnapshot(memory_snapshot));

    constexpr int32_t kPrepareThreadCount = 1;
    constexpr int32_t kReadThreadCount = 1;
    constexpr int64_t kBatchCount = 24;
    const int64_t concurrent_first_id = next_row_id;
    const int64_t total_row_count = concurrent_first_id + kBatchCount;

    std::atomic<bool> writer_done{false};
    std::atomic<bool> prepare_done{false};
    std::atomic<bool> commit_done{false};
    std::atomic<bool> refresh_done{false};
    std::atomic<int64_t> next_prepare_identifier{next_commit_identifier};
    std::atomic<int32_t> commit_count{0};
    std::atomic<int32_t> refresh_count{0};
    ConcurrentTestState state;
    std::map<int64_t, RealtimeCommitProgress> pending_commits;
    std::deque<int64_t> pending_snapshot_ids;
    std::vector<int32_t> prepare_call_counts(kPrepareThreadCount, 0);
    std::vector<int32_t> read_call_counts(kReadThreadCount, 0);

    auto enqueue_prepared_commits = [&](std::vector<RealtimeCommitProgress>&& commits) {
        std::string error;
        {
            std::lock_guard<std::mutex> lock(state.mutex);
            for (RealtimeCommitProgress& commit : commits) {
                const int64_t offset_from = commit.offset_range.from;
                if (!pending_commits.emplace(offset_from, std::move(commit)).second) {
                    error = "duplicate prepared PK real-time offset range";
                    break;
                }
            }
        }
        if (!error.empty()) {
            state.RecordError(error);
        }
        state.progress_cv.notify_all();
    };

    std::thread write_thread([&]() {
        state.WaitForStart();
        for (int64_t batch_index = 0; batch_index < kBatchCount && !state.ShouldStop();
             ++batch_index) {
            Result<std::unique_ptr<RecordBatch>> batch =
                MakeBatch(MakeRows(concurrent_first_id + batch_index, /*count=*/1,
                                   /*partition=*/"p0"),
                          /*partitioned=*/false);
            if (state.RecordErrorIfNotOk(batch) ||
                state.RecordErrorIfNotOk(writer->Write(std::move(batch).value()))) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            if (batch_index + 1 == kBatchCount / 2) {
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
                while (refresh_count.load(std::memory_order_acquire) == 0 && !state.ShouldStop() &&
                       std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                if (refresh_count.load(std::memory_order_acquire) == 0 && !state.ShouldStop()) {
                    state.RecordError("timed out waiting for a PK refresh while writing");
                    break;
                }
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> prepare_threads;
    prepare_threads.reserve(kPrepareThreadCount);
    for (int32_t thread_index = 0; thread_index < kPrepareThreadCount; ++thread_index) {
        prepare_threads.emplace_back([&, thread_index]() {
            state.WaitForStart();
            do {
                Result<std::vector<RealtimeCommitProgress>> progress =
                    writer->PrepareCommitWithProgress(
                        next_prepare_identifier.fetch_add(1, std::memory_order_relaxed));
                ++prepare_call_counts[thread_index];
                if (state.RecordErrorIfNotOk(progress)) {
                    break;
                }
                enqueue_prepared_commits(std::move(progress).value());
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } while (!writer_done.load(std::memory_order_acquire) && !state.ShouldStop());
        });
    }

    std::thread commit_thread([&]() {
        state.WaitForStart();
        int64_t next_offset = concurrent_first_id;
        int64_t commit_identifier = next_commit_identifier;
        while (!state.ShouldStop()) {
            std::optional<RealtimeCommitProgress> next_commit;
            {
                std::unique_lock<std::mutex> lock(state.mutex);
                state.progress_cv.wait(lock, [&]() {
                    return state.ShouldStop() || pending_commits.count(next_offset) > 0 ||
                           prepare_done.load(std::memory_order_acquire);
                });
                if (state.ShouldStop()) {
                    break;
                }
                auto iter = pending_commits.find(next_offset);
                if (iter == pending_commits.end()) {
                    if (prepare_done.load(std::memory_order_acquire)) {
                        if (!pending_commits.empty()) {
                            lock.unlock();
                            state.RecordError("prepared PK real-time offset ranges contain a gap");
                        }
                        break;
                    }
                    continue;
                }
                next_commit = std::move(iter->second);
                pending_commits.erase(iter);
            }

            std::vector<RealtimeCommitProgress> commits;
            commits.push_back(std::move(next_commit).value());
            const int64_t committed_offset = commits[0].offset_range.to;
            Result<int64_t> commit_result = Commit(commits, commit_identifier++);
            if (state.RecordErrorIfNotOk(commit_result)) {
                break;
            }
            next_offset = committed_offset + 1;
            {
                std::lock_guard<std::mutex> lock(state.mutex);
                pending_snapshot_ids.push_back(std::move(commit_result).value());
            }
            ++commit_count;
            state.snapshot_cv.notify_all();
        }
        commit_done.store(true, std::memory_order_release);
        state.snapshot_cv.notify_all();
    });

    std::thread refresh_thread([&]() {
        state.WaitForStart();
        while (!state.ShouldStop()) {
            std::optional<int64_t> snapshot_id;
            {
                std::unique_lock<std::mutex> lock(state.mutex);
                state.snapshot_cv.wait(lock, [&]() {
                    return state.ShouldStop() || !pending_snapshot_ids.empty() ||
                           commit_done.load(std::memory_order_acquire);
                });
                if (state.ShouldStop()) {
                    break;
                }
                if (pending_snapshot_ids.empty()) {
                    if (commit_done.load(std::memory_order_acquire)) {
                        break;
                    }
                    continue;
                }
                snapshot_id = pending_snapshot_ids.front();
                pending_snapshot_ids.pop_front();
            }
            if (state.RecordErrorIfNotOk(writer->RefreshCommittedSnapshot(snapshot_id.value()))) {
                break;
            }
            ++refresh_count;
        }
        refresh_done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> read_threads;
    read_threads.reserve(kReadThreadCount);
    for (int32_t thread_index = 0; thread_index < kReadThreadCount; ++thread_index) {
        read_threads.emplace_back([&, thread_index]() {
            state.WaitForStart();
            while (!refresh_done.load(std::memory_order_acquire) && !state.ShouldStop()) {
                Result<std::vector<Row>> rows = ReadRows(realtime_context);
                ++read_call_counts[thread_index];
                if (state.RecordErrorIfNotOk(rows) ||
                    state.RecordErrorIfNotOk(ValidateReadPrefix(rows.value(), total_row_count))) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }

    constexpr int32_t kWorkerCount = 1 + kPrepareThreadCount + 1 + 1 + kReadThreadCount;
    state.StartWhenReady(kWorkerCount);

    write_thread.join();
    for (std::thread& prepare_thread : prepare_threads) {
        prepare_thread.join();
    }
    if (!state.ShouldStop()) {
        Result<std::vector<RealtimeCommitProgress>> final_progress =
            writer->PrepareCommitWithProgress(
                next_prepare_identifier.fetch_add(1, std::memory_order_relaxed));
        if (!state.RecordErrorIfNotOk(final_progress)) {
            enqueue_prepared_commits(std::move(final_progress).value());
        }
    }
    prepare_done.store(true, std::memory_order_release);
    state.progress_cv.notify_all();

    commit_thread.join();
    refresh_thread.join();
    for (std::thread& read_thread : read_threads) {
        read_thread.join();
    }

    ASSERT_TRUE(state.Errors().empty()) << (state.Errors().empty() ? "" : state.Errors().front());
    for (int32_t call_count : prepare_call_counts) {
        ASSERT_GT(call_count, 0);
    }
    for (int32_t call_count : read_call_counts) {
        ASSERT_GT(call_count, 0);
    }
    ASSERT_GE(commit_count.load(), 2);
    ASSERT_GE(refresh_count.load(), 2);
    ASSERT_OK_AND_ASSIGN(std::vector<Row> final_rows, ReadRows(realtime_context));
    ASSERT_OK(ValidateReadPrefix(final_rows, total_row_count));
    ASSERT_EQ(total_row_count, static_cast<int64_t>(final_rows.size()));
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(total_row_count - 1,
              committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage, GetRealtimeMemoryUsage(realtime_context));
    ASSERT_EQ(0, memory_usage);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkMutationSequencesAreIndependentFromOffsets) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    CreatePkTable();

    WriteContextBuilder seed_builder(table_path_, commit_user_);
    seed_builder.SetOptions(options_).WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> seed_context, seed_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> seed_writer,
                         FileStoreWrite::Create(std::move(seed_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> seed_batch,
                         MakeBatch({Row{99, "seed", "p0"}}, /*partitioned=*/false));
    ASSERT_OK(seed_writer->Write(std::move(seed_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> seed_messages,
                         seed_writer->PrepareCommit(/*wait_compaction=*/false,
                                                    /*commit_identifier=*/0));
    ASSERT_OK(Commit(seed_messages));
    ASSERT_OK(seed_writer->Close());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> mutations = {
        {1, "one", "p0"}, {1, "one-new", "p0"}, {2, "deleted", "p0"}, {3, "three", "p0"}};
    std::vector<RecordBatch::RowKind> row_kinds = {
        RecordBatch::RowKind::INSERT, RecordBatch::RowKind::UPDATE_AFTER,
        RecordBatch::RowKind::DELETE, RecordBatch::RowKind::INSERT};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(mutations, /*partitioned=*/false, /*bucket=*/0, row_kinds));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> memory_sequences,
                         ReadPkQuerySequences(realtime_context));
    ASSERT_EQ(std::vector<int64_t>({2, 3, 4}), memory_sequences);
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, progress.size());
    ASSERT_EQ(Range(0, 3), progress[0].offset_range);
    ASSERT_EQ(1, NewFiles(progress).size());
    ASSERT_EQ(3, NewFiles(progress)[0]->row_count);
    ASSERT_EQ(std::optional<int64_t>(1), NewFiles(progress)[0]->delete_row_count);
    ASSERT_EQ(memory_sequences.front(), NewFiles(progress)[0]->min_sequence_number);
    ASSERT_EQ(memory_sequences.back(), NewFiles(progress)[0]->max_sequence_number);
    ASSERT_NE(progress[0].offset_range.to, NewFiles(progress)[0]->max_sequence_number);
    ASSERT_OK(Commit(progress, /*commit_identifier=*/1));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual, ReadRows());
    ASSERT_EQ((std::vector<Row>{{1, "one-new", "p0"}, {3, "three", "p0"}, {99, "seed", "p0"}}),
              actual);
}

TEST_F(RealtimeWriteInteTest, TestPkBucketRecovery) {
    options_[Options::BUCKET] = "2";
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    CreatePkTable();

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer, CreateRealtimeWriter());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> bucket0_batch,
                         MakeBatch({Row{1, "old-1", "p0"}, Row{2, "old-2", "p0"}},
                                   /*partitioned=*/false, /*bucket=*/0));
    ASSERT_OK(first_writer->Write(std::move(bucket0_batch)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> bucket1_batch,
        MakeBatch({Row{10, "old-10", "p0"}, Row{11, "old-11", "p0"}, Row{12, "old-12", "p0"}},
                  /*partitioned=*/false, /*bucket=*/1));
    ASSERT_OK(first_writer->Write(std::move(bucket1_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_progress,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    std::map<int32_t, Range> first_ranges;
    for (const RealtimeCommitProgress& progress : first_progress) {
        first_ranges.emplace(progress.partition_bucket.bucket, progress.offset_range);
    }
    ASSERT_EQ(Range(0, 1), first_ranges.at(0));
    ASSERT_EQ(Range(0, 2), first_ranges.at(1));
    ASSERT_OK(Commit(first_progress, /*commit_identifier=*/0));
    ASSERT_OK(first_writer->Close());
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap first_offsets, ReadCommittedOffsets());
    ASSERT_EQ(1, first_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_EQ(2, first_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/1)));

    auto factory = std::make_shared<DelegatingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> second_context,
                         RealtimeContext::Create(factory));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer,
                         CreateRealtimeWriter(second_context));
    ASSERT_OK_AND_ASSIGN(bucket0_batch, MakeBatch({Row{1, "new-1", "p0"}, Row{3, "new-3", "p0"}},
                                                  /*partitioned=*/false, /*bucket=*/0,
                                                  {RecordBatch::RowKind::UPDATE_AFTER,
                                                   RecordBatch::RowKind::INSERT}));
    ASSERT_OK(second_writer->Write(std::move(bucket0_batch)));
    ASSERT_OK_AND_ASSIGN(bucket1_batch,
                         MakeBatch({Row{10, "old-10", "p0"}, Row{13, "new-13", "p0"}},
                                   /*partitioned=*/false, /*bucket=*/1,
                                   {RecordBatch::RowKind::DELETE, RecordBatch::RowKind::INSERT}));
    ASSERT_OK(second_writer->Write(std::move(bucket1_batch)));

    ASSERT_EQ(2, factory->create_count);
    std::map<int32_t, int64_t> restored_sequences;
    for (const DelegatingMemIndexerFactory::CapturedRequest& request : factory->requests) {
        ASSERT_TRUE(std::holds_alternative<PrimaryKeyMemIndexerCreateConfig>(request.mode_config));
        const auto& primary_key_config =
            std::get<PrimaryKeyMemIndexerCreateConfig>(request.mode_config);
        ASSERT_EQ(std::vector<std::string>({"id"}), primary_key_config.primary_keys);
        ASSERT_TRUE(request.partition.empty());
        restored_sequences.emplace(request.bucket, primary_key_config.restore_max_sequence_number);
    }
    ASSERT_EQ(1, restored_sequences.at(0));
    ASSERT_EQ(2, restored_sequences.at(1));

    std::vector<Row> expected = {{1, "new-1", "p0"},   {2, "old-2", "p0"},   {3, "new-3", "p0"},
                                 {11, "old-11", "p0"}, {12, "old-12", "p0"}, {13, "new-13", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::vector<Row> realtime_rows, ReadRows(second_context));
    ASSERT_EQ(expected, realtime_rows);

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_progress,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    std::map<int32_t, Range> second_ranges;
    for (const RealtimeCommitProgress& progress : second_progress) {
        second_ranges.emplace(progress.partition_bucket.bucket, progress.offset_range);
    }
    ASSERT_EQ(Range(2, 3), second_ranges.at(0));
    ASSERT_EQ(Range(3, 4), second_ranges.at(1));
    ASSERT_OK(Commit(second_progress, /*commit_identifier=*/1));
    ASSERT_OK(second_writer->Close());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> disk_rows, ReadRows());
    ASSERT_EQ(expected, disk_rows);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap offsets, ReadCommittedOffsets());
    ASSERT_EQ(3, offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_EQ(4, offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/1)));
}

TEST_F(RealtimeWriteInteTest, TestPkPartitionedWrites) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    CreatePkTable(/*partition_keys=*/{"pt"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p0_disk,
                         MakeBatch({Row{1, "old", "p0"}, Row{2, "keep", "p0"}},
                                   /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p0_disk)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p1_disk,
                         MakeBatch({Row{1, "delete", "p1"}}, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p1_disk)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_progress, /*commit_identifier=*/0));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> p0_memory,
        MakeBatch({Row{1, "new", "p0"}, Row{3, "three", "p0"}}, /*partitioned=*/true,
                  /*bucket=*/0,
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::INSERT}));
    ASSERT_OK(writer->Write(std::move(p0_memory)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> p1_memory,
        MakeBatch({Row{1, "delete", "p1"}, Row{2, "two", "p1"}}, /*partitioned=*/true,
                  /*bucket=*/0, {RecordBatch::RowKind::DELETE, RecordBatch::RowKind::INSERT}));
    ASSERT_OK(writer->Write(std::move(p1_memory)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p2_memory,
                         MakeBatch({Row{1, "one", "p2"}}, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p2_memory)));

    std::vector<Row> expected = {{1, "new", "p0"},
                                 {2, "keep", "p0"},
                                 {3, "three", "p0"},
                                 {2, "two", "p1"},
                                 {1, "one", "p2"}};
    ASSERT_OK_AND_ASSIGN(std::vector<Row> realtime_rows, ReadRows(realtime_context));
    std::sort(expected.begin(), expected.end());
    std::sort(realtime_rows.begin(), realtime_rows.end());
    ASSERT_EQ(expected, realtime_rows);

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> memory_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(3, memory_progress.size());
    ASSERT_OK(Commit(memory_progress, /*commit_identifier=*/1));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> disk_rows, ReadRows());
    std::sort(disk_rows.begin(), disk_rows.end());
    ASSERT_EQ(expected, disk_rows);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap offsets, ReadCommittedOffsets());
    ASSERT_EQ(3, offsets.at(RealtimePartitionBucket({{"pt", "p0"}}, /*bucket=*/0)));
    ASSERT_EQ(2, offsets.at(RealtimePartitionBucket({{"pt", "p1"}}, /*bucket=*/0)));
    ASSERT_EQ(0, offsets.at(RealtimePartitionBucket({{"pt", "p2"}}, /*bucket=*/0)));
}

TEST_F(RealtimeWriteInteTest, TestPkRealtimeMergesDiskAndMemoryMutations) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    CreatePkTable();
    auto factory = std::make_shared<DelegatingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create(factory));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = {{1, "old", "p0"}, {2, "keep", "p0"}, {3, "delete-me", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_progress, /*commit_identifier=*/0));

    std::vector<Row> first_mutations = {
        {1, "new", "p0"}, {3, "delete-me", "p0"}, {4, "first", "p0"}};
    std::vector<RecordBatch::RowKind> first_kinds = {RecordBatch::RowKind::UPDATE_AFTER,
                                                     RecordBatch::RowKind::DELETE,
                                                     RecordBatch::RowKind::INSERT};
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> first_batch,
        MakeBatch(first_mutations, /*partitioned=*/false, /*bucket=*/0, first_kinds));
    ASSERT_OK(writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch({Row{4, "latest", "p0"}}, /*partitioned=*/false,
                                   /*bucket=*/0, {RecordBatch::RowKind::UPDATE_AFTER}));
    ASSERT_OK(writer->Write(std::move(second_batch)));

    ASSERT_EQ(1, factory->create_count);
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         realtime_context->AcquireReadViews());
    ASSERT_EQ(1, views.size());
    ASSERT_TRUE(std::dynamic_pointer_cast<DelegatingMemIndexer>(views[0].indexer));

    std::vector<Row> expected = {{1, "new", "p0"}, {2, "keep", "p0"}, {4, "latest", "p0"}};
    for (const char* sort_engine : {"loser-tree", "min-heap"}) {
        options_[Options::SORT_ENGINE] = sort_engine;
        ASSERT_OK_AND_ASSIGN(std::vector<Row> actual, ReadRows(realtime_context));
        ASSERT_EQ(expected, actual) << sort_engine;
    }

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    first_progress.insert(first_progress.end(), std::make_move_iterator(second_progress.begin()),
                          std::make_move_iterator(second_progress.end()));
    ASSERT_OK(Commit(first_progress, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> committed, ReadRows(realtime_context));
    ASSERT_EQ(expected, committed);
    ASSERT_OK_AND_ASSIGN(std::vector<Row> disk_only, ReadRows());
    ASSERT_EQ(expected, disk_only);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkConcurrentRead) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    constexpr int32_t kReaderCount = 4;
    std::atomic<int32_t> phase{0};
    std::atomic<bool> done{false};
    std::vector<int32_t> observed_phases(kReaderCount, 0);
    std::vector<int32_t> read_counts(kReaderCount, 0);
    ConcurrentTestState state;
    const std::vector<Row> expected = {
        {1, "v1", "p0"}, {3, "v0", "p0"}, {4, "v0", "p0"}, {5, "v1", "p0"}};

    auto validate_rows = [&expected](std::vector<Row> rows) -> Status {
        std::map<int64_t, std::string> values_by_id;
        for (const Row& row : rows) {
            const int64_t id = std::get<0>(row);
            const std::string& payload = std::get<1>(row);
            if (!values_by_id.emplace(id, payload).second) {
                return Status::Invalid("duplicate primary key in concurrent PK read");
            }
            if (payload != "v0" && payload != "v1") {
                return Status::Invalid("invalid versioned value in concurrent PK read");
            }
        }
        std::sort(rows.begin(), rows.end());
        if (rows != expected) {
            return Status::Invalid("unexpected rows in concurrent PK read");
        }
        return Status::OK();
    };

    auto wait_for_readers = [&](int32_t expected_phase) -> bool {
        std::unique_lock<std::mutex> lock(state.mutex);
        state.progress_cv.wait(lock, [&]() {
            return state.ShouldStop() || std::all_of(observed_phases.begin(), observed_phases.end(),
                                                     [expected_phase](int32_t observed) {
                                                         return observed >= expected_phase;
                                                     });
        });
        return !state.ShouldStop();
    };

    std::thread control_thread([&]() {
        state.WaitForStart();
        Result<std::unique_ptr<RecordBatch>> seed_batch = MakeBatch(
            {Row{1, "v0", "p0"}, Row{2, "v0", "p0"}, Row{3, "v0", "p0"}, Row{4, "v0", "p0"}},
            /*partitioned=*/false);
        if (state.RecordErrorIfNotOk(seed_batch) ||
            state.RecordErrorIfNotOk(writer->Write(std::move(seed_batch).value()))) {
            return;
        }
        Result<std::vector<RealtimeCommitProgress>> seed_progress =
            writer->PrepareCommitWithProgress(/*commit_identifier=*/0);
        if (state.RecordErrorIfNotOk(seed_progress)) {
            return;
        }
        Result<int64_t> seed_snapshot = Commit(seed_progress.value(), /*commit_identifier=*/0);
        if (state.RecordErrorIfNotOk(seed_snapshot) ||
            state.RecordErrorIfNotOk(writer->RefreshCommittedSnapshot(seed_snapshot.value()))) {
            return;
        }

        Result<std::unique_ptr<RecordBatch>> mutation_batch =
            MakeBatch({Row{1, "v1", "p0"}, Row{2, "v0", "p0"}, Row{5, "v1", "p0"}},
                      /*partitioned=*/false, /*bucket=*/0,
                      {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE,
                       RecordBatch::RowKind::INSERT});
        if (state.RecordErrorIfNotOk(mutation_batch) ||
            state.RecordErrorIfNotOk(writer->Write(std::move(mutation_batch).value()))) {
            return;
        }
        phase.store(1, std::memory_order_release);
        state.progress_cv.notify_all();
        if (!wait_for_readers(1)) {
            return;
        }

        Result<std::vector<RealtimeCommitProgress>> progress =
            writer->PrepareCommitWithProgress(/*commit_identifier=*/1);
        if (state.RecordErrorIfNotOk(progress)) {
            return;
        }
        Result<int64_t> committed_snapshot = Commit(progress.value(), /*commit_identifier=*/1);
        if (state.RecordErrorIfNotOk(committed_snapshot)) {
            return;
        }
        phase.store(2, std::memory_order_release);
        state.progress_cv.notify_all();
        if (!wait_for_readers(2)) {
            return;
        }

        if (state.RecordErrorIfNotOk(
                writer->RefreshCommittedSnapshot(committed_snapshot.value()))) {
            return;
        }
        phase.store(3, std::memory_order_release);
        state.progress_cv.notify_all();
        if (!wait_for_readers(3) || state.RecordErrorIfNotOk(writer->Close())) {
            return;
        }
        done.store(true, std::memory_order_release);
        state.progress_cv.notify_all();
    });

    std::vector<std::thread> reader_threads;
    reader_threads.reserve(kReaderCount);
    for (int32_t reader_index = 0; reader_index < kReaderCount; ++reader_index) {
        reader_threads.emplace_back([&, reader_index]() {
            state.WaitForStart();
            while (!done.load(std::memory_order_acquire) && !state.ShouldStop()) {
                const int32_t read_phase = phase.load(std::memory_order_acquire);
                if (read_phase == 0) {
                    std::unique_lock<std::mutex> lock(state.mutex);
                    state.progress_cv.wait(lock, [&]() {
                        return phase.load(std::memory_order_acquire) > 0 || state.ShouldStop();
                    });
                    continue;
                }
                Result<std::vector<Row>> rows = ReadRows(realtime_context);
                if (state.RecordErrorIfNotOk(rows) ||
                    state.RecordErrorIfNotOk(validate_rows(std::move(rows).value()))) {
                    return;
                }
                {
                    std::lock_guard<std::mutex> lock(state.mutex);
                    observed_phases[reader_index] =
                        std::max(observed_phases[reader_index], read_phase);
                    ++read_counts[reader_index];
                }
                state.progress_cv.notify_all();
            }
        });
    }

    state.StartWhenReady(kReaderCount + 1);
    control_thread.join();
    for (std::thread& reader_thread : reader_threads) {
        reader_thread.join();
    }

    ASSERT_TRUE(state.Errors().empty()) << (state.Errors().empty() ? "" : state.Errors().front());
    for (int32_t reader_index = 0; reader_index < kReaderCount; ++reader_index) {
        ASSERT_EQ(3, observed_phases[reader_index]);
        ASSERT_GT(read_counts[reader_index], 0);
    }
    ASSERT_OK_AND_ASSIGN(std::vector<Row> final_rows, ReadRows());
    ASSERT_OK(validate_rows(std::move(final_rows)));
}

TEST_F(RealtimeWriteInteTest, TestPkProjectionAndPredicateOverDiskAndMemory) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> scan_filtered_batch,
                         MakeBatch({Row{0, "disk-zero", "p0"}}, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(scan_filtered_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> scan_filtered_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(scan_filtered_progress, /*commit_identifier=*/0));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch({Row{2, "read-filtered", "p0"}, Row{4, "disk-old", "p0"},
                                    Row{5, "disk-five", "p0"}},
                                   /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK(Commit(disk_progress, /*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> memory_batch,
        MakeBatch({Row{3, "memory-three", "p0"}, Row{4, "memory-four", "p0"}},
                  /*partitioned=*/false, /*bucket=*/0,
                  {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::UPDATE_AFTER}));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    std::shared_ptr<Predicate> scan_predicate =
        PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT,
                                      Literal(static_cast<int64_t>(1)));
    std::shared_ptr<Predicate> read_predicate = PredicateBuilder::NotEqual(
        /*field_index=*/0, /*field_name=*/"payload", FieldType::STRING,
        Literal(FieldType::STRING, "read-filtered", 13));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, CreatePlan(realtime_context, scan_predicate));
    ASSERT_OK_AND_ASSIGN(
        CollectedReadResult result,
        ReadPlan(plan, {"payload", "id"}, read_predicate, /*enable_predicate_filter=*/true));
    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("payload", arrow::utf8()),
         arrow::field("id", arrow::int64())});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, "memory-three", 3],
            [0, "memory-four", 4],
            [0, "disk-five", 5]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, result.data);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result.data))
        << result.data->ToString();
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkQuotaExhaustionRotatesAcceptedWrites) {
    options_[Options::WRITE_BUFFER_SIZE] = "1";
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    options_[Options::WRITE_BUFFER_SPILL_MAX_DISK_SIZE] = "1b";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch({Row{1, "old", "p0"}, Row{2, "two", "p0"}},
                                   /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> second_batch,
        MakeBatch({Row{1, "new", "p0"}, Row{2, "deleted", "p0"}, Row{3, "three", "p0"}},
                  /*partitioned=*/false, /*bucket=*/0,
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE,
                   RecordBatch::RowKind::INSERT}));
    ASSERT_OK(writer->Write(std::move(second_batch)));

    std::vector<Row> expected = {{1, "new", "p0"}, {3, "three", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::vector<Row> memory_rows, ReadRows(realtime_context));
    ASSERT_EQ(expected, memory_rows);

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, progress.size());
    ASSERT_EQ(Range(0, 4), progress[0].offset_range);
    ASSERT_OK(Commit(progress, /*commit_identifier=*/0));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> restart_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> restart_writer,
                         CreateRealtimeWriter(restart_context));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> reopened_rows, ReadRows(restart_context));
    ASSERT_EQ(expected, reopened_rows);
    ASSERT_OK(restart_writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> disk_rows, ReadRows());
    ASSERT_EQ(expected, disk_rows);
}

TEST_F(RealtimeWriteInteTest, TestPkSpillSingleThread) {
    VerifyPkSpillWriteRead(/*write_buffer_spill_thread_number=*/0);
}

TEST_F(RealtimeWriteInteTest, TestPkSpillMultiThread) {
    VerifyPkSpillWriteRead(/*write_buffer_spill_thread_number=*/2);
}

TEST_F(RealtimeWriteInteTest, TestPkRealtimeReadsMemoryOnlyRoute) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        MakeBatch({Row{1, "first", "p0"}, Row{1, "latest", "p0"}},
                  /*partitioned=*/false, /*bucket=*/0,
                  {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::UPDATE_AFTER}));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual, ReadRows(realtime_context));
    ASSERT_EQ((std::vector<Row>{{1, "latest", "p0"}}), actual);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkSpillBackedPlanSurvivesRefreshAndReclaim) {
    options_[Options::WRITE_BUFFER_SIZE] = "1";
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    options_[Options::WRITE_BUFFER_SPILL_MAX_DISK_SIZE] = "1b";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch({Row{1, "disk", "p0"}}, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_progress, /*commit_identifier=*/0));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> pinned_batch,
                         MakeBatch({Row{1, "pinned", "p0"}}, /*partitioned=*/false,
                                   /*bucket=*/0, {RecordBatch::RowKind::UPDATE_AFTER}));
    ASSERT_OK(writer->Write(std::move(pinned_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> pinned_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> later_batch,
                         MakeBatch({Row{1, "later", "p0"}}, /*partitioned=*/false,
                                   /*bucket=*/0, {RecordBatch::RowKind::UPDATE_AFTER}));
    ASSERT_OK(writer->Write(std::move(later_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> later_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot,
                         Commit(later_progress, /*commit_identifier=*/1));
    ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> pinned_result, ReadRows(pinned_plan));
    ASSERT_EQ((std::vector<Row>{{1, "pinned", "p0"}}), pinned_result);
    ASSERT_OK_AND_ASSIGN(std::vector<Row> latest_result, ReadRows(realtime_context));
    ASSERT_EQ((std::vector<Row>{{1, "later", "p0"}}), latest_result);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkReadAndWriteSpillValidation) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "false";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         CreateScan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK(scan->CreatePlan());
    ASSERT_NOK_WITH_MSG(CreateRealtimeWriter(realtime_context), "requires write-buffer spill");
}

TEST_F(RealtimeWriteInteTest, TestPkScanValidation) {
    CreatePkTable();

    options_[Options::BUCKET] = "-2";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    Result<std::unique_ptr<TableScan>> scan = CreateScan(realtime_context, /*predicate=*/nullptr);
    ASSERT_TRUE(scan.status().IsInvalid());
    ASSERT_EQ("real-time union read requires fixed bucket mode", scan.status().message());

    options_[Options::BUCKET] = "1";
    options_[Options::DATA_EVOLUTION_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(realtime_context, RealtimeContext::Create());
    scan = CreateScan(realtime_context, /*predicate=*/nullptr);
    ASSERT_TRUE(scan.status().IsInvalid());
    ASSERT_EQ("real-time union read does not support data evolution", scan.status().message());
}

TEST_F(RealtimeWriteInteTest, TestPkWritePropagatesIoFailure) {
    options_[Options::WRITE_BUFFER_SPILLABLE] = "true";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({Row{1, "one", "p0"}}, /*partitioned=*/false,
                                   /*bucket=*/0));

    IOHook* io_hook = IOHook::GetInstance();
    ScopeGuard hook_guard([io_hook]() { io_hook->Clear(); });
    io_hook->Reset(/*pos=*/0, IOHook::Mode::RETURN_ERROR);
    Status operation_error = writer->Write(std::move(batch));
    io_hook->Clear();
    ASSERT_TRUE(operation_error.IsIOError()) << operation_error.ToString();
    ASSERT_NOK_WITH_MSG(operation_error, "io hook triggered io error");
    ASSERT_OK(writer->Close());
}

}  // namespace paimon::test

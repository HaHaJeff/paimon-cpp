/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
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
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/commit/realtime_snapshot_properties.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/read_context.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class RealtimeWriteInteTest : public ::testing::Test {
 protected:
    using Row = std::tuple<int64_t, std::string, std::string>;
    using ReadRow = std::tuple<int64_t, int64_t, std::string, std::string>;

    void SetUp() override {
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
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema_, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog,
                             Catalog::Create(dir_->Str(), options_));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema, partition_keys,
                                       /*primary_keys=*/{}, options_,
                                       /*ignore_if_exists=*/false));
    }

    Result<std::unique_ptr<FileStoreWrite>> CreateRealtimeWriter() const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContext> realtime_context,
                               RealtimeContext::Create());
        WriteContextBuilder builder(table_path_, commit_user_);
        builder.SetOptions(options_).WithStreamingMode(true).WithRealtimeContext(realtime_context);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> context, builder.Finish());
        return FileStoreWrite::Create(std::move(context));
    }

    Result<std::unique_ptr<RecordBatch>> MakeBatch(const std::vector<Row>& rows,
                                                   bool partitioned) const {
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
        if (partitioned) {
            builder.SetPartition({{"pt", partition}});
        }
        return builder.SetBucket(0).Finish();
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

    Status Commit(const std::vector<RealtimeCommitProgress>& realtime_commits,
                  int64_t commit_identifier) const {
        CommitContextBuilder builder(table_path_, commit_user_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->CommitWithProgress(realtime_commits, commit_identifier,
                                          /*watermark=*/std::nullopt);
    }

    static std::vector<ReadRow> WithExpectedOffsets(const std::vector<Row>& rows) {
        std::map<std::string, int64_t> next_offsets;
        std::vector<ReadRow> result;
        result.reserve(rows.size());
        for (const Row& row : rows) {
            const auto& [id, payload, partition] = row;
            int64_t offset = next_offsets[partition]++;
            result.emplace_back(offset, id, payload, partition);
        }
        return result;
    }

    Result<std::vector<ReadRow>> ReadRows() const {
        ScanContextBuilder scan_builder(table_path_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context,
                               scan_builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan,
                               TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, scan->CreatePlan());

        ReadContextBuilder read_builder(table_path_);
        read_builder.SetOptions(options_).SetReadFieldNames({"_OFFSET", "id", "payload", "pt"});
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                               TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                               table_read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> result,
                               ReadResultCollector::CollectResult(reader.get()));
        reader->Close();

        std::vector<ReadRow> rows;
        if (!result) {
            return rows;
        }
        for (const std::shared_ptr<arrow::Array>& chunk : result->chunks()) {
            std::shared_ptr<arrow::StructArray> data =
                std::dynamic_pointer_cast<arrow::StructArray>(chunk);
            if (!data || data->num_fields() != 5) {
                return Status::Invalid("unexpected real-time test read schema");
            }
            std::shared_ptr<arrow::Int8Array> row_kinds =
                std::dynamic_pointer_cast<arrow::Int8Array>(data->field(0));
            std::shared_ptr<arrow::Int64Array> offsets =
                std::dynamic_pointer_cast<arrow::Int64Array>(data->field(1));
            std::shared_ptr<arrow::Int64Array> ids =
                std::dynamic_pointer_cast<arrow::Int64Array>(data->field(2));
            std::shared_ptr<arrow::StringArray> payloads =
                std::dynamic_pointer_cast<arrow::StringArray>(data->field(3));
            std::shared_ptr<arrow::StringArray> partitions =
                std::dynamic_pointer_cast<arrow::StringArray>(data->field(4));
            if (!row_kinds || !offsets || !ids || !payloads || !partitions) {
                return Status::Invalid("unexpected real-time test read field type");
            }
            for (int64_t i = 0; i < data->length(); ++i) {
                if (row_kinds->IsNull(i) ||
                    row_kinds->Value(i) != static_cast<int8_t>(RecordBatch::RowKind::INSERT) ||
                    offsets->IsNull(i) || ids->IsNull(i) || payloads->IsNull(i) ||
                    partitions->IsNull(i)) {
                    return Status::Invalid("unexpected null or row kind in real-time test result");
                }
                rows.emplace_back(offsets->Value(i), ids->Value(i), payloads->GetString(i),
                                  partitions->GetString(i));
            }
        }
        return rows;
    }

    Result<RealtimeSnapshotProperties::OffsetMap> ReadCommittedOffsets() const {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_));
        SnapshotManager snapshot_manager(options.GetFileSystem(), table_path_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, snapshot_manager.LatestSnapshot());
        return RealtimeSnapshotProperties::ReadOffsets(snapshot, options.GetFileSystem());
    }

    void FinalizeCommitAndCheck(FileStoreWrite* writer,
                                std::vector<RealtimeCommitProgress> realtime_commits,
                                int64_t prepare_identifier, std::vector<Row> expected_rows) const {
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> final_commits,
                             writer->PrepareCommitWithProgress(prepare_identifier));
        realtime_commits.insert(realtime_commits.end(),
                                std::make_move_iterator(final_commits.begin()),
                                std::make_move_iterator(final_commits.end()));
        // Verify that commit orders prepared offset ranges instead of relying on caller order.
        std::reverse(realtime_commits.begin(), realtime_commits.end());
        ASSERT_OK(Commit(realtime_commits, prepare_identifier));
        ASSERT_OK(writer->Close());

        ASSERT_OK_AND_ASSIGN(std::vector<ReadRow> actual_rows, ReadRows());
        ASSERT_EQ(WithExpectedOffsets(expected_rows), actual_rows);
    }

    void RunConcurrentPrepareTest(int32_t prepare_thread_count) {
        CreateTable(/*partition_keys=*/{});
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());

        constexpr int64_t kBatchCount = 50;
        constexpr int64_t kRowsPerBatch = 4;
        std::vector<Row> expected_rows = MakeRows(
            /*first_id=*/0, kBatchCount * kRowsPerBatch, /*partition=*/"p0");

        std::atomic<bool> start{false};
        std::atomic<bool> writer_done{false};
        std::atomic<int32_t> ready_threads{0};
        std::atomic<int64_t> next_identifier{0};
        std::mutex result_mutex;
        std::vector<RealtimeCommitProgress> realtime_commits;
        std::vector<std::string> errors;
        std::vector<int32_t> prepare_call_counts(prepare_thread_count, 0);

        auto record_error = [&](const Status& status) {
            std::lock_guard<std::mutex> lock(result_mutex);
            errors.push_back(status.ToString());
        };
        auto append_commits = [&](std::vector<RealtimeCommitProgress>&& commits) {
            std::lock_guard<std::mutex> lock(result_mutex);
            realtime_commits.insert(realtime_commits.end(),
                                    std::make_move_iterator(commits.begin()),
                                    std::make_move_iterator(commits.end()));
        };

        std::thread write_thread([&]() {
            ++ready_threads;
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int64_t batch_index = 0; batch_index < kBatchCount; ++batch_index) {
                std::vector<Row> rows = MakeRows(batch_index * kRowsPerBatch, kRowsPerBatch, "p0");
                Result<std::unique_ptr<RecordBatch>> batch_result =
                    MakeBatch(rows, /*partitioned=*/false);
                if (!batch_result.ok()) {
                    record_error(batch_result.status());
                    break;
                }
                Status status = writer->Write(std::move(batch_result).value());
                if (!status.ok()) {
                    record_error(status);
                    break;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            writer_done.store(true, std::memory_order_release);
        });

        std::vector<std::thread> prepare_threads;
        prepare_threads.reserve(prepare_thread_count);
        for (int32_t thread_index = 0; thread_index < prepare_thread_count; ++thread_index) {
            prepare_threads.emplace_back([&, thread_index]() {
                ++ready_threads;
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                do {
                    int64_t identifier = next_identifier.fetch_add(1);
                    Result<std::vector<RealtimeCommitProgress>> result =
                        writer->PrepareCommitWithProgress(identifier);
                    ++prepare_call_counts[thread_index];
                    if (!result.ok()) {
                        record_error(result.status());
                        break;
                    }
                    append_commits(std::move(result).value());
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                } while (!writer_done.load(std::memory_order_acquire));
            });
        }

        while (ready_threads.load(std::memory_order_acquire) < prepare_thread_count + 1) {
            std::this_thread::yield();
        }
        start.store(true, std::memory_order_release);

        write_thread.join();
        for (std::thread& thread : prepare_threads) {
            thread.join();
        }

        ASSERT_TRUE(errors.empty()) << (errors.empty() ? "" : errors.front());
        for (int32_t call_count : prepare_call_counts) {
            ASSERT_GT(call_count, 0);
        }
        FinalizeCommitAndCheck(writer.get(), std::move(realtime_commits),
                               next_identifier.fetch_add(1), std::move(expected_rows));
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string table_path_;
    std::string commit_user_ = "realtime_commit_user";
    arrow::FieldVector fields_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
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

TEST_F(RealtimeWriteInteTest, TestMultiplePartitions) {
    CreateTable(/*partition_keys=*/{"pt"});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());
    std::vector<Row> expected_rows;
    for (int64_t partition_index = 0; partition_index < 3; ++partition_index) {
        std::string partition = "p" + std::to_string(partition_index);
        std::vector<Row> rows = MakeRows(partition_index * 10, /*count=*/10, partition);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/true));
        ASSERT_OK(writer->Write(std::move(batch)));
        expected_rows.insert(expected_rows.end(), rows.begin(), rows.end());
    }
    FinalizeCommitAndCheck(writer.get(), /*realtime_commits=*/{}, /*prepare_identifier=*/0,
                           std::move(expected_rows));
    ASSERT_OK_AND_ASSIGN(RealtimeSnapshotProperties::OffsetMap committed_offsets,
                         ReadCommittedOffsets());
    ASSERT_EQ(3, committed_offsets.size());
    for (int64_t partition_index = 0; partition_index < 3; ++partition_index) {
        std::map<std::string, std::string> partition = {
            {"pt", "p" + std::to_string(partition_index)}};
        PartitionBucket partition_bucket(partition, /*bucket=*/0);
        ASSERT_EQ(9, committed_offsets.at(partition_bucket));
    }
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

    PartitionBucket partition_bucket(/*partition=*/{}, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(RealtimeSnapshotProperties::OffsetMap second_committed_offsets,
                         ReadCommittedOffsets());
    ASSERT_EQ(4, second_committed_offsets.at(partition_bucket));
}

TEST_F(RealtimeWriteInteTest, TestConcurrentWriteAndPrepareCommit) {
    RunConcurrentPrepareTest(/*prepare_thread_count=*/1);
}

TEST_F(RealtimeWriteInteTest, TestConcurrentWriteAndMultiplePrepareCommitThreads) {
    RunConcurrentPrepareTest(/*prepare_thread_count=*/4);
}

}  // namespace paimon::test

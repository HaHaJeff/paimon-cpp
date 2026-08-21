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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
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
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {
namespace {

std::map<std::string, std::string> BaseOptions() {
    return {{Options::MANIFEST_FORMAT, "orc"}, {Options::FILE_FORMAT, "orc"},
            {Options::FILE_SYSTEM, "local"},   {Options::BUCKET, "1"},
            {Options::BUCKET_KEY, "id"},       {Options::TARGET_FILE_SIZE, "1MB"}};
}

std::map<std::string, std::string> DataEvolutionOptions() {
    return {{Options::MANIFEST_FORMAT, "orc"},       {Options::FILE_FORMAT, "orc"},
            {Options::FILE_SYSTEM, "local"},         {Options::TARGET_FILE_SIZE, "1MB"},
            {Options::ROW_TRACKING_ENABLED, "true"}, {Options::DATA_EVOLUTION_ENABLED, "true"}};
}

arrow::FieldVector BaseFields() {
    return {arrow::field("id", arrow::int64()), arrow::field("payload", arrow::utf8())};
}

arrow::FieldVector EvolvedFields() {
    return {arrow::field("id", arrow::int64()), arrow::field("payload", arrow::utf8()),
            arrow::field("extra", arrow::int32())};
}

arrow::FieldVector DataEvolutionFields() {
    return {arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::utf8()),
            arrow::field("f2", arrow::utf8())};
}

Result<std::unique_ptr<RecordBatch>> MakeBatch(
    const arrow::FieldVector& fields, const std::string& json,
    const std::map<std::string, std::string>& partition, int32_t bucket,
    const std::vector<RecordBatch::RowKind>& row_kinds = {}) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> array,
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), json));
    ArrowArray c_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
    RecordBatchBuilder builder(&c_array);
    return builder.SetPartition(partition).SetBucket(bucket).SetRowKinds(row_kinds).Finish();
}

Result<std::unique_ptr<RecordBatch>> MakeUnbucketedBatch(
    const arrow::FieldVector& fields, const std::string& json,
    const std::map<std::string, std::string>& partition,
    const std::vector<RecordBatch::RowKind>& row_kinds = {}) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> array,
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), json));
    ArrowArray c_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
    RecordBatchBuilder builder(&c_array);
    return builder.SetPartition(partition).SetRowKinds(row_kinds).Finish();
}

Result<std::unique_ptr<FileStoreWrite>> CreateWriter(
    const std::string& table_path, const std::map<std::string, std::string>& options,
    const std::shared_ptr<RealtimeContext>& realtime_context = nullptr,
    const std::vector<std::string>& write_schema = {}) {
    WriteContextBuilder builder(table_path, "schema_evolution_verify");
    builder.SetOptions(options).WithStreamingMode(true);
    if (realtime_context) {
        builder.WithRealtimeContext(realtime_context);
    }
    if (!write_schema.empty()) {
        builder.WithWriteSchema(write_schema);
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> context, builder.Finish());
    return FileStoreWrite::Create(std::move(context));
}

Result<std::vector<std::shared_ptr<CommitMessage>>> WriteWithNewWriter(
    const std::string& table_path, const std::map<std::string, std::string>& options,
    std::unique_ptr<RecordBatch> batch, int64_t commit_identifier,
    const std::vector<std::string>& write_schema = {}) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> writer,
                           CreateWriter(table_path, options, nullptr, write_schema));
    PAIMON_RETURN_NOT_OK(writer->Write(std::move(batch)));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> messages,
                           writer->PrepareCommit(/*wait_compaction=*/false, commit_identifier));
    PAIMON_RETURN_NOT_OK(writer->Close());
    return messages;
}

Result<std::unique_ptr<FileStoreCommit>> CreateCommit(
    const std::string& table_path, const std::map<std::string, std::string>& options) {
    CommitContextBuilder builder(table_path, "schema_evolution_verify");
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                           builder.SetOptions(options).IgnoreEmptyCommit(false).Finish());
    return FileStoreCommit::Create(std::move(context));
}

Status CommitMessages(const std::string& table_path,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::shared_ptr<CommitMessage>>& messages,
                      int64_t commit_identifier) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                           CreateCommit(table_path, options));
    return commit->Commit(messages, commit_identifier);
}

Result<int64_t> CommitRealtimeMessages(const std::string& table_path,
                                       const std::map<std::string, std::string>& options,
                                       const std::vector<RealtimeCommitProgress>& messages,
                                       int64_t commit_identifier) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                           CreateCommit(table_path, options));
    return commit->CommitWithProgress(messages, commit_identifier, /*watermark=*/std::nullopt);
}

Result<std::optional<Snapshot>> LatestSnapshot(const std::string& table_path,
                                               const std::map<std::string, std::string>& options,
                                               const std::shared_ptr<FileSystem>& file_system) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options, file_system));
    SnapshotManager snapshot_manager(core_options.GetFileSystem(), table_path);
    return snapshot_manager.LatestSnapshot();
}

Result<std::shared_ptr<Plan>> ScanTable(
    const std::string& table_path, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<RealtimeContext>& realtime_context = nullptr,
    const std::shared_ptr<Predicate>& predicate = nullptr) {
    ScanContextBuilder scan_builder(table_path);
    scan_builder.SetOptions(options)
        .AddOption(Options::SCAN_MODE, StartupMode::LatestFull().ToString())
        .SetPredicate(predicate)
        .WithMemoryPool(pool);
    if (realtime_context) {
        scan_builder.WithRealtimeContext(realtime_context);
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> table_scan,
                           TableScan::Create(std::move(scan_context)));
    return table_scan->CreatePlan();
}

std::vector<std::shared_ptr<DataFileMeta>> DataFilesFromPlan(const std::shared_ptr<Plan>& plan) {
    std::vector<std::shared_ptr<DataFileMeta>> files;
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        std::shared_ptr<Split> data_split = split;
        if (std::shared_ptr<IndexedSplitImpl> indexed_split =
                std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
            data_split = indexed_split->GetDataSplit();
        }
        std::shared_ptr<DataSplitImpl> split_impl =
            std::dynamic_pointer_cast<DataSplitImpl>(data_split);
        if (!split_impl) {
            continue;
        }
        const std::vector<std::shared_ptr<DataFileMeta>>& split_files = split_impl->DataFiles();
        files.insert(files.end(), split_files.begin(), split_files.end());
    }
    return files;
}

size_t CountIndexedSplits(const std::shared_ptr<Plan>& plan) {
    size_t count = 0;
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        if (std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
            count++;
        }
    }
    return count;
}

Status EvolveSchema(const std::string& table_path, const std::shared_ptr<FileSystem>& file_system,
                    const std::vector<DataField>& fields, int32_t highest_field_id,
                    const std::map<std::string, std::string>& options) {
    return TestHelper::WriteNextSchema(file_system, table_path, fields, highest_field_id, options);
}

void AssignFirstRowId(const std::vector<std::shared_ptr<CommitMessage>>& messages,
                      int64_t first_row_id) {
    for (const std::shared_ptr<CommitMessage>& commit_message : messages) {
        std::shared_ptr<CommitMessageImpl> message =
            std::dynamic_pointer_cast<CommitMessageImpl>(commit_message);
        ASSERT_TRUE(message);
        for (const std::shared_ptr<DataFileMeta>& file :
             message->GetNewFilesIncrement().NewFiles()) {
            file->AssignFirstRowId(first_row_id);
        }
    }
}

struct CollectedReadResult {
    std::unique_ptr<TableRead> table_read;
    std::unique_ptr<BatchReader> reader;
    std::shared_ptr<arrow::ChunkedArray> data;
};

Result<CollectedReadResult> ReadRows(
    const std::string& table_path, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<RealtimeContext>& realtime_context = nullptr,
    const std::shared_ptr<Predicate>& predicate = nullptr, bool enable_predicate_filter = true) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan,
                           ScanTable(table_path, options, pool, realtime_context, predicate));

    ReadContextBuilder read_builder(table_path);
    read_builder.SetOptions(options)
        .SetPredicate(predicate)
        .EnablePredicateFilter(enable_predicate_filter)
        .WithMemoryPool(pool);
    if (realtime_context) {
        read_builder.WithRealtimeContext(realtime_context);
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           TableRead::Create(std::move(read_context)));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> batch_reader,
                           table_read->CreateReader(plan->Splits()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> rows,
                           ReadResultCollector::CollectResult(batch_reader.get()));
    return CollectedReadResult{std::move(table_read), std::move(batch_reader), std::move(rows)};
}

void AssertResultEquals(const std::shared_ptr<arrow::ChunkedArray>& actual,
                        const arrow::FieldVector& fields, const std::string& expected_json) {
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    std::shared_ptr<arrow::Array> expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_with_row_kind),
                                                  expected_json)
            .ValueOrDie();
    auto expected = std::make_shared<arrow::ChunkedArray>(expected_array);
    ASSERT_TRUE(expected->Equals(actual, arrow::EqualOptions::Defaults().diff_sink(&std::cout)))
        << actual->ToString();
}

Status CreateTable(const std::string& warehouse, const std::shared_ptr<arrow::Schema>& schema,
                   const std::vector<std::string>& primary_keys,
                   const std::map<std::string, std::string>& options) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Catalog> catalog, Catalog::Create(warehouse, options));
    PAIMON_RETURN_NOT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/false));
    ArrowSchema c_schema;
    ArrowSchemaMarkReleased(&c_schema);
    ScopeGuard guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
    return catalog->CreateTable(Identifier("foo", "bar"), &c_schema,
                                /*partition_keys=*/{}, primary_keys, options,
                                /*ignore_if_exists=*/false);
}

Result<std::unique_ptr<FileIndexFormat::Reader>> CreateFileIndexReader(
    const std::shared_ptr<DataFileMeta>& data_file, const std::shared_ptr<MemoryPool>& pool) {
    if (data_file->embedded_index == nullptr) {
        return Status::Invalid("data file does not contain an embedded file index");
    }
    auto input = std::make_shared<ByteArrayInputStream>(data_file->embedded_index->data(),
                                                        data_file->embedded_index->size());
    return FileIndexFormat::CreateReader(input, pool);
}

Result<std::vector<std::shared_ptr<FileIndexReader>>> ReadEmbeddedIndexColumn(
    const std::shared_ptr<DataFileMeta>& data_file, const std::shared_ptr<arrow::Schema>& schema,
    const std::string& column, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexFormat::Reader> reader,
                           CreateFileIndexReader(data_file, pool));
    auto c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, c_schema.get()));
    return reader->ReadColumnIndex(column, c_schema.get());
}

class SchemaEvolutionWriteVerifyTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create("local");
        ASSERT_TRUE(dir_);
        table_path_ = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    }

    void TearDown() override {
        dir_.reset();
    }

    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string table_path_;
};

TEST_F(SchemaEvolutionWriteVerifyTest,
       NonRealtimeAppendOldWriterCommitsOldSchemaFileIntoNewSchemaSnapshot) {
    std::map<std::string, std::string> options = BaseOptions();
    options["file-index.bitmap.columns"] = "payload";
    options[Options::FILE_INDEX_IN_MANIFEST_THRESHOLD] = "1MB";
    ASSERT_OK(CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{}, options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> old_schema_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"], [2, "skip"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(old_schema_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         old_writer->PrepareCommit(/*wait_compaction=*/false,
                                                   /*commit_identifier=*/1));
    ASSERT_EQ(1, messages.size());
    std::shared_ptr<CommitMessageImpl> message =
        std::dynamic_pointer_cast<CommitMessageImpl>(messages[0]);
    ASSERT_TRUE(message);
    ASSERT_EQ(1, message->GetNewFilesIncrement().NewFiles().size());
    std::shared_ptr<DataFileMeta> old_file = message->GetNewFilesIncrement().NewFiles()[0];
    ASSERT_EQ(0, old_file->schema_id);
    ASSERT_TRUE(old_file->embedded_index);
    ASSERT_TRUE(old_file->extra_files.empty());
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<FileIndexReader>> payload_indexes,
        ReadEmbeddedIndexColumn(old_file, arrow::schema(BaseFields()), "payload", pool_));
    ASSERT_EQ(1, payload_indexes.size());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> payload_hit,
                         payload_indexes[0]->VisitEqual(Literal(FieldType::STRING, "old", 3)));
    ASSERT_OK_AND_ASSIGN(bool payload_remain, payload_hit->IsRemain());
    ASSERT_TRUE(payload_remain);

    ASSERT_OK(CommitMessages(table_path_, options, messages, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         LatestSnapshot(table_path_, options, dir_->GetFileSystem()));
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(1, snapshot->SchemaId());

    ASSERT_OK_AND_ASSIGN(CollectedReadResult all_rows, ReadRows(table_path_, options, pool_));
    AssertResultEquals(all_rows.data, EvolvedFields(),
                       R"([[0, 1, "old", null], [0, 2, "skip", null]])");

    auto predicate = PredicateBuilder::Equal(
        /*field_index=*/1, /*field_name=*/"payload", FieldType::STRING,
        Literal(FieldType::STRING, "old", 3));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult filtered_rows,
                         ReadRows(table_path_, options, pool_, /*realtime_context=*/nullptr,
                                  predicate, /*enable_predicate_filter=*/false));
    AssertResultEquals(filtered_rows.data, EvolvedFields(), R"([[0, 1, "old", null]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, NonRealtimeAppendNewWriterIndexesNewSchemaColumn) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{}, options));

    std::map<std::string, std::string> options_v1 = options;
    options_v1["file-index.bitmap.columns"] = "extra";
    options_v1[Options::FILE_INDEX_IN_MANIFEST_THRESHOLD] = "1MB";
    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options_v1));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_schema_batch,
                         MakeBatch(EvolvedFields(), R"([[1, "new", 20], [2, "skip", 30]])",
                                   /*partition=*/{}, /*bucket=*/0));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         WriteWithNewWriter(table_path_, options_v1, std::move(new_schema_batch),
                                            /*commit_identifier=*/1));
    ASSERT_EQ(1, messages.size());
    std::shared_ptr<CommitMessageImpl> message =
        std::dynamic_pointer_cast<CommitMessageImpl>(messages[0]);
    ASSERT_TRUE(message);
    std::shared_ptr<DataFileMeta> new_file = message->GetNewFilesIncrement().NewFiles()[0];
    ASSERT_EQ(1, new_file->schema_id);
    ASSERT_TRUE(new_file->embedded_index);
    ASSERT_TRUE(new_file->extra_files.empty());
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<FileIndexReader>> extra_indexes,
        ReadEmbeddedIndexColumn(new_file, arrow::schema(EvolvedFields()), "extra", pool_));
    ASSERT_EQ(1, extra_indexes.size());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> extra_hit,
                         extra_indexes[0]->VisitEqual(Literal(20)));
    ASSERT_OK_AND_ASSIGN(bool extra_remain, extra_hit->IsRemain());
    ASSERT_TRUE(extra_remain);

    ASSERT_OK(CommitMessages(table_path_, options_v1, messages, /*commit_identifier=*/1));
    std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
        /*field_index=*/2, /*field_name=*/"extra", FieldType::INT, Literal(20));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult rows,
                         ReadRows(table_path_, options_v1, pool_, /*realtime_context=*/nullptr,
                                  predicate, /*enable_predicate_filter=*/true));
    AssertResultEquals(rows.data, EvolvedFields(), R"([[0, 1, "new", 20]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, NonRealtimeAppendDataEvolutionWritesPartialNewColumnIndex) {
    std::map<std::string, std::string> options = DataEvolutionOptions();
    arrow::FieldVector fields = DataEvolutionFields();
    ASSERT_OK(CreateTable(dir_->Str(), arrow::schema(fields), /*primary_keys=*/{}, options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> old_schema_batch,
                         MakeUnbucketedBatch(fields, R"([[1, "old", "base"]])",
                                             /*partition=*/{}));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> old_messages,
                         WriteWithNewWriter(table_path_, options, std::move(old_schema_batch),
                                            /*commit_identifier=*/1,
                                            /*write_schema=*/{"f0", "f1", "f2"}));
    ASSERT_OK(CommitMessages(table_path_, options, old_messages, /*commit_identifier=*/1));

    std::map<std::string, std::string> options_v1 = DataEvolutionOptions();
    options_v1["file-index.bitmap.columns"] = "f2";
    options_v1[Options::FILE_INDEX_IN_MANIFEST_THRESHOLD] = "1MB";
    ASSERT_OK(
        EvolveSchema(table_path_, dir_->GetFileSystem(),
                     {DataField(0, fields[0]), DataField(1, fields[1]), DataField(2, fields[2])},
                     /*highest_field_id=*/2, options_v1));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> partial_batch,
                         MakeUnbucketedBatch({fields[2]}, R"([["updated"]])",
                                             /*partition=*/{}));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> new_messages,
                         WriteWithNewWriter(table_path_, options_v1, std::move(partial_batch),
                                            /*commit_identifier=*/2,
                                            /*write_schema=*/{"f2"}));
    ASSERT_EQ(1, new_messages.size());
    std::shared_ptr<CommitMessageImpl> new_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(new_messages[0]);
    ASSERT_TRUE(new_message);
    std::shared_ptr<DataFileMeta> new_file = new_message->GetNewFilesIncrement().NewFiles()[0];
    ASSERT_EQ(1, new_file->schema_id);
    const std::optional<std::vector<std::string>> expected_write_cols =
        std::vector<std::string>{"f2"};
    ASSERT_EQ(expected_write_cols, new_file->write_cols);
    ASSERT_TRUE(new_file->embedded_index);
    ASSERT_TRUE(new_file->extra_files.empty());
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<FileIndexReader>> f2_indexes,
        ReadEmbeddedIndexColumn(new_file, arrow::schema({fields[2]}), "f2", pool_));
    ASSERT_EQ(1, f2_indexes.size());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> f2_hit,
                         f2_indexes[0]->VisitEqual(Literal(FieldType::STRING, "updated", 7)));
    ASSERT_OK_AND_ASSIGN(bool f2_remain, f2_hit->IsRemain());
    ASSERT_TRUE(f2_remain);

    AssignFirstRowId(new_messages, /*first_row_id=*/0);
    ASSERT_OK(CommitMessages(table_path_, options_v1, new_messages, /*commit_identifier=*/2));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult all_rows, ReadRows(table_path_, options_v1, pool_));
    AssertResultEquals(all_rows.data, fields, R"([[0, 1, "old", "updated"]])");

    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                Literal(FieldType::STRING, "updated", 7));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult filtered_rows,
                         ReadRows(table_path_, options_v1, pool_, /*realtime_context=*/nullptr,
                                  predicate, /*enable_predicate_filter=*/false));
    AssertResultEquals(filtered_rows.data, fields, R"([[0, 1, "old", "updated"]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, NonRealtimePkOldAndNewSchemaFilesReadThroughLatestSchema) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"}, options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> base_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(base_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> base_messages,
                         old_writer->PrepareCommit(/*wait_compaction=*/false,
                                                   /*commit_identifier=*/1));
    ASSERT_OK(CommitMessages(table_path_, options, base_messages, /*commit_identifier=*/1));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> new_writer,
                         CreateWriter(table_path_, options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_schema_batch,
                         MakeBatch(EvolvedFields(), R"([[2, "new", 20]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(new_writer->Write(std::move(new_schema_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> new_messages,
                         new_writer->PrepareCommit(/*wait_compaction=*/false,
                                                   /*commit_identifier=*/2));
    ASSERT_EQ(1, new_messages.size());
    std::shared_ptr<CommitMessageImpl> new_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(new_messages[0]);
    ASSERT_TRUE(new_message);
    ASSERT_EQ(1, new_message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_EQ(1, new_message->GetNewFilesIncrement().NewFiles()[0]->schema_id);
    ASSERT_TRUE(new_message->GetNewFilesIncrement().NewFiles()[0]->extra_files.empty());

    ASSERT_OK(CommitMessages(table_path_, options, new_messages, /*commit_identifier=*/2));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> stale_schema_batch,
                         MakeBatch(BaseFields(), R"([[3, "stale"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(stale_schema_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> stale_messages,
                         old_writer->PrepareCommit(/*wait_compaction=*/false,
                                                   /*commit_identifier=*/3));
    ASSERT_EQ(1, stale_messages.size());
    std::shared_ptr<CommitMessageImpl> stale_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(stale_messages[0]);
    ASSERT_TRUE(stale_message);
    ASSERT_EQ(1, stale_message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_EQ(0, stale_message->GetNewFilesIncrement().NewFiles()[0]->schema_id);
    ASSERT_OK(CommitMessages(table_path_, options, stale_messages, /*commit_identifier=*/3));

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         LatestSnapshot(table_path_, options, dir_->GetFileSystem()));
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(1, snapshot->SchemaId());

    ASSERT_OK_AND_ASSIGN(CollectedReadResult rows, ReadRows(table_path_, options, pool_));
    AssertResultEquals(rows.data, EvolvedFields(),
                       R"([[0, 1, "old", null], [0, 2, "new", 20], [0, 3, "stale", null]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, NonRealtimePkOldWriterCanOverwriteNewColumnWithNull) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"}, options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_schema_batch,
                         MakeBatch(EvolvedFields(), R"([[1, "new", 20]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> new_messages,
                         WriteWithNewWriter(table_path_, options, std::move(new_schema_batch),
                                            /*commit_identifier=*/1));
    ASSERT_OK(CommitMessages(table_path_, options, new_messages, /*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> old_schema_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(old_schema_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> old_messages,
                         old_writer->PrepareCommit(/*wait_compaction=*/false,
                                                   /*commit_identifier=*/2));
    ASSERT_OK(CommitMessages(table_path_, options, old_messages, /*commit_identifier=*/2));

    ASSERT_OK_AND_ASSIGN(CollectedReadResult rows, ReadRows(table_path_, options, pool_));
    AssertResultEquals(rows.data, EvolvedFields(), R"([[0, 1, "old", null]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, NonRealtimePkNewWriterIndexesNewSchemaColumn) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"}, options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> old_schema_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> old_messages,
                         WriteWithNewWriter(table_path_, options, std::move(old_schema_batch),
                                            /*commit_identifier=*/1));
    ASSERT_OK(CommitMessages(table_path_, options, old_messages, /*commit_identifier=*/1));

    std::map<std::string, std::string> options_v1 = options;
    options_v1["file-index.bitmap.columns"] = "extra";
    options_v1[Options::FILE_INDEX_IN_MANIFEST_THRESHOLD] = "1B";
    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options_v1));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_schema_batch,
                         MakeBatch(EvolvedFields(), R"([[2, "new", 20], [3, "skip", 30]])",
                                   /*partition=*/{}, /*bucket=*/0));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> new_messages,
                         WriteWithNewWriter(table_path_, options_v1, std::move(new_schema_batch),
                                            /*commit_identifier=*/2));
    ASSERT_EQ(1, new_messages.size());
    std::shared_ptr<CommitMessageImpl> message =
        std::dynamic_pointer_cast<CommitMessageImpl>(new_messages[0]);
    ASSERT_TRUE(message);
    ASSERT_EQ(1, message->GetNewFilesIncrement().NewFiles().size());
    std::shared_ptr<DataFileMeta> new_file = message->GetNewFilesIncrement().NewFiles()[0];
    ASSERT_EQ(1, new_file->schema_id);
    ASSERT_FALSE(new_file->embedded_index);
    ASSERT_EQ(1, new_file->extra_files.size());
    ASSERT_TRUE(new_file->extra_files[0]);
    std::string index_path =
        PathUtil::JoinPath(table_path_, "bucket-0/" + new_file->extra_files[0].value());
    ASSERT_OK_AND_ASSIGN(bool index_exists, dir_->GetFileSystem()->Exists(index_path));
    ASSERT_TRUE(index_exists);

    ASSERT_OK(CommitMessages(table_path_, options_v1, new_messages, /*commit_identifier=*/2));
    std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
        /*field_index=*/2, /*field_name=*/"extra", FieldType::INT, Literal(20));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult rows,
                         ReadRows(table_path_, options_v1, pool_, /*realtime_context=*/nullptr,
                                  predicate, /*enable_predicate_filter=*/true));
    AssertResultEquals(rows.data, EvolvedFields(), R"([[0, 2, "new", 20]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, NonRealtimePkEmbeddedFileIndexFailsValueScan) {
    std::map<std::string, std::string> options = BaseOptions();
    options["file-index.bitmap.columns"] = "payload";
    options[Options::FILE_INDEX_IN_MANIFEST_THRESHOLD] = "1MB";
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"}, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(BaseFields(), R"([[1, "a"], [2, "b"]])",
                                   /*partition=*/{}, /*bucket=*/0));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         WriteWithNewWriter(table_path_, options, std::move(batch),
                                            /*commit_identifier=*/1));
    ASSERT_EQ(1, messages.size());
    std::shared_ptr<CommitMessageImpl> message =
        std::dynamic_pointer_cast<CommitMessageImpl>(messages[0]);
    ASSERT_TRUE(message);
    ASSERT_EQ(1, message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_TRUE(message->GetNewFilesIncrement().NewFiles()[0]->embedded_index);
    ASSERT_OK(CommitMessages(table_path_, options, messages, /*commit_identifier=*/1));

    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"payload", FieldType::STRING,
                                Literal(FieldType::STRING, "a", 1));
    ASSERT_NOK_WITH_MSG(
        ScanTable(table_path_, options, pool_, /*realtime_context=*/nullptr, predicate),
        "do not support embedded index in DataFileMeta");
}

TEST_F(SchemaEvolutionWriteVerifyTest, NonRealtimePkSortedIndexConfigDoesNotWriteIndexOnDataWrite) {
    std::map<std::string, std::string> options = BaseOptions();
    options[Options::PK_BTREE_INDEX_COLUMNS] = "payload";
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"}, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(BaseFields(), R"([[1, "a"], [2, "b"]])",
                                   /*partition=*/{}, /*bucket=*/0));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         WriteWithNewWriter(table_path_, options, std::move(batch),
                                            /*commit_identifier=*/1));
    ASSERT_EQ(1, messages.size());
    std::shared_ptr<CommitMessageImpl> message =
        std::dynamic_pointer_cast<CommitMessageImpl>(messages[0]);
    ASSERT_TRUE(message);
    ASSERT_TRUE(message->GetNewFilesIncrement().NewIndexFiles().empty());
    ASSERT_TRUE(message->GetCompactIncrement().NewIndexFiles().empty());
    ASSERT_OK(CommitMessages(table_path_, options, messages, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         LatestSnapshot(table_path_, options, dir_->GetFileSystem()));
    ASSERT_TRUE(snapshot);
    ASSERT_FALSE(snapshot->IndexManifest());

    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"payload", FieldType::STRING,
                                Literal(FieldType::STRING, "a", 1));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         ScanTable(table_path_, options, pool_,
                                   /*realtime_context=*/nullptr, predicate));
    ASSERT_EQ(0, CountIndexedSplits(plan));
    std::vector<std::shared_ptr<DataFileMeta>> planned_files = DataFilesFromPlan(plan);
    ASSERT_EQ(1, planned_files.size());
    ASSERT_EQ(0, planned_files[0]->schema_id);
    ASSERT_OK_AND_ASSIGN(CollectedReadResult rows,
                         ReadRows(table_path_, options, pool_, /*realtime_context=*/nullptr,
                                  predicate, /*enable_predicate_filter=*/true));
    AssertResultEquals(rows.data, BaseFields(), R"([[0, 1, "a"]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, RealtimeAppendRejectsDataEvolutionAtWriterCreation) {
    std::map<std::string, std::string> create_options = BaseOptions();
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{}, create_options));

    std::map<std::string, std::string> write_options = BaseOptions();
    write_options[Options::DATA_EVOLUTION_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_NOK_WITH_MSG(CreateWriter(table_path_, write_options, realtime_context),
                        "real-time append write does not support data evolution");
}

TEST_F(SchemaEvolutionWriteVerifyTest, RealtimeAppendScanRejectsDataEvolutionTable) {
    std::map<std::string, std::string> options = DataEvolutionOptions();
    arrow::FieldVector fields = DataEvolutionFields();
    ASSERT_OK(CreateTable(dir_->Str(), arrow::schema(fields), /*primary_keys=*/{}, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeUnbucketedBatch(fields, R"([[1, "old", "base"]])",
                                             /*partition=*/{}));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         WriteWithNewWriter(table_path_, options, std::move(batch),
                                            /*commit_identifier=*/1,
                                            /*write_schema=*/{"f0", "f1", "f2"}));
    ASSERT_OK(CommitMessages(table_path_, options, messages, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());

    ASSERT_NOK_WITH_MSG(ScanTable(table_path_, options, pool_, realtime_context),
                        "real-time union read requires fixed bucket mode");

    std::map<std::string, std::string> fixed_bucket_options = options;
    fixed_bucket_options[Options::BUCKET] = "1";
    ASSERT_NOK_WITH_MSG(ScanTable(table_path_, fixed_bucket_options, pool_, realtime_context),
                        "real-time union read does not support data evolution");
}

TEST_F(SchemaEvolutionWriteVerifyTest, RealtimeAppendReuseContextKeepsOldMemorySchemaAfterAlter) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{}, options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options, realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> base_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(base_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> base_progress,
                         old_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id,
                         CommitRealtimeMessages(table_path_, options, base_progress,
                                                /*commit_identifier=*/1));
    ASSERT_OK(old_writer->RefreshCommittedSnapshot(snapshot_id));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> reused_context_writer,
                         CreateWriter(table_path_, options, realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_schema_batch,
                         MakeBatch(EvolvedFields(), R"([[2, "new", 20]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_NOK_WITH_MSG(reused_context_writer->Write(std::move(new_schema_batch)),
                        "ArrowArray struct has 3 children, expected 2");

    ASSERT_OK_AND_ASSIGN(CollectedReadResult rows,
                         ReadRows(table_path_, options, pool_, realtime_context));
    AssertResultEquals(rows.data, EvolvedFields(), R"([[0, 1, "old", null]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, RealtimeAppendOldWriterAfterAlterCommitsOldSchemaFile) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{}, options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options, realtime_context));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> old_schema_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(old_schema_batch)));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult memory_rows,
                         ReadRows(table_path_, options, pool_, realtime_context));
    AssertResultEquals(memory_rows.data, EvolvedFields(), R"([[0, 1, "old", null]])");

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                         old_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, progress.size());
    std::shared_ptr<CommitMessageImpl> message =
        std::dynamic_pointer_cast<CommitMessageImpl>(progress[0].commit_message);
    ASSERT_TRUE(message);
    ASSERT_EQ(1, message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_EQ(0, message->GetNewFilesIncrement().NewFiles()[0]->schema_id);

    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, CommitRealtimeMessages(table_path_, options, progress,
                                                                     /*commit_identifier=*/1));
    ASSERT_OK(old_writer->RefreshCommittedSnapshot(snapshot_id));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         LatestSnapshot(table_path_, options, dir_->GetFileSystem()));
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(1, snapshot->SchemaId());
    ASSERT_OK_AND_ASSIGN(CollectedReadResult disk_rows, ReadRows(table_path_, options, pool_));
    AssertResultEquals(disk_rows.data, EvolvedFields(), R"([[0, 1, "old", null]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, RealtimeAppendNewContextUsesNewSchemaAfterAlter) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{}, options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> old_realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options, old_realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> base_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(base_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> base_progress,
                         old_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id,
                         CommitRealtimeMessages(table_path_, options, base_progress,
                                                /*commit_identifier=*/1));
    ASSERT_OK(old_writer->RefreshCommittedSnapshot(snapshot_id));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    std::map<std::string, std::string> options_v1 = options;
    options_v1["file-index.bitmap.columns"] = "extra";
    options_v1[Options::FILE_INDEX_IN_MANIFEST_THRESHOLD] = "1B";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> new_realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> new_writer,
                         CreateWriter(table_path_, options_v1, new_realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_schema_batch,
                         MakeBatch(EvolvedFields(), R"([[2, "new", 20]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(new_writer->Write(std::move(new_schema_batch)));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult memory_rows,
                         ReadRows(table_path_, options_v1, pool_, new_realtime_context));
    AssertResultEquals(memory_rows.data, EvolvedFields(),
                       R"([[0, 1, "old", null], [0, 2, "new", 20]])");

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> new_progress,
                         new_writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_EQ(1, new_progress.size());
    std::shared_ptr<CommitMessageImpl> message =
        std::dynamic_pointer_cast<CommitMessageImpl>(new_progress[0].commit_message);
    ASSERT_TRUE(message);
    ASSERT_EQ(1, message->GetNewFilesIncrement().NewFiles().size());
    std::shared_ptr<DataFileMeta> new_file = message->GetNewFilesIncrement().NewFiles()[0];
    ASSERT_EQ(1, new_file->schema_id);
    ASSERT_FALSE(new_file->embedded_index);
    ASSERT_EQ(1, new_file->extra_files.size());
    ASSERT_TRUE(new_file->extra_files[0]);
    std::string index_path =
        PathUtil::JoinPath(table_path_, "bucket-0/" + new_file->extra_files[0].value());
    ASSERT_OK_AND_ASSIGN(bool index_exists, dir_->GetFileSystem()->Exists(index_path));
    ASSERT_TRUE(index_exists);

    ASSERT_OK_AND_ASSIGN(snapshot_id, CommitRealtimeMessages(table_path_, options_v1, new_progress,
                                                             /*commit_identifier=*/2));
    ASSERT_OK(new_writer->RefreshCommittedSnapshot(snapshot_id));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult rows,
                         ReadRows(table_path_, options_v1, pool_, new_realtime_context));
    AssertResultEquals(rows.data, EvolvedFields(), R"([[0, 1, "old", null], [0, 2, "new", 20]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, RealtimePkRejectsDataEvolutionAtWriterCreation) {
    std::map<std::string, std::string> create_options = BaseOptions();
    ASSERT_OK(CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"},
                          create_options));

    std::map<std::string, std::string> write_options = BaseOptions();
    write_options[Options::DATA_EVOLUTION_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_NOK_WITH_MSG(CreateWriter(table_path_, write_options, realtime_context),
                        "PK realtime v1 does not support data evolution");
}

TEST_F(SchemaEvolutionWriteVerifyTest, RealtimePkReuseContextKeepsOldMemorySchemaAfterAlter) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"}, options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options, realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> base_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(base_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> base_progress,
                         old_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id,
                         CommitRealtimeMessages(table_path_, options, base_progress,
                                                /*commit_identifier=*/1));
    ASSERT_OK(old_writer->RefreshCommittedSnapshot(snapshot_id));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> reused_context_writer,
                         CreateWriter(table_path_, options, realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_schema_batch,
                         MakeBatch(EvolvedFields(), R"([[2, "new", 20]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_NOK_WITH_MSG(reused_context_writer->Write(std::move(new_schema_batch)),
                        "ArrowArray struct has 3 children, expected 2");

    ASSERT_OK_AND_ASSIGN(CollectedReadResult rows,
                         ReadRows(table_path_, options, pool_, realtime_context));
    AssertResultEquals(rows.data, EvolvedFields(), R"([[0, 1, "old", null]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, RealtimePkOldWriterAfterAlterReadsNewColumnAsNull) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"}, options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options, realtime_context));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> old_schema_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(old_schema_batch)));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult memory_rows,
                         ReadRows(table_path_, options, pool_, realtime_context));
    AssertResultEquals(memory_rows.data, EvolvedFields(), R"([[0, 1, "old", null]])");

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                         old_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, progress.size());
    std::shared_ptr<CommitMessageImpl> message =
        std::dynamic_pointer_cast<CommitMessageImpl>(progress[0].commit_message);
    ASSERT_TRUE(message);
    ASSERT_EQ(1, message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_EQ(0, message->GetNewFilesIncrement().NewFiles()[0]->schema_id);

    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, CommitRealtimeMessages(table_path_, options, progress,
                                                                     /*commit_identifier=*/1));
    ASSERT_OK(old_writer->RefreshCommittedSnapshot(snapshot_id));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult disk_rows, ReadRows(table_path_, options, pool_));
    AssertResultEquals(disk_rows.data, EvolvedFields(), R"([[0, 1, "old", null]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest,
       RealtimePkOldWriterAfterAlterCannotCommitBehindNewContextOffset) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"}, options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options, realtime_context));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> new_realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> new_writer,
                         CreateWriter(table_path_, options, new_realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_schema_batch,
                         MakeBatch(EvolvedFields(), R"([[1, "new", 20]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(new_writer->Write(std::move(new_schema_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> new_progress,
                         new_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id,
                         CommitRealtimeMessages(table_path_, options, new_progress,
                                                /*commit_identifier=*/1));
    ASSERT_OK(new_writer->RefreshCommittedSnapshot(snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> old_schema_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(old_schema_batch)));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult realtime_rows,
                         ReadRows(table_path_, options, pool_, realtime_context));
    AssertResultEquals(realtime_rows.data, EvolvedFields(), R"([[0, 1, "new", 20]])");
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> old_progress,
                         old_writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_NOK_WITH_MSG(CommitRealtimeMessages(table_path_, options, old_progress,
                                               /*commit_identifier=*/2),
                        "real-time commit offsets for bucket 0 are not contiguous");

    ASSERT_OK_AND_ASSIGN(CollectedReadResult disk_rows, ReadRows(table_path_, options, pool_));
    AssertResultEquals(disk_rows.data, EvolvedFields(), R"([[0, 1, "new", 20]])");
}

TEST_F(SchemaEvolutionWriteVerifyTest, RealtimePkNewContextUsesNewSchemaAfterAlter) {
    std::map<std::string, std::string> options = BaseOptions();
    ASSERT_OK(
        CreateTable(dir_->Str(), arrow::schema(BaseFields()), /*primary_keys=*/{"id"}, options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> old_realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> old_writer,
                         CreateWriter(table_path_, options, old_realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> base_batch,
                         MakeBatch(BaseFields(), R"([[1, "old"]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(old_writer->Write(std::move(base_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> base_progress,
                         old_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id,
                         CommitRealtimeMessages(table_path_, options, base_progress,
                                                /*commit_identifier=*/1));
    ASSERT_OK(old_writer->RefreshCommittedSnapshot(snapshot_id));

    ASSERT_OK(EvolveSchema(table_path_, dir_->GetFileSystem(),
                           {DataField(0, BaseFields()[0]), DataField(1, BaseFields()[1]),
                            DataField(2, EvolvedFields()[2])},
                           /*highest_field_id=*/2, options));

    std::map<std::string, std::string> options_v1 = options;
    options_v1["file-index.bitmap.columns"] = "extra";
    options_v1[Options::FILE_INDEX_IN_MANIFEST_THRESHOLD] = "1B";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> new_realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> new_writer,
                         CreateWriter(table_path_, options_v1, new_realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_schema_batch,
                         MakeBatch(EvolvedFields(), R"([[2, "new", 20]])", /*partition=*/{},
                                   /*bucket=*/0));
    ASSERT_OK(new_writer->Write(std::move(new_schema_batch)));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult memory_rows,
                         ReadRows(table_path_, options_v1, pool_, new_realtime_context));
    AssertResultEquals(memory_rows.data, EvolvedFields(),
                       R"([[0, 1, "old", null], [0, 2, "new", 20]])");

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> new_progress,
                         new_writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_EQ(1, new_progress.size());
    std::shared_ptr<CommitMessageImpl> message =
        std::dynamic_pointer_cast<CommitMessageImpl>(new_progress[0].commit_message);
    ASSERT_TRUE(message);
    ASSERT_EQ(1, message->GetNewFilesIncrement().NewFiles().size());
    std::shared_ptr<DataFileMeta> new_file = message->GetNewFilesIncrement().NewFiles()[0];
    ASSERT_EQ(1, new_file->schema_id);
    ASSERT_FALSE(new_file->embedded_index);
    ASSERT_EQ(1, new_file->extra_files.size());
    ASSERT_TRUE(new_file->extra_files[0]);
    std::string index_path =
        PathUtil::JoinPath(table_path_, "bucket-0/" + new_file->extra_files[0].value());
    ASSERT_OK_AND_ASSIGN(bool index_exists, dir_->GetFileSystem()->Exists(index_path));
    ASSERT_TRUE(index_exists);

    ASSERT_OK_AND_ASSIGN(snapshot_id, CommitRealtimeMessages(table_path_, options_v1, new_progress,
                                                             /*commit_identifier=*/2));
    ASSERT_OK(new_writer->RefreshCommittedSnapshot(snapshot_id));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult rows,
                         ReadRows(table_path_, options_v1, pool_, new_realtime_context));
    AssertResultEquals(rows.data, EvolvedFields(), R"([[0, 1, "old", null], [0, 2, "new", 20]])");
}

}  // namespace
}  // namespace paimon::test

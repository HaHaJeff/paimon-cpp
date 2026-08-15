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

#include "paimon/realtime/realtime_context.h"

#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/realtime/realtime_primary_key_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class TestingReadView : public MemReadView {
 public:
    explicit TestingReadView(const std::optional<Range>& offset_range)
        : offset_range_(offset_range) {}

    std::optional<Range> GetOffsetRange() const override {
        return offset_range_;
    }

 private:
    std::optional<Range> offset_range_;
};

class TestingMemIndexer : public MemIndexer {
 public:
    Status Write(RealtimeWriteBatch&& batch) override {
        if (!offset_range) {
            offset_range = batch.offset_range;
        } else {
            offset_range = Range(offset_range->from, batch.offset_range.to);
        }
        return Status::OK();
    }

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override {
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>&) override {
        return std::vector<std::unique_ptr<BatchReader>>();
    }

    Result<std::shared_ptr<MemReadView>> AcquireReadView() override {
        ++acquire_count;
        return std::make_shared<TestingReadView>(offset_range);
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<MemReadView>&, int64_t, const MemQueryContext&) override {
        return std::vector<std::unique_ptr<BatchReader>>();
    }

    Status AdvanceCommittedOffset(int64_t committed_offset) override {
        ++advance_count;
        if (fail_next_advance) {
            fail_next_advance = false;
            return Status::Invalid("injected committed offset failure");
        }
        committed_offsets.push_back(committed_offset);
        return Status::OK();
    }

    uint64_t GetMemoryUsage() const override {
        return 0;
    }

    int32_t acquire_count = 0;
    int32_t advance_count = 0;
    bool fail_next_advance = false;
    std::optional<Range> offset_range;
    std::vector<int64_t> committed_offsets;
};

class TestingMemIndexerFactory : public MemIndexerFactory {
 public:
    struct CapturedRequest {
        std::map<std::string, std::string> options;
        std::map<std::string, std::string> partition;
        int32_t bucket;
        MemIndexerCreateConfig mode_config;
    };

    Result<std::shared_ptr<MemIndexer>> Create(MemIndexerCreateRequest&& request) override {
        if (!request.write_schema || !request.write_schema->release) {
            return Status::Invalid("testing write schema is null");
        }
        ArrowSchemaRelease(request.write_schema.get());
        requests.push_back(CapturedRequest{std::move(request.options), std::move(request.partition),
                                           request.bucket, request.mode_config});
        ++create_count;
        if (create_error) {
            return create_error.value();
        }
        if (return_null) {
            return std::shared_ptr<MemIndexer>();
        }
        auto indexer = std::make_shared<TestingMemIndexer>();
        indexers.push_back(indexer);
        return indexer;
    }

    int32_t create_count = 0;
    std::optional<Status> create_error;
    bool return_null = false;
    std::vector<std::shared_ptr<TestingMemIndexer>> indexers;
    std::vector<CapturedRequest> requests;
};

std::unique_ptr<ArrowSchema> MakeWriteSchema() {
    auto c_schema = std::make_unique<ArrowSchema>();
    EXPECT_TRUE(
        arrow::ExportSchema(*arrow::schema({arrow::field("id", arrow::int64())}), c_schema.get())
            .ok());
    return c_schema;
}

MemIndexerCreateRequest MakeRequest(const std::map<std::string, std::string>& partition = {},
                                    int32_t bucket = 0) {
    return MemIndexerCreateRequest{MakeWriteSchema(), {},     GetDefaultPool(),
                                   partition,         bucket, AppendMemIndexerCreateConfig{}};
}

std::unique_ptr<RecordBatch> MakeWriteBatch(const std::string& json) {
    std::shared_ptr<arrow::DataType> type = arrow::struct_({arrow::field("id", arrow::int64())});
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(type, json).ValueOrDie();
    ArrowArray c_array;
    EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    return RecordBatchBuilder(&c_array).Finish().value();
}

Result<std::vector<int64_t>> ReadPrimaryKeyQuerySequences(
    const std::shared_ptr<MemIndexer>& indexer, const std::shared_ptr<MemReadView>& view) {
    auto read_schema = std::make_unique<ArrowSchema>();
    std::shared_ptr<arrow::Schema> requested_schema =
        arrow::schema({DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()),
                       arrow::field("id", arrow::int64())});
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*requested_schema, read_schema.get()));
    ScopeGuard schema_guard([schema = read_schema.get()]() { ArrowSchemaRelease(schema); });
    MemQueryContext query_context{read_schema.get(), /*predicate=*/nullptr,
                                  /*enable_predicate_pushdown=*/false};
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<BatchReader>> readers,
        indexer->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, query_context));

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
            if (values->num_fields() < 2 ||
                values->type()->field(0)->name() != SpecialFields::ValueKind().Name() ||
                values->type()->field(1)->name() != SpecialFields::SequenceNumber().Name()) {
                return Status::Invalid("PK query reader did not return PK mutation metadata");
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

TEST(RealtimeContextTest, TestReusesIndexerAndCapturesRegisteredViews) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();

    ASSERT_OK_AND_ASSIGN(
        RealtimeMemIndexerState first_state,
        context->GetOrCreateMemIndexer(MemIndexerCreateRequest{MakeWriteSchema(),
                                                               {{"k", "v"}},
                                                               pool,
                                                               {{"dt", "2026-08-02"}},
                                                               0,
                                                               AppendMemIndexerCreateConfig{}}));
    ASSERT_EQ(0, first_state.initial_offset);
    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState first_again_state,
                         context->GetOrCreateMemIndexer(MakeRequest({{"dt", "2026-08-02"}}, 0)));
    ASSERT_EQ(first_state.indexer, first_again_state.indexer);
    ASSERT_EQ(0, first_again_state.initial_offset);
    ASSERT_EQ(1, factory->indexers.size());
    ASSERT_EQ(1, factory->indexers[0]->acquire_count);

    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState second_state,
                         context->GetOrCreateMemIndexer(MakeRequest({{"dt", "2026-08-02"}}, 1)));
    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState third_state,
                         context->GetOrCreateMemIndexer(MakeRequest({{"dt", "2026-08-03"}}, 0)));
    ASSERT_NE(first_state.indexer, second_state.indexer);
    ASSERT_NE(first_state.indexer, third_state.indexer);
    ASSERT_EQ(3, factory->indexers.size());

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context->AcquireReadViews());
    ASSERT_EQ(3, views.size());
    const RealtimePartitionBucket expected_partition_bucket({{"dt", "2026-08-02"}}, 0);
    ASSERT_EQ(expected_partition_bucket, views[0].partition_bucket);
    ASSERT_EQ(first_state.indexer, views[0].indexer);
    ASSERT_TRUE(views[0].read_view);
    ASSERT_EQ(2, factory->indexers[0]->acquire_count);
    ASSERT_EQ(1, factory->indexers[1]->acquire_count);
    ASSERT_EQ(1, factory->indexers[2]->acquire_count);
}

TEST(RealtimeContextTest, TestLazyFactoryPreservesOffset) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};
    const RealtimePartitionBucket partition_bucket(partition, /*bucket=*/3);
    ASSERT_OK(context->AdvanceCommittedProgress(5, {{partition_bucket, /*offset=*/7}}));

    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState first_state,
                         context->GetOrCreateMemIndexer(MakeRequest(partition, 3)));
    std::shared_ptr<TestingMemIndexer> testing_indexer = factory->indexers.at(0);
    ASSERT_EQ(testing_indexer, first_state.indexer);
    ASSERT_EQ(8, first_state.initial_offset);
    ASSERT_EQ(1, factory->create_count);

    testing_indexer->offset_range = Range(8, 12);
    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState reused_state,
                         context->GetOrCreateMemIndexer(MakeRequest(partition, 3)));
    ASSERT_EQ(first_state.indexer, reused_state.indexer);
    ASSERT_EQ(13, reused_state.initial_offset);
    ASSERT_EQ(1, factory->create_count);
    ASSERT_EQ(1, testing_indexer->acquire_count);
}

TEST(RealtimeContextTest, TestPropagatesFactoryFailure) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    factory->create_error = Status::IOError("factory failed");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));

    Result<RealtimeMemIndexerState> result =
        context->GetOrCreateMemIndexer(MakeRequest(/*partition=*/{}, /*bucket=*/0));
    ASSERT_TRUE(result.status().IsIOError());
    ASSERT_NOK_WITH_MSG(result, "factory failed");
}

TEST(RealtimeContextTest, TestRejectsNullCreatedIndexer) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    factory->return_null = true;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));
    ASSERT_NOK_WITH_MSG(context->GetOrCreateMemIndexer(MakeRequest()),
                        "mem indexer factory returned null");

    factory->return_null = false;
    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState state,
                         context->GetOrCreateMemIndexer(MakeRequest()));
    ASSERT_EQ(factory->indexers.at(0), state.indexer);
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context->AcquireReadViews());
    ASSERT_EQ(1, views.size());
    ASSERT_EQ(state.indexer, views[0].indexer);
}

TEST(RealtimeContextTest, TestCustomFactorySupportsAppendAndPrimaryKey) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};
    ASSERT_OK(context->GetOrCreateMemIndexer(MakeRequest(partition, 2)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<RealtimePrimaryKeyWriter> first_writer,
        RealtimePrimaryKeyWriter::Create(
            partition, 3, MakeWriteSchema(), /*trimmed_primary_keys=*/{"id"}, context,
            /*merge_tree_writer=*/nullptr, /*options=*/{}, GetDefaultPool(),
            /*file_system=*/nullptr, /*temp_directory=*/"", /*enable_multi_thread_spill=*/false,
            /*restore_max_seq_number=*/-1));
    ASSERT_OK(first_writer->Write(MakeWriteBatch("[[1]]")));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<RealtimePrimaryKeyWriter> second_writer,
        RealtimePrimaryKeyWriter::Create(
            partition, 3, MakeWriteSchema(), /*trimmed_primary_keys=*/{"id"}, context,
            /*merge_tree_writer=*/nullptr, /*options=*/{}, GetDefaultPool(),
            /*file_system=*/nullptr, /*temp_directory=*/"", /*enable_multi_thread_spill=*/false,
            /*restore_max_seq_number=*/17));
    ASSERT_EQ(2, factory->requests.size());
    ASSERT_TRUE(
        std::holds_alternative<AppendMemIndexerCreateConfig>(factory->requests[0].mode_config));
    ASSERT_TRUE(
        std::holds_alternative<PrimaryKeyMemIndexerCreateConfig>(factory->requests[1].mode_config));
    const PrimaryKeyMemIndexerCreateConfig& primary_key_config =
        std::get<PrimaryKeyMemIndexerCreateConfig>(factory->requests[1].mode_config);
    ASSERT_EQ(std::vector<std::string>({"id"}), primary_key_config.primary_keys);
    ASSERT_EQ(partition, factory->requests[1].partition);
    ASSERT_EQ(3, factory->requests[1].bucket);
    ASSERT_EQ(-1, primary_key_config.restore_max_sequence_number);
    ASSERT_OK(second_writer->Write(MakeWriteBatch("[[2]]")));
    ASSERT_EQ(std::optional<Range>(Range(0, 1)), factory->indexers[1]->offset_range);
}

TEST(RealtimeContextTest, TestPrimaryKeyFactoryRejectsSequenceOverflow) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context, RealtimeContext::Create());
    std::unique_ptr<UniqueTestDirectory> temp_directory = UniqueTestDirectory::Create();
    ASSERT_NOK_WITH_MSG(
        RealtimePrimaryKeyWriter::Create(
            /*partition=*/{}, /*bucket=*/0, MakeWriteSchema(), /*trimmed_primary_keys=*/{"id"},
            context, /*merge_tree_writer=*/nullptr, {{Options::WRITE_BUFFER_SPILLABLE, "true"}},
            GetDefaultPool(), temp_directory->GetFileSystem(), temp_directory->Str(),
            /*enable_multi_thread_spill=*/false,
            /*restore_max_seq_number=*/std::numeric_limits<int64_t>::max()),
        "PK sequence number has reached INT64_MAX");
}

TEST(RealtimeContextTest, TestPrimaryKeyWriterUsesBuiltInIndexerAndRestoresSequence) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context, RealtimeContext::Create());
    std::unique_ptr<UniqueTestDirectory> temp_directory = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<RealtimePrimaryKeyWriter> writer,
        RealtimePrimaryKeyWriter::Create(
            /*partition=*/{}, /*bucket=*/0, MakeWriteSchema(), /*trimmed_primary_keys=*/{"id"},
            context, /*merge_tree_writer=*/nullptr, {{Options::WRITE_BUFFER_SPILLABLE, "true"}},
            GetDefaultPool(), temp_directory->GetFileSystem(), temp_directory->Str(),
            /*enable_multi_thread_spill=*/false, /*restore_max_seq_number=*/7));
    ASSERT_OK(writer->Write(MakeWriteBatch("[[1]]")));

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context->AcquireReadViews());
    ASSERT_EQ(1, views.size());
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> sequences,
                         ReadPrimaryKeyQuerySequences(views[0].indexer, views[0].read_view));
    ASSERT_EQ(std::vector<int64_t>({8}), sequences);
    ASSERT_EQ(std::optional<Range>(Range(0, 0)), views[0].read_view->GetOffsetRange());
}

TEST(RealtimeContextTest, TestCommittedProgressIsMonotonicAndSelective) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};

    ASSERT_OK(context->GetOrCreateMemIndexer(MakeRequest(partition, 0)));
    ASSERT_OK(context->GetOrCreateMemIndexer(MakeRequest(partition, 1)));
    ASSERT_EQ(2, factory->indexers.size());

    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(-1, {}),
                        "snapshot id must not be negative");
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(
                            4, {{RealtimePartitionBucket(partition, /*bucket=*/-1), /*offset=*/3}}),
                        "invalid partition-bucket committed offset");
    ASSERT_TRUE(factory->indexers[0]->committed_offsets.empty());
    ASSERT_TRUE(factory->indexers[1]->committed_offsets.empty());

    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {{RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/7},
            {RealtimePartitionBucket({{"dt", "unknown"}}, /*bucket=*/0), /*offset=*/9}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_TRUE(factory->indexers[1]->committed_offsets.empty());

    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState restored_state,
                         context->GetOrCreateMemIndexer(MakeRequest({{"dt", "unknown"}}, 0)));
    ASSERT_EQ(10, restored_state.initial_offset);

    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {{RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/10}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(4, {}),
                        "committed snapshot cannot move backwards");

    ASSERT_OK(context->AdvanceCommittedProgress(
        6, {{RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/7},
            {RealtimePartitionBucket(partition, /*bucket=*/1), /*offset=*/8},
            {RealtimePartitionBucket({{"dt", "unknown"}}, /*bucket=*/0), /*offset=*/9}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_EQ(std::vector<int64_t>({8}), factory->indexers[1]->committed_offsets);
}

TEST(RealtimeContextTest, TestRetriesOnlyIncompleteReclamation) {
    auto factory = std::make_shared<TestingMemIndexerFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> context,
                         RealtimeContext::Create(factory));
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};

    ASSERT_OK(context->GetOrCreateMemIndexer(MakeRequest(partition, 0)));
    ASSERT_OK(context->GetOrCreateMemIndexer(MakeRequest(partition, 1)));
    ASSERT_OK(context->GetOrCreateMemIndexer(MakeRequest(partition, 2)));
    ASSERT_EQ(3, factory->indexers.size());
    factory->indexers[1]->fail_next_advance = true;

    const RealtimeOffsetMap committed_offsets = {
        {RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/7},
        {RealtimePartitionBucket(partition, /*bucket=*/1), /*offset=*/8},
        {RealtimePartitionBucket(partition, /*bucket=*/2), /*offset=*/9}};
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(5, committed_offsets),
                        "injected committed offset failure");
    ASSERT_EQ(std::vector<int64_t>({7}), factory->indexers[0]->committed_offsets);
    ASSERT_TRUE(factory->indexers[1]->committed_offsets.empty());
    ASSERT_EQ(std::vector<int64_t>({9}), factory->indexers[2]->committed_offsets);

    ASSERT_OK_AND_ASSIGN(RealtimeMemIndexerState failed_indexer_state,
                         context->GetOrCreateMemIndexer(MakeRequest(partition, 1)));
    ASSERT_EQ(9, failed_indexer_state.initial_offset);

    ASSERT_OK(context->AdvanceCommittedProgress(5, committed_offsets));
    ASSERT_EQ(1, factory->indexers[0]->advance_count);
    ASSERT_EQ(2, factory->indexers[1]->advance_count);
    ASSERT_EQ(1, factory->indexers[2]->advance_count);
    ASSERT_EQ(std::vector<int64_t>({8}), factory->indexers[1]->committed_offsets);
}

TEST(RealtimeContextTest, TestRejectsNullFactory) {
    ASSERT_NOK_WITH_MSG(RealtimeContext::Create(nullptr), "mem indexer factory is null");
}

}  // namespace
}  // namespace paimon::test

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

#include "paimon/core/io/merged_key_value_record_reader.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/realtime/prepared_key_value_reader.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_key_value_data_file_record_reader.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

std::shared_ptr<arrow::Field> MakeField(const std::string& name,
                                        const std::shared_ptr<arrow::DataType>& type,
                                        int32_t field_id, bool nullable = true) {
    return DataField::ConvertDataFieldToArrowField(
        DataField(field_id, arrow::field(name, type, nullable)));
}

std::shared_ptr<arrow::Schema> MakePreparedSchema(const arrow::FieldVector& value_fields) {
    arrow::FieldVector prepared_fields = {
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
            ->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset())};
    prepared_fields.insert(prepared_fields.end(), value_fields.begin(), value_fields.end());
    return arrow::schema(prepared_fields);
}

Result<std::unique_ptr<KeyValueRecordReader>> AdaptPreparedBatchReaderForTest(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<arrow::Schema>& prepared_schema,
    const std::optional<OffsetRange>& visible_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    return PreparedKeyValueReaderFactory::Create(
        std::move(reader), prepared_schema, visible_offsets, key_schema, value_schema, memory_pool);
}

class TrackingBatchReader : public BatchReader {
 public:
    TrackingBatchReader(std::unique_ptr<BatchReader>&& delegate, int32_t* close_count)
        : delegate_(std::move(delegate)), close_count_(close_count) {}

    Result<ReadBatch> NextBatch() override {
        return delegate_->NextBatch();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return delegate_->GetReaderMetrics();
    }

    void Close() override {
        ++(*close_count_);
        delegate_->Close();
    }

 private:
    std::unique_ptr<BatchReader> delegate_;
    int32_t* close_count_;
};

class MalformedBitmapBatchReader : public BatchReader {
 public:
    MalformedBitmapBatchReader(std::unique_ptr<BatchReader>&& delegate, int32_t row_id)
        : delegate_(std::move(delegate)), row_id_(row_id) {}

    Result<ReadBatch> NextBatch() override {
        return delegate_->NextBatch();
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, delegate_->NextBatchWithBitmap());
        if (!IsEofBatch(batch)) {
            batch.second.Add(row_id_);
        }
        return batch;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return delegate_->GetReaderMetrics();
    }

    void Close() override {
        delegate_->Close();
    }

 private:
    std::unique_ptr<BatchReader> delegate_;
    int32_t row_id_;
};

}  // namespace

class MergedKeyValueRecordReaderTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        merge_function_wrapper_ = std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<ReducerMergeFunctionWrapper> merge_function_wrapper_;
};

TEST_F(MergedKeyValueRecordReaderTest, TestMergeAcrossUnderlyingBatches) {
    std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                     DataField(1, arrow::field("k1", arrow::int32())),
                                     DataField(2, arrow::field("v0", arrow::int32())),
                                     DataField(3, arrow::field("v1", arrow::int32())),
                                     DataField(4, arrow::field("v2", arrow::int32()))};

    auto value_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    auto arrow_fields = value_schema->fields();
    auto key_schema = arrow::schema({arrow_fields[0], arrow_fields[1]});
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(value_schema)->fields());

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 0, 1, 1, 10, 20, 30],
        [1, 0, 1, 1, 11, 21, 31],
        [2, 0, 2, 2, 12, 22, 32],
        [3, 0, 2, 2, 13, 23, 33]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                         FieldsComparator::Create({fields[0], fields[1]},
                                                  /*is_ascending_order=*/true));

    auto expected = KeyValueChecker::GenerateKeyValues(
        /*seq_vec=*/{1, 3}, /*key_vec=*/{{1, 1}, {2, 2}},
        /*value_vec=*/{{1, 1, 11, 21, 31}, {2, 2, 13, 23, 33}}, pool_);

    for (auto batch_size : {1, 2, 3}) {
        auto file_batch_reader = std::make_unique<MockFileBatchReader>(src_array, src_type,
                                                                       /*batch_size=*/batch_size);
        auto raw_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
            std::move(file_batch_reader), key_schema, value_schema, /*level=*/0, pool_);
        auto merged_reader = std::make_unique<MergedKeyValueRecordReader>(
            std::move(raw_reader), key_comparator, merge_function_wrapper_);

        ASSERT_OK_AND_ASSIGN(
            auto results,
            (ReadResultCollector::CollectKeyValueResult<
                MergedKeyValueRecordReader, KeyValueRecordReader::Iterator>(merged_reader.get())));
        KeyValueChecker::CheckResult(expected, results, /*key_arity=*/2, /*value_arity=*/5);
    }
}

TEST_F(MergedKeyValueRecordReaderTest, TestSkipMergedNulloptResultInHasNext) {
    auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/true);
    auto merge_function_wrapper = std::make_shared<ReducerMergeFunctionWrapper>(std::move(mfunc));

    std::vector<DataField> fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                     DataField(1, arrow::field("v0", arrow::int32()))};

    auto value_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    auto arrow_fields = value_schema->fields();
    auto key_schema = arrow::schema({arrow_fields[0]});
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(value_schema)->fields());

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"([
        [0, 3, 1, 10],
        [3, 3, 1, 11],
        [2, 3, 2, 200],
        [4, 3, 2, 240],
        [1, 0, 3, 300],
        [5, 3, 3, 30]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                         FieldsComparator::Create({fields[0]}, /*is_ascending_order=*/true));

    auto expected = KeyValueChecker::GenerateKeyValues(
        /*seq_vec=*/{1}, /*key_vec=*/{{3}}, /*value_vec=*/{{3, 300}}, pool_);

    for (auto batch_size : {1, 2, 3}) {
        auto file_batch_reader =
            std::make_unique<MockFileBatchReader>(src_array, src_type, /*batch_size=*/batch_size);
        auto raw_reader = std::make_unique<MockKeyValueDataFileRecordReader>(
            std::move(file_batch_reader), key_schema, value_schema, /*level=*/0, pool_);
        auto merged_reader = std::make_unique<MergedKeyValueRecordReader>(
            std::move(raw_reader), key_comparator, merge_function_wrapper);

        ASSERT_OK_AND_ASSIGN(
            auto results,
            (ReadResultCollector::CollectKeyValueResult<
                MergedKeyValueRecordReader, KeyValueRecordReader::Iterator>(merged_reader.get())));
        KeyValueChecker::CheckResult(expected, results, /*key_arity=*/1, /*value_arity=*/2);
    }
}

TEST_F(MergedKeyValueRecordReaderTest, TestPreparedReaderOffsetFilter) {
    std::vector<DataField> value_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                           DataField(1, arrow::field("v0", arrow::int32()))};
    std::shared_ptr<arrow::Schema> value_schema =
        DataField::ConvertDataFieldsToArrowSchema(value_fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema({value_schema->field(0)});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema(value_schema->fields());
    std::shared_ptr<arrow::DataType> prepared_type = arrow::struct_(prepared_schema->fields());
    auto prepared_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(prepared_type, R"([
        [0, 100, 0, 1, 10],
        [0, 101, 1, 2, 20],
        [0, 102, 4, 3, 30],
        [0, 103, 2, 4, 40],
        [0, 104, 5, 5, 50],
        [0, 105, 3, 6, 60]
    ])")
            .ValueOrDie());

    auto batch_reader = std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, 2);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        AdaptPreparedBatchReaderForTest(std::move(batch_reader), prepared_schema, OffsetRange(2, 4),
                                        key_schema, value_schema, pool_));
    ASSERT_OK_AND_ASSIGN(
        std::vector<KeyValue> results,
        (ReadResultCollector::CollectKeyValueResult<KeyValueRecordReader,
                                                    KeyValueRecordReader::Iterator>(reader.get())));

    std::vector<RowKind*> row_kinds = {const_cast<RowKind*>(RowKind::Insert()),
                                       const_cast<RowKind*>(RowKind::Insert())};
    std::vector<int64_t> levels = {KeyValue::UNKNOWN_LEVEL, KeyValue::UNKNOWN_LEVEL};
    std::vector<KeyValue> expected = KeyValueChecker::GenerateKeyValues(
        row_kinds, {103, 105}, levels, {{4}, {6}}, {{4, 40}, {6, 60}}, pool_);
    KeyValueChecker::CheckResult(expected, results, 1, 2);
}

TEST_F(MergedKeyValueRecordReaderTest, TestPreparedReaderRejectsReversedVisibleOffsets) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema({key});
    std::shared_ptr<arrow::DataType> prepared_type = arrow::struct_(prepared_schema->fields());
    std::shared_ptr<arrow::Array> prepared_array =
        arrow::ipc::internal::json::ArrayFromJSON(prepared_type, R"([[0, 10, 0, 1]])").ValueOrDie();
    auto batch_reader =
        std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, /*batch_size=*/1);

    Result<std::unique_ptr<KeyValueRecordReader>> result =
        AdaptPreparedBatchReaderForTest(std::move(batch_reader), prepared_schema, OffsetRange(2, 1),
                                        value_schema, value_schema, pool_);
    ASSERT_TRUE(result.status().IsInvalid());
    ASSERT_NOK_WITH_MSG(result, "prepared visible offset range begin exceeds end");
}

TEST_F(MergedKeyValueRecordReaderTest, TestPreparedReaderBitmapBounds) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema({key});
    std::shared_ptr<arrow::DataType> prepared_type = arrow::struct_(prepared_schema->fields());
    std::shared_ptr<arrow::Array> prepared_array =
        arrow::ipc::internal::json::ArrayFromJSON(prepared_type, R"([[0, 10, 0, 1]])").ValueOrDie();
    auto batch_reader = std::make_unique<MalformedBitmapBatchReader>(
        std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, /*batch_size=*/1),
        /*row_id=*/1);

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        AdaptPreparedBatchReaderForTest(std::move(batch_reader), prepared_schema, OffsetRange(0, 1),
                                        value_schema, value_schema, pool_));
    Result<std::vector<KeyValue>> result =
        ReadResultCollector::CollectKeyValueResult<KeyValueRecordReader,
                                                   KeyValueRecordReader::Iterator>(reader.get());
    ASSERT_TRUE(result.status().IsInvalid());
    ASSERT_NOK_WITH_MSG(result, "selected row id 1 is out of bounds for prepared batch length 1");
}

TEST_F(MergedKeyValueRecordReaderTest, TestPreparedReaderCommitSchema) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Field> extra = MakeField("extra", arrow::int32(), 1);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema({key, extra});
    std::shared_ptr<arrow::DataType> prepared_type = arrow::struct_(prepared_schema->fields());
    auto prepared_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(prepared_type, R"([[0, 10, 0, 1, 2]])")
            .ValueOrDie());

    auto query_batch_reader =
        std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, 1);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> query_reader,
        AdaptPreparedBatchReaderForTest(std::move(query_batch_reader), prepared_schema,
                                        OffsetRange(0, 1), value_schema, value_schema, pool_));
    ASSERT_OK_AND_ASSIGN(
        std::vector<KeyValue> query_results,
        (ReadResultCollector::CollectKeyValueResult<
            KeyValueRecordReader, KeyValueRecordReader::Iterator>(query_reader.get())));
    ASSERT_EQ(query_results.size(), 1);
    ASSERT_EQ(query_results[0].value->GetFieldCount(), 1);
    ASSERT_EQ(query_results[0].value->GetInt(0), 1);

    auto batch_reader = std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, 1);
    ASSERT_NOK_WITH_MSG(
        AdaptPreparedBatchReaderForTest(std::move(batch_reader), prepared_schema, std::nullopt,
                                        value_schema, value_schema, pool_),
        "exact");
}

TEST_F(MergedKeyValueRecordReaderTest, TestCommitOffsetCoverage) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema({key});
    std::shared_ptr<arrow::DataType> prepared_type = arrow::struct_(prepared_schema->fields());
    std::shared_ptr<arrow::Array> first_array =
        arrow::ipc::internal::json::ArrayFromJSON(prepared_type,
                                                  R"([[0, 10, 2, 1], [0, 11, 0, 3]])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> second_array =
        arrow::ipc::internal::json::ArrayFromJSON(prepared_type,
                                                  R"([[0, 12, 1, 2], [0, 13, 3, 4]])")
            .ValueOrDie();
    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    batch_readers.push_back(
        std::make_unique<MockFileBatchReader>(first_array, prepared_type, /*read_batch_size=*/1));
    batch_readers.push_back(
        std::make_unique<MockFileBatchReader>(second_array, prepared_type, /*read_batch_size=*/1));

    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                         PreparedKeyValueReaderFactory::CreateForCommit(
                             std::move(batch_readers), prepared_schema, OffsetRange(0, 4),
                             value_schema, value_schema, pool_));
    int64_t row_count = 0;
    for (const std::unique_ptr<KeyValueRecordReader>& reader : readers) {
        ASSERT_OK_AND_ASSIGN(
            std::vector<KeyValue> rows,
            (ReadResultCollector::CollectKeyValueResult<
                KeyValueRecordReader, KeyValueRecordReader::Iterator>(reader.get())));
        row_count += static_cast<int64_t>(rows.size());
    }
    ASSERT_EQ(4, row_count);
}

TEST_F(MergedKeyValueRecordReaderTest, TestRejectsDuplicateCommitOffset) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema({key});
    std::shared_ptr<arrow::DataType> prepared_type = arrow::struct_(prepared_schema->fields());
    std::shared_ptr<arrow::Array> prepared_array =
        arrow::ipc::internal::json::ArrayFromJSON(
            prepared_type, R"([[0, 10, 0, 1], [0, 11, 0, 2], [0, 12, 2, 3]])")
            .ValueOrDie();
    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    batch_readers.push_back(std::make_unique<MockFileBatchReader>(prepared_array, prepared_type,
                                                                  /*read_batch_size=*/1));

    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                         PreparedKeyValueReaderFactory::CreateForCommit(
                             std::move(batch_readers), prepared_schema, OffsetRange(0, 3),
                             value_schema, value_schema, pool_));
    ASSERT_NOK_WITH_MSG((ReadResultCollector::CollectKeyValueResult<KeyValueRecordReader,
                                                                    KeyValueRecordReader::Iterator>(
                            readers[0].get())),
                        "did not cover the sealed range");
}

TEST_F(MergedKeyValueRecordReaderTest, TestBadCommitBatch) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Field> value = MakeField("value", arrow::int32(), 1);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key, value});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema({key, value});
    std::shared_ptr<arrow::Schema> actual_schema = MakePreparedSchema({key});
    std::shared_ptr<arrow::DataType> actual_type = arrow::struct_(actual_schema->fields());
    std::shared_ptr<arrow::Array> actual =
        arrow::ipc::internal::json::ArrayFromJSON(actual_type, R"([[0, 10, 0, 1]])").ValueOrDie();

    auto batch_reader = std::make_unique<MockFileBatchReader>(actual, actual_type, 1);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        AdaptPreparedBatchReaderForTest(std::move(batch_reader), prepared_schema, std::nullopt,
                                        arrow::schema({key}), value_schema, pool_));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "field count");
}

TEST_F(MergedKeyValueRecordReaderTest, TestMissingCompositeKey) {
    std::shared_ptr<arrow::Field> key0 = MakeField("key0", arrow::int32(), 0);
    std::shared_ptr<arrow::Field> key1 = MakeField("key1", arrow::int32(), 1);
    std::shared_ptr<arrow::Field> value = MakeField("value", arrow::int32(), 2);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key0, key1, value});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema({key0, key1, value});
    std::shared_ptr<arrow::Schema> actual_schema = MakePreparedSchema({key0, value});
    std::shared_ptr<arrow::DataType> actual_type = arrow::struct_(actual_schema->fields());
    std::shared_ptr<arrow::Array> actual =
        arrow::ipc::internal::json::ArrayFromJSON(actual_type, R"([[0, 10, 0, 1, 20]])")
            .ValueOrDie();

    auto batch_reader = std::make_unique<MockFileBatchReader>(actual, actual_type, 1);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        AdaptPreparedBatchReaderForTest(std::move(batch_reader), prepared_schema, OffsetRange(0, 1),
                                        arrow::schema({key0, key1}), value_schema, pool_));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "field count");
}

TEST_F(MergedKeyValueRecordReaderTest, TestQueryReaderRequiresStoreAlignedSchema) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Field> old_value = MakeField("old_value", arrow::int32(), 1);
    std::shared_ptr<arrow::Field> renamed_value = MakeField("renamed_value", arrow::int32(), 1);
    std::shared_ptr<arrow::Field> added = MakeField("added", arrow::int32(), 2);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key, renamed_value, added});
    std::shared_ptr<arrow::Schema> prepared_schema =
        MakePreparedSchema({key, renamed_value, added});
    std::shared_ptr<arrow::Schema> actual_schema = MakePreparedSchema({key, old_value});
    std::shared_ptr<arrow::DataType> actual_type = arrow::struct_(actual_schema->fields());
    std::shared_ptr<arrow::Array> actual =
        arrow::ipc::internal::json::ArrayFromJSON(actual_type, R"([[0, 10, 0, 1, 20]])")
            .ValueOrDie();

    auto batch_reader = std::make_unique<MockFileBatchReader>(actual, actual_type, 1);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        AdaptPreparedBatchReaderForTest(std::move(batch_reader), prepared_schema, OffsetRange(0, 1),
                                        arrow::schema({key}), value_schema, pool_));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "field count");
}

TEST_F(MergedKeyValueRecordReaderTest, TestMergedReaderErrorRetry) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema({key});
    std::shared_ptr<arrow::DataType> prepared_type = arrow::struct_(prepared_schema->fields());
    std::shared_ptr<arrow::Array> prepared_array =
        arrow::ipc::internal::json::ArrayFromJSON(prepared_type, R"([[0, 10, 0, 1]])").ValueOrDie();
    auto failing_reader = std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, 1);
    failing_reader->SetNextBatchStatus(Status::IOError("stable prepared error"));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        AdaptPreparedBatchReaderForTest(std::move(failing_reader), prepared_schema,
                                        OffsetRange(0, 1), value_schema, value_schema, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FieldsComparator> key_comparator,
                         FieldsComparator::Create({DataField(0, key)}, true));
    MergedKeyValueRecordReader merged_reader(std::move(reader), key_comparator,
                                             merge_function_wrapper_);

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> first = merged_reader.NextBatch();
    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> retry = merged_reader.NextBatch();
    ASSERT_NOK(first);
    ASSERT_NOK(retry);
    ASSERT_EQ(first.status().ToString(), retry.status().ToString());
}

TEST_F(MergedKeyValueRecordReaderTest, TestPreparedReaderSafeDecode) {
    std::shared_ptr<arrow::Field> key = MakeField("key", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> value_schema = arrow::schema({key});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema({key});

    arrow::FieldVector invalid_fields = prepared_schema->fields();
    invalid_fields[0] = invalid_fields[0]->WithName("wrong_value_kind");
    invalid_fields[3] = MakeField("wrong_key", arrow::int32(), 99);
    std::shared_ptr<arrow::DataType> invalid_type = arrow::struct_(invalid_fields);
    auto invalid_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(invalid_type, R"([[0, 10, 0, 1]])").ValueOrDie());

    auto batch_reader = std::make_unique<MockFileBatchReader>(invalid_array, invalid_type, 1);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        AdaptPreparedBatchReaderForTest(std::move(batch_reader), prepared_schema, OffsetRange(0, 1),
                                        value_schema, value_schema, pool_));
    ASSERT_NOK_WITH_MSG(
        (ReadResultCollector::CollectKeyValueResult<KeyValueRecordReader,
                                                    KeyValueRecordReader::Iterator>(reader.get())),
        "prepared batch field");
}

TEST_F(MergedKeyValueRecordReaderTest, TestPreparedReaderNestedValues) {
    std::shared_ptr<arrow::Field> id = MakeField("id", arrow::int32(), 0);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema({id});
    std::shared_ptr<arrow::Field> query_item_b = MakeField("renamed_b", arrow::int32(), 11);
    std::shared_ptr<arrow::Field> query_item_a = MakeField("renamed_a", arrow::int32(), 10);
    std::shared_ptr<arrow::Field> query_items = MakeField(
        "items_renamed",
        arrow::list(arrow::field("element", arrow::struct_({query_item_b, query_item_a}))), 2);
    std::shared_ptr<arrow::Field> query_attr_y = MakeField("renamed_y", arrow::int32(), 21);
    std::shared_ptr<arrow::Field> query_attr_x = MakeField("renamed_x", arrow::int32(), 20);
    std::shared_ptr<arrow::Field> query_attrs =
        MakeField("attrs_renamed",
                  arrow::map(arrow::utf8(), arrow::struct_({query_attr_y, query_attr_x})), 3);
    std::shared_ptr<arrow::Field> query_key_right = MakeField("renamed_right", arrow::int32(), 31);
    std::shared_ptr<arrow::Field> query_key_left = MakeField("renamed_left", arrow::int32(), 30);
    std::shared_ptr<arrow::Field> query_keyed_values =
        MakeField("keyed_values_renamed",
                  arrow::map(arrow::struct_({query_key_right, query_key_left}), arrow::int32()), 4);
    std::shared_ptr<arrow::Schema> query_value_schema =
        arrow::schema({id, query_items, query_attrs, query_keyed_values});
    std::shared_ptr<arrow::Schema> prepared_schema =
        MakePreparedSchema(query_value_schema->fields());
    std::shared_ptr<arrow::DataType> prepared_type = arrow::struct_(prepared_schema->fields());
    std::shared_ptr<arrow::Array> prepared_array =
        arrow::ipc::internal::json::ArrayFromJSON(
            prepared_type,
            R"([[0, 10, 0, 1, [[200, 100], [400, 300]], [["k1", [8, 7]], ["k2", [10, 9]]], [[[12, 11], 13], [[22, 21], 23]]]])")
            .ValueOrDie();

    auto batch_reader = std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, 1);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<KeyValueRecordReader> reader,
        AdaptPreparedBatchReaderForTest(std::move(batch_reader), prepared_schema, OffsetRange(0, 1),
                                        key_schema, query_value_schema, pool_));
    ASSERT_OK_AND_ASSIGN(
        std::vector<KeyValue> results,
        (ReadResultCollector::CollectKeyValueResult<KeyValueRecordReader,
                                                    KeyValueRecordReader::Iterator>(reader.get())));

    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].key->GetInt(0), 1);
    ASSERT_EQ(results[0].value->GetFieldCount(), 4);
    ASSERT_EQ(results[0].value->GetInt(0), 1);

    std::shared_ptr<InternalArray> item_array = results[0].value->GetArray(1);
    ASSERT_EQ(item_array->Size(), 2);
    std::shared_ptr<InternalRow> first_item = item_array->GetRow(0, 2);
    ASSERT_EQ(first_item->GetInt(0), 200);
    ASSERT_EQ(first_item->GetInt(1), 100);
    std::shared_ptr<InternalRow> second_item = item_array->GetRow(1, 2);
    ASSERT_EQ(second_item->GetInt(0), 400);
    ASSERT_EQ(second_item->GetInt(1), 300);

    std::shared_ptr<InternalMap> attr_map = results[0].value->GetMap(2);
    ASSERT_EQ(attr_map->Size(), 2);
    std::shared_ptr<InternalArray> key_array = attr_map->KeyArray();
    ASSERT_EQ(std::string(key_array->GetStringView(0)), "k1");
    ASSERT_EQ(std::string(key_array->GetStringView(1)), "k2");
    std::shared_ptr<InternalArray> value_array = attr_map->ValueArray();
    std::shared_ptr<InternalRow> first_attr = value_array->GetRow(0, 2);
    ASSERT_EQ(first_attr->GetInt(0), 8);
    ASSERT_EQ(first_attr->GetInt(1), 7);
    std::shared_ptr<InternalRow> second_attr = value_array->GetRow(1, 2);
    ASSERT_EQ(second_attr->GetInt(0), 10);
    ASSERT_EQ(second_attr->GetInt(1), 9);

    std::shared_ptr<InternalMap> keyed_value_map = results[0].value->GetMap(3);
    ASSERT_EQ(keyed_value_map->Size(), 2);
    std::shared_ptr<InternalArray> struct_keys = keyed_value_map->KeyArray();
    std::shared_ptr<InternalRow> first_key = struct_keys->GetRow(0, 2);
    ASSERT_EQ(first_key->GetInt(0), 12);
    ASSERT_EQ(first_key->GetInt(1), 11);
    std::shared_ptr<InternalRow> second_key = struct_keys->GetRow(1, 2);
    ASSERT_EQ(second_key->GetInt(0), 22);
    ASSERT_EQ(second_key->GetInt(1), 21);
    ASSERT_EQ(keyed_value_map->ValueArray()->GetInt(0), 13);
    ASSERT_EQ(keyed_value_map->ValueArray()->GetInt(1), 23);
}

TEST_F(MergedKeyValueRecordReaderTest, TestPreparedReaderLifecycle) {
    std::vector<DataField> value_fields = {DataField(0, arrow::field("k0", arrow::int32())),
                                           DataField(1, arrow::field("v0", arrow::int32()))};
    std::shared_ptr<arrow::Schema> value_schema =
        DataField::ConvertDataFieldsToArrowSchema(value_fields);
    std::shared_ptr<arrow::Schema> key_schema = arrow::schema({value_schema->field(0)});
    std::shared_ptr<arrow::Schema> prepared_schema = MakePreparedSchema(value_schema->fields());
    std::shared_ptr<arrow::DataType> prepared_type = arrow::struct_(prepared_schema->fields());
    auto prepared_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(prepared_type, R"([
        [0, 10, 0, 1, 100]
    ])")
            .ValueOrDie());

    int32_t explicit_close_count = 0;
    {
        auto tracking_reader = std::make_unique<TrackingBatchReader>(
            std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, 1),
            &explicit_close_count);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<KeyValueRecordReader> reader,
            AdaptPreparedBatchReaderForTest(std::move(tracking_reader), prepared_schema,
                                            OffsetRange(0, 1), key_schema, value_schema, pool_));
        reader->Close();
    }
    ASSERT_EQ(explicit_close_count, 1);

    int32_t destructor_close_count = 0;
    {
        auto tracking_reader = std::make_unique<TrackingBatchReader>(
            std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, 1),
            &destructor_close_count);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<KeyValueRecordReader> reader,
            AdaptPreparedBatchReaderForTest(std::move(tracking_reader), prepared_schema,
                                            OffsetRange(0, 1), key_schema, value_schema, pool_));
    }
    ASSERT_EQ(destructor_close_count, 1);

    int32_t factory_failure_close_count = 0;
    {
        std::unique_ptr<BatchReader> tracking_reader = std::make_unique<TrackingBatchReader>(
            std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, 1),
            &factory_failure_close_count);
        std::shared_ptr<arrow::Schema> invalid_schema = arrow::schema(value_schema->fields());
        ASSERT_NOK(AdaptPreparedBatchReaderForTest(std::move(tracking_reader), invalid_schema,
                                                   OffsetRange(0, 1), key_schema, value_schema,
                                                   pool_));
        ASSERT_EQ(nullptr, tracking_reader);
    }
    ASSERT_EQ(factory_failure_close_count, 1);

    int32_t read_failure_close_count = 0;
    {
        auto failing_reader =
            std::make_unique<MockFileBatchReader>(prepared_array, prepared_type, 1);
        failing_reader->SetNextBatchStatus(Status::IOError("prepared reader failure"));
        auto tracking_reader = std::make_unique<TrackingBatchReader>(std::move(failing_reader),
                                                                     &read_failure_close_count);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<KeyValueRecordReader> reader,
            AdaptPreparedBatchReaderForTest(std::move(tracking_reader), prepared_schema,
                                            OffsetRange(0, 1), key_schema, value_schema, pool_));
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "prepared reader failure");
        ASSERT_EQ(read_failure_close_count, 1);
    }
    ASSERT_EQ(read_failure_close_count, 1);
}

}  // namespace paimon::test

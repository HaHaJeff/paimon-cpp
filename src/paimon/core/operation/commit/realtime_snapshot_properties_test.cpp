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

#include "paimon/core/operation/commit/realtime_snapshot_properties.h"

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

using Properties = std::map<std::string, std::string>;

const std::string kTargetJson = R"({
    "version": 1,
    "offsets": [
        {
            "partition": "",
            "bucket": 0,
            "offset": 7
        },
        {
            "partition": "dt=2",
            "bucket": 2,
            "offset": 9
        },
        {
            "partition": "dt=2",
            "bucket": 10,
            "offset": 12
        }
    ]
})";

class RealtimeSnapshotPropertiesTest : public testing::Test {
 protected:
    void SetUp() override {
        directory_ = UniqueTestDirectory::Create();
        ASSERT_NE(nullptr, directory_);
        file_system_ = directory_->GetFileSystem();
        ASSERT_NE(nullptr, file_system_);
    }

    Snapshot MakeSnapshot(
        const std::optional<std::map<std::string, std::string>>& properties) const {
        return Snapshot(
            /*id=*/1,
            /*schema_id=*/1,
            /*base_manifest_list=*/"base-manifest-list",
            /*base_manifest_list_size=*/std::nullopt,
            /*delta_manifest_list=*/"delta-manifest-list",
            /*delta_manifest_list_size=*/std::nullopt,
            /*changelog_manifest_list=*/std::nullopt,
            /*changelog_manifest_list_size=*/std::nullopt,
            /*index_manifest=*/std::nullopt,
            /*commit_user=*/"test-user",
            /*commit_identifier=*/1, Snapshot::CommitKind::Append(),
            /*time_millis=*/0,
            /*total_record_count=*/0,
            /*delta_record_count=*/0,
            /*changelog_record_count=*/std::nullopt,
            /*watermark=*/std::nullopt,
            /*statistics=*/std::nullopt, properties,
            /*next_row_id=*/std::nullopt);
    }

    Result<std::string> WriteJson(const std::string& json) {
        std::string path =
            directory_->Str() + "/input-" + std::to_string(next_file_id_++) + ".offsets";
        PAIMON_RETURN_NOT_OK(file_system_->WriteFile(path, json, /*overwrite=*/false));
        return path;
    }

    Result<RealtimeSnapshotProperties::OffsetMap> ReadJson(const std::string& json) {
        PAIMON_ASSIGN_OR_RAISE(std::string path, WriteJson(json));
        std::map<std::string, std::string> properties = {
            {RealtimeSnapshotProperties::kOffsetsKey, path}};
        return RealtimeSnapshotProperties::ReadOffsets(
            std::optional<Snapshot>(MakeSnapshot(properties)), file_system_);
    }

    std::unique_ptr<UniqueTestDirectory> directory_;
    std::shared_ptr<FileSystem> file_system_;
    int32_t next_file_id_ = 0;
};

RealtimeSnapshotProperties::OffsetMap TargetOffsets() {
    return {{PartitionBucket("", /*bucket=*/0), 7},
            {PartitionBucket("dt=2", /*bucket=*/2), 9},
            {PartitionBucket("dt=2", /*bucket=*/10), 12}};
}

}  // namespace

TEST_F(RealtimeSnapshotPropertiesTest, SerializeAndDeserializeTargetJson) {
    RealtimeSnapshotProperties::OffsetMap expected = TargetOffsets();
    ASSERT_OK_AND_ASSIGN(std::string actual_json,
                         RealtimeSnapshotProperties::SerializeOffsets(expected));
    ASSERT_EQ(kTargetJson, actual_json);

    ASSERT_OK_AND_ASSIGN(RealtimeSnapshotProperties::OffsetMap actual, ReadJson(kTargetJson));
    ASSERT_EQ(expected, actual);
    ASSERT_OK_AND_ASSIGN(std::string round_trip_json,
                         RealtimeSnapshotProperties::SerializeOffsets(actual));
    ASSERT_EQ(kTargetJson, round_trip_json);
}

TEST_F(RealtimeSnapshotPropertiesTest, SerializeEmptyOffsets) {
    ASSERT_OK_AND_ASSIGN(std::string actual_json,
                         RealtimeSnapshotProperties::SerializeOffsets(/*offsets=*/{}));
    ASSERT_EQ(R"({
    "version": 1,
    "offsets": []
})",
              actual_json);
}

TEST_F(RealtimeSnapshotPropertiesTest, SerializeRejectsInvalidOffsets) {
    RealtimeSnapshotProperties::OffsetMap negative_bucket = {{PartitionBucket("dt=2", -1), 0}};
    ASSERT_NOK_WITH_MSG(RealtimeSnapshotProperties::SerializeOffsets(negative_bucket),
                        "invalid bucket -1");

    RealtimeSnapshotProperties::OffsetMap negative_offset = {
        {PartitionBucket("dt=2", /*bucket=*/0), -1}};
    ASSERT_NOK_WITH_MSG(RealtimeSnapshotProperties::SerializeOffsets(negative_offset),
                        "invalid offset -1");
}

TEST_F(RealtimeSnapshotPropertiesTest, NormalizePartitionBucketAndOffsetsDirectory) {
    ASSERT_EQ("dt=2", PartitionBucket::NormalizePartition("dt=2/"));
    PartitionBucket expected_partition_bucket("dt=2", 3);
    ASSERT_EQ(expected_partition_bucket, PartitionBucket("dt=2/", /*bucket=*/3));
    ASSERT_EQ("/table/metadata", RealtimeSnapshotProperties::OffsetsDirectory("/table", "main"));
    ASSERT_EQ("/table/branch/branch-dev/metadata",
              RealtimeSnapshotProperties::OffsetsDirectory("/table", "dev"));
}

TEST_F(RealtimeSnapshotPropertiesTest, ReadOffsetsWithoutProgress) {
    ASSERT_OK_AND_ASSIGN(
        RealtimeSnapshotProperties::OffsetMap no_snapshot,
        RealtimeSnapshotProperties::ReadOffsets(/*snapshot=*/std::nullopt, file_system_));
    ASSERT_TRUE(no_snapshot.empty());

    ASSERT_OK_AND_ASSIGN(RealtimeSnapshotProperties::OffsetMap no_properties,
                         RealtimeSnapshotProperties::ReadOffsets(
                             std::optional<Snapshot>(MakeSnapshot(std::nullopt)), file_system_));
    ASSERT_TRUE(no_properties.empty());

    std::map<std::string, std::string> unrelated_properties = {{"other", "value"}};
    ASSERT_OK_AND_ASSIGN(
        RealtimeSnapshotProperties::OffsetMap no_offsets_property,
        RealtimeSnapshotProperties::ReadOffsets(
            std::optional<Snapshot>(MakeSnapshot(unrelated_properties)), file_system_));
    ASSERT_TRUE(no_offsets_property.empty());
}

TEST_F(RealtimeSnapshotPropertiesTest, ReadOffsetsRequiresFileSystem) {
    std::map<std::string, std::string> properties = {
        {RealtimeSnapshotProperties::kOffsetsKey, "metadata/test.offsets"}};
    ASSERT_NOK_WITH_MSG(
        RealtimeSnapshotProperties::ReadOffsets(std::optional<Snapshot>(MakeSnapshot(properties)),
                                                /*file_system=*/nullptr),
        "file system is null");
}

TEST_F(RealtimeSnapshotPropertiesTest, ReadOffsetsPropagatesFileError) {
    std::map<std::string, std::string> properties = {
        {RealtimeSnapshotProperties::kOffsetsKey, directory_->Str() + "/missing.offsets"}};
    ASSERT_NOK(RealtimeSnapshotProperties::ReadOffsets(
        std::optional<Snapshot>(MakeSnapshot(properties)), file_system_));
}

TEST_F(RealtimeSnapshotPropertiesTest, DeserializeRejectsInvalidJson) {
    const std::vector<std::pair<std::string, std::string>> invalid_json_cases = {
        {"{", "deserialize failed"},
        {R"([])", "value must be an object"},
        {R"({"offsets":[]})", "key must exist"},
        {R"({"version":2,"offsets":[]})", "unsupported offsets version 2"},
        {R"({"version":1,"offsets":{}})", "value must be an array"},
        {R"({"version":1,"offsets":[{"bucket":0,"offset":1}]})", "key must exist"},
        {R"({"version":1,"offsets":[{"partition":"dt=2","offset":1}]})", "key must exist"},
        {R"({"version":1,"offsets":[{"partition":"dt=2","bucket":0}]})", "key must exist"},
        {R"({"version":1,"offsets":[{"partition":"dt=2","bucket":"0","offset":1}]})",
         "value must be int"},
        {R"({"version":1,"offsets":[{"partition":"dt=2","bucket":0,"offset":"1"}]})",
         "value must be int64"},
        {R"({"version":1,"offsets":[{"partition":"dt=2","bucket":-1,"offset":1}]})",
         "invalid bucket -1"},
        {R"({"version":1,"offsets":[{"partition":"dt=2","bucket":0,"offset":-1}]})",
         "invalid offset -1"},
        {R"({"version":1,"offsets":[{"partition":"dt=2","bucket":0,"offset":1},{"partition":"dt=2/","bucket":0,"offset":2}]})",
         "duplicate partition 'dt=2/' bucket 0"}};

    for (const auto& [json, expected_error] : invalid_json_cases) {
        SCOPED_TRACE(json);
        ASSERT_NOK_WITH_MSG(ReadJson(json), expected_error);
    }
}

TEST_F(RealtimeSnapshotPropertiesTest, ValidateAndOrderProgress) {
    PartitionBucket bucket0("dt=2", /*bucket=*/0);
    PartitionBucket bucket1("dt=2", /*bucket=*/1);
    RealtimeSnapshotProperties::OffsetMap committed_offsets = {{bucket1, 4}};
    std::vector<RealtimeCommitProgress> commits = {
        {/*commit_message=*/nullptr, "dt=2", 0, Range(2, 3)},
        {/*commit_message=*/nullptr, "dt=2", 1, Range(5, 6)},
        {/*commit_message=*/nullptr, "dt=2", 0, Range(0, 1)}};

    ASSERT_OK_AND_ASSIGN(RealtimeSnapshotProperties::ValidatedCommitProgress validated_progress,
                         RealtimeSnapshotProperties::ValidateProgress(commits, committed_offsets));
    ASSERT_EQ(3, validated_progress.ordered_commits.size());
    ASSERT_EQ(Range(0, 1), validated_progress.ordered_commits[0].offset_range);
    ASSERT_EQ(Range(2, 3), validated_progress.ordered_commits[1].offset_range);
    ASSERT_EQ(Range(5, 6), validated_progress.ordered_commits[2].offset_range);
    ASSERT_EQ(3, validated_progress.delta_offsets.at(bucket0));
    ASSERT_EQ(6, validated_progress.delta_offsets.at(bucket1));
}

TEST_F(RealtimeSnapshotPropertiesTest, ValidateProgressRejectsInvalidProgress) {
    PartitionBucket bucket0("dt=2", /*bucket=*/0);
    RealtimeSnapshotProperties::OffsetMap committed_offsets = {{bucket0, 1}};

    std::vector<RealtimeCommitProgress> invalid_bucket = {
        {/*commit_message=*/nullptr, "dt=2", -1, Range(0, 0)}};
    ASSERT_NOK_WITH_MSG(
        RealtimeSnapshotProperties::ValidateProgress(invalid_bucket, /*committed_offsets=*/{}),
        "bucket -1 is invalid");

    std::vector<RealtimeCommitProgress> gap = {
        {/*commit_message=*/nullptr, "dt=2", 0, Range(3, 4)}};
    ASSERT_NOK_WITH_MSG(RealtimeSnapshotProperties::ValidateProgress(gap, committed_offsets),
                        "are not contiguous");

    std::vector<RealtimeCommitProgress> overlap = {
        {/*commit_message=*/nullptr, "dt=2", 0, Range(1, 2)}};
    ASSERT_NOK_WITH_MSG(RealtimeSnapshotProperties::ValidateProgress(overlap, committed_offsets),
                        "are not contiguous");

    RealtimeSnapshotProperties::OffsetMap exhausted_offsets = {
        {bucket0, std::numeric_limits<int64_t>::max()}};
    std::vector<RealtimeCommitProgress> after_max = {
        {/*commit_message=*/nullptr, "dt=2", 0,
         Range(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max())}};
    ASSERT_NOK_WITH_MSG(RealtimeSnapshotProperties::ValidateProgress(after_max, exhausted_offsets),
                        "are not contiguous");
}

TEST_F(RealtimeSnapshotPropertiesTest, ValidateEmptyProgress) {
    ASSERT_OK_AND_ASSIGN(
        RealtimeSnapshotProperties::ValidatedCommitProgress validated_progress,
        RealtimeSnapshotProperties::ValidateProgress(/*commits=*/{}, /*committed_offsets=*/{}));
    ASSERT_TRUE(validated_progress.ordered_commits.empty());
    ASSERT_TRUE(validated_progress.delta_offsets.empty());
}

TEST_F(RealtimeSnapshotPropertiesTest, MergeOffsetsWithoutDelta) {
    std::string latest_offsets_path = directory_->Str() + "/latest.offsets";
    std::map<std::string, std::string> latest_properties = {
        {RealtimeSnapshotProperties::kOffsetsKey, latest_offsets_path}};
    std::map<std::string, std::string> properties = {{"custom", "value"}};

    ASSERT_OK_AND_ASSIGN(Properties merged,
                         RealtimeSnapshotProperties::MergeOffsets(
                             properties, std::optional<Snapshot>(MakeSnapshot(latest_properties)),
                             file_system_, directory_->Str() + "/metadata"));
    ASSERT_EQ("value", merged.at("custom"));
    ASSERT_EQ(latest_offsets_path, merged.at(RealtimeSnapshotProperties::kOffsetsKey));

    ASSERT_OK_AND_ASSIGN(
        Properties merged_without_snapshot,
        RealtimeSnapshotProperties::MergeOffsets(properties, /*latest_snapshot=*/std::nullopt,
                                                 file_system_, directory_->Str() + "/metadata"));
    ASSERT_EQ(properties, merged_without_snapshot);
}

TEST_F(RealtimeSnapshotPropertiesTest, MergeOffsetsWritesMergedProgress) {
    ASSERT_OK_AND_ASSIGN(std::string latest_offsets_path, WriteJson(kTargetJson));
    std::map<std::string, std::string> latest_properties = {
        {RealtimeSnapshotProperties::kOffsetsKey, latest_offsets_path}};

    RealtimeSnapshotProperties::OffsetMap delta_offsets = {
        {PartitionBucket("", /*bucket=*/0), 5},
        {PartitionBucket("dt=2", /*bucket=*/2), 11},
        {PartitionBucket("dt=3", /*bucket=*/0), 4}};
    ASSERT_OK_AND_ASSIGN(std::string delta_json,
                         RealtimeSnapshotProperties::SerializeOffsets(delta_offsets));
    std::map<std::string, std::string> properties = {
        {"custom", "value"}, {RealtimeSnapshotProperties::kOffsetsDeltaKey, delta_json}};

    ASSERT_OK_AND_ASSIGN(Properties merged,
                         RealtimeSnapshotProperties::MergeOffsets(
                             properties, std::optional<Snapshot>(MakeSnapshot(latest_properties)),
                             file_system_, directory_->Str() + "/metadata"));
    ASSERT_EQ("value", merged.at("custom"));
    ASSERT_EQ(0, merged.count(RealtimeSnapshotProperties::kOffsetsDeltaKey));
    ASSERT_NE(latest_offsets_path, merged.at(RealtimeSnapshotProperties::kOffsetsKey));

    ASSERT_OK_AND_ASSIGN(RealtimeSnapshotProperties::OffsetMap actual,
                         RealtimeSnapshotProperties::ReadOffsets(
                             std::optional<Snapshot>(MakeSnapshot(merged)), file_system_));
    RealtimeSnapshotProperties::OffsetMap expected = TargetOffsets();
    expected[PartitionBucket("dt=2", /*bucket=*/2)] = 11;
    expected[PartitionBucket("dt=3", /*bucket=*/0)] = 4;
    ASSERT_EQ(expected, actual);
}

TEST_F(RealtimeSnapshotPropertiesTest, MergeEmptyDelta) {
    ASSERT_OK_AND_ASSIGN(std::string empty_delta,
                         RealtimeSnapshotProperties::SerializeOffsets(/*offsets=*/{}));
    std::map<std::string, std::string> properties = {
        {"custom", "value"}, {RealtimeSnapshotProperties::kOffsetsDeltaKey, empty_delta}};

    ASSERT_OK_AND_ASSIGN(Properties merged,
                         RealtimeSnapshotProperties::MergeOffsets(
                             properties, /*latest_snapshot=*/std::nullopt, /*file_system=*/nullptr,
                             /*offsets_directory=*/""));
    std::map<std::string, std::string> expected = {{"custom", "value"}};
    ASSERT_EQ(expected, merged);
}

TEST_F(RealtimeSnapshotPropertiesTest, MergeOffsetsRequiresFileSystem) {
    RealtimeSnapshotProperties::OffsetMap delta_offsets = {
        {PartitionBucket("dt=2", /*bucket=*/0), 1}};
    ASSERT_OK_AND_ASSIGN(std::string delta_json,
                         RealtimeSnapshotProperties::SerializeOffsets(delta_offsets));
    std::map<std::string, std::string> properties = {
        {RealtimeSnapshotProperties::kOffsetsDeltaKey, delta_json}};
    ASSERT_NOK_WITH_MSG(RealtimeSnapshotProperties::MergeOffsets(
                            properties, /*latest_snapshot=*/std::nullopt, /*file_system=*/nullptr,
                            directory_->Str() + "/metadata"),
                        "file system is null");

    ASSERT_NOK_WITH_MSG(
        RealtimeSnapshotProperties::MergeOffsets(properties, /*latest_snapshot=*/std::nullopt,
                                                 file_system_, /*offsets_directory=*/""),
        "offsets directory is empty");
}

}  // namespace paimon::test

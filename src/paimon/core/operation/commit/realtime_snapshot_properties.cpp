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

#include <algorithm>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"

namespace paimon {
namespace {

class OffsetEntryJson {
 public:
    OffsetEntryJson() = default;

    OffsetEntryJson(std::string partition, int32_t bucket, int64_t offset)
        : partition_(std::move(partition)), bucket_(bucket), offset_(offset) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const {
        rapidjson::Value value(rapidjson::kObjectType);
        value.AddMember("partition", RapidJsonUtil::SerializeValue(partition_, allocator),
                        *allocator);
        value.AddMember("bucket", RapidJsonUtil::SerializeValue(bucket_, allocator), *allocator);
        value.AddMember("offset", RapidJsonUtil::SerializeValue(offset_, allocator), *allocator);
        return value;
    }

    void FromJson(const rapidjson::Value& value) {
        partition_ = RapidJsonUtil::DeserializeKeyValue<std::string>(value, "partition");
        bucket_ = RapidJsonUtil::DeserializeKeyValue<int32_t>(value, "bucket");
        offset_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(value, "offset");
    }

    const std::string& Partition() const {
        return partition_;
    }

    int32_t Bucket() const {
        return bucket_;
    }

    int64_t Offset() const {
        return offset_;
    }

 private:
    std::string partition_;
    int32_t bucket_ = -1;
    int64_t offset_ = -1;
};

class OffsetsJson {
 public:
    OffsetsJson() = default;

    explicit OffsetsJson(const RealtimeSnapshotProperties::OffsetMap& offsets)
        : offsets_(offsets) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const {
        rapidjson::Value value(rapidjson::kObjectType);
        value.AddMember(
            "version",
            RapidJsonUtil::SerializeValue(RealtimeSnapshotProperties::kOffsetsVersion, allocator),
            *allocator);
        std::vector<OffsetEntryJson> entries;
        entries.reserve(offsets_.size());
        for (const auto& [partition_bucket, offset] : offsets_) {
            if (partition_bucket.bucket < 0) {
                throw std::invalid_argument(
                    fmt::format("invalid bucket {} in offsets", partition_bucket.bucket));
            }
            if (offset < 0) {
                throw std::invalid_argument(
                    fmt::format("invalid offset {} for partition '{}' "
                                "bucket {}",
                                offset, partition_bucket.partition, partition_bucket.bucket));
            }
            entries.emplace_back(partition_bucket.partition, partition_bucket.bucket, offset);
        }
        value.AddMember("offsets", RapidJsonUtil::SerializeValue(entries, allocator), *allocator);
        return value;
    }

    void FromJson(const rapidjson::Value& value) {
        int32_t version = RapidJsonUtil::DeserializeKeyValue<int32_t>(value, "version");
        if (version != RealtimeSnapshotProperties::kOffsetsVersion) {
            throw std::invalid_argument(fmt::format("unsupported offsets version {}", version));
        }
        std::vector<OffsetEntryJson> entries =
            RapidJsonUtil::DeserializeKeyValue<std::vector<OffsetEntryJson>>(value, "offsets");
        offsets_.clear();
        for (const OffsetEntryJson& entry : entries) {
            if (entry.Bucket() < 0) {
                throw std::invalid_argument(
                    fmt::format("invalid bucket {} in offsets", entry.Bucket()));
            }
            if (entry.Offset() < 0) {
                throw std::invalid_argument(
                    fmt::format("invalid offset {} in offsets", entry.Offset()));
            }
            PartitionBucket partition_bucket(entry.Partition(), entry.Bucket());
            if (!offsets_.emplace(std::move(partition_bucket), entry.Offset()).second) {
                throw std::invalid_argument(
                    fmt::format("duplicate partition '{}' bucket {} in offsets", entry.Partition(),
                                entry.Bucket()));
            }
        }
    }

    const RealtimeSnapshotProperties::OffsetMap& Offsets() const {
        return offsets_;
    }

 private:
    RealtimeSnapshotProperties::OffsetMap offsets_;
};

}  // namespace

Result<RealtimeSnapshotProperties::ValidatedCommitProgress>
RealtimeSnapshotProperties::ValidateProgress(const std::vector<RealtimeCommitProgress>& commits,
                                             const OffsetMap& committed_offsets) {
    ValidatedCommitProgress result;
    result.ordered_commits = commits;
    std::stable_sort(result.ordered_commits.begin(), result.ordered_commits.end(),
                     [](const RealtimeCommitProgress& lhs, const RealtimeCommitProgress& rhs) {
                         if (lhs.partition != rhs.partition) {
                             return lhs.partition < rhs.partition;
                         }
                         if (lhs.bucket != rhs.bucket) {
                             return lhs.bucket < rhs.bucket;
                         }
                         return lhs.offset_range.from < rhs.offset_range.from;
                     });

    OffsetMap last_offsets;
    for (const RealtimeCommitProgress& commit : result.ordered_commits) {
        if (commit.bucket < 0) {
            return Status::Invalid(
                fmt::format("real-time commit bucket {} is invalid", commit.bucket));
        }
        if (commit.offset_range.from > commit.offset_range.to) {
            return Status::Invalid("real-time commit offset range is invalid");
        }

        PartitionBucket partition_bucket(commit.partition, commit.bucket);
        auto last_iter = last_offsets.find(partition_bucket);
        int64_t previous_offset = -1;
        if (last_iter != last_offsets.end()) {
            previous_offset = last_iter->second;
        } else {
            auto committed_iter = committed_offsets.find(partition_bucket);
            if (committed_iter != committed_offsets.end()) {
                previous_offset = committed_iter->second;
            }
        }
        if (previous_offset == std::numeric_limits<int64_t>::max() ||
            commit.offset_range.from != previous_offset + 1) {
            return Status::Invalid(fmt::format(
                "real-time commit offsets for partition '{}' bucket {} are not contiguous",
                commit.partition, commit.bucket));
        }

        last_offsets[partition_bucket] = commit.offset_range.to;
        result.delta_offsets[partition_bucket] = commit.offset_range.to;
    }
    return result;
}

std::string RealtimeSnapshotProperties::OffsetsDirectory(const std::string& table_root,
                                                         const std::string& branch) {
    return PathUtil::JoinPath(BranchManager::BranchPath(table_root, branch), "metadata");
}

Result<RealtimeSnapshotProperties::OffsetMap> RealtimeSnapshotProperties::ReadOffsets(
    const std::optional<Snapshot>& snapshot, const std::shared_ptr<FileSystem>& file_system) {
    if (!snapshot || !snapshot->Properties()) {
        return OffsetMap{};
    }
    const std::map<std::string, std::string>& properties = snapshot->Properties().value();
    auto iter = properties.find(kOffsetsKey);
    if (iter == properties.end()) {
        return OffsetMap{};
    }
    if (file_system == nullptr) {
        return Status::Invalid("file system is null when reading real-time offsets");
    }
    std::string content;
    PAIMON_RETURN_NOT_OK(file_system->ReadFile(iter->second, &content));
    return ParseOffsets(content);
}

Result<std::string> RealtimeSnapshotProperties::SerializeOffsets(const OffsetMap& offsets) {
    std::string result;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(OffsetsJson(offsets), &result));
    return result;
}

Result<std::map<std::string, std::string>> RealtimeSnapshotProperties::MergeOffsets(
    const std::map<std::string, std::string>& properties,
    const std::optional<Snapshot>& latest_snapshot, const std::shared_ptr<FileSystem>& file_system,
    const std::string& offsets_directory) {
    std::map<std::string, std::string> merged_properties = properties;
    auto delta_iter = properties.find(kOffsetsDeltaKey);
    merged_properties.erase(kOffsetsDeltaKey);
    if (delta_iter == properties.end()) {
        if (latest_snapshot && latest_snapshot->Properties()) {
            const std::map<std::string, std::string>& latest_properties =
                latest_snapshot->Properties().value();
            auto offsets_iter = latest_properties.find(kOffsetsKey);
            if (offsets_iter != latest_properties.end()) {
                merged_properties[kOffsetsKey] = offsets_iter->second;
            }
        }
        return merged_properties;
    }

    PAIMON_ASSIGN_OR_RAISE(OffsetMap merged_offsets, ReadOffsets(latest_snapshot, file_system));
    PAIMON_ASSIGN_OR_RAISE(OffsetMap delta_offsets, ParseOffsets(delta_iter->second));
    for (const auto& [key, offset] : delta_offsets) {
        auto merged_iter = merged_offsets.find(key);
        if (merged_iter == merged_offsets.end()) {
            merged_offsets.emplace(key, offset);
        } else {
            merged_iter->second = std::max(merged_iter->second, offset);
        }
    }
    if (!merged_offsets.empty()) {
        PAIMON_ASSIGN_OR_RAISE(merged_properties[kOffsetsKey],
                               WriteOffsets(merged_offsets, file_system, offsets_directory));
    }
    return merged_properties;
}

Result<std::string> RealtimeSnapshotProperties::WriteOffsets(
    const OffsetMap& offsets, const std::shared_ptr<FileSystem>& file_system,
    const std::string& offsets_directory) {
    if (file_system == nullptr) {
        return Status::Invalid("file system is null when writing real-time offsets");
    }
    if (offsets_directory.empty()) {
        return Status::Invalid("real-time offsets directory is empty");
    }
    std::string uuid;
    if (!UUID::Generate(&uuid)) {
        return Status::Invalid("fail to generate uuid for real-time offsets file");
    }
    PAIMON_RETURN_NOT_OK(file_system->Mkdirs(offsets_directory));
    std::string path = PathUtil::JoinPath(offsets_directory, uuid + ".offsets");
    PAIMON_ASSIGN_OR_RAISE(std::string content, SerializeOffsets(offsets));
    PAIMON_RETURN_NOT_OK(file_system->WriteFile(path, content, /*overwrite=*/false));
    return path;
}

Result<RealtimeSnapshotProperties::OffsetMap> RealtimeSnapshotProperties::ParseOffsets(
    const std::string& value) {
    OffsetsJson offsets_json;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(value, &offsets_json));
    return offsets_json.Offsets();
}

}  // namespace paimon

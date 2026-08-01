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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/realtime/partition_bucket.h"
#include "paimon/core/snapshot.h"
#include "paimon/realtime/realtime_commit_progress.h"
#include "paimon/result.h"

namespace paimon {

class FileSystem;

class RealtimeSnapshotProperties {
 public:
    using OffsetMap = std::map<PartitionBucket, int64_t>;

    /// Ordered commit progress and the latest offset contributed by each partition-bucket.
    struct ValidatedCommitProgress {
        std::vector<RealtimeCommitProgress> ordered_commits;
        OffsetMap delta_offsets;
    };

    RealtimeSnapshotProperties() = delete;

    static constexpr const char* kOffsetsKey = "realtime.offsets";
    static constexpr const char* kOffsetsDeltaKey = "realtime.offsets.delta";
    static constexpr int32_t kOffsetsVersion = 1;

    /// Orders progress entries and verifies they form a contiguous prefix after committed offsets.
    static Result<ValidatedCommitProgress> ValidateProgress(
        const std::vector<RealtimeCommitProgress>& commits, const OffsetMap& committed_offsets);

    static std::string OffsetsDirectory(const std::string& table_root, const std::string& branch);

    static Result<OffsetMap> ReadOffsets(const std::optional<Snapshot>& snapshot,
                                         const std::shared_ptr<FileSystem>& file_system);

    static Result<std::string> SerializeOffsets(const OffsetMap& offsets);

    static Result<std::map<std::string, std::string>> MergeOffsets(
        const std::map<std::string, std::string>& properties,
        const std::optional<Snapshot>& latest_snapshot,
        const std::shared_ptr<FileSystem>& file_system, const std::string& offsets_directory);

 private:
    static Result<std::string> WriteOffsets(const OffsetMap& offsets,
                                            const std::shared_ptr<FileSystem>& file_system,
                                            const std::string& offsets_directory);

    static Result<OffsetMap> ParseOffsets(const std::string& value);
};

}  // namespace paimon

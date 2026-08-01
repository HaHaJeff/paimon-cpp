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
#include <string>
#include <utility>

#include "paimon/common/utils/path_util.h"

namespace paimon {

/// Identifies one partition-bucket after normalizing the partition path.
struct PartitionBucket {
    PartitionBucket(std::string partition, int32_t bucket)
        : partition(NormalizePartition(std::move(partition))), bucket(bucket) {}

    static std::string NormalizePartition(std::string partition) {
        PathUtil::TrimLastDelim(&partition);
        return partition;
    }

    bool operator<(const PartitionBucket& other) const {
        if (partition != other.partition) {
            return partition < other.partition;
        }
        return bucket < other.bucket;
    }

    bool operator==(const PartitionBucket& other) const {
        return partition == other.partition && bucket == other.bucket;
    }

    std::string partition;
    int32_t bucket = -1;
};

}  // namespace paimon

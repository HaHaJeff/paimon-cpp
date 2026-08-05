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

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>

namespace paimon {

/// Identifies one partition-bucket by its logical partition values.
struct PartitionBucket {
    PartitionBucket(std::map<std::string, std::string> partition, int32_t bucket)
        : partition(std::move(partition)), bucket(bucket) {}

    bool operator<(const PartitionBucket& other) const {
        if (partition != other.partition) {
            return partition < other.partition;
        }
        return bucket < other.bucket;
    }

    bool operator==(const PartitionBucket& other) const {
        return partition == other.partition && bucket == other.bucket;
    }

    std::map<std::string, std::string> partition;
    int32_t bucket = -1;
};

}  // namespace paimon

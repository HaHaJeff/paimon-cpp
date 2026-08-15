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
#include <memory>
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class MemIndexer;
class MemoryPool;

/// Recovery parameters for one primary-key partition-bucket indexer.
struct PAIMON_EXPORT PrimaryKeyMemIndexerCreationContext {
    /// Logical partition values, before partition-path escaping.
    std::map<std::string, std::string> partition;
    /// Fixed bucket id.
    int32_t bucket;
    /// Largest sequence number restored from the bucket's committed files, or `-1` if empty.
    int64_t restore_max_sequence_number;
};

/// Factory for application-provided primary-key `MemIndexer` implementations.
///
/// Commit readers must expose the raw mutation stream, including every update and delete. Query
/// readers must not apply predicate pushdown; they must be sorted by primary key, preserve
/// `_VALUE_KIND`, include `_SEQUENCE_NUMBER` when requested, and merge mutations within each memory
/// segment for disk-memory merge-on-read.
class PAIMON_EXPORT PrimaryKeyMemIndexerFactory {
 public:
    virtual ~PrimaryKeyMemIndexerFactory() = default;

    /// Creates an indexer for one primary-key partition-bucket and its recovery position.
    virtual Result<std::shared_ptr<MemIndexer>> Create(
        std::unique_ptr<::ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const PrimaryKeyMemIndexerCreationContext& context) = 0;
};

}  // namespace paimon

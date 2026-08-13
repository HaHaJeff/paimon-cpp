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

#pragma once

#include <memory>
#include <vector>

#include "paimon/result.h"

namespace arrow {
class Schema;
}

namespace paimon {

class InternalReadContext;
class KeyValueRecordReader;
class MemoryPool;
class RealtimeSplit;

/// Builds the generic merge-on-read input contributed by a PK real-time memory view.
class PrimaryKeyMergeInput {
 public:
    static Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> Create(
        const std::shared_ptr<RealtimeSplit>& split,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::shared_ptr<InternalReadContext>& context,
        const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon

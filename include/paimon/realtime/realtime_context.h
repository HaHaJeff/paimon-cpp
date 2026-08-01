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
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class MemIndexer;
class MemIndexerFactory;
class MemoryPool;

/// Shared context that owns the `MemIndexer` instances used by a real-time writer.
///
/// Applications attach one context to `WriteContext`. The context uses either the default Arrow
/// implementation or an application-provided factory and keeps each created indexer available
/// across multiple writes and prepare-commit operations.
class PAIMON_EXPORT RealtimeContext {
 public:
    /// Creates a context backed by Paimon's default Arrow `MemIndexer`.
    static Result<std::shared_ptr<RealtimeContext>> Create();

    /// Creates a context backed by an application-provided indexer factory.
    ///
    /// @param factory Non-null factory used to create indexers on demand.
    static Result<std::shared_ptr<RealtimeContext>> Create(
        const std::shared_ptr<MemIndexerFactory>& factory);

    /// Returns the stable indexer associated with a partition and bucket, creating it if needed.
    Result<std::shared_ptr<MemIndexer>> GetOrCreateMemIndexer(
        const std::string& partition, int32_t bucket, std::unique_ptr<::ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool);

    ~RealtimeContext();

 private:
    class Impl;

    explicit RealtimeContext(std::unique_ptr<Impl>&& impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon

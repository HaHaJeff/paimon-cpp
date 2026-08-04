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
#include <vector>

#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class MemIndexer;
class MemIndexerFactory;
class MemReadView;
class MemoryPool;

/// One partition-bucket and the immutable plugin view captured for a table scan.
struct PAIMON_EXPORT RealtimePartitionBucketView {
    /// Logical partition values, before partition-path escaping.
    std::map<std::string, std::string> partition;
    /// Fixed bucket id.
    int32_t bucket;
    /// Plugin instance that creates readers from `read_view`.
    std::shared_ptr<MemIndexer> indexer;
    /// Immutable rows pinned for one query plan.
    std::shared_ptr<MemReadView> read_view;
};

/// Committed progress for one partition-bucket loaded from a Paimon snapshot.
struct PAIMON_EXPORT RealtimePartitionBucketOffset {
    /// Logical partition values, before partition-path escaping.
    std::map<std::string, std::string> partition;
    /// Fixed bucket id.
    int32_t bucket;
    /// Largest `_OFFSET` committed for this partition-bucket.
    int64_t offset;
};

/// Shared context that owns the `MemIndexer` instances used by a real-time writer.
///
/// Applications share one context between `WriteContext` and `ScanContext`. The context uses
/// either the default Arrow implementation or an application-provided factory and keeps each
/// created indexer available across writes, prepare-commit operations, and process-local reads.
class PAIMON_EXPORT RealtimeContext {
 public:
    /// Required non-null BIGINT field carrying partition-bucket real-time progress.
    inline static constexpr char kOffsetFieldName[] = "_OFFSET";

    /// Prepends the required `_OFFSET` field to a table schema.
    ///
    /// Ownership of `schema` is transferred to this method. The returned Arrow C schema preserves
    /// the input fields and schema metadata. An input schema that already contains
    /// `kOffsetFieldName` is rejected.
    static Result<std::unique_ptr<::ArrowSchema>> BuildRealtimeSchema(
        std::unique_ptr<::ArrowSchema> schema);

    /// Creates a context backed by Paimon's default Arrow `MemIndexer`.
    static Result<std::shared_ptr<RealtimeContext>> Create();

    /// Creates a context backed by an application-provided indexer factory.
    ///
    /// @param factory Non-null factory used to create indexers on demand.
    static Result<std::shared_ptr<RealtimeContext>> Create(
        const std::shared_ptr<MemIndexerFactory>& factory);

    /// Returns the stable indexer associated with a partition and bucket, creating it if needed.
    Result<std::shared_ptr<MemIndexer>> GetOrCreateMemIndexer(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        std::unique_ptr<::ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool);

    /// Captures an immutable read view from every currently registered indexer.
    ///
    /// The indexer registry is fixed during this call and each returned plugin view is stable. New
    /// partition-buckets registered after this call are not visible in that query.
    Result<std::vector<RealtimePartitionBucketView>> AcquireReadViews();

    /// Advances the committed progress visible to the registered memory indexers.
    ///
    /// Calls are idempotent for the same snapshot and must advance snapshot ids monotonically.
    /// Each indexer is notified outside the context's registry lock and may choose how and when to
    /// release sealed data covered by its committed offset. Existing read views continue to pin
    /// referenced resources until their readers are closed.
    Status AdvanceCommittedProgress(
        int64_t snapshot_id, const std::vector<RealtimePartitionBucketOffset>& committed_offsets);

    ~RealtimeContext();

 private:
    class Impl;

    explicit RealtimeContext(std::unique_ptr<Impl>&& impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon

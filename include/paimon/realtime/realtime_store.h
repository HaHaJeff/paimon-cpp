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
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/c/abi.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/statistics_mode.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class MemoryPool;
class Predicate;

struct PAIMON_EXPORT AppendRealtimeStoreCreateConfig {
    StatisticsMode statistics_mode;
};

struct PAIMON_EXPORT PrimaryKeyRealtimeStoreCreateConfig {};

using RealtimeStoreCreateConfig =
    std::variant<AppendRealtimeStoreCreateConfig, PrimaryKeyRealtimeStoreCreateConfig>;

struct PAIMON_EXPORT RealtimeStoreCreateRequest {
    /// Schema whose ownership is transferred to the factory. Append mode receives the complete
    /// table write schema. Primary-key mode receives the prepared transport schema:
    /// [_VALUE_KIND, _SEQUENCE_NUMBER, _REALTIME_OFFSET, table write fields].
    std::unique_ptr<::ArrowSchema> write_schema;
    std::map<std::string, std::string> options;
    std::shared_ptr<MemoryPool> memory_pool;
    std::map<std::string, std::string> partition;
    int32_t bucket = -1;
    RealtimeStoreCreateConfig mode_config;
};

/// A record batch and its framework-assigned contiguous offset range.
///
/// Append-mode batches contain table write fields, and row `i` is associated with
/// `offset_range.begin + i`. Primary-key batches contain the prepared transport schema supplied
/// to the factory and are physically sorted by full primary key then sequence number; their
/// per-row `_REALTIME_OFFSET` field preserves the original write-order offset after sorting.
struct PAIMON_EXPORT RealtimeWriteBatch {
    /// Input batch whose ownership is transferred to `RealtimeStore::Write`.
    std::unique_ptr<RecordBatch> batch;
    /// Left-closed, right-open offset range covered by `batch`.
    OffsetRange offset_range;
};

/// Opaque handle to an immutable segment returned by `RealtimeStore::SealForCommit`.
///
/// A plugin may store the segment in memory or in spill files. Callers use this handle only to
/// request commit readers and inspect its offset range.
class PAIMON_EXPORT RealtimeSegmentHandle {
 public:
    virtual ~RealtimeSegmentHandle() = default;

    /// Returns the left-closed, right-open offset range covered by this segment.
    virtual OffsetRange GetOffsetRange() const = 0;
};

/// Opaque immutable view of the rows visible from one `RealtimeStore`.
///
/// A view pins all referenced resources until the readers created from it are closed. Later
/// writes, seals, and committed-offset reclamation do not change the contents of an existing view.
class PAIMON_EXPORT RealtimeReadView {
 public:
    virtual ~RealtimeReadView() = default;

    /// Returns the left-closed, right-open offset range visible in this view, or no range when it
    /// is empty.
    virtual std::optional<OffsetRange> GetOffsetRange() const = 0;
};

/// Parameters used by a `RealtimeStore` to create readers for a query.
struct PAIMON_EXPORT RealtimeQueryContext {
    /// Append mode receives the requested output fields before the mandatory leading
    /// `_VALUE_KIND` field is added. Primary-key mode receives the complete prepared schema.
    ::ArrowSchema* read_schema;
    /// Predicate using field indexes from `read_schema`.
    std::shared_ptr<Predicate> predicate;
    /// Whether the plugin may use `predicate` to prune candidate rows.
    ///
    /// Keep this disabled for primary-key merge-on-read. Pruning memory before PK merge may remove
    /// the newest row and incorrectly expose an older disk row. Exact predicate filtering, when
    /// requested, is applied by the Paimon read framework after plugin reader creation.
    bool enable_predicate_pushdown;
};

/// Customizable plugin interface for storing and querying real-time rows before Paimon data-file
/// generation.
///
/// Paimon serializes calls to `Write` and `SealForCommit` for the same store. After sealing,
/// `CreateCommitReaders` may read the immutable sealed segment while later `Write` calls append to
/// a new building segment. Paimon retains control of file format, rolling, indexes, and
/// commit-message generation. A store may choose its own in-memory representation, indexes, and
/// spill strategy. It may outlive an individual writer because the shared real-time context and
/// active read views retain it.
class PAIMON_EXPORT RealtimeStore {
 public:
    virtual ~RealtimeStore() = default;

    /// Adds a batch to the current building segment.
    ///
    /// The row count matches the framework-assigned `offset_range`.
    virtual Status Write(RealtimeWriteBatch&& batch) = 0;

    /// Seals the current building data and opens a new building segment.
    ///
    /// Returns an immutable segment handle, or `std::nullopt` when there is no data to seal.
    virtual Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() = 0;

    /// Creates readers that expose all rows in a sealed segment for Paimon file writing.
    ///
    /// Concatenating the returned readers must produce every sealed row exactly once. Append-mode
    /// readers preserve write order and contain `_VALUE_KIND` followed by table write fields.
    /// Primary-key readers expose raw prepared rows. Each returned reader's complete stream,
    /// including across `NextBatch` boundaries, is sorted by full primary key then sequence
    /// number; all readers collectively cover sealed mutations exactly once. Reader cardinality is
    /// independent of the number of writes. Paimon adapts and merges those rows before writing
    /// files.
    virtual Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) = 0;

    /// Acquires an immutable view containing the current sealed and building rows.
    ///
    /// This method may be called concurrently with query-reader creation and reclamation. It must
    /// also provide a consistent snapshot when a write or seal is in progress.
    virtual Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() = 0;

    /// Creates readers over rows in `view`. Append mode returns rows whose offsets are greater
    /// than or equal to `offset_begin`. Primary-key mode ignores `offset_begin` and returns raw
    /// prepared rows; Paimon applies offset filtering, projection, and merge-on-read adaptation.
    ///
    /// Append-mode output batches contain `_VALUE_KIND` first, followed by requested fields except
    /// a duplicate `_VALUE_KIND`; all returned append readers collectively cover every matching
    /// row exactly once. Primary-key output batches use the prepared transport schema and may
    /// contain multiple mutations per key. Each returned primary-key reader's complete stream is
    /// sorted by full primary key then sequence number, and all readers collectively cover raw
    /// mutations exactly once. Reader cardinality is independent of the number of writes. Paimon
    /// retains `view` for the lifetime of the resulting framework reader.
    virtual Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view, int64_t offset_begin,
        const RealtimeQueryContext& context) = 0;

    /// Notifies the store that its partition-bucket committed end offset has advanced.
    ///
    /// Calls are monotonic and may repeat the same offset after a previous call reports an error,
    /// so implementations must apply this notification idempotently.
    ///
    /// An implementation may reclaim covered segments immediately, defer destruction, spill them,
    /// or retain them. Existing read views continue to keep referenced resources alive.
    virtual Status AdvanceCommittedOffset(int64_t committed_end_offset) = 0;

    /// Returns the number of bytes currently retained by building and sealed segments.
    virtual uint64_t GetMemoryUsage() const = 0;
};

/// Factory for application-provided `RealtimeStore` implementations.
class PAIMON_EXPORT RealtimeStoreFactory {
 public:
    virtual ~RealtimeStoreFactory() = default;

    /// Creates a store for the requested table mode and partition-bucket.
    /// The factory consumes `request`, including ownership of `request.write_schema`.
    virtual Result<std::shared_ptr<RealtimeStore>> Create(RealtimeStoreCreateRequest&& request) = 0;
};

}  // namespace paimon

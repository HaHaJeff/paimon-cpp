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

#include "paimon/reader/batch_reader.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/utils/range.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class MemoryPool;
class Predicate;

/// A record batch and the contiguous offset range assigned to its rows.
///
/// The batch contains only the table write fields. `_OFFSET` is carried separately by
/// `offset_range`; row `i` corresponds to `offset_range.from + i`. Paimon adds the physical
/// `_OFFSET` column when the sealed segment is written to data files.
struct PAIMON_EXPORT RealtimeWriteBatch {
    /// Input batch whose ownership is transferred to `MemIndexer::Write`.
    std::unique_ptr<RecordBatch> batch;
    /// Inclusive `[from, to]` offset range covered by `batch`.
    Range offset_range;
};

/// Opaque handle to an immutable segment returned by `MemIndexer::SealForCommit`.
///
/// A plugin may store the segment in memory or in spill files. Callers use this handle only to
/// request commit readers and inspect its offset range.
class PAIMON_EXPORT RealtimeSegmentHandle {
 public:
    virtual ~RealtimeSegmentHandle() = default;

    /// Returns the inclusive offset range covered by this segment.
    virtual Range GetOffsetRange() const = 0;
};

/// Opaque immutable view of the rows visible from one `MemIndexer`.
///
/// A view pins all referenced resources until the readers created from it are closed. Later
/// writes, seals, and committed-offset reclamation do not change the contents of an existing view.
class PAIMON_EXPORT MemReadView {
 public:
    virtual ~MemReadView() = default;

    /// Returns the inclusive offset range visible in this view, or no range when it is empty.
    virtual std::optional<Range> GetOffsetRange() const = 0;
};

/// Parameters used by a `MemIndexer` to create readers for a query.
struct PAIMON_EXPORT MemQueryContext {
    /// Requested output fields before the mandatory leading `_VALUE_KIND` field is added.
    ///
    /// The schema uses the Arrow C Data Interface and is valid only during
    /// `CreateQueryReaders`. An implementation may consume it with an Arrow importer.
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

/// Plugin interface for buffering real-time writes before Paimon data-file generation.
///
/// Paimon serializes calls to `Write` and `SealForCommit` for the same indexer. After sealing,
/// `CreateCommitReaders` may read the immutable sealed segment while later `Write` calls append to
/// a new building segment. Paimon retains control of file format, rolling, indexes, and
/// commit-message generation.
class PAIMON_EXPORT MemIndexer {
 public:
    virtual ~MemIndexer() = default;

    /// Adds a batch to the current building segment.
    ///
    /// The number of rows must equal the size of `offset_range`.
    virtual Status Write(RealtimeWriteBatch&& batch) = 0;

    /// Seals the current building data and opens a new building segment.
    ///
    /// Returns an immutable segment handle, or `std::nullopt` when there is no data to seal.
    virtual Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() = 0;

    /// Creates readers that expose all rows in a sealed segment for Paimon file writing.
    ///
    /// Concatenating the returned readers must produce every sealed row exactly once and in write
    /// order. Each output batch contains `_VALUE_KIND` followed by all fields from the factory's
    /// `write_schema`; it does not contain `_OFFSET`.
    virtual Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) = 0;

    /// Acquires an immutable view containing the current sealed and building rows.
    ///
    /// This method may be called concurrently with query-reader creation and reclamation. It must
    /// also provide a consistent snapshot when a write or seal is in progress.
    virtual Result<std::shared_ptr<MemReadView>> AcquireReadView() = 0;

    /// Creates readers over rows in `view` whose offsets are greater than
    /// `offset_lower_exclusive`.
    ///
    /// Each output batch contains `_VALUE_KIND` first, followed by the fields requested by
    /// `context.read_schema` except a duplicate `_VALUE_KIND`. `_OFFSET` is returned when requested
    /// by the read schema. Concatenating all returned readers must produce every matching row once.
    virtual Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<MemReadView>& view, int64_t offset_lower_exclusive,
        const MemQueryContext& context) = 0;

    /// Releases this indexer's ownership of sealed segments fully covered by the committed offset.
    /// Existing read views continue to keep their referenced resources alive.
    virtual Status Reclaim(int64_t committed_offset) = 0;

    /// Returns the number of bytes currently retained by building and sealed segments.
    virtual uint64_t GetMemoryUsage() const = 0;

    /// Releases resources owned by this indexer and rejects subsequent writes or seals.
    virtual Status Close() = 0;
};

/// Factory for application-provided `MemIndexer` implementations.
class PAIMON_EXPORT MemIndexerFactory {
 public:
    virtual ~MemIndexerFactory() = default;

    /// Creates an indexer configured with the supplied schema, options, and memory pool.
    ///
    /// `write_schema` uses the Arrow C Data Interface and contains the table fields accepted by
    /// `Write`. It is valid only during this call. An implementation may consume its contents by
    /// using an Arrow C Data Interface importer; otherwise Paimon releases them after this method
    /// returns.
    /// @param write_schema Table write schema without Paimon-generated real-time fields.
    /// @param options Effective table options available to the indexer.
    /// @param memory_pool Memory pool provided by the write context.
    virtual Result<std::shared_ptr<MemIndexer>> Create(
        ::ArrowSchema* write_schema, const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool) = 0;
};

}  // namespace paimon

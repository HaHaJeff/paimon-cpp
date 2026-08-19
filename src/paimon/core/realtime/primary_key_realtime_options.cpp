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

#include "paimon/core/realtime/primary_key_realtime_options.h"

#include "paimon/core/core_options.h"

namespace paimon {

Status ValidatePrimaryKeyRealtimeOptions(const CoreOptions& options) {
    if (options.GetBucket() <= 0) {
        return Status::NotImplemented("PK realtime v1 requires fixed buckets");
    }
    if (options.GetMergeEngine() != MergeEngine::DEDUPLICATE) {
        return Status::NotImplemented("PK realtime v1 supports only the DEDUPLICATE merge engine");
    }
    if (options.DataEvolutionEnabled()) {
        return Status::NotImplemented("PK realtime v1 does not support data evolution");
    }
    if (!options.GetFieldsSequenceGroups().empty()) {
        return Status::NotImplemented("PK realtime v1 does not support sequence groups");
    }
    if (options.IgnoreDelete() || options.PartialUpdateRemoveRecordOnDelete() ||
        options.AggregationRemoveRecordOnDelete() ||
        !options.GetPartialUpdateRemoveRecordOnSequenceGroup().empty()) {
        return Status::NotImplemented("PK realtime v1 requires default delete behavior");
    }
    if (!options.GetSequenceField().empty()) {
        return Status::NotImplemented("PK realtime v1 does not support sequence.field");
    }
    if (!options.SequenceFieldSortOrderIsAscending()) {
        return Status::NotImplemented(
            "PK realtime v1 supports only ascending sequence.field.sort-order");
    }
    if (options.NeedLookup() || options.DeletionVectorsEnabled() ||
        options.GetChangelogProducer() != ChangelogProducer::NONE) {
        return Status::NotImplemented("PK realtime v1 does not support lookup or early MOR");
    }
    return Status::OK();
}

}  // namespace paimon

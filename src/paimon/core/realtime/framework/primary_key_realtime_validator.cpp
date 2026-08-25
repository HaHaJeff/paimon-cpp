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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/realtime/framework/primary_key_realtime_validator.h"

#include <vector>

#include "arrow/type.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/macros.h"

namespace paimon {

Status PrimaryKeyRealtimeValidator::ValidateOptions(const CoreOptions& options,
                                                    const TableSchema& schema) {
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
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> primary_key_fields,
                           schema.TrimmedPrimaryKeyFields());
    for (const DataField& field : primary_key_fields) {
        if (field.Type()->id() == arrow::Type::FLOAT || field.Type()->id() == arrow::Type::DOUBLE) {
            return Status::NotImplemented(
                "PK realtime v1 does not support FLOAT or DOUBLE primary keys");
        }
    }
    if (options.GlobalIndexEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexDefinitions definitions,
                               PrimaryKeyIndexDefinitions::Create(schema));
        if (!definitions.Definitions().empty()) {
            return Status::NotImplemented("PK realtime v1 does not support global indexes");
        }
    }
    return Status::OK();
}

}  // namespace paimon

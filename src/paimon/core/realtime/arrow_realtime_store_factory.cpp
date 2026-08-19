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

#include "paimon/realtime/arrow_realtime_store_factory.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/realtime/arrow_realtime_store.h"
#include "paimon/core/realtime/primary_key_realtime_store.h"
#include "paimon/macros.h"

namespace paimon {

Result<std::shared_ptr<RealtimeStore>> ArrowRealtimeStoreFactory::Create(
    RealtimeStoreCreateRequest&& request) {
    if (!request.write_schema || !request.write_schema->release) {
        return Status::Invalid("real-time store write schema is null");
    }
    ScopeGuard schema_guard(
        [schema = request.write_schema.get()]() { ArrowSchemaRelease(schema); });
    if (!request.memory_pool) {
        return Status::Invalid("real-time store memory pool is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> imported_schema,
                                      arrow::ImportSchema(request.write_schema.get()));
    if (std::holds_alternative<AppendRealtimeStoreCreateConfig>(request.mode_config)) {
        std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(request.memory_pool);
        return std::make_shared<ArrowRealtimeStore>(imported_schema, request.memory_pool,
                                                    arrow_pool);
    }

    const PrimaryKeyRealtimeStoreCreateConfig& primary_key_config =
        std::get<PrimaryKeyRealtimeStoreCreateConfig>(request.mode_config);
    std::vector<DataField> key_fields;
    key_fields.reserve(primary_key_config.primary_keys.size());
    for (const std::string& primary_key : primary_key_config.primary_keys) {
        const int32_t field_index = imported_schema->GetFieldIndex(primary_key);
        if (field_index < 0) {
            return Status::Invalid("primary key ", primary_key, " is missing from write schema");
        }
        key_fields.emplace_back(field_index, imported_schema->field(field_index));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(key_fields, /*is_ascending_order=*/true));
    auto merge_function_wrapper_factory = []() {
        auto merge_function = std::make_unique<DeduplicateMergeFunction>(
            /*ignore_delete=*/false);
        return std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
    };
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(request.options));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<PrimaryKeyRealtimeStore> store,
        PrimaryKeyRealtimeStore::Create(imported_schema, primary_key_config.primary_keys,
                                        key_comparator, merge_function_wrapper_factory,
                                        primary_key_config.restore_max_sequence_number,
                                        core_options.GetReadBatchSize(), request.memory_pool));
    return std::shared_ptr<RealtimeStore>(std::move(store));
}

}  // namespace paimon

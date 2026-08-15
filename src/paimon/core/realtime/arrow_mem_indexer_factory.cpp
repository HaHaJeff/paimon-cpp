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

#include "paimon/realtime/arrow_mem_indexer_factory.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/realtime/arrow_mem_indexer.h"
#include "paimon/core/realtime/primary_key_mem_indexer.h"
#include "paimon/macros.h"

namespace paimon {

Result<std::shared_ptr<MemIndexer>> ArrowMemIndexerFactory::Create(
    MemIndexerCreateRequest&& request) {
    if (!request.write_schema || !request.write_schema->release) {
        return Status::Invalid("mem indexer write schema is null");
    }
    ScopeGuard schema_guard(
        [schema = request.write_schema.get()]() { ArrowSchemaRelease(schema); });
    if (!request.memory_pool) {
        return Status::Invalid("mem indexer memory pool is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> imported_schema,
                                      arrow::ImportSchema(request.write_schema.get()));
    const AppendMemIndexerCreateConfig* append_config =
        std::get_if<AppendMemIndexerCreateConfig>(&request.mode_config);
    if (append_config) {
        std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(request.memory_pool);
        return std::make_shared<ArrowMemIndexer>(imported_schema, request.memory_pool, arrow_pool);
    }
    const PrimaryKeyMemIndexerCreateConfig& primary_key_config =
        std::get<PrimaryKeyMemIndexerCreateConfig>(request.mode_config);
    if (!primary_key_config.file_system) {
        return Status::Invalid("primary-key mem indexer file system is null");
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(request.options, primary_key_config.file_system));
    std::vector<DataField> key_fields;
    key_fields.reserve(primary_key_config.primary_keys.size());
    for (int32_t i = 0; i < static_cast<int32_t>(primary_key_config.primary_keys.size()); ++i) {
        std::shared_ptr<arrow::Field> field =
            imported_schema->GetFieldByName(primary_key_config.primary_keys[i]);
        if (!field) {
            return Status::Invalid("primary key ", primary_key_config.primary_keys[i],
                                   " is missing from write schema");
        }
        key_fields.emplace_back(i, field);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(key_fields, /*is_ascending_order=*/true));
    auto merge_function_wrapper_factory = []() {
        auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        return std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
    };
    auto io_manager = std::make_shared<IOManager>(primary_key_config.temp_directory,
                                                  primary_key_config.file_system);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<PrimaryKeyMemIndexer> indexer,
        PrimaryKeyMemIndexer::Create(imported_schema, primary_key_config.primary_keys,
                                     key_comparator, merge_function_wrapper_factory,
                                     primary_key_config.restore_max_sequence_number, core_options,
                                     io_manager, primary_key_config.enable_multi_thread_spill,
                                     request.memory_pool));
    return std::shared_ptr<MemIndexer>(std::move(indexer));
}

}  // namespace paimon

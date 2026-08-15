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

#include "paimon/core/realtime/primary_key_mem_indexer_factory.h"

#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/realtime/primary_key_mem_indexer.h"
#include "paimon/macros.h"

namespace paimon {

ArrowPrimaryKeyMemIndexerFactory::ArrowPrimaryKeyMemIndexerFactory(
    std::vector<std::string> trimmed_primary_keys, const std::shared_ptr<FileSystem>& file_system,
    std::string temp_directory, bool enable_multi_thread_spill)
    : trimmed_primary_keys_(std::move(trimmed_primary_keys)),
      file_system_(file_system),
      temp_directory_(std::move(temp_directory)),
      enable_multi_thread_spill_(enable_multi_thread_spill) {}

Result<std::shared_ptr<MemIndexer>> ArrowPrimaryKeyMemIndexerFactory::Create(
    std::unique_ptr<ArrowSchema> write_schema, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const PrimaryKeyMemIndexerCreationContext& context) {
    if (!write_schema || !write_schema->release) {
        return Status::Invalid("mem indexer write schema is null");
    }
    ScopeGuard schema_guard([schema = write_schema.get()]() { ArrowSchemaRelease(schema); });
    if (!memory_pool) {
        return Status::Invalid("mem indexer memory pool is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> imported_schema,
                                      arrow::ImportSchema(write_schema.get()));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options, file_system_));
    std::vector<DataField> key_fields;
    key_fields.reserve(trimmed_primary_keys_.size());
    for (int32_t i = 0; i < static_cast<int32_t>(trimmed_primary_keys_.size()); ++i) {
        std::shared_ptr<arrow::Field> field =
            imported_schema->GetFieldByName(trimmed_primary_keys_[i]);
        if (!field) {
            return Status::Invalid("primary key ", trimmed_primary_keys_[i],
                                   " is missing from write schema");
        }
        key_fields.emplace_back(i, field);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(key_fields, /*is_ascending_order=*/true));
    auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
    auto merge_wrapper = std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
    auto io_manager = std::make_shared<IOManager>(temp_directory_, file_system_);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<PrimaryKeyMemIndexer> indexer,
                           PrimaryKeyMemIndexer::Create(
                               imported_schema, trimmed_primary_keys_, key_comparator,
                               merge_wrapper, context.restore_max_sequence_number, core_options,
                               io_manager, enable_multi_thread_spill_, memory_pool));
    return std::shared_ptr<MemIndexer>(std::move(indexer));
}

}  // namespace paimon

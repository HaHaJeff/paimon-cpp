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
#include <string>
#include <vector>

#include "paimon/realtime/primary_key_mem_indexer_factory.h"

namespace paimon {

class FileSystem;

class ArrowPrimaryKeyMemIndexerFactory final : public PrimaryKeyMemIndexerFactory {
 public:
    ArrowPrimaryKeyMemIndexerFactory(std::vector<std::string> trimmed_primary_keys,
                                     const std::shared_ptr<FileSystem>& file_system,
                                     std::string temp_directory, bool enable_multi_thread_spill);

    Result<std::shared_ptr<MemIndexer>> Create(
        std::unique_ptr<::ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const PrimaryKeyMemIndexerCreationContext& context) override;

 private:
    const std::vector<std::string> trimmed_primary_keys_;
    const std::shared_ptr<FileSystem> file_system_;
    const std::string temp_directory_;
    const bool enable_multi_thread_spill_;
};

}  // namespace paimon

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

#include <algorithm>

#include "arrow/type.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/status.h"

namespace paimon {

/// Helpers for the physical table field used as real-time partition-bucket progress.
struct RealtimeOffset {
    /// Validates the table-level contract required by real-time reads and writes.
    static Status ValidateTableSchema(const TableSchema& table_schema) {
        Result<DataField> field_result = table_schema.GetField(RealtimeContext::kOffsetFieldName);
        if (!field_result.ok()) {
            return Status::Invalid("real-time mode requires a _OFFSET field in the table schema");
        }
        DataField field = field_result.value();
        if (field.Type()->id() != arrow::Type::INT64) {
            return Status::Invalid("real-time _OFFSET field must have BIGINT type");
        }
        if (field.Nullable()) {
            return Status::Invalid("real-time _OFFSET field must be non-nullable");
        }
        if (std::find(table_schema.PartitionKeys().begin(), table_schema.PartitionKeys().end(),
                      RealtimeContext::kOffsetFieldName) != table_schema.PartitionKeys().end()) {
            return Status::Invalid("real-time _OFFSET field cannot be a partition field");
        }
        return Status::OK();
    }
};

}  // namespace paimon

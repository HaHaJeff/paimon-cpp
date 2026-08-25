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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<TableSchema> PkSchema(
    const std::shared_ptr<arrow::DataType>& key_type = arrow::int64(),
    const std::map<std::string, std::string>& options = {}) {
    return TableSchema::Create(
               /*schema_id=*/0,
               arrow::schema({arrow::field("id", key_type), arrow::field("value", arrow::utf8())}),
               /*partition_keys=*/{}, /*primary_keys=*/{"id"}, options)
        .value();
}

}  // namespace

TEST(PrimaryKeyRealtimeValidatorTest, TestSupportedOptions) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({{Options::BUCKET, "1"}}));
    ASSERT_OK(PrimaryKeyRealtimeValidator::ValidateOptions(options, *PkSchema()));
}

TEST(PrimaryKeyRealtimeValidatorTest, TestUnsupportedOptions) {
    const std::string sequence_group =
        std::string(Options::FIELDS_PREFIX) + ".value." + Options::SEQUENCE_GROUP;
    const std::vector<std::map<std::string, std::string>> unsupported_options = {
        {{Options::BUCKET, "0"}},
        {{Options::BUCKET, "1"}, {Options::MERGE_ENGINE, "partial-update"}},
        {{Options::BUCKET, "1"}, {Options::DATA_EVOLUTION_ENABLED, "true"}},
        {{Options::BUCKET, "1"}, {sequence_group, "seq"}},
        {{Options::BUCKET, "1"}, {Options::SEQUENCE_FIELD, "seq"}},
        {{Options::BUCKET, "1"}, {Options::FORCE_LOOKUP, "true"}},
        {{Options::BUCKET, "1"}, {Options::DELETION_VECTORS_ENABLED, "true"}},
        {{Options::BUCKET, "1"}, {Options::CHANGELOG_PRODUCER, "input"}},
    };
    for (const std::map<std::string, std::string>& option_map : unsupported_options) {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(option_map));
        ASSERT_NOK(PrimaryKeyRealtimeValidator::ValidateOptions(options, *PkSchema()));
    }
}

TEST(PrimaryKeyRealtimeValidatorTest, TestRejectsFloatingPrimaryKeys) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({{Options::BUCKET, "1"}}));
    ASSERT_NOK_WITH_MSG(
        PrimaryKeyRealtimeValidator::ValidateOptions(options, *PkSchema(arrow::float32())),
        "FLOAT or DOUBLE primary keys");
    ASSERT_NOK_WITH_MSG(
        PrimaryKeyRealtimeValidator::ValidateOptions(options, *PkSchema(arrow::float64())),
        "FLOAT or DOUBLE primary keys");
}

TEST(PrimaryKeyRealtimeValidatorTest, TestRejectsEnabledGlobalIndex) {
    const std::map<std::string, std::string> option_map = {{Options::BUCKET, "1"},
                                                           {Options::PK_BTREE_INDEX_COLUMNS, "id"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(option_map));
    ASSERT_NOK_WITH_MSG(PrimaryKeyRealtimeValidator::ValidateOptions(
                            options, *PkSchema(arrow::int64(), option_map)),
                        "does not support global indexes");
}

}  // namespace paimon::test

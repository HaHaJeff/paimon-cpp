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

#include "paimon/core/casting/numeric_primitive_cast_executor.h"

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
NumericPrimitiveCastExecutor::NumericPrimitiveCastExecutor() {
    literal_cast_executor_map_ = {
        {std::make_pair(FieldType::TINYINT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::SMALLINT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::INT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::INT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::INT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::INT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::INT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::INT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::BIGINT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::FLOAT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<float, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<float, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<float, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<float, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<float, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<float, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::DOUBLE, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<double, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<double, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<double, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<double, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<double, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::DOUBLE), [&](const Literal& literal) {
             return CastLiteral<double, double>(literal, FieldType::DOUBLE);
         }}};
}

Result<Literal> NumericPrimitiveCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    FieldType src_type = literal.GetType();
    PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                           FieldTypeUtils::ConvertToFieldType(target_type->id()));
    auto iter = literal_cast_executor_map_.find(std::make_pair(src_type, target_field_type));
    if (iter == literal_cast_executor_map_.end()) {
        return Status::Invalid(
            fmt::format("cast literal in NumericPrimitiveCastExecutor failed: cannot find cast "
                        "function from {} to {}",
                        FieldTypeUtils::FieldTypeToString(src_type),
                        FieldTypeUtils::FieldTypeToString(target_field_type)));
    }
    return iter->second(literal);
}

namespace {
template <typename SrcArrayType, typename TargetBuilderType, typename TargetType>
Result<std::shared_ptr<arrow::Array>> JavaCastFloatingArray(const arrow::Array& src_array,
                                                            arrow::MemoryPool* pool) {
    const auto& typed_src_array = static_cast<const SrcArrayType&>(src_array);
    TargetBuilderType builder(pool);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(typed_src_array.length()));
    for (int64_t i = 0; i < typed_src_array.length(); i++) {
        if (typed_src_array.IsNull(i)) {
            builder.UnsafeAppendNull();
        } else {
            builder.UnsafeAppend(
                NumericPrimitiveCastExecutor::JavaFloatingToIntegerCast<TargetType>(
                    typed_src_array.Value(i)));
        }
    }
    std::shared_ptr<arrow::Array> target_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&target_array));
    return target_array;
}

template <typename SrcArrayType>
Result<std::shared_ptr<arrow::Array>> JavaCastFloatingArrayToInteger(
    const arrow::Array& src_array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) {
    switch (target_type->id()) {
        case arrow::Type::INT8:
            return JavaCastFloatingArray<SrcArrayType, arrow::Int8Builder, int8_t>(src_array, pool);
        case arrow::Type::INT16:
            return JavaCastFloatingArray<SrcArrayType, arrow::Int16Builder, int16_t>(src_array,
                                                                                     pool);
        case arrow::Type::INT32:
            return JavaCastFloatingArray<SrcArrayType, arrow::Int32Builder, int32_t>(src_array,
                                                                                     pool);
        case arrow::Type::INT64:
            return JavaCastFloatingArray<SrcArrayType, arrow::Int64Builder, int64_t>(src_array,
                                                                                     pool);
        default:
            return Status::Invalid(
                fmt::format("cast array in NumericPrimitiveCastExecutor failed: {} is not an "
                            "integer target type",
                            target_type->ToString()));
    }
}

// Deliberately not arrow::is_integer(): that also matches the unsigned types, which
// JavaCastFloatingArrayToInteger() does not handle and which still belong to Arrow's kernel.
bool IsSignedIntegerType(arrow::Type::type type) {
    return type == arrow::Type::INT8 || type == arrow::Type::INT16 || type == arrow::Type::INT32 ||
           type == arrow::Type::INT64;
}
}  // namespace

Result<std::shared_ptr<arrow::Array>> NumericPrimitiveCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    // Arrow's floating point to integer kernel is a plain static_cast, annotated
    // ARROW_DISABLE_UBSAN("float-cast-overflow"), so it is undefined for a value that does not
    // fit and answers it differently per architecture. Convert those here instead, with the same
    // Java semantics the literal overload uses, so that stats converted through one path and
    // column data converted through the other agree. This trades Arrow's vectorized kernel for a
    // scalar loop, which these conversions, reached from schema evolution, can afford.
    if (IsSignedIntegerType(target_type->id())) {
        if (array->type_id() == arrow::Type::FLOAT) {
            return JavaCastFloatingArrayToInteger<arrow::FloatArray>(*array, target_type, pool);
        }
        if (array->type_id() == arrow::Type::DOUBLE) {
            return JavaCastFloatingArrayToInteger<arrow::DoubleArray>(*array, target_type, pool);
        }
    }
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    options.allow_int_overflow = true;
    options.allow_float_truncate = true;
    return CastingUtils::Cast(array, target_type, options, pool);
}

}  // namespace paimon

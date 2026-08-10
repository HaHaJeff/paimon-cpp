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
#include <cmath>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>

#include "arrow/array/array_base.h"
#include "paimon/core/casting/cast_executor.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class MemoryPool;
}  // namespace arrow

namespace paimon {
enum class FieldType;

class NumericPrimitiveCastExecutor : public CastExecutor {
 public:
    NumericPrimitiveCastExecutor();
    Result<Literal> Cast(const Literal& literal,
                         const std::shared_ptr<arrow::DataType>& target_type) const override;

    Result<std::shared_ptr<arrow::Array>> Cast(const std::shared_ptr<arrow::Array>& array,
                                               const std::shared_ptr<arrow::DataType>& target_type,
                                               arrow::MemoryPool* pool) const override;

    /// Converts a floating point value to an integer type the way Java does, where a plain
    /// `static_cast` would be undefined behavior with an architecture dependent result (see
    /// docs/code-style.md): `NaN` becomes 0, and a value that does not fit saturates.
    ///
    /// Java reaches TINYINT and SMALLINT by narrowing an `int`, so those saturate at the int32
    /// bounds and are then truncated to width, which is why `(byte) Float.MAX_VALUE` is -1 and
    /// not 127. BIGINT saturates at the int64 bounds.
    template <typename TargetType, typename SrcType>
    static TargetType JavaFloatingToIntegerCast(SrcType value) {
        static_assert(std::is_floating_point_v<SrcType>, "source must be floating point");
        static_assert(std::is_integral_v<TargetType>, "target must be integral");
        static_assert(std::is_signed_v<TargetType>, "target must be signed");
        using WideType = std::conditional_t<std::is_same_v<TargetType, int64_t>, int64_t, int32_t>;
        if (std::isnan(value)) {
            return 0;
        }
        WideType wide_value;
        // Comparing against the bounds converted to SrcType is what keeps the truncation defined.
        // The standard leaves that conversion a choice between the two adjacent representable
        // values; under the IEEE round-to-nearest the supported toolchains use, an unrepresentable
        // upper bound rounds up to a power of two, so every value that reaches the truncation is
        // inside the integer range.
        if (value >= static_cast<SrcType>(std::numeric_limits<WideType>::max())) {
            wide_value = std::numeric_limits<WideType>::max();
        } else if (value <= static_cast<SrcType>(std::numeric_limits<WideType>::lowest())) {
            wide_value = std::numeric_limits<WideType>::lowest();
        } else {
            wide_value = static_cast<WideType>(value);
        }
        return NarrowToTwosComplement<TargetType>(wide_value);
    }

 private:
    /// Truncates to the width of TargetType keeping the low bits, which is what Java narrowing
    /// does. Converting an out of range value to a signed type directly is implementation
    /// defined until C++20, so the low bits are taken through the unsigned type, where the
    /// wrap-around is defined, and only an in-range value is converted back.
    template <typename TargetType, typename WideType>
    static TargetType NarrowToTwosComplement(WideType value) {
        static_assert(std::is_signed_v<TargetType>, "target must be signed");
        using UnsignedTarget = std::make_unsigned_t<TargetType>;
        constexpr TargetType kMin = std::numeric_limits<TargetType>::min();
        // The unsigned value of the minimum is 2^(width-1), the point where the low bits start
        // standing for a negative number.
        constexpr auto kHalf = static_cast<UnsignedTarget>(kMin);
        const auto low_bits = static_cast<UnsignedTarget>(value);
        if (low_bits < kHalf) {
            return static_cast<TargetType>(low_bits);
        }
        // low_bits - kHalf is in range, so converting it back is defined, and adding the minimum
        // gives the negative value those bits stand for.
        return static_cast<TargetType>(static_cast<TargetType>(low_bits - kHalf) + kMin);
    }

    template <typename SrcType, typename TargetType>
    static Literal CastLiteral(const Literal& literal, const FieldType& target_type) {
        if (literal.IsNull()) {
            return Literal(target_type);
        }
        SrcType value = literal.GetValue<SrcType>();
        if constexpr (std::is_floating_point_v<SrcType> && std::is_integral_v<TargetType>) {
            return Literal(JavaFloatingToIntegerCast<TargetType>(value));
        } else {
            return Literal(static_cast<TargetType>(value));
        }
    }

 private:
    std::map<std::pair<FieldType, FieldType>, std::function<Literal(const Literal&)>>
        literal_cast_executor_map_;
};

}  // namespace paimon

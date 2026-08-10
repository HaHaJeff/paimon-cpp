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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include "paimon/visibility.h"

namespace paimon {

/// Despite the name, this computes CRC-32 with the zlib polynomial 0x04C11DB7,
/// not Castagnoli CRC-32C. The value is persisted in the SST block trailer and in
/// the B-tree global index, so it must stay identical across compilers,
/// architectures and build flags.
///
/// An SSE4.2 `_mm_crc32` kernel used to live here behind PAIMON_HAVE_SSE4_2. It
/// computed Castagnoli, i.e. a different checksum for the same bytes, so it was
/// removed rather than left switchable. Any future hardware kernel has to
/// implement the zlib polynomial and be checked against crc32c_test.cpp.
class PAIMON_EXPORT CRC32C {
 public:
    /// @param data bytes to checksum
    /// @param length number of bytes
    /// @param crc running value to continue from, 0 to start a new checksum
    /// @return the checksum of `data` appended to the stream `crc` stands for
    static uint32_t calculate(const char* data, size_t length, uint32_t crc = 0);
};
}  // namespace paimon

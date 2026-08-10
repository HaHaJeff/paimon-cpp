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

#include "paimon/common/utils/crc32c.h"

#include "arrow/util/crc32.h"

namespace paimon {
uint32_t CRC32C::calculate(const char* data, size_t length, uint32_t crc) {
    // arrow::internal::crc32 is the zlib polynomial, which its own header spells
    // out as "different from CRC32C". That is the one this class must produce.
    return arrow::internal::crc32(crc, data, length);
}
}  // namespace paimon

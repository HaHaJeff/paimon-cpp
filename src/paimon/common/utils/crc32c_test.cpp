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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace paimon::test {

namespace {

constexpr size_t kPatternLength = 64;
constexpr uint32_t kPatternCrc32 = 2227724424u;

/// Built as exact bytes: converting 128-255 to a plain `char` is implementation
/// defined, and whether `char` is signed depends on the ABI and compiler flags.
std::vector<uint8_t> MakePattern(size_t length) {
    std::vector<uint8_t> pattern(length);
    for (size_t i = 0; i < length; ++i) {
        pattern[i] = static_cast<uint8_t>((i * 31 + 7) % 256);
    }
    return pattern;
}

const char* AsChars(const std::vector<uint8_t>& bytes) {
    return reinterpret_cast<const char*>(bytes.data());
}

}  // namespace

TEST(CRC32CTest, TestSimple) {
    char a = 'a';
    ASSERT_EQ(CRC32C::calculate(&a, 1), 3904355907);

    std::string data = "hello paimon c++";
    ASSERT_EQ(CRC32C::calculate(data.c_str(), data.size()), 1311805437);
}

// The SST block trailer and the B-tree global index persist whatever this
// produces, so it has to keep matching zlib's CRC-32 (polynomial 0x04C11DB7)
// byte for byte -- the class name notwithstanding, it is not Castagnoli.
// Expected values come from Python's zlib.crc32, not from this implementation.
TEST(CRC32CTest, TestMatchesZlibCrc32ForAllLengthResidues) {
    struct Case {
        size_t length;
        uint32_t crc;
    };
    // Lengths cover every residue modulo 8, so each combination of the 8/4/2/1-byte
    // steps a hardware kernel would take is exercised at least once.
    const std::vector<Case> cases = {
        {0, 0u},           {1, 1281784366u},  {2, 3700752837u},
        {3, 1063713196u},  {4, 1199752182u},  {5, 4062898429u},
        {6, 690260224u},   {7, 1044920610u},  {8, 2807432232u},
        {15, 2407004859u}, {16, 104245397u},  {31, 1171692296u},
        {32, 1227094950u}, {63, 2034968221u}, {kPatternLength, kPatternCrc32},
    };

    const std::vector<uint8_t> pattern = MakePattern(kPatternLength);
    for (const Case& test_case : cases) {
        ASSERT_EQ(CRC32C::calculate(AsChars(pattern), test_case.length), test_case.crc)
            << "length=" << test_case.length;
    }
}

// A hardware kernel would consume leading bytes one at a time until the pointer
// is aligned, so pinning alignment independence now keeps such a kernel from
// landing with an alignment-dependent checksum.
TEST(CRC32CTest, TestIsIndependentOfInputAlignment) {
    const std::vector<uint8_t> pattern = MakePattern(kPatternLength);
    // alignas so that `offset` really is the address residue: a char array's own
    // alignment is 1, so its base could otherwise sit at any residue itself.
    alignas(8) std::array<char, kPatternLength + 8> shifted{};
    for (size_t offset = 0; offset < 8; ++offset) {
        std::memcpy(shifted.data() + offset, pattern.data(), pattern.size());
        ASSERT_EQ(CRC32C::calculate(shifted.data() + offset, pattern.size()), kPatternCrc32)
            << "offset=" << offset;
    }
}

// SstFileWriter checksums a block and then folds the compression byte into the
// running value, so chaining must agree with a single pass over the same bytes.
TEST(CRC32CTest, TestRunningChecksumMatchesSinglePass) {
    std::string data = "hello paimon c++";
    for (size_t split = 0; split <= data.size(); ++split) {
        uint32_t running = CRC32C::calculate(data.c_str(), split);
        running = CRC32C::calculate(data.c_str() + split, data.size() - split, running);
        ASSERT_EQ(running, CRC32C::calculate(data.c_str(), data.size())) << "split=" << split;
    }
}

// The test above only shows the two paths agree with each other, which would
// still hold if both were wrong. Pin the seeded path to zlib's own output too.
// Expected values come from Python's zlib.crc32(data, seed).
TEST(CRC32CTest, TestSeededChecksumMatchesZlibCrc32) {
    constexpr uint32_t kSeed = 0x12345678u;
    const std::vector<uint8_t> pattern = MakePattern(kPatternLength);

    // Continuing a stream without appending anything leaves the running value alone.
    ASSERT_EQ(CRC32C::calculate(AsChars(pattern), 0, kSeed), kSeed);

    ASSERT_EQ(CRC32C::calculate(AsChars(pattern), pattern.size(), kSeed), 3207811452u);

    std::string data = "hello paimon c++";
    ASSERT_EQ(CRC32C::calculate(data.c_str(), data.size(), kSeed), 671566677u);
}

}  // namespace paimon::test

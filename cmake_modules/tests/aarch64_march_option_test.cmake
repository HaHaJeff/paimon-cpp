# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Regression tests for how cmake_modules/DefineOptions.cmake resolves
# PAIMON_AARCH64_MARCH. On AArch64 targets SetupCxxFlags.cmake turns it into
# "-march=${PAIMON_AARCH64_MARCH}", except for an explicitly empty value, the
# opt-out it answers with no -march flag at all. Only an undefined value may fall
# back to the default: clobbering the empty opt-out would put the flag back. The
# options themselves are only defined for a top-level build, which makes the
# add_subdirectory() path the interesting one.
#
#   cmake -P cmake_modules/tests/aarch64_march_option_test.cmake
#
# Script mode does have a cache: `cmake -D... -P` populates one, and
# set(... CACHE ...) defers to it exactly as a real configure does. That makes the
# -D override testable, and it also means each case has to clear the cache entry as
# well as the normal variable, so an ambient -DPAIMON_AARCH64_MARCH cannot change
# the outcome.

# Pin the policies the real build runs under: the top-level CMakeLists.txt
# requires 3.22, so CMP0126 is NEW there and set(CACHE) leaves a normal variable
# alone. Script mode would otherwise apply the OLD behavior and erase the
# explicitly empty opt-out value.
cmake_minimum_required(VERSION 3.22)

set(DEFINE_OPTIONS_MODULE "${CMAKE_CURRENT_LIST_DIR}/../DefineOptions.cmake")
set(FAILURES 0)
set(CHECKS 0)

# A macro, not a function, so FAILURES accumulates in this scope. Pass <unset> as
# the preset to leave PAIMON_AARCH64_MARCH undefined going in.
macro(expect_march
      description
      top_level
      preset
      expected)
    if(${top_level})
        set(CMAKE_SOURCE_DIR "/paimon")
        set(CMAKE_CURRENT_SOURCE_DIR "/paimon")
    else()
        set(CMAKE_SOURCE_DIR "/superproject")
        set(CMAKE_CURRENT_SOURCE_DIR "/superproject/paimon")
    endif()

    unset(PAIMON_AARCH64_MARCH)
    unset(PAIMON_AARCH64_MARCH CACHE)
    if(NOT "${preset}" STREQUAL "<unset>")
        set(PAIMON_AARCH64_MARCH "${preset}")
    endif()

    include("${DEFINE_OPTIONS_MODULE}")

    math(EXPR CHECKS "${CHECKS} + 1")
    if(NOT PAIMON_AARCH64_MARCH STREQUAL "${expected}")
        math(EXPR FAILURES "${FAILURES} + 1")
        message(SEND_ERROR "${description}: expected '${expected}', got "
                           "'${PAIMON_AARCH64_MARCH}'")
    endif()
endmacro()

# What -DPAIMON_AARCH64_MARCH=... produces on a top-level build: the cache entry
# already exists, so define_option_string must not overwrite it and the fallback
# must not clobber it either.
macro(expect_march_cached description value expected)
    set(CMAKE_SOURCE_DIR "/paimon")
    set(CMAKE_CURRENT_SOURCE_DIR "/paimon")
    unset(PAIMON_AARCH64_MARCH)
    unset(PAIMON_AARCH64_MARCH CACHE)
    set(PAIMON_AARCH64_MARCH
        "${value}"
        CACHE STRING "" FORCE)

    include("${DEFINE_OPTIONS_MODULE}")

    math(EXPR CHECKS "${CHECKS} + 1")
    if(NOT PAIMON_AARCH64_MARCH STREQUAL "${expected}")
        math(EXPR FAILURES "${FAILURES} + 1")
        message(SEND_ERROR "${description}: expected '${expected}', got "
                           "'${PAIMON_AARCH64_MARCH}'")
    endif()
endmacro()

# The option is never defined on this path, so only the fallback can supply a value.
expect_march("add_subdirectory consumer gets the default" FALSE "<unset>" "armv8-a")
expect_march("add_subdirectory keeps an explicit value" FALSE "armv8.2-a+crc"
             "armv8.2-a+crc")

# An explicitly empty value is the opt-out and must survive the fallback.
expect_march("an empty value survives as the opt-out, off the top level" FALSE "" "")
expect_march("an empty value survives as the opt-out, on the top level" TRUE "" "")

expect_march("top-level build gets the default" TRUE "<unset>" "armv8-a")

expect_march_cached("a -D override survives" "armv9-a" "armv9-a")
expect_march_cached("a -D override may add extensions" "armv8.2-a+crc" "armv8.2-a+crc")
expect_march_cached("a -D empty override survives as the opt-out" "" "")

if(FAILURES GREATER 0)
    message(FATAL_ERROR "${FAILURES} of ${CHECKS} AArch64 -march checks failed")
endif()
message(STATUS "All ${CHECKS} AArch64 -march checks passed")

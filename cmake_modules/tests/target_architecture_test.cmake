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

# Regression tests for cmake_modules/TargetArchitecture.cmake. No toolchain is
# needed, so this runs anywhere CMake does:
#
#   cmake -P cmake_modules/tests/target_architecture_test.cmake

set(TARGET_ARCHITECTURE_MODULE "${CMAKE_CURRENT_LIST_DIR}/../TargetArchitecture.cmake")
set(FAILURES 0)
set(CHECKS 0)

# Macros, not functions, so FAILURES accumulates in this scope. Every branch of the
# module assigns both outputs, so cases cannot leak into one another. The case
# helpers take at most four positional arguments to stay within the repository
# cmake-format profile, which would otherwise wrap every call onto six lines.
macro(run_case)
    set(PAIMON_TARGET_PROCESSOR "<unset>")
    set(PAIMON_TARGET_CPU_FAMILY "<unset>")

    include("${TARGET_ARCHITECTURE_MODULE}")

    math(EXPR CHECKS "${CHECKS} + 1")
    if(NOT PAIMON_TARGET_PROCESSOR STREQUAL "${EXPECTED_PROCESSOR}"
       OR NOT PAIMON_TARGET_CPU_FAMILY STREQUAL "${EXPECTED_FAMILY}")
        math(EXPR FAILURES "${FAILURES} + 1")
        message(SEND_ERROR "${CASE}: expected processor '${EXPECTED_PROCESSOR}' family "
                           "'${EXPECTED_FAMILY}', got processor '${PAIMON_TARGET_PROCESSOR}' "
                           "family '${PAIMON_TARGET_CPU_FAMILY}'")
    endif()
endmacro()

# A non-Apple target. CMAKE_OSX_ARCHITECTURES must be ignored there, so the
# resolved processor is always CMAKE_SYSTEM_PROCESSOR.
macro(expect_non_apple
      description
      processor
      osx_archs
      family)
    set(CASE "${description}")
    set(APPLE FALSE)
    set(CMAKE_SYSTEM_PROCESSOR "${processor}")
    set(CMAKE_OSX_ARCHITECTURES "${osx_archs}")
    set(EXPECTED_PROCESSOR "${processor}")
    set(EXPECTED_FAMILY "${family}")
    run_case()
endmacro()

# An Apple target, where CMAKE_OSX_ARCHITECTURES decides and may disagree with the
# host processor.
macro(expect_apple
      host_processor
      osx_archs
      expected_processor
      family)
    set(CASE "apple host='${host_processor}' osx='${osx_archs}'")
    set(APPLE TRUE)
    set(CMAKE_SYSTEM_PROCESSOR "${host_processor}")
    set(CMAKE_OSX_ARCHITECTURES "${osx_archs}")
    set(EXPECTED_PROCESSOR "${expected_processor}")
    set(EXPECTED_FAMILY "${family}")
    run_case()
endmacro()

# x86_64 spellings across Linux, the BSDs and Windows.
expect_non_apple("linux x86_64" "x86_64" "" "x86")
expect_non_apple("lowercase amd64" "amd64" "" "x86")
expect_non_apple("windows AMD64" "AMD64" "" "x86")

# Arm64 spellings: Linux reports aarch64, Apple and Windows report arm64/ARM64.
expect_non_apple("linux aarch64" "aarch64" "" "aarch64")
expect_non_apple("lowercase arm64" "arm64" "" "aarch64")
expect_non_apple("windows ARM64" "ARM64" "" "aarch64")

# Architectures without Paimon tuning must configure, not fail.
expect_non_apple("ppc64le is not an error" "ppc64le" "" "unknown")
expect_non_apple("s390x is not an error" "s390x" "" "unknown")
expect_non_apple("riscv64 is not an error" "riscv64" "" "unknown")
expect_non_apple("32-bit arm is not aarch64" "armv7l" "" "unknown")
expect_non_apple("empty processor" "" "" "unknown")

# CMAKE_OSX_ARCHITECTURES describes the target only on Apple platforms. A value
# inherited from a superproject or left in the cache must not retarget a Linux
# build, which would otherwise add an Arm -march= flag to an x86_64 compiler.
expect_non_apple("linux ignores stale osx arm64" "x86_64" "arm64" "x86")
expect_non_apple("linux ignores stale osx universal" "x86_64" "arm64;x86_64" "x86")

# On Apple the compiler targets CMAKE_OSX_ARCHITECTURES, which may disagree with
# the host CMAKE_SYSTEM_PROCESSOR.
expect_apple("arm64" "" "arm64" "aarch64")
expect_apple("x86_64" "arm64" "arm64" "aarch64")
expect_apple("arm64" "x86_64" "x86_64" "x86")

# A universal binary targets several architectures, so none of them may be tuned for.
expect_apple("arm64" "arm64;x86_64" "" "unknown")

if(FAILURES GREATER 0)
    message(FATAL_ERROR "${FAILURES} of ${CHECKS} target architecture checks failed")
endif()
message(STATUS "All ${CHECKS} target architecture checks passed")

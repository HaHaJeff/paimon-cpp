# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(paimon_sanitizer_flags INTERFACE)

if(PAIMON_USE_ASAN AND PAIMON_USE_TSAN)
    message(FATAL_ERROR "Address Sanitizer and Thread Sanitizer cannot be enabled together"
    )
endif()

if(PAIMON_USE_ASAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(paimon_sanitizer_flags INTERFACE -fsanitize=address
                                                                -fno-omit-frame-pointer)
        target_link_options(paimon_sanitizer_flags INTERFACE -fsanitize=address)
        message(STATUS "Address Sanitizer enabled")
    else()
        message(WARNING "Address Sanitizer is only supported for GCC and Clang compilers")
    endif()
endif()

if(PAIMON_USE_TSAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        # Bundled dependencies are linked statically into Paimon. Instrument them too so TSAN can
        # observe their synchronization primitives and does not report false races at the boundary.
        string(APPEND CMAKE_C_FLAGS " -fsanitize=thread -fno-omit-frame-pointer")
        string(APPEND CMAKE_CXX_FLAGS " -fsanitize=thread -fno-omit-frame-pointer")
        target_link_options(paimon_sanitizer_flags INTERFACE -fsanitize=thread)
        message(STATUS "Thread Sanitizer enabled")
    else()
        message(WARNING "Thread Sanitizer is only supported for GCC and Clang compilers")
    endif()
endif()

if(PAIMON_USE_UBSAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(paimon_sanitizer_flags
                               INTERFACE -fsanitize=undefined -fno-sanitize=vptr
                                         -fno-omit-frame-pointer)
        target_link_options(paimon_sanitizer_flags INTERFACE -fsanitize=undefined)
        # The signed-integer-overflow check on a 128-bit multiplication becomes a call to
        # __muloti4, which Clang takes from compiler-rt. libgcc does not provide it, and on
        # aarch64 Clang does not inline the check the way it can on x86-64, so the link needs
        # compiler-rt's builtins. Only that archive is added, rather than switching the whole
        # runtime library with --rtlib=compiler-rt. GCC lowers the check through libgcc and
        # never emits __muloti4, and it rejects the --rtlib= driver flag, so the probe is
        # Clang only.
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
            execute_process(COMMAND ${CMAKE_CXX_COMPILER} --rtlib=compiler-rt
                                    --print-libgcc-file-name
                            OUTPUT_VARIABLE PAIMON_COMPILER_RT_BUILTINS
                            OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
            if(EXISTS "${PAIMON_COMPILER_RT_BUILTINS}")
                target_link_libraries(paimon_sanitizer_flags
                                      INTERFACE "${PAIMON_COMPILER_RT_BUILTINS}")
                message(STATUS "Undefined Behavior Sanitizer builtins: ${PAIMON_COMPILER_RT_BUILTINS}"
                )
            else()
                message(WARNING "compiler-rt builtins not found; a 128-bit multiplication under "
                                "-fsanitize=undefined may fail to link")
            endif()
        endif()
        message(STATUS "Undefined Behavior Sanitizer enabled")
    else()
        message(WARNING "Undefined Behavior Sanitizer is only supported for GCC and Clang compilers"
        )
    endif()
endif()

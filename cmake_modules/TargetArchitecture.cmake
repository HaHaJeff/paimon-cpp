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

# Resolves the target processor and the CPU family that architecture specific
# build logic keys on. Deliberately free of compiler probes and of any state
# beyond its inputs, so that cmake_modules/tests/target_architecture_test.cmake
# can exercise it with `cmake -P`, without a toolchain.
#
# Inputs:  APPLE, CMAKE_SYSTEM_PROCESSOR, CMAKE_OSX_ARCHITECTURES
# Outputs: PAIMON_TARGET_PROCESSOR, PAIMON_TARGET_CPU_FAMILY

# Resolve the CMake-declared target processor. Only on Apple platforms does
# CMAKE_OSX_ARCHITECTURES describe the target -- elsewhere a stale or inherited
# value must not be read. A universal binary names several architectures at once,
# so it gets no architecture specific tuning.
if(APPLE)
    list(LENGTH CMAKE_OSX_ARCHITECTURES _paimon_osx_arch_count)
else()
    set(_paimon_osx_arch_count 0)
endif()
if(_paimon_osx_arch_count EQUAL 1)
    set(PAIMON_TARGET_PROCESSOR "${CMAKE_OSX_ARCHITECTURES}")
elseif(_paimon_osx_arch_count EQUAL 0)
    set(PAIMON_TARGET_PROCESSOR "${CMAKE_SYSTEM_PROCESSOR}")
else()
    set(PAIMON_TARGET_PROCESSOR "")
endif()

# Unrecognized processors are not an error; they just get no tuning flags.
if(PAIMON_TARGET_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    set(PAIMON_TARGET_CPU_FAMILY "x86")
elseif(PAIMON_TARGET_PROCESSOR MATCHES "^(aarch64|arm64|ARM64)$")
    set(PAIMON_TARGET_CPU_FAMILY "aarch64")
else()
    set(PAIMON_TARGET_CPU_FAMILY "unknown")
endif()

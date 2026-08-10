#!/usr/bin/env bash
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
#
# Argument-handling tests for build_and_package.sh. They only exercise
# --print-name, which resolves the package name and exits before any build step,
# so no toolchain is needed.

# No `set -e`: every check runs the script under test and inspects its exit
# status, so a non-zero status is data here, not a reason to abort.
set -uo pipefail

source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
script="${source_dir}/build_and_package.sh"

checks=0
failures=0

# A private directory: a fixed /tmp path would collide with a concurrent run and
# could make the script delete a file it did not create.
work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT
sentinel="${work_dir}/must-not-exist"

# --print-name is passed FIRST, never last: the arguments under test have to keep
# their own position, otherwise a trailing --print-name would become the value of
# a preceding --platform and the argument-exhaustion branch could never be
# reached. Parsing is order independent, so leading it changes nothing else.
expect_name() {
    local description=$1 expected=$2
    shift 2
    local actual
    checks=$((checks + 1))
    if ! actual=$("${script}" --print-name "$@" 2>&1); then
        failures=$((failures + 1))
        echo "FAIL ${description}: exited non-zero: ${actual}"
    elif [[ "${actual}" != "${expected}" ]]; then
        failures=$((failures + 1))
        echo "FAIL ${description}: expected '${expected}', got '${actual}'"
    fi
}

# Asserting the diagnostic, not just a non-zero exit: the script runs under
# `set -u`, so a missing validation branch would still abort -- with an "unbound
# variable" crash instead of a usable message.
expect_rejected() {
    local description=$1
    shift
    local output status
    checks=$((checks + 1))
    output=$("${script}" --print-name "$@" 2>&1)
    status=$?
    if [[ "${status}" -eq 0 ]]; then
        failures=$((failures + 1))
        echo "FAIL ${description}: expected a non-zero exit, but the value was accepted"
    elif [[ "${output}" != *"--platform requires a label"* ]]; then
        failures=$((failures + 1))
        echo "FAIL ${description}: expected the --platform diagnostic, got: ${output}"
    fi
}

# The default label is derived from `uname`, so stub it on PATH to check the
# derivation for hosts this machine is not, including the Darwin -> macos mapping.
expect_host_default() {
    local description=$1 uname_s=$2 uname_m=$3 expected=$4
    shift 4
    local stub_dir actual status
    checks=$((checks + 1))
    stub_dir=$(mktemp -d "${work_dir}/stub.XXXXXX")
    cat > "${stub_dir}/uname" <<EOF
#!/bin/sh
case "\$1" in
    -s) echo "${uname_s}" ;;
    -m) echo "${uname_m}" ;;
    *) echo "unexpected uname argument: \$1" >&2; exit 1 ;;
esac
EOF
    chmod +x "${stub_dir}/uname"
    actual=$(PATH="${stub_dir}:${PATH}" "${script}" --print-name "$@" 2>&1)
    status=$?
    if [[ "${status}" -ne 0 ]]; then
        failures=$((failures + 1))
        echo "FAIL ${description}: exited ${status}: ${actual}"
    elif [[ "${actual}" != "${expected}" ]]; then
        failures=$((failures + 1))
        echo "FAIL ${description}: expected '${expected}', got '${actual}'"
    fi
}

host_platform="$(uname -s | tr '[:upper:]' '[:lower:]')"
if [[ "${host_platform}" == "darwin" ]]; then
    host_platform="macos"
fi
host_platform="${host_platform}-$(uname -m)"

expect_name "release defaults to the host platform" "paimon-cpp-${host_platform}"
expect_name "debug keeps its own prefix" "paimon-cpp-debug-${host_platform}" --debug
expect_name "release is the default build type" "paimon-cpp-${host_platform}" --release
expect_name "--platform overrides the host" "paimon-cpp-linux-aarch64" --platform linux-aarch64
expect_name "--platform applies to debug too" "paimon-cpp-debug-macos-arm64" -d --platform \
    macos-arm64
expect_name "the last --platform wins" "paimon-cpp-linux-aarch64" --platform linux-x86_64 \
    --platform linux-aarch64

# The documented pattern admits dots, underscores and hyphens; keep that a contract.
expect_name "a dotted label is accepted" "paimon-cpp-linux.arm64" --platform linux.arm64
expect_name "an underscored label is accepted" "paimon-cpp-linux_musl-aarch64" --platform \
    linux_musl-aarch64

# The equals form goes through the same validation as the two-argument form.
expect_name "the equals form is accepted" "paimon-cpp-linux-aarch64" --platform=linux-aarch64
expect_rejected "empty equals form" --platform=
expect_rejected "equals form with a nested path" --platform=linux/x86_64

# Default labels for hosts other than this one.
expect_host_default "linux aarch64 host" Linux aarch64 "paimon-cpp-linux-aarch64"
expect_host_default "darwin arm64 host maps to macos" Darwin arm64 "paimon-cpp-macos-arm64"
expect_host_default "darwin x86_64 host maps to macos" Darwin x86_64 "paimon-cpp-macos-x86_64"
expect_host_default "debug on a linux aarch64 host" Linux aarch64 \
    "paimon-cpp-debug-linux-aarch64" --debug
expect_host_default "--platform still wins over the host" Linux aarch64 \
    "paimon-cpp-linux-x86_64" --platform linux-x86_64

# --platform as the final argument must be reported as a missing value rather than
# consuming whatever follows.
expect_rejected "--platform without a value" --platform

# Nothing that turns the package name into a path, or that a shell would treat as
# anything but a literal, may be accepted.
expect_rejected "--platform followed by an option" --platform --debug
expect_rejected "bare parent directory" --platform ".."
expect_rejected "parent directory traversal" --platform "../evil"
expect_rejected "absolute path" --platform "/etc/passwd"
expect_rejected "nested path" --platform "linux/x86_64"
expect_rejected "leading dash" --platform "-linux"
expect_rejected "leading dot" --platform ".linux"
expect_rejected "empty label" --platform ""
expect_rejected "command separator" --platform "linux;touch ${sentinel}"
expect_rejected "command substitution" --platform 'linux$(touch '"${sentinel}"')'
expect_rejected "whitespace" --platform "linux x86_64"

if [[ -e "${sentinel}" ]]; then
    failures=$((failures + 1))
    echo "FAIL a rejected label was still evaluated by a shell"
fi

if [[ "${failures}" -gt 0 ]]; then
    echo "${failures} of ${checks} packaging argument checks failed"
    exit 1
fi
echo "All ${checks} packaging argument checks passed"

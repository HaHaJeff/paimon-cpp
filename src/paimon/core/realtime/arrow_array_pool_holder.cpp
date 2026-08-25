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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/realtime/arrow_array_pool_holder.h"

#include <memory>
#include <new>

#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "arrow/memory_pool.h"

namespace paimon {
namespace {

struct ArrowArrayPrivateData {
    void (*release)(ArrowArray*);
    void* private_data;
    std::shared_ptr<arrow::MemoryPool> arrow_pool;
};

void ReleaseArrowArray(ArrowArray* array) {
    std::unique_ptr<ArrowArrayPrivateData> data(
        static_cast<ArrowArrayPrivateData*>(array->private_data));
    array->release = data->release;
    array->private_data = data->private_data;
    array->release(array);
}

}  // namespace

Status RetainArrowArrayMemoryPool(ArrowArray* array,
                                  const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (!array || !array->release) {
        return Status::Invalid("cannot retain Arrow array memory pool");
    }
    if (!arrow_pool) {
        ArrowArrayRelease(array);
        return Status::Invalid("cannot retain Arrow array memory pool");
    }
    std::unique_ptr<ArrowArrayPrivateData> data;
    try {
        data = std::make_unique<ArrowArrayPrivateData>(
            ArrowArrayPrivateData{array->release, array->private_data, arrow_pool});
    } catch (const std::bad_alloc&) {
        ArrowArrayRelease(array);
        return Status::OutOfMemory("failed to retain Arrow array memory pool");
    }
    array->private_data = data.release();
    array->release = ReleaseArrowArray;
    return Status::OK();
}

}  // namespace paimon

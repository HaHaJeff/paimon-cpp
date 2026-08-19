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

#include <cassert>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "paimon/core/disk/file_io_channel.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class SpillChannelManager : public std::enable_shared_from_this<SpillChannelManager> {
 private:
    struct ChannelEntry {
        int64_t pin_count = 0;
        int64_t file_size = 0;
        bool file_size_set = false;
        bool file_size_unknown = false;
        bool retired = false;
    };

    struct DeleteTask {
        FileIOChannel::ID channel_id;
        int64_t file_size = 0;
        bool file_size_unknown = false;
    };

 public:
    class Lease {
     public:
        ~Lease() {
            manager_->Release(channels_);
        }

     private:
        friend class SpillChannelManager;
        Lease(const std::shared_ptr<SpillChannelManager>& manager,
              std::vector<FileIOChannel::ID>&& channels)
            : manager_(manager), channels_(std::move(channels)) {}

        std::shared_ptr<SpillChannelManager> manager_;
        std::vector<FileIOChannel::ID> channels_;
    };

    SpillChannelManager(const std::shared_ptr<FileSystem>& fs, size_t initial_capacity) : fs_(fs) {
        channels_.reserve(initial_capacity);
    }

    SpillChannelManager(const std::shared_ptr<FileSystem>& fs,
                        const std::shared_ptr<IOManager>& io_manager, size_t initial_capacity)
        : fs_(fs), io_manager_(io_manager) {
        channels_.reserve(initial_capacity);
    }

    void AddChannel(const FileIOChannel::ID& channel_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        channels_.emplace(channel_id, ChannelEntry{});
    }

    Status SetChannelFileSize(const FileIOChannel::ID& channel_id, int64_t file_size) {
        if (file_size < 0) {
            return Status::Invalid("spill channel file size must not be negative");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        auto iter = channels_.find(channel_id);
        if (iter == channels_.end()) {
            return Status::Invalid("spill channel is not registered");
        }
        if (iter->second.file_size_set || iter->second.file_size_unknown) {
            return Status::Invalid("spill channel file size is already set");
        }
        if (file_size > std::numeric_limits<int64_t>::max() - total_spill_disk_bytes_) {
            return Status::Invalid("total spill disk bytes exceed INT64_MAX");
        }
        iter->second.file_size = file_size;
        iter->second.file_size_set = true;
        total_spill_disk_bytes_ += file_size;
        return Status::OK();
    }

    int64_t GetTotalSpillDiskBytes() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return unknown_spill_file_count_ == 0 ? total_spill_disk_bytes_
                                              : std::numeric_limits<int64_t>::max();
    }

    Status DeleteChannel(const FileIOChannel::ID& channel_id) {
        std::optional<DeleteTask> delete_task;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto iter = channels_.find(channel_id);
            if (iter == channels_.end()) {
                return Status::OK();
            }
            EnsureFileSizeAccountedLocked(channel_id, &iter->second);
            if (iter->second.pin_count > 0) {
                iter->second.retired = true;
                return Status::OK();
            }
            delete_task = CreateDeleteTask(channel_id, iter->second);
            channels_.erase(iter);
        }
        Status status = fs_->Delete(delete_task->channel_id.GetPath());
        if (status.ok()) {
            ReleaseDiskUsage(delete_task.value());
        }
        return status;
    }

    Result<std::shared_ptr<Lease>> PinChannels(const std::vector<FileIOChannel::ID>& channel_ids) {
        std::shared_ptr<SpillChannelManager> manager = weak_from_this().lock();
        if (!manager) {
            return Status::Invalid("spill channel manager requires shared ownership");
        }
        std::unordered_set<FileIOChannel::ID, FileIOChannel::ID::Hash> unique;
        unique.reserve(channel_ids.size());
        std::lock_guard<std::mutex> lock(mutex_);
        for (const FileIOChannel::ID& channel_id : channel_ids) {
            if (!unique.insert(channel_id).second) {
                return Status::Invalid("duplicate spill channel");
            }
            auto iter = channels_.find(channel_id);
            if (iter == channels_.end() || iter->second.retired) {
                return Status::Invalid("spill channel is not active");
            }
        }
        for (const FileIOChannel::ID& channel_id : channel_ids) {
            ++channels_.find(channel_id)->second.pin_count;
        }
        return std::shared_ptr<Lease>(
            new Lease(manager, std::vector<FileIOChannel::ID>(channel_ids)));
    }

    void Reset() {
        std::vector<DeleteTask> delete_tasks;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto iter = channels_.begin(); iter != channels_.end();) {
                EnsureFileSizeAccountedLocked(iter->first, &iter->second);
                if (iter->second.pin_count > 0) {
                    iter->second.retired = true;
                    ++iter;
                } else {
                    delete_tasks.push_back(CreateDeleteTask(iter->first, iter->second));
                    iter = channels_.erase(iter);
                }
            }
        }
        for (const DeleteTask& task : delete_tasks) {
            Status status = fs_->Delete(task.channel_id.GetPath());
            if (status.ok()) {
                ReleaseDiskUsage(task);
            }
        }
    }

    std::unordered_set<FileIOChannel::ID, FileIOChannel::ID::Hash> GetChannels() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::unordered_set<FileIOChannel::ID, FileIOChannel::ID::Hash> result;
        result.reserve(channels_.size());
        for (const auto& [channel_id, entry] : channels_) {
            result.insert(channel_id);
        }
        return result;
    }

 private:
    void Release(const std::vector<FileIOChannel::ID>& channel_ids) {
        std::vector<DeleteTask> delete_tasks;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const FileIOChannel::ID& channel_id : channel_ids) {
                auto iter = channels_.find(channel_id);
                if (iter == channels_.end()) {
                    continue;
                }
                --iter->second.pin_count;
                if (iter->second.pin_count == 0 && iter->second.retired) {
                    delete_tasks.push_back(CreateDeleteTask(iter->first, iter->second));
                    channels_.erase(iter);
                }
            }
        }
        for (const DeleteTask& task : delete_tasks) {
            Status status = fs_->Delete(task.channel_id.GetPath());
            if (status.ok()) {
                ReleaseDiskUsage(task);
            }
        }
    }

    void EnsureFileSizeAccountedLocked(const FileIOChannel::ID& channel_id, ChannelEntry* entry) {
        if (entry->file_size_set || entry->file_size_unknown) {
            return;
        }
        Result<std::unique_ptr<FileStatus>> file_status = fs_->GetFileStatus(channel_id.GetPath());
        if (!file_status.ok()) {
            entry->file_size_unknown = true;
            ++unknown_spill_file_count_;
            return;
        }
        const int64_t file_size = file_status.value()->GetLen();
        if (file_size < 0 ||
            file_size > std::numeric_limits<int64_t>::max() - total_spill_disk_bytes_) {
            entry->file_size_unknown = true;
            ++unknown_spill_file_count_;
            return;
        }
        entry->file_size = file_size;
        entry->file_size_set = true;
        total_spill_disk_bytes_ += file_size;
    }

    static DeleteTask CreateDeleteTask(const FileIOChannel::ID& channel_id,
                                       const ChannelEntry& entry) {
        return DeleteTask{channel_id, entry.file_size, entry.file_size_unknown};
    }

    void ReleaseDiskUsage(const DeleteTask& task) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (task.file_size_unknown) {
            assert(unknown_spill_file_count_ > 0);
            --unknown_spill_file_count_;
            return;
        }
        assert(total_spill_disk_bytes_ >= task.file_size);
        total_spill_disk_bytes_ -= task.file_size;
    }

    mutable std::mutex mutex_;
    std::unordered_map<FileIOChannel::ID, ChannelEntry, FileIOChannel::ID::Hash> channels_;
    int64_t total_spill_disk_bytes_ = 0;
    int64_t unknown_spill_file_count_ = 0;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<IOManager> io_manager_;
};

}  // namespace paimon

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

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "paimon/core/disk/file_io_channel.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class SpillChannelManager : public std::enable_shared_from_this<SpillChannelManager> {
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

    Status DeleteChannel(const FileIOChannel::ID& channel_id) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto iter = channels_.find(channel_id);
            if (iter == channels_.end()) {
                return Status::OK();
            }
            if (iter->second.pin_count > 0) {
                iter->second.retired = true;
                return Status::OK();
            }
            channels_.erase(iter);
        }
        return fs_->Delete(channel_id.GetPath());
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
        std::vector<FileIOChannel::ID> delete_tasks;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto iter = channels_.begin(); iter != channels_.end();) {
                if (iter->second.pin_count > 0) {
                    iter->second.retired = true;
                    ++iter;
                } else {
                    delete_tasks.push_back(iter->first);
                    iter = channels_.erase(iter);
                }
            }
        }
        for (const FileIOChannel::ID& channel_id : delete_tasks) {
            [[maybe_unused]] Status status = fs_->Delete(channel_id.GetPath());
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
    struct ChannelEntry {
        int64_t pin_count = 0;
        bool retired = false;
    };

    void Release(const std::vector<FileIOChannel::ID>& channel_ids) {
        std::vector<FileIOChannel::ID> delete_tasks;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (const FileIOChannel::ID& channel_id : channel_ids) {
                auto iter = channels_.find(channel_id);
                if (iter == channels_.end()) {
                    continue;
                }
                --iter->second.pin_count;
                if (iter->second.pin_count == 0 && iter->second.retired) {
                    delete_tasks.push_back(iter->first);
                    channels_.erase(iter);
                }
            }
        }
        for (const FileIOChannel::ID& channel_id : delete_tasks) {
            [[maybe_unused]] Status status = fs_->Delete(channel_id.GetPath());
        }
    }

    mutable std::mutex mutex_;
    std::unordered_map<FileIOChannel::ID, ChannelEntry, FileIOChannel::ID::Hash> channels_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<IOManager> io_manager_;
};

}  // namespace paimon

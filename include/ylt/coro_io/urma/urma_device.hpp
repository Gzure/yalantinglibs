/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstring>
#include <algorithm>

#include "ylt/easylog.hpp"
#include "ylt/coro_io/urma/urma_buffer.hpp"

#ifdef YLT_ENABLE_URMA
#include "ylt/urma/urma_api.h"
#endif

namespace coro_io {

class urma_buffer_pool_t;

// URMA Device abstraction - wrapper around URMA library's ::urma_device_t
// Note: Using ::urma_device_t to explicitly refer to the URMA library type
class urma_device_wrapper_t {
 public:
  urma_device_wrapper_t();
  ~urma_device_wrapper_t();

  urma_device_wrapper_t(const urma_device_wrapper_t&) = delete;
  urma_device_wrapper_t& operator=(const urma_device_wrapper_t&) = delete;

  bool init(const std::string& device_name, int eid_index = 0);
  bool configure_buffer_pool(std::size_t buffer_size,
                             std::size_t max_memory_usage);
  void close();

  urma_context_t* context() const { return context_; }
  urma_device_t* device() const { return device_ptr_; }
  const std::string& name() const { return name_; }
  int eid_index() const { return eid_index_; }
  const urma_eid_t& eid() const { return eid_; }
  const urma_device_attr_t& attr() const { return device_attr_; }
  uint32_t uasid() const { return context_ ? context_->uasid : 0; }
  uint32_t max_jetty() const { return device_attr_.dev_cap.max_jetty; }
  uint32_t max_jfc() const { return device_attr_.dev_cap.max_jfc; }
  bool supports_rm_rtp() const {
    return device_attr_.dev_cap.rm_tp_cap.bs.rtp != 0;
  }
  bool supports_rm_ctp() const {
    return device_attr_.dev_cap.rm_tp_cap.bs.ctp != 0;
  }

  std::string eid_string() const;
  asio::ip::address gid_address() const;
  bool is_valid() const { return context_ != nullptr && device_ptr_ != nullptr; }
  std::shared_ptr<urma_buffer_pool_t> get_buffer_pool() const { return buffer_pool_; }

 private:
  std::string name_;
  int eid_index_ = -1;
  ::urma_device_t* device_ptr_ = nullptr;
  urma_context_t* context_ = nullptr;
  urma_eid_t eid_{};
  urma_device_attr_t device_attr_{};
  std::shared_ptr<urma_buffer_pool_t> buffer_pool_;
};

// Backward compatibility alias
using urma_device_t = urma_device_wrapper_t;

// Global device management
class urma_device_manager {
 public:
  static urma_device_manager& instance();
  bool init();
  std::shared_ptr<urma_device_wrapper_t> get_device(const std::string& device_name = "", int eid_index = 0);
  std::vector<std::shared_ptr<urma_device_wrapper_t>> get_all_devices();
  std::shared_ptr<urma_device_wrapper_t> get_global_device();

 private:
  urma_device_manager() = default;
  ~urma_device_manager();
  bool initialized_ = false;
  std::vector<std::shared_ptr<urma_device_wrapper_t>> devices_;
  std::shared_ptr<urma_device_wrapper_t> global_device_;
};

// URMA buffer pool configuration
struct urma_buffer_pool_config_t {
  uint32_t buffer_size = 4 * 1024;             // buffer size
  uint64_t max_memory_usage = 20 * 1024 * 1024;  // max memory usage
  std::chrono::seconds idle_timeout{5};        // idle timeout
};

// URMA initialization configuration
struct urma_init_config_t {
  std::string dev_name;                        // device name, empty for auto-select
  urma_buffer_pool_config_t buffer_pool_config;  // buffer pool config
  int eid_index = 0;                           // EID index to use
};

inline std::shared_ptr<urma_device_wrapper_t> get_global_urma_device() {
  ELOG_DEBUG << "get_global_urma_device() called";
  return urma_device_manager::instance().get_global_device();
}

inline std::shared_ptr<urma_device_wrapper_t> get_global_urma_device(
    const urma_init_config_t& config) {
  ELOG_DEBUG << "get_global_urma_device(dev_name=" << config.dev_name << ", eid_index=" << config.eid_index << ") called";
  auto device =
      urma_device_manager::instance().get_device(config.dev_name,
                                                 config.eid_index);
  if (device) {
    device->configure_buffer_pool(config.buffer_pool_config.buffer_size,
                                  config.buffer_pool_config.max_memory_usage);
  }
  return device;
}

// ============= Implementation (inline in header) =============

inline urma_device_wrapper_t::urma_device_wrapper_t() = default;

inline urma_device_wrapper_t::~urma_device_wrapper_t() { close(); }

inline bool urma_device_wrapper_t::configure_buffer_pool(
    std::size_t buffer_size, std::size_t max_memory_usage) {
  if (!context_ || buffer_size == 0) return false;
  auto requested_buffer_count =
      std::max<std::size_t>(1, max_memory_usage / buffer_size);
  if (buffer_pool_) {
    if (buffer_pool_->buffer_size() == buffer_size &&
        buffer_pool_->total_buffer_count() >= requested_buffer_count) {
      return true;
    }
    if (buffer_pool_->free_buffer_count() !=
        buffer_pool_->total_buffer_count()) {
      ELOG_WARN << "cannot resize an in-use URMA buffer pool: current_size="
                << buffer_pool_->buffer_size()
                << ", current_count=" << buffer_pool_->total_buffer_count()
                << ", requested_size=" << buffer_size
                << ", requested_count=" << requested_buffer_count;
      return false;
    }
  }
  buffer_pool_ =
      std::make_shared<urma_buffer_pool_t>(context_, buffer_size,
                                           requested_buffer_count);
  return buffer_pool_->total_buffer_count() != 0;
}

inline bool urma_device_wrapper_t::init(const std::string& device_name, int eid_index) {
#ifdef YLT_ENABLE_URMA
  name_ = device_name;
  eid_index_ = eid_index;

  int num_devices = 0;
  // Use :: to explicitly call URMA library function
  ::urma_device_t** devices = urma_get_device_list(&num_devices);
  if (!devices || num_devices <= 0) {
    ELOG_ERROR << "urma_get_device_list failed";
    return false;
  }

  ::urma_device_t* found_device = nullptr;
  for (int i = 0; i < num_devices; ++i) {
    if (device_name.empty() || std::string(devices[i]->name) == device_name) {
      found_device = devices[i];
      break;
    }
  }

  if (!found_device) {
    ELOG_ERROR << "URMA device not found: " << device_name;
    urma_free_device_list(devices);
    return false;
  }

  device_ptr_ = found_device;
  name_ = found_device->name;

  uint32_t eid_cnt = 0;
  urma_eid_info_t* eid_list = urma_get_eid_list(device_ptr_, &eid_cnt);
  if (!eid_list || eid_cnt == 0) {
    ELOG_ERROR << "urma_get_eid_list failed";
    urma_free_device_list(devices);
    return false;
  }

  int eid_slot = 0;
  bool found_eid_index = false;
  for (uint32_t i = 0; i < eid_cnt; ++i) {
    if (eid_list[i].eid_index == static_cast<uint32_t>(eid_index)) {
      eid_slot = static_cast<int>(i);
      found_eid_index = true;
      break;
    }
  }
  if (!found_eid_index) {
    ELOG_WARN << "URMA EID index " << eid_index
              << " was not found; using index " << eid_list[0].eid_index;
  }
  eid_ = eid_list[eid_slot].eid;
  eid_index_ = static_cast<int>(eid_list[eid_slot].eid_index);
  urma_free_eid_list(eid_list);

  context_ = urma_create_context(device_ptr_, eid_index_);
  if (!context_) {
    ELOG_ERROR << "urma_create_context failed";
    urma_free_device_list(devices);
    return false;
  }

  if (urma_query_device(device_ptr_, &device_attr_) != 0) {
    ELOG_ERROR << "urma_query_device failed";
    urma_delete_context(context_);
    context_ = nullptr;
    urma_free_device_list(devices);
    return false;
  }

  urma_free_device_list(devices);

  auto default_pool_config = urma_buffer_pool_config_t{};
  if (!configure_buffer_pool(default_pool_config.buffer_size,
                             default_pool_config.max_memory_usage)) {
    close();
    return false;
  }
  ELOG_INFO << "URMA device: " << name_ << ", EID: " << eid_string();
  return true;
#else
  ELOG_WARN << "URMA not enabled";
  return false;
#endif
}

inline void urma_device_wrapper_t::close() {
#ifdef YLT_ENABLE_URMA
  if (context_) {
    buffer_pool_.reset();
    urma_delete_context(context_);
    context_ = nullptr;
  }
#endif
}

inline std::string urma_device_wrapper_t::eid_string() const {
  char buf[64] = {0};
  snprintf(buf, sizeof(buf),
          "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:"
          "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
          eid_.raw[0], eid_.raw[1], eid_.raw[2], eid_.raw[3],
          eid_.raw[4], eid_.raw[5], eid_.raw[6], eid_.raw[7],
          eid_.raw[8], eid_.raw[9], eid_.raw[10], eid_.raw[11],
          eid_.raw[12], eid_.raw[13], eid_.raw[14], eid_.raw[15]);
  return std::string(buf);
}

inline asio::ip::address urma_device_wrapper_t::gid_address() const {
  char buf[64];
  snprintf(buf, sizeof(buf), "%d.%d.%d.%d",
          eid_.raw[0], eid_.raw[1], eid_.raw[2], eid_.raw[3]);
  std::error_code ec;
  auto addr = asio::ip::make_address(buf, ec);
  if (ec) return asio::ip::make_address_v4(0x7F000001);
  return addr;
}

// urma_device_manager
inline urma_device_manager& urma_device_manager::instance() {
  static urma_device_manager inst;
  return inst;
}

inline urma_device_manager::~urma_device_manager() {
  devices_.clear();
  global_device_.reset();
}

inline bool urma_device_manager::init() {
#ifdef YLT_ENABLE_URMA
  if (initialized_) return true;
  urma_init_attr_t init_attr = {};
  auto status = urma_init(&init_attr);
  if (status != URMA_SUCCESS && status != URMA_EEXIST) {
    ELOG_ERROR << "urma_init failed";
    return false;
  }
  initialized_ = true;
  return true;
#else
  return false;
#endif
}

inline std::shared_ptr<urma_device_wrapper_t> urma_device_manager::get_device(
    const std::string& device_name, int eid_index) {
#ifdef YLT_ENABLE_URMA
  if (!initialized_ && !init()) return nullptr;

  for (auto& dev : devices_) {
    if ((device_name.empty() || dev->name() == device_name) &&
        dev->eid_index() == eid_index) {
      return dev;
    }
  }

  auto dev = std::make_shared<urma_device_wrapper_t>();
  if (!dev->init(device_name, eid_index)) return nullptr;

  devices_.push_back(dev);
  if (!global_device_) global_device_ = dev;
  return dev;
#else
  return nullptr;
#endif
}

inline std::vector<std::shared_ptr<urma_device_wrapper_t>>
urma_device_manager::get_all_devices() {
#ifdef YLT_ENABLE_URMA
  if (!devices_.empty()) return devices_;
  if (!initialized_) init();

  int num_devices = 0;
  ::urma_device_t** urma_dev_list = urma_get_device_list(&num_devices);
  if (!urma_dev_list || num_devices <= 0) return devices_;

  for (int i = 0; i < num_devices; ++i) {
    auto dev = std::make_shared<urma_device_wrapper_t>();
    if (dev->init(urma_dev_list[i]->name)) {
      devices_.push_back(dev);
      if (!global_device_) global_device_ = dev;
    }
  }
  urma_free_device_list(urma_dev_list);
  return devices_;
#else
  return {};
#endif
}

inline std::shared_ptr<urma_device_wrapper_t> urma_device_manager::get_global_device() {
  if (!global_device_) get_all_devices();
  return global_device_;
}

}  // namespace coro_io

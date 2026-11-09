/*
 * Copyright (c) 2026, Alibaba Group Holding Limited;
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

#ifdef YLT_ENABLE_URMA
#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>

#include "ylt/coro_io/urma/urma_socket.hpp"
#include "ylt/easylog.hpp"

namespace coro_io::detail {

inline std::string urma_rpc_lower_ascii(std::string_view value) {
  std::string result(value);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) {
                   return static_cast<char>(std::tolower(c));
                 });
  return result;
}

inline const char* urma_rpc_getenv(const char* name) {
  return std::getenv(name);
}

inline bool urma_rpc_env_enabled() {
  const char* value = urma_rpc_getenv("URMA_RPC_ENABLE");
  if (value == nullptr || *value == '\0')
    return false;

  auto normalized = urma_rpc_lower_ascii(value);
  return normalized == "1" || normalized == "on" || normalized == "true" ||
         normalized == "yes";
}

inline bool urma_rpc_env_flag(const char* name, bool default_value) {
  const char* value = urma_rpc_getenv(name);
  if (value == nullptr || *value == '\0')
    return default_value;
  auto normalized = urma_rpc_lower_ascii(value);
  if (normalized == "1" || normalized == "on" || normalized == "true" ||
      normalized == "yes")
    return true;
  if (normalized == "0" || normalized == "off" || normalized == "false" ||
      normalized == "no")
    return false;
  ELOG_WARN << "invalid " << name << " value: " << value << "; use default "
            << (default_value ? "true" : "false");
  return default_value;
}

template <typename T>
inline bool urma_rpc_parse_integer(std::string_view value, T& output) {
  static_assert(std::is_integral_v<T>);
  T parsed{};
  const auto* first = value.data();
  const auto* last = value.data() + value.size();
  auto [ptr, ec] = std::from_chars(first, last, parsed);
  if (ec != std::errc{} || ptr != last)
    return false;
  output = parsed;
  return true;
}

template <typename T>
inline void urma_rpc_parse_env_integer(const char* name, T& field) {
  const char* value = urma_rpc_getenv(name);
  if (value == nullptr || *value == '\0')
    return;

  T parsed{};
  if (!urma_rpc_parse_integer(std::string_view(value), parsed)) {
    ELOG_WARN << "invalid " << name << " value: " << value << "; use default "
              << field;
    return;
  }
  field = parsed;
}

inline void urma_rpc_parse_env_tp_type(urma_tp_type_t& tp_type) {
  const char* value = urma_rpc_getenv("URMA_RPC_TP_TYPE");
  if (value == nullptr || *value == '\0')
    return;

  auto normalized = urma_rpc_lower_ascii(value);
  if (normalized == "ctp" || normalized == "0") {
    tp_type = URMA_CTP;
    return;
  }
  if (normalized == "rtp" || normalized == "1") {
    tp_type = URMA_RTP;
    return;
  }

  ELOG_WARN << "invalid URMA_RPC_TP_TYPE value: " << value << "; use default "
            << static_cast<int>(tp_type);
}

inline coro_io::urma_socket_t::config_t make_urma_rpc_config_from_env() {
  coro_io::urma_socket_t::config_t config{};

  if (const char* device = urma_rpc_getenv("URMA_RPC_DEVICE");
      device != nullptr) {
    config.device_name = device;
  }

  urma_rpc_parse_env_integer("URMA_RPC_EID_INDEX", config.eid_index);
  urma_rpc_parse_env_integer("URMA_RPC_CQ_SIZE", config.cq_size);
  urma_rpc_parse_env_integer("URMA_RPC_RECV_BUFFER_CNT",
                             config.recv_buffer_cnt);
  urma_rpc_parse_env_integer("URMA_RPC_SEND_BUFFER_CNT",
                             config.send_buffer_cnt);
  urma_rpc_parse_env_integer("URMA_RPC_BUFFER_SIZE", config.buffer_size);
  urma_rpc_parse_env_integer("URMA_RPC_MAX_MEMORY_USAGE",
                             config.max_memory_usage);
  urma_rpc_parse_env_tp_type(config.tp_type);
  auto default_priority = config.jfs_priority;
  urma_rpc_parse_env_integer("URMA_RPC_PRIORITY", config.jfs_priority);
  if (config.jfs_priority > 15) {
    ELOG_WARN << "invalid URMA_RPC_PRIORITY value; use default "
              << static_cast<unsigned>(default_priority);
    config.jfs_priority = default_priority;
  }
  config.event_mode =
      urma_rpc_env_flag("URMA_RPC_EVENT_MODE", /*default*/ true);
  urma_rpc_parse_env_integer("URMA_RPC_BUSY_POLL_BUDGET",
                             config.busy_poll_budget);
  urma_rpc_parse_env_integer("URMA_RPC_PENDING_POLL_BUDGET",
                             config.pending_poll_budget);
  urma_rpc_parse_env_integer("URMA_RPC_JFC_MODERATE_COUNT",
                             config.jfc_moderate_count);
  urma_rpc_parse_env_integer("URMA_RPC_JFC_MODERATE_PERIOD",
                             config.jfc_moderate_period);
  constexpr std::size_t max_pending_poll_budget = 4096;
  if (config.pending_poll_budget > max_pending_poll_budget) {
    ELOG_WARN << "URMA_RPC_PENDING_POLL_BUDGET=" << config.pending_poll_budget
              << " is too large; clamped to " << max_pending_poll_budget
              << " (the reactor thread would otherwise spin for too long)";
    config.pending_poll_budget = max_pending_poll_budget;
  }
  if (config.jfc_moderate_count <= 0 || config.jfc_moderate_count > 0xFFFF) {
    ELOG_WARN << "invalid URMA_RPC_JFC_MODERATE_COUNT value: "
              << config.jfc_moderate_count
              << "; CQ event moderation count kept as driver default";
    config.jfc_moderate_count = -1;
  }
  if (config.jfc_moderate_period > 0xFFFF) {
    ELOG_WARN << "invalid URMA_RPC_JFC_MODERATE_PERIOD value: "
              << config.jfc_moderate_period
              << "; CQ event moderation period kept as driver default";
    config.jfc_moderate_period = -1;
  }
  {
    uint64_t interval_us = 5;
    urma_rpc_parse_env_integer("URMA_RPC_POLL_INTERVAL", interval_us);
    config.poll_interval = std::chrono::microseconds(interval_us);
  }
  config.poll_spin = urma_rpc_env_flag("URMA_RPC_POLL_SPIN", /*default*/ true);
  urma_rpc_parse_env_integer("URMA_RPC_IO_RATE", config.io_rate);
  constexpr uint32_t max_io_rate = 1024;
  if (config.io_rate < 1 || config.io_rate > max_io_rate) {
    ELOG_WARN << "invalid URMA_RPC_IO_RATE value: " << config.io_rate
              << "; clamped to the 1.." << max_io_rate << " range";
    config.io_rate = config.io_rate < 1 ? 1 : max_io_rate;
  }
  config.shared_jfc = urma_rpc_env_flag("URMA_RPC_SHARED_JFC",
                                        /*default*/ false);
  urma_rpc_parse_env_integer("URMA_RPC_GROUP_CQ_SIZE", config.group_cq_size);
  constexpr uint32_t min_group_cq_size = 64;
  constexpr uint32_t max_group_cq_size = 65536;
  if (config.group_cq_size < min_group_cq_size ||
      config.group_cq_size > max_group_cq_size) {
    ELOG_WARN << "invalid URMA_RPC_GROUP_CQ_SIZE value: "
              << config.group_cq_size << "; clamped to the "
              << min_group_cq_size << ".." << max_group_cq_size << " range";
    config.group_cq_size = config.group_cq_size < min_group_cq_size
                               ? min_group_cq_size
                               : max_group_cq_size;
  }

  return config;
}

inline std::optional<coro_io::urma_socket_t::config_t> probe_urma_rpc_config(
    coro_io::urma_socket_t::config_t config) {
  try {
    auto device = coro_io::get_global_urma_device(
        {.dev_name = config.device_name,
         .buffer_pool_config = {.buffer_size = config.buffer_size,
                                .max_memory_usage = config.max_memory_usage},
         .eid_index = config.eid_index});
    if (!device || !device->is_valid() || !device->get_buffer_pool()) {
      ELOG_WARN << "URMA_RPC_ENABLE is enabled but no usable URMA device was "
                   "found; fall back to TCP";
      return std::nullopt;
    }

    config.device_name = device->name();
    config.eid_index = device->eid_index();
    ELOG_INFO << "URMA RPC auto enable succeeded: device=" << config.device_name
              << ", eid_index=" << config.eid_index
              << ", tp_type=" << static_cast<int>(config.tp_type)
              << ", cq_size=" << config.cq_size
              << ", recv_buffer_cnt=" << config.recv_buffer_cnt
              << ", send_buffer_cnt=" << config.send_buffer_cnt
              << ", buffer_size=" << config.buffer_size
              << ", max_memory_usage=" << config.max_memory_usage
              << ", jfs_priority=" << static_cast<unsigned>(config.jfs_priority)
              << ", event_mode=" << (config.event_mode ? "on" : "off")
              << ", busy_poll_budget=" << config.busy_poll_budget
              << ", pending_poll_budget=" << config.pending_poll_budget
              << ", jfc_moderate_count=" << config.jfc_moderate_count
              << ", jfc_moderate_period=" << config.jfc_moderate_period
              << ", timer_poll=" << (config.poll_spin ? "spin" : "timer")
              << ", io_rate=" << config.io_rate
              << ", shared_jfc=" << (config.shared_jfc ? "on" : "off")
              << ", group_cq_size=" << config.group_cq_size;
    return config;
  } catch (const std::exception& e) {
    ELOG_WARN << "URMA RPC auto enable failed: " << e.what()
              << "; fall back to TCP";
    return std::nullopt;
  }
}

}  // namespace coro_io::detail

namespace coro_io {

inline std::optional<coro_io::urma_socket_t::config_t>
try_make_urma_rpc_config() {
  if (!detail::urma_rpc_env_enabled())
    return std::nullopt;

  static const auto cached_config = []() {
    return detail::probe_urma_rpc_config(
        detail::make_urma_rpc_config_from_env());
  }();
  return cached_config;
}

}  // namespace coro_io
#endif

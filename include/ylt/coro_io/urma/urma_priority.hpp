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

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string_view>

#include "ylt/easylog.hpp"

namespace coro_io::detail {
// Apply at both RPC configuration and socket construction so explicit socket
// configurations cannot bypass a process-level priority override.
inline uint8_t urma_priority_from_env(uint8_t default_priority) {
  const char* value = std::getenv("MC_YLT_URMA_PRIORITY");
  if (!value)
    return default_priority;
  unsigned priority = 0;
  std::string_view input(value);
  auto [end, ec] = std::from_chars(input.data(), input.data() + input.size(),
                                  priority);
  if (ec != std::errc{} || end != input.data() + input.size() || priority > 15) {
    ELOG_ERROR << "invalid MC_YLT_URMA_PRIORITY='" << value
               << "'; expected integer in [0, 15]";
    throw std::invalid_argument("invalid MC_YLT_URMA_PRIORITY: expected [0, 15]");
  }
  return static_cast<uint8_t>(priority);
}
}  // namespace coro_io::detail

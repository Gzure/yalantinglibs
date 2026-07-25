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

#include "ylt/urma/urma_api.h"

namespace coro_io::detail {

// Stateless deleter for URMA handles owned via std::unique_ptr.  Shared by
// urma_socket_shared_state_t (per-socket handles) and urma_jfc_group (shared
// jfc/jfce).  Defined here so both headers can use it without one including
// the other.
struct urma_deleter {
  void operator()(urma_jfc_t* value) const {
    if (value) urma_delete_jfc(value);
  }
  void operator()(urma_jfr_t* value) const {
    if (value) urma_delete_jfr(value);
  }
  void operator()(urma_jetty_t* value) const {
    if (value) urma_delete_jetty(value);
  }
  void operator()(urma_jfce_t* value) const {
    if (value) urma_delete_jfce(value);
  }
  void operator()(urma_target_jetty_t* value) const {
    if (value) urma_unimport_jetty(value);
  }
  void operator()(urma_target_seg_t* value) const {
    if (value) urma_unimport_seg(value);
  }
};

}  // namespace coro_io::detail

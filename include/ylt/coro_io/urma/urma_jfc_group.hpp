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

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "ylt/coro_io/urma/urma_ctx_codec.hpp"
#include "ylt/coro_io/urma/urma_deleter.hpp"
#include "ylt/easylog.hpp"
#include "ylt/urma/urma_api.h"
#include "ylt/urma/urma_types.h"

namespace coro_io::detail {

struct urma_socket_shared_state_t;  // forward

// One jfc group: a shared completion queue (jfc) + event channel (jfce) that
// many jetties (one per socket) feed.  A poll thread drains the jfc and
// dispatches completions back to the owning socket via its socket_id.
//
// Lifecycle: created by urma_poll_thread_pool; outlives any single socket.
// Sockets register/unregister; the poll thread looks up sockets by id.
class urma_jfc_group {
 public:
  urma_jfc_group(urma_context_t* context, uint32_t cq_size)
      : context_(context), cq_size_(cq_size) {}

  ~urma_jfc_group() { release_resources(); }

  // Create the shared jfc + jfce. Returns false on failure (caller falls back
  // to the legacy per-socket path).
  bool init() {
    errno = 0;
    jfce_.reset(urma_create_jfce(context_));
    if (!jfce_) {
      ELOG_WARN << "urma_create_jfce (group) failed: errno=" << errno;
      return false;
    }
    urma_jfc_cfg_t jfc_cfg{};
    jfc_cfg.depth = cq_size_;
    jfc_cfg.jfce = jfce_.get();
    errno = 0;
    jfc_.reset(urma_create_jfc(context_, &jfc_cfg));
    if (!jfc_) {
      ELOG_ERROR << "urma_create_jfc (group) failed: depth=" << cq_size_
                 << ", errno=" << errno;
      return false;
    }
    ELOG_INFO << "urma_jfc_group init: jfc_id=" << jfc_->jfc_id.id
              << ", depth=" << cq_size_ << ", jfce fd=" << jfce_->fd;
    return true;
  }

  urma_jfc_t* jfc() const noexcept { return jfc_.get(); }
  urma_jfce_t* jfce() const noexcept { return jfce_.get(); }

  // Register a socket; returns its per-group unique socket_id. Thread-safe
  // against unregister/lookup.  Stores a shared_ptr so the poll thread can
  // pin the socket's lifetime across dispatch (no UAF if close() races).
  uint32_t register_socket(std::shared_ptr<urma_socket_shared_state_t> s) {
    std::unique_lock lk(registry_mtx_);
    uint32_t id = next_socket_id_++;
    sockets_[id] = std::move(s);
    return id;
  }

  void unregister_socket(uint32_t socket_id) {
    std::unique_lock lk(registry_mtx_);
    sockets_.erase(socket_id);
  }

  // Lookup for the poll thread's dispatch path.  Returns a shared_ptr so the
  // caller pins the socket's lifetime for the duration of the dispatch call -
  // no UAF if close() concurrently unregisters and destroys the socket.
  std::shared_ptr<urma_socket_shared_state_t> lookup_socket(uint32_t socket_id) const {
    std::shared_lock lk(registry_mtx_);
    auto it = sockets_.find(socket_id);
    return it == sockets_.end() ? nullptr : it->second;
  }

  // Visit every registered socket (e.g. to wake them on a group-level error).
  // The callback receives a shared_ptr so the socket is pinned for the call.
  template <typename F>
  void for_each_socket(F&& f) const {
    std::shared_lock lk(registry_mtx_);
    for (auto& [id, s] : sockets_) {
      f(s);
    }
  }

  void mark_errored() { errored_.store(true, std::memory_order_release); }
  bool errored() const { return errored_.load(std::memory_order_acquire); }

 private:
  void release_resources() {
    jfc_.reset();
    jfce_.reset();
  }

  urma_context_t* context_;
  uint32_t cq_size_;
  std::unique_ptr<urma_jfc_t, urma_deleter> jfc_;
  std::unique_ptr<urma_jfce_t, urma_deleter> jfce_;

  mutable std::shared_mutex registry_mtx_;
  std::unordered_map<uint32_t, std::shared_ptr<urma_socket_shared_state_t>> sockets_;
  uint32_t next_socket_id_ = 1;  // 0 reserved for "none"
  std::atomic<bool> errored_{false};
};

}  // namespace coro_io::detail

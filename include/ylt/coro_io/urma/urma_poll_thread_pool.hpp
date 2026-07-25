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

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "ylt/coro_io/urma/urma_jfc_group.hpp"
#include "ylt/easylog.hpp"
#include "ylt/urma/urma_api.h"
#include "ylt/urma/urma_types.h"

namespace coro_io::detail {

class urma_socket_shared_state_t;  // forward

// Process-level singleton owning M poll threads and M jfc groups. Thread i is
// bound to group i.  Sockets are assigned to groups round-robin (or by peer
// hash) and register themselves into the chosen group.
class urma_poll_thread_pool {
 public:
  struct config {
    uint32_t thread_count = 4;
    uint32_t group_cq_size = 1024;
    std::size_t busy_poll_budget = 16;
    std::chrono::milliseconds wait_timeout{100};
    urma_context_t* context = nullptr;
  };

  // Lazily-initialized singleton. Returns nullptr if the pool could not be
  // initialized (caller falls back to the legacy per-socket path).
  static urma_poll_thread_pool* instance(const config& cfg) {
    static std::unique_ptr<urma_poll_thread_pool> pool = make(cfg);
    return pool.get();
  }

  // Pick a group for a new socket (round-robin by an opaque key).
  urma_jfc_group* select_group(uint64_t key) {
    return groups_[key % groups_.size()].get();
  }

  void stop() {
    if (stop_.exchange(true)) return;
    for (auto& t : threads_)
      if (t.joinable()) t.join();
    threads_.clear();
  }

  ~urma_poll_thread_pool() { stop(); }

 private:
  static std::unique_ptr<urma_poll_thread_pool> make(const config& cfg) {
    auto pool = std::unique_ptr<urma_poll_thread_pool>(
        new urma_poll_thread_pool(cfg));
    if (!pool->init()) return nullptr;
    return pool;
  }

  explicit urma_poll_thread_pool(const config& cfg) : cfg_(cfg) {}

  bool init() {
    if (cfg_.thread_count == 0 || !cfg_.context) return false;
    groups_.reserve(cfg_.thread_count);
    for (uint32_t i = 0; i < cfg_.thread_count; ++i) {
      auto g = std::make_unique<urma_jfc_group>(cfg_.context, cfg_.group_cq_size);
      if (!g->init()) {
        ELOG_ERROR << "urma_poll_thread_pool: group " << i << " init failed";
        return false;
      }
      groups_.push_back(std::move(g));
    }
    for (uint32_t i = 0; i < cfg_.thread_count; ++i) {
      threads_.emplace_back([this, i] { poll_loop(i); });
    }
    ELOG_INFO << "urma_poll_thread_pool started: threads=" << cfg_.thread_count;
    return true;
  }

  // Hybrid loop: busy-poll the group's jfc; after busy_poll_budget empty
  // polls, rearm + wait_jfc (bounded timeout).  On wakeup, resume busy-poll.
  // Escalates a *persistent* urma_poll_jfc failure (>= 64 consecutive errors)
  // to a group-level teardown: mark the group errored, wake every registered
  // socket via post_close_on_error(), and exit the poll thread (spec §5).
  //
  // Declared here, defined out-of-line in urma_socket.hpp (alongside dispatch)
  // because the group-level escalation calls s->post_close_on_error() through
  // group->for_each_socket(...), which needs the complete
  // urma_socket_shared_state_t type that is only visible in urma_socket.hpp.
  void poll_loop(uint32_t gi);

  // Decode user_ctx and hand the completion to the owning socket.  Defined
  // out-of-line in urma_socket.hpp (which has the full socket definition) to
  // avoid a circular include here.
  void dispatch(urma_jfc_group* group, const urma_cr_t& cr);

  config cfg_;
  std::atomic<bool> stop_{false};
  std::vector<std::unique_ptr<urma_jfc_group>> groups_;
  std::vector<std::thread> threads_;
};

}  // namespace coro_io::detail

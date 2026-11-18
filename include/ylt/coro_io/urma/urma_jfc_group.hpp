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

#include <urma_api.h>
#include <urma_ubagg.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include "asio/any_io_executor.hpp"
#include "asio/post.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/urma/urma_async_event.hpp"
#include "ylt/coro_io/urma/urma_jfc_registry.hpp"
#include "ylt/easylog.hpp"

namespace coro_io::detail {

// ---------------------------------------------------------------------------
// A completion queue shared by all URMA sockets of one io_context.
//
// This is the "one poll loop per reactor thread" shape: the group owns a single
// JFC, all jetties of this io_context feed it, and one posted tick drains it
// and dispatches completions inline to the owning sockets - all on the same
// thread, so there is no cross-thread handoff (which is what made the earlier
// per-group poll thread pool slow).
//
// Only used in poll mode (event_mode=false); the event-driven path keeps its
// per-socket jfce, which the socket layer falls back to when sharing is not
// possible.
// ---------------------------------------------------------------------------
class urma_jfc_group : public std::enable_shared_from_this<urma_jfc_group> {
 public:
  struct jfc_deleter {
    void operator()(urma_jfc_t* value) const {
      if (value)
        urma_delete_jfc(value);
    }
  };

  // executor and urma_context together identify a group: one io_context may in
  // principle carry sockets from different devices.
  //
  // The group keeps an asio::any_io_executor (not a raw ExecutorWrapper*), so
  // the io_context cannot be destroyed while the group still posts to it; that
  // also makes the execution_context address used as the lookup key stable for
  // as long as the group lives, so a recycled address cannot route a socket to
  // a stale group.
  //
  // async_events (optional) is the device's async event hub: the group
  // subscribes its JFC, so a JFC error event (e.g. an overflow) fails every
  // attached socket instead of leaving them on a JFC that never completes.
  static std::shared_ptr<urma_jfc_group> get(
      coro_io::ExecutorWrapper<>* executor, urma_context_t* context,
      uint32_t cq_size, uint32_t max_cq_depth, uint32_t io_rate,
      std::shared_ptr<urma_async_event_hub> async_events = nullptr) {
    if (!executor || !context)
      return nullptr;
    auto asio_executor = executor->get_asio_executor();
    const auto key =
        std::make_pair(static_cast<void*>(&asio_executor.context()),
                       static_cast<void*>(context));
    {
      // Fast path: an existing live group, without creating anything.
      std::lock_guard<std::mutex> lock(registry_mutex());
      auto& groups = registry();
      prune_expired_locked(groups);
      auto it = groups.find(key);
      if (it != groups.end()) {
        if (auto existing = it->second.lock()) {
          if (!existing->failed_.load(std::memory_order_relaxed))
            return existing;
          it->second.reset();  // failed groups must not be reused
        }
      }
    }

    // Create the JFC outside the process-wide lock: urma_create_jfc is a driver
    // call and used to serialize group creation for every io_context.
    auto group = std::shared_ptr<urma_jfc_group>(
        new urma_jfc_group(std::move(asio_executor), io_rate));
    if (!group->init(context, cq_size, max_cq_depth))
      return nullptr;

    {
      std::lock_guard<std::mutex> lock(registry_mutex());
      auto& groups = registry();
      prune_expired_locked(groups);
      auto it = groups.find(key);
      if (it != groups.end()) {
        if (auto existing = it->second.lock()) {
          if (!existing->failed_.load(std::memory_order_relaxed))
            return existing;  // another thread won: drop our extra JFC
        }
      }
      groups[key] = group;
    }
    group->subscribe_async_events(std::move(async_events));
    return group;
  }

  ~urma_jfc_group() {
    stop();
    // Before jfc_ is deleted, so a recycled address cannot reach this group.
    if (async_events_)
      async_events_->unsubscribe(jfc_.get());
  }

  bool valid() const noexcept { return static_cast<bool>(jfc_); }

  urma_jfc_t* jfc() const noexcept { return jfc_.get(); }
  uint32_t cq_size() const noexcept { return cq_size_; }

  // wr_reserve: work requests this socket may keep in flight (send + recv
  // buffers + slack). Returns sid, or < 0 on failure (group full / CQ too
  // small / group already failed).
  //
  // Everything happens under state_mutex_ so a concurrent fail_all() either
  // runs before this attach (which then returns -3) or after it (and its
  // for_each covers the new socket). There is no window that yields a socket
  // with a sid but no poll loop. state_mutex_ is per group: the process-wide
  // registry_mutex() only guards the group lookup table in get(), so reactor
  // threads of different io_contexts never contend on the poll path.
  int attach(std::shared_ptr<jfc_group_socket> socket, uint32_t wr_reserve) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (failed_.load(std::memory_order_relaxed))
      return -3;
    const int sid = registry_.attach(std::move(socket), wr_reserve);
    if (sid < 0)
      return sid;
    if (failed_.load(std::memory_order_relaxed)) {
      // fail_all() won the race: drop the reservation instead of handing out a
      // socket that would never see completions.
      registry_.detach(sid);
      return -3;
    }
    alive_.store(true, std::memory_order_relaxed);
    if (!polling_.exchange(true, std::memory_order_relaxed))
      schedule_locked();
    return sid;
  }

  void detach(int sid) { registry_.detach(sid); }
  bool bind_jetty(int sid, uint32_t jetty_id) {
    return registry_.bind_jetty(sid, jetty_id);
  }
  // Keeps a draining socket alive until detach(); see jfc_group_registry::pin.
  bool pin(int sid, std::shared_ptr<jfc_group_socket> socket) {
    return registry_.pin(sid, std::move(socket));
  }

  // Starts the single poll tick for this io_context (idempotent).
  void ensure_polling() {
    if (!valid() || failed_.load(std::memory_order_relaxed))
      return;
    alive_.store(true, std::memory_order_relaxed);
    if (!polling_.exchange(true, std::memory_order_relaxed))
      schedule();
  }

  // Stops the tick and marks the group unusable. Safe to call from any thread;
  // in-flight dispatches finish on the io_context thread.
  void stop() {
    alive_.store(false, std::memory_order_relaxed);
    polling_.store(false, std::memory_order_relaxed);
    failed_.store(true, std::memory_order_relaxed);
  }

  bool polling() const noexcept {
    return polling_.load(std::memory_order_relaxed);
  }
  bool failed() const noexcept {
    return failed_.load(std::memory_order_relaxed);
  }
  std::size_t socket_count() const { return registry_.size(); }

 private:
  using key_type = std::pair<void*, void*>;

  urma_jfc_group(asio::any_io_executor executor, uint32_t io_rate)
      : executor_(std::move(executor)),
        io_rate_(std::max<uint32_t>(io_rate, 1)) {}

  bool init(urma_context_t* context, uint32_t cq_size, uint32_t max_cq_depth) {
    if (max_cq_depth != 0 && cq_size > max_cq_depth) {
      ELOG_WARN << "URMA shared JFC: requested depth " << cq_size
                << " exceeds device max_jfc_depth " << max_cq_depth
                << "; clamped. Consider fewer sockets per io_context or "
                   "smaller URMA_RPC_RECV/SEND_BUFFER_CNT";
      cq_size = max_cq_depth;
    }
    urma_jfc_cfg_t cfg{};
    cfg.depth = cq_size;
    // Poll mode only: no event channel is created for the shared JFC.
    cfg.jfce = nullptr;
    errno = 0;
    jfc_.reset(urma_create_jfc(context, &cfg));
    if (!jfc_) {
      ELOG_WARN << "URMA shared JFC creation failed: depth=" << cfg.depth
                << ", errno=" << errno << "; falling back to per-socket JFC";
      return false;
    }
    cq_size_ = cq_size;
    registry_.set_max_wr(cq_size);
    ELOG_INFO << "URMA shared JFC created: depth=" << cq_size
              << ", io_rate=" << io_rate_;
    return true;
  }

  void schedule() {
    auto self = shared_from_this();
    asio::post(executor_, [self] {
      self->poll_once();
    });
  }

  // Caller holds state_mutex_.
  void schedule_locked() {
    if (!alive_.load(std::memory_order_relaxed))
      return;
    auto self = shared_from_this();
    asio::post(executor_, [self] {
      self->poll_once();
    });
  }

  void poll_once() {
    // Any early exit must clear polling_, otherwise a later attach() would skip
    // the restart and the group would keep a sid with no poll loop.
    if (!alive_.load(std::memory_order_relaxed) ||
        failed_.load(std::memory_order_relaxed)) {
      polling_.store(false, std::memory_order_relaxed);
      return;
    }
    std::array<urma_cr_t, kPollBatch> completions{};
    std::array<std::shared_ptr<jfc_group_socket>, kPollBatch> owners{};
    for (std::size_t round = 0; round < io_rate_; ++round) {
      if (!alive_.load(std::memory_order_relaxed) ||
          failed_.load(std::memory_order_relaxed)) {
        polling_.store(false, std::memory_order_relaxed);
        return;
      }
      int count = urma_poll_jfc(
          jfc_.get(), static_cast<int>(completions.size()), completions.data());
      if (count < 0) {
        fail_all(std::make_error_code(std::errc::io_error));
        return;
      }
      if (count == 0)
        continue;
      const auto batch = static_cast<std::size_t>(count);
      // One registry lock per batch instead of one per completion.
      const std::size_t resolved =
          registry_.resolve(completions.data(), batch, owners.data());
      stale_completions_ += batch - resolved;
      for (std::size_t i = 0; i < batch; ++i) {
        auto owner = std::move(owners[i]);
        if (!owner)
          continue;
        deliver_guarded(*owner, completions[i]);
      }
    }
    // No attached socket: let the group be destroyed instead of keeping itself
    // alive through the posted tick. attach() restarts the loop. The registry
    // is the single source of truth here (it already accounts for sockets that
    // died without detaching); a separate counter used to diverge from it and
    // keep the loop spinning forever.
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      if (!registry_.has_live_socket()) {
        if (stale_completions_ != 0 || dispatch_failures_ != 0) {
          ELOG_INFO << "URMA shared JFC: dropped " << stale_completions_
                    << " completion(s) for detached sockets or constructed by "
                       "hardware, "
                    << dispatch_failures_ << " dispatch exception(s)";
        }
        alive_.store(false, std::memory_order_relaxed);
        polling_.store(false, std::memory_order_relaxed);
        return;
      }
    }
    schedule();
  }

  // A socket callback resumes application coroutines. A throwing callback must
  // not kill the group's poll loop (which would silently stop every socket on
  // this io_context), so catch per completion and keep draining.
  void deliver_guarded(jfc_group_socket& socket, const urma_cr_t& cr) {
    try {
      jfc_group_registry::deliver(socket, cr);
    } catch (const std::exception& e) {
      ++dispatch_failures_;
      ELOG_ERROR << "URMA shared JFC dispatch threw: " << e.what();
    } catch (...) {
      ++dispatch_failures_;
      ELOG_ERROR << "URMA shared JFC dispatch threw an unknown exception";
    }
  }

  void subscribe_async_events(std::shared_ptr<urma_async_event_hub> hub) {
    if (!hub)
      return;
    async_events_ = std::move(hub);
    std::weak_ptr<urma_jfc_group> weak = weak_from_this();
    auto executor = executor_;
    async_events_->subscribe(
        jfc_.get(), weak, [weak, executor](urma_async_event_type_t type) {
          // Watcher thread: hop to the group's io_context.
          asio::post(executor, [weak, type] {
            auto group = weak.lock();
            if (!group || group->failed())
              return;
            ELOG_ERROR << "URMA shared JFC async error event "
                       << static_cast<int>(type);
            group->fail_all(std::make_error_code(std::errc::io_error));
          });
        });
  }

  void fail_all(std::error_code ec) {
    // Publish the failure while holding the same mutex attach() uses, and set
    // failed_ first: a concurrent attach then either sees it (and returns -3)
    // or is ordered before this and gets covered by for_each below.
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      failed_.store(true, std::memory_order_relaxed);
      alive_.store(false, std::memory_order_relaxed);
      polling_.store(false, std::memory_order_relaxed);
    }
    ELOG_ERROR << "URMA shared JFC poll failed: " << ec.message()
               << "; failing " << registry_.size() << " socket(s)";
    registry_.for_each([ec](jfc_group_socket& socket) {
      socket.on_group_error(ec);
    });
  }

  static void prune_expired_locked(
      std::map<key_type, std::weak_ptr<urma_jfc_group>>& groups) {
    for (auto it = groups.begin(); it != groups.end();) {
      if (it->second.expired())
        it = groups.erase(it);
      else
        ++it;
    }
  }

  static std::mutex& registry_mutex() {
    static auto* value = new std::mutex();
    return *value;
  }

  static std::map<key_type, std::weak_ptr<urma_jfc_group>>& registry() {
    static auto* value =
        new std::map<key_type, std::weak_ptr<urma_jfc_group>>();
    return *value;
  }

  // Completions drained per urma_poll_jfc call.
  static constexpr std::size_t kPollBatch = 16;

  asio::any_io_executor executor_;
  std::shared_ptr<urma_async_event_hub> async_events_;
  std::unique_ptr<urma_jfc_t, jfc_deleter> jfc_;
  // Orders attach() against fail_all() and the poll loop's idle exit.
  std::mutex state_mutex_;
  jfc_group_registry registry_;
  uint32_t io_rate_ = 8;
  uint32_t cq_size_ = 0;
  std::atomic<bool> polling_{false};
  std::atomic<bool> alive_{true};
  std::atomic<bool> failed_{false};
  std::size_t stale_completions_ = 0;
  std::size_t dispatch_failures_ = 0;
};

}  // namespace coro_io::detail

#endif  // YLT_ENABLE_URMA

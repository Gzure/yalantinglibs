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

#include <fcntl.h>
#include <urma_api.h>
#include <urma_ubagg.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
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
#include "asio/posix/stream_descriptor.hpp"
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
//
// Idle wait: spinning keeps a whole core busy even with no traffic at all
// (idle connections in a pool, a client between bursts). With idle_wait > 0
// the JFC is bound to a jfce, and once no completion arrived for idle_wait
// the loop arms the JFC and sleeps on the jfce fd until the next completion
// event; then it spins again. Under load nothing changes; only the first
// completion after an idle period pays the event wake-up.
// ---------------------------------------------------------------------------
class urma_jfc_group : public std::enable_shared_from_this<urma_jfc_group> {
 public:
  struct jfc_deleter {
    void operator()(urma_jfc_t* value) const {
      if (value)
        urma_delete_jfc(value);
    }
    void operator()(urma_jfce_t* value) const {
      if (value)
        urma_delete_jfce(value);
    }
  };

  struct options {
    uint32_t cq_size = 2048;
    uint32_t max_cq_depth = 0;  // device max_jfc_depth, 0 = unknown
    uint32_t io_rate = 8;       // poll rounds per tick
    // Work requests the requesting socket will reserve: get() returns a group
    // with that much room, or adds one.
    uint32_t wr_reserve = 0;
    // Sleep on the JFC's completion event after this long without a
    // completion; 0 = always spin.
    std::chrono::microseconds idle_wait{0};
    // The device's async event hub: the group subscribes its JFC, so a JFC
    // error event (e.g. an overflow) fails every attached socket instead of
    // leaving them on a JFC that never completes.
    std::shared_ptr<urma_async_event_hub> async_events;
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
  // The group parameters (cq_size, io_rate, idle_wait) come from the socket
  // that creates the group; later sockets join with whatever it was created
  // with.
  static std::shared_ptr<urma_jfc_group> get(
      coro_io::ExecutorWrapper<>* executor, urma_context_t* context,
      const options& opts) {
    const uint32_t wr_reserve = opts.wr_reserve;
    if (!executor || !context)
      return nullptr;
    auto asio_executor = executor->get_asio_executor();
    const auto key =
        std::make_pair(static_cast<void*>(&asio_executor.context()),
                       static_cast<void*>(context));
    {
      // Fast path: an existing live group with room, without creating one.
      std::lock_guard<std::mutex> lock(registry_mutex());
      if (auto existing = find_with_room_locked(key, wr_reserve))
        return existing;
    }

    // Every group of this io_context is full (or there is none yet): add one
    // instead of falling back to a per-socket JFC, whose spin loop would then
    // compete with the group's on the same reactor thread. The JFC is created
    // outside the process-wide lock: urma_create_jfc is a driver call and used
    // to serialize group creation for every io_context.
    auto group = std::shared_ptr<urma_jfc_group>(new urma_jfc_group(
        std::move(asio_executor), opts.io_rate, opts.idle_wait));
    if (!group->init(context, opts.cq_size, opts.max_cq_depth))
      return nullptr;
    if (!group->has_room(wr_reserve)) {
      // Even an empty group cannot hold this socket: adding more groups would
      // not help.
      ELOG_WARN << "URMA shared JFC: one socket needs " << wr_reserve
                << " CQ entries but the group depth is " << group->cq_size();
      return nullptr;
    }

    std::size_t group_count = 0;
    {
      std::lock_guard<std::mutex> lock(registry_mutex());
      // Another thread may have added a group with room meanwhile: use it and
      // drop our extra JFC.
      if (auto existing = find_with_room_locked(key, wr_reserve))
        return existing;
      auto& groups = registry()[key];
      groups.push_back(group);
      group_count = groups.size();
    }
    if (group_count > 1) {
      ELOG_INFO << "URMA shared JFC: io_context now has " << group_count
                << " groups (each full group spills into a new JFC)";
    }
    group->subscribe_async_events(opts.async_events);
    return group;
  }

  ~urma_jfc_group() {
    stop();
    // Before jfc_ is deleted, so a recycled address cannot reach this group.
    if (async_events_)
      async_events_->unsubscribe(jfc_.get());
    // The jfce owns the fd: release it from asio instead of closing it.
    if (event_fd_) {
      event_fd_->release();
      event_fd_.reset();
    }
    // urma_delete_jfc waits until every completion event is acked. Members
    // are then destroyed in reverse order: jfc_ before jfce_.
    ack_jfc_events();
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

  void detach(int sid) {
    registry_.detach(sid);
    wake_if_idle();
  }
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
  uint32_t io_rate() const noexcept { return io_rate_; }
  bool has_room(uint32_t wr_reserve) {
    return !failed() && registry_.has_room(wr_reserve);
  }

 private:
  using key_type = std::pair<void*, void*>;

  urma_jfc_group(asio::any_io_executor executor, uint32_t io_rate,
                 std::chrono::microseconds idle_wait)
      : executor_(std::move(executor)),
        idle_wait_(idle_wait.count() > 0 ? idle_wait
                                         : std::chrono::microseconds{0}),
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
    // The event channel is only needed to sleep while idle.
    cfg.jfce = idle_wait_.count() > 0 ? create_jfce(context) : nullptr;
    errno = 0;
    jfc_.reset(urma_create_jfc(context, &cfg));
    if (!jfc_) {
      ELOG_WARN << "URMA shared JFC creation failed: depth=" << cfg.depth
                << ", errno=" << errno << "; falling back to per-socket JFC";
      return false;
    }
    cq_size_ = cq_size;
    registry_.set_max_wr(cq_size);
    if (jfce_)
      setup_idle_wait();
    ELOG_INFO << "URMA shared JFC created: depth=" << cq_size
              << ", io_rate=" << io_rate_
              << ", idle_wait_us=" << (event_fd_ ? idle_wait_.count() : 0);
    return true;
  }

  // Returns the jfce to bind the JFC to, or null (always spin) on failure.
  urma_jfce_t* create_jfce(urma_context_t* context) {
    errno = 0;
    jfce_.reset(urma_create_jfce(context));
    if (!jfce_ || jfce_->fd < 0) {
      ELOG_WARN << "URMA shared JFC: urma_create_jfce failed: errno=" << errno
                << "; the poll loop always spins";
      jfce_.reset();
    }
    return jfce_.get();
  }

  // Wraps the jfce fd for asio and asks for one event per completion: the
  // driver's default moderation batches events and would add tens to
  // hundreds of microseconds to the first completion after an idle period.
  void setup_idle_wait() {
    const int flags = ::fcntl(jfce_->fd, F_GETFL);
    if (flags < 0 || ::fcntl(jfce_->fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      ELOG_WARN << "URMA shared JFC: jfce fd cannot be made non-blocking: "
                   "errno="
                << errno << "; the poll loop always spins";
      return;
    }
    try {
      event_fd_ = std::make_unique<asio::posix::stream_descriptor>(executor_,
                                                                   jfce_->fd);
    } catch (const std::exception& e) {
      ELOG_WARN << "URMA shared JFC: cannot watch the jfce fd: " << e.what()
                << "; the poll loop always spins";
      return;
    }
#ifndef YLT_URMA_NO_JFC_MODERATION
    urma_jfc_attr_t attr{};
    attr.mask = JFC_MODERATE_COUNT;
    attr.moderate_count = 1;
    if (urma_modify_jfc(jfc_.get(), &attr) != URMA_SUCCESS) {
      ELOG_WARN << "URMA shared JFC: urma_modify_jfc(moderate_count=1) failed: "
                   "errno="
                << errno << "; the first completion after idle may be late";
    }
#endif
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
    std::size_t delivered = 0;
    // io_rate_ bounds the poll rounds; kMaxCompletionsPerTick bounds the
    // dispatched callbacks, so a busy CQ cannot hold the reactor thread for
    // io_rate_ * kPollBatch callbacks (12672 at io_rate=99%). The rest stays
    // queued for the next tick, after the io_context ran its other handlers.
    for (std::size_t round = 0;
         round < io_rate_ && delivered < kMaxCompletionsPerTick; ++round) {
      if (!alive_.load(std::memory_order_relaxed) ||
          failed_.load(std::memory_order_relaxed)) {
        polling_.store(false, std::memory_order_relaxed);
        return;
      }
      const int count = poll_batch();
      if (count < 0)
        return;  // the group failed
      delivered += static_cast<std::size_t>(count);
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
    if (event_fd_) {
      const auto now = std::chrono::steady_clock::now();
      if (delivered > 0)
        last_activity_ = now;
      else if (now - last_activity_ >= idle_wait_) {
        try_sleep();
        return;
      }
    }
    schedule();
  }

  // Polls one batch and dispatches it. Returns the number of records polled,
  // or -1 after failing the group.
  int poll_batch() {
    std::array<urma_cr_t, kPollBatch> completions;
    std::array<std::shared_ptr<jfc_group_socket>, kPollBatch> owners;
    const int count = urma_poll_jfc(
        jfc_.get(), static_cast<int>(completions.size()), completions.data());
    if (count < 0) {
      fail_all(std::make_error_code(std::errc::io_error));
      return -1;
    }
    if (count == 0)
      return 0;
    const auto batch = static_cast<std::size_t>(count);
    // One registry lock per batch instead of one per completion.
    const std::size_t resolved =
        registry_.resolve(completions.data(), batch, owners.data());
    stale_completions_ += batch - resolved;
    for (std::size_t i = 0; i < batch; ++i) {
      auto owner = std::move(owners[i]);
      if (owner)
        deliver_guarded(*owner, completions[i]);
    }
    return count;
  }

  // Arms the JFC and sleeps on the jfce fd until the next completion event.
  // urma_rearm_jfc fails while records are unread, and a record that lands
  // between the last poll and the rearm raises no event, so poll once more
  // after arming: only an empty poll may go to sleep.
  void try_sleep() {
    if (urma_rearm_jfc(jfc_.get(), false) != URMA_SUCCESS) {
      schedule();  // records are pending: keep spinning
      return;
    }
    const int count = poll_batch();
    if (count < 0)
      return;
    if (count > 0) {
      // Armed but busy again: the event raised later just wakes an awake loop
      // early (on_wake acks it).
      last_activity_ = std::chrono::steady_clock::now();
      schedule();
      return;
    }
    sleeping_ = true;
    ++sleeps_;
    auto self = shared_from_this();
    event_fd_->async_wait(asio::posix::stream_descriptor::wait_read,
                          [self](const std::error_code& ec) {
                            self->on_wake(ec);
                          });
  }

  void on_wake(const std::error_code& ec) {
    sleeping_ = false;
    ack_jfc_events();
    if (ec && ec != asio::error::operation_aborted) {
      ELOG_WARN << "URMA shared JFC: waiting on the jfce fd failed: "
                << ec.message() << "; the poll loop always spins from now on";
      event_fd_->release();  // the jfce owns the fd: do not close it
      event_fd_.reset();
    }
    // Spin through a whole idle window again before the next sleep.
    last_activity_ = std::chrono::steady_clock::now();
    poll_once();
  }

  // Collects and acks the completion events of the JFC: urma_delete_jfc waits
  // for unacked ones. time_out 0 never blocks; an armed JFC raises at most one
  // event, so a couple of rounds cover a stale event plus a fresh one.
  void ack_jfc_events() {
    if (!jfce_)
      return;
    for (int round = 0; round < 4; ++round) {
      urma_jfc_t* ev_jfc = nullptr;
      if (urma_wait_jfc(jfce_.get(), 1, 0, &ev_jfc) <= 0 || !ev_jfc)
        return;
      uint32_t ack_cnt = 1;
      urma_ack_jfc(&ev_jfc, &ack_cnt, 1);
    }
  }

  // A sleeping loop only wakes on a completion: when the last socket detached
  // (or the group failed), wake it so it can exit and release the group.
  void wake_if_idle() {
    if (!event_fd_)
      return;
    std::weak_ptr<urma_jfc_group> weak = weak_from_this();
    asio::post(executor_, [weak] {
      auto group = weak.lock();
      if (!group || !group->sleeping_ || !group->event_fd_)
        return;
      if (group->failed() || !group->registry_.has_live_socket()) {
        std::error_code ignored;
        group->event_fd_->cancel(ignored);
      }
    });
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
    wake_if_idle();
    ELOG_ERROR << "URMA shared JFC poll failed: " << ec.message()
               << "; failing " << registry_.size() << " socket(s)";
    registry_.for_each([ec](jfc_group_socket& socket) {
      socket.on_group_error(ec);
    });
  }

  using group_list = std::vector<std::weak_ptr<urma_jfc_group>>;

  // Caller holds registry_mutex(). Drops dead and failed groups of `key` (a
  // failed group must not be reused) and returns the first one with room.
  static std::shared_ptr<urma_jfc_group> find_with_room_locked(
      const key_type& key, uint32_t wr_reserve) {
    auto& groups = registry();
    auto it = groups.find(key);
    if (it == groups.end())
      return nullptr;
    auto& list = it->second;
    std::shared_ptr<urma_jfc_group> found;
    for (auto entry = list.begin(); entry != list.end();) {
      auto group = entry->lock();
      if (!group || group->failed()) {
        entry = list.erase(entry);
        continue;
      }
      if (!found && group->has_room(wr_reserve))
        found = std::move(group);
      ++entry;
    }
    if (list.empty())
      groups.erase(it);
    return found;
  }

  static std::mutex& registry_mutex() {
    static auto* value = new std::mutex();
    return *value;
  }

  static std::map<key_type, group_list>& registry() {
    static auto* value = new std::map<key_type, group_list>();
    return *value;
  }

  // Completions drained per urma_poll_jfc call.
  static constexpr std::size_t kPollBatch = 16;
  // Completions dispatched per tick before yielding to the io_context.
  static constexpr std::size_t kMaxCompletionsPerTick = 256;

  asio::any_io_executor executor_;
  std::shared_ptr<urma_async_event_hub> async_events_;
  // Declared before jfc_: the JFC bound to it is deleted first.
  std::unique_ptr<urma_jfce_t, jfc_deleter> jfce_;
  std::unique_ptr<urma_jfc_t, jfc_deleter> jfc_;
  // Idle wait (see the class comment). Touched on the executor thread only.
  std::unique_ptr<asio::posix::stream_descriptor> event_fd_;
  std::chrono::microseconds idle_wait_{0};
  std::chrono::steady_clock::time_point last_activity_ =
      std::chrono::steady_clock::now();
  bool sleeping_ = false;
  std::size_t sleeps_ = 0;
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

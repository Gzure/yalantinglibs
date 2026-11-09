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
#include <utility>
#include <vector>

#include "asio/post.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/easylog.hpp"

namespace coro_io::detail {

// ---------------------------------------------------------------------------
// user_ctx encoding for a shared JFC.
//
// Multiple jetties (one per socket) feed one JFC, so a completion record no
// longer identifies its socket implicitly (a per-socket CQ used to guarantee
// that). Encode it in the work request's user_ctx instead:
//
//   bits 63..60 : op (0 = send, 1 = recv)
//   bits 59..32 : socket id inside the group (28 bits)
//   bits 31..0  : per-socket sequence for send WRs, or recv sequence for recv
//
// ---------------------------------------------------------------------------
enum class urma_ctx_op : uint8_t { send = 0, recv = 1 };

struct decoded_ctx {
  urma_ctx_op op = urma_ctx_op::send;
  uint32_t sid = 0;
  uint32_t seq = 0;
};

inline uint64_t encode_ctx(urma_ctx_op op, uint32_t sid,
                           uint32_t seq) noexcept {
  return (static_cast<uint64_t>(static_cast<uint8_t>(op) & 0x0F) << 60) |
         (static_cast<uint64_t>(sid & 0x0FFFFFFF) << 32) |
         static_cast<uint64_t>(seq);
}

inline decoded_ctx decode_ctx(uint64_t value) noexcept {
  return decoded_ctx{static_cast<urma_ctx_op>((value >> 60) & 0x0F),
                     static_cast<uint32_t>((value >> 32) & 0x0FFFFFFF),
                     static_cast<uint32_t>(value & 0xFFFFFFFF)};
}

// ---------------------------------------------------------------------------
// Fixed-capacity ring of move-only work items keyed by a 32-bit sequence.
//
// With a shared JFC, send completions from different sockets interleave, so the
// previous "pop the front of the FIFO" match is no longer valid. Keep the
// pending send descriptors in a slot ring indexed by seq % capacity and look
// the completion up by seq. The in-flight window is bounded by capacity() (the
// send buffer count), so a seq can never collide with a stale slot in practice;
// the tag check makes a collision harmless anyway.
// ---------------------------------------------------------------------------
template <typename T>
class seq_slot_ring {
 public:
  explicit seq_slot_ring(std::size_t capacity = 0)
      : slots_(capacity), tags_(capacity, 0) {}

  std::size_t capacity() const noexcept { return slots_.size(); }
  std::size_t in_flight() const noexcept { return in_flight_; }

  // Returns false when the slot is already occupied (caller must treat it as a
  // window overflow and fail the send instead of dropping the buffer).
  bool push(uint32_t seq, T value) {
    if (slots_.empty())
      return false;
    const std::size_t idx = seq % slots_.size();
    if (slots_[idx].has_value())
      return false;
    slots_[idx] = std::move(value);
    tags_[idx] = seq;
    ++in_flight_;
    return true;
  }

  std::optional<T> take(uint32_t seq) {
    if (slots_.empty())
      return std::nullopt;
    const std::size_t idx = seq % slots_.size();
    if (!slots_[idx].has_value() || tags_[idx] != seq)
      return std::nullopt;
    std::optional<T> out = std::move(slots_[idx]);
    slots_[idx].reset();
    --in_flight_;
    return out;
  }

  bool contains(uint32_t seq) const {
    if (slots_.empty())
      return false;
    const std::size_t idx = seq % slots_.size();
    return slots_[idx].has_value() && tags_[idx] == seq;
  }

  void clear() {
    for (auto& slot : slots_) slot.reset();
    in_flight_ = 0;
  }

 private:
  std::vector<std::optional<T>> slots_;
  std::vector<uint32_t> tags_;
  std::size_t in_flight_ = 0;
};

// ---------------------------------------------------------------------------
// One socket attached to a shared JFC. The group calls these on the io_context
// thread that owns the group; implementations must not block.
// ---------------------------------------------------------------------------
class jfc_group_socket {
 public:
  virtual ~jfc_group_socket() = default;
  virtual void on_send_completion(const urma_cr_t& cr) = 0;
  virtual void on_recv_completion(const urma_cr_t& cr) = 0;
  // Group-level failure (poll error or a jfc error event): every attached
  // socket must fail its pending work and close.
  virtual void on_group_error(std::error_code ec) = 0;
};

// ---------------------------------------------------------------------------
// Socket table of one JFC group, with the completion dispatch. Kept free of
// URMA control-plane calls so it can be unit-tested without a device.
//
// Thread-safety: attach/detach may come from any thread (socket close paths);
// dispatch runs on the owning io_context thread. Lookups copy the strong
// pointer under the lock and call the socket outside it, so a callback can
// re-enter the registry (e.g. close() -> detach()).
// ---------------------------------------------------------------------------
class jfc_group_registry {
 public:
  jfc_group_registry() = default;
  explicit jfc_group_registry(std::size_t max_wr) : max_wr_(max_wr) {}

  // Returns the sid (>= 0), -1 when the group is full, or -2 when the work
  // request reserve does not fit into the remaining CQ depth.
  int attach(std::shared_ptr<jfc_group_socket> socket, uint32_t wr_reserve) {
    if (!socket)
      return -1;
    std::lock_guard<std::mutex> lock(mutex_);
    if (max_wr_ != 0 && used_wr_ + wr_reserve > max_wr_)
      return -2;
    std::size_t sid = static_cast<std::size_t>(-1);
    for (std::size_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].expired()) {
        sid = i;
        break;
      }
    }
    if (sid == static_cast<std::size_t>(-1)) {
      sid = slots_.size();
      if (sid >= kMaxSockets)
        return -1;
      slots_.emplace_back();
      reserved_.push_back(0);
    }
    slots_[sid] = std::move(socket);
    reserved_[sid] = wr_reserve;
    used_wr_ += wr_reserve;
    return static_cast<int>(sid);
  }

  bool detach(int sid) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (sid < 0 || static_cast<std::size_t>(sid) >= slots_.size())
      return false;
    const auto idx = static_cast<std::size_t>(sid);
    if (slots_[idx].expired())
      return false;
    slots_[idx].reset();
    if (used_wr_ >= reserved_[idx])
      used_wr_ -= reserved_[idx];
    reserved_[idx] = 0;
    return true;
  }

  // Dispatches one completion record. Returns false for an unknown / expired
  // sid (a completion of a socket that already closed) so the caller can drop
  // it silently.
  bool dispatch(const urma_cr_t& cr) {
    const auto ctx = decode_ctx(cr.user_ctx);
    std::shared_ptr<jfc_group_socket> socket;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto sid = static_cast<std::size_t>(ctx.sid);
      if (sid >= slots_.size())
        return false;
      socket = slots_[sid].lock();
    }
    if (!socket)
      return false;
    if (ctx.op == urma_ctx_op::send)
      socket->on_send_completion(cr);
    else
      socket->on_recv_completion(cr);
    return true;
  }

  std::size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t count = 0;
    for (const auto& slot : slots_)
      if (!slot.expired())
        ++count;
    return count;
  }

  std::size_t used_wr() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_wr_;
  }

  void for_each(const std::function<void(jfc_group_socket&)>& fn) {
    std::vector<std::shared_ptr<jfc_group_socket>> sockets;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      sockets.reserve(slots_.size());
      for (const auto& slot : slots_)
        if (auto socket = slot.lock())
          sockets.push_back(std::move(socket));
    }
    for (auto& socket : sockets) fn(*socket);
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    slots_.clear();
    reserved_.clear();
    used_wr_ = 0;
  }

  // Sets the CQ depth this registry accounts against (0 = no limit check).
  void set_max_wr(std::size_t max_wr) {
    std::lock_guard<std::mutex> lock(mutex_);
    max_wr_ = max_wr;
  }

  static constexpr std::size_t kMaxSockets = 0x0FFFFFFF;

 private:
  mutable std::mutex mutex_;
  std::vector<std::weak_ptr<jfc_group_socket>> slots_;
  std::vector<uint32_t> reserved_;
  std::size_t used_wr_ = 0;
  std::size_t max_wr_ = 0;
};

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

  static std::shared_ptr<urma_jfc_group> get(
      coro_io::ExecutorWrapper<>* executor, urma_context_t* context,
      uint32_t cq_size, uint32_t max_cq_depth, uint32_t io_rate) {
    if (!executor || !context)
      return nullptr;
    auto* key = static_cast<void*>(&executor->get_asio_executor().context());
    std::lock_guard<std::mutex> lock(registry_mutex());
    auto& groups = registry();
    auto it = groups.find(key);
    if (it != groups.end()) {
      if (auto existing = it->second.lock())
        return existing;
    }
    auto group =
        std::shared_ptr<urma_jfc_group>(new urma_jfc_group(executor, io_rate));
    if (!group->init(context, cq_size, max_cq_depth))
      return nullptr;
    groups[key] = group;
    return group;
  }

  ~urma_jfc_group() { alive_.store(false, std::memory_order_relaxed); }

  bool valid() const noexcept { return static_cast<bool>(jfc_); }
  urma_jfc_t* jfc() const noexcept { return jfc_.get(); }
  uint32_t cq_size() const noexcept { return cq_size_; }

  // wr_reserve: work requests this socket may keep in flight (send + recv
  // buffers + slack). Returns sid, or < 0 on failure (group full / CQ too
  // small).
  int attach(std::shared_ptr<jfc_group_socket> socket, uint32_t wr_reserve) {
    return registry_.attach(std::move(socket), wr_reserve);
  }

  void detach(int sid) { registry_.detach(sid); }

  // Starts the single poll tick for this io_context (idempotent). Must be
  // called from the owning io_context thread.
  void ensure_polling() {
    if (!valid())
      return;
    bool expected = false;
    if (!polling_.compare_exchange_strong(expected, true))
      return;
    alive_.store(true, std::memory_order_relaxed);
    schedule();
  }

  // Stops the tick. Safe to call from any thread; in-flight dispatches finish
  // on the io_context thread.
  void stop() { alive_.store(false, std::memory_order_relaxed); }

  bool polling() const noexcept {
    return polling_.load(std::memory_order_relaxed);
  }
  std::size_t socket_count() const { return registry_.size(); }

 private:
  urma_jfc_group(coro_io::ExecutorWrapper<>* executor, uint32_t io_rate)
      : executor_(executor), io_rate_(std::max<uint32_t>(io_rate, 1)) {}

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
    if (!alive_.load(std::memory_order_relaxed))
      return;
    auto self = shared_from_this();
    asio::post(executor_->get_asio_executor(), [self] {
      self->poll_once();
    });
  }

  void poll_once() {
    if (!alive_.load(std::memory_order_relaxed))
      return;
    std::array<urma_cr_t, 16> completions{};
    for (std::size_t round = 0; round < io_rate_; ++round) {
      if (!alive_.load(std::memory_order_relaxed))
        return;
      int count = urma_poll_jfc(
          jfc_.get(), static_cast<int>(completions.size()), completions.data());
      if (count < 0) {
        fail_all(std::make_error_code(std::errc::io_error));
        return;
      }
      for (int i = 0; i < count; ++i) {
        if (!registry_.dispatch(completions[i])) {
          // Completion of a socket that already detached: the buffer pool is
          // device-global, and the detached socket returned its own buffers, so
          // there is nothing to reclaim here. Count it so a leak of stale
          // completions is visible.
          ++stale_completions_;
        }
      }
    }
    if (registry_.size() == 0) {
      // No attached socket: let the group be destroyed instead of keeping
      // itself alive through the posted tick. ensure_polling() restarts the
      // loop if a socket attaches again.
      ELOG_INFO << "URMA shared JFC poll loop stopped: no sockets attached";
      alive_.store(false, std::memory_order_relaxed);
      polling_.store(false, std::memory_order_relaxed);
      return;
    }
    schedule();
  }

  void fail_all(std::error_code ec) {
    ELOG_ERROR << "URMA shared JFC poll failed: " << ec.message()
               << "; failing " << registry_.size() << " socket(s)";
    alive_.store(false, std::memory_order_relaxed);
    polling_failure_reported_ = true;
    registry_.for_each([ec](jfc_group_socket& socket) {
      socket.on_group_error(ec);
    });
  }

  static std::mutex& registry_mutex() {
    static auto* value = new std::mutex();
    return *value;
  }

  static std::map<void*, std::weak_ptr<urma_jfc_group>>& registry() {
    static auto* value = new std::map<void*, std::weak_ptr<urma_jfc_group>>();
    return *value;
  }

  coro_io::ExecutorWrapper<>* executor_ = nullptr;
  std::unique_ptr<urma_jfc_t, jfc_deleter> jfc_;
  jfc_group_registry registry_;
  uint32_t io_rate_ = 8;
  uint32_t cq_size_ = 0;
  std::atomic<bool> polling_{false};
  std::atomic<bool> alive_{true};
  std::atomic<uint64_t> stale_completions_{0};
  bool polling_failure_reported_ = false;
};

}  // namespace coro_io::detail

#endif  // YLT_ENABLE_URMA

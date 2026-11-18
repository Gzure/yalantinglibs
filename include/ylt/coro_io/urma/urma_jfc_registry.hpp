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

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

// Device-independent pieces of the shared JFC: the user_ctx codec, the
// per-socket send slot ring and the socket registry with its completion
// routing. Kept free of URMA control-plane calls so they can be unit-tested
// without a device.
namespace coro_io::detail {

// ---------------------------------------------------------------------------
// user_ctx encoding for a shared JFC.
//
// Multiple jetties (one per socket) feed one JFC, so a completion record no
// longer identifies its socket implicitly (a per-socket CQ used to guarantee
// that). Encode it in the work request's user_ctx instead:
//
//   bits 63..60 : op (0 = send, 1 = recv)
//   bits 59..32 : socket id inside the group (28 bits, never reused)
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

inline bool is_valid_ctx_op(urma_ctx_op op) noexcept {
  return op == urma_ctx_op::send || op == urma_ctx_op::recv;
}

// URMA_CR_WR_SUSPEND_DONE and URMA_CR_WR_FLUSH_ERR_DONE are records the
// hardware constructs to report a jetty/jfs state change. The UMDK headers
// document their user_ctx as invalid, so they must never be routed by user_ctx
// (garbage may decode to a live sid) nor matched against a pending WR (they do
// not complete one). Real per-WR error records such as URMA_CR_WR_FLUSH_ERR
// keep a valid user_ctx and are delivered normally.
inline bool is_hw_constructed_cr(const urma_cr_t& cr) noexcept {
  return cr.status == URMA_CR_WR_SUSPEND_DONE ||
         cr.status == URMA_CR_WR_FLUSH_ERR_DONE;
}

// ---------------------------------------------------------------------------
// Fixed-capacity ring of move-only work items keyed by a 32-bit sequence.
//
// With a shared JFC, send completions from different sockets interleave, so the
// previous "pop the front of the FIFO" match is no longer valid. Keep the
// pending send descriptors in a slot ring indexed by seq % capacity and look
// the completion up by seq.
// ---------------------------------------------------------------------------
template <typename T>
class seq_slot_ring {
 public:
  explicit seq_slot_ring(std::size_t capacity = 0)
      : slots_(capacity), tags_(capacity, 0) {}

  std::size_t capacity() const noexcept { return slots_.size(); }
  std::size_t in_flight() const noexcept { return in_flight_; }

  // Reserves the slot for seq and moves the value in on success. On failure the
  // value is left untouched (NOT consumed), so the caller can still return its
  // buffer / resume its callback: urma_buffer_t has no RAII deleter.
  bool push(uint32_t seq, T&& value) {
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

  // Visits every live entry (used to settle a closing socket). Entries whose
  // seq is not congruent to their index modulo capacity are still visited, so
  // sequence holes cannot strand a pending send.
  template <typename Fn>
  void drain_each(Fn&& fn) {
    for (auto& slot : slots_) {
      if (!slot.has_value())
        continue;
      fn(std::move(*slot));
      slot.reset();
    }
    in_flight_ = 0;
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
  // A hardware-constructed record (SUSPEND_DONE / FLUSH_ERR_DONE) routed to
  // this socket by its jetty id (cr.local_id), never by the invalid user_ctx.
  // FLUSH_ERR_DONE is the hardware's statement that every outstanding tx/rx WR
  // of the jetty has completed, i.e. that its buffers may be released.
  virtual void on_hw_constructed_completion(const urma_cr_t&) {}
  // Group-level failure (poll error or a jfc error event): every attached
  // socket must fail its pending work and close.
  virtual void on_group_error(std::error_code ec) = 0;
};

// ---------------------------------------------------------------------------
// Socket table of one JFC group, with the completion dispatch. Kept free of
// URMA control-plane calls so it can be unit-tested without a device.
//
// Socket ids are monotonic and never reused. A completion that arrives for a
// socket that already detached therefore cannot be dispatched to a different
// (new) socket that happened to get the same slot.
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

  // Returns the sid (>= 0), -1 when the socket is invalid or the sid space is
  // exhausted, or -2 when the work request reserve does not fit into the
  // remaining CQ depth.
  int attach(std::shared_ptr<jfc_group_socket> socket, uint32_t wr_reserve) {
    if (!socket)
      return -1;
    std::lock_guard<std::mutex> lock(mutex_);
    prune_expired_locked();
    if (max_wr_ != 0 && used_wr_ + wr_reserve > max_wr_)
      return -2;
    if (next_sid_ >= kMaxSockets)
      return -1;
    const uint32_t sid = next_sid_++;
    slot entry;
    entry.weak = std::move(socket);
    entry.reserve = wr_reserve;
    slots_[sid] = std::move(entry);
    used_wr_ += wr_reserve;
    return static_cast<int>(sid);
  }

  // Detaches a socket: drops its routing entries and releases its CQ
  // reservation. A closing socket must only detach once its jetty has drained
  // (FLUSH_ERR_DONE seen, jetty/jfr deleted): until then its outstanding WRs
  // and the flush records still land in the shared JFC, and releasing the
  // budget early would let a new socket over-sell the CQ (a JFC overflow is a
  // fatal async error for every socket of the group).
  bool detach(int sid) {
    // Declared before the lock so a pinned socket is released after unlock:
    // dropping the last reference runs its destructor, which may re-enter.
    std::shared_ptr<jfc_group_socket> released_pin;
    std::lock_guard<std::mutex> lock(mutex_);
    if (sid < 0)
      return false;
    auto it = slots_.find(static_cast<uint32_t>(sid));
    if (it == slots_.end())
      return false;
    if (used_wr_ >= it->second.reserve)
      used_wr_ -= it->second.reserve;
    if (it->second.has_jetty) {
      auto jt = jetty_index_.find(it->second.jetty_id);
      if (jt != jetty_index_.end() && jt->second == it->first)
        jetty_index_.erase(jt);
    }
    released_pin = std::move(it->second.pin);
    slots_.erase(it);
    return true;
  }

  // Associates the socket's jetty id with its sid, so records that carry no
  // valid user_ctx can still be routed (by cr.local_id).
  bool bind_jetty(int sid, uint32_t jetty_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slot_locked(sid);
    if (it == slots_.end())
      return false;
    it->second.jetty_id = jetty_id;
    it->second.has_jetty = true;
    jetty_index_[jetty_id] = it->first;
    return true;
  }

  // Keeps a draining socket alive until it detaches: the registry otherwise
  // holds only a weak reference, and the application may drop its last handle
  // right after close() while the hardware still owns the socket's buffers.
  bool pin(int sid, std::shared_ptr<jfc_group_socket> socket) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slot_locked(sid);
    if (it == slots_.end() || !socket)
      return false;
    it->second.pin = std::move(socket);
    return true;
  }

  // Resolves the owning socket of every record in one batch, taking the lock
  // once per batch instead of once per completion. out[i] is left null for a
  // record that must be dropped: a hardware-constructed record, an unknown /
  // expired sid (a completion of a socket that already closed), an unknown op,
  // or a user_ctx the driver did not fill in. Returns the number of records
  // that resolved to a live socket.
  //
  // The strong pointers keep the sockets alive while the caller delivers the
  // batch outside the lock. A delivery may close (and detach) another socket of
  // the same batch; sockets therefore ignore completions after close().
  std::size_t resolve(const urma_cr_t* crs, std::size_t count,
                      std::shared_ptr<jfc_group_socket>* out) {
    std::size_t resolved = 0;
    std::lock_guard<std::mutex> lock(mutex_);
    for (std::size_t i = 0; i < count; ++i) {
      out[i].reset();
      uint32_t sid = 0;
      if (!routable_locked(crs[i], sid))
        continue;
      auto it = slots_.find(sid);
      if (it == slots_.end())
        continue;
      out[i] = it->second.weak.lock();
      if (out[i])
        ++resolved;
    }
    return resolved;
  }

  // Delivers a record that resolve() routed to `socket`.
  static void deliver(jfc_group_socket& socket, const urma_cr_t& cr) {
    if (is_hw_constructed_cr(cr))
      socket.on_hw_constructed_completion(cr);
    else if (cr.flag.bs.s_r == 0)
      socket.on_send_completion(cr);
    else
      socket.on_recv_completion(cr);
  }

  // Dispatches one completion record; see resolve() for when it is dropped.
  bool dispatch(const urma_cr_t& cr) {
    std::shared_ptr<jfc_group_socket> socket;
    if (resolve(&cr, 1, &socket) == 0)
      return false;
    deliver(*socket, cr);
    return true;
  }

  // True while at least one attached socket is alive. Stops at the first live
  // entry, so the poll loop's per-tick idle check is O(1) in the common case.
  bool has_live_socket() const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [sid, entry] : slots_)
      if (!entry.weak.expired())
        return true;
    return false;
  }

  std::size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t count = 0;
    for (const auto& [sid, entry] : slots_)
      if (!entry.weak.expired())
        ++count;
    return count;
  }

  std::size_t used_wr() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return used_wr_;
  }

  // True when attach(wr_reserve) would fit into the CQ depth (after dropping
  // sockets that died without detaching). A concurrent attach can still take
  // the room first; attach() stays authoritative and returns -2 then.
  bool has_room(uint32_t wr_reserve) {
    std::lock_guard<std::mutex> lock(mutex_);
    prune_expired_locked();
    return max_wr_ == 0 || used_wr_ + wr_reserve <= max_wr_;
  }

  void for_each(const std::function<void(jfc_group_socket&)>& fn) {
    std::vector<std::shared_ptr<jfc_group_socket>> sockets;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      sockets.reserve(slots_.size());
      for (auto& [sid, entry] : slots_)
        if (auto socket = entry.weak.lock())
          sockets.push_back(std::move(socket));
    }
    for (auto& socket : sockets) fn(*socket);
  }

  void clear() {
    // Destroyed after the lock is released: see detach().
    std::unordered_map<uint32_t, slot> released;
    std::lock_guard<std::mutex> lock(mutex_);
    released.swap(slots_);
    jetty_index_.clear();
    used_wr_ = 0;
  }

  // Sets the CQ depth this registry accounts against (0 = no limit check).
  void set_max_wr(std::size_t max_wr) {
    std::lock_guard<std::mutex> lock(mutex_);
    max_wr_ = max_wr;
  }

  static constexpr uint32_t kMaxSockets = 0x0FFFFFFF;

 private:
  struct slot {
    std::weak_ptr<jfc_group_socket> weak;
    uint32_t reserve = 0;
    uint32_t jetty_id = 0;
    bool has_jetty = false;
    // Strong reference held only while the socket drains (see pin()).
    std::shared_ptr<jfc_group_socket> pin;
  };

  std::unordered_map<uint32_t, slot>::iterator slot_locked(int sid) {
    if (sid < 0)
      return slots_.end();
    return slots_.find(static_cast<uint32_t>(sid));
  }

  // Validates a record and extracts its sid. A hardware-constructed record has
  // an invalid user_ctx, so it is routed by the jetty id in cr.local_id, and
  // only when the record says local_id is a jetty id (flag.jetty == 1): JFS /
  // JFR ids live in other id spaces and could alias a jetty id.
  bool routable_locked(const urma_cr_t& cr, uint32_t& sid) const noexcept {
    if (is_hw_constructed_cr(cr)) {
      if (cr.flag.bs.jetty == 0)
        return false;
      auto it = jetty_index_.find(cr.local_id);
      if (it == jetty_index_.end())
        return false;
      sid = it->second;
      return true;
    }
    if (cr.user_ctx == 0)
      return false;
    const auto ctx = decode_ctx(cr.user_ctx);
    if (!is_valid_ctx_op(ctx.op))
      return false;
    // The user_ctx and the completion direction must agree; if they do not, the
    // driver did not round-trip user_ctx correctly and dispatching would settle
    // the wrong pending work.
    if ((cr.flag.bs.s_r != 0) != (ctx.op == urma_ctx_op::recv))
      return false;
    sid = ctx.sid;
    return true;
  }

  // Drops entries whose socket died without detaching, so their CQ reservation
  // cannot block every future attach.
  void prune_expired_locked() {
    for (auto it = slots_.begin(); it != slots_.end();) {
      // A pinned socket cannot expire: the pin holds a strong reference.
      if (it->second.weak.expired()) {
        if (used_wr_ >= it->second.reserve)
          used_wr_ -= it->second.reserve;
        if (it->second.has_jetty) {
          auto jt = jetty_index_.find(it->second.jetty_id);
          if (jt != jetty_index_.end() && jt->second == it->first)
            jetty_index_.erase(jt);
        }
        it = slots_.erase(it);
      }
      else {
        ++it;
      }
    }
  }

  mutable std::mutex mutex_;
  std::unordered_map<uint32_t, slot> slots_;
  std::unordered_map<uint32_t, uint32_t> jetty_index_;  // jetty id -> sid
  uint32_t next_sid_ = 0;
  std::size_t used_wr_ = 0;
  std::size_t max_wr_ = 0;
};

}  // namespace coro_io::detail

#endif  // YLT_ENABLE_URMA

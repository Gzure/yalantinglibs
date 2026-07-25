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
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

namespace coro_io::detail {

// Forward-declared completion record so the header compiles without the
// vendor URMA types in pure-logic tests.  In production it is urma_cr_t; the
// tests use a stand-in.
struct handoff_cr {
  uint64_t user_ctx = 0;
  uint32_t status = 0;
  uint32_t completion_len = 0;
  uint8_t is_recv = 0;
};

// Lock-free handoff of a single completion from a poll thread to a request
// coroutine, with a fallback queue for completions that arrive when no one is
// waiting.
//
// States:
//   IDLE    - no one waiting; poll thread pushes to the fallback queue.
//   WAITING - a coroutine has suspended and registered a resume handle.
//   READY   - a completion has been handed off; the coroutine must take it.
//
// Coroutine (request thread):
//   if try_begin_wait(fn, ctx):    // store handle, THEN IDLE -> WAITING
//       suspend                    // handle already published, no lost wakeup
//   else:                          // already READY (branch A): poll thread won
//       take_pending(); try_finish();   // READY -> IDLE
//   on wake: take_pending(); try_finish();   // READY -> IDLE
//
// NOTE: the resume handle MUST be stored BEFORE publishing WAITING. Use the
// two-arg try_begin_wait(fn, ctx) for the production suspend path; the zero-arg
// try_begin_wait() + set_resume_handle() sequence publishes WAITING first and
// has a lost-wakeup window, so it is for tests only.
//
// Poll thread:
//   if try_deliver(cr):            // WAITING -> READY (direct handoff)
//       cr is now in pending_cr_; resume the handle.
//   else:                          // IDLE or READY -> enqueue
//       push_queue(cr)
class completion_handoff {
 public:
  enum class state : uint32_t { IDLE = 0, WAITING = 1, READY = 2 };

  using resume_fn = void (*)(void*);

  state load_state() const noexcept {
    return state(st_.load(std::memory_order_acquire));
  }

  // Coroutine side: attempt to enter WAITING without registering a resume
  // handle. For tests / when no resume callback is needed. PRODUCTION suspend
  // path MUST use the two-arg overload below (handle published BEFORE state) to
  // avoid a lost wakeup: the poll thread, on observing WAITING via its
  // try_deliver CAS, would call take_resume_handle() and get nullptr if the
  // handle has not been stored yet.
  bool try_begin_wait() noexcept {
    uint32_t expected = uint32_t(state::IDLE);
    return st_.compare_exchange_strong(expected, uint32_t(state::WAITING),
                                       std::memory_order_acq_rel);
  }

  // Coroutine side: store the resume handle, THEN attempt IDLE -> WAITING.
  // Publishing the handle before the state guarantees the poll thread, on
  // observing WAITING, can read a valid handle (no lost wakeup). Returns
  // false if the state is already READY (branch A: poll thread delivered
  // before the coroutine suspended); the coroutine should take_pending()
  // immediately without suspending.
  //
  // Memory ordering: the handle stores are sequenced-before the CAS, so the
  // CAS's release (on success) publishes both the WAITING state AND the handle
  // stores to any thread that acquires the state. A poll thread that succeeds
  // at its acquire CAS (WAITING->READY) therefore sees the handle stores. The
  // handle stores themselves are relaxed because they are only read through the
  // state-published synchronization; they need no independent ordering.
  bool try_begin_wait(resume_fn fn, void* ctx) noexcept {
    resume_ctx_.store(ctx, std::memory_order_relaxed);
    resume_fn_.store(fn, std::memory_order_relaxed);
    uint32_t expected = uint32_t(state::IDLE);
    return st_.compare_exchange_strong(expected, uint32_t(state::WAITING),
                                       std::memory_order_acq_rel);
  }

  // Coroutine side: publish the resume handle into an already-WAITING slot.
  // LEGACY / secondary path. The preferred production path is the two-arg
  // try_begin_wait(fn, ctx), which stores the handle BEFORE publishing WAITING
  // and therefore cannot lose a wakeup. Calling set_resume_handle AFTER a
  // successful zero-arg try_begin_wait() reintroduces the lost-wakeup window
  // (poll thread may observe WAITING before the handle is stored) and must NOT
  // be used in the production suspend path.
  void set_resume_handle(resume_fn fn, void* ctx) noexcept {
    resume_ctx_.store(ctx, std::memory_order_release);
    resume_fn_.store(fn, std::memory_order_release);
  }

  // Poll thread side: read and clear the resume handle (called only after a
  // successful WAITING->READY flip).
  std::pair<resume_fn, void*> take_resume_handle() noexcept {
    void* ctx = resume_ctx_.exchange(nullptr, std::memory_order_acquire);
    resume_fn fn = resume_fn_.exchange(nullptr, std::memory_order_acquire);
    return {fn, ctx};
  }

  // Poll thread side: try to hand off a completion directly. Returns true if
  // the handoff succeeded (state was WAITING and is now READY); the caller
  // must then take_resume_handle() and invoke it. Returns false if the state
  // was not WAITING (IDLE/READY); the caller must push_queue(cr) instead.
  bool try_deliver(handoff_cr cr) noexcept {
    uint32_t expected = uint32_t(state::WAITING);
    if (!st_.compare_exchange_strong(expected, uint32_t(state::READY),
                                     std::memory_order_acq_rel)) {
      return false;
    }
    pending_cr_.emplace(cr);
    return true;
  }

  // Coroutine side: take the directly-handed-off completion.  CONTRACT: the
  // caller must have observed state == READY via an acquire load (a failed
  // try_begin_wait CAS, or load_state() == READY) before calling this; that
  // acquire synchronizes with the poll thread's release CAS that sequenced
  // before pending_cr_ was emplaced, making the emplace visible here. Does
  // not change state.
  std::optional<handoff_cr> take_pending() noexcept {
    return pending_cr_;
  }

  // Coroutine side: return to IDLE after consuming pending_cr_ and draining
  // the queue as needed. Returns false if state is not READY.
  bool try_finish() noexcept {
    uint32_t expected = uint32_t(state::READY);
    bool ok = st_.compare_exchange_strong(expected, uint32_t(state::IDLE),
                                          std::memory_order_acq_rel);
    if (ok) pending_cr_.reset();
    return ok;
  }

  // Fallback queue: completions that arrived while IDLE or READY (no direct
  // handoff target). Guarded by q_mtx_; push/pop never block the poll thread.
  void push_queue(handoff_cr cr) {
    std::lock_guard lk(q_mtx_);
    queue_.push_back(std::move(cr));
  }
  std::optional<handoff_cr> pop_queue() {
    std::lock_guard lk(q_mtx_);
    if (queue_.empty()) return std::nullopt;
    handoff_cr cr = std::move(queue_.front());
    queue_.pop_front();
    return cr;
  }
  bool queue_empty() const {
    std::lock_guard lk(q_mtx_);
    return queue_.empty();
  }

 private:
  std::atomic<uint32_t> st_{uint32_t(state::IDLE)};
  std::atomic<void*> resume_ctx_{nullptr};
  std::atomic<resume_fn> resume_fn_{nullptr};
  std::optional<handoff_cr> pending_cr_;
  mutable std::mutex q_mtx_;
  std::deque<handoff_cr> queue_;
};

}  // namespace coro_io::detail

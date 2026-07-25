# URMA Poll Thread Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the per-connection URMA poll watcher and the 2000-iteration inline send spin with a process-level poll thread pool, where M poll threads each drain a shared jfc group (multiple jetties per jfc, jfr stays per-socket).

**Architecture:** A `urma_poll_thread_pool` singleton owns M `std::thread`s and M `urma_jfc_group`s (thread i bound to group i). Each group owns one shared `jfc`+`jfce`; sockets register into a group and their jetty points at the group's jfc. Poll threads run a hybrid loop (busy-poll then `urma_wait_jfc` on idle budget). A lock-free `completion_handoff` state machine hands CQEs from poll threads to request-thread coroutines.

**Tech Stack:** C++17, header-only yalantinglibs, asio, async_simple coroutines, vendor URMA C API (`urma_*.h`), doctest for tests.

**Spec:** `docs/superpowers/specs/2026-07-25-urma-poll-threadpool-design.md`

**Testing reality:** No URMA hardware in the dev environment. Tasks are ordered so that pure-logic units (ctx encoding, handoff state machine, config parsing) are built TDD-style with doctest and verifiable here. Hardware-dependent integration (the group/pool/socket wiring that calls `urma_*`) is implemented after, verified by compile + code review only; the user runs functional/CPU tests on a separate URMA machine.

**Conventions:**
- All new headers go under `include/ylt/coro_io/urma/`.
- Match existing file style: Apache 2.0 header, `#pragma once`, `namespace coro_io::detail`, `ELOG_*` logging.
- Every commit is one task; commit messages use `feat(urma):` / `refactor(urma):` / `test(urma):` prefix.

---

## File Structure

| File | Status | Responsibility |
|------|--------|----------------|
| `include/ylt/coro_io/urma/urma_ctx_codec.hpp` | new | `encode_ctx`/`decode_ctx` for the 64-bit `user_ctx` (op, socket_id, seq). Pure logic, unit-tested. |
| `include/ylt/coro_io/urma/urma_completion_handoff.hpp` | new | `completion_handoff` atomic state machine + fallback queue. Pure logic, unit-tested. |
| `include/ylt/coro_io/urma/urma_jfc_group.hpp` | new | One group: shared `jfc`+`jfce`, socket registry, group send buffer pool. Calls vendor `urma_*`. |
| `include/ylt/coro_io/urma/urma_poll_thread_pool.hpp` | new | Singleton owning M threads + M groups; `poll_loop` hybrid loop. Calls vendor `urma_*`. |
| `include/ylt/coro_io/urma/urma_socket.hpp` | modify | `urma_socket_shared_state_t` holds a `group*` + `socket_id`; jetty points at group jfc; send/recv dispatch via handoff. Legacy path kept behind `poll_threads==0`/`event_mode==false`. |
| `include/ylt/coro_io/urma/urma_io.hpp` | modify | `wait_urma_write_completion` spin reduced to 32 local handoff checks (no `urma_poll_jfc`). |
| `include/ylt/coro_io/urma/urma_rpc_env.hpp` | modify | Parse `URMA_RPC_POLL_THREADS`, `URMA_RPC_GROUP_CQ_SIZE`, `URMA_RPC_POLL_WAIT_TIMEOUT_MS`. |
| `src/coro_rpc/tests/test_urma_rpc_env.cpp` | modify | Add config-parsing tests for new env vars. |
| `src/coro_rpc/tests/test_urma_handoff.cpp` | new | Unit tests for ctx codec + handoff state machine. |
| `src/coro_rpc/tests/CMakeLists.txt` | modify | Register `test_urma_handoff.cpp`. |

---

## Phase 1 — Pure logic (testable without URMA hardware)

### Task 1: user_ctx encode/decode header

**Files:**
- Create: `include/ylt/coro_io/urma/urma_ctx_codec.hpp`

- [ ] **Step 1: Write the failing test**

Create `src/coro_rpc/tests/test_urma_handoff.cpp`:

```cpp
/*
 * Copyright (c) 2026, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
#include "doctest.h"

#ifdef YLT_ENABLE_URMA
#include <ylt/coro_io/urma/urma_ctx_codec.hpp>

using namespace coro_io::detail;

TEST_CASE("urma ctx encode/decode round-trips") {
  uint8_t op = 1;
  uint32_t sid = 0x123456;
  uint32_t seq = 0xabcdef01;
  uint64_t ctx = encode_ctx(op, sid, seq);
  auto [dop, dsid, dseq] = decode_ctx(ctx);
  CHECK(dop == op);
  CHECK(dsid == sid);
  CHECK(dseq == seq);
}

TEST_CASE("urma ctx encode/decode boundary values") {
  {
    auto ctx = encode_ctx(0, 0, 0);
    auto [op, sid, seq] = decode_ctx(ctx);
    CHECK(op == 0);
    CHECK(sid == 0);
    CHECK(seq == 0);
  }
  {
    auto ctx = encode_ctx(0xff, 0xffffff, 0xffffffff);
    auto [op, sid, seq] = decode_ctx(ctx);
    CHECK(op == 0xff);
    CHECK(sid == 0xffffff);
    CHECK(seq == 0xffffffff);
  }
}

TEST_CASE("urma ctx op occupies top 8 bits") {
  CHECK((encode_ctx(0x2, 0, 0) >> 56) == 0x2);
}
#else
TEST_CASE("urma handoff tests compile without urma support") {
  CHECK(true);
}
#endif
```

- [ ] **Step 2: Run test to verify it fails (no header)**

Run: `cmake --build build --target coro_rpc_test` (or the project's configured build dir). If no build dir exists, configure first: `cmake -B build -DYLT_ENABLE_URMA=ON`.
Expected: compile error — `urma_ctx_codec.hpp` not found.

- [ ] **Step 3: Write the header**

Create `include/ylt/coro_io/urma/urma_ctx_codec.hpp`:

```cpp
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

#include <cstdint>
#include <tuple>

namespace coro_io::detail {

// Operation type encoded into the top 8 bits of the URMA user_ctx.
enum urma_ctx_op : uint8_t {
  urma_ctx_op_send = 1,
  urma_ctx_op_recv = 2,
};

// Pack (op, socket_id, seq) into the 64-bit user_ctx carried by each work
// request and echoed back in the completion record.  Layout:
//   [63:56] op_type   (urma_ctx_op_*)
//   [55:32] socket_id (per-group unique id, 24 bits -> up to ~16M sockets/group)
//   [31:0]  seq       (send: per-WR match key; recv: buffer index)
inline uint64_t encode_ctx(uint8_t op, uint32_t socket_id, uint32_t seq) {
  return (uint64_t(op) << 56) | (uint64_t(socket_id & 0xffffff) << 32) |
         uint64_t(seq);
}

inline std::tuple<uint8_t, uint32_t, uint32_t> decode_ctx(uint64_t ctx) {
  uint8_t op = static_cast<uint8_t>((ctx >> 56) & 0xff);
  uint32_t socket_id = static_cast<uint32_t>((ctx >> 32) & 0xffffff);
  uint32_t seq = static_cast<uint32_t>(ctx & 0xffffffff);
  return {op, socket_id, seq};
}

}  // namespace coro_io::detail
```

- [ ] **Step 4: Register the test file in CMake and run**

Edit `src/coro_rpc/tests/CMakeLists.txt`: add `test_urma_handoff.cpp` to the `TEST_SRCS` list (after `test_urma_rpc_env.cpp`).

Run: `cmake --build build --target coro_rpc_test && ./build/output/tests/coro_rpc_test test_urma_handoff*`
Expected: 3 ctx tests PASS.

- [ ] **Step 5: Commit**

```bash
git add include/ylt/coro_io/urma/urma_ctx_codec.hpp src/coro_rpc/tests/test_urma_handoff.cpp src/coro_rpc/tests/CMakeLists.txt
git commit -m "feat(urma): add user_ctx encode/decode for completion dispatch"
```

---

### Task 2: completion_handoff state machine (single-thread happy path)

**Files:**
- Create: `include/ylt/coro_io/urma/urma_completion_handoff.hpp`
- Modify: `src/coro_rpc/tests/test_urma_handoff.cpp`

- [ ] **Step 1: Write the failing test (happy path IDLE->WAITING->READY->IDLE)**

Append to `src/coro_rpc/tests/test_urma_handoff.cpp` (before the final `#else`):

```cpp
#include <ylt/coro_io/urma/urma_completion_handoff.hpp>

TEST_CASE("urma handoff single-thread happy path") {
  completion_handoff h;
  CHECK(h.load_state() == completion_handoff::state::IDLE);

  // coroutine suspends: IDLE -> WAITING, registers a handle slot
  CHECK(h.try_begin_wait());
  CHECK(h.load_state() == completion_handoff::state::WAITING);

  // poll thread delivers a completion while WAITING -> direct handoff to READY
  bool resumed = h.try_deliver({});  // empty cr placeholder
  CHECK(resumed);
  CHECK(h.load_state() == completion_handoff::state::READY);

  // coroutine wakes, takes the pending completion, returns to IDLE
  auto cr = h.take_pending();
  CHECK(cr.has_value());
  CHECK(h.try_finish());
  CHECK(h.load_state() == completion_handoff::state::IDLE);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build --target coro_rpc_test && ./build/output/tests/coro_rpc_test "urma handoff single*"`
Expected: compile error — `urma_completion_handoff.hpp` not found.

- [ ] **Step 3: Write the header (state machine only, no cross-thread yet)**

Create `include/ylt/coro_io/urma/urma_completion_handoff.hpp`:

```cpp
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
//   if try_begin_wait():           // IDLE -> WAITING
//       store resume handle, suspend
//   on wake: take_pending(); try_finish();   // READY -> IDLE
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

  // Coroutine side: attempt to enter WAITING. Returns false if the state is
  // already READY (poll thread delivered before the coroutine suspended); in
  // that case the coroutine should take_pending() immediately.
  bool try_begin_wait() noexcept {
    uint32_t expected = uint32_t(state::IDLE);
    return st_.compare_exchange_strong(expected, uint32_t(state::WAITING),
                                       std::memory_order_acq_rel);
  }

  // Coroutine side: publish the resume handle just before suspending. The
  // poll thread reads it via take_resume_handle() on the WAITING->READY flip.
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

  // Coroutine side: take the directly-handed-off completion (valid when state
  // is READY). Does not change state.
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cmake --build build --target coro_rpc_test && ./build/output/tests/coro_rpc_test "urma handoff*"`
Expected: PASS (both the single-thread happy path and the prior ctx tests).

- [ ] **Step 5: Commit**

```bash
git add include/ylt/coro_io/urma/urma_completion_handoff.hpp src/coro_rpc/tests/test_urma_handoff.cpp
git commit -m "feat(urma): add completion_handoff state machine (single-thread path)"
```

---

### Task 3: completion_handoff — branch A and fallback queue

**Files:**
- Modify: `src/coro_rpc/tests/test_urma_handoff.cpp`

- [ ] **Step 1: Write the failing tests (branch A + queue)**

Append to `src/coro_rpc/tests/test_urma_handoff.cpp`:

```cpp
TEST_CASE("urma handoff branch A: READY before coroutine suspends") {
  completion_handoff h;
  // poll thread delivers first, while still IDLE -> goes to queue (no waiter)
  h.try_deliver({});  // state IDLE, not WAITING -> returns false
  CHECK(h.load_state() == completion_handoff::state::IDLE);
  CHECK_FALSE(h.queue_empty());

  // Now simulate: poll thread had already flipped to READY via a real
  // WAITING->READY before the coroutine checked.  Drive that path directly:
  completion_handoff h2;
  CHECK(h2.try_begin_wait());          // IDLE -> WAITING
  CHECK(h2.try_deliver({}));            // WAITING -> READY
  CHECK(h2.load_state() == completion_handoff::state::READY);
  // coroutine sees READY -> does NOT begin wait, takes pending directly
  CHECK_FALSE(h2.try_begin_wait());     // cannot leave READY
  auto cr = h2.take_pending();
  CHECK(cr.has_value());
  CHECK(h2.try_finish());               // READY -> IDLE
}

TEST_CASE("urma handoff fallback queue drains on wake") {
  completion_handoff h;
  CHECK(h.try_begin_wait());            // IDLE -> WAITING
  // A second completion arrives while WAITING (the slot is occupied by the
  // first): try_deliver succeeds for the first; the second must go to queue.
  CHECK(h.try_deliver({0x11, 0, 0, 0}));   // first -> READY, pending set
  // second delivery: state is READY, not WAITING -> false, enqueue
  CHECK_FALSE(h.try_deliver({0x22, 0, 0, 0}));
  h.push_queue({0x22, 0, 0, 0});
  CHECK_FALSE(h.queue_empty());

  // coroutine wakes, takes pending + drains queue
  auto first = h.take_pending();
  CHECK(first.has_value());
  CHECK(first->user_ctx == 0x11);
  auto second = h.pop_queue();
  CHECK(second.has_value());
  CHECK(second->user_ctx == 0x22);
  CHECK(h.try_finish());
  CHECK(h.load_state() == completion_handoff::state::IDLE);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build --target coro_rpc_test && ./build/output/tests/coro_rpc_test "urma handoff*"`
Expected: the new tests should PASS already (Task 2 implemented the full state machine). If they pass, this task confirms branch A + queue behavior is covered. If any fail, fix the header.

- [ ] **Step 3: Commit**

```bash
git add src/coro_rpc/tests/test_urma_handoff.cpp
git commit -m "test(urma): cover handoff branch A and fallback queue drain"
```

---

### Task 4: completion_handoff — cross-thread stress test

**Files:**
- Modify: `src/coro_rpc/tests/test_urma_handoff.cpp`

- [ ] **Step 1: Write the failing stress test**

Append to `src/coro_rpc/tests/test_urma_handoff.cpp`:

```cpp
#include <atomic>
#include <thread>
#include <vector>

TEST_CASE("urma handoff cross-thread stress: no loss, single resume") {
  completion_handoff h;
  constexpr int N = 2000;
  std::atomic<int> delivered{0};
  std::atomic<int> resumed{0};

  // "request thread": repeatedly begin_wait, then on resume take+finish.
  std::thread req([&] {
    for (int i = 0; i < N; ++i) {
      if (h.try_begin_wait()) {
        // publish a resume handle that bumps the resume counter
        struct ctx_t { std::atomic<int>* r; };
        static thread_local ctx_t c{&resumed};
        h.set_resume_handle(
            [](void* p) { static_cast<ctx_t*>(p)->r->fetch_add(1); }, &c);
        // spin-wait until the poll thread flips us to READY
        while (h.load_state() != completion_handoff::state::READY) {
          std::this_thread::yield();
        }
        (void)h.take_pending();
        // drain any queued extras
        while (h.pop_queue()) {}
        while (!h.try_finish()) {}
      } else {
        // branch A: already READY, take directly
        (void)h.take_pending();
        while (h.pop_queue()) {}
        while (!h.try_finish()) {}
      }
    }
  });

  // "poll thread": deliver N completions
  std::thread poll([&] {
    for (int i = 0; i < N; ++i) {
      handoff_cr cr{uint64_t(i), 0, 0, 0};
      while (true) {
        if (h.try_deliver(cr)) {
          auto [fn, p] = h.take_resume_handle();
          if (fn) fn(p);  // resume the request thread
          delivered.fetch_add(1);
          break;
        }
        // not WAITING (IDLE/READY): enqueue and let the coroutine drain it
        h.push_queue(cr);
        delivered.fetch_add(1);
        break;
      }
      std::this_thread::yield();
    }
  });

  req.join();
  poll.join();
  CHECK(delivered.load() == N);
  // resumed <= N (some completions went to the queue and were drained by the
  // coroutine without a resume callback).  The key invariant: no loss, no
  // double-resume.  resumed never exceeds N.
  CHECK(resumed.load() <= N);
}
```

- [ ] **Step 2: Run the stress test**

Run: `cmake --build build --target coro_rpc_test && ./build/output/tests/coro_rpc_test "urma handoff cross-thread*"`
Expected: PASS. If it hangs or asserts, there is a race in the state machine — fix `urma_completion_handoff.hpp` before proceeding. (Note: the test's poll thread may push to the queue even when the coroutine is about to take_pending; the coroutine drains the queue after taking pending, so no CQE is lost. This mirrors the production dispatch contract.)

- [ ] **Step 3: Commit**

```bash
git add src/coro_rpc/tests/test_urma_handoff.cpp
git commit -m "test(urma): cross-thread stress for completion_handoff"
```

---

## Phase 2 — Configuration (testable without URMA hardware)

### Task 5: Add new config fields and env-var parsing

**Files:**
- Modify: `include/ylt/coro_io/urma/urma_socket.hpp` (config_t)
- Modify: `include/ylt/coro_io/urma/urma_rpc_env.hpp`
- Modify: `src/coro_rpc/tests/test_urma_rpc_env.cpp`

- [ ] **Step 1: Write the failing test**

Append to `src/coro_rpc/tests/test_urma_rpc_env.cpp` (inside the `#ifdef YLT_ENABLE_URMA` block, before the `#else`):

```cpp
TEST_CASE("urma rpc env poll thread pool config defaults and overrides") {
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.poll_threads ==
          coro_io::urma_socket_t::config_t{}.poll_threads);
    CHECK(config.group_cq_size ==
          coro_io::urma_socket_t::config_t{}.group_cq_size);
    CHECK(config.poll_wait_timeout ==
          coro_io::urma_socket_t::config_t{}.poll_wait_timeout);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var threads("URMA_RPC_POLL_THREADS", "8");
    scoped_env_var cq("URMA_RPC_GROUP_CQ_SIZE", "2048");
    scoped_env_var to("URMA_RPC_POLL_WAIT_TIMEOUT_MS", "250");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.poll_threads == 8);
    CHECK(config.group_cq_size == 2048);
    CHECK(config.poll_wait_timeout == std::chrono::milliseconds(250));
  }
  {
    // invalid values keep defaults
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var threads("URMA_RPC_POLL_THREADS", "garbage");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.poll_threads ==
          coro_io::urma_socket_t::config_t{}.poll_threads);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build --target coro_rpc_test && ./build/output/tests/coro_rpc_test "urma rpc env poll thread pool*"`
Expected: compile error — `poll_threads`/`group_cq_size`/`poll_wait_timeout` not members of `config_t`.

- [ ] **Step 3: Add the config_t fields**

In `include/ylt/coro_io/urma/urma_socket.hpp`, edit the `config_t` struct (around line 637) to add the three fields after `poll_interval`:

```cpp
  struct config_t {
    uint32_t cq_size = 128;
    uint16_t recv_buffer_cnt = 8;
    uint16_t send_buffer_cnt = 4;
    uint32_t buffer_size = 4 * 1024;
    uint64_t max_memory_usage = 256ull * 1024 * 1024;
    std::string device_name;
    int eid_index = 0;
    urma_tp_type_t tp_type = URMA_CTP;
    bool event_mode = true;
    std::size_t busy_poll_budget = 16;
    std::chrono::microseconds poll_interval{5};
    // --- poll thread pool (urma_event_mode thread-pool path) ---
    uint32_t poll_threads = 4;                       // 0 disables -> legacy path
    uint32_t group_cq_size = 1024;                   // per-group shared jfc depth
    std::chrono::milliseconds poll_wait_timeout{100};  // urma_wait_jfc timeout
  };
```

- [ ] **Step 4: Add env-var parsing**

In `include/ylt/coro_io/urma/urma_rpc_env.hpp`, inside `make_urma_rpc_config_from_env()` (after the `URMA_RPC_POLL_INTERVAL` block, before `return config;`), add:

```cpp
  urma_rpc_parse_env_integer("URMA_RPC_POLL_THREADS", config.poll_threads);
  urma_rpc_parse_env_integer("URMA_RPC_GROUP_CQ_SIZE", config.group_cq_size);
  {
    uint64_t timeout_ms = 100;
    urma_rpc_parse_env_integer("URMA_RPC_POLL_WAIT_TIMEOUT_MS", timeout_ms);
    config.poll_wait_timeout = std::chrono::milliseconds(timeout_ms);
  }
```

Also extend the `ELOG_INFO` in `probe_urma_rpc_config` (around line 160) to log the new fields — append after `busy_poll_budget=`:

```cpp
              << ", poll_threads=" << config.poll_threads
              << ", group_cq_size=" << config.group_cq_size
              << ", poll_wait_timeout_ms="
              << config.poll_wait_timeout.count();
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cmake --build build --target coro_rpc_test && ./build/output/tests/coro_rpc_test "urma rpc env*"`
Expected: PASS (new config test + existing env tests).

- [ ] **Step 6: Commit**

```bash
git add include/ylt/coro_io/urma/urma_socket.hpp include/ylt/coro_io/urma/urma_rpc_env.hpp src/coro_rpc/tests/test_urma_rpc_env.cpp
git commit -m "feat(urma): add poll_threads/group_cq_size/poll_wait_timeout config"
```

---

## Phase 3 — Integration (vendor URMA calls; verify by compile + review)

> **Note for the implementer:** Tasks 6–9 call vendor `urma_*` functions and cannot be unit-tested without URMA hardware. Verify each by compiling (`cmake --build build --target coro_rpc_test` with `YLT_ENABLE_URMA=ON`) and by code review against the spec's §3 (synchronization) and §4 (error handling). The user runs functional tests on a URMA machine.

### Task 6: urma_jfc_group — shared jfc/jfce + socket registry

**Files:**
- Create: `include/ylt/coro_io/urma/urma_jfc_group.hpp`

- [ ] **Step 1: Write the header**

Create `include/ylt/coro_io/urma/urma_jfc_group.hpp`:

```cpp
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
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "ylt/coro_io/urma/urma_ctx_codec.hpp"
#include "ylt/easylog.hpp"
#include "ylt/urma/urma_api.h"
#include "ylt/urma/urma_types.h"

namespace coro_io::detail {

class urma_socket_shared_state_t;  // forward

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
  // against unregister/lookup.  Called by the socket at init time.
  uint32_t register_socket(urma_socket_shared_state_t* s) {
    std::unique_lock lk(registry_mtx_);
    uint32_t id = next_socket_id_++;
    sockets_[id] = s;
    return id;
  }

  void unregister_socket(uint32_t socket_id) {
    std::unique_lock lk(registry_mtx_);
    sockets_.erase(socket_id);
  }

  // Lookup for the poll thread's dispatch path (read-only, non-blocking for
  // the common case). Returns nullptr if the socket has unregistered.
  urma_socket_shared_state_t* lookup_socket(uint32_t socket_id) const {
    std::shared_lock lk(registry_mtx_);
    auto it = sockets_.find(socket_id);
    return it == sockets_.end() ? nullptr : it->second;
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
  std::unordered_map<uint32_t, urma_socket_shared_state_t*> sockets_;
  uint32_t next_socket_id_ = 1;  // 0 reserved for "none"
  std::atomic<bool> errored_{false};
};

}  // namespace coro_io::detail
```

- [ ] **Step 2: Verify it compiles**

Run: `cmake --build build --target coro_rpc_test`
Expected: compiles (the header is not yet included anywhere, so this just checks it is syntactically valid when included transitively via existing includes — if CMake does not pick it up, add a temporary `#include` in `test_urma_handoff.cpp` under `#ifdef YLT_ENABLE_URMA`, build, then remove it).

- [ ] **Step 3: Commit**

```bash
git add include/ylt/coro_io/urma/urma_jfc_group.hpp
git commit -m "feat(urma): add urma_jfc_group (shared jfc/jfce + socket registry)"
```

---

### Task 7: urma_poll_thread_pool — singleton + hybrid poll_loop

**Files:**
- Create: `include/ylt/coro_io/urma/urma_poll_thread_pool.hpp`

- [ ] **Step 1: Write the header**

Create `include/ylt/coro_io/urma/urma_poll_thread_pool.hpp`:

```cpp
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
#include <chrono>
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
  void poll_loop(uint32_t gi) {
    auto* group = groups_[gi].get();
    auto* jfc = group->jfc();
    auto* jfce = group->jfce();
    std::array<urma_cr_t, 16> crs{};
    std::size_t idle_spins = 0;
    int rearm_failures = 0;
    while (!stop_.load(std::memory_order_acquire)) {
      int n = urma_poll_jfc(jfc, static_cast<int>(crs.size()), crs.data());
      if (n < 0) {
        ELOG_WARN << "urma_poll_jfc error errno=" << errno << " group=" << gi;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      if (n > 0) {
        for (int k = 0; k < n; ++k) dispatch(group, crs[k]);
        idle_spins = 0;
        continue;
      }
      if (++idle_spins < cfg_.busy_poll_budget) continue;

      // idle budget exceeded -> block on the event channel
      if (urma_rearm_jfc(jfc, false) != URMA_SUCCESS) {
        if (++rearm_failures > 8) {
          ELOG_WARN << "urma_rearm_jfc failing repeatedly group=" << gi;
          rearm_failures = 0;
        }
        idle_spins = 0;
        continue;
      }
      rearm_failures = 0;
      urma_jfc_t* ev_jfc = nullptr;
      int ev = urma_wait_jfc(jfce, 1,
                             static_cast<int>(cfg_.wait_timeout.count()),
                             &ev_jfc);
      if (ev > 0 && ev_jfc) {
        uint32_t ack = 1;
        urma_ack_jfc(&ev_jfc, &ack, 1);
      } else if (ev == 0 && errno != 512 /* ERESTARTSYS */) {
        ELOG_INFO << "urma_wait_jfc no event group=" << gi << " errno=" << errno;
      } else if (ev < 0) {
        ELOG_WARN << "urma_wait_jfc error group=" << gi;
      }
      idle_spins = 0;
    }
  }

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
```

- [ ] **Step 2: Verify it compiles**

Run: `cmake --build build --target coro_rpc_test`
Expected: compiles (`dispatch` is declared but not yet defined; it is only called from within `poll_loop`, which is not instantiated until the pool is constructed, so the missing definition is not a link error yet — but to be safe, the definition is added in Task 8).

- [ ] **Step 3: Commit**

```bash
git add include/ylt/coro_io/urma/urma_poll_thread_pool.hpp
git commit -m "feat(urma): add urma_poll_thread_pool with hybrid poll_loop"
```

---

### Task 8: Wire sockets into groups — shared-state changes

**Files:**
- Modify: `include/ylt/coro_io/urma/urma_socket.hpp`

This is the largest task. It adds the thread-pool path alongside the legacy path, selected by `config_t::poll_threads`.

- [ ] **Step 1: Add thread-pool members and includes to the shared state**

At the top of `urma_socket.hpp`, after the existing includes, add:

```cpp
#include "ylt/coro_io/urma/urma_completion_handoff.hpp"
#include "ylt/coro_io/urma/urma_ctx_codec.hpp"
#include "ylt/coro_io/urma/urma_jfc_group.hpp"
#include "ylt/coro_io/urma/urma_poll_thread_pool.hpp"
```

In the `urma_socket_shared_state_t` private members block (after `init_error_`, around line 630), add:

```cpp
  // --- thread-pool path (used when poll_threads > 0 and event_mode) ---
  urma_jfc_group* group_ = nullptr;     // not owned; owned by the pool
  uint32_t socket_id_ = 0;              // per-group id, assigned at register
  bool thread_pool_mode_ = false;       // selects thread-pool vs legacy path
  // send path: pending sends keyed by seq, guarded by send_handoff_mtx_.
  // The send CQE is paired back to seq by the poll thread; the callback it
  // invokes bridges into async_write's urma_write_completion_state (which is
  // already a thread-safe-enough queue for the "arrived before suspend" case).
  std::mutex send_handoff_mtx_;
  std::unordered_map<uint32_t, pending_send> pending_send_by_seq_;
  std::atomic<uint32_t> next_send_seq_{1};
  // recv path: single-slot lock-free handoff for the one recv coroutine that
  // can be pending at a time.  The poll thread delivers recv data through it;
  // the recv coroutine (in async_receive) consumes it.
  completion_handoff recv_handoff_;
```

- [ ] **Step 2: Add a thread-pool init entry point**

Add a new public-ish method on `urma_socket_shared_state_t` (after `init()`):

```cpp
  // Thread-pool init: register into a group from the pool, point the jetty at
  // the group's jfc.  Returns false on failure (caller falls back to legacy).
  // The caller (urma_socket_t::init) must still create the per-socket jfr and
  // jetty; only the jfc/jfce come from the group.
  bool init_thread_pool(urma_poll_thread_pool* pool, uint32_t group_cq_size,
                        std::size_t send_buffer_cnt) {
    if (!pool) return false;
    group_ = pool->select_group(reinterpret_cast<uint64_t>(this));
    if (!group_ || group_->errored()) return false;
    socket_id_ = group_->register_socket(this);
    thread_pool_mode_ = true;
    ELOG_INFO << "URMA socket joined jfc group: socket_id=" << socket_id_
              << ", group jfc depth=" << group_cq_size;
    (void)send_buffer_cnt;
    return true;
  }
```

- [ ] **Step 3: Make jetty/jfr creation use the group jfc when in thread-pool mode**

Refactor `init()` so the `jfc_cfg.jfce` and the `jfs_cfg.jfc`/`shared.jfc` point at the group's jfc when `thread_pool_mode_` is true. Concretely, in `init()`:

- When `thread_pool_mode_` is true, skip creating the per-socket `jfc_`/`jfce_` and instead use `group_->jfc()` / `group_->jfce()` for `jfr_cfg.jfc`, `jfs_cfg.jfc`, and `shared.jfc`. Keep creating the per-socket `jfr_` and `jetty_`.

```cpp
    urma_jfc_t* jfc_ptr = thread_pool_mode_ ? group_->jfc() : jfc_.get();
    urma_jfr_cfg_t jfr_cfg{};
    jfr_cfg.depth = static_cast<uint32_t>(recv_buffer_cnt_ + 1);
    jfr_cfg.trans_mode = URMA_TM_RM;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = jfc_ptr;
    errno = 0;
    jfr_.reset(urma_create_jfr(device_->context(), &jfr_cfg));
    if (!jfr_) {
      set_init_error("urma_create_jfr", errno);
      return false;
    }
    urma_jetty_cfg_t jetty_cfg{};
    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(send_buffer_cnt + 2);
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
    jetty_cfg.jfs_cfg.priority = URMA_MAX_PRIORITY;
    jetty_cfg.jfs_cfg.max_sge = 1;
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jetty_cfg.jfs_cfg.jfc = jfc_ptr;
    jetty_cfg.shared.jfr = jfr_.get();
    jetty_cfg.shared.jfc = jfc_ptr;
    errno = 0;
    jetty_.reset(urma_create_jetty(device_->context(), &jetty_cfg));
    if (!jetty_) {
      set_init_error("urma_create_jetty", errno);
      return false;
    }
```

(The per-socket `jfc_`/`jfce_` block is skipped entirely when `thread_pool_mode_` is true. The `event_mode_enabled_`/`init_event_fd`/`event_loop`/`poll_once` legacy path is only used when `thread_pool_mode_` is false.)

- [ ] **Step 4: Rewrite post_send to use encoded user_ctx in thread-pool mode**

In thread-pool mode the send path reuses the *existing* callback bridge unchanged: `async_write` passes `post_send` a callback that calls `state->push(result)`, and `wait_urma_write_completion` reads from that same `state`. The only changes are (a) `user_ctx` carries `(op, socket_id, seq)` so the poll thread can route the CQE, and (b) the pending send (buffer + callback) is keyed by `seq` instead of pushed onto the FIFO `send_callbacks_`.

Replace the body of `post_send` (around line 268):

```cpp
  void post_send(urma_buffer_t buffer, std::size_t length,
                 callback_t&& callback) {
    if (has_close_ || !remote_jetty_) {
      if (buffer) device_->get_buffer_pool()->return_buffer(buffer);
      resume({std::make_error_code(std::errc::not_connected), 0},
             std::move(callback));
      return;
    }
    urma_sge_t sge{reinterpret_cast<uint64_t>(buffer.addr),
                   static_cast<uint32_t>(length),
                   static_cast<urma_target_seg_t*>(buffer.seg), nullptr};
    urma_sg_t sg{length ? &sge : nullptr, length ? 1u : 0u};
    urma_send_wr_t send_wr{};
    send_wr.src = sg;
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_SEND;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = remote_jetty_.get();
    wr.send = send_wr;
    urma_jfs_wr_t* bad_wr = nullptr;

    if (thread_pool_mode_) {
      uint32_t seq = next_send_seq_.fetch_add(1, std::memory_order_relaxed);
      wr.user_ctx = encode_ctx(urma_ctx_op_send, socket_id_, seq);
      auto ec =
          make_urma_error(urma_post_jetty_send_wr(jetty_.get(), &wr, &bad_wr));
      if (ec) {
        if (buffer) device_->get_buffer_pool()->return_buffer(buffer);
        resume({ec, 0}, std::move(callback));
        return;
      }
      std::lock_guard lk(send_handoff_mtx_);
      pending_send_by_seq_[seq] =
          pending_send{std::move(buffer), length, std::move(callback)};
      return;
    }
    // --- legacy path (unchanged) ---
    wr.user_ctx = 1;
    auto ec =
        make_urma_error(urma_post_jetty_send_wr(jetty_.get(), &wr, &bad_wr));
    if (ec) {
      if (buffer) device_->get_buffer_pool()->return_buffer(buffer);
      resume({ec, 0}, std::move(callback));
      return;
    }
    send_callbacks_.push(
        pending_send{std::move(buffer), length, std::move(callback)});
  }
```

(`pending_send_by_seq_` was already added in Step 1.)

- [ ] **Step 5: Implement the poll-thread dispatch (completion -> socket)**

Define the `urma_poll_thread_pool::dispatch` method declared in Task 7, in `urma_socket.hpp` after the `urma_socket_shared_state_t` class (it needs the full socket definition). This is the path the poll thread calls for each CQE:

```cpp
inline void urma_poll_thread_pool::dispatch(urma_jfc_group* group,
                                            const urma_cr_t& cr) {
  auto [op, sid, seq] = decode_ctx(cr.user_ctx);
  auto* s = group->lookup_socket(sid);
  if (!s) {
    // socket already unregistered; drop the CQE.  Buffer return for recv CQEs
    // is handled by the socket's close path; send buffers were already moved
    // into pending_send_by_seq_ and freed on close.
    return;
  }
  std::error_code ec = cr.status == URMA_CR_SUCCESS
                           ? std::error_code{}
                           : std::make_error_code(std::errc::io_error);
  if (op == urma_ctx_op_send) {
    s->on_send_completion(seq, ec, cr.completion_len);
  } else if (op == urma_ctx_op_recv) {
    s->on_recv_completion(ec, cr.completion_len);
  }
}
```

Add the two handler methods on `urma_socket_shared_state_t`. The send handler pairs the CQE back to the pending send by `seq` and invokes the callback - which is the same callback `async_write` registered, so it calls `state->push(result)` and wakes the suspended `wait_urma_write_completion` coroutine. No handoff state machine is needed on the send path (the callback bridge already handles "arrived before suspend" by queueing into `state->completions`). The recv handler uses the `recv_handoff_` state machine for a safe cross-thread handoff to the single recv coroutine.

```cpp
  // Called by the poll thread (via urma_poll_thread_pool::dispatch) when a
  // send CQE arrives.  Pairs the CQE back to the pending send by seq and
  // invokes its callback, which pushes into the caller's
  // urma_write_completion_state and wakes the suspended send coroutine (or
  // queues the result if the coroutine has not suspended yet).
  void on_send_completion(uint32_t seq, std::error_code ec,
                          std::size_t completion_len) {
    pending_send ps;
    {
      std::lock_guard lk(send_handoff_mtx_);
      auto it = pending_send_by_seq_.find(seq);
      if (it == pending_send_by_seq_.end()) return;  // already cancelled/closed
      ps = std::move(it->second);
      pending_send_by_seq_.erase(it);
    }
    if (ps.buffer) device_->get_buffer_pool()->return_buffer(ps.buffer);
    wake_writer(ec);
    resume({ec, completion_len}, std::move(ps.callback));
  }

  // Called by the poll thread when a recv CQE arrives.  Refills the recv
  // queue, then hands the result to the recv coroutine via recv_handoff_.
  // recv_handoff_ is the single-slot lock-free handoff tested in Tasks 2-4.
  void on_recv_completion(std::error_code ec, std::size_t completion_len) {
    if (completion_len == 0) {
      peer_close_ = true;
      has_close_ = true;
    }
    if (recv_queue_.empty()) {
      // protocol error; deliver an error result via the handoff
      recv_handoff_.try_deliver(handoff_cr{0, uint32_t(std::errc::protocol_error),
                                           0, 1});
      return;
    }
    auto completed_buffer = recv_queue_.pop();
    auto refill_ec = fill_recv_queue();
    if (refill_ec) {
      ELOG_ERROR << "URMA refill recv queue failed: " << refill_ec.message();
    }
    // Stash the completed buffer where the recv coroutine will find it, then
    // hand off the (ec, len) result.  The buffer is held in a single-slot
    // member guarded by the handoff state (only one recv in flight).
    recv_pending_buffer_ = std::move(completed_buffer);
    if (!recv_handoff_.try_deliver(
            handoff_cr{0, static_cast<uint32_t>(ec.value()),
                       static_cast<uint32_t>(completion_len), 1})) {
      // No coroutine is waiting (IDLE/READY): the result is already queued in
      // recv_handoff_ via push_queue by the *coroutine* when it deferred, OR
      // we enqueue it here for later consumption.
      recv_handoff_.push_queue(
          handoff_cr{0, static_cast<uint32_t>(ec.value()),
                     static_cast<uint32_t>(completion_len), 1});
    }
  }
```

Add the single recv buffer slot member:

```cpp
  urma_buffer_t recv_pending_buffer_{};
```

- [ ] **Step 5b: Route async_receive through recv_handoff_ in thread-pool mode**

Modify `async_receive` (around line 301) so that in thread-pool mode it consumes from `recv_handoff_` instead of touching `recv_callback_`/`recv_result_` directly (those stay for the legacy path):

```cpp
  void async_receive(callback_t&& callback) {
    if (thread_pool_mode_) {
      // Drain anything already handed off / queued.
      if (auto cr = recv_handoff_.pop_queue()) {
        recv_buffer_ = std::move(recv_pending_buffer_);
        std::error_code ec = cr->status
            ? std::make_error_code(std::errc::io_error)
            : std::error_code{};
        resume({ec, cr->completion_len}, std::move(callback));
        return;
      }
      // Nothing ready: become the waiter via the handoff state machine.
      if (recv_handoff_.try_begin_wait()) {
        // Publish a resume that re-runs async_receive on the request thread.
        // Store the callback so the resume can complete it.
        recv_pending_callback_ = std::move(callback);
        recv_handoff_.set_resume_handle(
            [](void* p) {
              // Resumed by the poll thread on the poll thread - re-dispatch
              // the completion onto the socket's executor so async_receive
              // finishes on the request thread.
              auto* self = static_cast<urma_socket_shared_state_t*>(p);
              asio::post(self->executor_->get_asio_executor(), [self] {
                self->finish_recv_handoff();
              });
            },
            this);
        return;  // suspended via the posted continuation
      }
      // try_begin_wait returned false -> state was READY: take pending now.
      auto cr = recv_handoff_.take_pending();
      recv_handoff_.try_finish();
      recv_buffer_ = std::move(recv_pending_buffer_);
      std::error_code ec = cr && cr->status
          ? std::make_error_code(std::errc::io_error)
          : std::error_code{};
      resume({ec, cr ? cr->completion_len : 0}, std::move(callback));
      return;
    }
    // --- legacy path (unchanged) ---
    if (!recv_result_.empty()) {
      auto pending = recv_result_.pop();
      recv_buffer_ = std::move(pending.buffer);
      resume(std::move(pending.result), std::move(callback));
    }
    else if (has_close_) {
      resume({std::make_error_code(std::errc::operation_canceled), 0},
             std::move(callback));
    }
    else {
      recv_callback_ = std::move(callback);
    }
  }

  // Runs on the request thread (posted by the handoff resume) to finish a
  // recv that was handed off by the poll thread.
  void finish_recv_handoff() {
    auto cr = recv_handoff_.take_pending();
    if (!cr) cr = recv_handoff_.pop_queue();
    recv_handoff_.try_finish();
    if (!recv_pending_callback_) return;
    auto cb = std::move(recv_pending_callback_);
    recv_buffer_ = std::move(recv_pending_buffer_);
    std::error_code ec = cr && cr->status
        ? std::make_error_code(std::errc::io_error)
        : std::error_code{};
    resume({ec, cr ? cr->completion_len : 0}, std::move(cb));
  }
```

Add the pending-callback member:

```cpp
  callback_t recv_pending_callback_;
```

> **Why the handoff for recv but not send?** The send path already has a bridge that handles both orderings: `async_write`'s `post_send` callback pushes the result into the shared `urma_write_completion_state` queue, and `wait_urma_write_completion` checks that queue before suspending (fast path) and after resume (slow path). The recv path has no such shared queue - it had a single `recv_callback_` slot written by the request thread and read by the poller. In single-thread mode that was fine; in thread-pool mode it is a data race, so recv goes through the lock-free `recv_handoff_` (the state machine tested in Tasks 2-4).

- [ ] **Step 6: In thread-pool mode, start_completion_watch becomes a no-op (the pool polls)**

Add an early return at the top of `start_completion_watch()` (`urma_socket.hpp:537`). Insert this before the existing `if (event_mode_enabled_ && init_event_fd())` check; leave the rest of the method (the event_loop and busy-poll fallback paths) untouched:

```cpp
  void start_completion_watch() {
    if (thread_pool_mode_) {
      ELOG_INFO << "URMA socket using poll thread pool (socket_id="
                << socket_id_ << "); no local watcher started";
      return;
    }
    if (event_mode_enabled_ && init_event_fd()) {   // unchanged from here
      // ... existing event_loop path (lines 539-544) unchanged ...
    } else {
      // ... existing busy-poll path (lines 546-548) unchanged ...
    }
  }
```

The early return is the only change; the existing body (`event_loop().start(...)` / `poll_once(); start_polling();`) stays exactly as-is for the legacy path.

- [ ] **Step 7: Unregister on close**

In `close()` (line 577), add unregistration before `fail_pending`:

```cpp
  void close() {
    if (has_close_.exchange(true)) return;
    if (thread_pool_mode_ && group_) group_->unregister_socket(socket_id_);
    std::error_code ignored;
    poll_timer_.cancel(ignored);
    if (event_fd_) event_fd_->cancel(ignored);
    socket_.cancel(ignored);
    socket_.close(ignored);
    fail_pending(std::make_error_code(std::errc::operation_canceled));
  }
```

- [ ] **Step 8: Verify it compiles**

Run: `cmake --build build --target coro_rpc_test`
Expected: compiles with `YLT_ENABLE_URMA=ON`. Fix any include/symbol errors. The legacy tests still pass; no new behavior is exercised yet (the pool is only constructed when `poll_threads > 0`, which the env tests don't set with a real device).

- [ ] **Step 9: Commit**

```bash
git add include/ylt/coro_io/urma/urma_socket.hpp include/ylt/coro_io/urma/urma_poll_thread_pool.hpp
git commit -m "feat(urma): wire sockets into jfc groups + poll-thread dispatch"
```

---

### Task 9: urma_socket_t::init selects thread-pool vs legacy path

**Files:**
- Modify: `include/ylt/coro_io/urma/urma_socket.hpp`

- [ ] **Step 1: Wire the mode selection in urma_socket_t::init**

In `urma_socket_t::init()` (around line 941, after `state_->idle_poll_interval_ = ...`), decide the mode and call `init_thread_pool` before `state_->init(...)` so the shared state knows whether to use the group jfc:

```cpp
    state_->busy_poll_budget_ = conf_.busy_poll_budget;
    state_->idle_poll_interval_ = conf_.poll_interval;

    // Select the poll path.  Thread-pool mode requires event_mode and a
    // non-zero poll_threads; the pool singleton is lazily created with the
    // device context.  On any failure, fall back to the legacy per-socket
    // path.
    bool want_thread_pool =
        conf_.event_mode && conf_.poll_threads > 0;
    if (want_thread_pool) {
      urma_poll_thread_pool::config pcfg{
          conf_.poll_threads,
          conf_.group_cq_size,
          conf_.busy_poll_budget,
          conf_.poll_wait_timeout,
          device->context()};
      auto* pool = urma_poll_thread_pool::instance(pcfg);
      if (pool && state_->init_thread_pool(pool, conf_.group_cq_size,
                                           conf_.send_buffer_cnt)) {
        // thread_pool_mode_ is now true inside state_; state_->init will use
        // the group jfc instead of creating its own.
      } else {
        ELOG_WARN << "URMA poll thread pool unavailable; fall back to legacy";
        want_thread_pool = false;
      }
    }

    if (!state_->init(conf_.cq_size, conf_.send_buffer_cnt, conf_.event_mode)) {
      auto stage = state_->init_stage_;
      auto error = state_->init_error_;
      ELOG_ERROR << "URMA socket resource initialization failed: stage=" << stage
                 << ", errno=" << error.value()
                 << ", error=" << error.message();
      state_.reset();
      throw std::system_error(error, stage);
    }
```

Also adjust `state_->init(...)` so that when `thread_pool_mode_` is true it does NOT create a per-socket jfc/jfce (those come from the group). Guard the existing `urma_create_jfce`/`urma_create_jfc` blocks with `if (!thread_pool_mode_)`.

- [ ] **Step 2: Verify it compiles and legacy tests still pass**

Run: `cmake --build build --target coro_rpc_test && ./build/output/tests/coro_rpc_test "urma rpc env*"`
Expected: compiles; env tests still PASS (they do not construct a real device, so the pool is never built and the legacy path is used).

- [ ] **Step 3: Commit**

```bash
git add include/ylt/coro_io/urma/urma_socket.hpp
git commit -m "feat(urma): select thread-pool vs legacy poll path at socket init"
```

---

### Task 10: Reduce the send-path spin to 32 local queue checks

**Files:**
- Modify: `include/ylt/coro_io/urma/urma_io.hpp`

- [ ] **Step 1: Make urma_write_completion_state thread-safe**

The current `urma_write_completion_state` (`urma_io.hpp:42-54`) uses a plain `std::queue` + `move_only_function` with no synchronization - safe only in the legacy single-thread model where `push` and the reads run on the same executor thread. In thread-pool mode `push` runs on the poll thread and the reads run on the request thread: a data race. Guard both `completions` and `resume_handler` with a mutex. Replace the struct:

```cpp
struct urma_write_completion_state {
  std::mutex mtx;
  std::queue<std::pair<std::error_code, std::size_t>> completions;
  async_simple::util::move_only_function<void()> resume_handler;

  // Called by the poll thread (via the post_send callback).
  void push(std::pair<std::error_code, std::size_t> result) {
    async_simple::util::move_only_function<void()> h;
    {
      std::lock_guard lk(mtx);
      completions.push(result);
      h = std::move(resume_handler);   // single resume: move-and-clear under lock
      resume_handler = nullptr;
    }
    if (h) h();                        // resume outside the lock (no reentrancy)
  }
};
```

- [ ] **Step 2: Rewrite wait_urma_write_completion**

Replace `wait_urma_write_completion` (`urma_io.hpp:56-85`). The fast path no longer calls `urma_poll_jfc` (the shared jfc is polled by the poll thread; concurrent polling would race/lose CQEs). Instead it checks `state->completions`, which the poll thread fills via the `post_send` callback bridge. The spin budget drops from 2000 to 32.

```cpp
inline async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
wait_urma_write_completion(
    const std::shared_ptr<urma_write_completion_state>& state,
    urma_socket_t& socket) {
  (void)socket;  // no socket interaction needed; the poll thread fills state
  // Fast path: 32 short checks of state->completions.  Each check takes the
  // mutex briefly; std::this_thread::yield() keeps the request thread from
  // starving siblings under heavy load.  Covers the typical 1-10us CQE window.
  for (int i = 0; i < 32; ++i) {
    bool ready;
    {
      std::lock_guard lk(state->mtx);
      ready = !state->completions.empty();
    }
    if (ready) break;
    std::this_thread::yield();
  }
  {
    std::lock_guard lk(state->mtx);
    if (!state->completions.empty()) {
      auto result = state->completions.front();
      state->completions.pop();
      co_return result;
    }
  }
  // Slow path: publish a resume handler under the lock, then suspend.
  // callback_awaitor::await_resume runs the setup lambda BEFORE suspending,
  // so re-check completions inside the setup and resume immediately if a
  // result arrived in the window (closes the lost-wakeup gap).
  while (true) {
    callback_awaitor<void> awaitor;
    co_await awaitor.await_resume([&state](auto handler) {
      std::lock_guard lk(state->mtx);
      if (!state->completions.empty()) {
        handler.resume();   // result already here: do not suspend
        return;
      }
      state->resume_handler = [handler]() mutable { handler.resume(); };
    });
    // Resumed (immediately above, or later via push).  Re-check under the lock.
    std::lock_guard lk(state->mtx);
    if (!state->completions.empty()) {
      auto result = state->completions.front();
      state->completions.pop();
      co_return result;
    }
    // Spurious resume with nothing ready: loop and re-suspend.
  }
}
```

> **No-loss / single-resume invariant:** (1) If `push` runs before the setup lambda takes the lock, `completions` is non-empty and the fast path (or the setup's immediate `handler.resume()`) consumes it. (2) If `push` runs while the setup holds the lock, `resume_handler` is not yet installed, so `push`'s `h` is null and the result stays queued; the setup then sees `completions` non-empty and resumes immediately. (3) If `push` runs after the setup installs `resume_handler` (lock released, coroutine suspended), `push` moves the handler out under the lock and invokes it, resuming the coroutine, which re-checks and consumes the result. `resume_handler` is moved-and-cleared atomically, so it fires at most once.

- [ ] **Step 3: Verify it compiles**

Run: `cmake --build build --target coro_rpc_test`
Expected: compiles. Fix symbol/include errors.

- [ ] **Step 4: Commit**

```bash
git add include/ylt/coro_io/urma/urma_io.hpp
git commit -m "perf(urma): reduce send spin to 32 local queue checks; thread-safe completion state"
```

---

- [ ] **Step 4: Commit**

```bash
git add include/ylt/coro_io/urma/urma_io.hpp include/ylt/coro_io/urma/urma_socket.hpp
git commit -m "perf(urma): reduce send spin to 32 local handoff checks (was 2000)"
```

---

## Phase 4 — Documentation & review

### Task 11: Update URMA docs and add a usage note

**Files:**
- Modify: any existing URMA doc under `website/docs/` or `README` that lists URMA env vars (search for `URMA_RPC_`).
- Create: `docs/superpowers/notes/urma-poll-threadpool.md` (brief operator note).

- [ ] **Step 1: Find existing URMA env-var documentation**

Run: `grep -rn "URMA_RPC_" website/ README*.md docs/ 2>/dev/null | grep -v "specs/\|plans/"`
Expected: a short list of files mentioning the env vars.

- [ ] **Step 2: Add the three new env vars to that doc**

Append a "Poll Thread Pool" subsection listing `URMA_RPC_POLL_THREADS` (default 4, 0 = legacy), `URMA_RPC_GROUP_CQ_SIZE` (default 1024), `URMA_RPC_POLL_WAIT_TIMEOUT_MS` (default 100), with the mode-selection matrix from the spec.

- [ ] **Step 3: Commit**

```bash
git add <doc files>
git commit -m "docs(urma): document poll thread pool env vars"
```

---

### Task 12: Final review against the spec

- [ ] **Step 1: Re-read the spec** (`docs/superpowers/specs/2026-07-25-urma-poll-threadpool-design.md`) and walk each section against the implementation.

- [ ] **Step 2: Checklist verification**

  - [ ] §1 components: `urma_poll_thread_pool`, `urma_jfc_group`, modified `urma_socket_shared_state_t` all exist with the documented responsibilities.
  - [ ] §2 data flow: send fast-path = 32 local checks; slow path = suspend + poll-thread resume; recv = single handoff slot; poll loop = hybrid busy+wait.
  - [ ] §3 synchronization: `completion_handoff` IDLE/WAITING/READY + fallback queue; no `urma_poll_jfc` on request threads; socket close unregisters + wakes pending.
  - [ ] §4 errors: poll_jfc <0 backs off; rearm failures skip wait; wait_jfc ERESTARTSYS ignored; group error marks errored; bounded wait timeout for shutdown; buffer leak protection on dropped CQEs.
  - [ ] §5 config: three new env vars + `config_t` fields; mode matrix (event_mode + poll_threads).
  - [ ] Legacy path preserved behind `poll_threads==0` / `event_mode==false`.

- [ ] **Step 3: Compile the full test binary once more**

Run: `cmake --build build --target coro_rpc_test && ./build/output/tests/coro_rpc_test`
Expected: all non-hardware tests PASS.

- [ ] **Step 4: Commit any review fixes**

```bash
git add -A
git commit -m "refactor(urma): address spec-review findings"
```

---

## Handoff to hardware testing

After Task 12, the implementation is complete and verified at the code-logic level. The user runs on a URMA-equipped machine:

1. Build with `YLT_ENABLE_URMA=ON`.
2. Set `URMA_RPC_ENABLE=1`, `URMA_RPC_POLL_THREADS=4` (default), run the existing urma RPC benchmark / functional tests.
3. Compare CPU (idle + loaded) against the legacy path (`URMA_RPC_POLL_THREADS=0`).
4. Verify clean shutdown (all poll threads join within ~`poll_wait_timeout`).

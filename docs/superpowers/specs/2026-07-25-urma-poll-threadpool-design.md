# URMA Poll Thread Pool Design

Date: 2026-07-25

## Context

The current URMA RPC implementation in yalantinglibs (branch `urma_event_mode`) has two CPU-hot paths that scale poorly with connection count:

1. **Per-connection poll watcher.** Each `urma_socket_t` creates its own `jfc` + `jfce` + `jfr` + `jetty` and runs a per-socket `event_loop` coroutine (or, in fallback mode, a `poll_once` / `start_polling` timer chain). N connections means N watcher coroutines and N copies of CQ resources. There is no shared completion queue.

2. **Inline 2000-iteration send spin.** Every send spins up to 2000 `urma_poll_jfc` calls on the calling thread inside `wait_urma_write_completion` (`include/ylt/coro_io/urma/urma_io.hpp:56-85`). This was cranked up over the commit history (`198fcf7` -> `20ae445` -> `b2a8294`) because the per-socket JFCE event-loop wakeup latency (50-300us) is too slow to wait on for sends whose CQEs arrive in 1-10us. The 2000-iter spin is the dominant CPU consumer and scales linearly with request rate.

The vendor URMA API already supports the missing model: `urma_wait_jfc(jfce, jfc_cnt, ...)` waits on a `jfce` for any of an array of `jfc`s to fire, `urma_ack_jfc(jfc[], nevents[], jfc_cnt)` acknowledges them, and `urma_jetty_cfg_t.shared.jfc` lets multiple jetties feed one shared completion queue. Today `urma_socket_shared_state_t` (`include/ylt/coro_io/urma/urma_socket.hpp:608-614`) owns `jfc_`/`jfce_` per socket, so the N:1 sharing is not exercised.

## Goal

Replace the per-connection poll watcher and the inline send spin with a **process-level poll thread pool**:

- M poll threads (default 4, `URMA_RPC_POLL_THREADS` configurable), each bound to one **jfc group**.
- Each group owns one shared `jfc` + `jfce`; multiple jetties (one per socket) feed that `jfc`. `jfr` stays per-socket (isolate recv resources).
- Poll threads run a **hybrid loop**: busy-poll drain the shared `jfc` for low latency, and switch to blocking `urma_wait_jfc` after an idle-spin budget to drop idle CPU to ~0.
- Send coroutines drop the 2000-iter spin to a short local check of the handoff state, then suspend and are resumed by the poll thread.

This removes both the per-connection watcher cost and the dominant send-spin CPU cost while keeping send latency low.

## Non-goals

- Do not share `jfr` across jetties (recv resources stay per-socket).
- Do not remove the legacy per-socket `event_loop` / `poll_once` paths; keep them as a fallback when `URMA_RPC_POLL_THREADS=0` or `URMA_RPC_EVENT_MODE=false`.
- Do not change the TCP/SSL RPC paths or the IBV/RDMA paths.
- Do not introduce programmatic per-connection config overrides at the RPC layer; configuration stays env-var driven.
- Do not require Mooncake or other consumers to change source.

## Architecture (Approach A)

### Component overview

Three components under `include/ylt/coro_io/urma/`:

| Component | Status | Responsibility |
|-----------|--------|----------------|
| `urma_poll_thread_pool.hpp` | new | Process-level singleton; owns M `std::thread`s and M `urma_jfc_group`s; thread i is bound to group i. |
| `urma_jfc_group.hpp` | new | One group = shared `jfc` + `jfce`; holds the socket registry (`socket_id` -> `urma_socket_shared_state_t*`); owns the group-level send buffer pool. |
| `urma_socket.hpp` | changed | Per-socket no longer creates `jfc`/`jfce`; holds a `urma_jfc_group*` reference and a `socket_id`; jetty points at the group's `jfc`. |

### Topology

```text
                        +-------------------------------------+
                        |     urma_poll_thread_pool (singleton)|
                        |   M threads (default 4, URMA config)|
                        +----------+----------+----------+----+
                        | thread 0 | thread 1 | thread 2 |... |
                        +----+-----+----+-----+----+-----+----+
                             |          |          |
                         +---+---+  +---+---+  +---+---+
                         |group 0|  |group 1|  |group 2|  (jfc + jfce per group)
                         | jfc0  |  | jfc1  |  | jfc2  |
                         | jfce0 |  | jfce1 |  | jfce2 |
                         +---+---+  +---+---+  +---+---+
                sockets---+----------+----------+--...
                             |          |
                      +------+--+ +-----+----+
                      | socket A| | socket C |  (per-socket: jetty + jfr)
                      | socket B| | socket D |  (jfs/jfr -> group jfc)
                      +---------+ +----------+
```

- thread i runs `poll_loop(group_i)`; thread and group are 1:1 bound (good thread affinity, no cross-thread callbacks within a group).
- Connection assignment: `socket_id = group.register_socket(this)`; group chosen by `hash(peer) % M` or round-robin for load balance.
- Each socket's `jetty_cfg.shared.jfc = group->jfc()`, `jfs_cfg.jfc = group->jfc()`; `jfr` is created per-socket as today.

### Why Approach A over alternatives

- **Approach B (global jfce + work stealing):** all groups share one jfce; load balances automatically but every wakeup must scan all jfcs to find which fired, callbacks go cross-thread out of order, and multi-jfc-per-jfce ack semantics need vendor verification. More complex, less predictable.
- **Approach C (per-group jfce, M threads polling N>M groups):** `urma_wait_jfc` takes a single jfce, so a thread cannot wait on multiple group jfces simultaneously; threads would have to busy-poll across groups (CPU waste) or serial-wait (latency). Does not fit the vendor API.

Approach A maps directly onto `urma_wait_jfc(jfce, 1, ...)` and keeps callbacks single-threaded per group.

## Data flow

### Path 1: send (write)

```text
send coroutine (request thread)
  1. take a buffer from the group send pool, fill payload
  2. urma_send(jetty, ..., user_ctx = encode_ctx(socket_id, OP_SEND, seq))
        user_ctx layout (uint64):
          [63:56] op_type   (OP_SEND / OP_RECV)
          [55:32] socket_id (per-group unique id)
          [31:0]  seq       (send match key; recv: buffer index)
  3. register pending send completion: seq -> resume handler
  4. FAST PATH: spin up to 32 times checking the local handoff state/queue
        hit  -> take the completion handed off by the poll thread, resume, return
        miss -> SLOW PATH
  5. SLOW PATH: suspend the coroutine via callback_awaitor
        resumed by the poll thread after it drains the matching send CQE
```

The fast path no longer calls `urma_poll_jfc` (see Synchronization below); it only checks whether the poll thread has already handed off a completion. 32 short checks cover the typical CQE-arrival window (1-10us) without busy-polling the CQ.

### Path 2: recv (read)

```text
at connection init:
  - take recv buffers from the group, post recv_buffer_cnt recv WRs to the
    per-socket jfr with user_ctx = encode_ctx(socket_id, OP_RECV, buf_idx)

recv coroutine (request thread)
  1. if recv_result_ queue non-empty -> take and return
  2. else suspend via callback_awaitor, set recv_callback_

poll thread drains an OP_RECV CQE:
  - decode user_ctx -> socket_id
  - socket = group->socket(socket_id) (if unregistered: drop CQE, return buffer)
  - socket->push_completion(cr):
        recv branch -> enqueue data into recv_result_, repost a recv WR,
                       if a recv_callback_ is pending -> resume it
```

### Path 3: poll thread main loop (hybrid)

Each poll thread i runs:

```text
poll_loop(group_i):
  idle_spins = 0
  while (!stop_):
      # phase 1: busy-poll (low latency)
      n = urma_poll_jfc(group_i->jfc, 16, cr[16])   # non-blocking, <=16 CQEs
      if n > 0:
          for k in 0..n: dispatch(cr[k])   # decode user_ctx -> socket, handoff + resume
          idle_spins = 0
          continue                            # poll again immediately
      else:
          idle_spins++

      # phase 2: idle budget exceeded -> block (save CPU)
      if idle_spins >= BUSY_POLL_BUDGET:      # default 16
          urma_rearm_jfc(group_i->jfc, false)
          ev_jfc = nullptr
          urma_wait_jfc(group_i->jfce, 1, wait_timeout_ms_, &ev_jfc)  # bounded timeout, not -1
          if ev_jfc: urma_ack_jfc(&ev_jfc, &1, 1)
          idle_spins = 0                      # back to phase 1
      # else: keep busy-polling (short spin)
```

Semantics:
- Under traffic: continuous non-blocking `urma_poll_jfc`, lowest latency (concentrated on the poll thread, not on request threads).
- Idle: after 16 empty spins, `rearm + wait_jfc` blocks the thread; CPU drops to ~0.
- On wakeup: immediately resume busy-poll to drain backlog.

`dispatch(cr)` (single-threaded, serial within the group):
- `decode_ctx(user_ctx) -> (op, socket_id, seq)`
- `socket = group_i->socket(socket_id)` (if not found: drop CQE + return buffer to group pool)
- send CQE -> `socket->push_send_completion(seq, cr)` -> resume matching pending handler via handoff
- recv CQE -> `socket->push_recv_data(cr)` -> repost recv WR -> resume recv_callback_ if pending

### user_ctx encoding

```cpp
inline uint64_t encode_ctx(uint8_t op, uint32_t sid, uint32_t seq) {
    return (uint64_t(op) << 56) | (uint64_t(sid) << 32) | uint64_t(seq);
}
inline auto decode_ctx(uint64_t ctx) {
    uint8_t  op  = (ctx >> 56) & 0xff;
    uint32_t sid = (ctx >> 32) & 0xffffff;
    uint32_t seq =  ctx        & 0xffffffff;
    return std::make_tuple(op, sid, seq);
}
```

`socket_id` is allocated by the group at `register_socket` time (monotonic; freed ids may be reused). `seq` is incremented by the socket on each `urma_send`.

## Synchronization model

### The core problem

Today `urma_socket_shared_state_t` is pinned to one io_context thread and uses no locks. With the thread pool, the poll thread touches a socket's completion queue and `resume_handler` from a *different* thread than the one the send/recv coroutine suspends on. `resume_handler` is a `move_only_function<void()>`, non-atomic; a raw reader/writer race is unsafe. A heavy lock would block the poll thread (which must drain every socket's CQEs quickly).

### Chosen approach: atomic state machine + fallback queue (lock-free handoff)

Each socket owns a `completion_handoff` struct:

```cpp
struct completion_handoff {
    enum class state : uint32_t { IDLE, WAITING, READY };
    std::atomic<state> st{state::IDLE};

    // coroutine writes its handle before publishing WAITING; poll thread reads on handoff
    std::atomic<async_simple::CoroHandle> resume_handle{};

    // poll-thread completion when state == READY
    std::optional<urma_cr_t> pending_cr;

    // fallback queue: CQEs that arrived while IDLE/READY (no one waiting to hand off to)
    std::mutex q_mtx;
    std::deque<urma_cr_t> queue;
};
```

### Protocol timeline (send slow path)

```text
--- request thread (send coroutine, slow path) ---
1. fast-path 32 checks missed
2. st.compare_exchange_strong(IDLE -> WAITING)
     branch A: if st == READY (poll thread was faster) -> take pending_cr, CAS(READY->IDLE), return without suspending
3. resume_handle.store(my_handle, release)
4. st.store(WAITING, release)
5. co_await suspend

   --- poll thread (drained a send CQE for this socket) ---
   a. decode cr, get seq
   b. s = st.load(acquire)
      if s == WAITING:
          if CAS(WAITING -> READY) succeeds:
              pending_cr = cr
              h = resume_handle.exchange(nullptr, acquire)
              h.resume()                 # wake request thread
          else: re-read st, handle
      else (IDLE or READY):
          lock(q_mtx); queue.push_back(cr); unlock   # no one to hand off to yet

--- request thread resumed ---
6. assert st == READY
7. take pending_cr; also drain queue under q_mtx
8. CAS(READY -> IDLE)
9. return result to send caller
```

Branch A covers the case where the poll thread produced the CQE before the coroutine finished its fast-path checks and suspended.

### Invariants

1. **No CQE loss.** Poll thread pushes to `queue` (under `q_mtx`) whenever state is IDLE or READY; the lock-free CAS path (WAITING->READY) is the only "direct handoff" that bypasses the queue. A resumed coroutine checks `pending_cr` first, then `queue`.
2. **Single resume.** `resume_handle.exchange(nullptr)` guarantees the poll thread resumes at most once. A coroutine that finds READY on its own (branch A) does not call the handler.
3. **No long lock.** `q_mtx` is held only for the moment of push/pop; the poll thread is never blocked by a slow coroutine (the deque is unbounded, push never waits).
4. **Memory ordering.** State flips use acquire/release; `resume_handle` store is release (after the handler is wired) and load is acquire, so the handler write is visible before the state publish.

recv uses a structurally similar handoff but single-slot (one `completion_handoff` shared by the one recv coroutine that can be suspended on a socket at a time - matching today's single `recv_callback_` slot). A recv CQE goes through `push_completion` and wakes that single slot; the resumed coroutine drains data into `recv_result_` (protected by `q_mtx` or a sibling mutex) before returning. Send, by contrast, may have multiple in-flight WRs, so the send side keeps a `seq -> handoff` map (one handoff slot per outstanding send WR) rather than a single slot.

### Critical change to the fast-path spin

Today `wait_urma_write_completion`'s spin calls `poll_completion_once()` which calls `urma_poll_jfc` directly. After the refactor, **request threads must not call `urma_poll_jfc`**: the shared `jfc` is being polled by the poll thread, and concurrent `urma_poll_jfc` on the same `jfc` is unsafe (CQEs can be lost). The send fast path therefore changes to:

```text
send fast path (request thread):
  for i in 0..32:
      if the socket's handoff queue is non-empty (under q_mtx):
          take the completion, return
      // never call urma_poll_jfc here
  // 32 checks missed -> slow path suspend
```

I.e. the fast path becomes "has the poll thread handed off a completion yet?" - an atomic state check plus at most one short lock. It does not touch the CQ.

### Socket close / unregister

```text
socket.close():
  1. group_->unregister_socket(socket_id)   # remove from registry (shared_mutex write lock)
  2. wake all pending coroutines (send/recv) on this socket -> operation_aborted
  3. return this socket's send/recv buffers to the group pool
```

After unregister, the poll thread may still drain a residual CQE for a send that was already posted; `dispatch` finds `socket_id` absent, drops the CQE, and returns its buffer to the group pool (leak protection). The group's socket registry uses `std::shared_mutex`: register/unregister take the write lock (low frequency), poll-thread `dispatch` takes the shared lock for read-only lookup (high frequency, non-blocking).

## Error handling and shutdown

### Poll-thread errors

| Source | Return | Handling |
|--------|--------|----------|
| `urma_poll_jfc` < 0 | errno | log, back off 1ms, continue loop (do not kill the thread -> avoid disabling the whole group) |
| `urma_poll_jfc` == 0 | no CQE | `idle_spins++`, normal phase-2 transition |
| `urma_rearm_jfc` != 0 | error | drain once then re-rearm (current strategy); after 8 consecutive failures log and skip the wait this round, return to busy-poll |
| `urma_wait_jfc` == 0, errno == ERESTARTSYS(512) | signal interrupt | vendor-mandated: ignore and continue waiting |
| `urma_wait_jfc` == 0 (other errno) | no event | log, return to busy-poll (do not block) |
| `urma_wait_jfc` == -1 | error | log, back off, continue |
| CQE carries `cr.status != 0` | transport error | dispatch to the socket: mark socket errored, wake all its pending coroutines with `operation_aborted`, trigger that socket's close. Other sockets in the group are unaffected (isolation). |

### Group-level error (jfc/jfce hardware failure)

Rare. Handling: log, mark the group errored, wake all sockets in the group with an error, exit that poll thread. New sockets attempting to join an errored group are rejected (TCP fallback or error return). The process does not crash; other groups keep running.

### Shutdown (waking blocked poll threads)

`urma_wait_jfc(timeout=-1)` blocks forever and cannot be interrupted by `stop()`. Use a **bounded timeout** (default 100ms, `URMA_RPC_POLL_WAIT_TIMEOUT_MS`):

- With a real CQE, `wait_jfc` returns immediately (event-driven, unaffected by the timeout).
- Idle: the thread wakes every 100ms, observes `stop_`, and exits. Idle CPU stays ~0; shutdown latency is at most the timeout.

```text
poll_thread_pool::stop():
  1. stop_.store(true)
  2. for each thread: join()            # at most ~wait_timeout
  3. for each group: wake all pending coroutines with operation_aborted
  4. destroy jfc/jfce/jetty/jfr (urma_destroy_*)
  5. release buffer pools
```

### Buffer leak protection

Every CQE's `user_ctx` encodes `(op, socket_id, seq/buf_idx)`. On `dispatch`:
- socket present: normal handoff; buffer ownership flows with the coroutine (send buffer returned after send returns; recv buffer returned after the data is consumed).
- socket absent (already unregistered): immediately return the buffer to the group pool and drop the CQE. No leak.

## Configuration

### New/adjusted env vars (`urma_rpc_env.hpp`, `make_urma_rpc_config_from_env`)

| Env var | `config_t` field | Default | Notes |
|---------|------------------|---------|-------|
| `URMA_RPC_POLL_THREADS` | `poll_threads` | 4 | poll thread count = group count. **0 disables** the pool, falling back to the legacy per-socket `event_loop`. |
| `URMA_RPC_GROUP_CQ_SIZE` | `group_cq_size` | 1024 | per-group `jfc` depth (must hold all outstanding WRs of the group's sockets). |
| `URMA_RPC_POLL_WAIT_TIMEOUT_MS` | `poll_wait_timeout` | 100ms | `urma_wait_jfc` timeout; balances idle CPU against shutdown latency. |

Reused (semantics unchanged):

| Env var | Default | Role under the new model |
|---------|---------|--------------------------|
| `URMA_RPC_EVENT_MODE` | true | Master switch. true + `poll_threads >= 1` -> thread pool; false -> legacy busy-poll. |
| `URMA_RPC_BUSY_POLL_BUDGET` | 16 | poll thread busy-poll empty-spin count before switching to `wait_jfc`. |
| `URMA_RPC_RECV_BUFFER_CNT` | 8 | per-socket recv WR count (unchanged). |
| `URMA_RPC_SEND_BUFFER_CNT` | 4 | per-socket send WR count (unchanged). |
| `URMA_RPC_BUFFER_SIZE` | 4096 | per-buffer size (unchanged). |
| `URMA_RPC_POLL_INTERVAL` | 5us | legacy fallback path only (unused by the thread pool path). |

`config_t` (`include/ylt/coro_io/urma/urma_socket.hpp:637`) additions:

```cpp
struct config_t {
  // ... existing fields ...
  uint32_t poll_threads = 4;
  uint32_t group_cq_size = 1024;
  std::chrono::milliseconds poll_wait_timeout{100};
};
```

### Mode selection matrix

```text
URMA_RPC_EVENT_MODE=true (default):
  +-- poll_threads >= 1 (default 4): thread pool mode (new)  <-- main path
  +-- poll_threads == 0:           legacy per-socket event_loop (fallback)
URMA_RPC_EVENT_MODE=false:
  +-- legacy per-socket busy-poll (poll_once/start_polling), unchanged
```

### Group cq_depth validation

At `group->init()`: `group_cq_size >= expected_sockets_per_group * (send_buffer_cnt + recv_buffer_cnt) + slack`. `expected_sockets_per_group` is not separately configured; the default `group_cq_size=1024` implies ~85 sockets/group (1024 / (4+8)), which is ample. If exceeded, `urma_create_jfc` fails; log and fall back to the legacy mode.

## Testing strategy

Verification is primarily code-logic verification (no URMA hardware in the development environment). Hardware-level functional/CPU tests run on a separate machine.

### Code-logic tests (no hardware required)

a. **`user_ctx` encode/decode unit test** (extend `src/coro_rpc/tests/test_urma_rpc_env.cpp`)
   - `encode_ctx` / `decode_ctx` round-trip; boundary values (socket_id = 0 / max; seq = 0 / max).

b. **`completion_handoff` state-machine unit test** (new `test_urma_handoff.cpp`)
   - IDLE -> WAITING -> READY -> IDLE happy path.
   - Branch A: READY seen by the coroutine -> take without suspending.
   - Poll-thread push to `queue` while IDLE; coroutine drains `queue` on wake.
   - Concurrency stress: one "poll thread" + N "request threads" repeatedly push/wait; assert no CQE loss and single resume. Uses `std::thread`, no URMA dependency.

c. **Config parsing unit test** (extend existing)
   - `URMA_RPC_POLL_THREADS=0/1/4`, `URMA_RPC_GROUP_CQ_SIZE`, `URMA_RPC_POLL_WAIT_TIMEOUT_MS` parse correctly with expected defaults.
   - Mode matrix: each combination yields the right fields from `try_make_urma_rpc_config`.

### Compile verification (no hardware, requires URMA SDK headers)

d. New headers `urma_poll_thread_pool.hpp`, `urma_jfc_group.hpp` and the changed `urma_socket.hpp` compile cleanly with URMA SDK headers present.

### Hardware tests (run on a separate URMA-equipped machine)

e. Functional: multi-connection RPC, verify correct CQE dispatch and no data loss.
f. CPU comparison: same load, thread-pool mode vs legacy mode idle/loaded CPU (expect idle CPU to drop sharply).
g. Shutdown: process exit joins all poll threads within ~timeout, no leaks, no stranded coroutines.

## Open questions

None at design time. The vendor API surface used (`urma_poll_jfc`, `urma_rearm_jfc`, `urma_wait_jfc`, `urma_ack_jfc`, `urma_create_jfc`, `urma_create_jfce`, `urma_create_jetty` with `shared.jfc`) has been verified against `include/ylt/urma/urma_api.h` and `urma_types.h`. Multi-jetty-per-jfc sharing is supported by `urma_jetty_cfg_t.shared.jfc` ("To replace the jfc related to the above jfr").

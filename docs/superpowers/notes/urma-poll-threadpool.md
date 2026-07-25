# URMA Poll Thread Pool - Operator Notes

Date: 2026-07-25

The URMA RPC path supports a process-level poll thread pool that replaces the
per-connection poll watcher and the inline send spin. Multiple jetties (one per
socket) share a completion queue (`jfc`) per group; a fixed pool of poll
threads drains the groups in a hybrid loop (busy-poll, then blocking
`urma_wait_jfc` when idle).

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `URMA_RPC_ENABLE` | off | Master enable for URMA RPC (1/on/true/yes). |
| `URMA_RPC_EVENT_MODE` | true | Completion-detection mode. `true` enables the event-driven path (thread pool when `URMA_RPC_POLL_THREADS >= 1`, else legacy per-socket `event_loop`). `false` forces the legacy timer-based busy poller. |
| `URMA_RPC_POLL_THREADS` | 4 | Number of poll threads = number of jfc groups. `0` disables the thread pool and falls back to the legacy per-socket `event_loop`. |
| `URMA_RPC_GROUP_CQ_SIZE` | 1024 | Per-group shared `jfc` depth. Must hold the outstanding work requests of all sockets in the group (default implies ~85 sockets/group at 4 send + 8 recv buffers). |
| `URMA_RPC_POLL_WAIT_TIMEOUT_MS` | 100 | `urma_wait_jfc` timeout in milliseconds. Bounds idle CPU (the poll thread blocks instead of busy-spinning) and shutdown latency (threads wake within this interval to observe stop). |
| `URMA_RPC_BUSY_POLL_BUDGET` | 16 | Empty polls before a poll thread switches from busy-poll to `urma_wait_jfc`. |
| `URMA_RPC_RECV_BUFFER_CNT` | 8 | Per-socket recv work-request count (unchanged). |
| `URMA_RPC_SEND_BUFFER_CNT` | 4 | Per-socket send work-request count (unchanged). |
| `URMA_RPC_BUFFER_SIZE` | 4096 | Per-buffer size in bytes (unchanged). |
| `URMA_RPC_POLL_INTERVAL` | 5 (us) | Legacy busy-poller idle interval only (unused by the thread pool path). |

## Mode selection

```
URMA_RPC_EVENT_MODE=true (default):
  +-- URMA_RPC_POLL_THREADS >= 1 (default 4): thread pool mode (recommended)
  +-- URMA_RPC_POLL_THREADS == 0:           legacy per-socket event_loop
URMA_RPC_EVENT_MODE=false:
  +-- legacy per-socket busy-poll (timer-based), unchanged
```

## Fallback

If the thread pool cannot be initialized (e.g. `urma_create_jfc`/`urma_create_jfce`
fails for a group, or no usable URMA device), each socket falls back to the
legacy per-socket path automatically. A warning is logged.

## Verification

This feature was implemented with code-logic verification only (no URMA hardware
in the dev environment). Run functional and CPU-comparison tests on a
URMA-equipped machine:

1. Build with `YLT_ENABLE_URMA=ON`.
2. Set `URMA_RPC_ENABLE=1` (defaults: `URMA_RPC_POLL_THREADS=4`).
3. Run the URMA RPC benchmark / functional tests.
4. Compare idle and loaded CPU against the legacy path
   (`URMA_RPC_POLL_THREADS=0`).
5. Verify clean shutdown (all poll threads join within ~`URMA_RPC_POLL_WAIT_TIMEOUT_MS`).

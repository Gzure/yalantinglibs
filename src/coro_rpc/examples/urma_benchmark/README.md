# URMA RPC Benchmark

`coro_rpc_urma_benchmark` is a small benchmark tool for URMA-based coro_rpc.
It can run on the same node or across two nodes, and supports low-load latency
and throughput tests.

## Build

Configure with URMA enabled and examples enabled:

```bash
cmake -S . -B build -DYLT_ENABLE_URMA=ON -DBUILD_EXAMPLES=ON
```

Build only this benchmark target:

```bash
cmake --build build --target coro_rpc_urma_benchmark -j
```

The binary is generated at:

```bash
build/output/examples/coro_rpc/coro_rpc_urma_benchmark
```

If the target prints this message:

```text
coro_rpc_urma_benchmark requires -DYLT_ENABLE_URMA=ON. Re-run CMake configure with URMA enabled.
```

re-run the CMake configure command above. If CMake was already configured
before this example was added, reconfigure the build directory once.

## Same-Node Test

Start the server:

```bash
./build/output/examples/coro_rpc/coro_rpc_urma_benchmark server \
  --host 127.0.0.1 \
  --port 9001 \
  --device bonding_dev_0
```

Run both latency and throughput tests:

```bash
./build/output/examples/coro_rpc/coro_rpc_urma_benchmark client \
  --host 127.0.0.1 \
  --port 9001 \
  --device bonding_dev_0 \
  --mode both \
  --payload 64 \
  --latency-iters 10000 \
  --connections 64 \
  --duration 10
```

## Cross-Node Test

On the server node:

```bash
./build/output/examples/coro_rpc/coro_rpc_urma_benchmark server \
  --host 0.0.0.0 \
  --port 9001 \
  --device bonding_dev_0
```

On the client node:

```bash
./build/output/examples/coro_rpc/coro_rpc_urma_benchmark client \
  --host <server-ip> \
  --port 9001 \
  --device bonding_dev_0 \
  --mode both \
  --payload 64 \
  --latency-iters 10000 \
  --connections 128 \
  --duration 30
```

Replace `<server-ip>` with the server address reachable from the client.

## Test Modes

Latency only:

```bash
./build/output/examples/coro_rpc/coro_rpc_urma_benchmark client \
  --host <server-ip> \
  --port 9001 \
  --mode latency \
  --payload 64 \
  --latency-iters 10000
```

Throughput only:

```bash
./build/output/examples/coro_rpc/coro_rpc_urma_benchmark client \
  --host <server-ip> \
  --port 9001 \
  --mode throughput \
  --payload 4096 \
  --connections 128 \
  --duration 30
```

## Common Options

```text
--host <ip>              Server listen/connect host. Server default 0.0.0.0,
                         client default 127.0.0.1
--port <port>            Server port. Default 9001
--transport <rpc|raw>    rpc uses coro_rpc. raw uses urma_socket directly.
                         Default rpc
--device <name>          URMA device. Default bonding_dev_0
--eid-index <n>          URMA EID index. Default 0
--payload <bytes>        Echo payload size. Default 64
--buffer-size <bytes>    URMA SEND chunk size. Default 4096 for CTP
--queue-depth <n>        URMA send/recv queue depth. Default 64
--max-memory-mib <n>     URMA buffer pool memory per process. Default 256,
                         auto-raised when payload/connections need more
--no-urma               Use TCP instead of URMA RPC. Default URMA.
--poll-mode <threadpool|event|busy>
                         Completion-detection mode. Default threadpool.
                           threadpool: shared jfc poll thread pool
                                       (--poll-threads threads, multiple
                                       jetties share one jfc per group)
                           event: legacy per-socket event_loop
                                  (--poll-threads 0)
                           busy: legacy timer-based busy poller
                                 (URMA_RPC_EVENT_MODE=0)
--poll-threads <n>       Poll threads (= jfc groups) in threadpool mode.
                         Default 4. 0 falls back to the legacy event mode.
--profile                Enable in-memory stage latency profiling. Default off.
--profile-sample-rate <n> Record one sample every n events per stage/thread.
                         Default 1.
--log <trace|debug|info|warn|error> Default info
```

Client-only options:

```text
--mode <latency|throughput|both>
--rpc <echo|sink|attach_sink> Echo returns the payload. Sink returns only
                             payload size. Attach_sink sends payload as a
                             request attachment and uses the URMA segmented
                             attachment fast path on the server.
--latency-iters <n>
--warmup-iters <n>
--connections <n>
--pipeline-depth <n> Outstanding RPC calls per connection in throughput mode.
                     Default 1.
--concurrency <n> Compatibility option; URMA throughput uses one worker per connection.
--duration <seconds>
--raw-report-interval <seconds> Raw server report interval. Default 0 disables
                                periodic server-side reports.
--client-threads <n>
```

Server-only option:

```text
--server-threads <n>
```

## Notes

- The default URMA transport type is CTP.
- Server mode defaults to `--host 0.0.0.0`, so cross-node tests work without
  explicitly overriding the listen address. Client mode defaults to
  `127.0.0.1`.
- The default `--buffer-size` is `4096`, matching the documented 4KB max send
  packet size for `bonding_dev_0` CTP.
- The URMA buffer pool allocates one large contiguous memory block and registers
  it as one segment, then splits it into fixed-size buffers. This avoids doing
  one `urma_register_seg` call per 4KB buffer during startup.
- The benchmark sizes the pool from `--max-memory-mib`, `--connections`,
  `--queue-depth`, and `--payload`. If the explicit memory value is too small,
  it is raised automatically for the benchmark process.
- `--connections` controls the number of RPC client connections and throughput
  workers. Each throughput worker owns one `coro_rpc_client`.
- `--pipeline-depth` keeps multiple outstanding RPC calls on each connection in
  throughput mode. Use it with `--rpc attach_sink` to test whether one
  request/response at a time is limiting throughput.
- `--concurrency` is retained as a compatibility option. The URMA benchmark does
  not run multiple throughput coroutines on the same connection because that can
  overrun the current URMA send/recv credit model and produce `WR_FLUSH_ERR`.
- Latency output is in microseconds and includes avg/min/p50/p90/p99/p999/max.
- Throughput output includes request rate and payload MiB/s.
- With `--profile`, the client prints one final `urma_profile` block after the
  selected benchmark modes finish. Each stage is sampled in thread-local memory
  and reports avg/p99/p9999/max in microseconds. Use
  `--profile-sample-rate` for long throughput runs to reduce memory and timing
  overhead.
- Use `--rpc sink` to remove the large echo response from the server side. If
  sink throughput is much higher than echo, the bottleneck is response
  serialization/sending. If sink is also low, focus on request read, RPC
  dispatch, and URMA receive/polling.
- Use `--rpc attach_sink` to test the URMA RPC fast path. The request payload is
  sent as attachment data, and the server handler reads segmented
  `owned_data_view`s instead of copying the attachment into a contiguous
  `std::string`.
- Use `--transport raw` to bypass coro_rpc and struct_pack. Raw mode sends the
  payload from client to server without a response, so it measures URMA ingress
  throughput more directly. The raw server does not print periodic throughput by
  default so stdout does not affect the measurement:

```bash
./coro_rpc_urma_benchmark server --transport raw --host 0.0.0.0 --port 9001
./coro_rpc_urma_benchmark client --transport raw --host <server-ip> \
  --payload 1048576 --connections 64 --queue-depth 128 --duration 30
```

Example RPC fast-path throughput test:

```bash
./coro_rpc_urma_benchmark client --host <server-ip> --mode throughput \
  --rpc attach_sink --payload 1048576 --connections 64 --pipeline-depth 8 \
  --queue-depth 128 --duration 30
```

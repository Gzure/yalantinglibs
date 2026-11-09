# URMA RPC

本文介绍 `coro_rpc` 的 URMA 传输层、自动升级机制、配置方式和底层接口。URMA 是可选功能；未启用或初始化失败时，`coro_rpc` 继续使用 TCP。

## 1. 架构与连接流程

URMA RPC 由三层组成：

1. `coro_rpc`：负责 RPC 编解码、handler、连接管理和 attachment。
2. `coro_io::urma_socket_t`：负责 TCP 握手、URMA Jetty 建链、收发队列和完成事件。
3. URMA 资源层：负责设备、context、JFC/JFCE、Jetty、Segment 和 work request。

连接建立先使用 TCP 交换 EID、UASID、Jetty ID 和 buffer pool segment；随后双方导入对端资源，数据面切换到 URMA。TCP 只用于握手。

```mermaid
flowchart TD
    RPC["coro_rpc client/server"] --> SOCKET["coro_io::urma_socket_t"]
    SOCKET --> HANDSHAKE["TCP handshake"]
    HANDSHAKE --> META["Peer metadata: EID, UASID, Jetty ID, Segment"]
    SOCKET --> RESOURCES["URMA resources"]
    RESOURCES --> CONTEXT["Device and context"]
    RESOURCES --> QUEUES["JFC/JFCE and Jetty"]
    RESOURCES --> SEGMENT["Registered buffer pool segment"]
    META --> IMPORT["Import peer resources"]
    QUEUES --> WR["Send/receive work requests"]
    SEGMENT --> WR
    IMPORT --> WR
    WR --> COMPLETION["Completion polling or event loop"]
```

buffer pool 将一块连续内存注册为 Segment，再切分为固定大小的 buffer，减少重复注册开销。发送窗口受本地发送 buffer 数和对端接收 buffer 数共同限制。

## 2. 构建与显式启用

```bash
cmake -S . -B build -DYLT_ENABLE_URMA=ON -DBUILD_EXAMPLES=ON
cmake --build build --target coro_rpc_urma_example -j
```

CMake 从 `URMA_ROOT`、`/usr/include` 和 `/usr/local/include` 查找 UMDK
头文件，支持 `urma/`、`umdk/urma/` 和 UMDK 源码目录布局。例如：

```bash
cmake -S . -B build -DYLT_ENABLE_URMA=ON \
  -DURMA_ROOT=/opt/umdk
```

也可以通过环境变量指定：

```bash
export URMA_ROOT=/opt/umdk
```

配置必须同时包含 `urma_api.h` 和 bonding 扩展头 `urma_ubagg.h`，并在启用
URMA 时链接系统 `liburma`。

```cpp
coro_rpc_server server(std::thread::hardware_concurrency(), 9000);
server.init_urma();
server.register_handler<echo>();
server.start();
```

```cpp
coro_rpc_client client;
if (!client.init_urma()) {
  co_return;
}
auto ec = co_await client.connect("127.0.0.1:9000");
auto result = co_await client.call_for<echo>(30s, "hello urma");
```

## 3. 配置

```cpp
coro_io::urma_socket_t::config_t config{
    .cq_size = 128,
    .recv_buffer_cnt = 64,
    .send_buffer_cnt = 64,
    .buffer_size = 4096,
    .max_memory_usage = 256ull * 1024 * 1024,
    .device_name = "bonding_dev_0",
    .eid_index = 0,
    .tp_type = URMA_CTP,
};

server.init_urma(config);
client.init_urma(config);
```

## 4. 环境变量自动升级

默认 TCP 配置下，设置 `URMA_RPC_ENABLE=1` 可自动探测 URMA 设备并升级：

```bash
export URMA_RPC_ENABLE=1
export URMA_RPC_DEVICE=bonding_dev_0
export URMA_RPC_EID_INDEX=0
```

`URMA_RPC_ENABLE` 接受 `1`、`on`、`true`、`yes`。未设置、关闭或设备探测失败时回退 TCP。显式调用 `init_urma(config)` 的配置优先。

| 变量 | 作用 |
| --- | --- |
| `URMA_RPC_ENABLE` | 启用自动升级 |
| `URMA_RPC_DEVICE` | 设备名；为空时自动选择 |
| `URMA_RPC_EID_INDEX` | EID 下标，默认 `0` |
| `URMA_RPC_CQ_SIZE` | completion queue 大小 |
| `URMA_RPC_RECV_BUFFER_CNT` / `URMA_RPC_SEND_BUFFER_CNT` | 收发 buffer 数 |
| `URMA_RPC_BUFFER_SIZE` | 单个 buffer 大小 |
| `URMA_RPC_MAX_MEMORY_USAGE` | buffer pool 上限，单位 byte |
| `URMA_RPC_TP_TYPE` | `ctp`、`rtp` 或其他支持的类型 |
| `URMA_RPC_PRIORITY` | JFS priority，范围 `0~15`，默认值为 `15` |
| `URMA_RPC_EVENT_MODE` | JFCE 事件模式开关 |
| `URMA_RPC_BUSY_POLL_BUDGET` | 事件唤醒后、**没有在途 send/recv 回调**时的空轮询预算（抓空闲突发），默认 `16`；实际执行 `budget+1` 次空 polling |
| `URMA_RPC_PENDING_POLL_BUDGET` | 事件唤醒后、**仍有在途 send/recv 回调**时的空轮询预算，默认 `1`（实际 `budget+1` 次）：事件已被 jfce fd 交付，抽干即回到 rearm+wait；保留 1 次是为了覆盖"CQE 落在最后一次抽干与 rearm 之间"的窗口。设为 `0` 用于极限压测 |
| `URMA_RPC_JFC_MODERATE_COUNT` / `URMA_RPC_JFC_MODERATE_PERIOD` | JFC 完成事件合并参数（`urma_modify_jfc`），默认 `1` / `-1`：`count=1` 让每个 CQE 立刻产生事件，`period` 保留驱动默认。驱动默认合并会把 JFCE 唤醒延迟抬到 `moderate_period` 量级（实测 50~300µs），对 13~20µs RTT 目标是致命的；任一字段设 `-1` 表示该字段沿用驱动默认（A/B 对照）。**吞吐提示**：`count=1` 下 jfce 事件数随 CQE 线性增长，极高吞吐场景可改用 `count=16`/`period=10` 或 `-1`/`-1` |
| `URMA_RPC_POLL_INTERVAL` | timer 兜底路径的轮询间隔，单位微秒 |
| `URMA_RPC_POLL_SPIN` | timer 兜底路径（`URMA_RPC_EVENT_MODE=0`）的轮询方式，默认 `1`：在 reactor 线程内连续轮询（按 `URMA_RPC_POLL_ROUNDS`，未设时由 `URMA_RPC_IO_RATE` 换算，决定每多少轮让出一次 io_context），不再为每个 5µs tick arm `asio::steady_timer`。单次轮询是用户态 `udma_u_poll_jfc`，而一次 timer tick 是 2 次 `timerfd_settime` + 1 次 epoll 唤醒（≈3.5µs CPU / 5µs 周期/连接）。代价：持有 timer 模式 socket 的 reactor 线程会占满一个核。设 `0` 恢复原 timer 节奏（空闲省 CPU，但保留 timerfd 风暴） |
| `URMA_RPC_IO_RATE` | spin 模式下**用于轮询 CQ 的时间占比（百分比）**，1~99，默认 `50`（= 每让出一次 io_context 前连续 poll 8 轮，与旧默认行为一致）。换算：`rounds = 8 × P / (100 − P)`（一次让出的 io_context 迭代 ≈ 8 次 poll 的开销）：`50→8`、`80→32`、`90→72`、`95→152`、`99→792`。越大 → 完成检测越及时，但 handler/fd/timer 处理越晚；`100` 会被钳到 `99`，因为循环必须让出才能处理 accept/fd/定时器 |
| `URMA_RPC_POLL_ROUNDS` | 直接指定**每让出一次 io_context 前连续 poll 的轮数**（1~1024），非 0 时覆盖 `URMA_RPC_IO_RATE`；用于百分比估算与实际机器不符的精确调参，默认 `0`（由百分比换算） |
| `URMA_RPC_SHARED_JFC` | `1` 时同一 io_context 的所有 URMA socket **共用一个 JFC**（`urma_jetty_cfg_t.shared.jfc`）：一个 poll loop 抽干组的 CQE，并按 `user_ctx`（sid/op/seq 编码）在**同一 reactor 线程内**分发给对应 socket。默认 `0`（每 socket 一个 JFC）。仅 poll 模式（`URMA_RPC_EVENT_MODE=0`）生效，event 模式自动回退到 per-socket JFC。收益：每次 tick 的 `urma_poll_jfc` 从 N 次降到 1 次、post 链从 N 条降到 1 条、JFC 资源从 N 个降到 1 个；代价：需要驱动支持多 jetty 共享 JFC，且 CQ 溢出会影响组内所有连接 |
| `URMA_RPC_GROUP_CQ_SIZE` | 共享 JFC 的深度，默认 `2048`，范围 64~65536（创建时再按设备 `max_jfc_depth` 钳制）。必须覆盖组内所有 socket 的在途 WR，约 `sockets × (recv_buffer_cnt + send_buffer_cnt + 4)`；不够时 `attach` 失败并自动回退 per-socket JFC |

**共享 JFC 的容量估算**：默认 `RECV_BUFFER_CNT=8 / SEND_BUFFER_CNT=4` 时每 socket 约 16 个在途 WR，`GROUP_CQ_SIZE=2048` 可容纳约 128 个 socket；若按吞吐配置 `128/128`，每 socket 约 260 个 WR，4 个 socket 就需要 ≥1040，建议直接把 `GROUP_CQ_SIZE` 设到 4096 或降低 buffer 数。共享 JFC 属于**未在硬件上充分验证**的能力，建议先用 `URMA_RPC_SHARED_JFC=0` 建立基线，再以 `1` 做 A/B，并观察 `URMA shared JFC`、`attach failed`、`poll failed` 日志。

**共享 JFC 已知待上机确认项**：

1. 驱动是否把 `urma_jfs_wr_t::user_ctx` / `urma_jfr_wr_t::user_ctx` 原样回填到 `urma_cr_t::user_ctx`。不回填时组会丢弃所有完成（socket 层看到 `user_ctx==0` 被当无效记录），表现为连接静默挂起；
2. 多 jetty 共享同一 JFC 是否被 UMDK 支持；`urma_delete_jetty` 时 flush CQE 是否进入共享 JFC。当前 `detach` 立即归还 CQ 额度，若驱动仍可能投递残留/flush CQE，短时间内可能超卖——后果是新的 post 失败并返回给调用方（不会静默错配），必要时改成延迟归还；
3. 同一 JFR 的 recv 完成是否严格保序（当前按 FIFO 从 `recv_queue_` 取 buffer）；若多 TP/多 sender 下可能乱序，需要改成按 `user_ctx` 里的 recv seq 匹配；
4. 共享模式要求 `init()`/`attach()` 与轮询在同一个 io_context 线程（组内单线程假设）；跨线程 attach 虽已加锁，但 `poll_once` 的调度仍应只在所属 io_context 线程执行。

**timer 兜底路径的取舍**：`URMA_RPC_POLL_SPIN=1`（默认）把完成检测延迟压到一次轮询（亚微秒），代价是每个含 timer 模式 socket 的 reactor 线程一个忙核——服务端用 `--rpc_thread_num` 限核，或设 `URMA_RPC_POLL_SPIN=0` 换回定时器。若目标是 13~20µs RTT，建议优先用事件模式（`URMA_RPC_EVENT_MODE=1`）；timer 路径是事件模式不可用时的兜底。

事件模式的延迟取决于 **JFCE 唤醒延迟**，而不是轮询预算。若事件模式时延仍偏高，按下面顺序排查：

1. 看启动日志 `URMA JFC event moderation applied: count=..., period_us=...`；出现 `urma_modify_jfc failed` 说明驱动不接受该参数（此时功能自动退回驱动默认，不影响正确性）。
2. 用 `URMA_RPC_JFC_MODERATE_COUNT=-1 URMA_RPC_JFC_MODERATE_PERIOD=-1` 跑一组对照，确认延迟差异是否来自合并。
3. 若目标 UMDK 版本没有 `urma_modify_jfc` / `urma_jfc_attr_t`，cmake 的 `check_cxx_source_compiles` 探测会自动定义 `YLT_URMA_NO_JFC_MODERATION` 把这段编译掉；也可以手工强制关闭（**定义宏时不要带值**，写成 `-DYLT_URMA_NO_JFC_MODERATION` 而不是 `=0`）。

实现位于 `include/ylt/coro_io/urma/urma_rpc_env.hpp`，行为测试位于 `src/coro_rpc/tests/test_urma_rpc.cpp`。

## 5. URMA 接口

| 层次 | 主要接口 | 作用 |
| --- | --- | --- |
| 生命周期 | `urma_init`、`urma_uninit` | 初始化和释放 runtime |
| 设备 | `urma_get_device_list`、`urma_get_eid_list`、`urma_query_device` | 查询设备、EID 和能力 |
| Context | `urma_create_context`、`urma_delete_context` | 创建设备上下文 |
| 完成队列 | `urma_create_jfc`、`urma_create_jfce`、`urma_poll_jfc`、`urma_wait_jfc` | 等待并读取完成记录 |
| 数据队列 | `urma_create_jfs`、`urma_create_jfr`、`urma_create_jetty` | 创建收发资源 |
| 远端资源 | `urma_import_seg`、`urma_import_jetty` | 导入对端 Segment 和 Jetty |
| 本地内存 | `urma_register_seg` | 注册 URMA 可访问内存 |
| 数据收发 | `urma_post_jetty_send_wr`、`urma_post_jetty_recv_wr` | 提交 work request |

典型顺序是：创建 context → 创建 JFC/JFCE 和 Jetty → 注册 Segment → 交换元数据 → 导入对端资源 → post WR → poll/wait completion → 释放资源。声明来自外部 UMDK 的 `urma_api.h` 和 `urma_types.h`。

## 6. 大数据与压测

大 payload 建议使用 attachment：

```cpp
client.set_req_attachment(payload);
auto result = co_await client.call_for<upload>(30s, payload.size());
```

```bash
cmake --build build --target coro_rpc_urma_benchmark -j
```

详细参数见 [`urma_benchmark/README.md`](../src/coro_rpc/examples/urma_benchmark/README.md)。`--transport raw` 用于排除 RPC 编解码开销，`--rpc attach_sink` 用于测试 attachment 快路径。

## 7. 排障

- 没有 URMA 目标：确认使用 `-DYLT_ENABLE_URMA=ON`，并安装匹配的库、头文件和驱动。
- 自动升级未生效：确认 `URMA_RPC_ENABLE` 有效，且应用走默认 TCP 配置路径。
- 初始化或连接失败：检查设备名、EID、TP 类型和 TCP 握手端口。
- 高并发下出现 RNR 或 `WR_FLUSH_ERR`：降低连接数、pipeline depth、队列深度，或增加内存上限。
- 定位延迟：启用 benchmark 的 `--profile`，观察握手、post send、completion wait、RPC dispatch 和 attachment 阶段。

示例：`src/coro_rpc/examples/urma_example/urma_example.cpp`；实现：`include/ylt/coro_io/urma/`；公共 API 来自 UMDK 的 `urma_api.h`、`urma_types.h` 和 `urma_ubagg.h`。

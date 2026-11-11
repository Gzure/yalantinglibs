# YalantingLibs URMA / UnifiedBus 独立资料核查

核查日期：2026-08-20  
用途：为面向 purecpp.cn 的宣传文章提供可引用事实、术语、架构、用法与性能材料。本文是独立研究笔记，不是宣传稿。

## 结论先行

- YalantingLibs 的 `coro_rpc` 已有 URMA 传输实现：issue #1211 描述了 `urma_socket_t`、设备管理、buffer pool、环境变量自动启用、TCP listener 复用和 attachment 零拷贝等设计，并声称这些实现已在主分支可用。
- 但截至本次核查，#1211 页面仍为 Open，页面显示没有关联分支或 PR。因此“已合入”这一宣传表述应补充具体 merge commit、release 或可复现版本；仅凭 issue 正文不能独立证明已经正式合并发布。
- URMA 的官方/一手定义是：面向 UB 语义的高性能异步通信库，提供异步内存访问和双边消息通信；UMDK 中文用户指南进一步说明其支持单边、双边和原子操作，并通过统一接口屏蔽底层硬件驱动差异。
- #1211 中的 TCP/URMA 性能表是项目 issue 自报的 Poll 模式测试数据，不是 UnifiedBus 或 UMDK 官方基准。文章可引用，但必须同时注明测试模式、payload、单位、环境缺失及“实测样本不可外推”的限制。

## 一、可直接引用的事实

### YalantingLibs 接入事实

1. `coro_rpc` 原有传输包括 TCP、IBV(RDMA)、SSL、NTLS；#1211 的目标是增加 URMA 作为一等传输，并让上层 RPC 业务接口保持不变。
2. 编译期开关为 `YLT_ENABLE_URMA=ON`。运行时有两种启用方式：显式 `client.init_urma(cfg)` / `server.init_urma(cfg)`，或设置 `URMA_RPC_ENABLE=1` 进行设备探测和自动升级。
3. 自动升级路径复用既有 TCP listener，通过首字节 magic number 嗅探触发升级，不需要额外端口；探测失败时回退 TCP。
4. 设计包含 `urma_socket_t`、`urma_device` / `urma_device_manager`、`urma_buffer_pool_t` 与 `urma_rpc_env`。buffer pool 的设计是先注册大段内存再切片，以减少逐小块注册开销。
5. #1211 声称大 attachment 可以直接 view 到 URMA receive buffer，并通过 RAII 归还 buffer pool，从而避免一次 payload memcpy。这是实现设计/当前状态声明，不应改写成所有 RPC 消息都“零拷贝”。

来源：

- [YalantingLibs issue #1211](https://github.com/alibaba/yalantinglibs/issues/1211)，特别是“架构设计”“接入方式”“关键设计决策”“当前实现状态”章节。
- 本地 [docs/urma_rpc.md](./urma_rpc.md)，其中记录编译、环境变量、API 和 benchmark 入口。
- 本地 [docs/proposals/urma_transport_proposal.zh.md](./proposals/urma_transport_proposal.zh.md)，属于项目提案/设计材料，引用时应标明其性质。

### UMDK / URMA 事实

1. UMDK 是“以内存语义为核心的分布式通信软件库”，面向数据中心网络内的主机间、设备间以及主机与设备间通信。
2. URMA（Unified Remote Memory Access，统一远程内存访问）是 UMDK 的核心基础通信库；用户指南将其定义为 UB 通信基础库，提供统一的单边、双边和原子远端内存操作方式，并提供面向应用的北向 API 与面向驱动开发者的南向接入 API。
3. URMA 用户侧的基本对象包括：Context、Segment、Jetty/JFS/JFR、JFC/JFCE。Segment 表示可被 URMA 内存事务访问的内存区域；Jetty 是事务操作/IO 队列的统一入口；JFS 负责发送，JFR 负责接收，JFC 保存完成记录。
4. 基本流程是：初始化 URMA 和 Context → 创建 Jetty/JFS/JFR/JFC → 注册本地 Segment → 通过带外机制交换资源 ID、地址、权限等信息 → import 远端 Jetty/Segment → 提交 WR → 轮询或等待 JFC 完成 → 按逆序释放资源。
5. 单边操作（read/write）直接访问远端内存，不要求远端应用线程参与；双边操作（send/recv）需要接收端预先提交接收请求；操作是异步非阻塞的，成功提交不等于已经完成，完成结果通过 JFC 获取。

来源：

- 本地 `C:\workspace\code\opensource\umdk\doc\ch\urma\URMA User Guide.ch.md`：第 1、2 章（UMDK/URMA 定义，第 2.1 节概念），第 4 章（Quick start），第 5 章（管理面/控制面/数据面），第 5.2.2、5.2.3、5.3 节（Jetty、Segment、单边/双边/完成记录）。
- 本地 `C:\workspace\code\opensource\umdk\doc\ch\urma\URMA QuickStart Guide.ch.md`：第 1 章（组件编译），第 2 章（安装），第 4 章（设备验证与 `urma_perftest` 示例）。
- [UnifiedBus Base Specification 2.0 Preview](https://www.unifiedbus.com/api-server/web/v1/docs/en/file/UB-Base-Specification-2.0-preview-en.pdf/2.0.0)，术语表及第 2、8.4 节：URMA 定义为支持 UB 语义的高性能异步通信库，提供异步内存访问和双边消息通信；Jetty 是 URMA 的基本通信单元。
- [openEuler/umdk README](https://gitee.com/openeuler/umdk/blob/master/README.md?skip_mobile=true)。

## 二、UnifiedBus 官方资料能支持什么

UnifiedBus 官方 Base Specification 将 UB 定义为面向 SuperPoD 的互联技术与协议栈，并强调统一协议、peer-to-peer 协同、计算/内存/存储资源池化、可扩展拓扑和高可用。它还明确区分：UB 是底层互联与协议栈，URMA 是其上的异步通信编程模型之一；不要把 URMA 写成“总线本身”。

Base Specification 给出的协议层次包括物理层、数据链路层、网络层、传输层、事务层和功能层。功能层提供 load/store 同步访问与 URMA 异步访问两种编程模型；URMA 应用通过 Jetty 建立通信对、提交事务并查询响应。

官方白皮书/文档可以支持以下定性表述：UB 面向 AI/HPC SuperPoD，目标包括异构处理单元之间的统一互联、高速数据移动、资源池化和 peer-to-peer 协同；但这些是架构目标/能力描述，不等于在任意硬件和任意应用上都能得到特定延迟或带宽。

来源：

- [UnifiedBus 官方文档主页](https://www.unifiedbus.com/en)：列出 Base Specification、OS 软件参考设计、Service Core 软件架构参考设计及 SuperPoD Reference Architecture White Paper。
- [UnifiedBus Base Specification 2.0 Preview](https://www.unifiedbus.com/api-server/web/v1/docs/en/file/UB-Base-Specification-2.0-preview-en.pdf/2.0.0)：第 2 章及第 8.4 节。
- [UB Service Core Software Architecture Reference Design 2.0](https://www.unifiedbus.com/api-server/web/v1/docs/zh/file/UB-Service-Core-SW-Arch-RD-2.0-zh.pdf/2.0.0)：第 5 章介绍 UBS Comm、HCOM、Socket、HCAL、RoUB 及其分层软件架构；其中 HCOM 明确把 TCP、RDMA、URMA、SHM 等低级 API 的差异封装在高级通信框架之下。

## 三、架构与优势：可写与不可过写

### 可写的架构关系

`coro_rpc` 业务 API → YalantingLibs 传输选择 → `urma_socket_t` → UMDK `liburma`/URMA C API → UB/UBoE 硬件与驱动。

URMA 自身可按管理面、控制面、数据面理解：管理面负责事务层/传输层资源与建链管理；控制面管理 Context、Jetty、Segment 等对象；数据面提交和执行内存/消息事务，并通过 JFC/JFCE 获取完成。

### 可谨慎宣传的优势

- 对已使用 `coro_rpc` 的应用，业务层调用接口可以保持不变；编译启用 URMA 后，可显式配置或通过环境变量尝试自动升级。
- 复用 TCP listener、探测失败回退 TCP，有利于渐进式部署和兼容已有服务端端口配置。
- URMA 提供统一的单边/双边/原子语义；YalantingLibs 的 RPC 封装重点使用的是异步收发与 buffer pool，不能据此宣称 `coro_rpc` 已暴露全部 URMA 原子 API。
- attachment 快路径有机会消除一次 payload memcpy；应限定为大 attachment/特定实现路径。
- 官方架构材料支持“面向高带宽、低时延、异构资源协同”的定性定位，但不支持脱离硬件、驱动和拓扑的固定性能承诺。

### 应避免的表述

- 避免写成“URMA 已经替代 TCP/RDMA”或“任何机器开环境变量就能获得 UB 性能”；官方资料和本地文档都显示需要匹配的硬件、驱动、UMDK 和运行环境。
- 避免把 #3217 中的“理论带宽 50 GB/s”“连接数无上限”当作 YalantingLibs 实测结果；那是 brpc 社区讨论中的方案判断/路线材料。
- 避免把 #1211 的 benchmark 结果写成官方认证数据。

## 四、性能与测试数据

### YalantingLibs issue #1211 自报数据

测试标签：Poll 模式；延迟单位 us。issue 未在表格附近给出完整硬件、驱动、CPU 绑定、消息协议、迭代次数和统计方法，因此以下仅作为“该 issue 报告的样本数据”。

| payload | TCP avg | URMA avg | TCP p99 | URMA p99 | TCP QPS | URMA QPS | URMA 吞吐 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 B | 62.27 | 13.11 | 78.10 | 20.00 | 166,455 | 641,904 | 39.18 MiB/s |
| 1 KiB | 124.99 | 14.22 | 131.94 | 21.00 | 87,937 | 657,047 | 641.65 MiB/s |
| 2 KiB | 135.67 | 14.47 | 154.42 | 21.00 | 51,646 | 623,325 | 1,217.43 MiB/s |
| 4 KiB | 173.62 | 16.09 | 188.05 | 23.00 | 27,833 | 379,267 | 1,481.51 MiB/s |
| 8 KiB | 270.68 | 19.77 | 298.26 | 26.00 | 14,112 | 248,534 | 1,941.67 MiB/s |
| 16 KiB | 417.17 | 22.19 | 453.11 | 28.00 | 7,078 | 163,239 | 2,550.61 MiB/s |

按 issue 自己的归纳，平均延迟优势从 64 B 的约 4.8× 增至 16 KiB 的约 18.8×；16 KiB 时 URMA 报告吞吐 2,550.61 MiB/s、QPS 约为 TCP 的 23.1×。文章若引用，应使用“在该 issue 报告的测试条件下”，并同时给出 payload、Poll 模式及单位。

### UMDK 官方测试入口

UMDK QuickStart 给出的工具是 `urma_perftest`，示例为：

```bash
urma_perftest send_bw -d bonding_dev_0 -s 2 -n 10 -I 128 -p 1
urma_perftest send_bw -d bonding_dev_0 -s 2 -n 10 -I 128 -p 1 -S <server_ip>
```

用户指南列出的测试类型包括 `send_lat`、`read_lat`、`write_lat`、`atomic_lat` 以及相应带宽测试。该工具适合在目标硬件/驱动环境中重新测量，但本地环境没有执行这些硬件测试，不能把工具存在写成已验证性能。

来源：

- [YalantingLibs issue #1211](https://github.com/alibaba/yalantinglibs/issues/1211) 第 4.2、4.3 节。
- 本地 `C:\workspace\code\opensource\umdk\doc\ch\urma\URMA QuickStart Guide.ch.md` 第 4.2 节。
- 本地 `C:\workspace\code\opensource\umdk\doc\ch\urma\URMA User Guide.ch.md` 第 6.4.1 节。

## 五、brpc Discussion #3217 的定位

brpc #3217 是 2026-02-05 发起的社区讨论，主题是 openEuler UB 总线协议、URMA 与 OBMM 的技术路线。讨论中提出：OBMM 更偏本机/远端 PCIe P2P 共享内存；URMA 目标是一套代码覆盖跨机 RoCE/IB/UB，并讨论“理论带宽 50 GB/s”等路线判断；还提出单后端与 A+B 双后端自动切换的可能方案。

可引用为：社区正在比较 OBMM 的共享内存路径与 URMA 的统一远程内存/消息路径，关注零拷贝、连接规模、GPU/CXL/异构内存和 brpc transport 集成。

不可引用为：brpc 已经集成 URMA、50 GB/s 已被验证、某种 transport 路线已经确定。页面本身是征求意见的讨论，不能替代实现、基准或发布说明。

来源：[Apache brpc Discussion #3217](https://github.com/apache/brpc/discussions/3217)。

## 六、文章用法建议

推荐叙事顺序：

1. 先解释 UB 是 SuperPoD 级互联与协议栈，URMA 是其异步通信编程库。
2. 再解释 URMA 的 Segment、Jetty/JFS/JFR、JFC 以及单边/双边语义。
3. 然后落到 YalantingLibs：`coro_rpc` 在业务 API 之下增加 URMA transport，可显式启用或自动探测并回退 TCP。
4. 最后给出 issue #1211 的样本 benchmark，并明确“项目测试数据、依赖硬件与软件栈、不可外推”。

建议的版本措辞：

> YalantingLibs 主线已出现 URMA transport 实现，`coro_rpc` 可以在编译启用 UMDK 后，通过显式配置或环境变量探测使用 URMA；现有 TCP listener 和业务调用路径保持兼容，探测失败可回退 TCP。正式发布/合入状态请以对应 merge commit 或 release tag 为准。

## 七、核查限制

- 本次未在实际 UB/UMDK 硬件环境运行 `urma_perftest` 或 YalantingLibs benchmark。
- `umdk` 中文 Markdown 在 PowerShell 输出中存在编码显示异常，但文件可读，章节与命令已通过原文件检索确认；引用时应保留本地路径，必要时用 UTF-8 编辑器打开。
- UnifiedBus 官方白皮书主页能确认文档定位与总体架构目标；具体 URMA 术语和协议层次主要依据官方 Base Specification Preview，而非二手文章。

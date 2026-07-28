/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "asio/ip/address.hpp"
#include "async_simple/Promise.h"
#include "async_simple/coro/Collect.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/SyncAwait.h"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_io/urma/urma_device.hpp"
#include "ylt/coro_io/urma/urma_benchmark_profile.hpp"
#include "ylt/coro_io/urma/urma_io.hpp"
#include "ylt/coro_io/urma/urma_socket.hpp"
#include "ylt/coro_rpc/coro_rpc_client.hpp"
#include "ylt/coro_rpc/coro_rpc_server.hpp"
#include "ylt/easylog.hpp"
#include "ylt/easylog/record.hpp"

using namespace async_simple::coro;
using namespace coro_rpc;
using namespace std::chrono_literals;

std::string_view bench_echo(std::string_view data) { return data; }
uint64_t bench_sink(std::string_view data) { return data.size(); }
void bench_attachment_sink(coro_rpc::context<uint64_t> ctx) {
  uint64_t size = 0;
  auto views = ctx.get_context_info()->get_request_attachment_views();
  if (!views.empty()) {
    for (auto& view : views) size += view.size();
  }
  else {
    size = ctx.get_context_info()->get_request_attachment2().size();
  }
  ctx.response_msg(size);
}

struct options_t {
  std::string role = "client";
  std::string transport = "rpc";
  std::string mode = "both";
  std::string rpc = "echo";
  std::string host = "127.0.0.1";
  uint16_t port = 9001;
  std::string device = "bonding_dev_0";
  int eid_index = 0;
  uint32_t payload_size = 64;
  uint32_t latency_iters = 10000;
  uint32_t warmup_iters = 1000;
  uint32_t concurrency = 64;
  uint32_t connections = 64;
  uint32_t pipeline_depth = 1;
  uint32_t duration_seconds = 10;
  uint32_t raw_report_interval_seconds = 0;
  uint32_t server_threads = std::max(1u, std::thread::hardware_concurrency());
  uint32_t client_threads = std::max(1u, std::thread::hardware_concurrency());
  uint16_t queue_depth = 64;
  uint32_t buffer_size = 4 * 1024;
  uint64_t max_memory_usage = 256ull * 1024 * 1024;
  bool profile = false;
  bool use_urma = true;
  // URMA completion-detection mode: "threadpool" (default) uses the shared
  // jfc poll thread pool; "event" uses the legacy per-socket event_loop;
  // "busy" uses the legacy timer-based busy poller (event_mode off).
  std::string poll_mode = "threadpool";
  // Number of poll threads (= jfc groups) in threadpool mode. 0 disables the
  // pool and falls back to the legacy event_loop.
  uint32_t poll_threads = 4;
  uint32_t profile_sample_rate = 1;
  easylog::Severity log_level = easylog::Severity::WARNING;
};

void status(std::string_view message) {
  std::cout << "[urma_benchmark] " << message << std::endl;
}

uint64_t effective_pool_memory_usage(const options_t& opt);

void set_process_env(const char* name, const std::string& value) {
#ifdef _WIN32
  _putenv_s(name, value.c_str());
#else
  setenv(name, value.c_str(), 1);
#endif
}

void configure_urma_rpc_env(const options_t& opt) {
  set_process_env("URMA_RPC_ENABLE", "1");
  set_process_env("URMA_RPC_DEVICE", opt.device);
  set_process_env("URMA_RPC_EID_INDEX", std::to_string(opt.eid_index));
  set_process_env("URMA_RPC_CQ_SIZE", std::to_string(opt.queue_depth * 2 + 8));
  set_process_env("URMA_RPC_RECV_BUFFER_CNT", std::to_string(opt.queue_depth));
  set_process_env("URMA_RPC_SEND_BUFFER_CNT", std::to_string(opt.queue_depth));
  set_process_env("URMA_RPC_BUFFER_SIZE", std::to_string(opt.buffer_size));
  set_process_env("URMA_RPC_MAX_MEMORY_USAGE",
                  std::to_string(effective_pool_memory_usage(opt)));
  set_process_env("URMA_RPC_TP_TYPE", "ctp");

  // Completion-detection mode selection.
  //   threadpool -> event_mode on + poll_threads poll threads (shared jfc)
  //   event      -> event_mode on + poll_threads=0 (legacy per-socket event_loop)
  //   busy       -> event_mode off (legacy timer-based busy poller)
  if (opt.poll_mode == "busy") {
    set_process_env("URMA_RPC_EVENT_MODE", "0");
  }
  else {
    set_process_env("URMA_RPC_EVENT_MODE", "1");
    set_process_env("URMA_RPC_POLL_THREADS",
                    opt.poll_mode == "event" ? "0"
                                             : std::to_string(opt.poll_threads));
  }
  // Group CQ depth: size for ~connections/group at the per-socket queue depth,
  // with slack.  Only used in threadpool mode.
  auto per_socket_wr = static_cast<uint64_t>(opt.queue_depth) * 2 + 8;
  auto group_cq_size = std::max<uint64_t>(
      1024u, (opt.connections / std::max<uint32_t>(opt.poll_threads, 1) + 1) *
                     per_socket_wr +
                 64);
  set_process_env("URMA_RPC_GROUP_CQ_SIZE", std::to_string(group_cq_size));
  set_process_env("URMA_RPC_POLL_WAIT_TIMEOUT_MS", "100");
}

void print_usage(const char* program) {
  std::cout
      << "Usage:\n"
      << "  " << program << " server [options]\n"
      << "  " << program << " client [options]\n\n"
      << "Common options:\n"
      << "  --host <ip>              Server listen/connect host. Default 127.0.0.1\n"
      << "  --port <port>            Server port. Default 9001\n"
      << "  --transport <rpc|raw>    rpc uses coro_rpc; raw uses urma_socket directly. Default rpc\n"
      << "  --device <name>          URMA device. Default bonding_dev_0\n"
      << "  --eid-index <n>          URMA EID index. Default 0\n"
      << "  --payload <bytes>        Echo payload size. Default 64\n"
      << "  --buffer-size <bytes>    URMA SEND chunk size. Default 4096 for CTP\n"
      << "  --queue-depth <n>        URMA send/recv queue depth. Default 64\n"
      << "  --max-memory-mib <n>     URMA buffer pool memory per process. Default 256, auto-raised when needed\n"
      << "  --no-urma               Use TCP instead of URMA RPC. Default URMA\n"
      << "  --poll-mode <threadpool|event|busy> Completion-detection mode. Default threadpool\n"
      << "                          threadpool: shared jfc poll thread pool (URMA_RPC_POLL_THREADS threads)\n"
      << "                          event: legacy per-socket event_loop (URMA_RPC_POLL_THREADS=0)\n"
      << "                          busy: legacy timer-based busy poller (URMA_RPC_EVENT_MODE=0)\n"
      << "  --poll-threads <n>      Poll threads (= jfc groups) in threadpool mode. Default 4. 0 falls back to event\n"
      << "  --profile                Enable in-memory stage latency profiling. Default off\n"
      << "  --profile-sample-rate <n> Record one sample every n events per stage/thread. Default 1\n"
      << "  --log <trace|debug|info|warn|error> Default info\n\n"
      << "Client options:\n"
      << "  --mode <latency|throughput|both> Default both\n"
      << "  --rpc <echo|sink|attach_sink> echo returns payload; sink returns payload size; attach_sink uses request attachment. Default echo\n"
      << "  --latency-iters <n>      Low-load serial requests. Default 10000\n"
      << "  --warmup-iters <n>       Warmup requests per client. Default 1000\n"
      << "  --concurrency <n>        Compatibility option; URMA throughput uses one worker per connection\n"
      << "  --connections <n>        URMA RPC connections and throughput workers. Default 64\n"
      << "  --pipeline-depth <n>     Outstanding RPC calls per connection in throughput mode. Default 1\n"
      << "  --duration <seconds>     Throughput duration. Default 10\n"
      << "  --raw-report-interval <seconds> Raw server report interval. Default 0 disables periodic reports\n"
      << "  --client-threads <n>     Client executor threads. Default hardware\n\n"
      << "Server options:\n"
      << "  --server-threads <n>     Server threads. Default hardware\n\n"
      << "Examples:\n"
      << "  same node server: " << program
      << " server --host 127.0.0.1 --port 9001\n"
      << "  same node client: " << program
      << " client --host 127.0.0.1 --port 9001 --mode both\n"
      << "  cross node server: " << program
      << " server --host 0.0.0.0 --port 9001\n"
      << "  cross node client: " << program
      << " client --host <server-ip> --port 9001 --mode throughput\n";
}

uint64_t parse_u64(std::string_view value, std::string_view name) {
  char* end = nullptr;
  auto str = std::string(value);
  auto result = std::strtoull(str.c_str(), &end, 10);
  if (end == str.c_str() || *end != '\0') {
    throw std::invalid_argument("invalid numeric option: " + std::string(name));
  }
  return result;
}

easylog::Severity parse_log_level(std::string_view value) {
  if (value == "trace") return easylog::Severity::TRACE;
  if (value == "debug") return easylog::Severity::DEBUG;
  if (value == "info") return easylog::Severity::INFO;
  if (value == "warn") return easylog::Severity::WARN;
  if (value == "error") return easylog::Severity::ERROR;
  throw std::invalid_argument("invalid --log value");
}

options_t parse_options(int argc, char** argv) {
  options_t opt;
  if (argc >= 2) opt.role = argv[1];
  if (opt.role != "server" && opt.role != "client") {
    print_usage(argv[0]);
    throw std::invalid_argument("role must be server or client");
  }

  bool host_was_set = false;
  for (int i = 2; i < argc; ++i) {
    std::string_view key = argv[i];
    auto require_value = [&]() -> std::string_view {
      if (i + 1 >= argc) {
        throw std::invalid_argument("missing value for " + std::string(key));
      }
      return argv[++i];
    };

    if (key == "--help" || key == "-h") {
      print_usage(argv[0]);
      std::exit(0);
    }
    else if (key == "--host") {
      opt.host = require_value();
      host_was_set = true;
    }
    else if (key == "--port") {
      opt.port = static_cast<uint16_t>(parse_u64(require_value(), key));
    }
    else if (key == "--transport") {
      opt.transport = require_value();
    }
    else if (key == "--device") {
      opt.device = require_value();
    }
    else if (key == "--eid-index") {
      opt.eid_index = static_cast<int>(parse_u64(require_value(), key));
    }
    else if (key == "--payload") {
      opt.payload_size = static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--buffer-size") {
      opt.buffer_size = static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--queue-depth") {
      opt.queue_depth = static_cast<uint16_t>(parse_u64(require_value(), key));
    }
    else if (key == "--max-memory-mib") {
      opt.max_memory_usage = parse_u64(require_value(), key) * 1024 * 1024;
    }
    else if (key == "--profile") {
      opt.profile = true;
    }
    else if (key == "--no-urma") {
      opt.use_urma = false;
    }
    else if (key == "--poll-mode") {
      opt.poll_mode = require_value();
    }
    else if (key == "--poll-threads") {
      opt.poll_threads =
          static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--profile-sample-rate") {
      opt.profile_sample_rate =
          static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--mode") {
      opt.mode = require_value();
    }
    else if (key == "--rpc") {
      opt.rpc = require_value();
    }
    else if (key == "--latency-iters") {
      opt.latency_iters =
          static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--warmup-iters") {
      opt.warmup_iters =
          static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--concurrency") {
      opt.concurrency = static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--connections") {
      opt.connections = static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--pipeline-depth") {
      opt.pipeline_depth =
          static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--duration") {
      opt.duration_seconds =
          static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--raw-report-interval") {
      opt.raw_report_interval_seconds =
          static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--server-threads") {
      opt.server_threads =
          static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--client-threads") {
      opt.client_threads =
          static_cast<uint32_t>(parse_u64(require_value(), key));
    }
    else if (key == "--log") {
      opt.log_level = parse_log_level(require_value());
    }
    else {
      throw std::invalid_argument("unknown option: " + std::string(key));
    }
  }

  if (opt.mode != "latency" && opt.mode != "throughput" &&
      opt.mode != "both") {
    throw std::invalid_argument("--mode must be latency, throughput, or both");
  }
  if (opt.transport != "rpc" && opt.transport != "raw") {
    throw std::invalid_argument("--transport must be rpc or raw");
  }
  if (opt.rpc != "echo" && opt.rpc != "sink" && opt.rpc != "attach_sink") {
    throw std::invalid_argument("--rpc must be echo, sink, or attach_sink");
  }
  if (opt.poll_mode != "threadpool" && opt.poll_mode != "event" &&
      opt.poll_mode != "busy") {
    throw std::invalid_argument(
        "--poll-mode must be threadpool, event, or busy");
  }
  opt.connections = std::max<uint32_t>(opt.connections, 1);
  opt.pipeline_depth = std::max<uint32_t>(opt.pipeline_depth, 1);
  opt.concurrency = std::max<uint32_t>(opt.concurrency, opt.connections);
  opt.queue_depth = std::max<uint16_t>(opt.queue_depth, 1);
  opt.profile_sample_rate = std::max<uint32_t>(opt.profile_sample_rate, 1);
  if (opt.role == "server" && !host_was_set) {
    opt.host = "0.0.0.0";
  }
  return opt;
}

uint64_t saturated_mul(uint64_t lhs, uint64_t rhs) {
  if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
    return std::numeric_limits<uint64_t>::max();
  }
  return lhs * rhs;
}

uint64_t ceil_div(uint64_t value, uint64_t divisor) {
  return divisor == 0 ? value : (value + divisor - 1) / divisor;
}

uint64_t estimated_pool_memory_usage(const options_t& opt) {
  auto buffer_size = std::max<uint64_t>(opt.buffer_size, 1);
  auto connections = std::max<uint64_t>(opt.connections, 1);
  auto queue_depth = std::max<uint64_t>(opt.queue_depth, 1);
  auto payload_chunks =
      ceil_div(static_cast<uint64_t>(opt.payload_size) + 1024, buffer_size);

  // Each connection keeps recv WRs posted and can also cache completed recv
  // buffers while the RPC layer consumes a large message. Keep enough slack for
  // send completions and protocol framing without requiring users to guess.
  auto payload_slack = std::min<uint64_t>(payload_chunks, queue_depth);
  auto buffers_per_connection =
      queue_depth * 2 + queue_depth + payload_slack + 16;
  auto required_buffers =
      saturated_mul(connections, buffers_per_connection) + 1024;
  return saturated_mul(required_buffers, buffer_size);
}

uint64_t effective_pool_memory_usage(const options_t& opt) {
  return std::max(opt.max_memory_usage, estimated_pool_memory_usage(opt));
}

coro_io::urma_socket_t::config_t make_urma_config(const options_t& opt) {
  return coro_io::urma_socket_t::config_t{
      .cq_size = static_cast<uint32_t>(opt.queue_depth * 2 + 8),
      .recv_buffer_cnt = opt.queue_depth,
      .send_buffer_cnt = opt.queue_depth,
      .buffer_size = opt.buffer_size,
      .max_memory_usage = effective_pool_memory_usage(opt),
      .device_name = opt.device,
      .eid_index = opt.eid_index,
      .tp_type = URMA_CTP};
}

bool init_global_urma(const options_t& opt) {
  status("initializing global URMA device");
  auto pool_memory = effective_pool_memory_usage(opt);
  if (pool_memory > opt.max_memory_usage) {
    std::cout << "[urma_benchmark] auto raise URMA buffer pool memory from "
              << opt.max_memory_usage / 1024 / 1024 << " MiB to "
              << pool_memory / 1024 / 1024
              << " MiB for payload/connections/queue-depth" << std::endl;
  }
  auto device = coro_io::get_global_urma_device(coro_io::urma_init_config_t{
      .dev_name = opt.device,
      .buffer_pool_config =
          {
              .buffer_size = opt.buffer_size,
              .max_memory_usage = pool_memory,
              .idle_timeout = 5s,
          },
      .eid_index = opt.eid_index});
  if (!device || !device->is_valid() || !device->get_buffer_pool()) {
    std::cerr << "[urma_benchmark] failed to initialize global URMA device"
              << std::endl;
    return false;
  }
  auto pool = device->get_buffer_pool();
  std::cout << "[urma_benchmark] global URMA device initialized, pool_buffers="
            << pool->total_buffer_count()
            << ", pool_free=" << pool->free_buffer_count()
            << ", pool_memory_mib="
            << pool->total_memory_size() / 1024 / 1024 << std::endl;
  return true;
}

Lazy<bool> connect_client(coro_rpc_client& client, const options_t& opt,
                          uint32_t client_index = 0) {
  std::cout << "[urma_benchmark] client " << client_index
            << " using URMA RPC auto configuration" << std::endl;
  std::cout << "[urma_benchmark] client " << client_index << " connecting to "
            << opt.host << ":" << opt.port << std::endl;
  auto ec = co_await client.connect(opt.host, std::to_string(opt.port), 30s);
  if (ec) {
    ELOG_ERROR << "connect failed: " << ec.message()
               << ". Check that the server process is running, listening on "
               << "0.0.0.0 or the requested NIC address, and that the TCP "
               << "handshake port is reachable: " << opt.host << ":"
               << opt.port;
    co_return false;
  }
  std::cout << "[urma_benchmark] client " << client_index << " connected"
            << std::endl;
  co_return true;
}

Lazy<bool> issue_rpc_call(coro_rpc_client& client, const std::string& payload,
                          std::string_view rpc, bool profile_call = true);

Lazy<bool> warmup(coro_rpc_client& client, const std::string& payload,
                  uint32_t count, std::string_view rpc,
                  uint32_t client_index = 0) {
  if (count == 0) co_return true;
  std::cout << "[urma_benchmark] client " << client_index
            << " warmup start, iterations=" << count << std::endl;
  for (uint32_t i = 0; i < count; ++i) {
    if (!(co_await issue_rpc_call(client, payload, rpc, false))) {
      ELOG_ERROR << "warmup failed at iteration " << i;
      co_return false;
    }
  }
  std::cout << "[urma_benchmark] client " << client_index << " warmup done"
            << std::endl;
  co_return true;
}

Lazy<bool> issue_rpc_call(coro_rpc_client& client, const std::string& payload,
                          std::string_view rpc, bool profile_call) {
  auto profile_begin = profile_call && coro_io::urma_benchmark_profile::enabled()
                           ? coro_io::urma_benchmark_profile::now_ns()
                           : 0;
  auto payload_size = payload.size();
  bool ok = false;
  if (rpc == "attach_sink") {
    auto result = co_await client.call<bench_attachment_sink>(
        request_config_t{30s, payload, {}, -1, -1});
    ok = result && result.value() == payload.size();
    if (profile_call) {
      coro_io::urma_benchmark_profile::record_since_with_size(
          coro_io::urma_benchmark_profile::stage::benchmark_rpc_call,
          profile_begin, payload_size);
    }
    co_return ok;
  }
  if (rpc == "sink") {
    auto result = co_await client.call_for<bench_sink>(30s, payload);
    ok = result && result.value() == payload.size();
    if (profile_call) {
      coro_io::urma_benchmark_profile::record_since_with_size(
          coro_io::urma_benchmark_profile::stage::benchmark_rpc_call,
          profile_begin, payload_size);
    }
    co_return ok;
  }
  auto result = co_await client.call_for<bench_echo>(30s, payload);
  ok = result && result.value().size() == payload.size();
  if (profile_call) {
    coro_io::urma_benchmark_profile::record_since_with_size(
        coro_io::urma_benchmark_profile::stage::benchmark_rpc_call,
        profile_begin, payload_size);
  }
  co_return ok;
}

void print_latency_result(std::vector<uint64_t>& samples) {
  if (samples.empty()) {
    std::cout << "latency: no samples\n";
    return;
  }
  std::sort(samples.begin(), samples.end());
  auto percentile = [&](double p) {
    auto index = static_cast<std::size_t>((samples.size() - 1) * p);
    return samples[index];
  };
  auto sum = std::accumulate(samples.begin(), samples.end(), uint64_t{0});
  auto avg = static_cast<double>(sum) / static_cast<double>(samples.size());
  std::cout << std::fixed << std::setprecision(2)
            << "latency_us count=" << samples.size() << " avg=" << avg
            << " min=" << samples.front() << " p50=" << percentile(0.50)
            << " p90=" << percentile(0.90) << " p99=" << percentile(0.99)
            << " p999=" << percentile(0.999) << " max=" << samples.back()
            << "\n";
}

Lazy<void> run_latency(const options_t& opt, const std::string& payload) {
  status("latency test starting");
  coro_rpc_client client;
  if (!(co_await connect_client(client, opt))) co_return;
  if (!(co_await warmup(client, payload, opt.warmup_iters, opt.rpc)))
    co_return;

  std::vector<uint64_t> samples;
  samples.reserve(opt.latency_iters);
  std::cout << "[urma_benchmark] latency measurement start, iterations="
            << opt.latency_iters << std::endl;
  coro_io::urma_benchmark_profile::configure(opt.profile,
                                             opt.profile_sample_rate);
  for (uint32_t i = 0; i < opt.latency_iters; ++i) {
    auto begin = std::chrono::steady_clock::now();
    bool ok = co_await issue_rpc_call(client, payload, opt.rpc);
    auto end = std::chrono::steady_clock::now();
    if (!ok) {
      ELOG_ERROR << "latency call failed at iteration " << i;
      break;
    }
    samples.push_back(static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(end - begin)
            .count()));
  }
  coro_io::urma_benchmark_profile::configure(false, opt.profile_sample_rate);
  print_latency_result(samples);
  status("latency test finished");
}

struct worker_result_t {
  uint64_t requests = 0;
  uint64_t errors = 0;
  uint64_t bytes = 0;
};

Lazy<worker_result_t> throughput_worker(coro_rpc_client& client,
                                         const std::string& payload,
                                         std::string_view rpc,
                                         uint32_t pipeline_depth,
                                         std::chrono::steady_clock::time_point
                                             deadline) {
  worker_result_t stat;
  while (std::chrono::steady_clock::now() < deadline) {
    std::vector<Lazy<bool>> calls;
    calls.reserve(pipeline_depth);
    for (uint32_t i = 0; i < pipeline_depth &&
                         std::chrono::steady_clock::now() < deadline;
         ++i) {
      calls.push_back(issue_rpc_call(client, payload, rpc));
    }
    if (calls.empty()) break;
    auto results = co_await collectAll(std::move(calls));
    for (auto& item : results) {
      bool ok = item.value();
      if (!ok) {
        ++stat.errors;
        continue;
      }
      ++stat.requests;
      stat.bytes += payload.size();
    }
  }
  co_return stat;
}

Lazy<void> run_throughput(const options_t& opt, const std::string& payload) {
  status("throughput test preparing clients");
  std::vector<std::unique_ptr<coro_rpc_client>> clients;
  clients.reserve(opt.connections);
  for (uint32_t i = 0; i < opt.connections; ++i) {
    auto client = std::make_unique<coro_rpc_client>();
    if (!(co_await connect_client(*client, opt, i))) co_return;
    if (!(co_await warmup(*client, payload, opt.warmup_iters, opt.rpc, i)))
      co_return;
    clients.push_back(std::move(client));
  }

  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(opt.duration_seconds);
  std::vector<Lazy<worker_result_t>> workers;
  workers.reserve(clients.size());
  for (auto& client : clients) {
    workers.push_back(throughput_worker(*client, payload, opt.rpc,
                                        opt.pipeline_depth, deadline));
  }

  std::cout << "[urma_benchmark] throughput measurement start, duration_s="
            << opt.duration_seconds << ", workers=" << workers.size()
            << ", connections=" << opt.connections
            << ", pipeline_depth=" << opt.pipeline_depth
            << ", requested_concurrency=" << opt.concurrency
            << ". URMA throughput uses one serial worker per connection."
            << std::endl;
  coro_io::urma_benchmark_profile::configure(opt.profile,
                                             opt.profile_sample_rate);
  auto begin = std::chrono::steady_clock::now();
  auto results = co_await collectAll(std::move(workers));
  auto end = std::chrono::steady_clock::now();
  coro_io::urma_benchmark_profile::configure(false, opt.profile_sample_rate);

  worker_result_t total;
  for (auto& item : results) {
    auto result = item.value();
    total.requests += result.requests;
    total.errors += result.errors;
    total.bytes += result.bytes;
  }

  auto seconds =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - begin)
          .count();
  auto rps = static_cast<double>(total.requests) / seconds;
  auto mbps = static_cast<double>(total.bytes) / seconds / 1024.0 / 1024.0;
  std::cout << std::fixed << std::setprecision(2)
            << "throughput duration_s=" << seconds
            << " requests=" << total.requests << " errors=" << total.errors
            << " rps=" << rps << " payload_mib_per_s=" << mbps
            << " workers=" << clients.size()
            << " connections=" << opt.connections << "\n";
  status("throughput test finished");
}

struct raw_result_t {
  uint64_t messages = 0;
  uint64_t errors = 0;
  uint64_t bytes = 0;
};

Lazy<void> raw_server_session(asio::ip::tcp::socket tcp_socket,
                              const options_t& opt) {
  coro_io::urma_socket_t socket(coro_io::get_global_executor(),
                                make_urma_config(opt));
  auto ec = co_await socket.accept(std::move(tcp_socket));
  if (ec) {
    ELOG_ERROR << "raw URMA accept failed: " << ec.message();
    co_return;
  }

  std::vector<char> buffer(opt.payload_size);
  uint64_t messages = 0;
  uint64_t bytes = 0;
  auto last = std::chrono::steady_clock::now();
  for (;;) {
    auto [read_ec, read_size] =
        co_await coro_io::async_read(socket, asio::buffer(buffer));
    if (read_ec) {
      ELOG_DEBUG << "raw URMA session closed: " << read_ec.message()
                 << ", messages=" << messages << ", bytes=" << bytes;
      co_return;
    }
    ++messages;
    bytes += read_size;
    if (opt.raw_report_interval_seconds == 0) continue;
    auto now = std::chrono::steady_clock::now();
    if (now - last >= std::chrono::seconds(opt.raw_report_interval_seconds)) {
      auto seconds =
          std::chrono::duration_cast<std::chrono::duration<double>>(now - last)
              .count();
      std::cout << "[urma_benchmark] raw server ingress payload_mib_per_s="
                << static_cast<double>(bytes) / seconds / 1024.0 / 1024.0
                << ", messages=" << messages << std::endl;
      messages = 0;
      bytes = 0;
      last = now;
    }
  }
}

Lazy<void> run_raw_server(const options_t& opt) {
  status("raw URMA server starting");
  if (!init_global_urma(opt)) co_return;
  auto executor = coro_io::get_global_executor(opt.server_threads);
  asio::ip::tcp::endpoint endpoint(asio::ip::make_address(opt.host), opt.port);
  asio::ip::tcp::acceptor acceptor(executor->context(), endpoint);
  std::cout << "raw URMA server listening on " << opt.host << ":" << opt.port
            << ", payload=" << opt.payload_size
            << ", buffer_size=" << opt.buffer_size
            << ", queue_depth=" << opt.queue_depth << std::endl;
  for (;;) {
    asio::ip::tcp::socket tcp_socket(executor->context());
    auto ec = co_await coro_io::async_accept(acceptor, tcp_socket);
    if (ec) {
      ELOG_ERROR << "raw accept failed: " << ec.message();
      co_return;
    }
    raw_server_session(std::move(tcp_socket), opt).start([](auto&&) {
    });
  }
}

Lazy<raw_result_t> raw_client_worker(const options_t& opt,
                                     const std::string& payload,
                                     std::chrono::steady_clock::time_point
                                         deadline,
                                     uint32_t client_index) {
  raw_result_t stat;
  coro_io::urma_socket_t socket(coro_io::get_global_executor(),
                                make_urma_config(opt));
  auto ec = co_await socket.connect(opt.host, std::to_string(opt.port));
  if (ec) {
    ELOG_ERROR << "raw client " << client_index << " connect failed: "
               << ec.message();
    stat.errors++;
    co_return stat;
  }
  while (std::chrono::steady_clock::now() < deadline) {
    auto profile_begin = coro_io::urma_benchmark_profile::enabled()
                             ? coro_io::urma_benchmark_profile::now_ns()
                             : 0;
    auto [write_ec, written] =
        co_await coro_io::async_write(socket, asio::buffer(payload));
    coro_io::urma_benchmark_profile::record_since(
        coro_io::urma_benchmark_profile::stage::raw_client_write,
        profile_begin);
    if (write_ec || written != payload.size()) {
      ++stat.errors;
      continue;
    }
    ++stat.messages;
    stat.bytes += written;
  }
  co_return stat;
}

Lazy<void> run_raw_client(const options_t& opt, const std::string& payload) {
  status("raw URMA throughput test starting");
  if (!init_global_urma(opt)) co_return;
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(opt.duration_seconds);
  std::vector<Lazy<raw_result_t>> workers;
  workers.reserve(opt.connections);
  for (uint32_t i = 0; i < opt.connections; ++i) {
    workers.push_back(raw_client_worker(opt, payload, deadline, i));
  }

  auto begin = std::chrono::steady_clock::now();
  coro_io::urma_benchmark_profile::configure(opt.profile,
                                             opt.profile_sample_rate);
  auto results = co_await collectAll(std::move(workers));
  auto end = std::chrono::steady_clock::now();
  coro_io::urma_benchmark_profile::configure(false, opt.profile_sample_rate);
  raw_result_t total;
  for (auto& item : results) {
    auto result = item.value();
    total.messages += result.messages;
    total.errors += result.errors;
    total.bytes += result.bytes;
  }
  auto seconds =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - begin)
          .count();
  std::cout << std::fixed << std::setprecision(2)
            << "raw_throughput duration_s=" << seconds
            << " messages=" << total.messages << " errors=" << total.errors
            << " payload_mib_per_s="
            << static_cast<double>(total.bytes) / seconds / 1024.0 / 1024.0
            << " connections=" << opt.connections << "\n";
  status("raw URMA throughput test finished");
}

int run_server(const options_t& opt) {
  if (opt.transport == "raw") {
    syncAwait(run_raw_server(opt));
    return 0;
  }
  std::cout << "[urma_benchmark] server starting, listen=" << opt.host << ":"
            << opt.port << ", device=" << opt.device
            << ", eid_index=" << opt.eid_index
            << ", buffer_size=" << opt.buffer_size
            << ", queue_depth=" << opt.queue_depth
            << ", max_memory_mib="
            << effective_pool_memory_usage(opt) / 1024 / 1024
            << ", poll_mode=" << opt.poll_mode
            << ", poll_threads=" << opt.poll_threads << std::endl;
  if (opt.use_urma) {
    configure_urma_rpc_env(opt);
    if (!init_global_urma(opt)) return 1;
    status("constructing RPC server with URMA RPC auto configuration");
  } else {
    status("constructing RPC server with TCP (URMA disabled)");
  }
  coro_rpc_server server(opt.server_threads, opt.port, opt.host);
  server.register_handler<bench_echo>();
  server.register_handler<bench_sink>();
  server.register_handler<bench_attachment_sink>();
  std::cout << (opt.use_urma ? "URMA" : "TCP")
            << " RPC benchmark server listening on " << opt.host << ":"
            << opt.port << ", device=" << opt.device
            << ", eid_index=" << opt.eid_index
            << ", buffer_size=" << opt.buffer_size
            << ", queue_depth=" << opt.queue_depth
            << ", max_memory_mib="
            << effective_pool_memory_usage(opt) / 1024 / 1024 << std::endl;
  status("entering server event loop");
  return !server.start();
}

int run_client(const options_t& opt) {
  std::cout << "[urma_benchmark] client starting, target=" << opt.host << ":"
            << opt.port << ", mode=" << opt.mode
            << ", transport=" << opt.transport
            << ", rpc=" << opt.rpc
            << ", payload=" << opt.payload_size
            << ", buffer_size=" << opt.buffer_size
            << ", queue_depth=" << opt.queue_depth
            << ", max_memory_mib="
            << effective_pool_memory_usage(opt) / 1024 / 1024
            << ", connections=" << opt.connections
            << ", pipeline_depth=" << opt.pipeline_depth
            << ", total_outstanding="
            << static_cast<uint64_t>(opt.connections) * opt.pipeline_depth
            << ", concurrency=" << opt.concurrency
            << ", use_urma=" << (opt.use_urma ? "on" : "off")
            << ", poll_mode=" << opt.poll_mode
            << ", poll_threads=" << opt.poll_threads
            << ", profile=" << (opt.profile ? "on" : "off")
            << ", profile_sample_rate=" << opt.profile_sample_rate
            << std::endl;
  coro_io::get_global_executor(opt.client_threads);
  std::string payload(opt.payload_size, 'x');
  if (opt.transport == "raw") {
    syncAwait(run_raw_client(opt, payload));
    if (opt.profile) coro_io::urma_benchmark_profile::print(std::cout);
    return 0;
  }
  if (opt.use_urma) {
    configure_urma_rpc_env(opt);
    if (!init_global_urma(opt)) return 1;
  }
  std::cout << (opt.use_urma ? "URMA" : "TCP")
            << " RPC benchmark client target " << opt.host << ":"
            << opt.port << ", mode=" << opt.mode
            << ", rpc=" << opt.rpc
            << ", payload=" << opt.payload_size
            << ", buffer_size=" << opt.buffer_size
            << ", queue_depth=" << opt.queue_depth << std::endl;

  if (opt.mode == "latency" || opt.mode == "both") {
    syncAwait(run_latency(opt, payload));
  }
  if (opt.mode == "throughput" || opt.mode == "both") {
    syncAwait(run_throughput(opt, payload));
  }
  if (opt.profile) coro_io::urma_benchmark_profile::print(std::cout);
  return 0;
}

int main(int argc, char** argv) {
  try {
    std::cout.setf(std::ios::unitbuf);
    std::cerr.setf(std::ios::unitbuf);
    auto opt = parse_options(argc, argv);
    coro_io::urma_benchmark_profile::configure(false,
                                               opt.profile_sample_rate);
    easylog::logger<>::instance().set_min_severity(opt.log_level);
    easylog::logger<>::instance().set_async(false);
    if (opt.role == "server") return run_server(opt);
    return run_client(opt);
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
}

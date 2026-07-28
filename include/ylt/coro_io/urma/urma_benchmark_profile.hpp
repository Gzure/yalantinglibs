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
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string_view>
#include <vector>

namespace coro_io::urma_benchmark_profile {

// Payload size buckets in bytes.
//   0: [0, 100)
//   1: [100, 500)
//   2: [500, 1K)
//   3..N: [1K, 2K), [2K, 3K), ... each 1K wide
inline constexpr std::size_t bucket_count = 16;
inline constexpr std::array<uint32_t, bucket_count> bucket_upper = {
    100,   500,   1000,  2000,  3000,  4000,  5000,  6000,
    7000,  8000,  9000,  10000, 12000, 16000, 32000, UINT32_MAX,
};
inline constexpr std::array<std::string_view, bucket_count> bucket_names = {
    "0-100",     "100-500",   "500-1K",    "1K-2K",     "2K-3K",
    "3K-4K",     "4K-5K",     "5K-6K",     "6K-7K",     "7K-8K",
    "8K-9K",     "9K-10K",    "10K-12K",   "12K-16K",   "16K-32K",
    "32K+",
};

inline std::size_t size_to_bucket(std::size_t payload_size) {
  for (std::size_t i = 0; i < bucket_count; ++i) {
    if (payload_size < bucket_upper[i]) return i;
  }
  return bucket_count - 1;
}

enum class stage : uint8_t {
  benchmark_rpc_call = 0,
  client_prepare_request,
  client_send_request,
  client_recv_header,
  client_recv_payload,
  client_deserialize_response,
  server_read_header,
  server_read_payload,
  server_deserialize_request,
  server_handler_execute,
  server_dispatch,
  server_serialize_response,
  server_serialize_result,
  server_response_queue,
  server_send_response,
  client_connect_total,
  client_connect_tcp,
  client_connect_handshake,
  urma_write_total,
  urma_write_copy,
  urma_post_send,
  urma_wait_send_completion,
  urma_read_wait_completion,
  urma_read_copy,
  urma_read_view,
  raw_client_write,
  count
};

inline constexpr std::array<std::string_view,
                            static_cast<std::size_t>(stage::count)>
    stage_names = {
        "benchmark.rpc_call",
        "client.prepare_request",
        "client.send_request",
        "client.recv_header",
        "client.recv_payload",
        "client.deserialize_response",
        "server.read_header",
        "server.read_payload",
        "server.deserialize_request",
        "server.handler_execute",
        "server.dispatch",
        "server.serialize_response",
        "server.serialize_result",
        "server.response_queue",
        "server.send_response",
        "client.connect_total",
        "client.connect_tcp",
        "client.connect_handshake",
        "urma.write_total",
        "urma.write_copy",
        "urma.post_send",
        "urma.wait_send_completion",
        "urma.read_wait_completion",
        "urma.read_copy",
        "urma.read_view",
        "raw.client_write",
};

inline std::atomic<bool>& enabled_flag() {
  static std::atomic<bool> value{false};
  return value;
}

inline std::atomic<uint32_t>& sample_rate_value() {
  static std::atomic<uint32_t> value{1};
  return value;
}

inline void print(std::ostream& os);

inline void init_from_env() {
  static std::once_flag flag;
  std::call_once(flag, [] {
    const char* env = std::getenv("YLT_RPC_PROFILE_ENABLE");
    if (env && (std::string_view(env) == "1" ||
                std::string_view(env) == "on" ||
                std::string_view(env) == "true")) {
      enabled_flag().store(true, std::memory_order_relaxed);
    }
    const char* rate = std::getenv("YLT_RPC_PROFILE_SAMPLE_RATE");
    if (rate && *rate) {
      uint32_t r = 0;
      for (const char* p = rate; *p >= '0' && *p <= '9'; ++p)
        r = r * 10 + (*p - '0');
      if (r > 0) sample_rate_value().store(r, std::memory_order_relaxed);
    }
    if (enabled_flag().load(std::memory_order_relaxed)) {
      std::atexit([]() {
        std::fprintf(stderr, "[rpc_profile] atexit: printing profile\n");
        print(std::cerr);
      });
    }
  });
}

inline bool enabled() noexcept {
  if (enabled_flag().load(std::memory_order_relaxed)) return true;
  init_from_env();
  return enabled_flag().load(std::memory_order_relaxed);
}

inline void configure(bool enabled, uint32_t sample_rate) noexcept {
  enabled_flag().store(enabled, std::memory_order_relaxed);
  sample_rate_value().store(std::max<uint32_t>(sample_rate, 1),
                            std::memory_order_relaxed);
}

inline uint64_t now_ns() noexcept {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

// Per-thread, per-stage, per-bucket samples.
struct thread_samples {
  std::array<std::array<std::vector<uint64_t>, bucket_count>,
             static_cast<std::size_t>(stage::count)>
      samples;
  std::array<uint32_t, static_cast<std::size_t>(stage::count)> counters{};
  ~thread_samples();
};

inline std::mutex& registry_mutex() {
  static std::mutex value;
  return value;
}

inline std::array<std::array<std::vector<uint64_t>, bucket_count>,
                  static_cast<std::size_t>(stage::count)>&
merged_samples() {
  static std::array<std::array<std::vector<uint64_t>, bucket_count>,
                    static_cast<std::size_t>(stage::count)>
      value;
  return value;
}

inline std::array<uint32_t, static_cast<std::size_t>(stage::count)>&
merged_counters() {
  static std::array<uint32_t, static_cast<std::size_t>(stage::count)> value{};
  return value;
}

inline std::vector<thread_samples*>& registry() {
  static std::vector<thread_samples*> value;
  return value;
}

inline thread_samples& local_samples() {
  thread_local thread_samples value;
  thread_local bool registered = [] {
    std::lock_guard<std::mutex> lock(registry_mutex());
    registry().push_back(&value);
    return true;
  }();
  (void)registered;
  return value;
}

inline thread_samples::~thread_samples() {
  std::lock_guard<std::mutex> lock(registry_mutex());
  auto& dst = merged_samples();
  auto& dst_cnt = merged_counters();
  for (std::size_t s = 0; s < samples.size(); ++s) {
    dst_cnt[s] += counters[s];
    for (std::size_t b = 0; b < bucket_count; ++b) {
      dst[s][b].insert(dst[s][b].end(), samples[s][b].begin(),
                       samples[s][b].end());
      samples[s][b].clear();
    }
  }
  auto& reg = registry();
  reg.erase(std::remove(reg.begin(), reg.end(), this), reg.end());
}

inline void record_with_size(stage stage_id, uint64_t duration_ns,
                             std::size_t payload_size) {
  if (!enabled()) return;
  auto& local = local_samples();
  auto s = static_cast<std::size_t>(stage_id);
  auto b = size_to_bucket(payload_size);
  auto rate = sample_rate_value().load(std::memory_order_relaxed);
  auto counter = ++local.counters[s];
  if ((counter % rate) != 0) return;
  local.samples[s][b].push_back(duration_ns);
}

inline void record(stage stage_id, uint64_t duration_ns) {
  record_with_size(stage_id, duration_ns, 0);
}

inline void record_since(stage stage_id, uint64_t begin_ns) {
  if (!enabled()) return;
  auto end_ns = now_ns();
  if (end_ns >= begin_ns) record(stage_id, end_ns - begin_ns);
}

inline void record_since_with_size(stage stage_id, uint64_t begin_ns,
                                  std::size_t payload_size) {
  if (!enabled()) return;
  auto end_ns = now_ns();
  if (end_ns >= begin_ns)
    record_with_size(stage_id, end_ns - begin_ns, payload_size);
}

inline void reserve_per_stage(std::size_t count) {
  if (!enabled()) return;
  auto& local = local_samples();
  for (auto& stage_buckets : local.samples)
    for (auto& bucket : stage_buckets) bucket.reserve(count);
}

inline void print(std::ostream& os) {
  // merged[stage][bucket] -> samples
  std::array<std::array<std::vector<uint64_t>, bucket_count>,
             static_cast<std::size_t>(stage::count)>
      merged;
  std::array<uint32_t, static_cast<std::size_t>(stage::count)> total_counters{};
  {
    std::lock_guard<std::mutex> lock(registry_mutex());
    for (std::size_t s = 0; s < merged.size(); ++s) {
      total_counters[s] += merged_counters()[s];
      for (std::size_t b = 0; b < bucket_count; ++b) {
        auto& src = merged_samples()[s][b];
        merged[s][b].insert(merged[s][b].end(), src.begin(), src.end());
      }
    }
    for (auto* thread : registry()) {
      if (thread == nullptr) continue;
      for (std::size_t s = 0; s < merged.size(); ++s) {
        total_counters[s] += thread->counters[s];
        for (std::size_t b = 0; b < bucket_count; ++b) {
          auto& src = thread->samples[s][b];
          merged[s][b].insert(merged[s][b].end(), src.begin(), src.end());
        }
      }
    }
  }

  os << "rpc_profile sample_rate="
     << sample_rate_value().load(std::memory_order_relaxed)
     << " unit=us\n";
  os << std::fixed << std::setprecision(2);

  // stats for one stage (merge all buckets)
  struct stats {
    std::size_t calls = 0, sampled = 0;
    double avg = 0, p50 = 0, p90 = 0, p99 = 0, p999 = 0, max = 0;
  };
  auto compute = [&](std::size_t s) -> stats {
    stats st;
    st.calls = total_counters[s];
    std::vector<uint64_t> all;
    for (std::size_t b = 0; b < bucket_count; ++b) {
      auto& v = merged[s][b];
      all.insert(all.end(), v.begin(), v.end());
    }
    if (all.empty()) return st;
    st.sampled = all.size();
    std::sort(all.begin(), all.end());
    auto pct = [&](double p) {
      return static_cast<double>(all[static_cast<std::size_t>((all.size()-1)*p)]) / 1000.0;
    };
    auto sum = std::accumulate(all.begin(), all.end(), 0LL);
    st.avg = static_cast<double>(sum) / all.size() / 1000.0;
    st.p50 = pct(0.50); st.p90 = pct(0.90); st.p99 = pct(0.99);
    st.p999 = pct(0.999); st.max = static_cast<double>(all.back()) / 1000.0;
    return st;
  };
  // stats for one bucket of one stage
  auto compute_b = [&](std::size_t s, std::size_t b) -> stats {
    stats st;
    st.calls = total_counters[s];
    auto& v = merged[s][b];
    if (v.empty()) return st;
    st.sampled = v.size();
    std::sort(v.begin(), v.end());
    auto pct = [&](double p) {
      return static_cast<double>(v[static_cast<std::size_t>((v.size()-1)*p)]) / 1000.0;
    };
    auto sum = std::accumulate(v.begin(), v.end(), 0LL);
    st.avg = static_cast<double>(sum) / v.size() / 1000.0;
    st.p50 = pct(0.50); st.p90 = pct(0.90); st.p99 = pct(0.99);
    st.p999 = pct(0.999); st.max = static_cast<double>(v.back()) / 1000.0;
    return st;
  };
  (void)compute_b;  // bucket detail available if needed later

  // Print a tree node: indent + branch char + label + stats
  auto node = [&](int indent, bool last, std::string_view label, const stats& st) {
    for (int i = 0; i < indent; ++i) os << (i < indent - 1 ? "│  " : "   ");
    if (indent > 0) os << (last ? "└─ " : "├─ ");
    os << std::left << std::setw(28) << label
       << " n=" << std::right << std::setw(8) << st.sampled
       << " avg=" << std::setw(10) << st.avg
       << " p50=" << std::setw(10) << st.p50
       << " p90=" << std::setw(10) << st.p90
       << " p99=" << std::setw(10) << st.p99
       << " p999=" << std::setw(10) << st.p999
       << " max=" << std::setw(10) << st.max
       << "\n";
  };
  // Print a parent node (with children)
  auto parent = [&](int indent, bool last, std::string_view label, const stats& st) {
    for (int i = 0; i < indent; ++i) os << (i < indent - 1 ? "│  " : "   ");
    if (indent > 0) os << (last ? "└─ " : "├─ ");
    os << std::left << std::setw(28) << label
       << " n=" << std::right << std::setw(8) << st.sampled
       << " avg=" << std::setw(10) << st.avg
       << " p50=" << std::setw(10) << st.p50
       << " p99=" << std::setw(10) << st.p99
       << " max=" << std::setw(10) << st.max
       << "\n";
  };
  auto has = [&](std::size_t s) { return total_counters[s] > 0; };
  auto st = [&](stage s) { return compute(static_cast<std::size_t>(s)); };

  // Print bucket breakdown for a stage under the current parent
  auto buckets = [&](int indent, stage s) {
    for (std::size_t b = 0; b < bucket_count; ++b) {
      auto& v = merged[static_cast<std::size_t>(s)][b];
      if (v.empty()) continue;
      auto bst = compute_b(static_cast<std::size_t>(s), b);
      for (int i = 0; i < indent; ++i) os << "  ";
      os << "  [" << bucket_names[b] << "]"
         << " n=" << std::right << std::setw(8) << bst.sampled
         << " avg=" << std::setw(10) << bst.avg
         << " p50=" << std::setw(10) << bst.p50
         << " p99=" << std::setw(10) << bst.p99
         << " max=" << std::setw(10) << bst.max
         << "\n";
    }
  };

  // ── Benchmark ──
  if (has(static_cast<std::size_t>(stage::benchmark_rpc_call))) {
    os << "\n[benchmark]\n";
    parent(0, true, "rpc_call (total)", st(stage::benchmark_rpc_call));
    buckets(1, stage::benchmark_rpc_call);
  }

  // ── Client ──
  os << "\n[client]\n";
  if (has(static_cast<std::size_t>(stage::client_send_request))) {
    parent(0, false, "send_request", st(stage::client_send_request));
    buckets(1, stage::client_send_request);
    if (has(static_cast<std::size_t>(stage::client_prepare_request)))
      node(1, !has(static_cast<std::size_t>(stage::urma_write_total)),
           "prepare_request", st(stage::client_prepare_request));
    if (has(static_cast<std::size_t>(stage::urma_write_total))) {
      parent(1, true, "urma.write_total", st(stage::urma_write_total));
      bool has_wc = has(static_cast<std::size_t>(stage::urma_write_copy));
      bool has_ps = has(static_cast<std::size_t>(stage::urma_post_send));
      bool has_wsc = has(static_cast<std::size_t>(stage::urma_wait_send_completion));
      int cnt = has_wc + has_ps + has_wsc;
      if (has_wc) node(2, --cnt == 0, "write_copy", st(stage::urma_write_copy));
      if (has_ps) node(2, --cnt == 0, "post_send", st(stage::urma_post_send));
      if (has_wsc) node(2, --cnt == 0, "wait_send_completion", st(stage::urma_wait_send_completion));
    }
  }
  if (has(static_cast<std::size_t>(stage::client_recv_header))) {
    parent(0, false, "recv_header", st(stage::client_recv_header));
    buckets(1, stage::client_recv_header);
    bool has_rwc = has(static_cast<std::size_t>(stage::urma_read_wait_completion));
    bool has_rc = has(static_cast<std::size_t>(stage::urma_read_copy));
    int cnt = has_rwc + has_rc;
    if (has_rwc) node(1, --cnt == 0, "urma.read_wait", st(stage::urma_read_wait_completion));
    if (has_rc) node(1, --cnt == 0, "urma.read_copy", st(stage::urma_read_copy));
  }
  if (has(static_cast<std::size_t>(stage::client_recv_payload))) {
    parent(0, false, "recv_payload", st(stage::client_recv_payload));
    buckets(1, stage::client_recv_payload);
  }
  if (has(static_cast<std::size_t>(stage::client_deserialize_response)))
    parent(0, false, "deserialize_response", st(stage::client_deserialize_response));
  if (has(static_cast<std::size_t>(stage::client_connect_total))) {
    parent(0, true, "connect_total", st(stage::client_connect_total));
    bool has_tcp = has(static_cast<std::size_t>(stage::client_connect_tcp));
    bool has_hs = has(static_cast<std::size_t>(stage::client_connect_handshake));
    int cnt = has_tcp + has_hs;
    if (has_tcp) node(1, --cnt == 0, "connect_tcp", st(stage::client_connect_tcp));
    if (has_hs) node(1, --cnt == 0, "connect_handshake", st(stage::client_connect_handshake));
  }

  // ── Server ──
  bool has_server = has(static_cast<std::size_t>(stage::server_read_header)) ||
                    has(static_cast<std::size_t>(stage::server_dispatch));
  if (has_server) {
    os << "\n[server]\n";
    if (has(static_cast<std::size_t>(stage::server_read_header))) {
      parent(0, false, "read_header", st(stage::server_read_header));
      buckets(1, stage::server_read_header);
    }
    if (has(static_cast<std::size_t>(stage::server_read_payload))) {
      parent(0, false, "read_payload", st(stage::server_read_payload));
      buckets(1, stage::server_read_payload);
      bool has_rwc = has(static_cast<std::size_t>(stage::urma_read_wait_completion));
      bool has_rc = has(static_cast<std::size_t>(stage::urma_read_copy));
      int cnt = has_rwc + has_rc;
      if (has_rwc) node(1, --cnt == 0, "urma.read_wait", st(stage::urma_read_wait_completion));
      if (has_rc) node(1, --cnt == 0, "urma.read_copy", st(stage::urma_read_copy));
    }
    if (has(static_cast<std::size_t>(stage::server_dispatch))) {
      parent(0, false, "dispatch", st(stage::server_dispatch));
      buckets(1, stage::server_dispatch);
      bool has_deser = has(static_cast<std::size_t>(stage::server_deserialize_request));
      bool has_exec = has(static_cast<std::size_t>(stage::server_handler_execute));
      bool has_sres = has(static_cast<std::size_t>(stage::server_serialize_result));
      int cnt = has_deser + has_exec + has_sres;
      if (has_deser) node(1, --cnt == 0, "deserialize_request", st(stage::server_deserialize_request));
      if (has_exec) node(1, --cnt == 0, "handler_execute", st(stage::server_handler_execute));
      if (has_sres) node(1, --cnt == 0, "serialize_result", st(stage::server_serialize_result));
    }
    if (has(static_cast<std::size_t>(stage::server_serialize_response)))
      parent(0, false, "serialize_response", st(stage::server_serialize_response));
    if (has(static_cast<std::size_t>(stage::server_response_queue)))
      parent(0, false, "response_queue", st(stage::server_response_queue));
    if (has(static_cast<std::size_t>(stage::server_send_response))) {
      parent(0, true, "send_response", st(stage::server_send_response));
      if (has(static_cast<std::size_t>(stage::urma_write_total))) {
        parent(1, true, "urma.write_total", st(stage::urma_write_total));
        bool has_wc = has(static_cast<std::size_t>(stage::urma_write_copy));
        bool has_ps = has(static_cast<std::size_t>(stage::urma_post_send));
        bool has_wsc = has(static_cast<std::size_t>(stage::urma_wait_send_completion));
        int cnt = has_wc + has_ps + has_wsc;
        if (has_wc) node(2, --cnt == 0, "write_copy", st(stage::urma_write_copy));
        if (has_ps) node(2, --cnt == 0, "post_send", st(stage::urma_post_send));
        if (has_wsc) node(2, --cnt == 0, "wait_send_completion", st(stage::urma_wait_send_completion));
      }
    }
  }

  if (has(static_cast<std::size_t>(stage::urma_read_view))) {
    os << "\n[urma]\n";
    parent(0, true, "read_view", st(stage::urma_read_view));
  }
}

}  // namespace coro_io::urma_benchmark_profile

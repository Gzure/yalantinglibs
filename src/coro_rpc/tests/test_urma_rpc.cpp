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
#include <cstdlib>
#include <string>
#include <variant>
#include <vector>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "doctest.h"

#ifdef _WIN32
inline void set_test_env(const char* name, const char* value) {
  _putenv_s(name, value);
}
inline void unset_test_env(const char* name) { _putenv_s(name, ""); }
#else
inline void set_test_env(const char* name, const char* value) {
  setenv(name, value, 1);
}
inline void unset_test_env(const char* name) { unsetenv(name); }
#endif

class scoped_env_var {
 public:
  scoped_env_var(const char* name, const char* value) : name_(name) {
    const char* old = std::getenv(name);
    if (old) {
      old_value_ = old;
      had_value_ = true;
    }
    set_test_env(name, value);
  }

  ~scoped_env_var() {
    if (had_value_) {
      set_test_env(name_.c_str(), old_value_.c_str());
    }
    else {
      unset_test_env(name_.c_str());
    }
  }

 private:
  std::string name_;
  std::string old_value_;
  bool had_value_ = false;
};

#ifdef YLT_ENABLE_URMA
#include <ylt/coro_io/urma/urma_jfc_group.hpp>
#include <ylt/coro_io/urma/urma_rpc_env.hpp>

TEST_CASE("urma rpc env enable parsing") {
  {
    scoped_env_var env("URMA_RPC_ENABLE", "1");
    CHECK(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("URMA_RPC_ENABLE", "ON");
    CHECK(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("URMA_RPC_ENABLE", "true");
    CHECK(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("URMA_RPC_ENABLE", "yes");
    CHECK(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("URMA_RPC_ENABLE", "0");
    CHECK_FALSE(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("URMA_RPC_ENABLE", "abc");
    CHECK_FALSE(coro_io::detail::urma_rpc_env_enabled());
  }
}

TEST_CASE("urma rpc env config parsing uses defaults for invalid values") {
  scoped_env_var enable("URMA_RPC_ENABLE", "1");
  scoped_env_var device("URMA_RPC_DEVICE", "test_dev");
  scoped_env_var eid("URMA_RPC_EID_INDEX", "7");
  scoped_env_var cq("URMA_RPC_CQ_SIZE", "abc");
  scoped_env_var recv("URMA_RPC_RECV_BUFFER_CNT", "9");
  scoped_env_var send("URMA_RPC_SEND_BUFFER_CNT", "10");
  scoped_env_var buffer("URMA_RPC_BUFFER_SIZE", "8192");
  scoped_env_var memory("URMA_RPC_MAX_MEMORY_USAGE", "16777216");
  scoped_env_var tp("URMA_RPC_TP_TYPE", "rtp");
  scoped_env_var priority("URMA_RPC_PRIORITY", "7");

  auto config = coro_io::detail::make_urma_rpc_config_from_env();
  CHECK(config.device_name == "test_dev");
  CHECK(config.eid_index == 7);
  CHECK(config.cq_size == coro_io::urma_socket_t::config_t{}.cq_size);
  CHECK(config.recv_buffer_cnt == 9);
  CHECK(config.send_buffer_cnt == 10);
  CHECK(config.buffer_size == 8192);
  CHECK(config.max_memory_usage == 16777216);
  CHECK(config.tp_type == URMA_RTP);
  CHECK(config.jfs_priority == 7);
}

TEST_CASE("urma priority selection follows transport type capability") {
  urma_device_attr_t attr{};
  attr.dev_cap.priority_info[3].tp_type.bs.rtp = 1;
  attr.dev_cap.priority_info[7].tp_type.bs.ctp = 1;
  attr.dev_cap.priority_info[11].tp_type.bs.utp = 1;

  using state_t = coro_io::detail::urma_socket_shared_state_t;
  CHECK(state_t::get_priority(attr, 3, URMA_RTP).value() == 3);
  CHECK(state_t::get_priority(attr, 3, URMA_CTP).value() == 7);
  CHECK(state_t::get_priority(attr, 3, URMA_UTP).value() == 11);
  CHECK_FALSE(state_t::get_priority(attr, 16, URMA_RTP).has_value());
}

TEST_CASE("urma buffer pool exposes initialization failure") {
  coro_io::urma_buffer_pool_t pool(nullptr, 0, 1);
  CHECK_FALSE(pool.is_valid());
  CHECK(pool.total_buffer_count() == 0);
}

TEST_CASE("urma client init recovers after failure") {
  auto executor = coro_io::get_global_executor();
  coro_io::urma_socket_t::config_t valid_config;
  bool found_usable_device = false;
  for (const auto& device :
       coro_io::urma_device_manager::instance().get_all_devices()) {
    auto pool = device->get_buffer_pool();
    if (!pool)
      continue;
    valid_config.device_name = device->name();
    valid_config.eid_index = device->eid_index();
    valid_config.buffer_size = static_cast<uint32_t>(pool->buffer_size());
    valid_config.max_memory_usage = pool->total_memory_size();
    valid_config.tp_type =
        device->supports_rm_rtp() && !device->supports_rm_ctp() ? URMA_RTP
                                                                : URMA_CTP;
    valid_config.event_mode = false;
    coro_io::socket_wrapper_t probe(executor);
    if (probe.init_client(valid_config)) {
      found_usable_device = true;
      break;
    }
  }
  if (!found_usable_device)
    return;

  coro_io::socket_wrapper_t socket(executor);
  auto invalid_config = valid_config;
  invalid_config.device_name = "device_that_should_not_exist_for_test";
  CHECK_FALSE(socket.init_client(invalid_config));
  CHECK_FALSE(socket.init_ok());
  REQUIRE(socket.init_client(valid_config));
  CHECK(socket.init_ok());
}

TEST_CASE("urma rpc env event mode defaults to on and can be overridden") {
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.event_mode == true);
    CHECK(config.busy_poll_budget ==
          coro_io::urma_socket_t::config_t{}.busy_poll_budget);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var mode("URMA_RPC_EVENT_MODE", "0");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.event_mode == false);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var mode("URMA_RPC_EVENT_MODE", "off");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.event_mode == false);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var budget("URMA_RPC_BUSY_POLL_BUDGET", "32");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.busy_poll_budget == 32);
  }
}

TEST_CASE("urma rpc env pending poll budget and jfc moderation") {
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // A productive event must not be followed by extra polling: the jfce fd
    // already signalled. 1 leaves headroom for a CQE that lands between the
    // last drain and the rearm.
    CHECK(config.pending_poll_budget == 1);
    // Event wake-up latency is bounded by CQ event moderation, so the default
    // asks for one event per CQE and leaves the period to the driver.
    CHECK(config.jfc_moderate_count == 1);
    CHECK(config.jfc_moderate_period == -1);
    // Timer-mode fallback polls from the reactor thread instead of arming an
    // asio timer per tick; URMA_RPC_POLL_SPIN=0 restores the timer cadence.
    CHECK(config.poll_spin == true);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var spin("URMA_RPC_POLL_SPIN", "0");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.poll_spin == false);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var spin("URMA_RPC_POLL_SPIN", "on");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.poll_spin == true);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // Poll rounds per io_context yield: the I/O share of the reactor loop.
    CHECK(config.io_rate == 8);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var rate("URMA_RPC_IO_RATE", "64");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.io_rate == 64);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var rate("URMA_RPC_IO_RATE", "4096");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.io_rate == 1024);  // clamped to the supported range
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var rate("URMA_RPC_IO_RATE", "0");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.io_rate == 1);  // 0 would mean "never poll"
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var pending("URMA_RPC_PENDING_POLL_BUDGET", "8");
    scoped_env_var count("URMA_RPC_JFC_MODERATE_COUNT", "-1");
    scoped_env_var period("URMA_RPC_JFC_MODERATE_PERIOD", "50");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.pending_poll_budget == 8);
    CHECK(config.jfc_moderate_count == -1);
    CHECK(config.jfc_moderate_period == 50);
  }
  {
    // count=-1 and period=-1 is the A/B control arm: urma_modify_jfc is skipped
    // entirely and the driver default moderation applies.
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var count("URMA_RPC_JFC_MODERATE_COUNT", "-1");
    scoped_env_var period("URMA_RPC_JFC_MODERATE_PERIOD", "-1");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.jfc_moderate_count == -1);
    CHECK(config.jfc_moderate_period == -1);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var count("URMA_RPC_JFC_MODERATE_COUNT", "70000");
    scoped_env_var period("URMA_RPC_JFC_MODERATE_PERIOD", "70000");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // Out of uint16 range: fall back to the driver default instead of failing.
    CHECK(config.jfc_moderate_count == -1);
    CHECK(config.jfc_moderate_period == -1);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var count("URMA_RPC_JFC_MODERATE_COUNT", "0");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // 0 is not a meaningful CQ event count: treat it as "driver default".
    CHECK(config.jfc_moderate_count == -1);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var pending("URMA_RPC_PENDING_POLL_BUDGET", "100000");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // Unbounded spins would hold the reactor thread; clamp instead.
    CHECK(config.pending_poll_budget == 4096);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var pending("URMA_RPC_PENDING_POLL_BUDGET", "abc");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.pending_poll_budget == 1);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var pending("URMA_RPC_PENDING_POLL_BUDGET", "-5");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // Negative values do not parse into std::size_t: keep the default.
    CHECK(config.pending_poll_budget == 1);
  }
}

TEST_CASE("urma rpc shared jfc env defaults and clamping") {
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // Sharing a JFC between sockets is opt-in: it needs driver support for
    // several jetties on one completion queue.
    CHECK(config.shared_jfc == false);
    CHECK(config.group_cq_size == 2048);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var shared("URMA_RPC_SHARED_JFC", "on");
    scoped_env_var cq("URMA_RPC_GROUP_CQ_SIZE", "4096");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.shared_jfc == true);
    CHECK(config.group_cq_size == 4096);
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var cq("URMA_RPC_GROUP_CQ_SIZE", "1");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.group_cq_size == 64);  // clamped to the minimum useful depth
  }
  {
    scoped_env_var enable("URMA_RPC_ENABLE", "1");
    scoped_env_var cq("URMA_RPC_GROUP_CQ_SIZE", "1000000");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.group_cq_size == 65536);
  }
}

TEST_CASE("urma shared jfc user_ctx codec round trips") {
  using coro_io::detail::decode_ctx;
  using coro_io::detail::encode_ctx;
  using coro_io::detail::urma_ctx_op;
  for (auto op : {urma_ctx_op::send, urma_ctx_op::recv}) {
    for (uint32_t sid : {0u, 1u, 12345u, (1u << 28) - 1}) {
      for (uint32_t seq : {0u, 1u, 0x12345678u, 0xFFFFFFFFu}) {
        const auto decoded = decode_ctx(encode_ctx(op, sid, seq));
        CHECK(decoded.op == op);
        CHECK(decoded.sid == sid);
        CHECK(decoded.seq == seq);
      }
    }
  }
  // Same sid+seq must still differ between send and recv, so a mis-tagged work
  // request is detectable instead of being dispatched to the wrong handler.
  CHECK(encode_ctx(urma_ctx_op::send, 7, 9) !=
        encode_ctx(urma_ctx_op::recv, 7, 9));
}

TEST_CASE("urma seq slot ring matches out-of-order completions") {
  coro_io::detail::seq_slot_ring<int> ring(8);
  CHECK(ring.push(1, 11));
  CHECK(ring.push(2, 22));
  CHECK(ring.in_flight() == 2);
  CHECK(ring.contains(2));
  auto v2 = ring.take(2);  // a shared CQ delivers completions out of order
  REQUIRE(v2.has_value());
  CHECK(*v2 == 22);
  CHECK(!ring.contains(2));
  auto v1 = ring.take(1);  // free slot 1 so seq 9 can wrap onto it
  REQUIRE(v1.has_value());
  CHECK(*v1 == 11);
  CHECK(ring.push(9, 99));  // 9 % 8 == 1
  auto v9 = ring.take(9);
  REQUIRE(v9.has_value());
  CHECK(*v9 == 99);
  CHECK(ring.in_flight() == 0);
  CHECK_FALSE(ring.take(2).has_value());  // already taken
  // A slot that is still occupied refuses a new seq mapping onto it instead of
  // silently overwriting a pending send (which would leak its buffer).
  CHECK(ring.push(3, 33));
  CHECK_FALSE(ring.push(3 + 8, 44));
  CHECK(ring.in_flight() == 1);
}

namespace {
struct mock_group_socket : coro_io::detail::jfc_group_socket {
  int sends = 0;
  int recvs = 0;
  int errors = 0;
  void on_send_completion(const urma_cr_t&) override { ++sends; }
  void on_recv_completion(const urma_cr_t&) override { ++recvs; }
  void on_group_error(std::error_code) override { ++errors; }
};

urma_cr_t make_group_cr(coro_io::detail::urma_ctx_op op, uint32_t sid,
                        uint32_t seq) {
  urma_cr_t cr{};
  cr.user_ctx = coro_io::detail::encode_ctx(op, sid, seq);
  cr.flag.bs.s_r = (op == coro_io::detail::urma_ctx_op::recv) ? 1 : 0;
  cr.status = URMA_CR_SUCCESS;
  return cr;
}
}  // namespace

TEST_CASE("urma jfc group registry dispatches by socket id") {
  using coro_io::detail::jfc_group_registry;
  using coro_io::detail::jfc_group_socket;
  using coro_io::detail::urma_ctx_op;

  jfc_group_registry registry(/*max_wr=*/512);
  auto a = std::make_shared<mock_group_socket>();
  auto b = std::make_shared<mock_group_socket>();
  const int sid_a = registry.attach(a, 64);
  const int sid_b = registry.attach(b, 64);
  REQUIRE(sid_a >= 0);
  REQUIRE(sid_b >= 0);
  CHECK(sid_a != sid_b);
  CHECK(registry.size() == 2);
  CHECK(registry.used_wr() == 128);

  CHECK(registry.dispatch(make_group_cr(urma_ctx_op::send, sid_a, 1)));
  CHECK(registry.dispatch(make_group_cr(urma_ctx_op::recv, sid_b, 7)));
  CHECK(a->sends == 1);
  CHECK(a->recvs == 0);
  CHECK(b->sends == 0);
  CHECK(b->recvs == 1);

  // Unknown socket id (or a completion that raced with close): dropped.
  CHECK_FALSE(registry.dispatch(make_group_cr(urma_ctx_op::send, 9999u, 1)));

  // Detached socket releases its CQ reservation and stops receiving dispatch.
  CHECK(registry.detach(sid_b));
  CHECK_FALSE(registry.dispatch(make_group_cr(urma_ctx_op::recv, sid_b, 8)));
  CHECK(registry.used_wr() == 64);
  CHECK(registry.size() == 1);

  // The next socket cannot exceed the group CQ depth.
  CHECK(registry.attach(std::make_shared<mock_group_socket>(), 512) == -2);

  // Group-level failure is fanned out to every attached socket.
  mock_group_socket* raw_a = a.get();
  registry.for_each([&registry](jfc_group_socket& socket) {
    socket.on_group_error(std::make_error_code(std::errc::io_error));
  });
  CHECK(raw_a->errors == 1);
}

TEST_CASE("urma jfc group registry never reuses a socket id") {
  using coro_io::detail::jfc_group_registry;
  using coro_io::detail::urma_ctx_op;

  jfc_group_registry registry(/*max_wr=*/512);
  auto old_socket = std::make_shared<mock_group_socket>();
  const int old_sid = registry.attach(old_socket, 64);
  REQUIRE(old_sid >= 0);
  auto stale_cr = make_group_cr(urma_ctx_op::send, old_sid, 1);
  CHECK(registry.detach(old_sid));

  auto new_socket = std::make_shared<mock_group_socket>();
  const int new_sid = registry.attach(new_socket, 64);
  REQUIRE(new_sid >= 0);
  // A completion of the detached socket must not be dispatched to the next
  // socket, which is why ids are monotonic instead of reusing free slots.
  CHECK(new_sid != old_sid);
  CHECK_FALSE(registry.dispatch(stale_cr));
  CHECK(new_socket->sends == 0);
}

TEST_CASE(
    "urma jfc group registry prunes dead sockets and rejects bad records") {
  using coro_io::detail::jfc_group_registry;
  using coro_io::detail::urma_ctx_op;

  jfc_group_registry registry(/*max_wr=*/512);
  {
    auto dead = std::make_shared<mock_group_socket>();
    REQUIRE(registry.attach(dead, 256) >= 0);
  }  // destroyed without detach: attach() must prune the reservation
  auto live = std::make_shared<mock_group_socket>();
  const int sid = registry.attach(live, 256);
  CHECK(sid >= 0);
  CHECK(registry.used_wr() == 256);
  CHECK(registry.size() == 1);

  // Invalid inputs are dropped instead of being misdispatched.
  CHECK(registry.attach(nullptr, 16) == -1);
  urma_cr_t empty_ctx{};
  CHECK_FALSE(registry.dispatch(empty_ctx));
  urma_cr_t bad_op{};
  bad_op.user_ctx =
      coro_io::detail::encode_ctx(static_cast<coro_io::detail::urma_ctx_op>(9),
                                  static_cast<uint32_t>(sid), 1);
  CHECK_FALSE(registry.dispatch(bad_op));

  // max_wr == 0 means "no capacity accounting".
  jfc_group_registry unbounded(/*max_wr=*/0);
  CHECK(unbounded.attach(std::make_shared<mock_group_socket>(), 1000000) >= 0);
}

TEST_CASE("urma seq slot ring rejects a completion whose seq tag differs") {
  coro_io::detail::seq_slot_ring<int> ring(8);
  CHECK(ring.push(1, 11));
  // 9 % 8 == 1 but the slot belongs to seq 1: a stale completion must not take
  // it.
  CHECK_FALSE(ring.take(9).has_value());
  CHECK(ring.in_flight() == 1);
  auto v1 = ring.take(1);
  REQUIRE(v1.has_value());
  CHECK(*v1 == 11);
}

TEST_CASE("urma rpc env disabled keeps default client tcp config") {
  scoped_env_var enable("URMA_RPC_ENABLE", "0");

  coro_rpc::coro_rpc_client client;
  CHECK(std::holds_alternative<coro_rpc::coro_rpc_client::tcp_config>(
      client.get_config().socket_config));
}

TEST_CASE("urma rpc enabled without usable device keeps client constructible") {
  scoped_env_var enable("URMA_RPC_ENABLE", "1");
  scoped_env_var device("URMA_RPC_DEVICE",
                        "device_that_should_not_exist_for_test");

  coro_rpc::coro_rpc_client client;
  CHECK(std::holds_alternative<coro_rpc::coro_rpc_client::tcp_config>(
      client.get_config().socket_config));
}

#else
TEST_CASE("urma rpc env tests compile without urma support") {
  coro_rpc::coro_rpc_client client;
  CHECK(std::holds_alternative<coro_rpc::coro_rpc_client::tcp_config>(
      client.get_config().socket_config));
}
#endif

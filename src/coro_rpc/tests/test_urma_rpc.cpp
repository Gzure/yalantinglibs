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
#include <ylt/coro_io/urma/urma_rpc_env.hpp>

TEST_CASE("urma rpc env enable parsing") {
  {
    scoped_env_var env("MC_YLT_URMA_ENABLE", "1");
    CHECK(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("MC_YLT_URMA_ENABLE", "ON");
    CHECK(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("MC_YLT_URMA_ENABLE", "true");
    CHECK(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("MC_YLT_URMA_ENABLE", "yes");
    CHECK(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("MC_YLT_URMA_ENABLE", "0");
    CHECK_FALSE(coro_io::detail::urma_rpc_env_enabled());
  }
  {
    scoped_env_var env("MC_YLT_URMA_ENABLE", "abc");
    CHECK_FALSE(coro_io::detail::urma_rpc_env_enabled());
  }
}

TEST_CASE("urma rpc env config parsing uses defaults for invalid values") {
  scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
  scoped_env_var device("MC_YLT_URMA_DEVICE", "test_dev");
  scoped_env_var eid("MC_YLT_URMA_EID_INDEX", "7");
  scoped_env_var cq("MC_YLT_URMA_CQ_SIZE", "abc");
  scoped_env_var recv("MC_YLT_URMA_RECV_BUFFER_CNT", "9");
  scoped_env_var send("MC_YLT_URMA_SEND_BUFFER_CNT", "10");
  scoped_env_var buffer("MC_YLT_URMA_BUFFER_SIZE", "8192");
  scoped_env_var memory("MC_YLT_URMA_MAX_MEMORY_USAGE", "16777216");
  scoped_env_var tp("MC_YLT_URMA_TP_TYPE", "rtp");
  scoped_env_var priority("MC_YLT_URMA_PRIORITY", "7");

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
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.event_mode == true);
    CHECK(config.busy_poll_budget ==
          coro_io::urma_socket_t::config_t{}.busy_poll_budget);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var mode("MC_YLT_URMA_EVENT_MODE", "0");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.event_mode == false);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var mode("MC_YLT_URMA_EVENT_MODE", "off");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.event_mode == false);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var budget("MC_YLT_URMA_BUSY_POLL_BUDGET", "32");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.busy_poll_budget == 32);
  }
}

TEST_CASE("urma rpc env pending poll budget and jfc moderation") {
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
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
    // asio timer per tick; MC_YLT_URMA_POLL_SPIN=0 restores the timer cadence.
    CHECK(config.poll_spin == true);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var spin("MC_YLT_URMA_POLL_SPIN", "0");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.poll_spin == false);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var spin("MC_YLT_URMA_POLL_SPIN", "on");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.poll_spin == true);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var pending("MC_YLT_URMA_PENDING_POLL_BUDGET", "8");
    scoped_env_var count("MC_YLT_URMA_JFC_MODERATE_COUNT", "-1");
    scoped_env_var period("MC_YLT_URMA_JFC_MODERATE_PERIOD", "50");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.pending_poll_budget == 8);
    CHECK(config.jfc_moderate_count == -1);
    CHECK(config.jfc_moderate_period == 50);
  }
  {
    // count=-1 and period=-1 is the A/B control arm: urma_modify_jfc is skipped
    // entirely and the driver default moderation applies.
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var count("MC_YLT_URMA_JFC_MODERATE_COUNT", "-1");
    scoped_env_var period("MC_YLT_URMA_JFC_MODERATE_PERIOD", "-1");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.jfc_moderate_count == -1);
    CHECK(config.jfc_moderate_period == -1);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var count("MC_YLT_URMA_JFC_MODERATE_COUNT", "70000");
    scoped_env_var period("MC_YLT_URMA_JFC_MODERATE_PERIOD", "70000");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // Out of uint16 range: fall back to the driver default instead of failing.
    CHECK(config.jfc_moderate_count == -1);
    CHECK(config.jfc_moderate_period == -1);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var count("MC_YLT_URMA_JFC_MODERATE_COUNT", "0");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // 0 is not a meaningful CQ event count: treat it as "driver default".
    CHECK(config.jfc_moderate_count == -1);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var pending("MC_YLT_URMA_PENDING_POLL_BUDGET", "100000");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // Unbounded spins would hold the reactor thread; clamp instead.
    CHECK(config.pending_poll_budget == 4096);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var pending("MC_YLT_URMA_PENDING_POLL_BUDGET", "abc");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    CHECK(config.pending_poll_budget == 1);
  }
  {
    scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
    scoped_env_var pending("MC_YLT_URMA_PENDING_POLL_BUDGET", "-5");
    auto config = coro_io::detail::make_urma_rpc_config_from_env();
    // Negative values do not parse into std::size_t: keep the default.
    CHECK(config.pending_poll_budget == 1);
  }
}

TEST_CASE("urma rpc env disabled keeps default client tcp config") {
  scoped_env_var enable("MC_YLT_URMA_ENABLE", "0");

  coro_rpc::coro_rpc_client client;
  CHECK(std::holds_alternative<coro_rpc::coro_rpc_client::tcp_config>(
      client.get_config().socket_config));
}

TEST_CASE("urma rpc enabled without usable device keeps client constructible") {
  scoped_env_var enable("MC_YLT_URMA_ENABLE", "1");
  scoped_env_var device("MC_YLT_URMA_DEVICE",
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

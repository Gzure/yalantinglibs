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
#include "doctest.h"
#ifdef YLT_ENABLE_URMA
#include <cstdlib>
#include <optional>
#include <string>
#include "ylt/coro_io/urma/urma_rpc_env.hpp"

namespace {
class priority_env {
 public:
  priority_env() {
    if (const char* old = std::getenv("MC_YLT_URMA_PRIORITY"))
      old_ = old;
    unsetenv("MC_YLT_URMA_PRIORITY");
  }
  ~priority_env() {
    if (old_)
      setenv("MC_YLT_URMA_PRIORITY", old_->c_str(), 1);
    else
      unsetenv("MC_YLT_URMA_PRIORITY");
  }
 private:
  std::optional<std::string> old_;
};
}

TEST_CASE("urma priority override validates before narrowing") {
  priority_env guard;
  CHECK(coro_io::detail::urma_priority_from_env(15) == 15);
  CHECK(coro_io::detail::urma_priority_from_env(6) == 6);
  for (const char* value : {"0", "6", "7", "15"}) {
    setenv("MC_YLT_URMA_PRIORITY", value, 1);
    CHECK(coro_io::detail::urma_priority_from_env(3) == std::atoi(value));
    CHECK(coro_io::detail::make_urma_rpc_config_from_env().jfs_priority ==
          std::atoi(value));
  }
  for (const char* value : {"", "-1", "16", "256", "99999999999999999999",
                            "7x", " 7", "7 ", "+7"}) {
    setenv("MC_YLT_URMA_PRIORITY", value, 1);
    CHECK_THROWS_AS(coro_io::detail::urma_priority_from_env(15),
                    std::invalid_argument);
    CHECK_THROWS_AS(coro_io::detail::make_urma_rpc_config_from_env(),
                    std::invalid_argument);
  }
}

TEST_CASE("urma fallback preserves ascending compatibility policy") {
  using state = coro_io::detail::urma_socket_shared_state_t;
  urma_device_attr_t attr{};
  CHECK(state::get_priority(attr, 15, URMA_CTP).value() == 15);
  attr.dev_cap.priority_info[6].tp_type.bs.ctp = 1;
  attr.dev_cap.priority_info[7].tp_type.bs.ctp = 1;
  CHECK(state::get_priority(attr, 15, URMA_CTP).value() == 6);
  CHECK(state::get_priority(attr, 7, URMA_CTP).value() == 7);
  attr.dev_cap.priority_info[7].tp_type.bs.ctp = 0;
  CHECK(state::get_priority(attr, 7, URMA_CTP).value() == 6);
  CHECK_FALSE(state::get_priority(attr, 6, URMA_RTP).has_value());
  CHECK_FALSE(state::get_priority(attr, 16, URMA_CTP).has_value());
}
#endif

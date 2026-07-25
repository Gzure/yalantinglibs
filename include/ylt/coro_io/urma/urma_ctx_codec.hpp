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
#pragma once

#include <cstdint>
#include <tuple>

namespace coro_io::detail {

// Operation type encoded into the top 8 bits of the URMA user_ctx.
enum urma_ctx_op : uint8_t {
  urma_ctx_op_send = 1,
  urma_ctx_op_recv = 2,
};

// Pack (op, socket_id, seq) into the 64-bit user_ctx carried by each work
// request and echoed back in the completion record.  Layout:
//   [63:56] op_type   (urma_ctx_op_*)
//   [55:32] socket_id (per-group unique id, 24 bits -> up to ~16M sockets/group)
//   [31:0]  seq       (send: per-WR match key; recv: buffer index)
inline uint64_t encode_ctx(uint8_t op, uint32_t socket_id, uint32_t seq) {
  return (uint64_t(op) << 56) | (uint64_t(socket_id & 0xffffff) << 32) |
         uint64_t(seq);
}

inline std::tuple<uint8_t, uint32_t, uint32_t> decode_ctx(uint64_t ctx) {
  uint8_t op = static_cast<uint8_t>((ctx >> 56) & 0xff);
  uint32_t socket_id = static_cast<uint32_t>((ctx >> 32) & 0xffffff);
  uint32_t seq = static_cast<uint32_t>(ctx & 0xffffffff);
  return {op, socket_id, seq};
}

}  // namespace coro_io::detail

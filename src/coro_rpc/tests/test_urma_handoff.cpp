/*
 * Copyright (c) 2026, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
#include "doctest.h"

#ifdef YLT_ENABLE_URMA
#include <ylt/coro_io/urma/urma_ctx_codec.hpp>

using namespace coro_io::detail;

TEST_CASE("urma ctx encode/decode round-trips") {
  uint8_t op = 1;
  uint32_t sid = 0x123456;
  uint32_t seq = 0xabcdef01;
  uint64_t ctx = encode_ctx(op, sid, seq);
  auto [dop, dsid, dseq] = decode_ctx(ctx);
  CHECK(dop == op);
  CHECK(dsid == sid);
  CHECK(dseq == seq);
}

TEST_CASE("urma ctx encode/decode boundary values") {
  {
    auto ctx = encode_ctx(0, 0, 0);
    auto [op, sid, seq] = decode_ctx(ctx);
    CHECK(op == 0);
    CHECK(sid == 0);
    CHECK(seq == 0);
  }
  {
    auto ctx = encode_ctx(0xff, 0xffffff, 0xffffffff);
    auto [op, sid, seq] = decode_ctx(ctx);
    CHECK(op == 0xff);
    CHECK(sid == 0xffffff);
    CHECK(seq == 0xffffffff);
  }
}

TEST_CASE("urma ctx op occupies top 8 bits") {
  CHECK((encode_ctx(0x2, 0, 0) >> 56) == 0x2);
}
#else
TEST_CASE("urma handoff tests compile without urma support") {
  CHECK(true);
}
#endif

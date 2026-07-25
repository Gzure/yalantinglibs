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

#include <ylt/coro_io/urma/urma_completion_handoff.hpp>

TEST_CASE("urma handoff single-thread happy path") {
  completion_handoff h;
  CHECK(h.load_state() == completion_handoff::state::IDLE);

  // coroutine suspends: IDLE -> WAITING, registers a handle slot
  CHECK(h.try_begin_wait());
  CHECK(h.load_state() == completion_handoff::state::WAITING);

  // poll thread delivers a completion while WAITING -> direct handoff to READY
  bool resumed = h.try_deliver({});  // empty cr placeholder
  CHECK(resumed);
  CHECK(h.load_state() == completion_handoff::state::READY);

  // coroutine wakes, takes the pending completion, returns to IDLE
  auto cr = h.take_pending();
  CHECK(cr.has_value());
  CHECK(h.try_finish());
  CHECK(h.load_state() == completion_handoff::state::IDLE);
}

TEST_CASE("urma handoff branch A: READY before coroutine suspends") {
  completion_handoff h;
  // poll thread delivers first, while still IDLE -> goes to queue (no waiter)
  h.try_deliver({});  // state IDLE, not WAITING -> returns false
  CHECK(h.load_state() == completion_handoff::state::IDLE);
  CHECK_FALSE(h.queue_empty());

  // Now simulate: poll thread had already flipped to READY via a real
  // WAITING->READY before the coroutine checked.  Drive that path directly:
  completion_handoff h2;
  CHECK(h2.try_begin_wait());          // IDLE -> WAITING
  CHECK(h2.try_deliver({}));            // WAITING -> READY
  CHECK(h2.load_state() == completion_handoff::state::READY);
  // coroutine sees READY -> does NOT begin wait, takes pending directly
  CHECK_FALSE(h2.try_begin_wait());     // cannot leave READY
  auto cr = h2.take_pending();
  CHECK(cr.has_value());
  CHECK(h2.try_finish());               // READY -> IDLE
}

TEST_CASE("urma handoff fallback queue drains on wake") {
  completion_handoff h;
  CHECK(h.try_begin_wait());            // IDLE -> WAITING
  // A second completion arrives while WAITING (the slot is occupied by the
  // first): try_deliver succeeds for the first; the second must go to queue.
  CHECK(h.try_deliver({0x11, 0, 0, 0}));   // first -> READY, pending set
  // second delivery: state is READY, not WAITING -> false, enqueue
  CHECK_FALSE(h.try_deliver({0x22, 0, 0, 0}));
  h.push_queue({0x22, 0, 0, 0});
  CHECK_FALSE(h.queue_empty());

  // coroutine wakes, takes pending + drains queue
  auto first = h.take_pending();
  CHECK(first.has_value());
  CHECK(first->user_ctx == 0x11);
  auto second = h.pop_queue();
  CHECK(second.has_value());
  CHECK(second->user_ctx == 0x22);
  CHECK(h.try_finish());
  CHECK(h.load_state() == completion_handoff::state::IDLE);
}
#else
TEST_CASE("urma handoff tests compile without urma support") {
  CHECK(true);
}
#endif

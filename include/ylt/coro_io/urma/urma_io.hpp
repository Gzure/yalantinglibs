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
#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "async_simple/Promise.h"
#include "async_simple/util/move_only_function.h"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/urma/urma_benchmark_profile.hpp"
#include "ylt/coro_io/urma/urma_socket.hpp"

namespace coro_io {
namespace detail {

template <typename AsioBuffer, typename BufferSequence>
void make_urma_buffers(std::vector<AsioBuffer>& result,
                       BufferSequence& buffers) {
  auto first = asio::buffer_sequence_begin(buffers);
  auto last = asio::buffer_sequence_end(buffers);
  for (; first != last; ++first) result.emplace_back(*first);
}

struct urma_write_completion_state {
  // Lock-free single-slot result.  push() (poll thread) writes the result to
  // atomic fields and bumps seq_ (release).  The coroutine spins on seq_
  // (acquire) and reads the result directly from atomics - NO mutex in the
  // fast path.  The mutex is only used in the rare slow path (spin missed,
  // coroutine suspends).
  //   seq_: 0 = no result pending; 1 = result ready.
  std::atomic<uint64_t> seq_{0};
  // Result stored as two atomics so the coroutine can read them lock-free
  // after observing seq_ change.
  std::atomic<uint32_t> ec_value_{0};       // std::error_code::value()
  std::atomic<std::size_t> length_{0};
  // Slow-path only (mutex protects resume_handler + has_result_).
  std::mutex mtx;
  bool has_result_ = false;
  async_simple::util::move_only_function<void()> resume_handler;

  void push(std::pair<std::error_code, std::size_t> r) {
    // Write result to atomics first (relaxed - seq_ release publishes them).
    ec_value_.store(static_cast<uint32_t>(r.first.value()),
                    std::memory_order_relaxed);
    length_.store(r.second, std::memory_order_relaxed);
    seq_.store(1, std::memory_order_release);  // publish
    // Check if the coroutine suspended (slow path).
    async_simple::util::move_only_function<void()> h;
    {
      std::lock_guard lk(mtx);
      has_result_ = true;
      h = std::move(resume_handler);
      resume_handler = nullptr;
    }
    if (h) h();
  }
};

inline async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
wait_urma_write_completion(
    const std::shared_ptr<urma_write_completion_state>& state,
    urma_socket_t& socket) {
  (void)socket;
  // Phase 1: busy-spin on seq_ (pure atomic, no mutex, no yield).
  // CQE arrives in 1-10us; spin catches it -> read result from atomics ->
  // return.  Zero mutex, zero poll-thread involvement.
  while (state->seq_.load(std::memory_order_acquire) == 0) {
    // tight spin
  }
  // Result is ready: read from atomics (no mutex).
  auto ec_val = state->ec_value_.load(std::memory_order_relaxed);
  auto len = state->length_.load(std::memory_order_relaxed);
  // Reset for next send.
  state->seq_.store(0, std::memory_order_release);
  std::error_code ec = ec_val
      ? std::error_code(ec_val, std::generic_category())
      : std::error_code{};
  co_return std::pair{ec, len};
}

template <typename Buffer>
async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_urma_read(urma_socket_t& socket, Buffer&& raw_buffer, bool read_some) {
  if (!socket.get_executor().running_in_this_thread())
    co_await dispatch(socket.get_executor());

  std::vector<asio::mutable_buffer> buffers;
  make_urma_buffers(buffers, raw_buffer);
  std::size_t completed = 0;
  for (auto& buffer : buffers) {
    if (socket.remain_read_buffer_size()) {
      auto count =
          socket.consume(static_cast<char*>(buffer.data()), buffer.size());
      buffer += count;
      completed += count;
    }
    while (buffer.size()) {
      auto wait_begin = urma_benchmark_profile::enabled()
                            ? urma_benchmark_profile::now_ns()
                            : 0;
      auto [ec, length] =
          co_await async_io<std::pair<std::error_code, std::size_t>>(
              [&](auto&& callback) {
                socket.post_recv(std::move(callback));
              },
              socket);
      urma_benchmark_profile::record_since_with_size(
          urma_benchmark_profile::stage::urma_read_wait_completion,
          wait_begin, length);
      if (ec) co_return std::pair{ec, completed};
      auto recv = socket.get_recv_buffer();
      auto count = std::min<std::size_t>(length, buffer.size());
      auto copy_begin = urma_benchmark_profile::enabled()
                            ? urma_benchmark_profile::now_ns()
                            : 0;
      std::memcpy(buffer.data(), reinterpret_cast<void*>(recv.addr), count);
      urma_benchmark_profile::record_since_with_size(
          urma_benchmark_profile::stage::urma_read_copy, copy_begin, count);
      buffer += count;
      completed += count;
      socket.set_read_buffer_len(count, length - count);
      if (read_some) co_return std::pair{std::error_code{}, completed};
    }
  }
  co_return std::pair{std::error_code{}, completed};
}

inline async_simple::coro::Lazy<
    std::pair<std::error_code, std::vector<owned_data_view>>>
async_urma_read_views(urma_socket_t& socket, std::size_t size) {
  if (!socket.get_executor().running_in_this_thread())
    co_await dispatch(socket.get_executor());

  std::vector<owned_data_view> views;
  std::size_t completed = 0;
  if (socket.remain_read_buffer_size()) {
    auto view = socket.detach_remain_data_view();
    if (!view.empty()) {
      completed += view.size();
      views.push_back(std::move(view));
    }
  }
  while (completed < size) {
    auto wait_begin = urma_benchmark_profile::enabled()
                          ? urma_benchmark_profile::now_ns()
                          : 0;
    auto [ec, length] =
        co_await async_io<std::pair<std::error_code, std::size_t>>(
            [&](auto&& callback) {
              socket.post_recv(std::move(callback));
            },
            socket);
    urma_benchmark_profile::record_since_with_size(
        urma_benchmark_profile::stage::urma_read_wait_completion, wait_begin, length);
    if (ec) co_return std::pair{ec, std::move(views)};
    if (completed + length > size) {
      ELOG_ERROR << "URMA read view received more data than requested: "
                 << "requested=" << size << ", completed=" << completed
                 << ", incoming=" << length;
      co_return std::pair{std::make_error_code(std::errc::protocol_error),
                          std::move(views)};
    }
    auto view_begin = urma_benchmark_profile::enabled()
                          ? urma_benchmark_profile::now_ns()
                          : 0;
    auto view = socket.detach_recv_buffer_view(length);
    if (!view.empty()) {
      completed += view.size();
      views.push_back(std::move(view));
    }
    urma_benchmark_profile::record_since_with_size(
        urma_benchmark_profile::stage::urma_read_view, view_begin, length);
  }
  co_return std::pair{std::error_code{}, std::move(views)};
}

}  // namespace detail

template <typename Buffer>
async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>> async_write(
    urma_socket_t& socket, Buffer&& raw_buffer) {
  if (!socket.get_executor().running_in_this_thread())
    co_await dispatch(socket.get_executor());

  std::vector<asio::const_buffer> buffers;
  detail::make_urma_buffers(buffers, raw_buffer);
  std::size_t total_size = 0;
  for (auto& item : buffers) total_size += item.size();
  std::size_t completed = 0;
  auto total_begin = urma_benchmark_profile::enabled()
                         ? urma_benchmark_profile::now_ns()
                         : 0;
  ELOG_DEBUG << "URMA async_write start: total_size=" << total_size
             << ", chunk_size=" << socket.get_buffer_size()
             << ", send_window=" << socket.get_send_window_size();

  // Fire-and-forget send: post all chunks to hardware and return immediately.
  // CQEs are reclaimed asynchronously by the poll thread's on_send_completion
  // (which returns the send buffer to the pool and manages flow control via
  // wake_writer).  This eliminates the wait_send_completion latency (~9us)
  // from the send path - send becomes just memcpy + urma_post_jetty_send_wr.
  //
  // Flow control: if the send window is full (in_flight >= window), wait for
  // a slot via waiting_write_over() (which suspends until a CQE frees a slot).
  // In latency mode (1 chunk, window=4) this never triggers.
  const auto send_window = std::max<std::size_t>(socket.get_send_window_size(), 1);
  while (!buffers.empty()) {
    // Wait for a free send slot if the window is full.
    while (socket.sent_request_count() >= send_window) {
      auto ec = co_await socket.waiting_write_over();
      if (ec) co_return std::pair{ec, completed};
    }
    auto buffer = socket.get_send_buffer();
    if (!buffer)
      co_return std::pair{std::make_error_code(std::errc::no_buffer_space),
                          completed};
    std::size_t length = 0;
    auto copy_begin = urma_benchmark_profile::enabled()
                          ? urma_benchmark_profile::now_ns()
                          : 0;
    while (!buffers.empty() && length < socket.get_buffer_size()) {
      auto count = std::min<std::size_t>(
          buffers.front().size(), socket.get_buffer_size() - length);
      std::memcpy(static_cast<char*>(buffer.addr) + length,
                  buffers.front().data(), count);
      length += count;
      buffers.front() += count;
      if (buffers.front().size() == 0) buffers.erase(buffers.begin());
    }
    urma_benchmark_profile::record_since_with_size(
        urma_benchmark_profile::stage::urma_write_copy, copy_begin, length);
    auto post_begin = urma_benchmark_profile::enabled()
                          ? urma_benchmark_profile::now_ns()
                          : 0;
    // Fire-and-forget: the callback just returns the buffer.  No
    // urma_write_completion_state, no wait, no spin.
    socket.post_send(std::move(buffer), length,
                     [](std::pair<std::error_code, std::size_t>) {});
    urma_benchmark_profile::record_since_with_size(
        urma_benchmark_profile::stage::urma_post_send, post_begin, length);
    completed += length;
  }

  ELOG_DEBUG << "URMA async_write done: total_size=" << total_size
             << ", completed=" << completed;
  urma_benchmark_profile::record_since_with_size(
      urma_benchmark_profile::stage::urma_write_total, total_begin, total_size);
  co_return std::pair{std::error_code{}, completed};
}

template <typename Buffer>
async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>> async_read(
    urma_socket_t& socket, Buffer&& buffer) {
  return detail::async_urma_read(socket, std::forward<Buffer>(buffer), false);
}

template <typename Buffer>
async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_read_some(urma_socket_t& socket, Buffer&& buffer) {
  return detail::async_urma_read(socket, std::forward<Buffer>(buffer), true);
}

}  // namespace coro_io

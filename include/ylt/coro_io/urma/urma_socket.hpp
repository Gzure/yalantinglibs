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
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "asio/dispatch.hpp"
#include "asio/ip/address.hpp"
#include "asio/ip/tcp.hpp"
#include "asio/steady_timer.hpp"
#include "async_simple/Future.h"
#include "async_simple/Promise.h"
#include "async_simple/coro/FutureAwaiter.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/util/move_only_function.h"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/data_view.hpp"
#include "ylt/coro_io/detail/circle_buffer.hpp"
#include "ylt/coro_io/urma/urma_buffer.hpp"
#include "ylt/coro_io/urma/urma_device.hpp"
#include "ylt/easylog.hpp"
#include "ylt/struct_pack.hpp"
#include "ylt/urma/urma_api.h"
#include "ylt/urma/urma_ubagg.h"

namespace coro_io {
namespace detail {

struct urma_recv_buffer_owner {
  urma_recv_buffer_owner(std::shared_ptr<urma_buffer_pool_t> pool,
                         urma_buffer_t buffer)
      : pool(std::move(pool)), buffer(std::move(buffer)) {}
  ~urma_recv_buffer_owner() {
    if (pool && buffer) pool->return_buffer(buffer);
  }

  std::shared_ptr<urma_buffer_pool_t> pool;
  urma_buffer_t buffer;
};

inline std::error_code make_urma_error(int status) {
  if (status == URMA_SUCCESS) return {};
  return std::error_code(std::abs(status), std::generic_category());
}

struct urma_deleter {
  void operator()(urma_jfc_t* value) const {
    if (value) urma_delete_jfc(value);
  }
  void operator()(urma_jfr_t* value) const {
    if (value) urma_delete_jfr(value);
  }
  void operator()(urma_jetty_t* value) const {
    if (value) urma_delete_jetty(value);
  }
  void operator()(urma_jfce_t* value) const {
    if (value) urma_delete_jfce(value);
  }
  void operator()(urma_target_jetty_t* value) const {
    if (value) urma_unimport_jetty(value);
  }
  void operator()(urma_target_seg_t* value) const {
    if (value) urma_unimport_seg(value);
  }
};

struct urma_socket_shared_state_t
    : std::enable_shared_from_this<urma_socket_shared_state_t> {
  using callback_t = async_simple::util::move_only_function<void(
      std::pair<std::error_code, std::size_t>)>;

  struct pending_send {
    urma_buffer_t buffer;
    std::size_t length;
    callback_t callback;
  };

  struct pending_recv {
    std::pair<std::error_code, std::size_t> result;
    urma_buffer_t buffer;
  };

  urma_socket_shared_state_t(std::shared_ptr<urma_device_wrapper_t> device,
                             ExecutorWrapper<>* executor,
                             std::size_t recv_buffer_cnt,
                             std::size_t send_buffer_cnt,
                             std::size_t cq_size)
      : executor_(executor),
        device_(std::move(device)),
        socket_(executor->get_asio_executor()),
        poll_timer_(executor->get_asio_executor()),
        recv_buffer_cnt_(recv_buffer_cnt),
        recv_queue_(recv_buffer_cnt + 1),
        recv_result_(cq_size),
        send_callbacks_(send_buffer_cnt + 2) {}

  ~urma_socket_shared_state_t() { release_resources(); }

  static void resume(std::pair<std::error_code, std::size_t> result,
                     callback_t&& callback) {
    if (callback) {
      auto cb = std::move(callback);
      cb(std::move(result));
    }
  }

  bool init(std::size_t cq_size, std::size_t send_buffer_cnt) {
    const auto& cap = device_->attr().dev_cap;
    ELOG_INFO << "URMA resource init: device=" << device_->name()
              << ", eid=" << device_->eid_string()
              << ", eid_index=" << device_->eid_index()
              << ", uasid=" << device_->uasid()
              << ", jfc_depth=" << cq_size
              << ", jfr_depth=" << recv_buffer_cnt_ + 1
              << ", jfs_depth=" << send_buffer_cnt + 2
              << ", max_jfc_depth=" << cap.max_jfc_depth
              << ", max_jfr_depth=" << cap.max_jfr_depth
              << ", max_jfs_depth=" << cap.max_jfs_depth;

    // Create JFCE and attach to JFC (matching perftest create_jfc pattern,
    // perftest_resources.c:371-386).  Even without explicit wait_jfc in the
    // poll loop, creating+rearming the JFCE enables the hardware's event
    // completion path which is required for reliable CTP SEND on bonding.
    jfce_.reset(urma_create_jfce(device_->context()));
    if (!jfce_) {
      set_init_error("urma_create_jfce", errno);
      ELOG_ERROR << "urma_create_jfce failed: errno="
                 << init_error_.value();
    }

    urma_jfc_cfg_t jfc_cfg{};
    jfc_cfg.depth = static_cast<uint32_t>(cq_size);
    jfc_cfg.jfce = jfce_.get();
    errno = 0;
    jfc_.reset(urma_create_jfc(device_->context(), &jfc_cfg));
    if (!jfc_) {
      set_init_error("urma_create_jfc", errno);
      ELOG_ERROR << "urma_create_jfc failed: depth=" << jfc_cfg.depth
                 << ", context=" << device_->context()
                 << ", errno=" << init_error_.value()
                 << ", error=" << init_error_.message();
      return false;
    }
    ELOG_INFO << "urma_create_jfc succeeded: jfc_id="
              << jfc_->jfc_id.id << ", depth=" << jfc_cfg.depth;

    // Create JFR matching perftest fill_jfr_cfg (perftest_resources.c:538-555).
    urma_jfr_cfg_t jfr_cfg{};
    jfr_cfg.depth = static_cast<uint32_t>(recv_buffer_cnt_ + 1);
    jfr_cfg.trans_mode = URMA_TM_RM;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.flag.bs.tag_matching = URMA_NO_TAG_MATCHING;
    jfr_cfg.jfc = jfc_.get();
    errno = 0;
    jfr_.reset(urma_create_jfr(device_->context(), &jfr_cfg));
    if (!jfr_) {
      set_init_error("urma_create_jfr", errno);
      ELOG_ERROR << "urma_create_jfr failed: depth=" << jfr_cfg.depth
                 << ", trans_mode=" << static_cast<int>(jfr_cfg.trans_mode)
                 << ", max_sge=" << static_cast<unsigned>(jfr_cfg.max_sge)
                 << ", jfc=" << jfr_cfg.jfc
                 << ", errno=" << init_error_.value()
                 << ", error=" << init_error_.message();
      return false;
    }
    ELOG_INFO << "urma_create_jfr succeeded: jfr_id="
              << jfr_->jfr_id.id << ", depth=" << jfr_cfg.depth;

    // Jetty creation matching perftest create_jetty with share_jfr=true
    // (perftest_resources.c:616-629).  JFS config mirrors fill_jfs_cfg
    // (lines 511-536), JFR is shared via jetty_cfg.shared.
    urma_jetty_cfg_t jetty_cfg{};
    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg.depth =
        static_cast<uint32_t>(send_buffer_cnt + 2);
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
    jetty_cfg.jfs_cfg.priority = URMA_MAX_PRIORITY;
    jetty_cfg.jfs_cfg.max_sge = 1;
    jetty_cfg.jfs_cfg.max_rsge = 1;
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jetty_cfg.jfs_cfg.jfc = jfc_.get();
    jetty_cfg.shared.jfr = jfr_.get();
    jetty_cfg.shared.jfc = jfc_.get();
    errno = 0;
    jetty_.reset(urma_create_jetty(device_->context(), &jetty_cfg));
    if (!jetty_) {
      set_init_error("urma_create_jetty", errno);
      ELOG_ERROR << "urma_create_jetty failed: jfs_depth="
                 << jetty_cfg.jfs_cfg.depth
                 << ", trans_mode="
                 << static_cast<int>(jetty_cfg.jfs_cfg.trans_mode)
                 << ", priority="
                 << static_cast<unsigned>(jetty_cfg.jfs_cfg.priority)
                 << ", shared_jfr=" << jetty_cfg.shared.jfr
                 << ", jfc=" << jetty_cfg.jfs_cfg.jfc
                 << ", errno=" << init_error_.value()
                 << ", error=" << init_error_.message();
      return false;
    }
    ELOG_INFO << "urma_create_jetty succeeded: jetty_id="
              << jetty_->jetty_id.id << ", uasid="
              << jetty_->jetty_id.uasid;
    return true;
  }

  void set_init_error(std::string_view stage, int error) {
    init_stage_ = stage;
    init_error_ =
        error != 0 ? std::error_code(error, std::generic_category())
                   : std::make_error_code(std::errc::io_error);
  }

  std::error_code post_recv(urma_buffer_t buffer) {
    urma_sge_t sge{reinterpret_cast<uint64_t>(buffer.addr),
                   static_cast<uint32_t>(buffer.length),
                   static_cast<urma_target_seg_t*>(buffer.seg), nullptr};
    urma_sg_t sg{&sge, 1};
    urma_jfr_wr_t wr{sg, 0, nullptr};
    urma_jfr_wr_t* bad_wr = nullptr;
    // Use urma_post_jetty_recv_wr to post recv through the jetty (bonding-
    // aware).  urma_post_jfr_wr posts directly to the JFR and bypasses the
    // bonding layer, causing recv buffers to miss the physical device.
    auto ec = make_urma_error(
        urma_post_jetty_recv_wr(jetty_.get(), &wr, &bad_wr));
    if (!ec) recv_queue_.push(std::move(buffer));
    return ec;
  }

  std::error_code fill_recv_queue() {
    while (recv_queue_.size() < recv_buffer_cnt_) {
      auto buffer = device_->get_buffer_pool()->get_buffer();
      if (!buffer) return std::make_error_code(std::errc::no_buffer_space);
      auto ec = post_recv(std::move(buffer));
      if (ec) return ec;
    }
    return {};
  }

  void post_send(urma_buffer_t buffer, std::size_t length,
                 callback_t&& callback) {
    if (has_close_ || !remote_jetty_) {
      if (buffer) device_->get_buffer_pool()->return_buffer(buffer);
      resume({std::make_error_code(std::errc::not_connected), 0},
             std::move(callback));
      return;
    }

    urma_sge_t sge{reinterpret_cast<uint64_t>(buffer.addr),
                   static_cast<uint32_t>(length),
                   static_cast<urma_target_seg_t*>(buffer.seg), nullptr};
    urma_sg_t sg{length ? &sge : nullptr, length ? 1u : 0u};
    urma_send_wr_t send_wr{};
    send_wr.src = sg;
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_SEND;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = remote_jetty_.get();
    wr.user_ctx = 1;
    wr.send = send_wr;
    urma_jfs_wr_t* bad_wr = nullptr;
    auto ec =
        make_urma_error(urma_post_jetty_send_wr(jetty_.get(), &wr, &bad_wr));
    if (ec) {
      if (buffer) device_->get_buffer_pool()->return_buffer(buffer);
      resume({ec, 0}, std::move(callback));
      return;
    }
    send_callbacks_.push(
        pending_send{std::move(buffer), length, std::move(callback)});
  }

  void async_receive(callback_t&& callback) {
    if (!recv_result_.empty()) {
      auto pending = recv_result_.pop();
      recv_buffer_ = std::move(pending.buffer);
      resume(std::move(pending.result), std::move(callback));
    }
    else if (has_close_) {
      resume({std::make_error_code(std::errc::operation_canceled), 0},
             std::move(callback));
    }
    else {
      recv_callback_ = std::move(callback);
    }
  }

  std::pair<std::error_code, std::size_t> poll_completion() {
    std::array<urma_cr_t, 16> completions{};
    int count = 0;
    std::size_t polled = 0;
    do {
      count = urma_poll_jfc(jfc_.get(), static_cast<int>(completions.size()),
                            completions.data());
      if (count < 0)
        return {std::make_error_code(std::errc::io_error), polled};
      polled += static_cast<std::size_t>(count);
      for (int i = 0; i < count; ++i) {
        auto& cr = completions[i];
        auto ec = cr.status == URMA_CR_SUCCESS
                      ? std::error_code{}
                      : std::make_error_code(std::errc::io_error);
        if (ec) {
          ELOG_ERROR << "URMA completion failed: status="
                     << static_cast<int>(cr.status)
                     << ", direction=" << (cr.flag.bs.s_r ? "recv" : "send")
                     << ", opcode=" << static_cast<int>(cr.opcode)
                     << ", completion_len=" << cr.completion_len
                     << ", user_ctx=" << cr.user_ctx
                     << ", local_id=" << cr.local_id;
        }
        if (cr.flag.bs.s_r == 0) {
          if (send_callbacks_.empty()) continue;
          auto pending = send_callbacks_.pop();
          if (pending.buffer)
            device_->get_buffer_pool()->return_buffer(pending.buffer);
          resume({ec, pending.length}, std::move(pending.callback));
          wake_writer(ec);
          continue;
        }

        if (recv_queue_.empty())
          return {std::make_error_code(std::errc::protocol_error), polled};
        if (cr.completion_len == 0) {
          peer_close_ = true;
          has_close_ = true;
          if (recv_callback_) {
            resume({std::make_error_code(std::errc::connection_reset), 0},
                   std::move(recv_callback_));
          }
          continue;
        }
        auto completed_buffer = recv_queue_.pop();
        auto refill_ec = fill_recv_queue();
        if (refill_ec) {
          ELOG_ERROR << "URMA refill recv queue failed: "
                     << refill_ec.message()
                     << ", recv_queue_size=" << recv_queue_.size()
                     << ", recv_buffer_cnt=" << recv_buffer_cnt_;
        }
        if (recv_callback_) {
          recv_buffer_ = std::move(completed_buffer);
          resume({ec, cr.completion_len}, std::move(recv_callback_));
        }
        else {
          if (recv_result_.full()) {
            ELOG_ERROR << "URMA recv result queue is full; cannot cache "
                          "completed recv buffer";
            device_->get_buffer_pool()->return_buffer(completed_buffer);
            return {std::make_error_code(std::errc::no_buffer_space), polled};
          }
          recv_result_.push(
              pending_recv{{ec, cr.completion_len}, std::move(completed_buffer)});
        }
      }
    } while (count == static_cast<int>(completions.size()));
    return {{}, polled};
  }

  void start_polling() {
    // Rearm JFCE so the event channel is ready for the next poll cycle.
    if (jfce_) {
      urma_rearm_jfc(jfc_.get(), false);
    }
    auto self = shared_from_this();
    poll_timer_.expires_after(idle_poll_interval_);
    poll_timer_.async_wait([self](const std::error_code& ec) {
      if (ec || self->has_close_) return;
      self->poll_once();
    });
  }

  void poll_once() {
    if (has_close_) return;
    auto [poll_ec, completion_count] = poll_completion();
    if (poll_ec) {
      fail_pending(poll_ec);
      close();
      return;
    }
    if (completion_count == 0) {
      active_poll_budget_ = max_active_poll_budget_;
      start_polling();
      return;
    }
    if (active_poll_budget_ == 0) {
      active_poll_budget_ = max_active_poll_budget_;
      start_polling();
      return;
    }
    --active_poll_budget_;
    auto self = shared_from_this();
    asio::post(executor_->get_asio_executor(), [self] {
      if (self->has_close_) return;
      self->poll_once();
    });
  }

  async_simple::coro::Lazy<std::error_code> wait_for_send_slot(
      std::size_t limit) {
    if (send_callbacks_.size() < limit) co_return std::error_code{};
    write_promise_.emplace();
    co_return co_await write_promise_->getFuture();
  }

  void wake_writer(std::error_code ec) {
    if (!write_promise_) return;
    auto promise = std::move(*write_promise_);
    write_promise_.reset();
    promise.setValue(ec);
  }

  void fail_pending(std::error_code ec) {
    if (recv_callback_) resume({ec, 0}, std::move(recv_callback_));
    while (!send_callbacks_.empty()) {
      auto pending = send_callbacks_.pop();
      if (pending.buffer)
        device_->get_buffer_pool()->return_buffer(pending.buffer);
      resume({ec, 0}, std::move(pending.callback));
    }
    wake_writer(ec);
  }

  void close() {
    if (has_close_.exchange(true)) return;
    std::error_code ignored;
    poll_timer_.cancel(ignored);
    socket_.cancel(ignored);
    socket_.close(ignored);
    fail_pending(std::make_error_code(std::errc::operation_canceled));
  }

  void release_resources() {
    close();
    if (recv_buffer_) device_->get_buffer_pool()->return_buffer(recv_buffer_);
    while (!recv_queue_.empty()) {
      auto buffer = recv_queue_.pop();
      device_->get_buffer_pool()->return_buffer(buffer);
    }
    remote_seg_.reset();
    remote_jetty_.reset();
    jetty_.reset();
    jfce_.reset();
    jfr_.reset();
    jfc_.reset();
  }

  ExecutorWrapper<>* executor_;
  std::shared_ptr<urma_device_wrapper_t> device_;
  asio::ip::tcp::socket socket_;
  asio::steady_timer poll_timer_;
  std::unique_ptr<urma_jfc_t, urma_deleter> jfc_;
  std::unique_ptr<urma_jfr_t, urma_deleter> jfr_;
  std::unique_ptr<urma_jetty_t, urma_deleter> jetty_;
  std::unique_ptr<urma_jfce_t, urma_deleter> jfce_;
  std::unique_ptr<urma_target_jetty_t, urma_deleter> remote_jetty_;
  std::unique_ptr<urma_target_seg_t, urma_deleter> remote_seg_;
  std::size_t recv_buffer_cnt_;
  circle_buffer<urma_buffer_t> recv_queue_;
  circle_buffer<pending_recv> recv_result_;
  circle_buffer<pending_send> send_callbacks_;
  callback_t recv_callback_;
  urma_buffer_t recv_buffer_;
  std::optional<async_simple::Promise<std::error_code>> write_promise_;
  std::atomic<bool> has_close_{false};
  bool peer_close_ = false;
  static constexpr std::chrono::microseconds idle_poll_interval_{5};
  static constexpr std::size_t max_active_poll_budget_ = 64;
  std::size_t active_poll_budget_ = max_active_poll_budget_;
  std::string init_stage_;
  std::error_code init_error_;
};

}  // namespace detail

class urma_socket_t {
 public:
  struct config_t {
    uint32_t cq_size = 128;
    uint16_t recv_buffer_cnt = 8;
    uint16_t send_buffer_cnt = 4;
    uint32_t buffer_size = 4 * 1024;
    uint64_t max_memory_usage = 256ull * 1024 * 1024;
    std::string device_name;
    int eid_index = 0;
    urma_tp_type_t tp_type = URMA_CTP;
  };

  enum io_type { recv = 0, send = 1 };
  using callback_t = detail::urma_socket_shared_state_t::callback_t;

  struct urma_socket_info {
    uint8_t eid[16];
    uint32_t uasid;
    uint32_t jetty_id;
    uint32_t buffer_size;
    uint16_t recv_buffer_cnt;
    uint8_t tp_type;
    // Buffer pool segment info for urma_import_seg (flattened from urma_seg_t
    // which contains unions/bitfields not trivially serializable by struct_pack).
    uint8_t seg_eid[16];
    uint32_t seg_uasid;
    uint64_t seg_va;
    uint64_t seg_len;
    uint32_t seg_token_id;
    constexpr static auto struct_pack_config = struct_pack::DISABLE_TYPE_INFO;
  };

  urma_socket_t(ExecutorWrapper<>* executor, const config_t& config)
      : executor_(executor) {
    init(config);
  }
  explicit urma_socket_t(
      ExecutorWrapper<>* executor = coro_io::get_global_executor())
      : executor_(executor) {
    init(config_t{});
  }
  explicit urma_socket_t(const config_t& config)
      : executor_(coro_io::get_global_executor()) {
    init(config);
  }
  urma_socket_t(urma_socket_t&&) = default;
  urma_socket_t& operator=(urma_socket_t&& other) {
    if (this == &other) return *this;
    close();
    executor_ = other.executor_;
    conf_ = std::move(other.conf_);
    state_ = std::move(other.state_);
    remote_address_ = std::move(other.remote_address_);
    handshake_remote_address_ = std::move(other.handshake_remote_address_);
    handshake_local_address_ = std::move(other.handshake_local_address_);
    remote_jetty_id_ = other.remote_jetty_id_;
    handshake_remote_port_ = other.handshake_remote_port_;
    handshake_local_port_ = other.handshake_local_port_;
    buffer_size_ = other.buffer_size_;
    send_window_size_ = other.send_window_size_;
    remain_data_ = other.remain_data_;
    return *this;
  }
  ~urma_socket_t() { close(); }

  bool is_open() const noexcept {
    return state_ && !state_->has_close_ && state_->remote_jetty_;
  }
  auto get_executor() const { return executor_->get_asio_executor(); }
  auto get_coro_executor() const { return executor_; }
  const config_t& get_config() const noexcept { return conf_; }
  uint32_t get_buffer_size() const noexcept { return buffer_size_; }
  std::shared_ptr<urma_buffer_pool_t> buffer_pool() const {
    return state_->device_->get_buffer_pool();
  }

  async_simple::coro::Lazy<std::error_code> connect(
      const std::string& host, const std::string& port) noexcept {
    auto ec =
        co_await coro_io::async_connect(executor_, state_->socket_, host, port);
    if (!ec) ec = co_await connect_impl();
    if (ec) close();
    co_return ec;
  }

  template <typename EndPointSeq>
  async_simple::coro::Lazy<std::error_code> connect(
      const EndPointSeq& endpoint) noexcept {
    auto ec = co_await coro_io::async_connect(state_->socket_, endpoint);
    if (!ec) ec = co_await connect_impl();
    if (ec) close();
    co_return ec;
  }

  async_simple::coro::Lazy<std::error_code> accept(
      std::string_view magic = "") noexcept {
    urma_socket_info peer_info{};
    constexpr auto size = struct_pack::get_needed_size(peer_info);
    if (magic.size() >= size.size())
      co_return std::make_error_code(std::errc::protocol_error);
    std::array<char, size.size()> bytes{};
    std::memcpy(bytes.data(), magic.data(), magic.size());
    auto [ec, ignored] = co_await async_read(
        state_->socket_,
        asio::buffer(bytes.data() + magic.size(), bytes.size() - magic.size()));
    if (ec) co_return ec;
    if (struct_pack::deserialize_to(peer_info, std::span{bytes}))
      co_return std::make_error_code(std::errc::protocol_error);

    ec = import_peer(peer_info);
    if (ec) co_return ec;
    ec = state_->fill_recv_queue();
    if (ec) co_return ec;

    auto local_info = make_local_info();
    struct_pack::serialize_to(bytes.data(), size, local_info);
    auto [write_ec, ignored_write] =
        co_await async_write(state_->socket_, asio::buffer(bytes));
    if (write_ec) co_return write_ec;
    record_handshake_endpoints();
    close_handshake_socket();
    // Drain JFC before starting poll (see connect_impl comment).
    state_->poll_once();
    state_->start_polling();
    co_return std::error_code{};
  }

  async_simple::coro::Lazy<std::error_code> accept(
      asio::ip::tcp::socket socket) noexcept {
    prepare_accept(std::move(socket));
    return accept();
  }

  void prepare_accept(asio::ip::tcp::socket socket) noexcept {
    state_->socket_ = std::move(socket);
  }

  void close() const noexcept {
    if (!state_) return;
    asio::dispatch(executor_->get_asio_executor(),
                   [state = state_] { state->close(); });
  }

  auto cancel() const {
    close();
    return std::error_code{};
  }

  void post_recv(callback_t&& callback) {
    state_->async_receive(std::move(callback));
  }

  void post_send(urma_buffer_t buffer, std::size_t length,
                 callback_t&& callback) {
    state_->post_send(std::move(buffer), length, std::move(callback));
  }

  async_simple::coro::Lazy<std::error_code> waiting_write_over() {
    return state_->wait_for_send_slot(send_window_size_);
  }

  std::size_t sent_request_count() const noexcept {
    return state_->send_callbacks_.size();
  }

  std::size_t get_send_window_size() const noexcept {
    return send_window_size_;
  }

  urma_buffer_t get_send_buffer() { return buffer_pool()->get_buffer(); }

  urma_sge_t get_recv_buffer() const {
    return {reinterpret_cast<uint64_t>(state_->recv_buffer_.addr),
            static_cast<uint32_t>(state_->recv_buffer_.length),
            static_cast<urma_target_seg_t*>(state_->recv_buffer_.seg), nullptr};
  }

  std::size_t consume(char* destination, std::size_t size) {
    auto length = std::min(size, remain_data_.size());
    std::memcpy(destination, remain_data_.data(), length);
    remain_data_.remove_prefix(length);
    if (remain_data_.empty()) release_recv_buffer();
    return length;
  }

  std::size_t remain_read_buffer_size() const { return remain_data_.size(); }

  owned_data_view detach_remain_data_view() {
    if (remain_data_.empty() || !state_->recv_buffer_) return {};
    auto owner = std::make_shared<detail::urma_recv_buffer_owner>(
        buffer_pool(), std::move(state_->recv_buffer_));
    owned_data_view view{data_view{remain_data_, -1}, std::move(owner)};
    remain_data_ = {};
    return view;
  }

  owned_data_view detach_recv_buffer_view(std::size_t length) {
    if (!state_->recv_buffer_ || length == 0) return {};
    auto size = std::min<std::size_t>(length, state_->recv_buffer_.length);
    auto data = data_view{state_->recv_buffer_.addr, size, -1};
    auto owner = std::make_shared<detail::urma_recv_buffer_owner>(
        buffer_pool(), std::move(state_->recv_buffer_));
    return owned_data_view{data, std::move(owner)};
  }

  void set_read_buffer_len(std::size_t consumed, std::size_t remaining) {
    remain_data_ = std::string_view(
        static_cast<char*>(state_->recv_buffer_.addr) + consumed, remaining);
    if (remaining == 0) release_recv_buffer();
  }

  asio::ip::address get_remote_address() const noexcept {
    return handshake_remote_port_ != 0 ? handshake_remote_address_
                                       : remote_address_;
  }
  uint32_t get_remote_qp_num() const noexcept {
    return handshake_remote_port_ != 0 ? handshake_remote_port_
                                       : remote_jetty_id_;
  }
  asio::ip::address get_local_address() const noexcept {
    return handshake_local_port_ != 0 ? handshake_local_address_
                                      : state_->device_->gid_address();
  }
  uint32_t get_local_qp_num() const noexcept {
    return handshake_local_port_ != 0 ? handshake_local_port_
                                      : state_->jetty_->jetty_id.id;
  }

  constexpr static uint32_t urma_md5_header =
      struct_pack::get_type_code<urma_socket_info>();
  constexpr static char urma_md5_first_header = urma_md5_header % 256;

 private:
  void init(config_t config) {
    ELOG_INFO << "URMA socket init requested: device=" << config.device_name
              << ", eid_index=" << config.eid_index
              << ", tp_type=" << static_cast<int>(config.tp_type)
              << ", cq_size=" << config.cq_size
              << ", recv_buffer_cnt=" << config.recv_buffer_cnt
              << ", send_buffer_cnt=" << config.send_buffer_cnt
              << ", buffer_size=" << config.buffer_size
              << ", max_memory_usage=" << config.max_memory_usage
              << ", executor=" << executor_;
    constexpr uint32_t ctp_max_send_size = 4 * 1024;
    if (config.tp_type == URMA_CTP && config.buffer_size > ctp_max_send_size) {
      ELOG_WARN << "URMA CTP buffer_size " << config.buffer_size
                << " is larger than the documented bonding CTP max send packet "
                   "size; clamp to "
                << ctp_max_send_size;
      config.buffer_size = ctp_max_send_size;
    }
    config.recv_buffer_cnt = std::max<uint16_t>(config.recv_buffer_cnt, 1);
    config.send_buffer_cnt = std::max<uint16_t>(config.send_buffer_cnt, 1);
    config.cq_size =
        std::max<uint32_t>(config.cq_size, config.recv_buffer_cnt +
                                               config.send_buffer_cnt + 2);
    conf_ = std::move(config);
    auto device = get_global_urma_device(
        {.dev_name = conf_.device_name,
         .buffer_pool_config = {.buffer_size = conf_.buffer_size,
                                .max_memory_usage =
                                    conf_.max_memory_usage},
         .eid_index = conf_.eid_index});
    if (!device || !device->is_valid() || !device->get_buffer_pool())
      throw std::system_error(
          std::make_error_code(std::errc::no_such_device));
    const auto& cap = device->attr().dev_cap;
    ELOG_INFO << "URMA device capabilities: device=" << device->name()
              << ", rm_tp_cap=" << cap.rm_tp_cap.value
              << ", rtp=" << cap.rm_tp_cap.bs.rtp
              << ", ctp=" << cap.rm_tp_cap.bs.ctp
              << ", ctp_en=" << cap.feature.bs.ctp_en
              << ", trans_mode=" << cap.trans_mode
              << ", max_jfc=" << cap.max_jfc
              << ", max_jfr=" << cap.max_jfr
              << ", max_jetty=" << cap.max_jetty;
    if (conf_.tp_type == URMA_CTP && !device->supports_rm_ctp()) {
      ELOG_WARN << "URMA device capability does not report RM CTP support; "
                   "continuing because some providers expose CTP through "
                   "feature.ctp_en or resource creation";
    }
    if (conf_.tp_type == URMA_RTP && !device->supports_rm_rtp()) {
      ELOG_WARN << "URMA device capability does not report RM RTP support; "
                   "continuing and relying on resource creation";
    }
    buffer_size_ = std::min<uint32_t>(
        conf_.buffer_size, device->get_buffer_pool()->buffer_size());
    state_ = std::make_shared<detail::urma_socket_shared_state_t>(
        std::move(device), executor_, conf_.recv_buffer_cnt,
        conf_.send_buffer_cnt, conf_.cq_size);
    if (!state_->init(conf_.cq_size, conf_.send_buffer_cnt)) {
      auto stage = state_->init_stage_;
      auto error = state_->init_error_;
      ELOG_ERROR << "URMA socket resource initialization failed: stage="
                 << stage << ", errno=" << error.value()
                 << ", error=" << error.message();
      state_.reset();
      throw std::system_error(error, stage);
    }
    ELOG_INFO << "URMA socket init succeeded: device="
              << state_->device_->name() << ", jetty_id="
              << state_->jetty_->jetty_id.id;
  }

  urma_socket_info make_local_info() const {
    urma_socket_info info{};
    std::memcpy(info.eid, state_->device_->eid().raw, sizeof(info.eid));
    info.uasid = state_->jetty_->jetty_id.uasid;
    info.jetty_id = state_->jetty_->jetty_id.id;
    info.buffer_size = buffer_pool()->buffer_size();
    info.recv_buffer_cnt = conf_.recv_buffer_cnt;
    info.tp_type = static_cast<uint8_t>(conf_.tp_type);
    auto pool_seg = buffer_pool()->seg();
    std::memcpy(info.seg_eid, pool_seg.ubva.eid.raw, 16);
    info.seg_uasid = pool_seg.ubva.uasid;
    info.seg_va = pool_seg.ubva.va;
    info.seg_len = pool_seg.len;
    info.seg_token_id = pool_seg.token_id;
    return info;
  }

  std::error_code import_peer(const urma_socket_info& peer) {
    urma_rjetty_t remote{};
    std::memcpy(remote.jetty_id.eid.raw, peer.eid, sizeof(peer.eid));
    remote.jetty_id.uasid = peer.uasid;
    remote.jetty_id.id = peer.jetty_id;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    if (peer.tp_type > static_cast<uint8_t>(URMA_UTP)) {
      ELOG_ERROR << "invalid remote URMA TP type: "
                 << static_cast<unsigned>(peer.tp_type);
      return std::make_error_code(std::errc::protocol_error);
    }
    remote.tp_type = static_cast<urma_tp_type_t>(peer.tp_type);

    // Import the peer's buffer pool segment BEFORE importing the jetty.
    // The URMA perftest reference implementation calls urma_import_seg
    // before urma_import_jetty/urma_import_jetty_ex.  Without this step,
    // the kernel may not establish the transport path (TP) routing for
    // the remote EID, causing the first SEND to be immediately rejected
    // by hardware with URMA_CR_RNR_RETRY_CNT_EXC_ERR (status=10).
    urma_token_t seg_token{};
    urma_import_seg_flag_t seg_flag{};
    seg_flag.bs.cacheable = URMA_NON_CACHEABLE;
    seg_flag.bs.access =
        URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    seg_flag.bs.mapping = URMA_SEG_NOMAP;
    urma_seg_t peer_seg{};
    std::memcpy(peer_seg.ubva.eid.raw, peer.seg_eid, 16);
    peer_seg.ubva.uasid = peer.seg_uasid;
    peer_seg.ubva.va = peer.seg_va;
    peer_seg.len = peer.seg_len;
    peer_seg.token_id = peer.seg_token_id;
    state_->remote_seg_.reset(
        urma_import_seg(state_->device_->context(), &peer_seg,
                        &seg_token, 0, seg_flag));
    if (!state_->remote_seg_) {
      ELOG_WARN << "urma_import_seg failed: errno=" << errno
                << ", continuing with urma_import_jetty";
    } else {
      ELOG_INFO << "urma_import_seg succeeded for peer EID="
                << eid_to_address(peer.eid).to_string();
    }

    urma_token_t token{};
    errno = 0;

    // For bonding devices in RM mode, pass has_drv_ext and the local jetty
    // pointer via bondp_rjetty_t so the bonding driver (bondp_import_jetty)
    // establishes the virtual connection routing table.  Without this, the
    // bonding layer cannot route SENDs to the correct physical device,
    // causing the first SEND to be rejected with status=10.
    auto dev_name = state_->device_->name();
    if (dev_name.compare(0, 7, "bonding") == 0 &&
        remote.trans_mode == URMA_TM_RM) {
      bondp_rjetty_t bondp_rjetty{};
      bondp_rjetty.base = remote;
      bondp_rjetty.base.flag.bs.has_drv_ext = 1;
      bondp_rjetty.jetty = state_->jetty_.get();
      state_->remote_jetty_.reset(
          urma_import_jetty(state_->device_->context(),
                            &bondp_rjetty.base, &token));
    } else {
      state_->remote_jetty_.reset(
          urma_import_jetty(state_->device_->context(), &remote, &token));
    }

    if (!state_->remote_jetty_) {
      auto error = errno != 0
                       ? std::error_code(errno, std::generic_category())
                       : std::make_error_code(std::errc::connection_refused);
      ELOG_ERROR << "urma_import_jetty failed: " << error.message()
                 << ", errno=" << errno << ", remote_eid="
                 << eid_to_address(peer.eid).to_string()
                 << ", remote_uasid=" << peer.uasid
                 << ", remote_jetty_id=" << peer.jetty_id
                 << ", trans_mode=" << remote.trans_mode
                 << ", tp_type=" << remote.tp_type;
      return error;
    }
    remote_jetty_id_ = peer.jetty_id;
    buffer_size_ =
        std::min<uint32_t>(peer.buffer_size, buffer_pool()->buffer_size());
    const auto remote_recv_capacity =
        std::max<std::size_t>(peer.recv_buffer_cnt, 1);
    send_window_size_ = std::min<std::size_t>(
        conf_.send_buffer_cnt,
        remote_recv_capacity > 1 ? remote_recv_capacity - 1 : 1);
    ELOG_INFO << "URMA peer imported: remote_recv_buffer_cnt="
              << peer.recv_buffer_cnt
              << ", local_send_buffer_cnt=" << conf_.send_buffer_cnt
              << ", effective_send_window=" << send_window_size_;
    remote_address_ = eid_to_address(peer.eid);
    return {};
  }

  async_simple::coro::Lazy<std::error_code> connect_impl() {
    auto ec = state_->fill_recv_queue();
    if (ec) co_return ec;
    auto local_info = make_local_info();
    constexpr auto size = struct_pack::get_needed_size(local_info);
    std::array<char, size.size()> bytes{};
    struct_pack::serialize_to(bytes.data(), size, local_info);
    auto [write_ec, ignored_write] =
        co_await async_write(state_->socket_, asio::buffer(bytes));
    if (write_ec) co_return write_ec;
    auto [read_ec, ignored_read] =
        co_await async_read(state_->socket_, asio::buffer(bytes));
    if (read_ec) co_return read_ec;
    urma_socket_info peer{};
    if (struct_pack::deserialize_to(peer, std::span{bytes}))
      co_return std::make_error_code(std::errc::protocol_error);
    ec = import_peer(peer);
    if (ec) co_return ec;
    record_handshake_endpoints();
    close_handshake_socket();
    // Drain the JFC before starting timed polling, matching perftest's
    // warmup pattern (perftest_resources.c:2667).  This ensures any CQEs
    // from setup (e.g. recv buffer posting) are consumed before the first
    // application SEND, and primes the hardware poll path.
    state_->poll_once();
    state_->start_polling();
    co_return std::error_code{};
  }

  void record_handshake_endpoints() {
    std::error_code remote_ec;
    auto remote_ep = state_->socket_.remote_endpoint(remote_ec);
    if (!remote_ec) {
      handshake_remote_address_ = remote_ep.address();
      handshake_remote_port_ = remote_ep.port();
    }
    std::error_code local_ec;
    auto local_ep = state_->socket_.local_endpoint(local_ec);
    if (!local_ec) {
      handshake_local_address_ = local_ep.address();
      handshake_local_port_ = local_ep.port();
    }
    ELOG_INFO << "URMA handshake TCP endpoint: remote="
              << handshake_remote_address_.to_string() << ":"
              << handshake_remote_port_ << ", local="
              << handshake_local_address_.to_string() << ":"
              << handshake_local_port_ << ", remote_jetty_id="
              << remote_jetty_id_ << ", local_jetty_id="
              << state_->jetty_->jetty_id.id;
  }

  void release_recv_buffer() {
    if (state_->recv_buffer_)
      state_->device_->get_buffer_pool()->return_buffer(state_->recv_buffer_);
  }

  void close_handshake_socket() {
    std::error_code ignored;
    state_->socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
    state_->socket_.close(ignored);
  }

  static asio::ip::address eid_to_address(const uint8_t* eid) {
    asio::ip::address_v6::bytes_type bytes{};
    std::memcpy(bytes.data(), eid, bytes.size());
    return asio::ip::address_v6(bytes);
  }

  ExecutorWrapper<>* executor_;
  config_t conf_;
  std::shared_ptr<detail::urma_socket_shared_state_t> state_;
  asio::ip::address remote_address_;
  asio::ip::address handshake_remote_address_;
  asio::ip::address handshake_local_address_;
  uint32_t remote_jetty_id_ = 0;
  uint32_t handshake_remote_port_ = 0;
  uint32_t handshake_local_port_ = 0;
  uint32_t buffer_size_ = 0;
  std::size_t send_window_size_ = 1;
  std::string_view remain_data_;
};

template <typename EndPointSeq>
inline async_simple::coro::Lazy<std::error_code> async_connect(
    urma_socket_t& socket, const EndPointSeq& endpoint) noexcept {
  return socket.connect(endpoint);
}

inline async_simple::coro::Lazy<std::error_code> async_connect(
    urma_socket_t& socket, const std::string& host,
    const std::string& port) noexcept {
  return socket.connect(host, port);
}

}  // namespace coro_io

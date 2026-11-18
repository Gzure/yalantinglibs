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

#ifdef YLT_ENABLE_URMA

#include <fcntl.h>
#include <poll.h>
#include <urma_api.h>

#include <atomic>
#include <cerrno>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ylt/easylog.hpp"

namespace coro_io {

// ---------------------------------------------------------------------------
// Async (exception) events of one urma_context_t.
//
// The hardware reports JFC overflow, jetty / JFS / JFR errors and device
// failures only through the context's async event file; nothing polls a JFC
// that went into error, so without this watcher a JFC overflow would silently
// stop every socket on it. One thread per device context waits on the fd,
// routes each event to the subscriber of the failed object and acks it right
// away: UMDK requires an object's events to be acked before the object is
// deleted, and deletion blocks until they are.
//
// Handlers run on the watcher thread and must not block: they only post work
// to their owner's executor.
// ---------------------------------------------------------------------------
class urma_async_event_hub {
 public:
  using handler_t = std::function<void(urma_async_event_type_t)>;

  // context may be null (tests): subscriptions and dispatch() still work, but
  // no watcher thread is started.
  explicit urma_async_event_hub(urma_context_t* context) : context_(context) {}
  ~urma_async_event_hub() { shutdown(); }

  urma_async_event_hub(const urma_async_event_hub&) = delete;
  urma_async_event_hub& operator=(const urma_async_event_hub&) = delete;

  // Routes events whose element is `object` (a urma_jfc_t*, urma_jetty_t*,
  // urma_jfs_t* or urma_jfr_t*) to `handler`. `owner` guards against a
  // recycled address: the handler is skipped once the owner expired. Starts
  // the watcher thread on first use.
  void subscribe(const void* object, std::weak_ptr<void> owner,
                 handler_t handler) {
    if (!object || !handler)
      return;
    std::lock_guard<std::mutex> lock(mutex_);
    prune_expired_locked();
    subscribers_[object] = entry{std::move(owner), std::move(handler)};
    start_locked();
  }

  // Must run before the object is deleted, so a recycled address cannot reach
  // the old owner.
  void unsubscribe(const void* object) {
    std::lock_guard<std::mutex> lock(mutex_);
    subscribers_.erase(object);
  }

  // Routes one event. element is the failed object for object-level events and
  // ignored for device-level ones. Returns the number of handlers invoked.
  std::size_t dispatch(urma_async_event_type_t type, const void* element) {
    std::vector<handler_t> handlers;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (type == URMA_EVENT_DEV_FATAL) {
        // Every object of the context is gone: fail all of them.
        for (auto& [object, sub] : subscribers_)
          if (!sub.owner.expired())
            handlers.push_back(sub.handler);
      }
      else if (is_object_error(type) && element) {
        auto it = subscribers_.find(element);
        if (it != subscribers_.end() && !it->second.owner.expired())
          handlers.push_back(it->second.handler);
      }
    }
    for (auto& handler : handlers) handler(type);
    return handlers.size();
  }

  std::size_t subscriber_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return subscribers_.size();
  }

  bool running() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return thread_.joinable();
  }

  // Stops and joins the watcher. Must run before the context is deleted.
  void shutdown() {
    std::thread thread;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stop_.store(true, std::memory_order_release);
      thread = std::move(thread_);
    }
    if (thread.joinable())
      thread.join();
  }

  // Errors that concern one object and close whatever owns it. Watermark
  // (LIMIT) events and port / EID / ELR notifications are logged only: a port
  // that goes down surfaces as per-jetty errors.
  static bool is_object_error(urma_async_event_type_t type) noexcept {
    switch (type) {
      case URMA_EVENT_JFC_ERR:
      case URMA_EVENT_JFS_ERR:
      case URMA_EVENT_JFR_ERR:
      case URMA_EVENT_JETTY_ERR:
        return true;
      default:
        return false;
    }
  }

 private:
  struct entry {
    std::weak_ptr<void> owner;
    handler_t handler;
  };

  static constexpr int kPollTimeoutMs = 100;

  void start_locked() {
    if (thread_.joinable() || stop_.load(std::memory_order_acquire) ||
        !context_ || context_->async_fd < 0)
      return;
    // Non-blocking, so a spurious readiness cannot park the watcher inside
    // urma_get_async_event and hang shutdown().
    const int flags = ::fcntl(context_->async_fd, F_GETFL);
    if (flags < 0 ||
        ::fcntl(context_->async_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      ELOG_WARN << "URMA async event fd " << context_->async_fd
                << " cannot be made non-blocking: errno=" << errno
                << "; async events are not watched";
      return;
    }
    thread_ = std::thread([this] {
      run();
    });
  }

  void run() {
    pollfd pfd{context_->async_fd, POLLIN, 0};
    while (!stop_.load(std::memory_order_acquire)) {
      pfd.revents = 0;
      const int rc = ::poll(&pfd, 1, kPollTimeoutMs);
      if (rc < 0 && errno == EINTR)
        continue;
      if (rc < 0 || (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))) {
        ELOG_ERROR << "URMA async event fd failed: rc=" << rc
                   << ", revents=" << pfd.revents << ", errno=" << errno
                   << "; async events are no longer watched";
        return;
      }
      if (rc == 0)
        continue;
      urma_async_event_t event{};
      if (urma_get_async_event(context_, &event) != URMA_SUCCESS)
        continue;
      handle(event);
      urma_ack_async_event(&event);
    }
  }

  void handle(const urma_async_event_t& event) {
    const auto type = event.event_type;
    const void* element = element_of(event);
    const std::size_t handled = dispatch(type, element);
    if (is_object_error(type) || type == URMA_EVENT_DEV_FATAL) {
      ELOG_ERROR << "URMA async event " << static_cast<int>(type)
                 << " on object " << element << ": " << handled
                 << " subscriber(s) notified";
    }
    else {
      ELOG_WARN << "URMA async event " << static_cast<int>(type)
                << " (not handled)";
    }
  }

  static const void* element_of(const urma_async_event_t& event) noexcept {
    switch (event.event_type) {
      case URMA_EVENT_JFC_ERR:
        return event.element.jfc;
      case URMA_EVENT_JFS_ERR:
        return event.element.jfs;
      case URMA_EVENT_JFR_ERR:
      case URMA_EVENT_JFR_LIMIT:
        return event.element.jfr;
      case URMA_EVENT_JETTY_ERR:
      case URMA_EVENT_JETTY_LIMIT:
        return event.element.jetty;
      default:
        return nullptr;
    }
  }

  void prune_expired_locked() {
    for (auto it = subscribers_.begin(); it != subscribers_.end();) {
      if (it->second.owner.expired())
        it = subscribers_.erase(it);
      else
        ++it;
    }
  }

  urma_context_t* context_;
  mutable std::mutex mutex_;
  std::unordered_map<const void*, entry> subscribers_;
  std::thread thread_;
  std::atomic<bool> stop_{false};
};

}  // namespace coro_io

#endif  // YLT_ENABLE_URMA

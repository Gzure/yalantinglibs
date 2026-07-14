/*
 * Minimal URMA SEND test matching perftest's API flow exactly.
 * Not using coro_rpc::urma_socket_t -- calls URMA APIs directly.
 *
 * Usage:
 *   server: urma_minimal_test server --host 0.0.0.0 --port 9101
 *   client: urma_minimal_test client --host <server-ip> --port 9101
 */
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>
#include <random>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <csignal>
#include <atomic>
#include <cassert>

#include "ylt/easylog.hpp"
#include "ylt/urma/urma_api.h"
#include "ylt/urma/urma_ubagg.h"
#include "ylt/urma/urma_opcode.h"

// ---- TCP helpers (handshake for segment/jetty info) ----

static int tcp_listen(uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) { perror("socket"); return -1; }
  int opt = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  struct sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);
  if (::bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); ::close(fd); return -1; }
  if (::listen(fd, 1) < 0) { perror("listen"); ::close(fd); return -1; }
  return fd;
}

static int tcp_accept(int listen_fd) {
  struct sockaddr_in peer{};
  socklen_t len = sizeof(peer);
  int fd = ::accept(listen_fd, (struct sockaddr*)&peer, &len);
  if (fd < 0) { perror("accept"); }
  return fd;
}

static int tcp_connect(const char* host, uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) { perror("socket"); return -1; }
  struct hostent* he = ::gethostbyname(host);
  if (!he) { fprintf(stderr, "gethostbyname failed: %s\n", host); return -1; }
  struct sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);
  if (::connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("connect"); return -1; }
  return fd;
}

static bool tcp_send(int fd, const void* data, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::send(fd, (const char*)data + off, len - off, 0);
    if (n <= 0) return false;
    off += n;
  }
  return true;
}

static bool tcp_recv(int fd, void* data, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = ::recv(fd, (char*)data + off, len - off, MSG_WAITALL);
    if (n <= 0) return false;
    off += n;
  }
  return true;
}

// ---- URMA setup (matching perftest's create_duplex_ctx) ----

struct test_config {
  std::string device = "bonding_dev_0";
  int eid_index = 0;
  uint32_t cq_size = 128;
  uint32_t jfr_depth = 32;
  uint32_t jfs_depth = 64;
  uint32_t payload_size = 64;
  uint32_t iters = 100;
  urma_tp_type_t tp_type = URMA_CTP;
  uint8_t priority = 0;
};

struct test_context {
  urma_context_t* urma_ctx = nullptr;
  urma_device_attr_t dev_attr{};

  // URMA resources (one per connection)
  urma_jfce_t* jfce = nullptr;
  urma_jfc_t* jfc = nullptr;
  urma_jfr_t* jfr = nullptr;
  urma_jetty_t* jetty = nullptr;

  // Data buffer
  void* local_buf = nullptr;
  urma_target_seg_t* local_tseg = nullptr;  // local registered segment

  // Remote resources
  urma_seg_t remote_seg{};              // received via TCP
  urma_target_seg_t* import_tseg = nullptr;
  urma_rjetty_t remote_jetty_id{};      // received via TCP
  urma_target_jetty_t* import_tjetty = nullptr;

  // Stats
  uint64_t scnt = 0;  // send count
  uint64_t ccnt = 0;  // completion count
};

static bool init_device(test_context* ctx, const test_config& cfg) {
  urma_init_attr_t init_attr{};
  init_attr.token = 0;
  init_attr.uasid = 0;

  if (urma_init(&init_attr) != URMA_SUCCESS) {
    fprintf(stderr, "urma_init failed\n");
    return false;
  }

  // Find device by name (simpler than iterating list)
  char dev_name_buf[256];
  strncpy(dev_name_buf, cfg.device.c_str(), sizeof(dev_name_buf) - 1);
  urma_device_t* dev = urma_get_device_by_name(dev_name_buf);
  if (!dev) {
    fprintf(stderr, "device '%s' not found\n", cfg.device.c_str());
    return false;
  }

  urma_device_attr_t attr;
  if (urma_query_device(dev, &attr) != URMA_SUCCESS) {
    fprintf(stderr, "urma_query_device failed\n");
    return false;
  }
  ctx->dev_attr = attr;

  urma_context_t* urma_ctx = urma_create_context(dev, cfg.eid_index);
  if (!urma_ctx) {
    fprintf(stderr, "urma_open_device failed\n");
    return false;
  }
  ctx->urma_ctx = urma_ctx;

  // Bonding user_ctl (best-effort)
  if (cfg.device.compare(0, 7, "bonding") == 0) {
    bondp_set_bonding_mode_in_t in_arg{};
    in_arg.bonding_mode = BONDP_BONDING_MODE_STANDALONE;
    in_arg.bonding_level = BONDP_BONDING_LEVEL_IODIE;
    urma_user_ctl_in_t in{};
    in.addr = (uint64_t)&in_arg;
    in.len = sizeof(in_arg);
    in.opcode = BONDP_USER_CTL_SET_BONDING_MODE;
    urma_user_ctl_out_t out{};
    auto st = urma_user_ctl(urma_ctx, &in, &out);
    ELOG_DEBUG << "user_ctl SET_BONDING_MODE: " << st;
  }

  return true;
}

static bool create_resources(test_context* ctx, const test_config& cfg) {
  // JFCE (perftest create_jfc:371-382)
  ctx->jfce = urma_create_jfce(ctx->urma_ctx);
  if (!ctx->jfce) {
    fprintf(stderr, "urma_create_jfce failed\n");
    return false;
  }

  // JFC with JFCE attached (perftest create_jfc:385-393)
  urma_jfc_cfg_t jfc_cfg{};
  jfc_cfg.depth = cfg.cq_size;
  jfc_cfg.jfce = ctx->jfce;
  ctx->jfc = urma_create_jfc(ctx->urma_ctx, &jfc_cfg);
  if (!ctx->jfc) {
    fprintf(stderr, "urma_create_jfc failed\n");
    return false;
  }

  // JFR (perftest fill_jfr_cfg:538-555)
  urma_jfr_cfg_t jfr_cfg{};
  jfr_cfg.depth = cfg.jfr_depth;
  jfr_cfg.trans_mode = URMA_TM_RM;
  jfr_cfg.max_sge = 1;
  jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
  jfr_cfg.flag.bs.tag_matching = URMA_NO_TAG_MATCHING;
  jfr_cfg.jfc = ctx->jfc;
  ctx->jfr = urma_create_jfr(ctx->urma_ctx, &jfr_cfg);
  if (!ctx->jfr) {
    fprintf(stderr, "urma_create_jfr failed\n");
    return false;
  }

  // Jetty with share_jfr (perftest create_jetty:616-629)
  urma_jetty_cfg_t jetty_cfg{};
  jetty_cfg.flag.bs.share_jfr = 1;
  jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(cfg.jfs_depth);
  jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
  jetty_cfg.jfs_cfg.priority = cfg.priority;
  jetty_cfg.jfs_cfg.max_sge = 1;
  jetty_cfg.jfs_cfg.max_rsge = 1;
  jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
  jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
  jetty_cfg.jfs_cfg.jfc = ctx->jfc;
  jetty_cfg.shared.jfr = ctx->jfr;
  jetty_cfg.shared.jfc = ctx->jfc;
  ctx->jetty = urma_create_jetty(ctx->urma_ctx, &jetty_cfg);
  if (!ctx->jetty) {
    fprintf(stderr, "urma_create_jetty failed\n");
    return false;
  }

  return true;
}

static bool register_data_buffer(test_context* ctx, const test_config& cfg) {
  // Allocate and register data buffer (perftest register_mem:756-813)
  size_t buf_len = cfg.payload_size * 4;  // enough space
  ctx->local_buf = ::aligned_alloc(4096, buf_len);
  if (!ctx->local_buf) {
    perror("aligned_alloc");
    return false;
  }
  memset(ctx->local_buf, 'x', buf_len);

  urma_reg_seg_flag_t flag{};
  flag.bs.token_policy = URMA_TOKEN_NONE;
  flag.bs.cacheable = URMA_NON_CACHEABLE;
  flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;

  urma_seg_cfg_t seg_cfg{};
  seg_cfg.va = (uint64_t)ctx->local_buf;
  seg_cfg.len = buf_len;
  seg_cfg.flag = flag;

  ctx->local_tseg = urma_register_seg(ctx->urma_ctx, &seg_cfg);
  if (!ctx->local_tseg) {
    fprintf(stderr, "urma_register_seg failed: errno=%d\n", errno);
    return false;
  }
  return true;
}

// Pack segment info + jetty ID for TCP exchange
struct __attribute__((packed)) handshake_info {
  urma_seg_t seg;
  urma_jetty_id_t jetty_id;
};

static handshake_info make_local_info(test_context* ctx) {
  handshake_info info{};
  info.seg = ctx->local_tseg->seg;
  info.jetty_id = ctx->jetty->jetty_id;
  return info;
}

static bool import_remote_peer(test_context* ctx, const handshake_info& peer,
                                const test_config& cfg) {
  // Import remote segment (perftest import_seg_for_duplex:1473)
  urma_import_seg_flag_t seg_flag{};
  seg_flag.bs.cacheable = URMA_NON_CACHEABLE;
  seg_flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
  seg_flag.bs.mapping = URMA_SEG_NOMAP;
  urma_token_t seg_token{};
  urma_seg_t remote_seg_copy = peer.seg;
  ctx->import_tseg = urma_import_seg(ctx->urma_ctx, &remote_seg_copy,
                                      &seg_token, 0, seg_flag);
  if (!ctx->import_tseg) {
    fprintf(stderr, "urma_import_seg failed: errno=%d\n", errno);
    // Continue anyway (segment import is non-critical for SEND)
  }

  // Import remote jetty with has_drv_ext for bonding (perftest connect_jetty_default:1676-1684)
  urma_rjetty_t rjetty{};
  rjetty.jetty_id = peer.jetty_id;
  rjetty.trans_mode = URMA_TM_RM;
  rjetty.type = URMA_JETTY;
  rjetty.tp_type = cfg.tp_type;

  bool is_bonding = cfg.device.compare(0, 7, "bonding") == 0;
  bondp_rjetty_t bondp_rjetty{};
  if (is_bonding) {
    rjetty.flag.bs.has_drv_ext = 1;
    bondp_rjetty.base = rjetty;
    bondp_rjetty.jetty = ctx->jetty;
  }

  urma_token_t token{};
  ctx->import_tjetty = urma_import_jetty(
      ctx->urma_ctx,
      is_bonding ? &bondp_rjetty.base : &rjetty,
      &token);
  if (!ctx->import_tjetty) {
    fprintf(stderr, "urma_import_jetty failed: errno=%d\n", errno);
    return false;
  }
  return true;
}

static bool post_recv_buffers(test_context* ctx, const test_config& cfg) {
  // Post recv buffers using urma_post_jfr_wr matching perftest SEND BW
  // (perftest_run_test.c:1305).  For share_jfr, the JFR is shared by the jetty.
  urma_jfr_wr_t* bad_wr = nullptr;
  for (uint32_t i = 0; i < cfg.jfr_depth; i++) {
    char* buf = (char*)ctx->local_buf;
    urma_sge_t sge{(uint64_t)buf, cfg.payload_size,
                   (urma_target_seg_t*)ctx->local_tseg, nullptr};
    urma_sg_t sg{&sge, 1};
    urma_jfr_wr_t wr{sg, 0, nullptr};
    auto st = urma_post_jfr_wr(ctx->jfr, &wr, &bad_wr);
    if (st != URMA_SUCCESS) {
      fprintf(stderr, "urma_post_jfr_wr failed: %d\n", (int)st);
      return false;
    }
  }
  return true;
}

static bool send_and_poll(test_context* ctx, const test_config& cfg) {
  // Rearm JFCE (perftest rearm_jfc)
  if (urma_rearm_jfc(ctx->jfc, false) != URMA_SUCCESS) {
    fprintf(stderr, "urma_rearm_jfc failed\n");
    return false;
  }

  // Build SEND WR (matching perftest init_jfs_send_wr)
  urma_sge_t sge{(uint64_t)ctx->local_buf, cfg.payload_size,
                 (urma_target_seg_t*)ctx->local_tseg, nullptr};
  urma_sg_t sg{&sge, 1};
  urma_send_wr_t send_wr{};
  send_wr.src = sg;

  urma_jfs_wr_t wr{};
  wr.opcode = URMA_OPC_SEND;
  wr.flag.bs.complete_enable = 1;
  wr.tjetty = ctx->import_tjetty;
  wr.user_ctx = 1;
  wr.send = send_wr;

  urma_jfs_wr_t* bad_wr = nullptr;
  auto st = urma_post_jetty_send_wr(ctx->jetty, &wr, &bad_wr);
  if (st != URMA_SUCCESS) {
    fprintf(stderr, "urma_post_jetty_send_wr failed: %d\n", (int)st);
    return false;
  }
  ctx->scnt++;

  // Poll for completion (matching perftest poll loop)
  urma_cr_t cr{};
  for (int attempt = 0; attempt < 1000; attempt++) {
    int cnt = urma_poll_jfc(ctx->jfc, 1, &cr);
    if (cnt > 0) {
      ctx->ccnt++;
      if (cr.status != URMA_CR_SUCCESS) {
        fprintf(stderr, "SEND completion failed: status=%d (10=RNR)\n",
                (int)cr.status);
        return false;
      }
      return true;
    }
    if (cnt < 0) {
      fprintf(stderr, "urma_poll_jfc failed: %d\n", cnt);
      return false;
    }
    // Yield CPU
    std::this_thread::sleep_for(std::chrono::microseconds(5));
  }
  fprintf(stderr, "Timeout waiting for send completion\n");
  return false;
}

// ---- server ----

static int run_server(uint16_t port, const test_config& cfg) {
  test_context ctx{};
  if (!init_device(&ctx, cfg)) return 1;
  if (!create_resources(&ctx, cfg)) return 1;
  if (!register_data_buffer(&ctx, cfg)) return 1;

  // Wait for TCP connection
  int listen_fd = tcp_listen(port);
  if (listen_fd < 0) return 1;
  printf("[server] listening on port %u\n", port);
  int conn_fd = tcp_accept(listen_fd);
  if (conn_fd < 0) return 1;
  ::close(listen_fd);

  // Send local info, receive peer info
  auto local_info = make_local_info(&ctx);
  if (!tcp_send(conn_fd, &local_info, sizeof(local_info))) {
    fprintf(stderr, "tcp_send failed\n");
    return 1;
  }
  handshake_info peer_info{};
  if (!tcp_recv(conn_fd, &peer_info, sizeof(peer_info))) {
    fprintf(stderr, "tcp_recv failed\n");
    return 1;
  }
  ::close(conn_fd);

  // Import peer FIRST (matching perftest: connect_jetty before post_recv)
  if (!import_remote_peer(&ctx, peer_info, cfg)) return 1;

  // Then post recv buffers (after remote jetty is established)
  if (!post_recv_buffers(&ctx, cfg)) return 1;

  printf("[server] waiting for sends...\n");

  // Poll loop (server receives)
  urma_cr_t cr{};
  while (true) {
    int cnt = urma_poll_jfc(ctx.jfc, 1, &cr);
    if (cnt > 0) {
      if (cr.status != URMA_CR_SUCCESS) {
        fprintf(stderr, "[server] RECV completion failed: status=%d\n",
                (int)cr.status);
        return 1;
      }
      printf("[server] recv completion: len=%lu, user_ctx=%lu\n",
             cr.completion_len, cr.user_ctx);
    } else if (cnt < 0) {
      fprintf(stderr, "[server] poll failed\n");
      return 1;
    }
  }
  return 0;
}

// ---- client ----

static int run_client(const char* host, uint16_t port, const test_config& cfg) {
  test_context ctx{};
  if (!init_device(&ctx, cfg)) return 1;
  if (!create_resources(&ctx, cfg)) return 1;
  if (!register_data_buffer(&ctx, cfg)) return 1;

  // Connect to server via TCP
  int conn_fd = tcp_connect(host, port);
  if (conn_fd < 0) return 1;

  // Receive server info, send local info
  handshake_info peer_info{};
  if (!tcp_recv(conn_fd, &peer_info, sizeof(peer_info))) {
    fprintf(stderr, "tcp_recv failed\n");
    return 1;
  }
  auto local_info = make_local_info(&ctx);
  if (!tcp_send(conn_fd, &local_info, sizeof(local_info))) {
    fprintf(stderr, "tcp_send failed\n");
    return 1;
  }
  ::close(conn_fd);

  // Import peer
  if (!import_remote_peer(&ctx, peer_info, cfg)) return 1;

  printf("[client] connected, sending %u messages...\n", cfg.iters);

  for (uint32_t i = 0; i < cfg.iters; i++) {
    if (!send_and_poll(&ctx, cfg)) {
      fprintf(stderr, "[client] send %u failed\n", i);
      return 1;
    }
    printf("[client] send %u OK\n", i);
  }
  printf("[client] all sends completed successfully\n");
  return 0;
}

// ---- main ----

int main(int argc, char** argv) {
  test_config cfg;
  std::string role = "client";
  std::string host;
  uint16_t port = 9101;

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "server") role = "server";
    else if (arg == "client") role = "client";
    else if (arg == "--host" && i + 1 < argc) host = argv[++i];
    else if (arg == "--port" && i + 1 < argc) port = (uint16_t)atoi(argv[++i]);
    else if (arg == "--device" && i + 1 < argc) cfg.device = argv[++i];
    else if (arg == "--payload" && i + 1 < argc) cfg.payload_size = (uint32_t)atoi(argv[++i]);
    else if (arg == "--iters" && i + 1 < argc) cfg.iters = (uint32_t)atoi(argv[++i]);
    else if (arg == "--log" && i + 1 < argc) {
      std::string level = argv[++i];
      if (level == "info") easylog::set_min_severity(easylog::Severity::INFO);
      else if (level == "debug") easylog::set_min_severity(easylog::Severity::DEBUG);
    }
  }

  if (role == "server") {
    return run_server(port, cfg);
  } else {
    if (host.empty()) {
      fprintf(stderr, "client requires --host\n");
      return 1;
    }
    return run_client(host.c_str(), port, cfg);
  }
}

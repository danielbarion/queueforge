/* Open N AMQP 0-9-1 connections, hold them, then time one-confirm publishes.
   Linux build for a container on the broker network.
   Build: gcc -O2 -Wall -Wextra -o connscale-linux connscale-linux.c
   Usage: connscale-linux <port> <n> <hold_ms> <probes> <ip> [ip...]
*/
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

enum { EVFILT_READ = 1, EVFILT_WRITE = 2 };

enum {
  ST_NEW = 0,
  ST_CONNECTING,
  ST_WAIT_START,
  ST_WAIT_TUNE,
  ST_WAIT_OPEN,
  ST_WAIT_CHAN,
  ST_UP,
  ST_RETRY,
  ST_FAIL
};

enum { FAIL_NONE = 0, FAIL_REFUSED, FAIL_RESET, FAIL_TIMEOUT, FAIL_OTHER, FAIL_ADDR };

typedef struct {
  int fd;
  int state;
  int fail;
  int retries;
  int dest;
  long long start_ms;
  long long retry_at;
  const unsigned char *wbuf;
  int woff;
  int wlen;
  unsigned char rbuf[8192];
  int rlen;
  int hb;
  int interest;
} Conn;

static Conn *g_conns;

static unsigned char FRAME_START_OK[96];
static int LEN_START_OK;
static unsigned char FRAME_TUNE_OPEN[64];
static int LEN_TUNE_OPEN;
static unsigned char FRAME_CHAN[32];
static int LEN_CHAN;
static unsigned char FRAME_HB[8];
static int *fd_of;
static int fd_cap;

static void remember(int fd, int idx) {
  int i;
  int ncap;
  if (fd < fd_cap) {
    fd_of[fd] = idx;
    return;
  }
  ncap = fd_cap ? fd_cap : 4096;
  while (ncap <= fd) ncap *= 2;
  fd_of = realloc(fd_of, (size_t)ncap * sizeof(int));
  for (i = fd_cap; i < ncap; i++) fd_of[i] = -1;
  fd_cap = ncap;
  fd_of[fd] = idx;
}

static int index_of(int fd) {
  if (fd < 0 || fd >= fd_cap) return -1;
  return fd_of[fd];
}

static long long now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static long long now_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static int env_int(const char *name, int fallback, int lo, int hi) {
  const char *v = getenv(name);
  int n;
  if (!v || !*v) return fallback;
  n = atoi(v);
  if (n < lo) return lo;
  if (n > hi) return hi;
  return n;
}

static int put_frame(unsigned char *out, int channel, const unsigned char *payload, int len) {
  out[0] = 1;
  out[1] = (unsigned char)(channel >> 8);
  out[2] = (unsigned char)channel;
  out[3] = (unsigned char)(len >> 24);
  out[4] = (unsigned char)(len >> 16);
  out[5] = (unsigned char)(len >> 8);
  out[6] = (unsigned char)len;
  memcpy(out + 7, payload, (size_t)len);
  out[7 + len] = 0xCE;
  return 8 + len;
}

static void build_frames(void) {
  unsigned char p[80];
  int n = 0;
  const char *user = "admin";
  const char *pass = "devpassword12";
  int ulen = 5;
  int plen = 13;
  int sasl = 1 + ulen + 1 + plen;
  p[n++] = 0;
  p[n++] = 10;
  p[n++] = 0;
  p[n++] = 11;
  p[n++] = 0;
  p[n++] = 0;
  p[n++] = 0;
  p[n++] = 0;
  p[n++] = 5;
  memcpy(p + n, "PLAIN", 5);
  n += 5;
  p[n++] = 0;
  p[n++] = 0;
  p[n++] = 0;
  p[n++] = (unsigned char)sasl;
  p[n++] = 0;
  memcpy(p + n, user, (size_t)ulen);
  n += ulen;
  p[n++] = 0;
  memcpy(p + n, pass, (size_t)plen);
  n += plen;
  p[n++] = 5;
  memcpy(p + n, "en_US", 5);
  n += 5;
  LEN_START_OK = put_frame(FRAME_START_OK, 0, p, n);

  n = 0;
  p[n++] = 0;
  p[n++] = 10;
  p[n++] = 0;
  p[n++] = 31;
  p[n++] = 0x07;
  p[n++] = 0xFF;
  p[n++] = 0;
  p[n++] = 0x02;
  p[n++] = 0;
  p[n++] = 0;
  p[n++] = 0;
  p[n++] = 0;
  LEN_TUNE_OPEN = put_frame(FRAME_TUNE_OPEN, 0, p, n);
  n = 0;
  p[n++] = 0;
  p[n++] = 10;
  p[n++] = 0;
  p[n++] = 40;
  p[n++] = 1;
  p[n++] = '/';
  p[n++] = 0;
  p[n++] = 0;
  LEN_TUNE_OPEN += put_frame(FRAME_TUNE_OPEN + LEN_TUNE_OPEN, 0, p, n);

  n = 0;
  p[n++] = 0;
  p[n++] = 20;
  p[n++] = 0;
  p[n++] = 10;
  p[n++] = 0;
  LEN_CHAN = put_frame(FRAME_CHAN, 1, p, n);

  FRAME_HB[0] = 8;
  FRAME_HB[7] = 0xCE;
}

static void watch(int ep, int fd, int filter, int on) {
  int idx = index_of(fd);
  uint32_t bit;
  uint32_t cur;
  uint32_t next;
  struct epoll_event ev;
  if (idx < 0 || !g_conns) return;
  bit = (filter == EVFILT_READ) ? EPOLLIN : EPOLLOUT;
  cur = (uint32_t)g_conns[idx].interest;
  next = on ? (cur | bit) : (cur & ~bit);
  if (next == cur) return;
  memset(&ev, 0, sizeof(ev));
  ev.events = next;
  ev.data.fd = fd;
  if (cur == 0) {
    if (epoll_ctl(ep, EPOLL_CTL_ADD, fd, &ev) != 0) return;
  } else if (next == 0) {
    if (epoll_ctl(ep, EPOLL_CTL_DEL, fd, NULL) != 0) return;
  } else if (epoll_ctl(ep, EPOLL_CTL_MOD, fd, &ev) != 0) {
    return;
  }
  g_conns[idx].interest = (int)next;
}

static void arm_write(Conn *c, const unsigned char *buf, int len) {
  c->wbuf = buf;
  c->woff = 0;
  c->wlen = len;
}

static void drop_fd(Conn *c, int kq) {
  if (c->fd < 0) return;
  watch(kq, c->fd, EVFILT_READ, 0);
  watch(kq, c->fd, EVFILT_WRITE, 0);
  if (c->fd < fd_cap) fd_of[c->fd] = -1;
  close(c->fd);
  c->fd = -1;
}

static void fail_conn(Conn *c, int why, int kq) {
  drop_fd(c, kq);
  c->state = ST_FAIL;
  c->fail = why;
}

static int start_conn(Conn *c, int idx, int kq, struct sockaddr_in *dests, int ndest) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  int flags;
  int rc;
  if (fd < 0) {
    c->state = ST_RETRY;
    c->retry_at = now_ms() + 50;
    return -1;
  }
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  rc = connect(fd, (struct sockaddr *)&dests[c->dest % ndest], sizeof(dests[0]));
  c->fd = fd;
  remember(fd, idx);
  c->rlen = 0;
  c->hb = 0;
  c->start_ms = now_ms();
  if (rc == 0) {
    static const unsigned char hdr[8] = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};
    arm_write(c, hdr, 8);
    c->state = ST_WAIT_START;
    watch(kq, fd, EVFILT_READ, 1);
    watch(kq, fd, EVFILT_WRITE, 1);
    return 0;
  }
  if (errno == EINPROGRESS) {
    c->state = ST_CONNECTING;
    watch(kq, fd, EVFILT_READ, 1);
    watch(kq, fd, EVFILT_WRITE, 1);
    return 0;
  }
  if (errno == ECONNREFUSED || errno == EADDRNOTAVAIL) {
    close(fd);
    c->fd = -1;
    c->retries++;
    if (c->retries > 40) {
      c->state = ST_FAIL;
      c->fail = errno == EADDRNOTAVAIL ? FAIL_ADDR : FAIL_REFUSED;
      return 0;
    }
    c->state = ST_RETRY;
    c->retry_at = now_ms() + 20;
    c->dest++;
    return 0;
  }
  close(fd);
  c->fd = -1;
  c->state = ST_FAIL;
  c->fail = FAIL_OTHER;
  return 0;
}

static int pump_write(Conn *c, int kq) {
  while (c->woff < c->wlen) {
    ssize_t n = write(c->fd, c->wbuf + c->woff, (size_t)(c->wlen - c->woff));
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
      if (errno == EPIPE || errno == ECONNRESET) {
        fail_conn(c, FAIL_RESET, kq);
        return -1;
      }
      fail_conn(c, FAIL_OTHER, kq);
      return -1;
    }
    c->woff += (int)n;
  }
  if (c->hb) {
    c->hb = 0;
    arm_write(c, FRAME_HB, 8);
    return pump_write(c, kq);
  }
  watch(kq, c->fd, EVFILT_WRITE, 0);
  return 0;
}

static void on_method(Conn *c, int cls, int mid, int kq) {
  if (cls == 10 && mid == 10 && c->state == ST_WAIT_START) {
    arm_write(c, FRAME_START_OK, LEN_START_OK);
    c->state = ST_WAIT_TUNE;
    watch(kq, c->fd, EVFILT_WRITE, 1);
    return;
  }
  if (cls == 10 && mid == 30 && c->state == ST_WAIT_TUNE) {
    arm_write(c, FRAME_TUNE_OPEN, LEN_TUNE_OPEN);
    c->state = ST_WAIT_OPEN;
    watch(kq, c->fd, EVFILT_WRITE, 1);
    return;
  }
  if (cls == 10 && mid == 41 && c->state == ST_WAIT_OPEN) {
    arm_write(c, FRAME_CHAN, LEN_CHAN);
    c->state = ST_WAIT_CHAN;
    watch(kq, c->fd, EVFILT_WRITE, 1);
    return;
  }
  if (cls == 20 && mid == 11 && c->state == ST_WAIT_CHAN) {
    c->state = ST_UP;
    return;
  }
  if (cls == 10 && mid == 50) {
    fail_conn(c, FAIL_RESET, kq);
  }
}

static void on_read(Conn *c, int kq) {
  for (;;) {
    ssize_t n;
    if (c->rlen >= (int)sizeof(c->rbuf)) {
      fail_conn(c, FAIL_OTHER, kq);
      return;
    }
    n = read(c->fd, c->rbuf + c->rlen, sizeof(c->rbuf) - (size_t)c->rlen);
    if (n == 0) {
      fail_conn(c, c->state == ST_UP ? FAIL_RESET : FAIL_REFUSED, kq);
      return;
    }
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;
      fail_conn(c, FAIL_RESET, kq);
      return;
    }
    c->rlen += (int)n;
  }
  {
    int off = 0;
    while (c->state != ST_FAIL && c->rlen - off >= 8) {
      unsigned char *b = c->rbuf + off;
      int type = b[0];
      int size = ((int)b[3] << 24) | ((int)b[4] << 16) | ((int)b[5] << 8) | b[6];
      int total;
      if (size < 0 || size > 200000) {
        fail_conn(c, FAIL_OTHER, kq);
        return;
      }
      total = 8 + size;
      if (c->rlen - off < total) break;
      if (b[7 + size] != 0xCE) {
        fail_conn(c, FAIL_OTHER, kq);
        return;
      }
      if (type == 8) {
        if (!c->wbuf || c->woff >= c->wlen) {
          arm_write(c, FRAME_HB, 8);
          watch(kq, c->fd, EVFILT_WRITE, 1);
        } else {
          c->hb = 1;
        }
      } else if (type == 1 && size >= 4) {
        int cls = ((int)b[7] << 8) | b[8];
        int mid = ((int)b[9] << 8) | b[10];
        on_method(c, cls, mid, kq);
      }
      off += total;
    }
    if (off > 0) {
      memmove(c->rbuf, c->rbuf + off, (size_t)(c->rlen - off));
      c->rlen -= off;
    }
  }
}

static void on_write_ready(Conn *c, int kq) {
  if (c->state == ST_CONNECTING) {
    int err = 0;
    socklen_t sl = sizeof(err);
    getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &sl);
    if (err == ECONNREFUSED || err == EADDRNOTAVAIL || err == ETIMEDOUT) {
      int why = err;
      drop_fd(c, kq);
      c->retries++;
      if (c->retries > 40) {
        c->state = ST_FAIL;
        c->fail = why == EADDRNOTAVAIL ? FAIL_ADDR : FAIL_REFUSED;
        return;
      }
      c->state = ST_RETRY;
      c->retry_at = now_ms() + 15;
      c->dest++;
      return;
    }
    if (err != 0) {
      fail_conn(c, FAIL_OTHER, kq);
      return;
    }
    {
      static const unsigned char hdr[8] = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};
      arm_write(c, hdr, 8);
      c->state = ST_WAIT_START;
    }
  }
  if (c->wlen > c->woff) pump_write(c, kq);
}

static int cmp_ll(const void *a, const void *b) {
  long long x = *(const long long *)a;
  long long y = *(const long long *)b;
  return (x > y) - (x < y);
}

/* poll, not select: macOS select rejects fd >= FD_SETSIZE (1024), and a held herd uses those fds. */
static int read_frame(int fd, unsigned char *buf, int cap, int timeout_ms, int *out_len) {
  long long deadline = now_ms() + timeout_ms;
  int got = 0;
  int total = 8;
  while (got < total) {
    struct pollfd pfd;
    long long left = deadline - now_ms();
    ssize_t n;
    int rc;
    if (left <= 0) return -1;
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    rc = poll(&pfd, 1, (int)left);
    if (rc <= 0) return -1;
    if (got >= cap) return -1;
    n = read(fd, buf + got, (size_t)(cap - got));
    if (n <= 0) return -1;
    got += (int)n;
    if (got >= 7) {
      int size = ((int)buf[3] << 24) | ((int)buf[4] << 16) | ((int)buf[5] << 8) | buf[6];
      total = 8 + size;
      if (total > cap || size < 0) return -1;
    }
  }
  *out_len = total;
  return buf[0];
}

static int blocking_connect(struct sockaddr_in *addr) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  if (fd < 0) return -1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  if (connect(fd, (struct sockaddr *)addr, sizeof(*addr)) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

static int wait_method(int fd, int want_cls, int want_mid) {
  unsigned char buf[8192];
  int guard = 0;
  while (guard++ < 20) {
    int len = 0;
    int type = read_frame(fd, buf, (int)sizeof(buf), 3000, &len);
    if (type < 0) return -1;
    if (type == 8) {
      if (write(fd, FRAME_HB, 8) != 8) return -1;
      continue;
    }
    if (type == 1 && len >= 12) {
      int cls = ((int)buf[7] << 8) | buf[8];
      int mid = ((int)buf[9] << 8) | buf[10];
      if (cls == 10 && mid == 50) return -1;
      if (cls == want_cls && mid == want_mid) return 0;
    }
  }
  return -1;
}

static int probe(struct sockaddr_in *addr, int samples, long long *p50, long long *p99) {
  int fd = blocking_connect(addr);
  unsigned char buf[256];
  unsigned char payload[64];
  long long *lats;
  int ok = 0;
  int i;
  int n;
  static const unsigned char hdr[8] = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};
  const char *q = "cscale";
  if (fd < 0 || samples <= 0) return 0;
  if (write(fd, hdr, 8) != 8) goto done;
  if (wait_method(fd, 10, 10) != 0) goto done;
  if (write(fd, FRAME_START_OK, LEN_START_OK) != LEN_START_OK) goto done;
  if (wait_method(fd, 10, 30) != 0) goto done;
  if (write(fd, FRAME_TUNE_OPEN, LEN_TUNE_OPEN) != LEN_TUNE_OPEN) goto done;
  if (wait_method(fd, 10, 41) != 0) goto done;
  if (write(fd, FRAME_CHAN, LEN_CHAN) != LEN_CHAN) goto done;
  if (wait_method(fd, 20, 11) != 0) goto done;

  n = 0;
  payload[n++] = 0;
  payload[n++] = 50;
  payload[n++] = 0;
  payload[n++] = 10;
  payload[n++] = 0;
  payload[n++] = 0;
  payload[n++] = 6;
  memcpy(payload + n, q, 6);
  n += 6;
  payload[n++] = 0x02;
  payload[n++] = 0;
  payload[n++] = 0;
  payload[n++] = 0;
  payload[n++] = 0;
  {
    int fl = put_frame(buf, 1, payload, n);
    if (write(fd, buf, (size_t)fl) != fl) goto done;
  }
  if (wait_method(fd, 50, 11) != 0) goto done;

  n = 0;
  payload[n++] = 0;
  payload[n++] = 85;
  payload[n++] = 0;
  payload[n++] = 10;
  payload[n++] = 0;
  {
    int fl = put_frame(buf, 1, payload, n);
    if (write(fd, buf, (size_t)fl) != fl) goto done;
  }
  if (wait_method(fd, 85, 11) != 0) goto done;

  lats = calloc((size_t)samples, sizeof(long long));
  if (!lats) goto done;
  for (i = 0; i < samples; i++) {
    unsigned char pub[256];
    int pn = 0;
    int fl;
    long long t0, t1;
    unsigned char header[16];
    payload[0] = 0;
    payload[1] = 60;
    payload[2] = 0;
    payload[3] = 40;
    payload[4] = 0;
    payload[5] = 0;
    payload[6] = 0;
    payload[7] = 6;
    memcpy(payload + 8, q, 6);
    payload[14] = 0;
    fl = put_frame(pub, 1, payload, 15);
    pn = fl;
    /* content header: class 60, weight 0, body size 1, delivery-mode=2 */
    header[0] = 0;
    header[1] = 60;
    header[2] = 0;
    header[3] = 0;
    memset(header + 4, 0, 7);
    header[11] = 1;
    header[12] = 0x10;
    header[13] = 0;
    header[14] = 2;
    pub[pn++] = 2;
    pub[pn++] = 0;
    pub[pn++] = 1;
    pub[pn++] = 0;
    pub[pn++] = 0;
    pub[pn++] = 0;
    pub[pn++] = 15;
    memcpy(pub + pn, header, 15);
    pn += 15;
    pub[pn++] = 0xCE;
    pub[pn++] = 3;
    pub[pn++] = 0;
    pub[pn++] = 1;
    pub[pn++] = 0;
    pub[pn++] = 0;
    pub[pn++] = 0;
    pub[pn++] = 1;
    pub[pn++] = 'x';
    pub[pn++] = 0xCE;
    t0 = now_us();
    if (write(fd, pub, (size_t)pn) != pn) break;
    if (wait_method(fd, 60, 80) != 0) break;
    t1 = now_us();
    lats[ok++] = t1 - t0;
  }
  if (ok > 0) {
    qsort(lats, (size_t)ok, sizeof(long long), cmp_ll);
    *p50 = lats[ok / 2];
    *p99 = lats[(ok * 99) / 100];
    if ((ok * 99) / 100 >= ok) *p99 = lats[ok - 1];
  }
  free(lats);
done:
  if (fd >= 0) close(fd);
  return ok;
}

static void dispatch_events(int ep, struct epoll_event *evs, int nev, int up_only) {
  int i;
  for (i = 0; i < nev; i++) {
    int fd = (int)evs[i].data.fd;
    int idx = index_of(fd);
    uint32_t events = evs[i].events;
    if (idx < 0 || g_conns[idx].fd != fd) continue;
    if (up_only && g_conns[idx].state != ST_UP) continue;
    if ((events & (EPOLLERR | EPOLLHUP)) && g_conns[idx].state == ST_CONNECTING) {
      on_write_ready(&g_conns[idx], ep);
      continue;
    }
    if (events & EPOLLOUT) on_write_ready(&g_conns[idx], ep);
    if (g_conns[idx].fd == fd && (events & (EPOLLIN | EPOLLHUP | EPOLLERR))) on_read(&g_conns[idx], ep);
  }
}

int main(int argc, char **argv) {
  int port, n, hold_ms, probes, ndest, ep, i, inflight_cap, budget_ms;
  int up = 0, refused = 0, reset = 0, timed = 0, other = 0, held = 0, addr = 0, pending = 0;
  long long t0, connect_ms = 0, p50 = -1, p99 = -1;
  int probe_ok = 0;
  struct sockaddr_in *dests;
  Conn *conns;
  int started_clock = 0;
  if (argc < 6) {
    fprintf(stderr, "usage: %s <port> <n> <hold_ms> <probes> <ip> [ip...]\n", argv[0]);
    return 2;
  }
  port = atoi(argv[1]);
  n = atoi(argv[2]);
  hold_ms = atoi(argv[3]);
  probes = atoi(argv[4]);
  ndest = argc - 5;
  if (port <= 0 || n <= 0 || ndest <= 0) return 2;
  build_frames();
  dests = calloc((size_t)ndest, sizeof(*dests));
  conns = calloc((size_t)n, sizeof(*conns));
  if (!dests || !conns) return 1;
  for (i = 0; i < ndest; i++) {
    dests[i].sin_family = AF_INET;
    dests[i].sin_port = htons((unsigned short)port);
    if (inet_pton(AF_INET, argv[5 + i], &dests[i].sin_addr) != 1) {
      fprintf(stderr, "bad ip %s\n", argv[5 + i]);
      return 2;
    }
  }
  for (i = 0; i < n; i++) {
    conns[i].fd = -1;
    conns[i].dest = i;
  }
  signal(SIGPIPE, SIG_IGN);
  g_conns = conns;
  ep = epoll_create1(EPOLL_CLOEXEC);
  if (ep < 0) return 1;
  inflight_cap = env_int("CONN_INFLIGHT", 32, 1, 512);
  budget_ms = env_int("CONN_DEADLINE_MS", 180000, 1000, 600000);
  t0 = now_ms();
  {
    long long deadline = t0 + budget_ms;
    int done = 0;
    while (!done && now_ms() < deadline) {
      int inflight = 0;
      int finished = 0;
      struct epoll_event evs[256];
      int nev;
      long long tnow = now_ms();
      for (i = 0; i < n; i++) {
        int st = conns[i].state;
        if (st == ST_UP || st == ST_FAIL) {
          finished++;
          continue;
        }
        if (st == ST_NEW || (st == ST_RETRY && conns[i].retry_at <= tnow)) {
          if (inflight >= inflight_cap) continue;
          start_conn(&conns[i], i, ep, dests, ndest);
          st = conns[i].state;
        }
        if (st == ST_CONNECTING || st == ST_WAIT_START || st == ST_WAIT_TUNE || st == ST_WAIT_OPEN ||
            st == ST_WAIT_CHAN) {
          inflight++;
          if (tnow - conns[i].start_ms > 30000) fail_conn(&conns[i], FAIL_TIMEOUT, ep);
        }
      }
      if (finished == n) break;
      nev = epoll_wait(ep, evs, 256, 50);
      if (nev < 0) {
        if (errno == EINTR) continue;
        break;
      }
      dispatch_events(ep, evs, nev, 0);
    }
  }
  connect_ms = now_ms() - t0;
  for (i = 0; i < n; i++) {
    if (conns[i].state == ST_UP) up++;
    else if (conns[i].fail == FAIL_REFUSED) refused++;
    else if (conns[i].fail == FAIL_RESET) reset++;
    else if (conns[i].fail == FAIL_TIMEOUT) timed++;
    else if (conns[i].fail == FAIL_ADDR) addr++;
    else if (conns[i].state != ST_FAIL) pending++;
    else other++;
  }
  printf("HOLD connected=%d\n", up);
  fflush(stdout);
  if (hold_ms > 0 && up > 0) {
    long long until = now_ms() + hold_ms;
    while (now_ms() < until && access("/tmp/qf-release", F_OK) != 0) {
      struct epoll_event evs[256];
      int nev = epoll_wait(ep, evs, 256, 100);
      if (nev < 0) {
        if (errno == EINTR) continue;
        break;
      }
      dispatch_events(ep, evs, nev, 1);
    }
  }
  held = 0;
  for (i = 0; i < n; i++)
    if (conns[i].state == ST_UP) held++;
  if (probes > 0 && held > 0) probe_ok = probe(&dests[0], probes, &p50, &p99);
  printf(
      "RESULT connected=%d held=%d connect_ms=%lld refused=%d reset=%d timeout=%d other=%d "
      "addr=%d pending=%d probe_ok=%d probe_p50_ms=%.3f probe_p99_ms=%.3f\n",
      up, held, connect_ms, refused, reset, timed, other, addr, pending, probe_ok,
      p50 < 0 ? -1.0 : p50 / 1000.0, p99 < 0 ? -1.0 : p99 / 1000.0);
  fflush(stdout);
  for (i = 0; i < n; i++)
    if (conns[i].fd >= 0) close(conns[i].fd);
  close(ep);
  g_conns = NULL;
  free(conns);
  free(dests);
  (void)started_clock;
  return 0;
}

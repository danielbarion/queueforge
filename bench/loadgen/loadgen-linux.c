#define _GNU_SOURCE
/* Multi-connection AMQP 0-9-1 load generator for a Linux container.
   One classic durable queue set, persistent 256-byte bodies, publisher
   confirms, and acking consumers. Build:
   gcc -O2 -Wall -Wextra -o loadgen-linux loadgen-linux.c
*/
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

enum {
  S_CONNECTING = 0,
  S_WAIT_START,
  S_WAIT_TUNE,
  S_WAIT_OPEN,
  S_WAIT_CHAN,
  S_READY,
  S_WAIT_DECLARE,
  S_WAIT_CONFIRM,
  S_WAIT_QOS,
  S_WAIT_CONSUME,
  S_RUN,
  S_FAIL
};

enum { R_PING = 1, R_SETUP, R_PUB, R_CON };
enum { EX_METHOD = 0, EX_HEADER, EX_BODY };
enum { PH_WAIT_READY = 0, PH_DECLARE, PH_ARM, PH_RUN };
enum { RCAP = 256 * 1024, WCAP = 256 * 1024, MAX_Q = 64, MAX_CONN = 160 };

typedef struct {
  int idx;
  int fd;
  int state;
  int role;
  int qindex;
  int interest;
  int in_epoll;
  int blocked;
  int flow_paused;
  int expect;
  int recycling;
  int woff;
  int wlen;
  int rpos;
  int rlen;
  int declares_done;
  int inflight;
  int nfree;
  int ack_since_flush;
  unsigned char *wbuf;
  unsigned char *rbuf;
  unsigned long long body_need;
  unsigned long long body_got;
  unsigned long long deliver_tag;
  unsigned long long next_tag;
  int *free_stack;
  unsigned long long *slot_tag;
  long long *slot_us;
  unsigned char *slot_used;
  unsigned char consume_frame[160];
  int consume_len;
  char err[160];
} Conn;

static Conn *conns;
static int nconns;
static int epfd = -1;
static int g_window = 128;
static int g_pubs, g_cons, g_queues, g_prefetch, g_body;
static int phase;
static int ping_mode;
static int failed;
static int marked;
static int finishing;
static long long t0, t_mark, t_end;
static long long warmup_us, measure_us, startup_us;
static char first_err[160];

static unsigned long long hist[2001];
static unsigned long long samples_n, confirmed, consumed, sent_n, nacks, missed, returns_n, blocked_n;

static unsigned char FRAME_START_OK[128];
static unsigned char FRAME_TUNE_OPEN[128];
static unsigned char FRAME_CHAN[64];
static unsigned char FRAME_QOS[64];
static int LEN_START_OK, LEN_TUNE_OPEN, LEN_CHAN, LEN_QOS;

typedef struct {
  unsigned char *bytes;
  int len;
} Blob;

static Blob declare_blob[MAX_Q];
static Blob publish_blob[MAX_Q];

static long long now_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
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

static unsigned rd16(const unsigned char *p) { return ((unsigned)p[0] << 8) | p[1]; }

static unsigned rd32(const unsigned char *p) {
  return ((unsigned)p[0] << 24) | ((unsigned)p[1] << 16) | ((unsigned)p[2] << 8) | p[3];
}

static unsigned long long rd64(const unsigned char *p) {
  unsigned long long v = 0;
  int i;
  for (i = 0; i < 8; i++) v = (v << 8) | p[i];
  return v;
}

static void wr64(unsigned char *p, unsigned long long v) {
  int i;
  for (i = 7; i >= 0; i--) {
    p[i] = (unsigned char)v;
    v >>= 8;
  }
}

static void sanitize(char *s) {
  for (; *s; s++) {
    char c = *s;
    int ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
             c == '.' || c == ':' || c == '-';
    if (!ok) *s = '_';
  }
}

static void *xmalloc(size_t n) {
  void *p = calloc(1, n);
  if (!p) {
    fprintf(stderr, "oom\n");
    exit(1);
  }
  return p;
}

static int frame_write(unsigned char *dst, int cap, int *off, int type, int channel, const unsigned char *payload,
                       int len) {
  unsigned char *o;
  if (*off < 0 || len < 0 || *off + 8 + len > cap) return -1;
  o = dst + *off;
  o[0] = (unsigned char)type;
  o[1] = (unsigned char)(channel >> 8);
  o[2] = (unsigned char)channel;
  o[3] = (unsigned char)((len >> 24) & 0xff);
  o[4] = (unsigned char)((len >> 16) & 0xff);
  o[5] = (unsigned char)((len >> 8) & 0xff);
  o[6] = (unsigned char)(len & 0xff);
  if (len) memcpy(o + 7, payload, (size_t)len);
  o[7 + len] = 0xCE;
  *off += 8 + len;
  return 0;
}

static int put_method(unsigned char *out, int cap, int channel, const unsigned char *payload, int len) {
  int off = 0;
  if (frame_write(out, cap, &off, 1, channel, payload, len) < 0) return -1;
  return off;
}

static int build_handshake(void) {
  unsigned char p[96];
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
  LEN_START_OK = put_method(FRAME_START_OK, (int)sizeof FRAME_START_OK, 0, p, n);
  if (LEN_START_OK < 0) return -1;

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
  LEN_TUNE_OPEN = put_method(FRAME_TUNE_OPEN, (int)sizeof FRAME_TUNE_OPEN, 0, p, n);
  if (LEN_TUNE_OPEN < 0) return -1;
  n = 0;
  p[n++] = 0;
  p[n++] = 10;
  p[n++] = 0;
  p[n++] = 40;
  p[n++] = 1;
  p[n++] = '/';
  p[n++] = 0;
  p[n++] = 0;
  {
    int extra = put_method(FRAME_TUNE_OPEN + LEN_TUNE_OPEN, (int)sizeof FRAME_TUNE_OPEN - LEN_TUNE_OPEN, 0, p, n);
    if (extra < 0) return -1;
    LEN_TUNE_OPEN += extra;
  }

  n = 0;
  p[n++] = 0;
  p[n++] = 20;
  p[n++] = 0;
  p[n++] = 10;
  p[n++] = 0;
  LEN_CHAN = put_method(FRAME_CHAN, (int)sizeof FRAME_CHAN, 1, p, n);
  if (LEN_CHAN < 0) return -1;
  return 0;
}

static int build_qos(unsigned char *out, int cap, int prefetch) {
  unsigned char p[16];
  int m = 0;
  int off = 0;
  p[m++] = 0;
  p[m++] = 60;
  p[m++] = 0;
  p[m++] = 10;
  p[m++] = 0;
  p[m++] = 0;
  p[m++] = 0;
  p[m++] = 0;
  p[m++] = (unsigned char)(prefetch >> 8);
  p[m++] = (unsigned char)prefetch;
  p[m++] = 0;
  if (frame_write(out, cap, &off, 1, 1, p, m) < 0) return -1;
  return off;
}

static int build_declare(unsigned char *out, int cap, const char *qname) {
  unsigned char method[256];
  unsigned char entry[64];
  const char *key = "x-queue-type";
  const char *val = "classic";
  int qn = (int)strlen(qname);
  int klen = (int)strlen(key);
  int vlen = (int)strlen(val);
  int m = 0;
  int e = 0;
  int off = 0;
  method[m++] = 0;
  method[m++] = 50;
  method[m++] = 0;
  method[m++] = 10;
  method[m++] = 0;
  method[m++] = 0;
  method[m++] = (unsigned char)qn;
  memcpy(method + m, qname, (size_t)qn);
  m += qn;
  method[m++] = 0x02;
  entry[e++] = (unsigned char)klen;
  memcpy(entry + e, key, (size_t)klen);
  e += klen;
  entry[e++] = 'S';
  entry[e++] = 0;
  entry[e++] = 0;
  entry[e++] = 0;
  entry[e++] = (unsigned char)vlen;
  memcpy(entry + e, val, (size_t)vlen);
  e += vlen;
  method[m++] = 0;
  method[m++] = 0;
  method[m++] = (unsigned char)(e >> 8);
  method[m++] = (unsigned char)e;
  memcpy(method + m, entry, (size_t)e);
  m += e;
  if (frame_write(out, cap, &off, 1, 1, method, m) < 0) return -1;
  return off;
}

static int build_publish(unsigned char *out, int cap, const char *qname, int body_len) {
  unsigned char method[128];
  unsigned char header[32];
  int qn = (int)strlen(qname);
  int m = 0;
  int h = 0;
  int off = 0;
  int s;
  method[m++] = 0;
  method[m++] = 60;
  method[m++] = 0;
  method[m++] = 40;
  method[m++] = 0;
  method[m++] = 0;
  method[m++] = 0;
  method[m++] = (unsigned char)qn;
  memcpy(method + m, qname, (size_t)qn);
  m += qn;
  method[m++] = 0;
  header[h++] = 0;
  header[h++] = 60;
  header[h++] = 0;
  header[h++] = 0;
  for (s = 56; s >= 0; s -= 8) header[h++] = (unsigned char)((unsigned long long)body_len >> s);
  header[h++] = 0x10;
  header[h++] = 0x00;
  header[h++] = 2;
  if (frame_write(out, cap, &off, 1, 1, method, m) < 0) return -1;
  if (frame_write(out, cap, &off, 2, 1, header, h) < 0) return -1;
  if (off + 8 + body_len > cap) return -1;
  {
    unsigned char *o = out + off;
    o[0] = 3;
    o[1] = 0;
    o[2] = 1;
    o[3] = (unsigned char)((body_len >> 24) & 0xff);
    o[4] = (unsigned char)((body_len >> 16) & 0xff);
    o[5] = (unsigned char)((body_len >> 8) & 0xff);
    o[6] = (unsigned char)(body_len & 0xff);
    memset(o + 7, 'x', (size_t)body_len);
    o[7 + body_len] = 0xCE;
    off += 8 + body_len;
  }
  return off;
}

static int build_consume(unsigned char *out, int cap, const char *qname, const char *tag) {
  unsigned char method[192];
  int qn = (int)strlen(qname);
  int tn = (int)strlen(tag);
  int m = 0;
  int off = 0;
  method[m++] = 0;
  method[m++] = 60;
  method[m++] = 0;
  method[m++] = 20;
  method[m++] = 0;
  method[m++] = 0;
  method[m++] = (unsigned char)qn;
  memcpy(method + m, qname, (size_t)qn);
  m += qn;
  method[m++] = (unsigned char)tn;
  memcpy(method + m, tag, (size_t)tn);
  m += tn;
  method[m++] = 0;
  method[m++] = 0;
  method[m++] = 0;
  method[m++] = 0;
  method[m++] = 0;
  if (frame_write(out, cap, &off, 1, 1, method, m) < 0) return -1;
  return off;
}

static int build_confirm(unsigned char *out, int cap) {
  unsigned char p[8];
  int off = 0;
  p[0] = 0;
  p[1] = 85;
  p[2] = 0;
  p[3] = 10;
  p[4] = 0;
  if (frame_write(out, cap, &off, 1, 1, p, 5) < 0) return -1;
  return off;
}

static int build_ack(unsigned char *out, int cap, unsigned long long tag) {
  unsigned char p[16];
  int off = 0;
  p[0] = 0;
  p[1] = 60;
  p[2] = 0;
  p[3] = 80;
  wr64(p + 4, tag);
  p[12] = 0;
  if (frame_write(out, cap, &off, 1, 1, p, 13) < 0) return -1;
  return off;
}

static void finish(int reached);

static void fail_conn(Conn *c, const char *why) {
  if (ping_mode) {
    fprintf(stderr, "PING_FAIL %s\n", why);
    printf("connected=0\n");
    exit(1);
  }
  if (c->state == S_FAIL) return;
  snprintf(c->err, sizeof c->err, "%s", why);
  fprintf(stderr, "FAIL c%d %s\n", c->idx, why);
  c->state = S_FAIL;
  if (c->fd >= 0) {
    if (c->in_epoll) epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    c->fd = -1;
    c->in_epoll = 0;
  }
  if (!failed) {
    failed = 1;
    snprintf(first_err, sizeof first_err, "c%d_%s", c->idx, why);
  }
  finish(0);
}

static int inflight_sum(void) {
  int i, n = 0;
  if (!conns) return 0;
  for (i = 0; i < nconns; i++)
    if (conns[i].role == R_PUB) n += conns[i].inflight;
  return n;
}

static long long percentile(int pct) {
  unsigned long long target, cum = 0;
  int i;
  if (samples_n == 0) return -1;
  target = (samples_n * (unsigned long long)pct + 99ULL) / 100ULL;
  if (target < 1) target = 1;
  for (i = 0; i < 2000; i++) {
    cum += hist[i];
    if (cum >= target) return (long long)(i + 1) * 100;
  }
  return 200000;
}

static void finish(int reached) {
  long long now;
  double secs, confirm_s, consume_s, sent_s;
  char err[160];
  int ok;
  if (finishing) exit(1);
  finishing = 1;
  now = now_us();
  if (reached && measure_us > 0) secs = (double)measure_us / 1000000.0;
  else if (t_mark > 0 && now > t_mark) secs = (double)(now - t_mark) / 1000000.0;
  else secs = 0;
  confirm_s = secs > 0 ? (double)confirmed / secs : 0;
  consume_s = secs > 0 ? (double)consumed / secs : 0;
  sent_s = secs > 0 ? (double)sent_n / secs : 0;
  ok = reached && !failed;
  if (first_err[0]) {
    snprintf(err, sizeof err, "%s", first_err);
    sanitize(err);
  } else {
    snprintf(err, sizeof err, "none");
  }
  printf(
      "RESULT ok=%d confirm_s=%.1f consume_s=%.1f sent_s=%.1f p50_us=%lld p99_us=%lld confirmed=%llu consumed=%llu "
      "sent=%llu samples=%llu inflight=%d blocked=%llu nacks=%llu missed=%llu returns=%llu elapsed_s=%.3f "
      "pubs=%d cons=%d queues=%d window=%d prefetch=%d body=%d err=%s\n",
      ok, confirm_s, consume_s, sent_s, percentile(50), percentile(99), confirmed, consumed, sent_n, samples_n,
      inflight_sum(), blocked_n, nacks, missed, returns_n, secs, g_pubs, g_cons, g_queues, g_window, g_prefetch,
      g_body, err);
  fflush(stdout);
  exit(ok ? 0 : 1);
}

static int wspace(Conn *c) { return WCAP - c->wlen; }

static void wcompact(Conn *c) {
  int n;
  if (c->woff <= 0) return;
  n = c->wlen - c->woff;
  memmove(c->wbuf, c->wbuf + c->woff, (size_t)n);
  c->wlen = n;
  c->woff = 0;
}

static int enqueue(Conn *c, const unsigned char *p, int n) {
  if (n > wspace(c)) wcompact(c);
  if (n > wspace(c)) return -1;
  memcpy(c->wbuf + c->wlen, p, (size_t)n);
  c->wlen += n;
  return 0;
}

static void set_interest(Conn *c) {
  uint32_t ev;
  struct epoll_event e;
  int op;
  if (c->fd < 0 || c->state == S_FAIL) return;
  ev = EPOLLIN;
  if (c->state == S_CONNECTING || c->woff < c->wlen) ev |= EPOLLOUT;
  if (c->in_epoll && ev == (uint32_t)c->interest) return;
  memset(&e, 0, sizeof e);
  e.events = ev;
  e.data.u32 = (uint32_t)c->idx;
  op = c->in_epoll ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
  if (epoll_ctl(epfd, op, c->fd, &e) < 0) {
    fail_conn(c, "epoll_ctl");
    return;
  }
  c->in_epoll = 1;
  c->interest = (int)ev;
}

static int flush_conn(Conn *c) {
  while (c->woff < c->wlen) {
    int n = (int)send(c->fd, c->wbuf + c->woff, (size_t)(c->wlen - c->woff), MSG_NOSIGNAL);
    if (n > 0) {
      c->woff += n;
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    fail_conn(c, n == 0 ? "send0" : (errno == ECONNRESET ? "reset" : "send_err"));
    return -1;
  }
  if (c->woff >= c->wlen) {
    c->woff = 0;
    c->wlen = 0;
  } else if (c->woff > 65536) {
    wcompact(c);
  }
  return 0;
}

static void kick(Conn *c) {
  if (c->state == S_FAIL || c->fd < 0) return;
  flush_conn(c);
  if (c->state == S_FAIL || c->fd < 0) return;
  set_interest(c);
}

static void send_blob(Conn *c, const unsigned char *b, int n) {
  if (enqueue(c, b, n) < 0) fail_conn(c, "wbuf_full");
  else kick(c);
}

static void note_lat(long long sent_at, long long now) {
  long long d = now - sent_at;
  unsigned idx;
  if (d < 0) d = 0;
  if (d >= 200000) idx = 2000;
  else idx = (unsigned)(d / 100);
  hist[idx]++;
  samples_n++;
}

static void retire_slot(Conn *c, int s, long long now, int is_nack) {
  long long sent_at;
  if (!c->slot_used[s]) return;
  sent_at = c->slot_us[s];
  c->slot_used[s] = 0;
  if (c->nfree >= g_window) {
    fail_conn(c, "slot_overflow");
    return;
  }
  c->free_stack[c->nfree++] = s;
  if (c->inflight > 0) c->inflight--;
  if (!(t_mark && now >= t_mark && now < t_end)) return;
  if (is_nack) {
    nacks++;
    return;
  }
  confirmed++;
  if (sent_at >= t_mark) note_lat(sent_at, now);
}

static void on_ack(Conn *c, unsigned long long tag, int multiple, long long now, int is_nack) {
  int s, found = 0;
  if (c->role != R_PUB) return;
  if (multiple) {
    for (s = 0; s < g_window; s++) {
      if (c->slot_used[s] && c->slot_tag[s] <= tag) {
        retire_slot(c, s, now, is_nack);
        found = 1;
      }
    }
  } else {
    for (s = 0; s < g_window; s++) {
      if (c->slot_used[s] && c->slot_tag[s] == tag) {
        retire_slot(c, s, now, is_nack);
        found = 1;
        break;
      }
    }
  }
  if (!found && !multiple) missed++;
}

static void send_ack(Conn *c, unsigned long long tag) {
  unsigned char frame[32];
  int n = build_ack(frame, (int)sizeof frame, tag);
  if (n < 0 || enqueue(c, frame, n) < 0) {
    fail_conn(c, "ack_full");
    return;
  }
  c->ack_since_flush++;
  if (c->ack_since_flush >= 32) {
    kick(c);
    c->ack_since_flush = 0;
  }
}

static void finish_content(Conn *c, long long now) {
  if (c->recycling == 1) {
    send_ack(c, c->deliver_tag);
    if (t_mark && now >= t_mark && now < t_end) consumed++;
  } else if (c->recycling == 2) {
    returns_n++;
  }
  c->expect = EX_METHOD;
  c->recycling = 0;
}

static int skip_shortstr(const unsigned char **p, int *left) {
  int n;
  if (*left < 1) return -1;
  n = (*p)[0];
  if (*left < 1 + n) return -1;
  *p += 1 + n;
  *left -= 1 + n;
  return 0;
}

static void close_reason(const unsigned char *payload, int size, char *out, int outcap) {
  int code, sl, n, i;
  char tmp[96];
  if (size < 7) {
    snprintf(out, (size_t)outcap, "close_short");
    return;
  }
  code = (payload[4] << 8) | payload[5];
  sl = payload[6];
  if (size < 7 + sl) sl = size - 7;
  if (sl < 0) sl = 0;
  n = sl > 80 ? 80 : sl;
  memcpy(tmp, payload + 7, (size_t)n);
  tmp[n] = 0;
  for (i = 0; i < n; i++) {
    unsigned char ch = (unsigned char)tmp[i];
    if (ch < 32 || ch > 126) tmp[i] = '_';
  }
  snprintf(out, (size_t)outcap, "%d_%s", code, tmp);
}

static int all_ready(void) {
  int i;
  for (i = 0; i < nconns; i++)
    if (conns[i].state != S_READY) return 0;
  return 1;
}

static void begin_declare(void) {
  phase = PH_DECLARE;
  conns[0].declares_done = 0;
  conns[0].state = S_WAIT_DECLARE;
  send_blob(&conns[0], declare_blob[0].bytes, declare_blob[0].len);
}

static unsigned char FRAME_CONFIRM[32];
static int LEN_CONFIRM;

static void arm_clients(void) {
  int i;
  phase = PH_ARM;
  fprintf(stderr, "ARMED queues=%d\n", g_queues);
  for (i = 0; i < nconns; i++) {
    if (conns[i].role == R_PUB) {
      conns[i].state = S_WAIT_CONFIRM;
      send_blob(&conns[i], FRAME_CONFIRM, LEN_CONFIRM);
    } else if (conns[i].role == R_CON) {
      conns[i].state = S_WAIT_QOS;
      send_blob(&conns[i], FRAME_QOS, LEN_QOS);
    }
  }
}

static void check_run(long long now) {
  int i;
  if (phase != PH_ARM) return;
  for (i = 0; i < nconns; i++) {
    if (conns[i].role == R_PUB || conns[i].role == R_CON) {
      if (conns[i].state != S_RUN) return;
    }
  }
  phase = PH_RUN;
  t_mark = now + warmup_us;
  t_end = t_mark + measure_us;
  fprintf(stderr, "RUNNING\n");
}

static void emit_mark(void) {
  const char *path;
  marked = 1;
  path = getenv("MARK_PATH");
  if (path && *path) {
    FILE *f = fopen(path, "w");
    if (f) {
      fputs("MARK\n", f);
      fclose(f);
    }
  }
  printf("MARK\n");
  fflush(stdout);
}

static int unexpected_logs;

static void log_unexpected(Conn *c, unsigned cls, unsigned meth) {
  if (unexpected_logs++ > 16) return;
  fprintf(stderr, "UNEXPECTED c%d st=%d cls=%u meth=%u\n", c->idx, c->state, cls, meth);
}

static void on_method(Conn *c, const unsigned char *payload, int size, long long now) {
  unsigned cls, meth;
  char why[160];
  if (size < 4) {
    fail_conn(c, "short_method");
    return;
  }
  cls = rd16(payload);
  meth = rd16(payload + 2);
  if (cls == 10 && meth == 10 && c->state == S_WAIT_START) {
    send_blob(c, FRAME_START_OK, LEN_START_OK);
    c->state = S_WAIT_TUNE;
    return;
  }
  if (cls == 10 && meth == 30 && c->state == S_WAIT_TUNE) {
    send_blob(c, FRAME_TUNE_OPEN, LEN_TUNE_OPEN);
    c->state = S_WAIT_OPEN;
    return;
  }
  if (cls == 10 && meth == 41 && c->state == S_WAIT_OPEN) {
    send_blob(c, FRAME_CHAN, LEN_CHAN);
    c->state = S_WAIT_CHAN;
    return;
  }
  if (cls == 10 && meth == 50) {
    close_reason(payload, size, why, (int)sizeof why);
    fprintf(stderr, "AMQP_CLOSE c%d %s\n", c->idx, why);
    fail_conn(c, why);
    return;
  }
  if (cls == 10 && meth == 60) {
    c->blocked = 1;
    blocked_n++;
    return;
  }
  if (cls == 10 && meth == 61) {
    c->blocked = 0;
    return;
  }
  if (cls == 20 && meth == 11 && c->state == S_WAIT_CHAN) {
    if (c->role == R_PING) {
      printf("connected=1\n");
      exit(0);
    }
    c->state = S_READY;
    return;
  }
  if (cls == 20 && meth == 40) {
    close_reason(payload, size, why, (int)sizeof why);
    fprintf(stderr, "AMQP_CLOSE c%d %s\n", c->idx, why);
    fail_conn(c, why);
    return;
  }
  if (cls == 20 && meth == 20 && size >= 5) {
    int active = payload[4] & 1;
    unsigned char p[8];
    unsigned char frame[24];
    int off = 0;
    c->flow_paused = !active;
    p[0] = 0;
    p[1] = 20;
    p[2] = 0;
    p[3] = 21;
    p[4] = (unsigned char)active;
    if (frame_write(frame, (int)sizeof frame, &off, 1, 1, p, 5) < 0 || enqueue(c, frame, off) < 0) {
      fail_conn(c, "flow_full");
      return;
    }
    kick(c);
    return;
  }
  if (cls == 50 && meth == 11 && c->state == S_WAIT_DECLARE) {
    c->declares_done++;
    if (c->declares_done < g_queues) {
      c->state = S_WAIT_DECLARE;
      send_blob(c, declare_blob[c->declares_done].bytes, declare_blob[c->declares_done].len);
    } else {
      fprintf(stderr, "DECLARED %d\n", c->declares_done);
      c->state = S_READY;
      arm_clients();
    }
    return;
  }
  if (cls == 85 && meth == 11 && c->state == S_WAIT_CONFIRM) {
    c->state = S_RUN;
    return;
  }
  if (cls == 60 && meth == 11 && c->state == S_WAIT_QOS) {
    c->state = S_WAIT_CONSUME;
    send_blob(c, c->consume_frame, c->consume_len);
    return;
  }
  if (cls == 60 && meth == 21 && c->state == S_WAIT_CONSUME) {
    c->state = S_RUN;
    return;
  }
  if (cls == 60 && meth == 80 && size >= 13) {
    on_ack(c, rd64(payload + 4), payload[12] & 1, now, 0);
    return;
  }
  if (cls == 60 && meth == 120 && size >= 13) {
    on_ack(c, rd64(payload + 4), payload[12] & 1, now, 1);
    return;
  }
  if (cls == 60 && meth == 60) {
    const unsigned char *p = payload + 4;
    int left = size - 4;
    unsigned long long tag;
    if (skip_shortstr(&p, &left) < 0 || left < 9) {
      fail_conn(c, "bad_deliver");
      return;
    }
    tag = rd64(p);
    p += 8;
    left -= 8;
    p++; /* redelivered */
    left--;
    if (skip_shortstr(&p, &left) < 0 || skip_shortstr(&p, &left) < 0) {
      fail_conn(c, "bad_deliver");
      return;
    }
    c->deliver_tag = tag;
    c->recycling = 1;
    c->expect = EX_HEADER;
    return;
  }
  if (cls == 60 && meth == 50) {
    c->recycling = 2;
    c->expect = EX_HEADER;
    return;
  }
  log_unexpected(c, cls, meth);
}

static void on_frame(Conn *c, int type, const unsigned char *payload, int size, long long now) {
  if (type == 8) return;
  if (type == 1) {
    if (c->expect != EX_METHOD) {
      fail_conn(c, "method_during_body");
      return;
    }
    on_method(c, payload, size, now);
    return;
  }
  if (type == 2) {
    if (c->expect != EX_HEADER || size < 12) {
      fail_conn(c, "bad_header");
      return;
    }
    c->body_need = rd64(payload + 4);
    c->body_got = 0;
    if (c->body_need == 0) finish_content(c, now);
    else c->expect = EX_BODY;
    return;
  }
  if (type == 3) {
    if (c->expect != EX_BODY) {
      fail_conn(c, "bad_body");
      return;
    }
    if (c->body_got + (unsigned long long)size > c->body_need) {
      fail_conn(c, "body_over");
      return;
    }
    c->body_got += (unsigned long long)size;
    if (c->body_got == c->body_need) finish_content(c, now);
    return;
  }
  fail_conn(c, "bad_type");
}

static void rcompact(Conn *c) {
  int n;
  if (c->rpos <= 0) return;
  n = c->rlen - c->rpos;
  memmove(c->rbuf, c->rbuf + c->rpos, (size_t)n);
  c->rlen = n;
  c->rpos = 0;
}

static void parse_frames(Conn *c, long long now) {
  while (c->state != S_FAIL && c->rlen - c->rpos >= 7) {
    unsigned char *f = c->rbuf + c->rpos;
    int size = (int)rd32(f + 3);
    if (size < 0 || 8 + size > RCAP) {
      fail_conn(c, "frame_too_big");
      return;
    }
    if (c->rlen - c->rpos < 8 + size) break;
    if (f[7 + size] != 0xCE) {
      fail_conn(c, "bad_frame_end");
      return;
    }
    on_frame(c, f[0], f + 7, size, now);
    c->rpos += 8 + size;
  }
}

static void on_readable(Conn *c, long long now) {
  rcompact(c);
  for (;;) {
    int n = (int)read(c->fd, c->rbuf + c->rlen, (size_t)(RCAP - c->rlen));
    if (n > 0) {
      c->rlen += n;
      if (c->rlen >= RCAP) break;
      continue;
    }
    if (n == 0) {
      fail_conn(c, "eof");
      return;
    }
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
    fail_conn(c, errno == ECONNRESET ? "reset" : "read_err");
    return;
  }
  parse_frames(c, now);
  if (c->state == S_FAIL) return;
  if (c->rpos == 0 && c->rlen == RCAP) {
    fail_conn(c, "rbuf_full");
    return;
  }
  kick(c);
}

static void on_connected(Conn *c) {
  int err = 0;
  socklen_t len = sizeof err;
  unsigned char hdr[8] = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};
  if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err) {
    fail_conn(c, err == ECONNREFUSED ? "refused" : "connect_err");
    return;
  }
  if (enqueue(c, hdr, 8) < 0) {
    fail_conn(c, "wbuf_full");
    return;
  }
  c->state = S_WAIT_START;
  kick(c);
}

static void fill_pub(Conn *c, long long now) {
  int batch = 0;
  Blob *b;
  if (phase != PH_RUN || c->state != S_RUN || c->blocked || c->flow_paused) return;
  if (t_end && now >= t_end) return;
  if (flush_conn(c) < 0) return;
  if (c->woff < c->wlen) return;
  b = &publish_blob[c->qindex];
  while (c->inflight < g_window && batch < 32 && c->nfree > 0) {
    int s;
    if (b->len > wspace(c)) break;
    s = c->free_stack[--c->nfree];
    if (enqueue(c, b->bytes, b->len) < 0) {
      c->free_stack[c->nfree++] = s;
      break;
    }
    c->slot_used[s] = 1;
    c->slot_tag[s] = c->next_tag++;
    c->slot_us[s] = now;
    c->inflight++;
    if (t_mark && now >= t_mark && now < t_end) sent_n++;
    batch++;
  }
  kick(c);
}

static void dial(Conn *c, const struct sockaddr_in *addr) {
  int fd, one = 1, buf = 1 << 20, flags, rc;
  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) fail_conn(c, "socket");
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof buf);
  setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof buf);
  flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  c->fd = fd;
  rc = connect(fd, (const struct sockaddr *)addr, sizeof *addr);
  if (rc == 0) {
    on_connected(c);
    return;
  }
  if (errno == EINPROGRESS) {
    c->state = S_CONNECTING;
    set_interest(c);
    return;
  }
  fail_conn(c, "connect_err");
}

typedef struct {
  int type, off, size;
} Fr;

static int walk(const unsigned char *b, int n, Fr *fr, int max) {
  int o = 0, f = 0;
  while (o + 7 <= n) {
    int sz = (int)rd32(b + o + 3);
    if (sz < 0 || o + 8 + sz > n) return -1;
    if (b[o + 7 + sz] != 0xCE) return -1;
    if (f < max) {
      fr[f].type = b[o];
      fr[f].off = o + 7;
      fr[f].size = sz;
    }
    f++;
    o += 8 + sz;
  }
  if (o != n) return -1;
  return f;
}

static int selftest(void) {
  unsigned char declare[256], publish[1024], consume[160], ack[32], qos[64], confirm[32];
  Fr fr[4];
  int n, flags_at, name_at;
  const unsigned char *body;
  if (build_handshake() < 0) return 1;
  if (LEN_START_OK < 20 || LEN_TUNE_OPEN < 20 || LEN_CHAN < 10) return 1;
  if (!memmem(FRAME_START_OK, (size_t)LEN_START_OK, "PLAIN", 5)) return 1;
  if (!memmem(FRAME_START_OK, (size_t)LEN_START_OK, "devpassword12", 13)) return 1;
  n = build_declare(declare, (int)sizeof declare, "q0");
  if (walk(declare, n, fr, 4) != 1 || fr[0].type != 1) return 1;
  name_at = fr[0].off + 4 + 2;
  flags_at = name_at + 1 + declare[name_at];
  if (declare[flags_at] != 0x02) return 1;
  if (!memmem(declare, (size_t)n, "x-queue-type", 12) || !memmem(declare, (size_t)n, "classic", 7)) return 1;
  n = build_publish(publish, (int)sizeof publish, "q0", 256);
  if (walk(publish, n, fr, 4) != 3) return 1;
  if (fr[0].type != 1 || fr[1].type != 2 || fr[2].type != 3) return 1;
  if (rd64(publish + fr[1].off + 4) != 256) return 1;
  if (publish[fr[1].off + 12] != 0x10 || publish[fr[1].off + 13] != 0x00) return 1;
  if (publish[fr[1].off + fr[1].size - 1] != 2) return 1;
  if (fr[2].size != 256) return 1;
  body = publish + fr[2].off;
  if (body[0] != 'x' || body[255] != 'x') return 1;
  n = build_consume(consume, (int)sizeof consume, "q0", "c0");
  if (walk(consume, n, fr, 4) != 1) return 1;
  n = build_ack(ack, (int)sizeof ack, 12345);
  if (walk(ack, n, fr, 4) != 1 || rd64(ack + fr[0].off + 4) != 12345 || ack[fr[0].off + 12] != 0) return 1;
  n = build_qos(qos, (int)sizeof qos, 512);
  if (walk(qos, n, fr, 4) != 1 || qos[fr[0].off + 8] != 2 || qos[fr[0].off + 9] != 0) return 1;
  n = build_confirm(confirm, (int)sizeof confirm);
  if (walk(confirm, n, fr, 4) != 1 || confirm[fr[0].off] != 0 || confirm[fr[0].off + 1] != 85) return 1;
  printf("selftest=ok\n");
  return 0;
}

int main(int argc, char **argv) {
  struct sockaddr_in addr;
  int i, port;
  signal(SIGPIPE, SIG_IGN);
  setvbuf(stdout, NULL, _IONBF, 0);
  setvbuf(stderr, NULL, _IONBF, 0);
  if (argc >= 2 && strcmp(argv[1], "--selftest") == 0) return selftest();
  if (argc < 3) {
    fprintf(stderr, "usage: loadgen <ip> <port>\n");
    return 1;
  }
  ping_mode = getenv("PING") && getenv("PING")[0] == '1';
  g_pubs = env_int("PUBS", 16, 1, 64);
  g_cons = env_int("CONS", 8, 1, 64);
  g_queues = env_int("QUEUES", 1, 1, MAX_Q);
  g_window = env_int("WINDOW", 128, 1, 1024);
  g_prefetch = env_int("PREFETCH", 512, 1, 65535);
  g_body = env_int("BODY", 256, 1, 4096);
  warmup_us = (long long)env_int("WARMUP_MS", 2000, 0, 60000) * 1000LL;
  measure_us = (long long)env_int("MEASURE_MS", 8000, 200, 120000) * 1000LL;
  startup_us = ping_mode ? 1500LL * 1000LL : 20000LL * 1000LL;
  memset(&addr, 0, sizeof addr);
  addr.sin_family = AF_INET;
  port = atoi(argv[2]);
  if (port < 1 || port > 65535 || inet_pton(AF_INET, argv[1], &addr.sin_addr) != 1) {
    fprintf(stderr, "bad address\n");
    return 1;
  }
  addr.sin_port = htons((unsigned short)port);
  if (build_handshake() < 0) return 1;
  LEN_QOS = build_qos(FRAME_QOS, (int)sizeof FRAME_QOS, g_prefetch);
  LEN_CONFIRM = build_confirm(FRAME_CONFIRM, (int)sizeof FRAME_CONFIRM);
  if (LEN_QOS < 0 || LEN_CONFIRM < 0) return 1;
  if (ping_mode) {
    nconns = 1;
  } else {
    if (g_cons < g_queues && g_queues > 1) {
      fprintf(stderr, "need at least one consumer per queue\n");
      return 1;
    }
    nconns = 1 + g_pubs + g_cons;
    for (i = 0; i < g_queues; i++) {
      char name[16];
      snprintf(name, sizeof name, "q%d", i);
      declare_blob[i].bytes = xmalloc(256);
      declare_blob[i].len = build_declare(declare_blob[i].bytes, 256, name);
      publish_blob[i].bytes = xmalloc((size_t)(512 + g_body));
      publish_blob[i].len = build_publish(publish_blob[i].bytes, 512 + g_body, name, g_body);
      if (declare_blob[i].len < 0 || publish_blob[i].len < 0) return 1;
    }
  }
  conns = xmalloc((size_t)nconns * sizeof(Conn));
  epfd = epoll_create1(EPOLL_CLOEXEC);
  if (epfd < 0) return 1;
  for (i = 0; i < nconns; i++) {
    Conn *c = &conns[i];
    c->idx = i;
    c->fd = -1;
    c->expect = EX_METHOD;
    c->rbuf = xmalloc(RCAP);
    c->wbuf = xmalloc(WCAP);
    c->next_tag = 1;
    if (ping_mode) {
      c->role = R_PING;
    } else if (i == 0) {
      c->role = R_SETUP;
    } else if (i <= g_pubs) {
      int s;
      c->role = R_PUB;
      c->qindex = (i - 1) % g_queues;
      c->free_stack = xmalloc((size_t)g_window * sizeof(int));
      c->slot_tag = xmalloc((size_t)g_window * sizeof(unsigned long long));
      c->slot_us = xmalloc((size_t)g_window * sizeof(long long));
      c->slot_used = xmalloc((size_t)g_window);
      for (s = 0; s < g_window; s++) c->free_stack[c->nfree++] = s;
    } else {
      char qname[16], tag[16];
      c->role = R_CON;
      c->qindex = (i - 1 - g_pubs) % g_queues;
      snprintf(qname, sizeof qname, "q%d", c->qindex);
      snprintf(tag, sizeof tag, "c%d", i);
      c->consume_len = build_consume(c->consume_frame, (int)sizeof c->consume_frame, qname, tag);
      if (c->consume_len < 0) return 1;
    }
  }
  fprintf(stderr, "CONFIG ping=%d pubs=%d cons=%d queues=%d window=%d prefetch=%d body=%d warmup_us=%lld measure_us=%lld\n",
          ping_mode, g_pubs, g_cons, g_queues, g_window, g_prefetch, g_body, warmup_us, measure_us);
  t0 = now_us();
  for (i = 0; i < nconns; i++) dial(&conns[i], &addr);
  for (;;) {
    struct epoll_event evs[256];
    long long now = now_us();
    int n;
    if (ping_mode && now - t0 > startup_us) {
      printf("connected=0\n");
      return 1;
    }
    if (!ping_mode && phase != PH_RUN && now - t0 > startup_us) {
      int counts[12] = {0};
      for (i = 0; i < nconns; i++) {
        int st = conns[i].state;
        if (st >= 0 && st < 12) counts[st]++;
      }
      snprintf(first_err, sizeof first_err, "startup_ph%d_conn%d_start%d_tune%d_open%d_chan%d_ready%d_decl%d_conf%d_qos%d_consume%d",
               phase, counts[S_CONNECTING], counts[S_WAIT_START], counts[S_WAIT_TUNE], counts[S_WAIT_OPEN], counts[S_WAIT_CHAN],
               counts[S_READY], counts[S_WAIT_DECLARE], counts[S_WAIT_CONFIRM], counts[S_WAIT_QOS], counts[S_WAIT_CONSUME]);
      failed = 1;
      finish(0);
    }
    if (phase == PH_RUN && now >= t_end) finish(1);
    if (!ping_mode && phase == PH_WAIT_READY && all_ready()) begin_declare();
    check_run(now);
    if (phase == PH_RUN && !marked && now >= t_mark) emit_mark();
    if (phase == PH_RUN && marked && now > t_mark + 1000000 && consumed == 0 && sent_n > 100) {
      snprintf(first_err, sizeof first_err, "no_consume");
      failed = 1;
      finish(0);
    }
    if (phase == PH_RUN) {
      for (i = 0; i < nconns; i++)
        if (conns[i].role == R_PUB) fill_pub(&conns[i], now);
    }
    n = epoll_wait(epfd, evs, 256, phase == PH_RUN ? 0 : 20);
    if (n < 0) {
      if (errno == EINTR) continue;
      snprintf(first_err, sizeof first_err, "epoll");
      failed = 1;
      finish(0);
    }
    now = now_us();
    if (phase == PH_RUN && now >= t_end) finish(1);
    for (i = 0; i < n; i++) {
      Conn *c = &conns[evs[i].data.u32];
      uint32_t events = evs[i].events;
      if (c->state == S_FAIL || c->fd < 0) continue;
      if (c->state == S_CONNECTING && (events & (EPOLLOUT | EPOLLERR | EPOLLHUP))) on_connected(c);
      if (c->state == S_FAIL || c->fd < 0) continue;
      if (events & (EPOLLIN | EPOLLERR | EPOLLHUP)) on_readable(c, now);
      if (c->state == S_FAIL || c->fd < 0) continue;
      if (events & EPOLLOUT) {
        flush_conn(c);
        if (c->role == R_PUB) fill_pub(c, now);
        else set_interest(c);
      }
    }
  }
}

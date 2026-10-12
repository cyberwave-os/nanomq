// Stats snapshot and Mosquitto-compatible $SYS/broker/... publisher.
#include <inttypes.h>
#include <math.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "include/broker.h"
#include "include/mqtt_api.h"
#include "include/sys_stats.h"
#include "include/version.h"
#include "include/web_server.h"
#include "nng/mqtt/mqtt_client.h"
#include "nng/protocol/mqtt/mqtt_parser.h"
#include "nng/protocol/mqtt/nmq_mqtt.h"
#include "nng/protocol/reqrep0/req.h"
#include "nng/supplemental/nanolib/log.h"
#include "nng/supplemental/nanolib/mqtt_db.h"
#include "nng/supplemental/util/idhash.h"
#include "nng/supplemental/util/platform.h"

static void
count_pipe_cb(void *key, void *value, void *arg)
{
	nmq_stats *s       = arg;
	nng_pipe   pipe    = { .id = *(uint32_t *) key };
	(void) value;
	s->sessions++;
	if (!nng_pipe_status(pipe)) {
		s->connections++;
	}
}

static uint64_t
resident_bytes(void)
{
#if defined(__linux__)
	FILE *fp = fopen("/proc/self/statm", "r");
	if (fp == NULL) {
		return 0;
	}
	unsigned long size = 0, rss = 0;
	int           n    = fscanf(fp, "%lu %lu", &size, &rss);
	fclose(fp);
	return n == 2 ? (uint64_t) rss * (uint64_t) getpagesize() : 0;
#else
	return 0;
#endif
}

void
nmq_stats_snapshot(nmq_stats *s, nng_socket *broker_sock)
{
	memset(s, 0, sizeof(*s));
	nng_id_map *pipe_map = NULL;
	if (broker_sock != NULL &&
	    nng_socket_get_ptr(*broker_sock, NMQ_OPT_MQTT_PIPES,
	        (void **) &pipe_map) == 0 &&
	    pipe_map != NULL) {
		nng_id_map_foreach2(pipe_map, count_pipe_cb, s);
	}
	s->subscriptions = dbtree_subscription_count();
	s->retained      = dbtree_retain_count(get_broker_retain_db());
#ifdef STATISTICS
	s->msgs_in      = nanomq_get_message_in();
	s->msgs_out     = nanomq_get_message_out();
	s->msgs_dropped = nanomq_get_message_drop();
	s->bytes_in     = nanomq_get_bytes_in();
	s->bytes_out    = nanomq_get_bytes_out();
#endif
	s->heap_bytes = resident_bytes();
	s->uptime_s   = (uint64_t) ((nng_clock() - get_boot_time()) / 1000);
}

// ---- $SYS publisher -------------------------------------------------------

#define SYS_PREFIX "$SYS/broker/"

typedef struct {
	nng_socket *broker_sock;
	uint32_t    interval_s;
} sys_ctx;

// Per-minute rates averaged over 1, 5 and 15 minutes, like Mosquitto's
// $SYS/broker/load/*. An exponential moving average stands in for Mosquitto's
// sliding window.
static const struct {
	const char *suffix;
	double      window_s;
} load_windows[] = { { "1min", 60 }, { "5min", 300 }, { "15min", 900 } };
#define N_WINDOWS 3

typedef struct {
	const char *topic;  // under $SYS/broker/load/
	size_t      offset; // of the source counter in nmq_stats
	uint64_t    prev;
	double      ema[N_WINDOWS];
	bool        primed;
} load_metric;

static int
send_sys(nng_socket req, const char *topic, const char *value)
{
	nng_msg *pub = NULL;
	if (nng_mqtt_msg_alloc(&pub, 0) != 0) {
		return -1;
	}
	nng_mqtt_msg_set_packet_type(pub, NNG_MQTT_PUBLISH);
	nng_mqtt_msg_set_publish_qos(pub, 0);
	nng_mqtt_msg_set_publish_retain(pub, true);
	nng_mqtt_msg_set_publish_topic(pub, topic);
	nng_mqtt_msg_set_publish_payload(
	    pub, (uint8_t *) value, strlen(value));

	nng_msg *msg = NULL;
	if (encode_common_mqtt_msg(
	        &msg, pub, NMQ_SYS_CLIENTID, MQTT_PROTOCOL_VERSION_v311) != 0) {
		nng_msg_free(pub);
		return -1;
	}
	if (nng_sendmsg(req, msg, 0) != 0) {
		nng_msg_free(msg);
		return -1;
	}
	// The broker answers every request; drain it so the REQ socket does not
	// accumulate state.
	nng_msg *reply = NULL;
	if (nng_recvmsg(req, &reply, 0) == 0) {
		nng_msg_free(reply);
	}
	return 0;
}

static void
pub_fmt(nng_socket req, const char *name, const char *fmt, ...)
{
	char    topic[128], val[64];
	va_list ap;
	snprintf(topic, sizeof(topic), SYS_PREFIX "%s", name);
	va_start(ap, fmt);
	vsnprintf(val, sizeof(val), fmt, ap);
	va_end(ap);
	send_sys(req, topic, val);
}

static void
update_and_pub_load(nng_socket req, load_metric *m, uint64_t now, double dt_s)
{
	double rate_per_min =
	    dt_s > 0 ? (double) (now - m->prev) * 60.0 / dt_s : 0.0;
	m->prev = now;
	for (int i = 0; i < N_WINDOWS; i++) {
		if (!m->primed) {
			m->ema[i] = rate_per_min;
		} else {
			double a  = 1.0 - exp(-dt_s / load_windows[i].window_s);
			m->ema[i] = m->ema[i] + a * (rate_per_min - m->ema[i]);
		}
		char name[96];
		snprintf(name, sizeof(name), "load/%s/%s", m->topic,
		    load_windows[i].suffix);
		pub_fmt(req, name, "%.2f", m->ema[i]);
	}
	m->primed = true;
}

static void
sys_stats_thread(void *arg)
{
	sys_ctx   *ctx = arg;
	nng_socket req;
	if (nng_req0_open(&req) != 0 ||
	    nng_dial(req, INPROC_SERVER_URL, NULL, 0) != 0) {
		log_error("sys_topic: cannot reach the internal publish path");
		return;
	}
	nng_socket_set_ms(req, NNG_OPT_RECVTIMEO, 2000);
	nng_socket_set_ms(req, NNG_OPT_SENDTIMEO, 2000);

	char version[64];
	snprintf(version, sizeof(version), "NanoMQ version %d.%d.%d-%s",
	    NANO_VER_MAJOR, NANO_VER_MINOR, NANO_VER_PATCH,
	    NANO_VER_ID_SHORT);

	load_metric loads[] = {
		{ .topic = "messages/received", .offset = offsetof(nmq_stats, msgs_in) },
		{ .topic = "messages/sent", .offset = offsetof(nmq_stats, msgs_out) },
		{ .topic = "publish/received", .offset = offsetof(nmq_stats, msgs_in) },
		{ .topic = "publish/sent", .offset = offsetof(nmq_stats, msgs_out) },
		{ .topic = "publish/dropped", .offset = offsetof(nmq_stats, msgs_dropped) },
		{ .topic = "bytes/received", .offset = offsetof(nmq_stats, bytes_in) },
		{ .topic = "bytes/sent", .offset = offsetof(nmq_stats, bytes_out) },
	};
	// Plain gauges and counters; aliases are separate rows over one field.
	static const struct {
		const char *topic;
		size_t      offset;
	} plain[] = {
		{ "clients/connected", offsetof(nmq_stats, connections) },
		{ "clients/total", offsetof(nmq_stats, sessions) },
		{ "messages/received", offsetof(nmq_stats, msgs_in) },
		{ "messages/sent", offsetof(nmq_stats, msgs_out) },
		{ "publish/messages/received", offsetof(nmq_stats, msgs_in) },
		{ "publish/messages/sent", offsetof(nmq_stats, msgs_out) },
		{ "publish/messages/dropped", offsetof(nmq_stats, msgs_dropped) },
		{ "bytes/received", offsetof(nmq_stats, bytes_in) },
		{ "bytes/sent", offsetof(nmq_stats, bytes_out) },
		{ "publish/bytes/received", offsetof(nmq_stats, bytes_in) },
		{ "publish/bytes/sent", offsetof(nmq_stats, bytes_out) },
		{ "subscriptions/count", offsetof(nmq_stats, subscriptions) },
		{ "retained messages/count", offsetof(nmq_stats, retained) },
		{ "heap/current", offsetof(nmq_stats, heap_bytes) },
	};
	uint64_t max_clients = 0, max_heap = 0;
	nng_time last        = nng_clock();

	for (;;) {
		nng_msleep((nng_duration) ctx->interval_s * 1000);
		nmq_stats s;
		nmq_stats_snapshot(&s, ctx->broker_sock);
		nng_time now  = nng_clock();
		double   dt_s = (double) (now - last) / 1000.0;
		last          = now;
		max_clients   = s.connections > max_clients ? s.connections : max_clients;
		max_heap      = s.heap_bytes > max_heap ? s.heap_bytes : max_heap;

		pub_fmt(req, "version", "%s", version);
		pub_fmt(req, "uptime", "%" PRIu64 " seconds", s.uptime_s);
		pub_fmt(req, "clients/maximum", "%" PRIu64, max_clients);
		pub_fmt(req, "heap/maximum", "%" PRIu64, max_heap);
		for (size_t i = 0; i < sizeof(plain) / sizeof(plain[0]); i++) {
			pub_fmt(req, plain[i].topic, "%" PRIu64,
			    *(uint64_t *) ((char *) &s + plain[i].offset));
		}
		for (size_t i = 0; i < sizeof(loads) / sizeof(loads[0]); i++) {
			update_and_pub_load(req, &loads[i],
			    *(uint64_t *) ((char *) &s + loads[i].offset), dt_s);
		}
	}
}

int
nmq_sys_stats_start(conf *config, nng_socket *broker_sock)
{
	if (!config->sys_topic.enable) {
		return 0;
	}
	if (!config->http_server.enable) {
		log_error("sys_topic needs http_server enabled (internal publish "
		          "path); $SYS statistics are off");
		return -1;
	}
	static sys_ctx ctx;
	ctx.broker_sock = broker_sock;
	ctx.interval_s  = config->sys_topic.interval;
	nng_thread *thr = NULL;
	if (nng_thread_create(&thr, sys_stats_thread, &ctx) != 0) {
		log_error("sys_topic: thread start failed");
		return -1;
	}
	log_warn("$SYS/broker statistics publishing every %us",
	    (unsigned) ctx.interval_s);
	return 0;
}

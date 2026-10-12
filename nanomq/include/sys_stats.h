#ifndef NANOMQ_SYS_STATS_H
#define NANOMQ_SYS_STATS_H

#include <stdint.h>
#include "nng/nng.h"
#include "nng/supplemental/nanolib/conf.h"

// One consistent read of every broker statistic. Both the REST/Prometheus
// output and the $SYS publisher format from this struct, so a new metric is a
// field here, a counter behind it, and one line in each output.
typedef struct {
	uint64_t connections;   // pipes currently connected
	uint64_t sessions;      // pipes known to the broker (connected or not)
	uint64_t subscriptions; // (topic filter, pipe) pairs
	uint64_t retained;      // retained messages stored
	uint64_t msgs_in;       // PUBLISH packets received, incl. internal events
	uint64_t msgs_out;      // PUBLISH deliveries, one per subscriber
	uint64_t msgs_dropped;  // publishes that matched no subscriber
	uint64_t bytes_in;      // publish payload bytes received from clients
	uint64_t bytes_out;     // publish payload bytes delivered
	uint64_t heap_bytes;    // process resident memory
	uint64_t uptime_s;
} nmq_stats;

// Fill s. broker_sock may be NULL, in which case connections/sessions are 0.
void nmq_stats_snapshot(nmq_stats *s, nng_socket *broker_sock);

// Start the periodic $SYS/broker/... publisher (needs http_server enabled: it
// injects messages through the internal HTTP-server path). Returns 0 on
// success. The socket must outlive the broker, like http_server.broker_sock.
int nmq_sys_stats_start(conf *config, nng_socket *broker_sock);

// Client id carried by the injected publishes. handle_pub() trusts publishes
// with this id only when they arrive on the internal HTTP-server path.
#define NMQ_SYS_CLIENTID "$nmq-sys-stats"

#endif

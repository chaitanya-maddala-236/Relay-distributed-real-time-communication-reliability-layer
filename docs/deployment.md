# Deployment

## Compose stack

```bash
make up
```

Brings up Nginx, two gateway nodes, Redis, Postgres, Prometheus, and
Grafana. Two nodes by default so distributed behavior is exercised
locally rather than only in production.

> This stack has not been executed in the environment Relay was developed
> in (no Docker daemon available). The local multi-process workflow in the
> README is the path that has been run end to end.

| Service | Port | Purpose |
| --- | --- | --- |
| nginx | 8080 | WebSocket + HTTP entry point, serves the console |
| control-plane | 8001 | Admin API |
| relay-node-1/2 | internal | Data plane |
| redis | 6379 | Pub/Sub, streams, sessions, sequences |
| postgres | 5432 | Control-plane config |
| prometheus | 9090 | Metrics |
| grafana | 3000 | Dashboards |

## Load balancer requirements

WebSockets are long-lived, and the defaults of a request/response proxy
are wrong for them:

1. **Upgrade handling** — `proxy_http_version 1.1` plus `Upgrade` and
   `Connection` headers.
2. **Read timeout above the heartbeat interval** — otherwise an idle but
   healthy connection is cut by the proxy. Relay pings every 15 s; the
   Nginx config uses 300 s.
3. **Buffering off** — buffered streaming defeats the point.
4. **Health checks against `/health/live`, routing against
   `/health/ready`** — a node with a broken Redis should leave the pool
   without being killed.
5. **Draining support** — a draining node fails readiness and asks its
   clients to reconnect.

**Sticky sessions are not required and are not the reliability
mechanism.** Session state lives in Redis, so a client may reconnect to
any node and resume there. `least_conn` is used because long-lived
connections accumulate unevenly under round-robin.

## Scaling

Add gateway nodes. Each holds its own WebSockets; Redis coordinates
everything shared. There is no leader, no node-to-node communication, and
no shard assignment to rebalance.

What scales with node count: connections, fanout work, socket writes.
What does not: Redis — it is the shared dependency and the first thing to
watch. Per publish, Relay performs INCR, XADD, XTRIM, and PUBLISH.

## Graceful deploys

On SIGTERM a node marks itself draining, fails readiness, sends
`DISCONNECT(NODE_DRAINING)` to its clients, closes sockets with 4503,
stops its Pub/Sub listeners, and deletes its node key. Clients reconnect
with jitter onto remaining nodes and resume. Sessions survive, so
messages published during the changeover replay on reconnect.

Roll one node at a time and let clients settle between steps — draining
every node at once produces exactly the reconnect storm the jitter is
meant to avoid.

## Configuration

Every setting is an environment variable prefixed `RELAY_` (see
`packages/common/config.py`). The ones most worth reviewing before
production:

```bash
RELAY_NODE_ID=relay-node-1                 # must be unique per process
RELAY_REDIS_URL=redis://redis:6379/0
RELAY_DATABASE_URL=postgresql+asyncpg://...
RELAY_SESSION_RESUME_WINDOW_SECONDS=300
RELAY_MESSAGE_RETENTION_SECONDS=300
RELAY_MAX_INFLIGHT_MESSAGES=100
RELAY_CONNECTION_QUEUE_MAX_MESSAGES=1000
RELAY_HEARTBEAT_INTERVAL_SECONDS=15
```

Run Redis with persistence enabled (`appendonly yes`). Without it, a
Redis restart resets channel sequence counters — measured and documented
in [failure-model.md](failure-model.md).

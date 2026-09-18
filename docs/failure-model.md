# Failure model

For every failure: what breaks, what continues, which guarantee survives,
which is temporarily lost, and how recovery happens.

Entries marked **measured** were observed by running the failure, not
reasoned about. The Redis outage row comes from
`tests/chaos/redis_outage_observe.py`.

---

## Client disconnect (clean or abrupt)

| | |
| --- | --- |
| Breaks | The connection. Its `connection_id` is gone permanently |
| Continues | The session, for `session_resume_window_seconds` (default 300s). Other clients unaffected |
| Survives | Ack position, subscriptions (restored on RESUME), retained messages |
| Lost | Nothing, if the client returns within the window |
| Recovery | Reconnect → `CONNECT` with `session_id` → `RESUME` from last ack → replay |

Cleanup on disconnect: stop writes, cancel sender and heartbeat tasks,
release subscriptions, tear down the node's Pub/Sub listener if this was
the last local subscriber, remove from the registry. The session is
deliberately **not** deleted.

## Heartbeat timeout

| | |
| --- | --- |
| Breaks | Connection marked DEAD, socket closed with 4408 |
| Continues | Session remains resumable |
| Recovery | Identical to client disconnect |

A connection that stops responding to PING is removed within
`heartbeat_interval + heartbeat_timeout` (default ~25s). No connection
stays registered indefinitely.

## Slow consumer

| | |
| --- | --- |
| Breaks | The offending connection only, closed 4409 with `SLOW_CONSUMER` |
| Continues | Every other subscriber, at full rate — **measured**: 400/400 in order, p95 6.3 ms, while a co-subscribed client was being disconnected |
| Survives | The slow client's session and ack position |
| Recovery | Reconnect and RESUME. If it fell behind further than retention, `RESUME_GAP` |

## Gateway node crash

| | |
| --- | --- |
| Breaks | Every connection on that node, instantly and without cleanup |
| Continues | Other nodes. Redis state is untouched |
| Survives | Sessions, ack positions, sequence counters, recovery buffers — all in Redis |
| Lost | The node's local registry (rebuilt implicitly as clients reconnect) |
| Recovery | Clients back off with jitter, reconnect through the load balancer to **any** node, and resume there |

The dead node's `relay:nodes:{id}` key expires after `node_ttl_seconds`
(15s), so it disappears from `/admin/nodes` without explicit
deregistration — which is what an unclean crash requires.

Cross-node session portability is verified in
`tests/distributed/multinode_verify.py`: a session created on node A is
resumed on node B and the missed messages replay correctly.

## Redis outage — **measured**

Observed by stopping Redis under a running gateway:

| Behavior | Observed |
| --- | --- |
| `GET /health/live` | **200** — the process is healthy; killing it would help nobody |
| `GET /health/ready` | **503** — the node is pulled from the load balancer |
| `POST /v1/publish` | **503** `REDIS_UNAVAILABLE` — explicit failure, not a silent accept |
| Existing WebSockets | **stay open** |
| New connections | **rejected** (session creation requires Redis) |
| After Redis returns | readiness **200**, delivery **resumes automatically** |

What is lost during the outage: live fanout, resume state, new sessions,
sequence allocation. What survives: established connections, and the
process itself.

Two implementation details this outage exposed, both fixed:

1. **Pub/Sub subscriptions do not survive a Redis restart.** A plain
   `async for` over `pubsub.listen()` simply ends, leaving the node
   subscribed to nothing — delivery never recovered. Listeners now run a
   supervised reconnect loop with capped backoff (observed: retry at
   0.5 s → 1 s → 2 s, then `pubsub_subscribed`).
2. **Pooled connections are dead but look reusable after a restart.** The
   Redis client now uses health checks and a retry policy; without them
   readiness stayed 503 for seconds after Redis was healthy.

**Sequence continuity depends on Redis persistence.** Measured: with
Redis restarted without persistence, channel sequence counters reset and
a post-restart publish was assigned sequence 2 where the pre-outage
message was sequence 1. A client would see this as a sequence regression.
`docker-compose.yml` therefore runs Redis with `appendonly yes`. Running
Relay against a non-persistent Redis means accepting that a Redis restart
invalidates in-flight ordering and resume state for every channel.

## Redis latency (not yet measured)

Expected: publish latency rises, fanout lags, heartbeats are unaffected
(local timers). Not yet exercised with an injected-latency proxy; this
row will be replaced with measurements when it is.

## Postgres outage

| | |
| --- | --- |
| Breaks | New authentication (the key lookup), all control-plane writes |
| Continues | Established connections, message delivery, fanout, resume — Postgres is not on the message path |
| Recovery | Automatic when Postgres returns |

Not yet measured. Caching authentication results would let new
connections survive a Postgres outage; it is not implemented, so new
connections currently fail.

## Network partition (gateway ↔ Redis)

Equivalent to the Redis outage from the isolated node's perspective. The
partitioned node keeps its existing sockets, fails readiness, and stops
delivering cross-node traffic. Nodes on the other side of the partition
continue among themselves.

Relay does not attempt to arbitrate a partition. There is no consensus
protocol and no leader election. Distributed state is Redis's state, TTLs
bound how long stale local views persist, and the system is eventually
consistent when connectivity returns.

## Retention expiry

| | |
| --- | --- |
| Breaks | Resume from a position older than the buffer retains |
| Response | `ERROR(RESUME_GAP)` for that channel, listed in `RESUMED.gaps` |
| Client obligation | Full resynchronization from the application's source of truth |

Relay does not return a partial replay that looks complete. This is the
distinction the gap detection exists to preserve — verified in
`tests/reliability/test_resume.py`.

Note that Redis' approximate stream trimming keeps roughly a macro-node
(~100 entries) beyond a small configured bound. Relay uses **exact**
trimming so the retained set matches the configured bound and gap
detection is truthful.

## Session expiry

After `session_resume_window_seconds` with no activity, the session key
expires. A reconnect with that `session_id` is treated as a new session:
the client gets a fresh `session_id` and must resynchronize.

## Node draining (planned deploy)

```text
SIGTERM -> status=draining -> readiness 503 -> DISCONNECT(NODE_DRAINING)
        -> sockets closed 4503 -> listeners stopped -> node key deleted
```

Clients reconnect with jitter and land on a remaining node. Sessions
survive, so resume replays anything published during the gap.

## Publisher failure

A publisher that sees a 5xx or timeout should retry with the same
`Idempotency-Key`. A retried publish returns the original `message_id`
and `sequence` rather than creating a second logical message — verified
in `tests/integration/e2e_verify.py`.

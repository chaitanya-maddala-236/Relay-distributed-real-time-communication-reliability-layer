# Performance

**No numbers in this document are estimated, extrapolated, or invented.**
Everything below was produced by running the referenced test. Where a
figure has not been measured, the row says so rather than guessing.

## Environment

These measurements come from a development container, not a benchmarking
host:

| | |
| --- | --- |
| Host | Shared Linux container (Ubuntu 24.04), CPU/memory not isolated |
| Python | 3.12.3 |
| Gateway | uvicorn, single worker per node |
| Redis | 7.x, local, no persistence for these runs |
| Postgres | 16, local |
| Topology | Client, gateway, Redis, and Postgres all on the same host (no network hop) |

**These numbers are directional only.** Co-locating everything removes
real network latency, and a shared container gives no CPU isolation.
Treat them as a floor for correctness verification, not as a capacity
claim.

## Measured: delivery latency under a burst

`tests/reliability/slow_consumer_verify.py` — 400 messages of ~8 KB
published sequentially over HTTP, two subscribers on one channel, one of
which stops acknowledging.

| Metric | Value |
| --- | --- |
| Messages delivered to healthy client | 400 / 400 |
| Order | preserved (monotonic sequence) |
| Delivery latency p50 | 5.2 ms |
| Delivery latency p95 | 6.3 ms |
| Delivery latency max | 26.8 ms |

Latency is measured from publish-call time to client receipt, so it
includes the HTTP publish round trip, Redis Stream append, Redis Pub/Sub
hop, queueing, and the WebSocket write.

## Measured: correctness under concurrency

| Test | Result |
| --- | --- |
| 200 concurrent sequence allocations on one channel | 200 unique, contiguous 1..200, zero collisions |
| 25-message ordered burst, single channel | sequences strictly increasing, payload order preserved |
| 10 messages alternating across 2 nodes | one monotonic sequence space; both nodes delivered identical ordering |
| Stream trimming at `max=20` after 500 publishes | bounded (exact trimming) |

## Measured: failure behavior

| Scenario | Result |
| --- | --- |
| Redis stopped | liveness 200, readiness 503, publish 503, existing sockets stay open |
| Redis restarted | readiness 200, delivery resumes automatically; Pub/Sub reconnect observed at 0.5 s → 1 s → 2 s |
| Non-acking client under load | disconnected at exactly the configured queue bound (depth 20, 164 834 bytes) |
| Healthy client during that disconnect | unaffected (see latency table above) |

## Not yet measured

These require a dedicated host and are deliberately left blank rather
than filled with plausible-looking numbers:

- Maximum concurrent connections per node
- Connection establishment rate (handshakes/sec)
- Sustained messages/sec at 1 k / 5 k / 10 k connections
- Fanout latency at 1 000+ subscribers on one channel
- Reconnect-storm recovery time for 5 000 simultaneous clients
- Memory per connection
- Redis operations/sec at load
- Behavior under injected Redis latency

## Known bottlenecks (structural, not yet quantified)

1. **Redis Pub/Sub fanout is per-channel, per-node.** Every node with a
   subscriber receives every message on that channel. Fine at moderate
   scale; a very large number of channels per node means a large number
   of listener tasks.
2. **Every publish costs at least three Redis round trips** (INCR, XADD,
   XTRIM, PUBLISH). Pipelining these is the obvious first optimization.
3. **Exact stream trimming costs more than approximate.** This was a
   deliberate correctness trade (see failure-model.md); it should be
   measured before scaling channel counts.
4. **One uvicorn worker per node.** Python's GIL bounds a single node's
   throughput; horizontal scaling is the intended answer, and the
   multi-node path is tested.

## Next optimization

Pipeline the publish path's Redis calls into a single round trip and
measure the difference. Do this before any throughput claim is made.

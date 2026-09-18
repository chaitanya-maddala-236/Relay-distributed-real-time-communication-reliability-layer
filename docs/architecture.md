# Architecture

## Layers

```text
Connection Layer     sockets, auth, heartbeat, lifecycle, cleanup
        |
Session Layer        session ids, resume positions, subscriptions
        |
Messaging Layer      envelope, sequence assignment, publish, fanout
        |
Reliability Layer    acks, flow control, replay, gap detection, dedup
        |
Coordination Layer   Redis: Pub/Sub, streams, node registry, presence
```

## State placement

The central decision is what lives where.

| State | Where | Why |
| --- | --- | --- |
| WebSocket object | Process memory only | A live socket cannot be serialized; it belongs to one process by definition |
| Outbound queue, in-flight window | Process memory | Per-connection, meaningless outside the owning process |
| Session, ack positions | Redis (TTL) | Must outlive a connection and be readable from any node |
| Sequence counters | Redis | Must be atomic across all publishers on all nodes |
| Recovery buffer | Redis Streams (trimmed) | Must be readable by whichever node a client resumes on |
| Node registry | Redis (TTL) | A crashed node must disappear without cooperating |
| Tenants, keys, channel config | Postgres | Durable configuration, changes rarely, read at connect time |

Corollary: a gateway process holds nothing irreplaceable. Killing one
loses connections, not data.

## Publish path

```text
POST /v1/publish  (or a client MESSAGE)
   |
   +-- authenticate, authorize channel against tenant namespace
   +-- size check
   +-- idempotency lookup (if Idempotency-Key present)
   |
   v
INCR relay:sequence:{channel}          atomic sequence assignment
   |
   v
XADD relay:messages:{channel}          bounded recovery buffer (exact MAXLEN)
   |
   v
PUBLISH relay:pubsub:{channel}         live fanout to every node
   |
   +---------------------+---------------------+
   v                     v                     v
 Node A listener      Node B listener      Node C listener
   |                     |                     |
   v                     v                     v
 per-connection queues -> sender tasks -> WebSocket writes
```

Sequence assignment happens once, before fanout, which is why every node
delivers the same order.

Each node runs at most one Pub/Sub listener per channel it has local
subscribers for, regardless of how many local connections share it.

## Resume path

```text
CONNECT(session_id) -> session found in Redis? -> reuse : create new
RESUME(last_ack_by_channel)
   |
   +-- XRANGE relay:messages:{channel}
   +-- oldest retained > last_ack + 1 ?  -> ERROR(RESUME_GAP)
   +-- else replay everything > last_ack, re-attach subscription
   v
RESUMED
```

## Concurrency model

One asyncio task per connection for receiving, one for sending, one for
heartbeat; one task per channel per node for Pub/Sub. Fanout does not
spawn a task per recipient — it enqueues, which is what preserves
ordering and bounds task count under large fanout.

All long-lived tasks are tracked and cancelled on shutdown. Listener
tasks are supervised and restarted if they die (learned the hard way; see
failure-model.md).

## Process topology

```text
control_plane  (FastAPI)   admin API, reads/writes Postgres
gateway        (FastAPI)   WebSockets, publish API, metrics — N processes
```

They share the database and Redis but have no direct dependency on each
other. The control plane being down does not stop message delivery.

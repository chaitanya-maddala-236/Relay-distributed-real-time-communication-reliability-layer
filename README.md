# Relay

**Distributed real-time communication reliability layer.**

A horizontally scalable WebSocket reliability layer providing session
resumption, at-least-once delivery, channel-level ordering,
acknowledgements, bounded backpressure, distributed fanout, and failure
recovery.

Relay is not a chat application. It is the infrastructure underneath
real-time applications: the part that deals with connections dying,
messages arriving twice, messages arriving out of order, one slow client
degrading everyone else, and server instances disappearing mid-flight.

---

## Documented guarantees

The point of this table is to be precise about what Relay does *not*
promise. Anything not listed here is not guaranteed.

| Capability | Guarantee |
| --- | --- |
| Delivery | At-least-once in reliable mode |
| Ordering | Per channel, for a connected subscriber |
| Duplicate prevention | Message IDs + bounded client-side dedup window |
| Resume | Within the retention window only |
| Global ordering | **Not guaranteed** |
| Exactly-once | **Not guaranteed** |
| Presence | Eventually consistent |
| Fanout | Best effort within configured limits |
| Offline recovery | Only messages still retained |
| Message durability | Relay is not an archive — buffers are short-lived and bounded |

Duplicates are expected under retry. Applications whose processing is not
idempotent must deduplicate on `message_id`; the reference client does
this within a bounded window, but the window is bounded on purpose, so it
is a convenience and not a guarantee. See
[docs/delivery-semantics.md](docs/delivery-semantics.md).

---

## Architecture

```text
                         Internet
                            |
                     +-------------+
                     |    Nginx    |     WebSocket upgrade, no sticky sessions
                     +-------------+
                            |
             +--------------+--------------+
             v              v              v
       Relay Node 1   Relay Node 2   Relay Node N     data plane (WebSockets)
             |              |              |
             +--------------+--------------+
                            |
                       +----------+
                       |  Redis   |  Pub/Sub (live fanout)
                       |          |  Streams (recovery buffer)
                       |          |  sequence counters, sessions, presence
                       +----------+
                            |
                       +----------+
                       | Postgres |  control plane only: tenants, keys,
                       +----------+  channel config, rooms
```

Each WebSocket lives on exactly one node. Everything that must outlive a
single connection — sessions, ack positions, sequence counters, the
recovery buffer — lives in Redis, which is why a client can reconnect to
a *different* node and resume (verified in
`tests/distributed/multinode_verify.py`).

Postgres is deliberately kept off the message hot path. It is read at
connection time for authentication and otherwise not touched per message.

---

## Quick start

### Docker (full distributed stack)

```bash
make up          # nginx + 2 relay nodes + redis + postgres + prometheus + grafana
make seed        # prints a tenant id, API key, and a ready-to-use channel name
open http://localhost:8080          # reliability console
```

> Note: the Compose stack is written but has not been executed in the
> environment this was developed in (no Docker daemon available). The
> local workflow below is the one that has actually been run end to end.

### Local (no Docker)

```bash
make install
redis-server --daemonize yes
make db

# terminal 1
PYTHONPATH=. .venv/bin/uvicorn apps.control_plane.main:app --port 8001
# terminal 2
PYTHONPATH=. RELAY_NODE_ID=relay-node-a .venv/bin/uvicorn apps.gateway.main:app --port 8000
# terminal 3 (second node, for distributed behavior)
PYTHONPATH=. RELAY_NODE_ID=relay-node-b .venv/bin/uvicorn apps.gateway.main:app --port 8010

make seed
```

Then open `frontend/index.html` and paste in the API key and channel.

---

## Protocol at a glance

```text
client                           server
  |-- CONNECT(token, session?) ---->|   authenticate, create or resume session
  |<------------- CONNECTED --------|   connection_id, session_id, node_id
  |-- SUBSCRIBE(channel) ---------->|   authorize against tenant namespace
  |<------------ SUBSCRIBED --------|
  |<------------- MESSAGE ----------|   message_id, sequence, payload
  |-- ACK(channel, sequence) ------>|   cumulative; also reopens the flow window
  |<---------------- PING ----------|   heartbeat
  |-- PONG ------------------------>|
  |            (connection dies)    |
  |-- CONNECT(token, session_id) -->|   same session, new connection_id
  |-- RESUME(last_ack_by_channel) ->|
  |<------------- MESSAGE ----------|   replay of everything after last ack
  |<-------------- RESUMED ---------|   or ERROR(RESUME_GAP) if unrecoverable
```

Full message reference: [docs/protocol.md](docs/protocol.md).

---

## Running the tests

```bash
make test-unit            # 27 tests, no services needed
make test-reliability     # 12 tests, needs Redis
make test-integration     # 31 end-to-end protocol checks, needs gateway + control plane
make test-distributed     # 15 cross-node checks, needs two gateways
make test-slow-consumer   # backpressure, run gateway with small bounds (see file header)
make test-chaos           # stops and restarts Redis, reports observed behavior
```

The integration, distributed, and chaos suites drive real WebSockets
against real processes. Nothing about delivery, resume, or failure
handling is simulated.

---

## Documentation

| Document | Contents |
| --- | --- |
| [architecture.md](docs/architecture.md) | Layers, state placement, request paths |
| [protocol.md](docs/protocol.md) | Every frame, its schema and error cases |
| [delivery-semantics.md](docs/delivery-semantics.md) | Why at-least-once, what duplicates mean |
| [ordering.md](docs/ordering.md) | Ordering scope and what is not ordered |
| [session-resumption.md](docs/session-resumption.md) | Sessions, resume, gaps, retention |
| [backpressure.md](docs/backpressure.md) | Flow control design and why it is ACK-based |
| [presence.md](docs/presence.md) | Presence model, TTLs, multi-connection handling |
| [failure-model.md](docs/failure-model.md) | Per-failure: what breaks, what survives, what recovers |
| [security.md](docs/security.md) | Auth, authorization, tenant isolation, limits |
| [performance.md](docs/performance.md) | Measured numbers and their limitations |
| [deployment.md](docs/deployment.md) | Compose stack, scaling, load balancer requirements |
| [adr/](docs/adr/) | Architecture decision records |

---

## Project status

Implemented and tested: connection lifecycle, heartbeats, authentication,
authorization, tenant isolation, subscriptions, sequencing, publish API,
idempotency, ACKs, flow control, slow-consumer handling, multi-node
fanout, node registration, session resume with gap detection, presence,
graceful draining, health probes, Prometheus metrics, structured logging,
and a reference TypeScript client.

Not yet implemented: ACK-timeout retry, per-operation rate
limits, OpenTelemetry spans, Grafana dashboard definitions, and
large-scale load/reconnect-storm testing. `docs/performance.md` reports
only what has actually been measured.
"# Relay-distributed-real-time-communication-reliability-layer" 

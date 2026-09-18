# ADR-010: Local connection state, distributed metadata

## Context
Some state belongs to one process; some must be visible to all.

## Decision
Keep live sockets, outbound queues, and in-flight windows in process
memory. Keep sessions, ack positions, sequence counters, recovery
buffers, and node registration in Redis. Never serialize a WebSocket.

## Alternatives
- **All state in Redis** — impossible for a live socket, and a Redis
  round trip per frame would be ruinous.
- **All state local** — no cross-node fanout and no session portability.

## Tradeoffs
Two sources of truth to reason about, and a local view that can be
briefly stale. Accepted: TTLs bound staleness, and no local decision
requires perfectly synchronized global state.

## Consequences
A gateway process holds nothing irreplaceable; losing one loses
connections, not data.

# ADR-008: Server-side sequence allocation via Redis INCR

## Context
Ordering and gap detection require a monotonic sequence per channel,
correct across multiple gateway processes and concurrent publishers.

## Decision
Allocate sequences with `INCR relay:sequence:{channel}` before fanout.

## Alternatives
- **Process-local counters** — N processes produce N sequence spaces for
  one channel. Wrong.
- **Client-supplied sequences** — clients are untrusted and cannot
  coordinate with each other.
- **Timestamps** — not unique, not monotonic under clock skew.
- **Postgres sequence** — correct, but a database round trip per message.

## Tradeoffs
One Redis round trip per publish, and a hard dependency on Redis for
publishing. Verified collision-free under 200 concurrent allocations.

## Consequences
Sequence continuity depends on Redis persistence. Measured: a Redis
restart without persistence resets counters, which a client sees as a
sequence regression. Compose enables `appendonly`.

# ADR-005: Redis Streams as the recovery buffer

## Context
Session resumption needs to replay messages a client missed while
disconnected. Pub/Sub cannot do this — it has no history.

## Decision
Append every published message to a per-channel Redis Stream, trimmed by
both count and age, and replay from it on RESUME.

## Alternatives
- **Postgres table** — durable, but puts a database write on every
  message's hot path.
- **In-process buffer** — lost on crash and invisible to other nodes,
  defeating cross-node resume.
- **No recovery buffer** — every reconnect becomes a full resync.

## Tradeoffs
Bounded retention means resume is possible only within a window. Streams
are memory-resident, so the bound is also a memory budget.

## Decision detail: exact trimming
Redis' approximate `MAXLEN` trims only at macro-node boundaries, so a
bound of 10 can retain ~100 entries. That is acceptable for memory safety
but not for gap detection, which depends on knowing precisely what is
retained. Relay uses exact trimming and accepts the cost.

## Consequences
`RESUME_GAP` exists and is a first-class outcome. Relay is documented as
not being a message archive.

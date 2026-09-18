# ADR-009: TTL-based presence

## Context
Presence must be correct when a process dies without cleaning up, and a
user may hold several connections at once.

## Decision
Represent presence as Redis keys with a TTL refreshed by heartbeat.
Expiry means offline. Presence is connection-aware: a user is online
while any of their connections is.

## Alternatives
- **In-memory presence** — wrong the moment a node crashes.
- **Explicit offline events only** — a crashed node sends none.
- **Database-backed presence** — a write per heartbeat per user.

## Tradeoffs
Presence is eventually consistent, with staleness bounded by the TTL. A
crashed node's users appear online until their keys expire.

## Consequences
Presence is listed as eventually consistent in the guarantees table.
Implemented in `apps/gateway/presence/tracker.py`; the multi-connection
race is verified in `tests/integration/presence_verify.py`.

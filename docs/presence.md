# Presence

## Model

Presence is **derived from connection state**, never asserted by a
client, and stored as TTL-bounded Redis state rather than process memory.

```text
relay:presence:{tenant_id}:{user_id}  ->  SET of connection_id   (TTL 30s)
relay:presence:{tenant_id}:{user_id}:state -> "IDLE"             (optional)
```

| State | Meaning |
| --- | --- |
| `ONLINE` | At least one live connection |
| `IDLE` | Connections exist; the client reported inactivity |
| `OFFLINE` | No live connections (set emptied, or key expired) |

`ONLINE` and `OFFLINE` are derived and not client-settable. `IDLE` is the
only state a client may report, because only the client knows its tab is
in the background.

## Why TTL rather than events

A node that crashes sends no "offline" event. Any presence design that
depends on a clean shutdown reports crashed users as permanently online.
So heartbeats refresh a TTL, and the absence of heartbeats is what marks
a user offline. Staleness is bounded by `presence_ttl_seconds`
(default 30s).

## The multi-connection race

A user commonly holds several connections — two tabs, a phone and a
laptop. Closing one must not mark them offline.

```text
alice: {conn-001, conn-002}
conn-001 closes ->  alice: {conn-002}  -> still ONLINE
conn-002 closes ->  alice: {}          -> OFFLINE
```

Presence tracks a set of connection ids, not a boolean, and
`connection_offline()` reports "went offline" only when the set empties.

Verified in `tests/integration/presence_verify.py`: closing one of two
connections keeps the user ONLINE with the count decremented; closing the
last one flips them to OFFLINE.

## Tenant scoping

Presence keys embed `tenant_id`, and the API derives the tenant from the
caller's API key rather than accepting it as a parameter. A tenant
querying another tenant's user sees `OFFLINE` — verified in the same
test.

## API

```http
GET /v1/presence/{user_id}    ->  {"user_id": "alice", "state": "ONLINE", "connections": 2}
GET /v1/presence              ->  [ ... every user with presence in this tenant ... ]
```

Listing uses `SCAN`, never `KEYS`, so a large tenant cannot block Redis.

## Client protocol

Supply `user_id` in the `CONNECT` frame to participate in presence; omit
it and the connection is anonymous and untracked. To report idleness:

```json
{ "type": "PRESENCE", "state": "IDLE" }
```

## Guarantee

Presence is **eventually consistent**. After an unclean node death, that
node's users appear online until their keys expire. This is listed as a
non-guarantee in the README table and is a deliberate trade — see
[adr/ADR-009-ttl-based-presence.md](adr/ADR-009-ttl-based-presence.md).

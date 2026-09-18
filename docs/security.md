# Security

## Authentication

API keys, verified at connection time and on every publish. Keys are
generated with `secrets.token_urlsafe(32)` and stored as a SHA-256 hash
plus a non-secret prefix for lookup and log correlation. The plaintext
key is returned exactly once, at creation, and never persisted.

Verification uses `secrets.compare_digest` to avoid timing leaks.

A connection that fails authentication receives `ERROR(AUTH_FAILED)` and
is closed with code 4401 before it can subscribe to anything.

## Authorization

Authentication is not authorization. Channels are namespaced
`t-<tenant_id>:<name>`, and a connection may only subscribe within its
own tenant's namespace. Cross-tenant subscribe returns
`CHANNEL_FORBIDDEN`; cross-tenant publish returns HTTP 403.

Making authorization a pure function of the channel string keeps it off
the database hot path — no query per SUBSCRIBE.

Verified in `tests/integration/e2e_verify.py`: a tenant B key cannot
subscribe to or publish on a tenant A channel.

## Tenant isolation

Every connection, session, subscription, and message carries a
`tenant_id`. Channel names embed it, so Redis keys, Pub/Sub topics,
stream keys, and sequence counters are all tenant-scoped by construction
rather than by a check that could be forgotten.

## Input validation

Channel names are validated before reaching Redis: non-empty, ≤200
characters, restricted to `[A-Za-z0-9:_-.]`. Rejected: control
characters, whitespace, glob characters (`*`, `?`), braces, and null
bytes. A channel name becomes part of a Redis key and a Pub/Sub topic —
unvalidated input there is an injection surface.

Frames are parsed into Pydantic models; unknown or malformed frames are
rejected with `INVALID_FRAME` or `UNKNOWN_MESSAGE` rather than crashing
the handler.

## Resource limits

All configurable, all enforced:

| Limit | Setting | Default |
| --- | --- | --- |
| Message size | `max_message_size_bytes` | 1 MB |
| Connections per tenant | `max_connections_per_tenant` | 10 000 |
| Connections per API key | `max_connections_per_api_key` | 1 000 |
| Concurrent handshakes | `max_handshakes_in_flight` | 500 |
| Queue per connection | `connection_queue_max_messages` / `_bytes` | 1000 / 2 MB |
| In-flight per connection | `max_inflight_messages` | 100 |
| Recovery buffer | `channel_buffer_max_messages` / `_bytes` | 1000 / 5 MB |

Handshake concurrency is bounded by a semaphore so a reconnect storm
cannot force unbounded simultaneous authentication work.

## Secret handling

The structured logger redacts `token`, `api_key`, `authorization`,
`password`, `secret`, and `payload` fields. Message contents are not
logged by default — Relay carries other people's data and should not
narrate it into a log aggregator.

Prometheus labels deliberately exclude `message_id`, `session_id`,
`user_id`, and raw channel names: each distinct value would create a new
time series, and those identifiers belong in logs and traces.

## Not yet implemented

- Per-operation rate limiting (connection / message / subscription rates
  are specified but not enforced separately)
- API key expiry enforcement (`expires_at` is stored but not checked)
- TLS termination is delegated to Nginx and not configured in the demo
  Compose stack
- Fuzz testing of the protocol parser

# Relay client protocol

All frames are JSON objects with a `type` field. The connection is a
WebSocket at `/ws`. Every frame below is implemented in
`packages/protocol/frames.py` and exercised in `tests/integration/`.

## Connection states

```text
        (socket open)
              |
              v
      HANDSHAKING  --- CONNECT ---> authenticated? --no--> ERROR(AUTH_FAILED), close
              |                          |yes
              |                          v
              +--------------------> ACTIVE
                                        |
                  heartbeat timeout / slow consumer / DISCONNECT
                                        v
                                      DEAD  (session survives)
```

A frame other than `CONNECT` sent while HANDSHAKING is rejected with
`ERROR(INVALID_FRAME)` and the socket is closed.

---

## Frames

### CONNECT — client → server

```json
{ "type": "CONNECT", "token": "relay_...", "session_id": null }
```

| Field | Required | Notes |
| --- | --- | --- |
| `token` | yes | API key. Verified against a SHA-256 hash; never stored in plaintext |
| `session_id` | no | Supply to resume an existing session. Ignored if unknown, expired, or owned by another tenant — a new session is created instead |

Errors: `AUTH_FAILED` (invalid/revoked key, inactive tenant),
`RATE_LIMITED` (tenant connection limit reached).

### CONNECTED — server → client

```json
{
  "type": "CONNECTED",
  "connection_id": "conn-a1b2c3d4e5f6",
  "session_id": "6f1b...",
  "node_id": "relay-node-a",
  "heartbeat_interval_seconds": 15.0
}
```

`connection_id` is ephemeral (one per socket). `session_id` is
longer-lived and is what makes resume possible. `node_id` tells the
client which node it landed on, which is useful for debugging but must
not be relied on for correctness — sessions are portable across nodes.

### PING / PONG

```json
{ "type": "PING", "server_time": "1789679457.71" }
{ "type": "PONG" }
```

Server → client every `heartbeat_interval_seconds` (default 15s). If no
PONG arrives within `heartbeat_timeout_seconds` (default 10s) the
connection is marked dead and cleaned up. PING is a HIGH-priority control
frame: it bypasses flow control, so a client whose message window is
saturated still receives heartbeats.

Clients **must** respond to PING. A client that does not will be
disconnected even if its socket is fine.

### SUBSCRIBE / SUBSCRIBED

```json
{ "type": "SUBSCRIBE",  "channel": "t-<tenant_id>:room-engineering" }
{ "type": "SUBSCRIBED", "channel": "t-<tenant_id>:room-engineering" }
```

Channel names are validated before use: non-empty, ≤200 characters, and
restricted to `[A-Za-z0-9:_-.]`. Control characters, whitespace, glob
characters and braces are rejected — a channel name reaches Redis key and
Pub/Sub names, so unvalidated input is an injection surface.

Channels are namespaced `t-<tenant_id>:<name>`. Subscribing outside your
own tenant namespace returns `ERROR(CHANNEL_FORBIDDEN)`. Authentication
is not authorization: holding a valid key does not grant access to an
arbitrary channel.

Errors: `INVALID_CHANNEL`, `CHANNEL_FORBIDDEN`.

### UNSUBSCRIBE / UNSUBSCRIBED

```json
{ "type": "UNSUBSCRIBE",  "channel": "..." }
{ "type": "UNSUBSCRIBED", "channel": "..." }
```

Unsubscribing drops the local subscription and, if this was the node's
last local subscriber for the channel, tears down the node's Pub/Sub
listener for it.

### MESSAGE — server → client

The application message envelope:

```json
{
  "type": "MESSAGE",
  "message_id": "019a...",
  "channel": "t-<tenant_id>:room-engineering",
  "sequence": 1042,
  "timestamp": "2026-09-17T12:00:00Z",
  "payload": { "text": "hello" }
}
```

`message_id` is stable across delivery attempts — it is the key clients
deduplicate on. `sequence` is allocated server-side and is monotonic per
channel. `timestamp` is a server timestamp; client clocks are not assumed
to be synchronized.

### ACK — client → server

```json
{ "type": "ACK", "channel": "t-...:room-engineering", "sequence": 1050 }
```

Acknowledgements are **cumulative**: acking 1050 acknowledges everything
at or below 1050 on that channel. This reduces ack traffic and makes the
resume point a single integer per channel.

An ACK does two things:

1. advances the session's durable resume position in Redis, and
2. releases in-flight slots in the connection's flow-control window.

Consequence: **a client that stops acking stops receiving.** This is the
intended backpressure behavior, not a bug — see
[backpressure.md](backpressure.md).

Acks that move backwards are ignored. Duplicate acks are harmless.

### RESUME / RESUMED

```json
{
  "type": "RESUME",
  "session_id": "6f1b...",
  "last_ack_by_channel": { "t-...:room-engineering": 1050 }
}
```

```json
{
  "type": "RESUMED",
  "session_id": "6f1b...",
  "replayed_by_channel": { "t-...:room-engineering": 3 },
  "gaps": []
}
```

The server replays everything after the client's acked position from the
channel's recovery buffer, re-attaches the subscription, and then sends
RESUMED. Replayed messages arrive **before** the RESUMED frame.

If the client's position is older than what the buffer still retains, the
server emits `ERROR(RESUME_GAP)` for that channel and lists it in
`gaps` — it does not send a partial stream that looks complete. The
client must then resynchronize from its own source of truth.

### NACK — client → server

```json
{ "type": "NACK", "channel": "...", "message_id": "...", "reason": "..." }
```

Signals that a message could not be processed. Accepted by the protocol
schema; retry-on-NACK is not yet implemented.

### ERROR — server → client

```json
{ "type": "ERROR", "code": "RESUME_GAP", "message": "..." }
```

| Code | Meaning |
| --- | --- |
| `AUTH_FAILED` | Invalid, expired, or revoked credentials |
| `CHANNEL_FORBIDDEN` | Authenticated, but not authorized for this channel |
| `INVALID_CHANNEL` | Channel name failed validation |
| `INVALID_FRAME` | Malformed frame, or a frame sent in the wrong state |
| `MESSAGE_TOO_LARGE` | Payload exceeded `max_message_size_bytes` |
| `RESUME_WINDOW_EXPIRED` | Session outlived the resume window |
| `RESUME_GAP` | Requested position is older than retained messages |
| `SLOW_CONSUMER` | Connection exceeded its queue bounds |
| `RATE_LIMITED` | A connection or operation limit was hit |
| `UNKNOWN_MESSAGE` | Unrecognized frame type |

### DISCONNECT

```json
{ "type": "DISCONNECT", "reason": "NODE_DRAINING" }
```

Either direction. Server reasons: `NODE_DRAINING` (deploy in progress —
reconnect with backoff), `SLOW_CONSUMER` (the client fell behind).

---

## Client obligations

A conforming client must:

1. send `CONNECT` as its first frame,
2. respond to every `PING` with `PONG`,
3. acknowledge messages it has processed (or it will stop receiving),
4. reconnect with capped exponential backoff **and jitter**,
5. supply its `session_id` on reconnect and `RESUME` from its acked position,
6. restore its subscriptions after reconnect,
7. deduplicate on `message_id`,
8. treat `RESUME_GAP` / `RESUME_WINDOW_EXPIRED` as "resynchronize from source of truth".

`client/src/RelayClient.ts` implements all eight.

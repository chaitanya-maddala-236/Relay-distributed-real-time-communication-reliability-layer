# Sessions and resumption

## Session vs connection

```text
session_id = abc123          (survives disconnects, TTL-bounded)
  |
  +-- connection_id = conn-001   (one WebSocket)
  |        X dies
  +-- connection_id = conn-002   (reconnect, same session)
```

A connection is one TCP/WebSocket lifetime. A session is the durable
identity that carries ack positions and subscriptions across
connections. Sessions live in Redis (`relay:session:{id}`) with a TTL
equal to `session_resume_window_seconds`, refreshed on activity.

Because session state is in Redis and not in a node's memory, a session
can be resumed on **any** node. This is why Relay does not need sticky
sessions at the load balancer.

## Resume flow

```text
disconnect
   |
   v  (session retained, messages continue to buffer)
client reconnects: CONNECT(token, session_id)
   |
   v  CONNECTED (same session_id, new connection_id)
client sends: RESUME(last_ack_by_channel)
   |
   +-- position within retention -> replay messages > last_ack -> RESUMED
   |
   +-- position older than retention -> ERROR(RESUME_GAP) -> client resyncs
```

Replayed messages arrive before the `RESUMED` frame. Subscriptions for
replayed channels are re-attached automatically — clients do not
re-subscribe manually (though `RelayClient` also re-sends SUBSCRIBE for
its full subscription set, which is idempotent).

## The resume position

One integer per channel: the highest cumulatively acked sequence. Stored
at `relay:session:{id}:ack:{channel}`, advanced only forward — a late or
duplicate ACK for a lower sequence never moves it backwards.

## Gap detection

The recovery buffer is bounded (`channel_buffer_max_messages`,
`message_retention_seconds`). If a client's position is older than the
oldest retained message, the honest answer is "I cannot prove you did not
miss something":

```text
client asks from:  500
oldest retained:   700
                   ^ messages 501..699 are gone
```

Relay returns `RESUME_GAP` rather than replaying 700 onward and letting
the client believe it is caught up.

## Retention

| Setting | Default | Meaning |
| --- | --- | --- |
| `RELAY_SESSION_RESUME_WINDOW_SECONDS` | 300 | How long a session stays resumable |
| `RELAY_MESSAGE_RETENTION_SECONDS` | 300 | Age bound on buffered messages |
| `RELAY_CHANNEL_BUFFER_MAX_MESSAGES` | 1000 | Count bound on buffered messages |
| `RELAY_CHANNEL_BUFFER_MAX_BYTES` | 5000000 | Size bound |

Both bounds apply: whichever binds first evicts. A busy channel may fall
out of resume range in far less than 300 seconds — this is expected, and
it is what `RESUME_GAP` exists to communicate.

Relay is not a message archive. Anything that must be durable belongs in
the application's own store.

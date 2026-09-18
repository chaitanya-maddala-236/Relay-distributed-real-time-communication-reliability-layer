# Backpressure and flow control

## The problem

```text
fast producer ----> [ queue ] ----> slow consumer
```

Without bounds, the queue is a memory leak that grows until the process
dies. With naive bounds shared across connections, one slow client
degrades everyone.

## The design that did not work

The obvious approach: let the socket write block, let the per-connection
queue fill behind it, disconnect when the queue exceeds its bound.

This was implemented and measured, and it does not work on this
transport. Pushing ~9 MB to a client that had stopped reading, the
server-side queue never grew past zero:

```text
published   queue_depth   queue_bytes
        0             0             0
       50             0             0
      ...
      300             0             0
```

The ASGI server buffers WebSocket writes internally, so `send_text`
returns long before bytes reach the client. The server had no signal that
anything was wrong. A slow-consumer policy built on that signal would
have looked correct in code and never fired in production.

## The design that works: ACK-based flow control

Flow control is applied at the application level, using the ACKs the
protocol already requires.

```text
                    in-flight window (max_inflight_messages)
                  +----------------------------------------+
  bounded queue ->| delivered, awaiting ACK                 |-> client
  (max_messages,  +----------------------------------------+
   max_bytes)                    ^
                                 |
                       ACK releases slots (cumulative)
```

1. A connection may have at most `max_inflight_messages` delivered but
   unacknowledged messages (default 100).
2. Beyond that, the sender task stops and messages accumulate in the
   connection's bounded queue.
3. An `ACK` for sequence N releases every in-flight slot at or below N on
   that channel and the sender resumes immediately.
4. If the queue exceeds `max_queue_messages` or `max_queue_bytes`, the
   connection is disconnected with `SLOW_CONSUMER`.

This is transport-independent, and it makes "slow consumer" mean the
right thing: a client that cannot keep up with **processing**, not one
whose TCP window happens to be full for a moment.

## Priorities

| Priority | Flow-controlled | Droppable | Used for |
| --- | --- | --- | --- |
| HIGH | no | no | PING, DISCONNECT, ERROR |
| NORMAL | yes | no | application messages |
| LOW | yes | yes | ephemeral signals (typing, presence) |

HIGH bypasses both the window and the queue bound: a client whose window
is saturated must still receive heartbeats and its own disconnect notice.
LOW frames are dropped rather than triggering a disconnect, because
dropping a stale typing indicator is correct behavior, not a failure.

## Isolation

Each connection owns its queue, its window, and its sender task. Fanout
enqueues to each subscriber and never waits on any of them; a connection
that hits its bound is terminated by a separate task so the fanout loop
continues immediately to the next recipient.

Measured (`tests/reliability/slow_consumer_verify.py`, 400 messages
×8 KB, `max_inflight=10`, `max_queue_messages=20`):

| | Non-acking client | Healthy client |
| --- | --- | --- |
| Outcome | disconnected, `SLOW_CONSUMER` | stayed connected |
| Messages received | stopped early (window held it) | 400 / 400, in order |
| Delivery latency | — | p50 5.2 ms, p95 6.3 ms, max 26.8 ms |

Server log at the moment of disconnect:

```json
{"event": "slow_consumer_disconnected", "connection_id": "conn-f800b0033d1d",
 "queue_depth": 20, "queue_bytes": 164834}
```

The queue stopped exactly at its configured bound of 20.

## Consequence for client authors

**A client that does not ACK will stop receiving and then be
disconnected.** This is documented behavior, not a malfunction. If you
need to pause processing, keep acking what you have handled; if you stop
acking entirely, expect the window to close within
`max_inflight_messages` and a disconnect shortly after.

## Configuration

| Setting | Default | Effect |
| --- | --- | --- |
| `RELAY_MAX_INFLIGHT_MESSAGES` | 100 | Unacked messages per connection |
| `RELAY_CONNECTION_QUEUE_MAX_MESSAGES` | 1000 | Queue bound (count) |
| `RELAY_CONNECTION_QUEUE_MAX_BYTES` | 2000000 | Queue bound (bytes) |
| `RELAY_CONNECTION_QUEUE_HIGH_WATERMARK_RATIO` | 0.8 | Warning threshold |

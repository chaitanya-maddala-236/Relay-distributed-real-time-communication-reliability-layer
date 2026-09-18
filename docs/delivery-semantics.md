# Delivery semantics

## Relay provides at-least-once delivery

A message accepted by `POST /v1/publish` and retained in the recovery
buffer will be delivered to a subscribed session one or more times,
provided the session reconnects within the retention window.

Relay does **not** provide exactly-once delivery, and no configuration
enables it.

## Why not exactly-once

Exactly-once delivery across a network partition requires an atomic
commit between "the message left the server" and "the client durably
processed it". Relay cannot observe the second half: a client can receive
a message, process it, and die before its ACK is written, and that is
indistinguishable from a client that never received it. The only honest
options are to deliver again (duplicates) or not to deliver (loss). Relay
chooses duplicates and gives clients the tools to detect them.

Exactly-once *processing* is achievable — but it is an application-level
property, built from at-least-once delivery plus idempotent handling
keyed on `message_id`. Relay supplies the identifier; the application
supplies the idempotency.

## Modes

| Mode | Behavior |
| --- | --- |
| `AT_MOST_ONCE` | Fire and forget. No ack tracking, no replay. A message lost in transit is gone |
| `AT_LEAST_ONCE` | Default. Acked, retained, replayed on resume. Duplicates possible |

Configured per channel via `delivery_mode` on the control-plane channel
record.

## Where duplicates come from

1. **Resume overlap.** A client processes message 1051 but dies before
   its ACK is recorded. On resume it asks from 1050 and receives 1051
   again. This is the common case.
2. **Retry.** A message pending ACK past `ack_timeout` may be redelivered.
3. **Client reconnect races.** A reconnect that overlaps an in-flight
   delivery can produce a repeat.

In all three the `message_id` is stable across attempts, which is what
makes suppression possible.

## What clients must do

If your processing is naturally idempotent (setting a value, rendering
the latest state), you need nothing. If it is not (incrementing a
counter, charging a card, appending to a log), deduplicate on
`message_id` before processing.

`RelayClient` keeps a bounded FIFO of recently seen ids
(`dedupWindowSize`, default 5000) and suppresses repeats within it. The
window is bounded deliberately — an unbounded dedup map is a memory leak
with a long fuse (Section 95). For workloads where a duplicate must never
be processed regardless of age, deduplicate in your own durable store.

## What "delivered" means in the metrics

`relay_messages_delivered_total` counts messages written to a
connection's outbound path, not messages processed by an application.
Messages acknowledged is the closer proxy for that:
`relay_messages_acked_total`.

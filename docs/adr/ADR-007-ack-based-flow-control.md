# ADR-007: ACK-based flow control instead of socket backpressure

## Context
A fast producer and a slow consumer will exhaust memory unless delivery
is bounded. The intuitive mechanism is a bounded queue behind a blocking
socket write.

## Decision
Bound delivery with an application-level in-flight window released by
cumulative ACKs, backed by a bounded per-connection queue.

## Why not socket backpressure
It was implemented first and measured: ~9 MB was written to a client that
had stopped reading and the server-side queue never grew past zero,
because the ASGI server buffers WebSocket writes internally. A
slow-consumer policy built on that signal would look correct and never
fire.

## Alternatives
- **Trust `send()` to block** — does not work here, as measured.
- **Reach into the transport's write buffer** — server-implementation
  specific and fragile.
- **Drop messages silently under pressure** — violates at-least-once.

## Tradeoffs
Clients must ACK to keep receiving; a non-acking client is throttled and
eventually disconnected. This is stricter than some clients expect, so it
is documented prominently.

## Consequences
Control frames are HIGH priority and bypass the window, or a throttled
client would never see its own heartbeat or disconnect notice. LOW
priority frames are dropped rather than escalating to a disconnect.

# Ordering

## Guarantee

Relay guarantees ordering **per channel**, for a subscriber that remains
connected, within the retention contract.

For a channel `C`, if the server assigns sequence numbers 1, 2, 3 to
three messages, every connected subscriber of `C` receives them in that
relative order.

## Non-guarantees

- **No global ordering.** Messages on `room:123` and `room:456` have no
  defined order relative to each other, even within one tenant, even for
  a client subscribed to both.
- **No cross-channel causality.** If your application needs "A happened
  before B" across channels, encode that in the payload.
- **No ordering across a gap.** If a resume returns `RESUME_GAP`, the
  client has lost its position in the sequence and ordering claims no
  longer apply until it resynchronizes.

## Why global ordering is not offered

Global ordering requires a single serialization point for every message
in the system. That is one counter, one shard, one bottleneck — it caps
throughput at whatever a single sequencer can do and makes every publish
wait on it. Almost no real-time application needs it: a chat room needs
its own messages ordered, not ordered against an unrelated room's.

Per-channel ordering keeps the serialization point per channel, so
unrelated channels scale independently.

## How sequences are assigned

```text
publisher -> INCR relay:sequence:{channel} -> sequence -> buffer -> fanout
```

`INCR` in Redis is atomic, so concurrent publishers on the same channel
cannot collide. This happens **before** fanout, so every node delivering
the message delivers the same sequence, and ordering is identical
regardless of which node a subscriber is on.

Application-local counters are deliberately not used: with N gateway
processes they would produce N independent sequence spaces for the same
channel.

Verified in:
- `tests/reliability/test_resume.py::test_sequence_allocation_is_monotonic_under_concurrency`
  — 200 concurrent allocations, zero duplicates
- `tests/distributed/multinode_verify.py` — alternating publishes across
  two nodes produce one monotonic sequence, and both nodes deliver the
  same order

## Ordering during fanout

Each connection has a single sender task draining a FIFO queue, so
per-connection delivery order follows enqueue order. Relay does not spawn
a task per recipient per message, which would reorder under load.

## Ordering during replay

Replayed messages are sorted by sequence before delivery and arrive
before the `RESUMED` frame, so a resuming client sees the same order a
continuously connected one would.

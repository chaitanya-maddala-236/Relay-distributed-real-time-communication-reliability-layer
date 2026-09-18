# ADR-004: Redis Pub/Sub for live fanout

## Context
A message published on one node must reach subscribers connected to other
nodes.

## Decision
Use Redis Pub/Sub for live cross-node fanout, and only for live fanout.

## Alternatives
- **Node-to-node mesh** — no broker, but O(N²) connections and its own
  membership problem.
- **Kafka** — durable and ordered, but heavy for ephemeral fanout and
  explicitly out of scope.
- **Redis Streams as the fanout path** — durable, but consumer groups add
  coordination cost on the hot path.

## Tradeoffs
Pub/Sub is fast and simple, but **not durable**: a message published
while a node is disconnected from Redis is not redelivered, and
subscriptions do not survive a Redis restart.

## Consequences
Pub/Sub is never used for recovery (see ADR-005). Listeners must be
supervised and resubscribe after a Redis restart — without this,
delivery never recovered after an outage, which was observed in testing.

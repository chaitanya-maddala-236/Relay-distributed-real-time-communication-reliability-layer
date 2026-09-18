# ADR-002: At-least-once delivery, never exactly-once

## Context
Clients disconnect mid-delivery. An ACK can be lost after processing
succeeded. The server cannot distinguish "processed but ack lost" from
"never received".

## Decision
Provide at-least-once delivery in reliable mode. Provide `message_id` for
deduplication. Never claim exactly-once, in code, docs, or metrics.

## Alternatives
- **At-most-once only** — simpler, but silently loses messages across
  reconnects, which defeats the purpose of the system.
- **Claim exactly-once** — achievable only as exactly-once *processing*,
  and only with application cooperation. Claiming it at the transport
  layer would be false.

## Tradeoffs
Applications must handle duplicates. In exchange, no message is silently
dropped on reconnect, and the failure modes are explicit.

## Consequences
The protocol carries `message_id` and cumulative `sequence` acks. The
reference client dedups within a bounded window. The guarantees table
states exactly-once is not provided.

# ADR-003: Ordering scoped to a channel

## Context
Global ordering requires a single serialization point for all messages,
which caps throughput and couples unrelated channels.

## Decision
Guarantee ordering per channel only. Document explicitly that messages on
different channels have no defined relative order.

## Alternatives
- **Global ordering** — one counter for the whole system; a hard
  throughput ceiling and a single point of contention.
- **Per-tenant ordering** — still serializes every busy tenant's traffic.
- **No ordering** — pushes reassembly onto every application.

## Tradeoffs
Applications needing cross-channel causality must encode it themselves.
In exchange, unrelated channels scale independently.

## Consequences
One Redis counter per channel. Cross-channel ordering is listed as a
non-guarantee in the README table.

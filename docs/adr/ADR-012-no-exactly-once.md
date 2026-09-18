# ADR-012: Publishing honest guarantees

## Context
Real-time infrastructure is routinely described with claims it cannot
support: exactly-once, zero message loss, infinite scale.

## Decision
Publish a guarantees table naming what is *not* guaranteed, and only
state performance numbers that have been measured on a described
environment. Where a failure mode has not been exercised, say so.

## Alternatives
- **Market the strongest plausible claim** — cheap, and wrong the first
  time someone depends on it.
- **Say nothing about limits** — leaves users to discover them in
  production.

## Tradeoffs
The system looks less impressive on paper. It is far more useful to build
on, and a reviewer can tell which claims were tested.

## Consequences
`docs/performance.md` has a "not yet measured" section. Gap detection
returns `RESUME_GAP` instead of a partial replay. Redis failure returns
503 instead of a silent accept. Two bugs found this way — Pub/Sub not
recovering after a Redis restart, and backpressure never firing — were
invisible until the failure was actually run.

# ADR-011: Graceful node draining

## Context
Deploys should not look like an outage to connected clients.

## Decision
On SIGTERM: mark draining, fail readiness, refuse new connections, notify
clients with `DISCONNECT(NODE_DRAINING)`, close sockets, stop listeners,
delete the node key. Sessions are preserved so clients resume elsewhere.

## Alternatives
- **Hard kill** — every client reconnects simultaneously with no warning:
  a self-inflicted reconnect storm.
- **Wait for connections to close naturally** — long-lived by design;
  they never do.

## Tradeoffs
A brief window where the node is up but refusing work. Clients experience
a reconnect, not an error.

## Consequences
Clients must implement backoff with jitter (ADR-012 companion behavior in
the reference client). Deploys should roll one node at a time.

# ADR-006: Session-based reconnection, not sticky sessions

## Context
A reconnecting client must resume where it left off. The load balancer
may send it to a different node.

## Decision
Give each client a `session_id` stored in Redis with a TTL, carrying its
ack positions. Any node can resume any session.

## Alternatives
- **Sticky sessions** — pins a client to a node; that node's crash is
  exactly when resumption matters most.
- **Client-held state only** — the client cannot know what it never
  received.
- **Full resync on every reconnect** — expensive and often impossible for
  the application to do cheaply.

## Tradeoffs
Requires Redis on the connect path: if Redis is down, new connections are
rejected. Accepted, because the alternative is a system that cannot
recover from node loss.

## Consequences
No sticky sessions in Nginx. Session portability across nodes is verified
in the distributed test suite.

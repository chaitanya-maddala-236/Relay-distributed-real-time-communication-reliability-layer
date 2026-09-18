# ADR-001: WebSocket as the client transport

## Context
Relay needs bidirectional, low-latency, long-lived client connections
through ordinary web infrastructure (browsers, proxies, corporate
networks).

## Decision
Use WebSockets over HTTP/1.1 Upgrade as the sole client transport.

## Alternatives
- **Server-Sent Events + POST** — unidirectional server→client; needs a
  second channel for upstream, and reconnection semantics are weaker.
- **Long polling** — works everywhere, but latency and connection churn
  are poor and it complicates ordering.
- **Raw TCP / QUIC** — better control, unusable from a browser without a
  custom client.
- **WebTransport** — attractive (streams, unreliable datagrams) but
  browser and proxy support is uneven today.

## Tradeoffs
WebSockets give browser reach and a simple framing model. They do not
give us backpressure signalling we can trust (see ADR-007), and they
require load-balancer configuration that differs from ordinary HTTP.

## Consequences
Nginx must be configured for Upgrade, long read timeouts, and no
buffering. Flow control must be solved at the application layer.

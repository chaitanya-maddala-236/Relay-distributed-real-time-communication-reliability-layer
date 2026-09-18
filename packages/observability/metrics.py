"""Prometheus metrics (Section 64).

Deliberately label-free counters and gauges. Section 65 forbids
message_id / session_id / user_id / raw channel names as labels because
each distinct value creates a new time series; those identifiers belong
in structured logs and traces instead.
"""
from __future__ import annotations

from collections import defaultdict

METRICS: dict[str, float] = defaultdict(float)

_COUNTERS = [
    "relay_connections_total",
    "relay_connections_rejected_total",
    "relay_disconnects_total",
    "relay_messages_received_total",
    "relay_messages_delivered_total",
    "relay_messages_acked_total",
    "relay_messages_retried_total",
    "relay_messages_dropped_total",
    "relay_slow_consumers_total",
    "relay_resume_attempts_total",
    "relay_resume_success_total",
    "relay_resume_failures_total",
    "relay_auth_failures_total",
    "relay_subscription_rejections_total",
]

_GAUGES = [
    "relay_connections_active",
    "relay_queue_depth",
    "relay_queue_bytes",
    "relay_active_sessions",
    "relay_presence_users",
    "relay_fanout_size",
]

_HELP = {
    "relay_connections_active": "Currently active WebSocket connections on this node",
    "relay_messages_delivered_total": "Messages written to a client socket",
    "relay_slow_consumers_total": "Connections disconnected for exceeding queue bounds",
    "relay_resume_failures_total": "Resume attempts that could not be satisfied",
}


def render_metrics() -> str:
    lines: list[str] = []
    for name in _COUNTERS:
        if name in _HELP:
            lines.append(f"# HELP {name} {_HELP[name]}")
        lines.append(f"# TYPE {name} counter")
        lines.append(f"{name} {METRICS[name]}")
    for name in _GAUGES:
        if name in _HELP:
            lines.append(f"# HELP {name} {_HELP[name]}")
        lines.append(f"# TYPE {name} gauge")
        lines.append(f"{name} {METRICS[name]}")
    return "\n".join(lines) + "\n"

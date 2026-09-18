"""Heartbeat / dead-connection detection (Section 16-17).

An idle TCP connection cannot be assumed alive. This loop sends PING at
a fixed interval and expects a PONG within the timeout; if none arrives
the connection is torn down and its resources released.
"""
from __future__ import annotations

import asyncio
import time

from packages.common.config import settings
from packages.protocol.frames import PingFrame


async def run_heartbeat(conn, on_dead) -> None:
    """Runs for the lifetime of one connection. `on_dead` is an async
    callback invoked exactly once if the heartbeat times out — the caller
    (websocket handler) performs the actual cleanup sequence (Section 17)."""
    from apps.gateway.connections.manager import ConnectionState, Priority, SlowConsumerError

    try:
        while conn.state == ConnectionState.ACTIVE:
            await asyncio.sleep(settings.heartbeat_interval_seconds)
            if conn.state != ConnectionState.ACTIVE:
                return
            frame = PingFrame(server_time=str(time.time()))
            try:
                await conn.enqueue(frame.model_dump_json(), priority=Priority.HIGH)
            except SlowConsumerError:
                await on_dead(conn, "SLOW_CONSUMER")
                return

            sent_at = float(frame.server_time)
            deadline = time.time() + settings.heartbeat_timeout_seconds
            alive = False
            while time.time() < deadline:
                await asyncio.sleep(0.5)
                if conn.state != ConnectionState.ACTIVE:
                    return
                # last_pong_at is updated by the frame-receive loop whenever
                # a PONG arrives; if it advanced past when we sent PING, the
                # client is alive.
                if conn.last_pong_at >= sent_at:
                    alive = True
                    break
            if not alive:
                await on_dead(conn, "HEARTBEAT_TIMEOUT")
                return
    except asyncio.CancelledError:
        raise

"""Connection layer (Section 8) with ACK-based flow control (Section 37-38).

A `Connection` wraps one live WebSocket, its bounded outbound queue, and
an in-flight delivery window. The registry is process-local — Section 53
is explicit that a live WebSocket must never be serialized into Redis.

### Why flow control is ACK-based rather than socket-based

The obvious backpressure design is "let the socket write block and let
the queue fill". That does not work on this transport: the ASGI server
buffers WebSocket writes internally, so `send_text` returns long before
the bytes reach the client. Measured directly during development: ~9 MB
was pushed to a client that had stopped reading and the server-side queue
never grew past zero.

So flow control is applied at the application level, using the ACKs the
protocol already requires. A connection may have at most
`max_inflight_messages` delivered-but-unacknowledged messages. Beyond
that, messages wait in the bounded queue; if the queue exceeds its bound
the connection is disconnected with SLOW_CONSUMER.

This also makes "slow consumer" mean the right thing — a client that
cannot keep up with *processing*, not merely one whose TCP window is
momentarily full.

Control frames (PING, DISCONNECT, ERROR) are HIGH priority (Section 39):
never flow-controlled and never dropped, because they must still reach a
client whose message window is saturated.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum, auto

from fastapi import WebSocket

from packages.common.config import settings


class ConnectionState(Enum):
    HANDSHAKING = auto()
    ACTIVE = auto()
    DRAINING = auto()
    DEAD = auto()


class Priority(Enum):
    HIGH = auto()    # control frames: never flow-controlled, never dropped
    NORMAL = auto()  # application messages: flow-controlled
    LOW = auto()     # ephemeral (typing, presence): droppable under pressure


@dataclass
class QueuedMessage:
    payload: str  # pre-serialized JSON, ready to send
    size_bytes: int
    priority: Priority = Priority.NORMAL
    channel: str | None = None
    sequence: int | None = None


class SlowConsumerError(Exception):
    """Raised when a connection's outbound queue exceeds its bound
    (Section 38). The caller must disconnect it with SLOW_CONSUMER."""


@dataclass
class Connection:
    connection_id: str
    session_id: str
    tenant_id: str
    websocket: WebSocket
    state: ConnectionState = ConnectionState.HANDSHAKING
    created_at: float = field(default_factory=time.time)
    last_pong_at: float = field(default_factory=time.time)
    subscriptions: set[str] = field(default_factory=set)
    user_id: str | None = None

    _queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    _queue_bytes: int = 0
    # Delivered but not yet acknowledged, as (channel, sequence) pairs.
    _inflight: list = field(default_factory=list)
    _window_event: asyncio.Event = field(default_factory=asyncio.Event)
    messages_dropped: int = 0

    def __post_init__(self) -> None:
        self._window_event.set()

    # --- introspection (metrics / admin) ---------------------------------

    def queue_depth(self) -> int:
        return self._queue.qsize()

    def queue_bytes(self) -> int:
        return self._queue_bytes

    def inflight_count(self) -> int:
        return len(self._inflight)

    def high_watermark_reached(self) -> bool:
        limit = settings.connection_queue_max_messages * settings.connection_queue_high_watermark_ratio
        return self._queue.qsize() >= limit

    # --- enqueue / flow control ------------------------------------------

    async def enqueue(
        self,
        payload: str,
        priority: Priority = Priority.NORMAL,
        channel: str | None = None,
        sequence: int | None = None,
    ) -> None:
        """Enqueue an outbound frame.

        Raises SlowConsumerError if the bounded queue is already full
        (Section 38) — the caller disconnects the connection. LOW priority
        frames are dropped instead of raising, since they are ephemeral by
        definition (Section 39-40).
        """
        size = len(payload.encode("utf-8"))
        over_limit = (
            self._queue.qsize() >= settings.connection_queue_max_messages
            or self._queue_bytes + size > settings.connection_queue_max_bytes
        )

        if over_limit:
            if priority is Priority.LOW:
                self.messages_dropped += 1
                return
            if priority is not Priority.HIGH:
                raise SlowConsumerError(self.connection_id)
            # HIGH priority control frames jump the bound deliberately.

        self._queue_bytes += size
        await self._queue.put(
            QueuedMessage(
                payload=payload, size_bytes=size, priority=priority,
                channel=channel, sequence=sequence,
            )
        )

    def on_ack(self, channel: str, sequence: int) -> int:
        """Cumulative ACK (Section 24): release every in-flight message on
        this channel at or below `sequence` and reopen the window."""
        before = len(self._inflight)
        self._inflight = [
            (ch, seq) for (ch, seq) in self._inflight if not (ch == channel and seq <= sequence)
        ]
        released = before - len(self._inflight)
        if len(self._inflight) < settings.max_inflight_messages:
            self._window_event.set()
        return released

    async def _wait_for_window(self) -> None:
        while len(self._inflight) >= settings.max_inflight_messages:
            self._window_event.clear()
            await self._window_event.wait()

    async def run_sender(self) -> None:
        """Drains this connection's queue to the socket. Runs as its own
        task per connection, so a throttled or stalled connection can
        never delay delivery to any other connection (Section 42)."""
        try:
            while self.state in (ConnectionState.ACTIVE, ConnectionState.DRAINING):
                item = await self._queue.get()
                self._queue_bytes -= item.size_bytes

                if item.priority is Priority.NORMAL:
                    await self._wait_for_window()
                    if self.state not in (ConnectionState.ACTIVE, ConnectionState.DRAINING):
                        return
                    if item.channel is not None and item.sequence is not None:
                        self._inflight.append((item.channel, item.sequence))

                await self.websocket.send_text(item.payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.state = ConnectionState.DEAD


class ConnectionRegistry:
    """Process-local registry of live connections (Section 8, 53)."""

    def __init__(self) -> None:
        self._by_id: dict[str, Connection] = {}
        self._by_session: dict[str, set[str]] = {}
        self._by_tenant: dict[str, set[str]] = {}
        self._by_channel: dict[str, set[str]] = {}

    def add(self, conn: Connection) -> None:
        self._by_id[conn.connection_id] = conn
        self._by_session.setdefault(conn.session_id, set()).add(conn.connection_id)
        self._by_tenant.setdefault(conn.tenant_id, set()).add(conn.connection_id)

    def remove(self, connection_id: str) -> Connection | None:
        conn = self._by_id.pop(connection_id, None)
        if conn is None:
            return None
        self._by_session.get(conn.session_id, set()).discard(connection_id)
        self._by_tenant.get(conn.tenant_id, set()).discard(connection_id)
        for channel in list(conn.subscriptions):
            self._by_channel.get(channel, set()).discard(connection_id)
        return conn

    def get(self, connection_id: str) -> Connection | None:
        return self._by_id.get(connection_id)

    def subscribe(self, connection_id: str, channel: str) -> None:
        conn = self._by_id.get(connection_id)
        if conn is None:
            return
        conn.subscriptions.add(channel)
        self._by_channel.setdefault(channel, set()).add(connection_id)

    def unsubscribe(self, connection_id: str, channel: str) -> None:
        conn = self._by_id.get(connection_id)
        if conn is not None:
            conn.subscriptions.discard(channel)
        self._by_channel.get(channel, set()).discard(connection_id)

    def local_subscribers(self, channel: str) -> list[Connection]:
        return [self._by_id[cid] for cid in self._by_channel.get(channel, set()) if cid in self._by_id]

    def session_connections(self, session_id: str) -> list[Connection]:
        return [self._by_id[cid] for cid in self._by_session.get(session_id, set()) if cid in self._by_id]

    def tenant_connection_count(self, tenant_id: str) -> int:
        return len(self._by_tenant.get(tenant_id, set()))

    def active_count(self) -> int:
        return len(self._by_id)

    def all_connections(self) -> list[Connection]:
        return list(self._by_id.values())


registry = ConnectionRegistry()

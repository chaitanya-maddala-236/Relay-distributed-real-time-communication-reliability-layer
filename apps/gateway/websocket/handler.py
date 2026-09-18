"""WebSocket endpoint: the full connection lifecycle (Section 12, 76).

CONNECT -> auth -> session create/resume -> CONNECTED
  loop: SUBSCRIBE / UNSUBSCRIBE / ACK / RESUME / PING-PONG
DISCONNECT / heartbeat timeout / slow consumer -> cleanup (Section 17)
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid

from fastapi import WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from apps.gateway.connections.manager import Connection, ConnectionState, registry
from apps.gateway.heartbeat.monitor import run_heartbeat
from apps.gateway.presence import tracker as presence
from apps.gateway.protocol.auth import AuthenticationError, authenticate_token, channel_belongs_to_tenant
from apps.gateway.recovery.replay import replay_channel
from apps.gateway.rooms.subscriptions import get_subscription_manager
from apps.gateway.sessions.manager import create_session, get_session, record_ack, touch_session
from packages.common.config import settings
from packages.database.engine import SessionLocal
from packages.observability.logging import log_event
from packages.observability.metrics import METRICS
from packages.protocol.frames import (
    AckFrame,
    ConnectedFrame,
    ErrorCode,
    ErrorFrame,
    FrameType,
    ResumedFrame,
    ResumeFrame,
    SubscribedFrame,
    SubscribeFrame,
    UnsubscribedFrame,
    UnsubscribeFrame,
)
from packages.redis.client import get_redis

logger = logging.getLogger("relay.gateway.websocket")

_handshake_semaphore = asyncio.Semaphore(settings.max_handshakes_in_flight)


async def _send_error(ws: WebSocket, code: str, message: str) -> None:
    await ws.send_text(ErrorFrame(code=code, message=message).model_dump_json())


async def handle_connection(websocket: WebSocket) -> None:
    await websocket.accept()
    r = get_redis()

    async with _handshake_semaphore:
        try:
            raw = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        except (TimeoutError, WebSocketDisconnect):
            return

        if raw.get("type") != FrameType.CONNECT:
            await _send_error(websocket, ErrorCode.INVALID_FRAME, "expected CONNECT frame first")
            await websocket.close()
            return

        token = raw.get("token", "")
        requested_session_id = raw.get("session_id")
        # Optional client-supplied identity for presence. It is scoped to
        # the authenticated tenant, so it cannot be used to observe or
        # impersonate another tenant's users.
        user_id = raw.get("user_id")

        async with SessionLocal() as db:
            try:
                auth_ctx = await authenticate_token(db, token)
            except AuthenticationError as e:
                await _send_error(websocket, ErrorCode.AUTH_FAILED, str(e))
                await websocket.close(code=4401)
                return

        if registry.tenant_connection_count(auth_ctx.tenant_id) >= settings.max_connections_per_tenant:
            await _send_error(websocket, ErrorCode.RATE_LIMITED, "tenant connection limit reached")
            await websocket.close(code=4429)
            return

        # Session creation or resumption (Section 12, 19).
        session_id = None
        if requested_session_id:
            existing = await get_session(r, requested_session_id)
            if existing and existing.get("tenant_id") == auth_ctx.tenant_id:
                session_id = requested_session_id
        if session_id is None:
            session_id = await create_session(r, auth_ctx.tenant_id, settings.node_id)
        else:
            await touch_session(r, session_id, settings.node_id)

        connection_id = f"conn-{uuid.uuid4().hex[:12]}"
        conn = Connection(
            connection_id=connection_id,
            session_id=session_id,
            tenant_id=auth_ctx.tenant_id,
            websocket=websocket,
            state=ConnectionState.ACTIVE,
        )
        registry.add(conn)
        conn.user_id = user_id
        if user_id:
            await presence.connection_online(r, auth_ctx.tenant_id, user_id, connection_id)

        sender_task = asyncio.create_task(conn.run_sender())
        heartbeat_task = asyncio.create_task(run_heartbeat(conn, _on_dead_connection))

        await websocket.send_text(
            ConnectedFrame(
                connection_id=connection_id,
                session_id=session_id,
                node_id=settings.node_id,
                heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
            ).model_dump_json()
        )
        log_event(logger, logging.INFO, "connection_established", tenant_id=auth_ctx.tenant_id,
                  connection_id=connection_id, session_id=session_id)

    try:
        await _frame_loop(websocket, conn, auth_ctx.tenant_id, r)
    except WebSocketDisconnect:
        pass
    finally:
        await _cleanup_connection(conn, sender_task, heartbeat_task, r)


async def _frame_loop(websocket: WebSocket, conn: Connection, tenant_id: str, r) -> None:
    sub_mgr = get_subscription_manager()

    while conn.state == ConnectionState.ACTIVE:
        raw = await websocket.receive_json()
        frame_type = raw.get("type")

        if frame_type == FrameType.PONG:
            conn.last_pong_at = time.time()
            # A live heartbeat is exactly what presence should key off.
            await touch_session(r, conn.session_id, settings.node_id)
            if conn.user_id:
                await presence.refresh(r, tenant_id, conn.user_id)
            continue

        if frame_type == "PRESENCE":
            # Clients may report IDLE; ONLINE/OFFLINE stay derived from
            # connection state and are not client-settable.
            if conn.user_id:
                state = presence.PresenceState.IDLE if raw.get("state") == "IDLE" \
                    else presence.PresenceState.ONLINE
                await presence.set_state(r, tenant_id, conn.user_id, state)
            continue

        if frame_type == FrameType.SUBSCRIBE:
            try:
                frame = SubscribeFrame(**raw)
            except ValidationError as e:
                await _send_error(websocket, ErrorCode.INVALID_CHANNEL, str(e))
                continue
            if not channel_belongs_to_tenant(frame.channel, tenant_id):
                await _send_error(websocket, ErrorCode.CHANNEL_FORBIDDEN, frame.channel)
                continue
            registry.subscribe(conn.connection_id, frame.channel)
            await sub_mgr.on_local_subscribe(r, frame.channel)
            await websocket.send_text(SubscribedFrame(channel=frame.channel).model_dump_json())
            continue

        if frame_type == FrameType.UNSUBSCRIBE:
            frame = UnsubscribeFrame(**raw)
            registry.unsubscribe(conn.connection_id, frame.channel)
            await sub_mgr.on_local_unsubscribe(r, frame.channel)
            await websocket.send_text(UnsubscribedFrame(channel=frame.channel).model_dump_json())
            continue

        if frame_type == FrameType.ACK:
            frame = AckFrame(**raw)
            # Two effects: durable ack position for resume (Redis), and
            # releasing the in-flight flow-control window (local).
            await record_ack(r, conn.session_id, frame.channel, frame.sequence)
            released = conn.on_ack(frame.channel, frame.sequence)
            METRICS["relay_messages_acked_total"] += released
            continue

        if frame_type == FrameType.RESUME:
            frame = ResumeFrame(**raw)
            replayed: dict[str, int] = {}
            gaps: list[str] = []
            for channel, last_ack in frame.last_ack_by_channel.items():
                if not channel_belongs_to_tenant(channel, tenant_id):
                    continue
                result = await replay_channel(r, channel, last_ack)
                if result.gap:
                    gaps.append(channel)
                    await _send_error(websocket, ErrorCode.RESUME_GAP, channel)
                    continue
                for msg in result.messages:
                    from packages.protocol.frames import MessageFrame

                    mf = MessageFrame(
                        message_id=msg["message_id"],
                        channel=channel,
                        sequence=msg["sequence"],
                        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(msg["created_at"])),
                        payload=msg["payload"],
                    )
                    await conn.enqueue(
                        mf.model_dump_json(), channel=channel, sequence=msg["sequence"]
                    )
                replayed[channel] = len(result.messages)
                registry.subscribe(conn.connection_id, channel)
                await sub_mgr.on_local_subscribe(r, channel)
            await websocket.send_text(
                ResumedFrame(session_id=conn.session_id, replayed_by_channel=replayed, gaps=gaps).model_dump_json()
            )
            continue

        if frame_type == FrameType.DISCONNECT:
            break

        await _send_error(websocket, ErrorCode.UNKNOWN_MESSAGE, str(frame_type))


async def _on_dead_connection(conn: Connection, reason: str) -> None:
    """Heartbeat-timeout / slow-consumer callback (Section 17, 38)."""
    conn.state = ConnectionState.DEAD
    try:
        await conn.websocket.close(code=4408 if reason == "HEARTBEAT_TIMEOUT" else 4409)
    except Exception:
        pass
    log_event(logger, logging.WARNING, "connection_terminated", connection_id=conn.connection_id, reason=reason)


async def _cleanup_connection(conn: Connection, sender_task, heartbeat_task, r) -> None:
    """Dead connection cleanup sequence (Section 17): stop writes, close
    socket, release subscriptions, decrement counters, preserve the
    resumable session, emit metrics/logs."""
    conn.state = ConnectionState.DEAD
    sender_task.cancel()
    heartbeat_task.cancel()

    sub_mgr = get_subscription_manager()
    for channel in list(conn.subscriptions):
        await sub_mgr.on_local_unsubscribe(r, channel)

    if getattr(conn, "user_id", None):
        # Returns True only when this was the user's last connection —
        # a second tab staying open must keep them ONLINE (Section 49).
        went_offline = await presence.connection_offline(
            r, conn.tenant_id, conn.user_id, conn.connection_id
        )
        if went_offline:
            log_event(logger, logging.INFO, "presence_offline",
                      tenant_id=conn.tenant_id, connection_id=conn.connection_id)

    registry.remove(conn.connection_id)
    try:
        await conn.websocket.close()
    except Exception:
        pass

    log_event(logger, logging.INFO, "connection_closed", connection_id=conn.connection_id,
              session_id=conn.session_id)
    # Session itself is intentionally left alive in Redis (TTL-bound) so
    # the client can RESUME on a fresh connection (Section 19).

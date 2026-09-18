"""Relay gateway (data plane).

Hosts the WebSocket endpoint, the server-to-server publish API, health
probes, and Prometheus metrics. Node registration and graceful draining
are handled in the lifespan (Section 54-56, 97).
"""
from __future__ import annotations

import contextlib
import json
import logging
from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI, Header, HTTPException, Response, WebSocket
from pydantic import BaseModel, Field
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.ext.asyncio import AsyncSession

from apps.gateway.connections.manager import registry
from apps.gateway.fanout.publisher import publish_message
from apps.gateway.nodes.registration import NodeRegistrar
from apps.gateway.presence import tracker as presence
from apps.gateway.protocol.auth import AuthenticationError, authenticate_token, channel_belongs_to_tenant
from apps.gateway.websocket.handler import handle_connection
from packages.common.config import settings
from packages.database.engine import get_db
from packages.observability.logging import configure_logging, log_event
from packages.observability.metrics import METRICS, render_metrics
from packages.protocol.frames import validate_channel_name
from packages.redis.client import RedisKeys, close_redis, get_redis

logger = logging.getLogger("relay.gateway")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    r = get_redis()
    registrar = NodeRegistrar(settings.node_id, r)
    await registrar.start()
    app.state.registrar = registrar
    app.state.draining = False
    log_event(logger, logging.INFO, "gateway_started", node_id=settings.node_id)
    try:
        yield
    finally:
        # Graceful shutdown sequence (Section 97).
        app.state.draining = True
        await registrar.mark_draining()
        from apps.gateway.rooms.subscriptions import get_subscription_manager

        await _drain_connections()
        await get_subscription_manager().shutdown()
        await registrar.stop()
        await close_redis()
        log_event(logger, logging.INFO, "gateway_stopped", node_id=settings.node_id)


async def _drain_connections() -> None:
    """Tell every connected client to reconnect elsewhere, then close
    (Section 56). Clients receive a DISCONNECT with a reconnect signal so
    they back off with jitter instead of reconnecting instantly."""
    from packages.protocol.frames import DisconnectFrame

    frame = DisconnectFrame(reason="NODE_DRAINING").model_dump_json()
    for conn in registry.all_connections():
        with contextlib.suppress(Exception):
            await conn.websocket.send_text(frame)
        with contextlib.suppress(Exception):
            await conn.websocket.close(code=4503)


app = FastAPI(title="Relay Gateway", version="0.1.0", lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    if getattr(app.state, "draining", False):
        await websocket.close(code=4503)
        return
    await handle_connection(websocket)


# --- Publish API (Section 77-79) -------------------------------------------------


class PublishRequest(BaseModel):
    channel: str
    payload: dict = Field(default_factory=dict)


class PublishResponse(BaseModel):
    message_id: str
    sequence: int


async def _auth_http(db: AsyncSession, authorization: str | None):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    try:
        return await authenticate_token(db, authorization.removeprefix("Bearer "))
    except AuthenticationError as e:
        raise HTTPException(status_code=401, detail=str(e)) from e


@app.post("/v1/publish", response_model=PublishResponse)
async def publish(
    body: PublishRequest,
    authorization: str | None = Header(default=None),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    db: AsyncSession = Depends(get_db),
) -> PublishResponse:
    auth = await _auth_http(db, authorization)

    try:
        validate_channel_name(body.channel)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    if not channel_belongs_to_tenant(body.channel, auth.tenant_id):
        raise HTTPException(status_code=403, detail="CHANNEL_FORBIDDEN")

    size = len(json.dumps(body.payload).encode())
    if size > settings.max_message_size_bytes:
        raise HTTPException(status_code=413, detail="MESSAGE_TOO_LARGE")

    r = get_redis()

    # Idempotent publish (Section 79): a retried POST with the same key
    # returns the original message instead of creating a second one.
    if idempotency_key:
        try:
            cached = await r.get(RedisKeys.idempotency(auth.tenant_id, idempotency_key))
        except RedisConnectionError as e:
            raise HTTPException(status_code=503, detail="REDIS_UNAVAILABLE") from e
        if cached:
            data = json.loads(cached)
            return PublishResponse(**data)

    try:
        result = await publish_message(r, body.channel, body.payload)
    except RedisConnectionError as e:
        # Section 87: surface the real failure instead of pretending the
        # message was accepted. Publishers should retry with their
        # Idempotency-Key.
        raise HTTPException(status_code=503, detail="REDIS_UNAVAILABLE") from e
    METRICS["relay_messages_received_total"] += 1

    if idempotency_key:
        await r.set(
            RedisKeys.idempotency(auth.tenant_id, idempotency_key),
            json.dumps({"message_id": result.message_id, "sequence": result.sequence}),
            ex=settings.idempotency_ttl_seconds,
        )

    return PublishResponse(message_id=result.message_id, sequence=result.sequence)


@app.get("/v1/presence/{user_id}")
async def get_presence(
    user_id: str,
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Presence for one user, scoped to the caller's tenant (Section 47)."""
    auth = await _auth_http(db, authorization)
    return await presence.get_presence(get_redis(), auth.tenant_id, user_id)


@app.get("/v1/presence")
async def list_presence(
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    auth = await _auth_http(db, authorization)
    return await presence.list_online(get_redis(), auth.tenant_id)


# --- Operational API (Section 75) ------------------------------------------------


@app.get("/health/live")
async def health_live() -> dict:
    """Liveness: the process itself is running. Deliberately does NOT
    check Redis — a transient Redis problem should not cause the
    orchestrator to kill a process that is otherwise healthy
    (Section 75)."""
    return {"status": "alive", "node_id": settings.node_id}


@app.get("/health/ready")
async def health_ready(response: Response) -> dict:
    """Readiness: checks the dependencies required to serve new
    connections."""
    checks = {}
    try:
        await get_redis().ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {type(e).__name__}"

    ready = all(v == "ok" for v in checks.values()) and not getattr(app.state, "draining", False)
    if not ready:
        response.status_code = 503
    return {"ready": ready, "checks": checks, "draining": getattr(app.state, "draining", False)}


@app.get("/metrics")
async def metrics() -> Response:
    METRICS["relay_connections_active"] = registry.active_count()
    METRICS["relay_queue_depth"] = sum(c.queue_depth() for c in registry.all_connections())
    METRICS["relay_queue_bytes"] = sum(c.queue_bytes() for c in registry.all_connections())
    METRICS["relay_presence_users"] = len(
        {c.user_id for c in registry.all_connections() if c.user_id}
    )
    return Response(content=render_metrics(), media_type="text/plain; version=0.0.4")

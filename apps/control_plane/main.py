"""Control plane (Section 68, 74).

Manages durable configuration: tenants, API keys, channels, rooms. It
never sits in the hot path of message delivery — the gateway reads what
it needs at connection time and caches nothing beyond the request.
"""
from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.gateway.nodes.registration import NodeRegistrar
from packages.database.engine import get_db
from packages.database.models import ApiKey, Channel, Room, Tenant
from packages.observability.logging import configure_logging
from packages.redis.client import close_redis, get_redis
from packages.security.api_keys import generate_api_key


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    yield
    await close_redis()


app = FastAPI(title="Relay Control Plane", version="0.1.0", lifespan=lifespan)


class TenantCreate(BaseModel):
    name: str


class TenantOut(BaseModel):
    id: str
    name: str
    status: str


@app.post("/admin/tenants", response_model=TenantOut, status_code=201)
async def create_tenant(body: TenantCreate, db: AsyncSession = Depends(get_db)) -> TenantOut:
    tenant = Tenant(name=body.name)
    db.add(tenant)
    await db.commit()
    await db.refresh(tenant)
    return TenantOut(id=str(tenant.id), name=tenant.name, status=tenant.status)


@app.get("/admin/tenants", response_model=list[TenantOut])
async def list_tenants(db: AsyncSession = Depends(get_db)) -> list[TenantOut]:
    rows = (await db.execute(select(Tenant))).scalars().all()
    return [TenantOut(id=str(t.id), name=t.name, status=t.status) for t in rows]


class ApiKeyCreate(BaseModel):
    tenant_id: str


class ApiKeyOut(BaseModel):
    id: str
    tenant_id: str
    key_prefix: str
    api_key: str | None = None  # returned exactly once, on creation


@app.post("/admin/api-keys", response_model=ApiKeyOut, status_code=201)
async def create_api_key(body: ApiKeyCreate, db: AsyncSession = Depends(get_db)) -> ApiKeyOut:
    tenant = await db.get(Tenant, body.tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="tenant not found")

    plaintext, prefix, key_hash = generate_api_key()
    key = ApiKey(tenant_id=tenant.id, key_prefix=prefix, key_hash=key_hash)
    db.add(key)
    await db.commit()
    await db.refresh(key)
    return ApiKeyOut(id=str(key.id), tenant_id=str(key.tenant_id), key_prefix=prefix, api_key=plaintext)


@app.delete("/admin/api-keys/{key_id}", status_code=204)
async def revoke_api_key(key_id: str, db: AsyncSession = Depends(get_db)) -> None:
    key = await db.get(ApiKey, key_id)
    if key is None:
        raise HTTPException(status_code=404, detail="api key not found")
    key.status = "revoked"
    await db.commit()


class ChannelCreate(BaseModel):
    tenant_id: str
    name: str
    ordering_enabled: bool = True
    retention_seconds: int = 300


@app.post("/admin/channels", status_code=201)
async def create_channel(body: ChannelCreate, db: AsyncSession = Depends(get_db)) -> dict:
    channel = Channel(
        tenant_id=body.tenant_id,
        name=body.name,
        ordering_enabled=body.ordering_enabled,
        retention_seconds=body.retention_seconds,
    )
    db.add(channel)
    await db.commit()
    await db.refresh(channel)
    return {"id": str(channel.id), "name": channel.name, "tenant_id": str(channel.tenant_id)}


@app.get("/admin/channels")
async def list_channels(db: AsyncSession = Depends(get_db)) -> list[dict]:
    rows = (await db.execute(select(Channel))).scalars().all()
    return [{"id": str(c.id), "name": c.name, "tenant_id": str(c.tenant_id)} for c in rows]


class RoomCreate(BaseModel):
    tenant_id: str
    name: str


@app.post("/admin/rooms", status_code=201)
async def create_room(body: RoomCreate, db: AsyncSession = Depends(get_db)) -> dict:
    room = Room(tenant_id=body.tenant_id, name=body.name)
    db.add(room)
    await db.commit()
    await db.refresh(room)
    return {"id": str(room.id), "name": room.name, "tenant_id": str(room.tenant_id)}


@app.get("/admin/rooms")
async def list_rooms(db: AsyncSession = Depends(get_db)) -> list[dict]:
    rows = (await db.execute(select(Room))).scalars().all()
    return [{"id": str(r.id), "name": r.name, "tenant_id": str(r.tenant_id)} for r in rows]


@app.get("/admin/nodes")
async def list_nodes() -> list[dict]:
    """Live node view, read from Redis TTL keys (Section 54-55). A node
    that stopped heartbeating simply disappears from this list."""
    return await NodeRegistrar.list_nodes(get_redis())


@app.get("/health/live")
async def live() -> dict:
    return {"status": "alive"}


@app.get("/health/ready")
async def ready(db: AsyncSession = Depends(get_db)) -> dict:
    from sqlalchemy import text

    await db.execute(text("SELECT 1"))
    return {"ready": True}

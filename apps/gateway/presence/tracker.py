"""Presence (Section 47-49).

Presence is derived from connection state, not asserted by clients, and
it is stored as TTL-bounded Redis state rather than process memory — a
node that crashes cannot send "offline" events, so absence of a heartbeat
has to be what marks a user offline (ADR-009).

The subtlety is Section 49: a user may hold several connections at once
(two browser tabs, a phone and a laptop). Closing one must not mark them
offline while another is live. So presence tracks a *set* of connection
ids per user:

    relay:presence:{tenant_id}:{user_id}  ->  SET of connection_id

The key carries a TTL refreshed by heartbeat. Explicit disconnects remove
their own member; an unclean node death removes nothing, and the TTL
takes care of it.

States (Section 47):
    ONLINE   at least one live connection
    IDLE     connections exist but the client reported inactivity
    OFFLINE  no live connections (key expired or emptied)
"""
from __future__ import annotations

from enum import StrEnum

import redis.asyncio as redis

from packages.common.config import settings


class PresenceState(StrEnum):
    ONLINE = "ONLINE"
    IDLE = "IDLE"
    OFFLINE = "OFFLINE"


def _key(tenant_id: str, user_id: str) -> str:
    return f"relay:presence:{tenant_id}:{user_id}"


def _state_key(tenant_id: str, user_id: str) -> str:
    return f"relay:presence:{tenant_id}:{user_id}:state"


async def connection_online(r: redis.Redis, tenant_id: str, user_id: str, connection_id: str) -> None:
    """Register a connection as live for this user."""
    key = _key(tenant_id, user_id)
    pipe = r.pipeline()
    pipe.sadd(key, connection_id)
    pipe.expire(key, settings.presence_ttl_seconds)
    await pipe.execute()


async def refresh(r: redis.Redis, tenant_id: str, user_id: str) -> None:
    """Heartbeat: extend the TTL. If heartbeats stop — including because
    the whole node died — the key expires and the user goes OFFLINE."""
    key = _key(tenant_id, user_id)
    pipe = r.pipeline()
    pipe.expire(key, settings.presence_ttl_seconds)
    pipe.expire(_state_key(tenant_id, user_id), settings.presence_ttl_seconds)
    await pipe.execute()


async def connection_offline(r: redis.Redis, tenant_id: str, user_id: str, connection_id: str) -> bool:
    """Deregister one connection. Returns True if the user is now fully
    offline (no remaining connections) — this is the Section 49 race: the
    answer must be False while any sibling connection is still live."""
    key = _key(tenant_id, user_id)
    await r.srem(key, connection_id)
    remaining = await r.scard(key)
    if remaining == 0:
        await r.delete(key, _state_key(tenant_id, user_id))
        return True
    return False


async def set_state(r: redis.Redis, tenant_id: str, user_id: str, state: PresenceState) -> None:
    """Clients may report IDLE (e.g. tab in background). ONLINE/OFFLINE
    are derived from connections and are not client-settable."""
    if state is PresenceState.IDLE:
        await r.set(_state_key(tenant_id, user_id), state.value, ex=settings.presence_ttl_seconds)
    else:
        await r.delete(_state_key(tenant_id, user_id))


async def get_presence(r: redis.Redis, tenant_id: str, user_id: str) -> dict:
    key = _key(tenant_id, user_id)
    connections = await r.scard(key)
    if connections == 0:
        return {"user_id": user_id, "state": PresenceState.OFFLINE, "connections": 0}

    reported = await r.get(_state_key(tenant_id, user_id))
    state = PresenceState.IDLE if reported == PresenceState.IDLE.value else PresenceState.ONLINE
    return {"user_id": user_id, "state": state, "connections": connections}


async def list_online(r: redis.Redis, tenant_id: str) -> list[dict]:
    """Tenant-scoped presence listing. Uses SCAN, never KEYS, so a large
    tenant cannot block Redis."""
    out: list[dict] = []
    prefix = f"relay:presence:{tenant_id}:"
    async for key in r.scan_iter(match=f"{prefix}*", count=100):
        if key.endswith(":state"):
            continue
        user_id = key[len(prefix):]
        out.append(await get_presence(r, tenant_id, user_id))
    return out

"""Session layer (Section 9).

A session outlives any one WebSocket connection. It is identified by
`session_id`, tracked in Redis with a TTL equal to the resume window
(Section 18), and carries per-channel ack positions so reconnection can
resume from the right place (Section 19-20).
"""
from __future__ import annotations

import time
import uuid

import redis.asyncio as redis

from packages.common.config import settings
from packages.redis.client import RedisKeys


class SessionExpiredError(Exception):
    pass


async def create_session(r: redis.Redis, tenant_id: str, node_id: str) -> str:
    session_id = str(uuid.uuid4())
    key = RedisKeys.session(session_id)
    now = time.time()
    await r.hset(
        key,
        mapping={
            "tenant_id": tenant_id,
            "current_node": node_id,
            "created_at": now,
            "last_seen": now,
        },
    )
    await r.expire(key, settings.session_resume_window_seconds)
    return session_id


async def touch_session(r: redis.Redis, session_id: str, node_id: str) -> None:
    key = RedisKeys.session(session_id)
    await r.hset(key, mapping={"last_seen": time.time(), "current_node": node_id})
    await r.expire(key, settings.session_resume_window_seconds)


async def get_session(r: redis.Redis, session_id: str) -> dict | None:
    key = RedisKeys.session(session_id)
    data = await r.hgetall(key)
    return data or None


async def record_ack(r: redis.Redis, session_id: str, channel: str, sequence: int) -> None:
    """Cumulative ack (Section 24): only advance, never move backwards."""
    key = RedisKeys.session_ack(session_id, channel)
    current = await r.get(key)
    if current is None or int(current) < sequence:
        await r.set(key, sequence, ex=settings.session_resume_window_seconds)


async def get_ack(r: redis.Redis, session_id: str, channel: str) -> int:
    val = await r.get(RedisKeys.session_ack(session_id, channel))
    return int(val) if val is not None else 0

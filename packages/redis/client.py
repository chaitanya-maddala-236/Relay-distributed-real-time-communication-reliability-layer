"""Redis access layer.

Centralizes key naming (Sections 70-73) so the rest of the codebase never
hand-builds Redis keys. Everything here uses TTLs or trimming — nothing is
allowed to grow unbounded (Section 95).
"""
from __future__ import annotations

import time
from typing import Any

import redis.asyncio as redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from packages.common.config import settings


class RedisKeys:
    """Single source of truth for Redis key shapes."""

    @staticmethod
    def connection(connection_id: str) -> str:
        return f"relay:connection:{connection_id}"

    @staticmethod
    def session(session_id: str) -> str:
        return f"relay:session:{session_id}"

    @staticmethod
    def session_ack(session_id: str, channel: str) -> str:
        return f"relay:session:{session_id}:ack:{channel}"

    @staticmethod
    def channel_stream(channel: str) -> str:
        return f"relay:messages:{channel}"

    @staticmethod
    def channel_sequence(channel: str) -> str:
        return f"relay:sequence:{channel}"

    @staticmethod
    def node(node_id: str) -> str:
        return f"relay:nodes:{node_id}"

    @staticmethod
    def presence(user_key: str) -> str:
        return f"relay:presence:{user_key}"

    @staticmethod
    def dedup(channel: str, message_id: str) -> str:
        return f"relay:dedup:{channel}:{message_id}"

    @staticmethod
    def idempotency(tenant_id: str, key: str) -> str:
        return f"relay:idempotency:{tenant_id}:{key}"

    @staticmethod
    def room_nodes(room: str) -> str:
        return f"relay:room:{room}:nodes"

    @staticmethod
    def pubsub_channel(channel: str) -> str:
        return f"relay:pubsub:{channel}"

    @staticmethod
    def rate_limit(scope: str, key: str) -> str:
        return f"relay:ratelimit:{scope}:{key}"


_pool: redis.Redis | None = None


def get_redis() -> redis.Redis:
    """Shared client for this process.

    `health_check_interval` and the retry policy matter after a Redis
    restart: pooled connections are dead but look reusable, so the first
    command on each one fails. Without this, readiness stayed 503 for
    several seconds after Redis recovered even though Redis was healthy
    (observed in tests/chaos/redis_outage_observe.py).
    """
    global _pool
    if _pool is None:
        _pool = redis.from_url(
            settings.redis_url,
            decode_responses=True,
            health_check_interval=10,
            socket_connect_timeout=2,
            socket_keepalive=True,
            retry=Retry(ExponentialBackoff(base=0.05, cap=1.0), retries=3),
            retry_on_error=[RedisConnectionError, RedisTimeoutError],
        )
    return _pool


async def close_redis() -> None:
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None


async def allocate_sequence(r: redis.Redis, channel: str) -> int:
    """Atomically allocate the next sequence number for a channel
    (Section 80). A Redis INCR is used instead of a local counter because
    multiple gateway processes may publish to the same channel."""
    return await r.incr(RedisKeys.channel_sequence(channel))


async def append_to_channel_buffer(
    r: redis.Redis, channel: str, message_id: str, sequence: int, payload: dict[str, Any]
) -> None:
    """Append a message to the channel's bounded recovery buffer
    (Section 27-29). Uses a Redis Stream trimmed by both length and age."""
    import json

    stream_key = RedisKeys.channel_stream(channel)
    # Exact (not approximate) MAXLEN trimming. Redis' approximate mode only
    # trims at macro-node boundaries, so a stream configured for 10 entries
    # can legitimately hold ~100. That is fine for memory safety but not for
    # gap detection: Relay's RESUME_GAP contract depends on knowing exactly
    # what is still retained, so we pay for exact trimming here.
    await r.xadd(
        stream_key,
        {
            "message_id": message_id,
            "sequence": sequence,
            "payload": json.dumps(payload),
            "created_at": str(time.time()),
        },
        maxlen=settings.channel_buffer_max_messages,
        approximate=False,
    )
    # Belt-and-suspenders time-based trim so retention_seconds is honored
    # even if maxlen alone would keep older entries around.
    min_id = int((time.time() - settings.message_retention_seconds) * 1000)
    await r.xtrim(stream_key, minid=min_id, approximate=True)

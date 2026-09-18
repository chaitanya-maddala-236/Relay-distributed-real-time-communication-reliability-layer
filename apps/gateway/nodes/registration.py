"""Node registration and dead-node detection (Section 54-55).

Each gateway process writes `relay:nodes:{node_id}` with a short TTL and
refreshes it on a timer. If a node dies, the key expires and other nodes
stop seeing it — no explicit deregistration handshake required, which is
exactly the behavior needed for an unclean crash (Section 89).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time

import redis.asyncio as redis

from packages.common.config import settings
from packages.redis.client import RedisKeys


class NodeRegistrar:
    def __init__(self, node_id: str, r: redis.Redis) -> None:
        self.node_id = node_id
        self.r = r
        self._task: asyncio.Task | None = None
        self._status = "active"

    async def _write(self) -> None:
        from apps.gateway.connections.manager import registry

        await self.r.set(
            RedisKeys.node(self.node_id),
            json.dumps(
                {
                    "node_id": self.node_id,
                    "status": self._status,
                    "connections": registry.active_count(),
                    "updated_at": time.time(),
                }
            ),
            ex=settings.node_ttl_seconds,
        )

    async def _loop(self) -> None:
        while True:
            with contextlib.suppress(Exception):
                await self._write()
            await asyncio.sleep(settings.node_heartbeat_interval_seconds)

    async def start(self) -> None:
        with contextlib.suppress(Exception):
            await self._write()
        self._task = asyncio.create_task(self._loop())

    async def mark_draining(self) -> None:
        self._status = "draining"
        with contextlib.suppress(Exception):
            await self._write()

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        with contextlib.suppress(Exception):
            await self.r.delete(RedisKeys.node(self.node_id))

    @staticmethod
    async def list_nodes(r: redis.Redis) -> list[dict]:
        nodes = []
        async for key in r.scan_iter(match="relay:nodes:*"):
            raw = await r.get(key)
            if raw:
                nodes.append(json.loads(raw))
        return nodes

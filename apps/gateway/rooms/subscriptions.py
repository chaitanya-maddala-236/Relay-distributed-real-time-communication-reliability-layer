"""Distributed room/channel membership (Section 50-52, 71).

Each Relay node runs at most one Redis Pub/Sub listener task per channel
that has local subscribers, no matter how many local connections are on
it. Redis also gets a note of which nodes currently care about a channel
(`relay:room:{channel}:nodes`), which is useful for observability and
future targeted-publish optimizations, even though the current publish
path just fans out to Pub/Sub unconditionally.
"""
from __future__ import annotations

import asyncio

import redis.asyncio as redis

from apps.gateway.fanout.publisher import run_pubsub_listener
from packages.common.config import settings
from packages.redis.client import RedisKeys


class ChannelSubscriptionManager:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self._listeners: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}
        self._local_subscriber_counts: dict[str, int] = {}

    async def on_local_subscribe(self, r: redis.Redis, channel: str) -> None:
        self._local_subscriber_counts[channel] = self._local_subscriber_counts.get(channel, 0) + 1
        await r.sadd(RedisKeys.room_nodes(channel), self.node_id)
        await r.expire(RedisKeys.room_nodes(channel), settings.node_ttl_seconds * 3)

        # Start a listener if there isn't one, or if the previous one died
        # (a crashed listener that stays in the dict would leave this node
        # subscribed to nothing — see the chaos test for how that shows up).
        existing = self._listeners.get(channel)
        if existing is None or existing[0].done():
            stop_event = asyncio.Event()
            task = asyncio.create_task(run_pubsub_listener(r, channel, stop_event))
            self._listeners[channel] = (task, stop_event)

    async def on_local_unsubscribe(self, r: redis.Redis, channel: str) -> None:
        count = self._local_subscriber_counts.get(channel, 0) - 1
        if count <= 0:
            self._local_subscriber_counts.pop(channel, None)
            if channel in self._listeners:
                task, stop_event = self._listeners.pop(channel)
                stop_event.set()
                task.cancel()
            await r.srem(RedisKeys.room_nodes(channel), self.node_id)
        else:
            self._local_subscriber_counts[channel] = count

    async def shutdown(self) -> None:
        for task, stop_event in self._listeners.values():
            stop_event.set()
            task.cancel()
        self._listeners.clear()


subscription_manager: ChannelSubscriptionManager | None = None


def get_subscription_manager() -> ChannelSubscriptionManager:
    global subscription_manager
    if subscription_manager is None:
        from packages.common.config import settings as s

        subscription_manager = ChannelSubscriptionManager(node_id=s.node_id)
    return subscription_manager

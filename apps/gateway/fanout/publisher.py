"""Messaging + fanout layer (Section 10, 30-31, 41-43, 80).

`publish_message` is the single path every message takes, whether it
originates from a WebSocket client or the HTTP publish API:

  1. allocate a monotonically increasing sequence number for the channel
     (server-side, via Redis INCR — never a local counter, Section 80)
  2. append to the channel's bounded recovery buffer (Redis Stream)
  3. announce it on the channel's Redis Pub/Sub topic for live fanout
     across all Relay nodes (Section 30)
  4. deliver to this node's local subscribers directly (avoids a
     round-trip through Redis for the common case)

Ordering (Section 43): sequence assignment happens once, before fanout,
so every subscriber sees the same order for a given channel regardless
of which node they're connected to.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from dataclasses import dataclass

import redis.asyncio as redis

from apps.gateway.connections.manager import ConnectionState, Priority, SlowConsumerError, registry
from packages.observability.logging import log_event
from packages.observability.metrics import METRICS
from packages.protocol.frames import DisconnectFrame, ErrorCode, MessageFrame
from packages.redis.client import RedisKeys, allocate_sequence, append_to_channel_buffer

logger = logging.getLogger("relay.gateway.fanout")


@dataclass
class PublishResult:
    message_id: str
    sequence: int


async def publish_message(r: redis.Redis, channel: str, payload: dict) -> PublishResult:
    message_id = str(uuid.uuid4())
    sequence = await allocate_sequence(r, channel)
    await append_to_channel_buffer(r, channel, message_id, sequence, payload)

    frame = MessageFrame(
        message_id=message_id,
        channel=channel,
        sequence=sequence,
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        payload=payload,
    )
    envelope = frame.model_dump_json()

    # Live fanout to every node (including this one) via Pub/Sub.
    await r.publish(RedisKeys.pubsub_channel(channel), envelope)

    return PublishResult(message_id=message_id, sequence=sequence)


async def terminate_slow_consumer(conn) -> None:
    """Enforce the slow-client policy (Section 38). The queue is already
    at its bound, so we cannot enqueue a DISCONNECT frame through the
    normal path — we write the close reason directly and drop the socket.
    The session is left intact so the client can reconnect and RESUME."""
    conn.state = ConnectionState.DEAD
    METRICS["relay_slow_consumers_total"] += 1
    METRICS["relay_disconnects_total"] += 1
    try:
        await asyncio.wait_for(
            conn.websocket.send_text(
                DisconnectFrame(reason=ErrorCode.SLOW_CONSUMER).model_dump_json()
            ),
            timeout=1.0,
        )
    except Exception:
        pass
    try:
        await conn.websocket.close(code=4409)
    except Exception:
        pass
    log_event(
        logger,
        logging.WARNING,
        "slow_consumer_disconnected",
        connection_id=conn.connection_id,
        queue_depth=conn.queue_depth(),
        queue_bytes=conn.queue_bytes(),
    )


async def deliver_local(channel: str, envelope_json: str, sequence: int | None = None) -> list[str]:
    """Deliver an already-serialized MESSAGE frame to every connection on
    this node subscribed to `channel`. Each connection has its own bounded
    queue (Section 42), so one slow consumer never blocks the others — it
    is disconnected out-of-band while delivery to everyone else continues
    without waiting."""
    disconnected: list[str] = []
    fanout_size = 0
    for conn in registry.local_subscribers(channel):
        if conn.state != ConnectionState.ACTIVE:
            continue
        fanout_size += 1
        try:
            await conn.enqueue(
                envelope_json,
                priority=Priority.NORMAL,
                channel=channel,
                sequence=sequence,
            )
            METRICS["relay_messages_delivered_total"] += 1
        except SlowConsumerError:
            disconnected.append(conn.connection_id)
            # Terminate out-of-band: fanout to the remaining subscribers
            # must not wait on a socket that is already backed up.
            asyncio.create_task(terminate_slow_consumer(conn))
    METRICS["relay_fanout_size"] = fanout_size
    return disconnected


async def run_pubsub_listener(r: redis.Redis, channel: str, stop_event) -> None:
    """One supervised task per subscribed channel, per node, translating
    Redis Pub/Sub messages into local per-connection queue deliveries.

    Redis Pub/Sub subscriptions do not survive a Redis restart or a
    dropped connection, and a bare `async for` over `pubsub.listen()`
    simply ends when that happens — leaving the node silently subscribed
    to nothing. (Observed directly in tests/chaos/redis_outage_observe.py:
    before this supervision loop existed, delivery never recovered after
    Redis came back.) So the listener reconnects with capped backoff until
    it is explicitly stopped.
    """
    backoff = 0.25
    while not stop_event.is_set():
        pubsub = None
        try:
            pubsub = r.pubsub()
            await pubsub.subscribe(RedisKeys.pubsub_channel(channel))
            backoff = 0.25  # reset after a successful (re)subscribe
            log_event(logger, logging.INFO, "pubsub_subscribed", channel=channel)

            async for message in pubsub.listen():
                if stop_event.is_set():
                    break
                if message["type"] != "message":
                    continue
                try:
                    seq = json.loads(message["data"]).get("sequence")
                except Exception:
                    seq = None
                await deliver_local(channel, message["data"], sequence=seq)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log_event(
                logger, logging.WARNING, "pubsub_listener_reconnecting",
                channel=channel, error=type(e).__name__, retry_in_seconds=backoff,
            )
        finally:
            if pubsub is not None:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()

        if stop_event.is_set():
            return
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 5.0)

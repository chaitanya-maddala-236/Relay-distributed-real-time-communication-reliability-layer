"""Session resumption replay (Section 19-20, 44-45).

Given a client's last-acknowledged sequence for a channel, replay any
buffered messages after that point. If the requested sequence is older
than what the bounded recovery buffer still holds, report RESUME_GAP
rather than silently skipping messages (Section 45: "Do not pretend
recovery succeeded").
"""
from __future__ import annotations

import json
from dataclasses import dataclass

import redis.asyncio as redis

from packages.redis.client import RedisKeys


@dataclass
class ReplayResult:
    messages: list[dict]
    gap: bool  # True if the oldest available message is newer than requested


async def replay_channel(r: redis.Redis, channel: str, last_ack_sequence: int) -> ReplayResult:
    stream_key = RedisKeys.channel_stream(channel)
    entries = await r.xrange(stream_key, min="-", max="+")

    if not entries:
        return ReplayResult(messages=[], gap=False)

    parsed = []
    for _entry_id, fields in entries:
        parsed.append(
            {
                "message_id": fields["message_id"],
                "sequence": int(fields["sequence"]),
                "payload": json.loads(fields["payload"]),
                "created_at": float(fields["created_at"]),
            }
        )

    parsed.sort(key=lambda m: m["sequence"])
    oldest_available = parsed[0]["sequence"]

    # A gap exists if the client's next-expected message (ack+1) is older
    # than anything we still retain — i.e. we cannot prove nothing was
    # missed in between.
    gap = last_ack_sequence + 1 < oldest_available and last_ack_sequence != 0

    to_replay = [m for m in parsed if m["sequence"] > last_ack_sequence]
    return ReplayResult(messages=to_replay, gap=gap)

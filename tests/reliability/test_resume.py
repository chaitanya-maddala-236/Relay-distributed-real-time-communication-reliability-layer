"""Resume / recovery-buffer tests (Section 27-28, 44-45, 108).

These run against a real Redis instance. The property under test is the
honest one: when the recovery buffer no longer holds what a client asks
for, Relay must report a gap rather than silently returning a partial
stream that looks complete.
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from apps.gateway.recovery.replay import replay_channel
from apps.gateway.sessions.manager import create_session, get_ack, get_session, record_ack
from packages.common.config import settings
from packages.redis.client import RedisKeys, allocate_sequence, append_to_channel_buffer


@pytest_asyncio.fixture
async def r():
    """A fresh client per test: the module-level singleton binds to the
    event loop that created it, and pytest-asyncio gives each test its
    own loop."""
    import redis.asyncio as redis_asyncio

    client = redis_asyncio.from_url(
        settings.redis_url, decode_responses=True, max_connections=250
    )
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("Redis not available")
    yield client
    await client.aclose()


@pytest_asyncio.fixture
def channel() -> str:
    return f"test:{uuid.uuid4().hex[:8]}"


async def _publish(r, channel: str, n: int, start_payload: int = 0) -> list[int]:
    seqs = []
    for i in range(n):
        seq = await allocate_sequence(r, channel)
        await append_to_channel_buffer(r, channel, f"msg-{seq}", seq, {"i": start_payload + i})
        seqs.append(seq)
    return seqs


@pytest.mark.asyncio
async def test_replay_returns_only_messages_after_last_ack(r, channel) -> None:
    seqs = await _publish(r, channel, 10)
    result = await replay_channel(r, channel, last_ack_sequence=seqs[4])
    assert [m["sequence"] for m in result.messages] == seqs[5:]
    assert result.gap is False


@pytest.mark.asyncio
async def test_replay_preserves_order(r, channel) -> None:
    await _publish(r, channel, 50)
    result = await replay_channel(r, channel, last_ack_sequence=0)
    seqs = [m["sequence"] for m in result.messages]
    assert seqs == sorted(seqs)


@pytest.mark.asyncio
async def test_fully_acked_channel_replays_nothing(r, channel) -> None:
    seqs = await _publish(r, channel, 5)
    result = await replay_channel(r, channel, last_ack_sequence=seqs[-1])
    assert result.messages == []
    assert result.gap is False


@pytest.mark.asyncio
async def test_gap_detected_when_buffer_evicted_past_client_position(r, channel, monkeypatch) -> None:
    """Section 45: client asks for sequence 500, oldest retained is 700 ->
    RESUME_GAP, not a silent partial replay."""
    monkeypatch.setattr(settings, "channel_buffer_max_messages", 10)
    await _publish(r, channel, 60)  # buffer trimmed; early messages evicted

    result = await replay_channel(r, channel, last_ack_sequence=2)
    assert result.gap is True, "client is behind the retained window; must report a gap"


@pytest.mark.asyncio
async def test_no_gap_reported_when_client_is_within_retention(r, channel, monkeypatch) -> None:
    monkeypatch.setattr(settings, "channel_buffer_max_messages", 100)
    seqs = await _publish(r, channel, 20)
    result = await replay_channel(r, channel, last_ack_sequence=seqs[10])
    assert result.gap is False
    assert len(result.messages) == 9


@pytest.mark.asyncio
async def test_empty_channel_replays_nothing_without_claiming_a_gap(r, channel) -> None:
    result = await replay_channel(r, channel, last_ack_sequence=0)
    assert result.messages == []
    assert result.gap is False


@pytest.mark.asyncio
async def test_buffer_is_bounded(r, channel, monkeypatch) -> None:
    """Section 28/95: the recovery buffer must never grow without bound."""
    monkeypatch.setattr(settings, "channel_buffer_max_messages", 20)
    await _publish(r, channel, 500)
    length = await r.xlen(RedisKeys.channel_stream(channel))
    # Redis MAXLEN ~ is approximate, so allow headroom but assert it is
    # nowhere near the 500 published.
    assert length < 200, f"buffer grew to {length}; trimming is not working"


# --- session + cumulative ack state (Section 24, 46) ------------------------


@pytest.mark.asyncio
async def test_session_created_with_ttl(r) -> None:
    session_id = await create_session(r, tenant_id="tenant-1", node_id="relay-node-a")
    ttl = await r.ttl(RedisKeys.session(session_id))
    assert 0 < ttl <= settings.session_resume_window_seconds
    data = await get_session(r, session_id)
    assert data["tenant_id"] == "tenant-1"


@pytest.mark.asyncio
async def test_ack_position_advances_but_never_regresses(r) -> None:
    session_id = await create_session(r, "tenant-1", "relay-node-a")
    await record_ack(r, session_id, "chat", 100)
    await record_ack(r, session_id, "chat", 150)
    assert await get_ack(r, session_id, "chat") == 150
    await record_ack(r, session_id, "chat", 120)  # late/duplicate ack
    assert await get_ack(r, session_id, "chat") == 150


@pytest.mark.asyncio
async def test_ack_positions_are_per_channel(r) -> None:
    session_id = await create_session(r, "tenant-1", "relay-node-a")
    await record_ack(r, session_id, "chat", 100)
    await record_ack(r, session_id, "orders", 5)
    assert await get_ack(r, session_id, "chat") == 100
    assert await get_ack(r, session_id, "orders") == 5


@pytest.mark.asyncio
async def test_unknown_session_returns_none(r) -> None:
    assert await get_session(r, "does-not-exist") is None


@pytest.mark.asyncio
async def test_sequence_allocation_is_monotonic_under_concurrency(r, channel) -> None:
    """Section 80: concurrent publishers must not collide on a sequence."""
    import asyncio

    results = await asyncio.gather(*[allocate_sequence(r, channel) for _ in range(200)])
    assert len(set(results)) == 200, "duplicate sequence numbers allocated"
    assert sorted(results) == list(range(1, 201))

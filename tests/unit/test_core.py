"""Unit tests (Section 105) — pure logic, no external services required."""
from __future__ import annotations

import asyncio

import pytest
from pydantic import ValidationError

from apps.gateway.connections.manager import (
    Connection,
    ConnectionRegistry,
    ConnectionState,
    Priority,
    SlowConsumerError,
)
from packages.common.config import settings
from packages.protocol.frames import (
    AckFrame,
    MessageFrame,
    SubscribeFrame,
    validate_channel_name,
)
from packages.security.api_keys import generate_api_key, verify_api_key

# --- channel validation (Section 33) -----------------------------------------


@pytest.mark.parametrize("channel", ["chat:room-42", "notifications:user-123", "events:order-456"])
def test_valid_channel_names_accepted(channel: str) -> None:
    assert validate_channel_name(channel) == channel


@pytest.mark.parametrize(
    "channel",
    [
        "",                    # empty
        "a" * 500,             # oversized
        "room\n42",            # control character
        "room 42",             # space
        "room\x00null",        # null byte
        "room*glob",           # redis glob pattern
        "room:{injection}",    # brace injection
    ],
)
def test_unsafe_channel_names_rejected(channel: str) -> None:
    with pytest.raises(ValueError):
        validate_channel_name(channel)


def test_subscribe_frame_rejects_unsafe_channel() -> None:
    with pytest.raises(ValidationError):
        SubscribeFrame(channel="bad channel!")


# --- envelope (Section 10) ---------------------------------------------------


def test_message_envelope_requires_core_fields() -> None:
    frame = MessageFrame(
        message_id="m-1", channel="chat:room-42", sequence=1042,
        timestamp="2026-09-17T12:00:00Z", payload={"text": "hello"},
    )
    data = frame.model_dump()
    for field in ("message_id", "channel", "sequence", "type", "payload"):
        assert field in data


def test_ack_frame_is_cumulative_by_sequence() -> None:
    ack = AckFrame(channel="chat:room-42", sequence=1050)
    assert ack.sequence == 1050


# --- API keys (Section 13, 92) ----------------------------------------------


def test_api_key_is_hashed_not_stored_plaintext() -> None:
    plaintext, prefix, key_hash = generate_api_key()
    assert plaintext not in key_hash
    assert len(key_hash) == 64
    assert plaintext.startswith(prefix)
    assert verify_api_key(plaintext, key_hash)


def test_wrong_api_key_rejected() -> None:
    _, _, key_hash = generate_api_key()
    assert not verify_api_key("relay_wrong_key", key_hash)


def test_api_keys_are_unique() -> None:
    keys = {generate_api_key()[0] for _ in range(100)}
    assert len(keys) == 100


# --- connection queue bounds + flow control (Section 37-39) -----------------


class _FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_text(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self, code: int = 1000) -> None:
        pass


def _make_connection() -> Connection:
    return Connection(
        connection_id="conn-test", session_id="sess-test", tenant_id="tenant-test",
        websocket=_FakeWebSocket(), state=ConnectionState.ACTIVE,
    )


@pytest.mark.asyncio
async def test_queue_is_bounded_and_raises_slow_consumer(monkeypatch) -> None:
    monkeypatch.setattr(settings, "connection_queue_max_messages", 5)
    conn = _make_connection()
    for i in range(5):
        await conn.enqueue(f'{{"n":{i}}}', channel="c", sequence=i)
    with pytest.raises(SlowConsumerError):
        await conn.enqueue('{"n":"overflow"}', channel="c", sequence=99)


@pytest.mark.asyncio
async def test_byte_bound_enforced_independently(monkeypatch) -> None:
    monkeypatch.setattr(settings, "connection_queue_max_messages", 1000)
    monkeypatch.setattr(settings, "connection_queue_max_bytes", 100)
    conn = _make_connection()
    with pytest.raises(SlowConsumerError):
        for _ in range(50):
            await conn.enqueue("x" * 30)


@pytest.mark.asyncio
async def test_low_priority_dropped_instead_of_disconnecting(monkeypatch) -> None:
    """Ephemeral frames are droppable under pressure (Section 39-40)."""
    monkeypatch.setattr(settings, "connection_queue_max_messages", 2)
    conn = _make_connection()
    await conn.enqueue("a", channel="c", sequence=1)
    await conn.enqueue("b", channel="c", sequence=2)
    await conn.enqueue("typing", priority=Priority.LOW)  # must not raise
    assert conn.messages_dropped == 1


@pytest.mark.asyncio
async def test_high_priority_control_frame_bypasses_bound(monkeypatch) -> None:
    monkeypatch.setattr(settings, "connection_queue_max_messages", 1)
    conn = _make_connection()
    await conn.enqueue("msg", channel="c", sequence=1)
    await conn.enqueue("PING", priority=Priority.HIGH)  # must not raise
    assert conn.queue_depth() == 2


@pytest.mark.asyncio
async def test_cumulative_ack_releases_window(monkeypatch) -> None:
    """A single ACK at N releases everything <= N (Section 24)."""
    monkeypatch.setattr(settings, "max_inflight_messages", 10)
    conn = _make_connection()
    conn._inflight = [("chat", i) for i in range(1, 6)]
    released = conn.on_ack("chat", 3)
    assert released == 3
    assert conn.inflight_count() == 2


@pytest.mark.asyncio
async def test_ack_on_one_channel_does_not_release_another() -> None:
    conn = _make_connection()
    conn._inflight = [("chat", 1), ("chat", 2), ("orders", 1)]
    conn.on_ack("chat", 5)
    assert conn._inflight == [("orders", 1)]


@pytest.mark.asyncio
async def test_duplicate_ack_is_harmless() -> None:
    conn = _make_connection()
    conn._inflight = [("chat", 1), ("chat", 2)]
    assert conn.on_ack("chat", 2) == 2
    assert conn.on_ack("chat", 2) == 0  # replayed ack releases nothing further


@pytest.mark.asyncio
async def test_sender_stops_at_window_and_resumes_on_ack(monkeypatch) -> None:
    """The core flow-control property: an unacknowledging client stops
    receiving once the window is full, and resumes the moment it acks."""
    monkeypatch.setattr(settings, "max_inflight_messages", 3)
    monkeypatch.setattr(settings, "connection_queue_max_messages", 100)
    conn = _make_connection()
    ws: _FakeWebSocket = conn.websocket  # type: ignore[assignment]

    for i in range(1, 11):
        await conn.enqueue(f'{{"seq":{i}}}', channel="chat", sequence=i)

    task = asyncio.create_task(conn.run_sender())
    await asyncio.sleep(0.05)
    assert len(ws.sent) == 3, f"window should cap delivery at 3, got {len(ws.sent)}"

    conn.on_ack("chat", 3)
    await asyncio.sleep(0.05)
    assert len(ws.sent) == 6, f"acking 3 should release exactly 3 more, got {len(ws.sent)}"

    task.cancel()


# --- registry (Section 8, 14, 42) -------------------------------------------


def test_registry_isolates_subscribers_per_channel() -> None:
    reg = ConnectionRegistry()
    a, b = _make_connection(), _make_connection()
    b.connection_id = "conn-b"
    reg.add(a)
    reg.add(b)
    reg.subscribe(a.connection_id, "chat:one")
    reg.subscribe(b.connection_id, "chat:two")
    assert [c.connection_id for c in reg.local_subscribers("chat:one")] == ["conn-test"]
    assert [c.connection_id for c in reg.local_subscribers("chat:two")] == ["conn-b"]


def test_registry_removal_cleans_up_all_indexes() -> None:
    reg = ConnectionRegistry()
    conn = _make_connection()
    reg.add(conn)
    reg.subscribe(conn.connection_id, "chat:one")
    assert reg.active_count() == 1
    reg.remove(conn.connection_id)
    assert reg.active_count() == 0
    assert reg.local_subscribers("chat:one") == []
    assert reg.tenant_connection_count("tenant-test") == 0


def test_tenant_connection_counting() -> None:
    reg = ConnectionRegistry()
    for i in range(3):
        c = _make_connection()
        c.connection_id = f"conn-{i}"
        reg.add(c)
    assert reg.tenant_connection_count("tenant-test") == 3

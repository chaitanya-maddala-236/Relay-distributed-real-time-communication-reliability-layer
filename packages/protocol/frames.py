"""Relay client protocol (Section 11, documented fully in docs/protocol.md).

Every frame the client and server exchange over the WebSocket is one of
these types. Frames are JSON objects; binary payloads are supported
separately (Section 61) and are out of scope for this schema layer.
"""
from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

MAX_CHANNEL_LENGTH = 200
_CHANNEL_SAFE_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:_-."
)


class FrameType(StrEnum):
    CONNECT = "CONNECT"
    CONNECTED = "CONNECTED"
    PING = "PING"
    PONG = "PONG"
    SUBSCRIBE = "SUBSCRIBE"
    SUBSCRIBED = "SUBSCRIBED"
    UNSUBSCRIBE = "UNSUBSCRIBE"
    UNSUBSCRIBED = "UNSUBSCRIBED"
    MESSAGE = "MESSAGE"
    ACK = "ACK"
    NACK = "NACK"
    RESUME = "RESUME"
    RESUMED = "RESUMED"
    ERROR = "ERROR"
    DISCONNECT = "DISCONNECT"


def validate_channel_name(channel: str) -> str:
    """Reject unsafe channel names (Section 33). Raises ValueError."""
    if not channel:
        raise ValueError("channel must not be empty")
    if len(channel) > MAX_CHANNEL_LENGTH:
        raise ValueError(f"channel exceeds max length {MAX_CHANNEL_LENGTH}")
    if not set(channel).issubset(_CHANNEL_SAFE_CHARS):
        raise ValueError("channel contains unsafe characters")
    return channel


class ConnectFrame(BaseModel):
    type: Literal[FrameType.CONNECT] = FrameType.CONNECT
    token: str
    session_id: str | None = None


class ConnectedFrame(BaseModel):
    type: Literal[FrameType.CONNECTED] = FrameType.CONNECTED
    connection_id: str
    session_id: str
    node_id: str
    heartbeat_interval_seconds: float


class PingFrame(BaseModel):
    type: Literal[FrameType.PING] = FrameType.PING
    server_time: str


class PongFrame(BaseModel):
    type: Literal[FrameType.PONG] = FrameType.PONG


class SubscribeFrame(BaseModel):
    type: Literal[FrameType.SUBSCRIBE] = FrameType.SUBSCRIBE
    channel: str

    @field_validator("channel")
    @classmethod
    def _validate(cls, v: str) -> str:
        return validate_channel_name(v)


class SubscribedFrame(BaseModel):
    type: Literal[FrameType.SUBSCRIBED] = FrameType.SUBSCRIBED
    channel: str


class UnsubscribeFrame(BaseModel):
    type: Literal[FrameType.UNSUBSCRIBE] = FrameType.UNSUBSCRIBE
    channel: str


class UnsubscribedFrame(BaseModel):
    type: Literal[FrameType.UNSUBSCRIBED] = FrameType.UNSUBSCRIBED
    channel: str


class MessageFrame(BaseModel):
    """The application message envelope (Section 10)."""

    type: Literal[FrameType.MESSAGE] = FrameType.MESSAGE
    message_id: str
    channel: str
    sequence: int
    timestamp: str
    payload: dict[str, Any] = Field(default_factory=dict)


class AckFrame(BaseModel):
    type: Literal[FrameType.ACK] = FrameType.ACK
    channel: str
    sequence: int  # cumulative ack (Section 24): all <= sequence are acked


class NackFrame(BaseModel):
    type: Literal[FrameType.NACK] = FrameType.NACK
    channel: str
    message_id: str
    reason: str


class ResumeFrame(BaseModel):
    type: Literal[FrameType.RESUME] = FrameType.RESUME
    session_id: str
    last_ack_by_channel: dict[str, int] = Field(default_factory=dict)


class ResumedFrame(BaseModel):
    type: Literal[FrameType.RESUMED] = FrameType.RESUMED
    session_id: str
    replayed_by_channel: dict[str, int] = Field(default_factory=dict)
    gaps: list[str] = Field(default_factory=list)  # channels where full replay wasn't possible


class ErrorFrame(BaseModel):
    type: Literal[FrameType.ERROR] = FrameType.ERROR
    code: str
    message: str


class DisconnectFrame(BaseModel):
    type: Literal[FrameType.DISCONNECT] = FrameType.DISCONNECT
    reason: str


# Error codes used across the gateway (kept as constants so tests and docs
# reference the same strings — Section 126).
class ErrorCode:
    AUTH_FAILED = "AUTH_FAILED"
    CHANNEL_FORBIDDEN = "CHANNEL_FORBIDDEN"
    INVALID_CHANNEL = "INVALID_CHANNEL"
    INVALID_FRAME = "INVALID_FRAME"
    MESSAGE_TOO_LARGE = "MESSAGE_TOO_LARGE"
    NOT_CONNECTED = "NOT_CONNECTED"
    RESUME_WINDOW_EXPIRED = "RESUME_WINDOW_EXPIRED"
    RESUME_GAP = "RESUME_GAP"
    SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
    SLOW_CONSUMER = "SLOW_CONSUMER"
    RATE_LIMITED = "RATE_LIMITED"
    UNKNOWN_MESSAGE = "UNKNOWN_MESSAGE"

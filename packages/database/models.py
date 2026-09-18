"""Control-plane data model (Section 69).

Only durable configuration lives in Postgres: tenants, api keys, channel
config, rooms, and session *records* (not every ephemeral heartbeat -
Section 46, 72). Hot-path connection/session liveness lives in Redis.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)

    api_keys: Mapped[list[ApiKey]] = relationship(back_populates="tenant")
    channels: Mapped[list[Channel]] = relationship(back_populates="tenant")
    rooms: Mapped[list[Room]] = relationship(back_populates="tenant")


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    key_prefix: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    tenant: Mapped[Tenant] = relationship(back_populates="api_keys")


class Channel(Base):
    __tablename__ = "channels"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    ordering_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    delivery_mode: Mapped[str] = mapped_column(String(32), default="AT_LEAST_ONCE")
    retention_seconds: Mapped[int] = mapped_column(Integer, default=300)
    max_message_size: Mapped[int] = mapped_column(Integer, default=1_000_000)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    tenant: Mapped[Tenant] = relationship(back_populates="channels")


class Room(Base):
    __tablename__ = "rooms"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    tenant: Mapped[Tenant] = relationship(back_populates="rooms")


class SessionRecord(Base):
    """Durable session record for audit/analytics. Live session state
    (last_seen, current_node, ack positions) lives in Redis; this row is
    the control-plane-visible record (Section 72)."""

    __tablename__ = "sessions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active")

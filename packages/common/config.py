"""Central runtime configuration for Relay.

All tunables referenced throughout the PRD (heartbeat intervals, retention
windows, queue limits, etc.) live here as a single Pydantic settings object
so every module reads from one source of truth and every limit is
configurable via environment variables (12-factor style).
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RELAY_", env_file=".env", extra="ignore")

    # Identity
    node_id: str = "relay-node-a"
    environment: str = "development"

    # Postgres (control plane)
    database_url: str = "postgresql+asyncpg://relay:relay@localhost:5432/relay"

    # Redis (distributed coordination / data plane)
    redis_url: str = "redis://localhost:6379/0"

    # Heartbeat (Section 16)
    heartbeat_interval_seconds: float = 15.0
    heartbeat_timeout_seconds: float = 10.0

    # Session (Section 18)
    session_resume_window_seconds: int = 300  # 5 minutes

    # Message retention / recovery buffer (Section 27-28)
    message_retention_seconds: int = 300
    channel_buffer_max_messages: int = 1000
    channel_buffer_max_bytes: int = 5_000_000

    # Backpressure (Section 37-38)
    connection_queue_max_messages: int = 1000
    connection_queue_max_bytes: int = 2_000_000
    connection_queue_high_watermark_ratio: float = 0.8
    # Delivered-but-unacknowledged window per connection. This is the
    # primary flow-control mechanism (see connections/manager.py).
    max_inflight_messages: int = 100

    # ACK / retry (Section 25, 82-83)
    ack_timeout_seconds: float = 10.0
    max_retry_attempts: int = 3
    retry_base_delay_seconds: float = 0.25

    # Connection limits (Section 35)
    max_connections_per_tenant: int = 10_000
    max_connections_per_api_key: int = 1_000
    max_handshakes_in_flight: int = 500

    # Message limits (Section 60)
    max_message_size_bytes: int = 1_000_000

    # Presence (Section 48)
    presence_ttl_seconds: int = 30

    # Node registration (Section 54)
    node_ttl_seconds: int = 15
    node_heartbeat_interval_seconds: float = 5.0

    # Dedup window (Section 26)
    dedup_window_seconds: int = 300

    # Idempotency (Section 79)
    idempotency_ttl_seconds: int = 600


settings = Settings()

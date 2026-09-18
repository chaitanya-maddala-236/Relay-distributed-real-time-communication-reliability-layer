"""Authentication (Section 13) and channel authorization (Section 34).

Authentication proves *who* is connecting (which tenant/API key).
Authorization is a separate check on top: being authenticated does not
imply the right to subscribe to any given channel.
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from packages.database.models import ApiKey, Tenant
from packages.security.api_keys import hash_api_key


@dataclass
class AuthContext:
    tenant_id: str
    api_key_id: str


class AuthenticationError(Exception):
    pass


async def authenticate_token(db: AsyncSession, token: str) -> AuthContext:
    if not token:
        raise AuthenticationError("missing token")

    key_hash = hash_api_key(token)
    result = await db.execute(
        select(ApiKey, Tenant)
        .join(Tenant, ApiKey.tenant_id == Tenant.id)
        .where(ApiKey.key_hash == key_hash, ApiKey.status == "active", Tenant.status == "active")
    )
    row = result.first()
    if row is None:
        raise AuthenticationError("invalid or revoked API key")

    api_key, tenant = row
    return AuthContext(tenant_id=str(tenant.id), api_key_id=str(api_key.id))


def channel_belongs_to_tenant(channel: str, tenant_id: str) -> bool:
    """Enforce tenant isolation on channel names (Section 14, 34).

    Channels are namespaced by tenant: `t-<tenant_id>:<logical-name>`.
    A connection may only subscribe within its own tenant namespace.
    This keeps authorization a pure function of the channel string
    instead of a database round trip on every SUBSCRIBE.
    """
    return channel.startswith(f"t-{tenant_id}:")


def namespaced_channel(tenant_id: str, logical_name: str) -> str:
    return f"t-{tenant_id}:{logical_name}"

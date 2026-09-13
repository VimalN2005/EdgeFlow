from dataclasses import dataclass
from typing import Optional
import jwt
from fastapi import HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from edgeflow.config import settings

bearer_scheme = HTTPBearer(auto_error=False)


@dataclass
class ClientIdentity:
    """Represents an authenticated caller identity and tier."""
    client_id: str
    tenant_id: str
    tier: str  # 'free', 'pro', 'enterprise'
    role: str  # 'user', 'admin'

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


def decode_jwt_token(token: str) -> Optional[ClientIdentity]:
    """Verify and decode a JWT bearer token."""
    try:
        payload = jwt.decode(
            token,
            settings.gateway_secret_key,
            algorithms=[settings.jwt_algorithm],
        )
        return ClientIdentity(
            client_id=str(payload.get("sub", "unknown-user")),
            tenant_id=str(payload.get("tenant_id", "default-tenant")),
            tier=str(payload.get("tier", "free")).lower(),
            role=str(payload.get("role", "user")).lower(),
        )
    except jwt.PyJWTError:
        return None


async def authenticate_request(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
) -> ClientIdentity:
    """Authenticate request using either X-API-Key header or Bearer JWT token."""
    # 1. Check X-API-Key Header
    api_key = request.headers.get(settings.api_key_header)
    if api_key:
        key_data = settings.api_keys.get(api_key)
        if key_data:
            return ClientIdentity(
                client_id=api_key[:8] + "...",
                tenant_id=key_data.get("tenant_id", "tenant-default"),
                tier=key_data.get("tier", "free").lower(),
                role=key_data.get("role", "user").lower(),
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API Key provided.",
            )

    # 2. Check Bearer Token (via FastAPI security credentials or direct header)
    token = None
    if isinstance(credentials, HTTPAuthorizationCredentials) and credentials.credentials:
        token = credentials.credentials
    else:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()

    if token:
        # First check if token matches direct secret key
        if token == settings.gateway_secret_key:
            return ClientIdentity(
                client_id="secret-bearer-user",
                tenant_id="admin-tenant",
                tier="enterprise",
                role="admin",
            )
        # Decode JWT
        identity = decode_jwt_token(token)
        if identity:
            return identity
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired Bearer Token.",
        )

    # 3. Allow anonymous caller as 'free' tier
    client_ip = request.client.host if request.client else "anonymous"
    return ClientIdentity(
        client_id=f"anon-{client_ip}",
        tenant_id="public",
        tier="free",
        role="guest",
    )

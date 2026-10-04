import secrets
from dataclasses import dataclass

from starlette.datastructures import Headers

from atlas.config import Settings

PUBLIC_PATHS = frozenset(
    {"/health", "/ready", "/metrics", "/docs", "/docs/oauth2-redirect", "/openapi.json"}
)
WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


@dataclass(frozen=True)
class AuthorizationFailure:
    status_code: int
    detail: str


def authorize_request(
    settings: Settings,
    *,
    method: str,
    path: str,
    headers: Headers,
) -> AuthorizationFailure | None:
    """Authorize an API request without ever logging or returning credential material."""

    if not settings.auth_enabled or path in PUBLIC_PATHS or method.upper() == "OPTIONS":
        return None

    candidate = _credential(headers)
    if candidate is None:
        return AuthorizationFailure(401, "API credential required")

    read_key = settings.api_read_key
    write_key = settings.api_write_key
    if read_key is None or write_key is None:  # guarded by Settings validation
        return AuthorizationFailure(503, "API authentication is misconfigured")

    write_matches = secrets.compare_digest(candidate, write_key.get_secret_value())
    if method.upper() in WRITE_METHODS:
        return None if write_matches else AuthorizationFailure(403, "Write credential required")

    read_matches = secrets.compare_digest(candidate, read_key.get_secret_value())
    return (
        None
        if read_matches or write_matches
        else AuthorizationFailure(403, "Invalid API credential")
    )


def _credential(headers: Headers) -> str | None:
    explicit = headers.get("x-atlas-api-key")
    if explicit:
        return explicit
    authorization = headers.get("authorization", "")
    scheme, _, value = authorization.partition(" ")
    if scheme.lower() == "bearer" and value:
        return value
    return None

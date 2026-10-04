import pytest
from pydantic import ValidationError
from starlette.datastructures import Headers

from atlas.api.security import authorize_request
from atlas.config import Settings


def secured_settings() -> Settings:
    return Settings(
        auth_enabled=True,
        api_read_key="read-secret-value",
        api_write_key="write-secret-value",
        _env_file=None,
    )


def test_authentication_requires_distinct_configured_keys() -> None:
    with pytest.raises(ValidationError, match="required when auth is enabled"):
        Settings(auth_enabled=True, _env_file=None)

    with pytest.raises(ValidationError, match="must be different"):
        Settings(
            auth_enabled=True,
            api_read_key="same-secret",
            api_write_key="same-secret",
            _env_file=None,
        )


def test_read_key_can_read_but_cannot_submit_scenario() -> None:
    settings = secured_settings()
    headers = Headers({"X-Atlas-API-Key": "read-secret-value"})

    assert authorize_request(settings, method="GET", path="/v1/scenarios", headers=headers) is None
    failure = authorize_request(settings, method="POST", path="/v1/scenarios", headers=headers)

    assert failure is not None
    assert failure.status_code == 403
    assert failure.detail == "Write credential required"


def test_write_key_can_read_and_write_with_bearer_header() -> None:
    settings = secured_settings()
    headers = Headers({"Authorization": "Bearer write-secret-value"})

    assert authorize_request(settings, method="GET", path="/v1/scenarios", headers=headers) is None
    assert authorize_request(settings, method="POST", path="/v1/scenarios", headers=headers) is None


def test_missing_and_invalid_credentials_have_distinct_responses() -> None:
    settings = secured_settings()

    missing = authorize_request(settings, method="GET", path="/v1/scenarios", headers=Headers())
    invalid = authorize_request(
        settings,
        method="GET",
        path="/v1/scenarios",
        headers=Headers({"X-Atlas-API-Key": "not-a-valid-key"}),
    )

    assert missing is not None and missing.status_code == 401
    assert invalid is not None and invalid.status_code == 403


def test_operational_endpoints_remain_available_to_internal_probes() -> None:
    settings = secured_settings()

    for path in ("/health", "/ready", "/metrics"):
        assert authorize_request(settings, method="GET", path=path, headers=Headers()) is None

import jwt
import pytest

from dal_obscura.infrastructure.adapters.identity_default import AuthConfig, DefaultIdentityAdapter

JWT_SECRET = "test-jwt-secret-32-characters-long"


def _bearer_token(subject: str = "user1") -> str:
    token = jwt.encode({"sub": subject}, JWT_SECRET, algorithm="HS256")
    return f"Bearer {token}"


def test_auth_jwt_from_authorization_header():
    config = AuthConfig(jwt_secret=JWT_SECRET)
    result = DefaultIdentityAdapter(config).authenticate({"authorization": _bearer_token()})
    assert result.id == "user1"


def test_auth_rejects_non_bearer_authorization_header():
    config = AuthConfig(jwt_secret=JWT_SECRET)
    token = jwt.encode({"sub": "user1"}, JWT_SECRET, algorithm="HS256")

    with pytest.raises(PermissionError):
        DefaultIdentityAdapter(config).authenticate({"authorization": f"Token {token}"})


def test_default_identity_adapter_accepts_provider_style_constructor_kwargs():
    adapter = DefaultIdentityAdapter(jwt_secret=JWT_SECRET)

    result = adapter.authenticate({"authorization": _bearer_token("provider-user")})

    assert result.id == "provider-user"

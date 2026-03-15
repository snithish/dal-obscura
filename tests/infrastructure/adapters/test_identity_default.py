from dal_obscura.infrastructure.adapters.identity_default import AuthConfig, DefaultIdentityAdapter


def test_auth_api_key():
    config = AuthConfig(api_keys={"key": "user1"})
    result = DefaultIdentityAdapter(config).authenticate({"authorization": "ApiKey key"})
    assert result.id == "user1"

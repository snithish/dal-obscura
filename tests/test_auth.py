from dal_obscura.auth import AuthConfig, authenticate


def test_auth_api_key():
    config = AuthConfig(api_keys={"key": "user1"})
    result = authenticate({"authorization": "ApiKey key"}, config)
    assert result.principal_id == "user1"

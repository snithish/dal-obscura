from __future__ import annotations

from collections.abc import Mapping


class FakeSecretProvider:
    def __init__(
        self,
        *,
        database_url: str,
        config: Mapping[str, object],
        secrets: Mapping[str, str],
    ) -> None:
        self.database_url = database_url
        self.config = dict(config)
        self.secrets = dict(secrets)

    def get_secret(self, key: str) -> str | None:
        if key == "missing":
            return None
        return f"{self.config['prefix']}:{self.secrets['token']}:{key}"


class NotASecretProvider:
    pass

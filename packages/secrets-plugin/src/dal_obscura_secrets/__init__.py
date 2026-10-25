"""Environment and mounted-file backends; the host authorizes references."""

import os
import re
import stat
from collections.abc import Mapping
from contextlib import ExitStack
from pathlib import Path

from dal_obscura_plugin_api import ExecutionContext
from dal_obscura_plugin_api.descriptors import load_packaged_descriptor
from dal_obscura_plugin_api.execution import active

_MOUNTED_SECRET_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}\Z")
MAX_MOUNTED_SECRET_BYTES = 64 * 1024


class EnvironmentBackend:
    descriptor = load_packaged_descriptor(
        "dal-obscura-secrets", __package__, "secret_backend", "environment"
    )

    """Secret provider that reads secrets from environment variables."""

    def __init__(
        self,
        *,
        config: Mapping[str, object] | None = None,
    ) -> None:
        if config and set(config) - {"prefix"}:
            raise ValueError("Environment secret configuration supports prefix only")
        raw_prefix = config.get("prefix") if config is not None else None
        self._prefix = str(raw_prefix if raw_prefix is not None else "")

    def get_secret(self, key: str) -> str | None:
        return os.getenv(f"{self._prefix}{key}")

    def close(self) -> None:
        pass


class MountedFileBackend:
    descriptor = load_packaged_descriptor(
        "dal-obscura-secrets", __package__, "secret_backend", "mounted_file"
    )

    """Read scoped flat files from a fixed operator-owned POSIX mount."""

    def __init__(self, *, config: Mapping[str, object]) -> None:
        if set(config) - {"directory"}:
            raise ValueError("Mounted secret configuration supports directory only")
        directory = config.get("directory")
        if not isinstance(directory, str) or not Path(directory).is_absolute():
            raise ValueError("Mounted secret directory must be an absolute directory")
        if os.open not in os.supports_dir_fd or not all(
            hasattr(os, flag) for flag in ("O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK")
        ):
            raise ValueError("Mounted secrets require secure POSIX file operations")
        try:
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                root = os.fstat(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            raise ValueError("Mounted secret directory is unavailable") from None
        self._directory = Path(directory)
        self._root_identity = (root.st_dev, root.st_ino)

    def get_secret(self, key: str) -> str | None:
        if not _MOUNTED_SECRET_NAME.fullmatch(key):
            return None
        try:
            with ExitStack() as opened:
                directory_fd = os.open(
                    self._directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                )
                opened.callback(os.close, directory_fd)
                root = os.fstat(directory_fd)
                if (root.st_dev, root.st_ino) != self._root_identity:
                    return None
                secret_fd = os.open(
                    key, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd
                )
                opened.callback(os.close, secret_fd)
                info = os.fstat(secret_fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink > 1:
                    return None
                payload = bytearray()
                while len(payload) <= MAX_MOUNTED_SECRET_BYTES:
                    chunk = os.read(secret_fd, MAX_MOUNTED_SECRET_BYTES + 1 - len(payload))
                    if not chunk:
                        break
                    payload.extend(chunk)
                if not payload or len(payload) > MAX_MOUNTED_SECRET_BYTES:
                    return None
                return payload.decode("utf-8")
        except OSError, UnicodeError:
            return None

    def close(self) -> None:
        pass


@active
def environment_factory(
    options: Mapping[str, object], context: ExecutionContext
) -> EnvironmentBackend:
    return EnvironmentBackend(config=options)


@active
def mounted_file_factory(
    options: Mapping[str, object], context: ExecutionContext
) -> MountedFileBackend:
    return MountedFileBackend(config=options)

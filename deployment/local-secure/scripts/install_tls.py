"""Copy only each consumer's certificates with container-native ownership."""

import os
import shutil
from pathlib import Path

for name, uid, files in (
    ("keycloak", 1000, ("keycloak.crt", "keycloak.key")),
    ("flight", 10001, ("flight.crt", "flight.key")),
    ("edge", 10001, ("ui.crt", "ui.key", "ca.crt")),
    (
        "client",
        10001,
        ("client.crt", "client.key", "ca.crt", "untrusted-client.crt", "untrusted-client.key"),
    ),
):
    directory = Path(f"/{name}-tls")
    os.chown(directory, uid, uid)
    directory.chmod(0o700)
    for filename in files:
        target = directory / filename
        shutil.copyfile(Path("/source") / filename, target)
        os.chown(target, uid, uid)
        target.chmod(0o400 if filename.endswith(".key") else 0o444)

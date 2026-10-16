"""Copied example scripts and executable command seams never touch live demo state."""

import json
import os
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest


@pytest.fixture
def demo_process(tmp_path):
    repository = tmp_path / "repo"
    for source in (
        "examples/demo/keycloak/scripts/demo.py",
        "deployment/local-secure/scripts/demo.py",
    ):
        destination = repository / source
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    binaries = tmp_path / "bin"
    binaries.mkdir()
    docker = binaries / "docker"
    docker.write_text(
        f"#!{sys.executable}\nimport json, os, sys\n"
        "with open(os.environ['TEST_DOCKER_LOG'], 'a') as log:\n"
        "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "raise SystemExit(int(os.environ.get('TEST_DOCKER_EXIT', '0')))\n"
    )
    docker.chmod(0o755)
    log = tmp_path / "docker.jsonl"

    def create(*, secure=False, failure=False):
        script = repository / (
            "deployment/local-secure/scripts/demo.py"
            if secure
            else "examples/demo/keycloak/scripts/demo.py"
        )
        environment = {
            **os.environ,
            "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
            "TEST_DOCKER_LOG": str(log),
            "TEST_DOCKER_EXIT": "1" if failure else "0",
        }

        def run(*arguments):
            return subprocess.run(
                [sys.executable, str(script), *arguments],
                cwd=repository,
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
            )

        def commands():
            return [json.loads(line) for line in log.read_text().splitlines()]

        return SimpleNamespace(
            root=script.parent.parent, run=run, commands=commands, environment=environment
        )

    return create

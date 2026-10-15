"""Operator argument parsing and command dispatch."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from dal_obscura.interfaces.cli import (
    deployment,
    plugin_ops,
)


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(
        description="dal-obscura operator commands; no arguments starts Flight"
    )
    cli.add_argument("--workspace", type=Path, default=Path(".dal-obscura-deploy"))
    commands = cli.add_subparsers(dest="command", required=True)
    targets = commands.add_parser("target", help="Initialize an operator deployment target")
    target_actions = targets.add_subparsers(dest="action", required=True)
    initialize = target_actions.add_parser(
        "init", help="Collect target settings without secret values"
    )
    from dal_obscura.interfaces.cli.target import TARGET_FIELDS

    initialize.add_argument(
        "--non-interactive", action="store_true", help="Require every target field through flags"
    )
    for name, label in TARGET_FIELDS:
        initialize.add_argument("--" + name.replace("_", "-"), help=label)
    for name, label in (
        ("env-file", "Existing Compose environment file; only its path is stored"),
        ("project-name", "Existing Compose project name"),
        ("ca-file", "Flight CA certificate path"),
        ("cert-file", "Flight client certificate path"),
        ("key-file", "Flight client private-key path; only its path is stored"),
    ):
        initialize.add_argument("--" + name, help=label)
    initialize.add_argument("--admin-token-env", default="DAL_OBSCURA_DEPLOY_ADMIN_TOKEN")
    initialize.add_argument("--flight-token-env", default="DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN")
    diagnostics = commands.add_parser(
        "doctor", help="Check deployment prerequisites without changing services"
    )
    diagnostics.add_argument(
        "--offline",
        action="store_true",
        help="Check files and references without network or containers",
    )
    diagnostics.add_argument(
        "--json", action="store_true", help="Emit a versioned machine-readable report"
    )
    plugins = commands.add_parser("plugin", help="Prepare plugin admission and catalog settings")
    actions = plugins.add_subparsers(dest="action", required=True)
    add = actions.add_parser("add", help="Install a package in an isolated worker image")
    add.add_argument("package")
    configure = actions.add_parser(
        "configure", help="Prepare catalog options and secret references"
    )
    configure.add_argument("plugin_id")
    configure.add_argument("--name")
    configure.add_argument("--options", type=Path)
    configure.add_argument("--expected-revision", type=int)
    deployment = commands.add_parser(
        "deploy", help="Publish immutable image/lock and verify Compose workers"
    )
    deployment.add_argument(
        "--prepare", action="store_true", help="Build/review release without publishing"
    )
    deployment.add_argument(
        "--yes", action="store_true", help="Approve the printed admission for automation"
    )
    return cli


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if not arguments:
        from dal_obscura.interfaces.cli.read import main as serve

        serve()
        return 0
    if arguments in (["--help"], ["-h"]):
        from dal_obscura.interfaces.cli.read import _HELP_TEXT

        print(_HELP_TEXT)
        parser().print_help()
        return 0
    args = parser().parse_args(arguments)
    try:
        if args.command in {"target", "doctor"}:
            from dal_obscura.interfaces.cli.target import diagnose_target, initialize_target

            if args.command == "doctor":
                return diagnose_target(args.workspace, args)
            initialize_target(args.workspace, args)
        elif args.command == "deploy":
            deployment.deploy(args.workspace, args)
        elif args.action == "add":
            plugin_ops.add_plugin(args.workspace, args.package)
        else:
            plugin_ops.configure_plugin(args.workspace, args)
    except (
        OSError,
        ValueError,
        KeyError,
        EOFError,
        subprocess.SubprocessError,
    ) as exc:
        # Captured subprocess stderr and HTTP bodies may contain secrets; do not echo them.
        if isinstance(exc, subprocess.SubprocessError):
            message = "Container command failed; release was not marked verified"
        elif isinstance(exc, KeyError):
            message = "Missing deployment setting or credential environment variable"
        elif isinstance(exc, EOFError):
            message = "Configuration input ended; no target was saved"
        else:
            message = str(exc)
        print(message, file=sys.stderr)
        return 1
    return 0

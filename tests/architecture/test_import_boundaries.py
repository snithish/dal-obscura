"""Dependency rules apply to every AST import, including deferred/type-only edges."""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

_ROOT = Path("src/dal_obscura")
_PREFIX = "dal_obscura."
_CONCRETE = ("adapters", "identity", "plugins", "runtime")


@dataclass(frozen=True)
class ImportEdge:
    source: str
    target: str
    name: str
    line: int


def _module(path: Path) -> str:
    parts = path.relative_to(_ROOT.parent).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def _imports(
    source: str, module: str, modules: set[str], *, package: bool = False
) -> list[ImportEdge]:
    edges = []
    parent = module if package else module.rpartition(".")[0]
    tree = ast.parse(source)
    dynamic_names = {"__import__"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "importlib":
            dynamic_names.update(
                alias.asname or alias.name for alias in node.names if alias.name == "import_module"
            )
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            edges.extend(ImportEdge(module, alias.name, "", node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                parts = parent.split(".")
                base = ".".join(parts[: len(parts) - node.level + 1])
                base += "." + node.module if node.module else ""
            else:
                base = node.module or ""
            for alias in node.names:
                child = base + "." + alias.name
                edges.append(
                    ImportEdge(
                        module,
                        child
                        if any(name == child or name.startswith(child + ".") for name in modules)
                        else base,
                        alias.name,
                        node.lineno,
                    )
                )
        elif isinstance(node, ast.Call) and (
            (isinstance(node.func, ast.Name) and node.func.id in dynamic_names)
            or (isinstance(node.func, ast.Attribute) and node.func.attr == "import_module")
        ):
            if (
                node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                edges.append(ImportEdge(module, node.args[0].value, "", node.lineno))
            else:
                edges.append(ImportEdge(module, "<dynamic module>", "", node.lineno))
    return edges


def _within(target: str, owner: str) -> bool:
    return target == _PREFIX + owner or target.startswith(_PREFIX + owner + ".")


def _kernel_violation(edge: ImportEdge) -> bool:
    source, target = edge.source, edge.target
    if target != "dal_obscura" and not target.startswith(_PREFIX):
        root = target.split(".", 1)[0]
        if root in sys.stdlib_module_names | {"__future__"}:
            return False
        if _within(source, "kernel.identity"):
            return True
        return not (
            target == "pyarrow"
            or root == "sqlglot"
            or target.startswith(("dal_obscura_plugin_api.paths", "dal_obscura_plugin_api.schema"))
        )
    for owner, allowed in (
        ("kernel.identity", ("kernel.identity",)),
        ("kernel.policy", ("kernel.policy", "kernel.identity")),
        ("kernel.plans", ("kernel.plans", "kernel.policy", "kernel.identity")),
    ):
        if _within(source, owner):
            return not any(_within(target, item) for item in allowed)
    return target != "dal_obscura.kernel"


def _violation(edge: ImportEdge) -> bool:
    source, target = edge.source, edge.target
    root = target.split(".", 1)[0]
    if target == "<dynamic module>":
        # Provider admission uses fixed identity identifiers and locked entry points,
        # not an unchecked import expression that escapes this dependency graph.
        return True
    if source.startswith(_PREFIX + "kernel."):
        return _kernel_violation(edge)
    if _within(source, "application.reads") or _within(source, "application.governance"):
        if target != "dal_obscura" and not target.startswith(_PREFIX):
            return root in {"sqlalchemy", "fastapi", "duckdb", "uvicorn"}
        owner = (
            "application.reads"
            if _within(source, "application.reads")
            else "application.governance"
        )
        if owner == "application.governance" and target == _PREFIX + "application.reads.ports":
            return edge.name not in {"ProjectionCompiler", "RowTransformPort"}
        return not (_within(target, owner) or _within(target, "kernel"))
    if any(_within(source, item) for item in _CONCRETE):
        return _within(target, "interfaces") or _within(target, "bootstrap")
    if _within(source, "interfaces"):
        if target == "dal_obscura":
            return edge.name != "__version__"
        if target != "dal_obscura" and not target.startswith(_PREFIX):
            return root in {"sqlalchemy", "duckdb"}
        if _within(target, "bootstrap"):
            return not _within(source, "interfaces.cli")
        return not any(
            _within(target, item)
            for item in (
                "interfaces",
                "kernel",
                "application",
                "flight",
                "observability",
                "logging_config",
            )
        )
    return False


@pytest.mark.parametrize(
    "statement",
    [
        "from dal_obscura.adapters.sql import snapshots",
        "from dal_obscura import adapters",
        "from dal_obscura.adapters import sql",
        "def later():\n    import dal_obscura.adapters.sql.snapshots",
        (
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n"
            "    from dal_obscura.adapters.sql.snapshots import LiveConfigStore"
        ),
        'import importlib\nimportlib.import_module("dal_obscura.adapters.sql.snapshots")',
        "def later(name):\n    return __import__(name)",
        'from importlib import import_module as load\nload("dal_obscura.adapters.sql.snapshots")',
        "from importlib import import_module as load\nload(configured_name)",
    ],
)
def test_application_rejects_every_form_of_concrete_import(statement: str) -> None:
    modules = {"dal_obscura.adapters.sql.snapshots"}
    edges = _imports(statement, "dal_obscura.application.reads.service", modules)
    assert any(_violation(edge) for edge in edges)


def test_relative_and_package_imports_resolve_actual_module_owner() -> None:
    modules = {"dal_obscura.application.reads.ports", "dal_obscura.kernel.identity.actor"}
    source = "dal_obscura.application.governance.ports"
    assert (
        _imports("from ..reads import ports", source, modules)[0].target
        == "dal_obscura.application.reads.ports"
    )
    edge = _imports("from dal_obscura.kernel.identity import actor", source, modules)[0]
    assert edge.target == "dal_obscura.kernel.identity.actor"
    assert not _violation(edge)
    package = _imports("from . import actor", "dal_obscura.kernel.identity", modules, package=True)[
        0
    ]
    assert package.target == edge.target


@pytest.mark.parametrize(
    ("source", "target"),
    [
        ("kernel.identity.actor", "kernel.policy.models"),
        ("kernel.policy.models", "kernel.plans.projection"),
        ("kernel.plans.governed", "application.reads.service"),
        ("application.reads.service", "application.governance.ports"),
        ("application.governance.assets.service", "application.reads.service"),
        ("adapters.providers.catalogs", "interfaces.http.app"),
        ("identity.sessions", "interfaces.cli.control"),
        ("plugins.registry", "bootstrap.control"),
        ("runtime.configuration", "interfaces.http.app"),
        ("interfaces.http.routes.assets", "adapters.sql.unit_of_work"),
        ("interfaces.flight.server", "bootstrap.read"),
    ],
)
def test_forbidden_ownership_edges(source: str, target: str) -> None:
    assert _violation(ImportEdge(_PREFIX + source, _PREFIX + target, "", 1))


def test_governance_may_use_only_read_compiler_and_transform_contracts() -> None:
    source = _PREFIX + "application.governance.ports"
    target = _PREFIX + "application.reads.ports"
    assert not _violation(ImportEdge(source, target, "ProjectionCompiler", 1))
    assert not _violation(ImportEdge(source, target, "RowTransformPort", 1))
    assert _violation(ImportEdge(source, target, "IdentityPort", 1))


def test_production_imports_follow_complete_dependency_policy() -> None:
    paths = sorted(_ROOT.rglob("*.py"))
    modules = {_module(path) for path in paths}
    failures = []
    for path in paths:
        for edge in _imports(
            path.read_text(), _module(path), modules, package=path.name == "__init__.py"
        ):
            if _violation(edge):
                failures.append(f"{path}:{edge.line}: {edge.source} -> {edge.target} ({edge.name})")
    assert not failures, "\n".join(failures)

# ruff: noqa: E501
from __future__ import annotations

from html import escape
from importlib import resources
from typing import Any, cast

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from starlette.staticfiles import StaticFiles

_ASSET_PACKAGE = "dal_obscura.control_plane.interfaces"


def install_ui(app: FastAPI) -> None:
    asset_root = resources.files(_ASSET_PACKAGE).joinpath("ui_assets")
    static_root = asset_root.joinpath("static")

    app.mount("/ui/static", StaticFiles(directory=str(static_root)), name="control-plane-ui-static")

    @app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    def control_plane_ui() -> HTMLResponse:
        shell = asset_root.joinpath("shell.html").read_text(encoding="utf-8")
        return HTMLResponse(shell)


def htmx_response(fragment: str) -> HTMLResponse:
    return HTMLResponse(fragment)


def viewHeader(kicker: str, title: str, body: str) -> str:
    return (
        '<section class="view-header">'
        f'<p class="eyebrow">{escape(kicker)}</p>'
        f"<h2>{escape(title)}</h2>"
        f"<p>{escape(body)}</p>"
        "</section>"
    )


def tabs(items: list[tuple[str, str]], active: str) -> str:
    buttons = "".join(
        (
            f'<button type="button" class="{"is-active" if label == active else ""}" '
            f'data-route-template="{escape(route)}">{escape(label)}</button>'
        )
        for route, label in items
    )
    return f'<div class="tabs" role="tablist">{buttons}</div>'


def statStrip(items: list[tuple[str, object]]) -> str:
    stats = "".join(
        f"<div><small>{escape(label)}</small><strong>{escape(str(value))}</strong></div>"
        for label, value in items
    )
    return f'<section class="stat-strip">{stats}</section>'


def card(title: str, body: str, footer: str = "") -> str:
    rendered_footer = f'<footer class="card-footer">{footer}</footer>' if footer else ""
    return (
        '<section class="card">'
        f"<header><h3>{escape(title)}</h3></header>"
        f'<div class="card-body">{body}</div>'
        f"{rendered_footer}</section>"
    )


def badge(text: str, tone: str = "neutral") -> str:
    return f'<span class="badge {escape(tone)}">{escape(text)}</span>'


def actionBar(actions: list[dict[str, str]]) -> str:
    buttons = "".join(
        (
            f'<button type="button" class="{escape(action.get("class", "secondary-action"))}" '
            f"{_attrs(action)}>{escape(action['label'])}</button>"
        )
        for action in actions
    )
    return f'<div class="action-bar">{buttons}</div>'


def tableSection(title: str, rows: list[dict[str, Any]], *, empty: str) -> str:
    if not rows:
        return card(title, f'<p class="section-copy">{escape(empty)}</p>')
    columns = list(rows[0])
    header = "".join(
        f'<button type="button" data-sort-key="{escape(column)}">{escape(column)}</button>'
        for column in columns
    )
    body = "".join(
        "<tr>"
        + "".join(f"<td>{escape(_format_value(row.get(column)))}</td>" for column in columns)
        + "</tr>"
        for row in rows
    )
    return card(
        title,
        (
            '<input class="filter-input" type="search" placeholder="Filter rows" '
            "data-table-filter>"
            '<div class="table-wrap"><table class="resource-table" data-table>'
            f"<thead><tr>{header}</tr></thead><tbody>{body}</tbody></table></div>"
        ),
    )


def render_runtime_settings_partial(runtime: dict[str, object] | None) -> str:
    if runtime is None:
        return (
            f"{viewHeader('Runtime', 'No runtime settings', 'No runtime settings have been saved for this cell.')}"
            + card(
                "Next action",
                '<p class="section-copy">Save runtime settings before publishing this cell.</p>',
            )
        )
    return tableSection("Runtime settings", [runtime], empty="No runtime settings.")


def render_catalogs_partial(catalogs: list[dict[str, object]]) -> str:
    rows = "".join(_catalog_card(catalog) for catalog in catalogs)
    return (
        f"{viewHeader('Tenant workspace', 'Catalogs', 'Connect sources, refresh tables, and choose which assets enter policy review.')}"
        + statStrip(
            [("Connected", len(catalogs)), ("Known tables", "refresh"), ("Needs review", "scan")]
        )
        + actionBar(
            [
                {
                    "label": "Add catalog",
                    "class": "primary-action",
                    "route": "/v1/tenants/{tenant_id}/cells/{cell_id}/catalogs/{name}",
                },
                {
                    "label": "Refresh tables",
                    "class": "secondary-action",
                    "route": "/v1/cells/{cell_id}/catalogs",
                },
            ]
        )
        + f'<section class="catalog-list" data-table>{rows or _empty("No catalogs configured.")}</section>'
        + card(
            "Recently discovered",
            (
                '<input class="filter-input" type="search" placeholder="Filter tables" data-table-filter>'
                '<article class="resource-row"><span><strong>prod.claims</strong>'
                "<small>Detected in analytics during last scan</small></span>"
                f"{badge('Add asset', 'success')}</article>"
            ),
        )
    )


def render_assets_partial(assets: list[dict[str, object]]) -> str:
    rows = "".join(_asset_card(asset) for asset in assets)
    return (
        f"{viewHeader('Tenant workspace', 'Policy Center', 'Protect assets by table, principal, columns, masks, and row filters.')}"
        + statStrip(
            [
                ("Assets", len(assets)),
                ("Needs review", _needs_review_count(assets)),
                ("Policy coverage", _coverage(assets)),
            ]
        )
        + tabs(
            [
                ("/v1/cells/{cell_id}/assets", "Assets"),
                ("/v1/assets/{asset_id}/policy-rules", "Rules"),
                ("/v1/assets/{asset_id}/policy-rules", "Masks"),
                ("/v1/assets/{asset_id}/policy-rules", "Preview"),
            ],
            "Assets",
        )
        + '<input class="filter-input" type="search" placeholder="Filter assets or SQL" data-table-filter>'
        + f'<section class="asset-list" data-table>{rows or _empty("No assets configured.")}</section>'
    )


def render_policy_rules_partial(rules: list[dict[str, object]]) -> str:
    if not rules:
        return viewHeader(
            "Policy detail", "Edit Policy Rule", "No policy rules exist for this asset yet."
        ) + card(
            "Preview result",
            '<p class="section-copy">Add a rule before previewing masked output.</p>',
        )
    rule = rules[0]
    return (
        viewHeader(
            "Policy detail",
            "Edit Policy Rule",
            "Tune access for principals, columns, masks, and row filters before publishing.",
        )
        + card(
            "Who",
            _chips(rule.get("principals", []), "success")
            + _chips([f"{k}={v}" for k, v in _dict_value(rule.get("when", {})).items()], "neutral"),
        )
        + card("Columns", _chips(rule.get("columns", []), "success"))
        + card(
            "Masks",
            _chips([f"{key} mask" for key in _dict_value(rule.get("masks", {}))], "warning"),
        )
        + card(
            "Row filter", f'<pre class="sql-box">{escape(str(rule.get("row_filter") or ""))}</pre>'
        )
    )


def _catalog_card(catalog: dict[str, object]) -> str:
    name = str(catalog.get("name", "catalog"))
    options = _dict_value(catalog.get("options", {}))
    return (
        '<article class="catalog-card">'
        f"<div><h2>{escape(name)}</h2><p>{escape(str(catalog.get('module', 'catalog')))} · {escape(str(options.get('uri', 'configured')))}</p></div>"
        f'<div class="chip-row">{badge("connected", "success")}{badge("last refresh pending", "neutral")}</div>'
        f"<footer>{actionBar([{'label': 'Refresh tables', 'class': 'primary-action', 'route': '/v1/cells/{cell_id}/catalogs'}, {'label': 'Remove catalog', 'class': 'danger-action', 'route': '/v1/cells/{cell_id}/catalogs'}])}</footer>"
        "</article>"
    )


def _asset_card(asset: dict[str, object]) -> str:
    return (
        '<article class="asset-card">'
        f"<div><h2>{escape(str(asset.get('target', 'asset')))}</h2><p>Catalog {escape(str(asset.get('catalog', 'catalog')))} · {escape(str(asset.get('backend', 'backend')))}</p></div>"
        f'<div class="chip-row">{badge("policy review", "warning")}{badge("row filter", "neutral")}</div>'
        "<footer><span>Open policy configuration</span>"
        '<button type="button" data-route-template="/v1/assets/{asset_id}/policy-rules">Edit policy</button></footer>'
        "</article>"
    )


def _chips(values: object, tone: str) -> str:
    if not isinstance(values, list):
        return ""
    return f'<div class="chip-row">{"".join(badge(str(value), tone) for value in values)}</div>'


def _dict_value(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _empty(message: str) -> str:
    return f'<p class="section-copy">{escape(message)}</p>'


def _attrs(action: dict[str, str]) -> str:
    route = action.get("route")
    return f'data-route-template="{escape(route)}"' if route else ""


def _format_value(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _needs_review_count(assets: list[dict[str, object]]) -> int:
    return sum(1 for asset in assets if not asset.get("policy_rules"))


def _coverage(assets: list[dict[str, object]]) -> str:
    if not assets:
        return "0%"
    covered = len(assets) - _needs_review_count(assets)
    return f"{round((covered / len(assets)) * 100)}%"

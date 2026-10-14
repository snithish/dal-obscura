from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from dal_obscura_plugin_api import ExecutionContext
from dal_obscura_plugin_api.options import PluginOptions


def test_global_options_use_admitted_namespace_and_instance_precedence():
    options = PluginOptions.from_conf(
        {"dal.plugins.iceberg.catalog.uri": "global", "dal.plugins.iceberg.batch_rows": 128},
        {"iceberg", "iceberg.catalog"},
    )
    assert dict(options.for_plugin("iceberg.catalog", {"uri": "instance"})) == {"uri": "instance"}
    assert dict(options.for_plugin("iceberg")) == {"batch_rows": 128}


def test_unknown_plugin_namespace_is_rejected():
    with pytest.raises(ValueError, match="namespace"):
        PluginOptions.from_conf({"dal.plugins.missing.uri": "value"}, {"iceberg"})


def test_execution_options_are_detached_and_immutable():
    source = {"io": {"timeout": 5}}
    context = ExecutionContext(datetime.now(UTC) + timedelta(minutes=1), "options", options=source)
    source["io"]["timeout"] = 99
    assert cast(dict[str, object], context.options["io"])["timeout"] == 5
    with pytest.raises(TypeError):
        cast(Any, context.options)["new"] = True

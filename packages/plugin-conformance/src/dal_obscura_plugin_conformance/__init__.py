"""Public conformance checks for trusted catalog and table-format plugins."""

from dal_obscura_plugin_conformance.golden import nested_golden_table
from dal_obscura_plugin_conformance.runner import (
    ConformanceResult,
    check_capabilities,
    check_record_batches,
    check_schema_descriptor,
    run_format_checks,
)

__all__ = [
    "ConformanceResult",
    "check_capabilities",
    "check_record_batches",
    "check_schema_descriptor",
    "nested_golden_table",
    "run_format_checks",
]


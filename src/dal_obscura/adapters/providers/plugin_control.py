"""Application-facing control over the startup-admitted registry."""

from dal_obscura_plugin_api import PluginDescriptor, PluginKind

from dal_obscura.plugins import PluginLifecycleState, PluginRegistry


class AdmittedPluginControl:
    def __init__(self, registry: PluginRegistry) -> None:
        self._registry = registry

    def admitted(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        return self._registry.admitted()

    def status_report(self) -> tuple[dict[str, object], ...]:
        return self._registry.status_report()

    def set_lifecycle(self, kind: PluginKind, plugin_id: str, target: str) -> str:
        return self._registry.set_lifecycle(kind, plugin_id, PluginLifecycleState(target)).value

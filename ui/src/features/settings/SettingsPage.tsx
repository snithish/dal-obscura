import { FormEvent, useEffect, useState } from "react";

import { apiGet, apiPut } from "../../api/client";

type RuntimeSettings = {
  ticket_ttl_seconds: number;
  max_tickets: number;
  max_ticket_exchanges: number;
  path_rules: Record<string, unknown>[];
};

const defaults: RuntimeSettings = {
  ticket_ttl_seconds: 900,
  max_tickets: 64,
  max_ticket_exchanges: 2,
  path_rules: [{ glob: "s3://warehouse/*", allow: true }],
};

export function SettingsPage() {
  const [settings, setSettings] = useState(defaults);
  const [pathRulesJson, setPathRulesJson] = useState(
    JSON.stringify(defaults.path_rules, null, 2),
  );
  const [status, setStatus] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);

  useEffect(() => {
    void apiGet<RuntimeSettings | null>("/v1/settings/runtime")
      .then((loaded) => {
        if (loaded) {
          setSettings(loaded);
          setPathRulesJson(JSON.stringify(loaded.path_rules, null, 2));
        }
      })
      .catch(() => setStatus("Could not load runtime settings."));
  }, []);

  async function submitSettings(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus(null);
    setIsSaving(true);
    try {
      const path_rules = JSON.parse(pathRulesJson) as Record<string, unknown>[];
      const saved = await apiPut<RuntimeSettings>("/v1/settings/runtime", {
        ...settings,
        path_rules,
      });
      setSettings(saved);
      setPathRulesJson(JSON.stringify(saved.path_rules, null, 2));
      setStatus("Runtime settings saved.");
    } catch (caught) {
      setStatus(caught instanceof SyntaxError ? "Path rules must be valid JSON." : "Save failed.");
    } finally {
      setIsSaving(false);
    }
  }

  return (
    <div className="grid gap-5">
      <header>
        <p className="text-xs font-black uppercase tracking-wide text-muted">Operations</p>
        <h1 className="mt-1 text-3xl font-black">Settings</h1>
      </header>
      <form className="surface grid gap-5 p-5" onSubmit={submitSettings}>
        <div>
          <h2 className="text-lg font-black">Runtime configuration</h2>
          <p className="mt-2 text-sm leading-6 text-muted">
            Configure ticket limits and path allow rules before publishing changes.
          </p>
        </div>
        {status ? <div className="alert">{status}</div> : null}
        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          <label className="block">
            <span className="text-xs font-black uppercase tracking-wide text-muted">
              Ticket TTL seconds
            </span>
            <input
              className="field mt-2"
              min={1}
              type="number"
              value={settings.ticket_ttl_seconds}
              onChange={(event) =>
                setSettings({ ...settings, ticket_ttl_seconds: Number(event.target.value) })
              }
            />
          </label>
          <label className="block">
            <span className="text-xs font-black uppercase tracking-wide text-muted">
              Max tickets
            </span>
            <input
              className="field mt-2"
              min={1}
              type="number"
              value={settings.max_tickets}
              onChange={(event) =>
                setSettings({ ...settings, max_tickets: Number(event.target.value) })
              }
            />
          </label>
          <label className="block">
            <span className="text-xs font-black uppercase tracking-wide text-muted">
              Max exchanges
            </span>
            <input
              className="field mt-2"
              min={1}
              type="number"
              value={settings.max_ticket_exchanges}
              onChange={(event) =>
                setSettings({
                  ...settings,
                  max_ticket_exchanges: Number(event.target.value),
                })
              }
            />
          </label>
        </div>
        <label className="block">
          <span className="text-xs font-black uppercase tracking-wide text-muted">
            Path rules JSON
          </span>
          <textarea
            className="field mt-2 min-h-[170px] py-3 font-mono text-xs leading-5"
            value={pathRulesJson}
            onChange={(event) => setPathRulesJson(event.target.value)}
          />
        </label>
        <div>
          <button className="btn-primary" disabled={isSaving} type="submit">
            {isSaving ? "Saving..." : "Save runtime settings"}
          </button>
        </div>
      </form>
    </div>
  );
}

import { FormEvent, useEffect, useState } from "react";

import { apiGet, apiPut } from "../../api/client";
import {
  AUTH_PROVIDER_TYPES,
  authProviderFormFromProvider,
  authProviderPayloadFromForm,
  defaultAuthProvider,
  runtimePayloadFromForm,
  type AuthProvider,
  type RuntimeSettings,
} from "./settingsLogic";

const defaults: RuntimeSettings = {
  ticket_ttl_seconds: 900,
  max_tickets: 64,
  max_ticket_exchanges: 2,
};

export function SettingsPage() {
  const [settings, setSettings] = useState(defaults);
  const [authProvider, setAuthProvider] = useState(defaultAuthProvider);
  const [status, setStatus] = useState<string | null>(null);
  const [isSavingRuntime, setIsSavingRuntime] = useState(false);
  const [isSavingAuth, setIsSavingAuth] = useState(false);

  useEffect(() => {
    void Promise.all([
      apiGet<RuntimeSettings | null>("/v1/settings/runtime"),
      apiGet<AuthProvider[]>("/v1/settings/auth-providers"),
    ])
      .then(([loadedRuntime, loadedProviders]) => {
        if (loadedRuntime) {
          setSettings(loadedRuntime);
        }
        const provider = loadedProviders[0];
        if (provider) {
          setAuthProvider(authProviderFormFromProvider(provider));
        }
      })
      .catch(() => setStatus("Could not load settings."));
  }, []);

  async function submitRuntime(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus(null);
    setIsSavingRuntime(true);
    try {
      const payload = runtimePayloadFromForm(settings);
      if (!payload.ok) {
        setStatus(payload.error);
        return;
      }
      const saved = await apiPut<RuntimeSettings>("/v1/settings/runtime", payload.settings);
      setSettings(saved);
      setStatus("Runtime settings saved.");
    } catch {
      setStatus("Runtime settings save failed.");
    } finally {
      setIsSavingRuntime(false);
    }
  }

  async function submitAuth(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus(null);
    setIsSavingAuth(true);
    try {
      await apiPut<{ providers: AuthProvider[] }>("/v1/settings/auth-providers", {
        providers: [authProviderPayloadFromForm(authProvider)],
      });
      setStatus("Authentication provider saved.");
    } catch {
      setStatus("Authentication provider save failed.");
    } finally {
      setIsSavingAuth(false);
    }
  }

  return (
    <div className="grid gap-5">
      <header>
        <p className="text-xs font-black uppercase tracking-wide text-muted">Operations</p>
        <h1 className="mt-1 text-3xl font-black">Settings</h1>
        <p className="mt-2 max-w-2xl text-sm leading-6 text-muted">
          Configure runtime ticket limits and the control-plane auth provider without
          editing generated configuration files.
        </p>
      </header>

      {status ? <div className="alert">{status}</div> : null}

      <form className="surface grid gap-5 p-5" onSubmit={submitRuntime}>
        <div>
          <h2 className="text-lg font-black">Runtime configuration</h2>
          <p className="mt-2 text-sm leading-6 text-muted">
            Ticket limits are validated before publishing changes.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          <NumberField
            label="Ticket TTL seconds"
            value={settings.ticket_ttl_seconds}
            onChange={(value) => setSettings({ ...settings, ticket_ttl_seconds: value })}
          />
          <NumberField
            label="Max tickets"
            value={settings.max_tickets}
            onChange={(value) => setSettings({ ...settings, max_tickets: value })}
          />
          <NumberField
            label="Max exchanges"
            value={settings.max_ticket_exchanges}
            onChange={(value) => setSettings({ ...settings, max_ticket_exchanges: value })}
          />
        </div>

        <div>
          <button className="btn-primary" disabled={isSavingRuntime} type="submit">
            {isSavingRuntime ? "Saving..." : "Save runtime settings"}
          </button>
        </div>
      </form>

      <form className="surface grid gap-5 p-5" onSubmit={submitAuth}>
        <div>
          <h2 className="text-lg font-black">Authentication provider</h2>
          <p className="mt-2 text-sm leading-6 text-muted">
            Local deployments can use the default JWT identity adapter while
            platform deployments point the secret at their own environment.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-4 md:grid-cols-[120px_minmax(0,1fr)_240px]">
          <NumberField
            label="Order"
            value={authProvider.ordinal}
            onChange={(value) => setAuthProvider({ ...authProvider, ordinal: value })}
          />
          <label className="block">
            <span className="text-xs font-black uppercase tracking-wide text-muted">
              Provider type
            </span>
            <select
              className="field mt-2"
              value={authProvider.providerType}
              onChange={(event) =>
                setAuthProvider({
                  ...authProvider,
                  providerType: event.target.value as keyof typeof AUTH_PROVIDER_TYPES,
                })
              }
            >
              {Object.entries(AUTH_PROVIDER_TYPES).map(([value, provider]) => (
                <option key={value} value={value}>
                  {provider.label}
                </option>
              ))}
            </select>
          </label>
          <label className="block">
            <span className="text-xs font-black uppercase tracking-wide text-muted">
              Status
            </span>
            <select
              className="field mt-2"
              value={authProvider.enabled ? "enabled" : "disabled"}
              onChange={(event) =>
                setAuthProvider({ ...authProvider, enabled: event.target.value === "enabled" })
              }
            >
              <option value="enabled">Enabled</option>
              <option value="disabled">Disabled</option>
            </select>
          </label>
        </div>
        <label className="block">
          <span className="text-xs font-black uppercase tracking-wide text-muted">
            JWT secret environment variable
          </span>
          <input
            className="field mt-2"
            value={authProvider.jwtSecretEnv}
            onChange={(event) =>
              setAuthProvider({ ...authProvider, jwtSecretEnv: event.target.value })
            }
          />
        </label>
        <div>
          <button className="btn-primary" disabled={isSavingAuth} type="submit">
            {isSavingAuth ? "Saving..." : "Save auth provider"}
          </button>
        </div>
      </form>
    </div>
  );
}

function NumberField({
  label,
  onChange,
  value,
}: {
  label: string;
  onChange: (value: number) => void;
  value: number;
}) {
  return (
    <label className="block">
      <span className="text-xs font-black uppercase tracking-wide text-muted">{label}</span>
      <input
        className="field mt-2"
        min={1}
        type="number"
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
    </label>
  );
}

import { useEffect, useState } from "react";
import { NavLink, Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { BrowserRouter } from "react-router-dom";

import {
  clearOidcSession,
  completeDemoLogin,
  completeOidcCallback,
  loadUiAuthConfig,
  startOidcLogin,
  type UiLoginShortcut,
  type UiAuthConfig,
} from "../api/auth";
import { apiGet, type SessionActor } from "../api/client";
import { AssetsPage } from "../features/assets/AssetsPage";
import { CatalogsPage } from "../features/catalogs/CatalogsPage";
import { PoliciesPage } from "../features/policies/PoliciesPage";
import { PublishPage } from "../features/publish/PublishPage";
import { SettingsPage } from "../features/settings/SettingsPage";

const navItems = [
  { to: "/ui/assets", label: "Assets", hint: "Inventory" },
  { to: "/ui/catalogs", label: "Catalogs", hint: "Sources" },
  { to: "/ui/policies", label: "Policies", hint: "Rules" },
  { to: "/ui/publish", label: "Versions", hint: "Policy history" },
  { to: "/ui/settings", label: "Settings", hint: "Runtime" },
];

export function App() {
  return (
    <BrowserRouter>
      <AppShell />
    </BrowserRouter>
  );
}

function AppShell() {
  const [session, setSession] = useState<SessionActor | null>(null);
  const [authConfig, setAuthConfig] = useState<UiAuthConfig | null>(null);
  const [sessionStatus, setSessionStatus] = useState("Checking session");

  async function refreshSession() {
    try {
      const loaded = await apiGet<SessionActor>("/v1/session");
      setSession(loaded);
      setSessionStatus("Signed in");
    } catch {
      setSession(null);
      setSessionStatus("Sign in required");
    }
  }

  useEffect(() => {
    void loadUiAuthConfig().then(setAuthConfig);
    void refreshSession();
  }, []);

  async function signIn(loginHint?: string, returnTo = window.location.pathname) {
    if (authConfig) {
      await startOidcLogin(authConfig, returnTo, { loginHint });
    }
  }

  async function demoSignIn(shortcut: UiLoginShortcut, returnTo: string) {
    if (shortcut.demo_login_path) {
      await completeDemoLogin(shortcut.demo_login_path, shortcut.login_hint);
      await refreshSession();
      return;
    }
    await signIn(shortcut.login_hint, returnTo);
  }

  function signOut() {
    clearOidcSession(authConfig);
    setSession(null);
    setSessionStatus("Sign in required");
  }

  return (
    <div className="min-h-screen bg-app text-ink">
        <div className="grid min-h-screen grid-cols-1 lg:grid-cols-[268px_minmax(0,1fr)]">
          <aside className="border-b border-border bg-white px-4 py-4 lg:border-b-0 lg:border-r lg:py-5">
            <div className="mb-4 flex items-center gap-3 lg:mb-7">
              <div className="grid h-10 w-10 place-items-center rounded-card bg-ink text-sm font-black text-white shadow-soft">
                do
              </div>
              <div>
                <strong className="block leading-tight">dal-obscura</strong>
                <span className="text-xs font-semibold text-muted">control plane</span>
              </div>
            </div>
            <nav className="flex gap-2 overflow-x-auto lg:grid lg:overflow-visible">
              {navItems.map((item) => (
                <NavLink
                  className={({ isActive }) =>
                    [
                      "group grid shrink-0 grid-cols-[4px_minmax(0,1fr)] gap-3 rounded-card border px-0 py-0 text-left transition-colors",
                      isActive
                        ? "border-accent bg-[#edf8f4] text-ink shadow-panel"
                        : "border-transparent text-muted hover:border-border hover:bg-soft hover:text-ink",
                    ].join(" ")
                  }
                  key={item.to}
                  to={item.to}
                >
                  {({ isActive }) => (
                    <>
                      <span
                        className={[
                          "block rounded-l-card",
                          isActive ? "bg-accent" : "bg-transparent group-hover:bg-border",
                        ].join(" ")}
                      />
                      <span className="min-w-0 py-2.5 pr-3">
                        <span className="block text-sm font-black">{item.label}</span>
                        <span className="block text-xs font-semibold text-muted">
                          {item.hint}
                        </span>
                      </span>
                    </>
                  )}
                </NavLink>
              ))}
            </nav>
            <div className="mt-7 hidden rounded-card border border-border bg-soft p-4 lg:block">
              <p className="text-xs font-black uppercase text-muted">Operating model</p>
              <p className="mt-2 text-sm font-black">Catalog, govern, publish</p>
              <p className="mt-1 text-xs leading-5 text-muted">
                Draft policy changes stay local until an immutable version is submitted.
              </p>
            </div>
          </aside>
          <main className="min-w-0">
            <div className="border-b border-border bg-white px-4 py-3 lg:px-8">
              <div className="flex flex-col justify-between gap-2 md:flex-row md:items-center">
                <div>
                  <p className="text-xs font-black uppercase text-muted">
                    Open-source deployment console
                  </p>
                  <p className="mt-1 text-sm text-muted">
                    Configure assets, ownership, policies, and runtime settings.
                  </p>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <span className="badge badge-success">
                    {session ? session.principal : sessionStatus}
                  </span>
                  {session ? (
                    <button className="btn-secondary h-9" type="button" onClick={signOut}>
                      Sign out
                    </button>
                  ) : (
                    <NavLink className="btn-primary h-9" to="/ui/login">
                      Sign in
                    </NavLink>
                  )}
                </div>
              </div>
            </div>
            <div className="px-4 py-5 lg:px-8 lg:py-7">
            <Routes>
              <Route element={<Navigate replace to="/ui/assets" />} path="/ui" />
              <Route
                element={<AuthCallback onSessionReady={() => void refreshSession()} />}
                path="/ui/auth/callback"
              />
              <Route
                element={
                  session ? (
                    <Navigate replace to="/ui/assets" />
                  ) : (
                    <LoginPage
                      authConfig={authConfig}
                      onDemoSignIn={(shortcut, returnTo) => demoSignIn(shortcut, returnTo)}
                      onSignIn={(returnTo) => void signIn(undefined, returnTo)}
                    />
                  )
                }
                path="/ui/login"
              />
              <Route element={session ? <AssetsPage /> : <Navigate replace to="/ui/login" />} path="/ui/assets" />
              <Route element={session ? <CatalogsPage /> : <Navigate replace to="/ui/login" />} path="/ui/catalogs" />
              <Route element={session ? <PoliciesPage /> : <Navigate replace to="/ui/login" />} path="/ui/policies" />
              <Route element={session ? <PublishPage /> : <Navigate replace to="/ui/login" />} path="/ui/publish" />
              <Route element={session ? <SettingsPage /> : <Navigate replace to="/ui/login" />} path="/ui/settings" />
            </Routes>
            </div>
          </main>
        </div>
      </div>
  );
}

function LoginPage({
  authConfig,
  onDemoSignIn,
  onSignIn,
}: {
  authConfig: UiAuthConfig | null;
  onDemoSignIn: (shortcut: UiLoginShortcut, returnTo: string) => Promise<void>;
  onSignIn: (returnTo: string) => void;
}) {
  const navigate = useNavigate();
  const [status, setStatus] = useState("");
  const returnTo = "/ui/assets";

  async function useShortcut(shortcut: UiLoginShortcut) {
    try {
      setStatus(`Signing in as ${shortcut.label}`);
      await onDemoSignIn(shortcut, returnTo);
      navigate(returnTo, { replace: true });
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Sign in failed");
    }
  }

  return (
    <section className="mx-auto max-w-xl space-y-4">
      <div className="surface p-5">
        <p className="text-xs font-black uppercase text-muted">Sign in</p>
        <h1 className="mt-2 text-2xl font-black text-ink">Choose access</h1>
        <div className="mt-5 grid gap-3 sm:grid-cols-2">
          {authConfig?.login_shortcuts?.map((shortcut) => (
            <button
              className="btn-secondary h-11"
              key={shortcut.login_hint}
              type="button"
              onClick={() => void useShortcut(shortcut)}
            >
              {shortcut.label}
            </button>
          ))}
        </div>
        <button
          className="btn-primary mt-4 w-full"
          type="button"
          disabled={!authConfig}
          onClick={() => onSignIn(returnTo)}
        >
          Sign in with identity provider
        </button>
        {status ? <p className="mt-3 text-sm font-semibold text-muted">{status}</p> : null}
      </div>
    </section>
  );
}

function AuthCallback({ onSessionReady }: { onSessionReady: () => void }) {
  const navigate = useNavigate();
  const [status, setStatus] = useState("Completing sign in");

  useEffect(() => {
    async function complete() {
      try {
        const config = await loadUiAuthConfig();
        if (!config) {
          throw new Error("UI auth is not configured");
        }
        const returnTo = await completeOidcCallback(config, window.location.href);
        onSessionReady();
        navigate(returnTo, { replace: true });
      } catch (error) {
        setStatus(error instanceof Error ? error.message : "Sign in failed");
      }
    }

    void complete();
  }, [navigate, onSessionReady]);

  return (
    <section className="space-y-3">
      <p className="text-sm font-black text-ink">{status}</p>
    </section>
  );
}

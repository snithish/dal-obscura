import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import { BrowserRouter } from "react-router-dom";

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
                Draft changes stay local until an immutable publication is activated.
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
                    Configure assets, ownership, policies, and data-plane publications.
                  </p>
                </div>
                <span className="badge badge-success">local admin mode</span>
              </div>
            </div>
            <div className="px-4 py-5 lg:px-8 lg:py-7">
            <Routes>
              <Route element={<Navigate replace to="/ui/assets" />} path="/ui" />
              <Route element={<AssetsPage />} path="/ui/assets" />
              <Route element={<CatalogsPage />} path="/ui/catalogs" />
              <Route element={<PoliciesPage />} path="/ui/policies" />
              <Route element={<PublishPage />} path="/ui/publish" />
              <Route element={<SettingsPage />} path="/ui/settings" />
            </Routes>
            </div>
          </main>
        </div>
      </div>
    </BrowserRouter>
  );
}

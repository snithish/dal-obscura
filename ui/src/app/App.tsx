import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import { BrowserRouter } from "react-router-dom";

import { AssetsPage } from "../features/assets/AssetsPage";
import { CatalogsPage } from "../features/catalogs/CatalogsPage";
import { PoliciesPage } from "../features/policies/PoliciesPage";
import { PublishPage } from "../features/publish/PublishPage";
import { SettingsPage } from "../features/settings/SettingsPage";

const navItems = [
  { to: "/ui/assets", label: "Assets" },
  { to: "/ui/catalogs", label: "Catalogs" },
  { to: "/ui/policies", label: "Policies" },
  { to: "/ui/publish", label: "Publish" },
  { to: "/ui/settings", label: "Settings" },
];

export function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-app text-ink">
        <div className="grid min-h-screen grid-cols-1 lg:grid-cols-[240px_minmax(0,1fr)]">
          <aside className="border-b border-border bg-ink px-4 py-4 text-white lg:border-b-0 lg:border-r lg:py-5">
            <div className="mb-4 flex items-center gap-3 lg:mb-8">
              <div className="grid h-10 w-10 place-items-center rounded-card bg-accent text-sm font-black text-white shadow-soft">
                do
              </div>
              <div>
                <strong className="block leading-tight">dal-obscura</strong>
                <span className="text-xs font-semibold text-[#9fb3bd]">control plane</span>
              </div>
            </div>
            <nav className="flex gap-1 overflow-x-auto lg:grid lg:overflow-visible">
              {navItems.map((item) => (
                <NavLink
                  className={({ isActive }) =>
                    [
                      "shrink-0 rounded-card px-3 py-2 text-sm font-bold",
                      isActive
                        ? "bg-white text-ink shadow-soft"
                        : "text-[#b9c9d0] hover:bg-[#213842] hover:text-white",
                    ].join(" ")
                  }
                  key={item.to}
                  to={item.to}
                >
                  {item.label}
                </NavLink>
              ))}
            </nav>
            <div className="mt-8 hidden rounded-card border border-white/10 bg-white/5 p-3 lg:block">
              <p className="text-xs font-black uppercase tracking-wide text-[#9fb3bd]">
                Workspace
              </p>
              <p className="mt-2 text-sm font-bold">Local control plane</p>
              <p className="mt-1 text-xs leading-5 text-[#b9c9d0]">
                Asset-first policy authoring. Runtime setup stays in settings.
              </p>
            </div>
          </aside>
          <main className="min-w-0 px-4 py-5 lg:px-8 lg:py-7">
            <Routes>
              <Route element={<Navigate replace to="/ui/assets" />} path="/ui" />
              <Route element={<AssetsPage />} path="/ui/assets" />
              <Route element={<CatalogsPage />} path="/ui/catalogs" />
              <Route element={<PoliciesPage />} path="/ui/policies" />
              <Route element={<PublishPage />} path="/ui/publish" />
              <Route element={<SettingsPage />} path="/ui/settings" />
            </Routes>
          </main>
        </div>
      </div>
    </BrowserRouter>
  );
}

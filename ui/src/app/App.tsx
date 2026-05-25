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
      <div className="min-h-screen bg-[#f6f9fa] text-ink">
        <div className="grid min-h-screen grid-cols-[240px_minmax(0,1fr)]">
          <aside className="border-r border-border bg-white px-4 py-5">
            <div className="mb-8 flex items-center gap-3">
              <div className="grid h-9 w-9 place-items-center rounded-card bg-accent text-sm font-black text-white">
                do
              </div>
              <div>
                <strong className="block leading-tight">dal-obscura</strong>
                <span className="text-xs font-semibold text-muted">control plane</span>
              </div>
            </div>
            <nav className="grid gap-1">
              {navItems.map((item) => (
                <NavLink
                  className={({ isActive }) =>
                    [
                      "rounded-card px-3 py-2 text-sm font-bold",
                      isActive
                        ? "bg-ink text-white"
                        : "text-muted hover:bg-soft hover:text-ink",
                    ].join(" ")
                  }
                  key={item.to}
                  to={item.to}
                >
                  {item.label}
                </NavLink>
              ))}
            </nav>
          </aside>
          <main className="min-w-0 px-6 py-5">
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

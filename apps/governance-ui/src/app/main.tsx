import { createRoot } from "react-dom/client";
import { AppProviders } from "../design/AppProviders";
import { SessionBoundary } from "../features/identity/SessionBoundary";
import { Workspace } from "./Workspace";
import "./styles.css";
import "../design/base.css";

createRoot(document.getElementById("root")!).render(
  <AppProviders>
    <SessionBoundary><Workspace /></SessionBoundary>
  </AppProviders>,
);

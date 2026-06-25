import React from "react";
import ReactDOM from "react-dom/client";

import { loadRuntimeConfig } from "./api/config";
import { App } from "./app/App";
import "./styles/global.css";

void loadRuntimeConfig().finally(() => {
  ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
});

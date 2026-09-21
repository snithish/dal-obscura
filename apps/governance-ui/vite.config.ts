import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { previewSecurity } from "./preview-security";

export default defineConfig({
  plugins: [react(), previewSecurity()],
  server: {
    proxy: {
      "/v1": "http://127.0.0.1:8821",
      "/auth": "http://127.0.0.1:8821",
    },
  },
});

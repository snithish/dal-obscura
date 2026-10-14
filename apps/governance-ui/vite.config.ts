import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { previewSecurity } from "./preview-security";

export default defineConfig({
  plugins: [react(), previewSecurity()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("/node_modules/@mantine/")) return "mantine";
          if (["react", "react-dom", "scheduler"].some((name) => id.includes(`/node_modules/${name}/`))) {
            return "react";
          }
        },
      },
    },
  },
  server: {
    proxy: {
      "/v1": "http://127.0.0.1:8821",
      "/auth": "http://127.0.0.1:8821",
    },
  },
});

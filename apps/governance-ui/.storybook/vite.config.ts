import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Never inherit the application's /v1 and /auth development proxies.
export default defineConfig({ plugins: [react()], server: { host: "127.0.0.1" } });

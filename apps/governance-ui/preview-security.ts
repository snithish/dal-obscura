import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import type { Plugin } from "vite";

/** Exercise the built shell with the gateway's actual CSP, not Vite dev's permissive policy. */
export function previewSecurity(): Plugin {
  return {
    name: "obscura-preview-security",
    configurePreviewServer(server) {
      const nginx = readFileSync(new URL("../../ui/nginx.conf", import.meta.url), "utf8");
      const policy = nginx.match(/add_header Content-Security-Policy "([^"]+)"/)?.[1];
      if (!policy) throw new Error("Gateway CSP is missing");
      server.middlewares.use((req, res, next) => {
        const path = req.url?.split("?", 1)[0];
        if ((req.method !== "GET" && req.method !== "HEAD") || (path !== "/" && path !== "/index.html")) return next();
        const nonce = randomBytes(16).toString("hex");
        const html = readFileSync(new URL("./dist/index.html", import.meta.url), "utf8");
        res.setHeader("Content-Type", "text/html; charset=utf-8");
        res.setHeader("Cache-Control", "no-store");
        res.setHeader("Content-Security-Policy", policy.replaceAll("$request_id", nonce));
        res.end(req.method === "HEAD" ? undefined : html.replaceAll("__OBSCURA_STYLE_NONCE__", nonce));
      });
    },
  };
}

FROM node:24-alpine AS build

WORKDIR /app
COPY apps/governance-ui/package.json apps/governance-ui/pnpm-lock.yaml apps/governance-ui/pnpm-workspace.yaml ./
RUN corepack enable && corepack install && pnpm install --frozen-lockfile
COPY apps/governance-ui/ ./
RUN pnpm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine

RUN mkdir -p /etc/nginx/obscura \
    && chown nginx:nginx /etc/nginx/conf.d /etc/nginx/obscura
COPY --chown=nginx:nginx ui/nginx.conf /etc/nginx/obscura/default.conf.template
COPY --chown=nginx:nginx --chmod=755 ui/resolve-upstream-dns.sh /docker-entrypoint.d/40-resolve-upstream-dns.sh
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 8080

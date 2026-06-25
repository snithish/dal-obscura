# syntax=docker/dockerfile:1.7

FROM node:22-bookworm-slim AS builder

WORKDIR /workspace

ENV PNPM_HOME=/pnpm \
    PATH=/pnpm:${PATH}

RUN corepack enable

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY ui/package.json ./ui/package.json

RUN --mount=type=cache,target=/pnpm/store \
    pnpm install --frozen-lockfile

COPY ui ./ui

RUN pnpm --filter @dal-obscura/control-plane-ui build

FROM caddy:2.9-alpine AS runtime

COPY ui/Caddyfile /etc/caddy/Caddyfile
COPY ui/docker-entrypoint.sh /usr/local/bin/dal-obscura-ui-entrypoint
COPY --from=builder /workspace/ui/dist /usr/share/caddy

RUN chmod 0555 /usr/local/bin/dal-obscura-ui-entrypoint

EXPOSE 8080

ENTRYPOINT ["dal-obscura-ui-entrypoint"]

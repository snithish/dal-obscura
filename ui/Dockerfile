FROM node:22-alpine AS build

WORKDIR /app
COPY apps/governance-ui/package.json apps/governance-ui/pnpm-lock.yaml apps/governance-ui/pnpm-workspace.yaml ./
RUN corepack enable && corepack install && pnpm install --frozen-lockfile
COPY apps/governance-ui/ ./
RUN pnpm run build

FROM nginx:1.27-alpine

COPY ui/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 8080

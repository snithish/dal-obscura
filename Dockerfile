# syntax=docker/dockerfile:1.7

# Builder image includes uv and a matching CPython runtime. Dependency
# resolution is locked by uv.lock, so CI and local builds produce the same
# install set unless the lockfile changes.
FROM rust:1.94.0-slim-bookworm AS builder

# Native reader is compiled once; the runtime carries no compiler. Copy Python
# and uv into the Rust stage instead of duplicating the much larger toolchain.
COPY --from=ghcr.io/astral-sh/uv:python3.12-bookworm-slim /usr/local /usr/local
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

# Copy dependency metadata first for better layer caching. README and LICENSE
# are package metadata inputs for the setuptools build backend.
COPY pyproject.toml uv.lock README.md LICENSE rust-toolchain.toml ./
COPY src ./src
COPY packages/plugin-api ./packages/plugin-api
COPY packages/iceberg-reader ./packages/iceberg-reader

# Install only production dependencies plus the Flight server and Postgres
# driver extras. No dev, test, or build caches are copied into the runtime stage.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/packages/iceberg-reader/target \
    uv sync --extra server --extra postgres --frozen --no-dev

# Runtime image has Python but not uv. Keeping the package manager out of the
# final image reduces both size and attack surface.
FROM python:3.12-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="dal-obscura" \
      org.opencontainers.image.description="Data and control plane services for policy-aware Arrow Flight access" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app

# Python logs must flush immediately in containers, and bytecode writes are
# disabled because the runtime filesystem should be treated as immutable.
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Use a stable numeric identity so Kubernetes/OpenShift policies can refer to
# it without relying on /etc/passwd names.
RUN groupadd --gid 10001 dalobscura \
    && useradd --uid 10001 --gid 10001 --home-dir /home/dalobscura --create-home --shell /usr/sbin/nologin dalobscura

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY scripts/docker-entrypoint.sh /usr/local/bin/dal-obscura-entrypoint

RUN chmod 0555 /usr/local/bin/dal-obscura-entrypoint \
    && chown -R 10001:10001 /home/dalobscura

USER 10001:10001

# 8815 is the Arrow Flight data-plane port; 8820 is the HTTP control-plane port.
EXPOSE 8815 8820

# Default to the data plane. Use `control-plane` as the first argument to start
# the API process from the same immutable image.
ENTRYPOINT ["dal-obscura-entrypoint"]
CMD ["data-plane"]

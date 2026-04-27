FROM ghcr.io/spiffe/spire-agent:1.14.5 AS spire-agent
FROM ghcr.io/spiffe/spire-server:1.14.5 AS spire-server

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /workspace

ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV UV_NO_SYNC=1

RUN groupadd --gid 1001 spiffe-server \
    && useradd --uid 1001 --gid 1001 --home-dir /home/spiffe-server --create-home spiffe-server \
    && groupadd --gid 1002 spiffe-client \
    && useradd --uid 1002 --gid 1002 --home-dir /home/spiffe-client --create-home spiffe-client

COPY --from=spire-agent /opt/spire/bin/spire-agent /usr/local/bin/spire-agent
COPY --from=spire-server /opt/spire/bin/spire-server /usr/local/bin/spire-server
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY examples/auth/_shared ./examples/auth/_shared

RUN uv sync --extra server

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /workspace

ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
COPY examples/auth/_shared ./examples/auth/_shared

RUN uv sync --extra server --frozen --no-dev
RUN chmod +x /workspace/examples/auth/_shared/scripts/dal-obscura-example-read
RUN ln -s /workspace/examples/auth/_shared/scripts/dal-obscura-example-read /usr/local/bin/dal-obscura-example-read

# syntax=docker/dockerfile:1.7
FROM rust:1.94.0-slim-bookworm AS native
COPY --from=ghcr.io/astral-sh/uv:python3.14-bookworm-slim /usr/local /usr/local
RUN apt-get update && apt-get install --no-install-recommends -y git ca-certificates && rm -rf /var/lib/apt/lists/*
COPY packages/iceberg-reader /reader
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/usr/local/cargo/git --mount=type=cache,target=/reader/target uv build --wheel --out-dir /wheels /reader

FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim
RUN apt-get update && apt-get install --no-install-recommends -y openjdk-17-jdk-headless maven && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
COPY packages ./packages
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-editable --dev --extra server --extra postgres --extra cloud --no-install-package dal-obscura-iceberg-reader
RUN uv pip install --no-deps debugpy==1.8.17
COPY --from=native /wheels /wheels
RUN uv pip install --no-deps /wheels/*.whl
COPY scripts/build_plugin_lock.py /example/build_plugin_lock.py
COPY examples/nested-mor/scripts /example/
RUN /app/.venv/bin/python /example/build_plugin_lock.py --output /support/plugin-lock.json --plugin catalog:delta.directory --plugin catalog:delta.catalog --plugin table_format:delta
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT []
CMD ["python", "/example/verify.py"]

# syntax=docker/dockerfile:1.7
FROM rust:1.94.0-slim-bookworm AS native
COPY --from=ghcr.io/astral-sh/uv:python3.12-bookworm-slim /usr/local /usr/local
RUN apt-get update && apt-get install --no-install-recommends -y git ca-certificates && rm -rf /var/lib/apt/lists/*
COPY packages/iceberg-reader /reader
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/usr/local/cargo/git --mount=type=cache,target=/reader/target uv build --wheel --out-dir /wheels /reader

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
COPY packages/plugin-api ./packages/plugin-api
COPY packages/iceberg-reader ./packages/iceberg-reader
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --dev --extra server --extra postgres --extra cloud --no-install-package dal-obscura-iceberg-reader
COPY --from=native /wheels /wheels
RUN uv pip install --no-deps /wheels/*.whl
COPY packages/delta-plugin /delta-plugin
RUN uv pip install --no-deps /delta-plugin
COPY scripts/build_plugin_lock.py /example/build_plugin_lock.py
COPY tests/support/delta.py tests/support/iceberg.py /support/tests/support/
COPY examples/nested-mor/scripts /example/
RUN /app/.venv/bin/python /example/build_plugin_lock.py --output /support/plugin-lock.json --plugin catalog:delta.directory --plugin catalog:delta.catalog --plugin table_format:delta
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT []
CMD ["python", "/example/verify.py"]

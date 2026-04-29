FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /workspace

# Keep Python output unbuffered so container logs show up immediately.
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy

# Run the Flight server as a dedicated non-root user.
RUN groupadd --gid 10001 dalobscura \
    && useradd --uid 10001 --gid 10001 --home-dir /home/dalobscura --create-home dalobscura

# Copy the lock inputs before syncing so dependency resolution only changes when
# the project metadata or pinned dependency set changes.
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src

RUN uv sync --extra server --frozen --no-dev

USER 10001:10001
EXPOSE 8815

# Start with DAL_OBSCURA_DATABASE_URL, DAL_OBSCURA_CELL_ID, and ticket/TLS env vars.
ENTRYPOINT ["/workspace/.venv/bin/dal-obscura"]

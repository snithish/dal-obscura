ARG DAL_OBSCURA_IMAGE=ghcr.io/snithish/dal-obscura:latest
FROM ${DAL_OBSCURA_IMAGE}

USER root

WORKDIR /workspace

ENV PYTHONUNBUFFERED=1

# The setup script provisions the control-plane app in-process with FastAPI's
# TestClient, which requires httpx. The published service image intentionally
# omits example-only dependencies, so install just that helper dependency here.
RUN python -m ensurepip --upgrade \
    && python -m pip install --no-cache-dir "httpx>=0.27.0"

COPY examples/auth/_shared ./examples/auth/_shared

RUN chmod +x /workspace/examples/auth/_shared/scripts/dal-obscura-example-read
RUN ln -s /workspace/examples/auth/_shared/scripts/dal-obscura-example-read /usr/local/bin/dal-obscura-example-read

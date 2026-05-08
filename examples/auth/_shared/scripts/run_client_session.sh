#!/usr/bin/env sh
set -eu

. /workspace/runtime/data-plane.env
exec /workspace/.venv/bin/python examples/auth/_shared/scripts/client_session.py

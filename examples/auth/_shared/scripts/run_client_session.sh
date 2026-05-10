#!/usr/bin/env sh
set -eu

. /workspace/runtime/data-plane.env
exec python examples/auth/_shared/scripts/client_session.py

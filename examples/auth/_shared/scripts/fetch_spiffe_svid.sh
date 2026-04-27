#!/bin/sh
set -eu

OUT_DIR=${1:?usage: fetch_spiffe_svid.sh OUT_DIR}
SOCKET=${SPIFFE_ENDPOINT_SOCKET:-unix:///run/spire/agent/public/api.sock}
SOCKET_PATH=${SOCKET#unix://}

mkdir -p "$OUT_DIR"

for _ in $(seq 1 60); do
  if /usr/local/bin/spire-agent api fetch x509 \
    -socketPath "$SOCKET_PATH" \
    -write "$OUT_DIR" \
    -silent >/dev/null 2>&1; then
    if [ -s "$OUT_DIR/svid.0.pem" ] && [ -s "$OUT_DIR/svid.0.key" ] && [ -s "$OUT_DIR/bundle.0.pem" ]; then
      exit 0
    fi
  fi
  sleep 1
done

echo "Unable to fetch SPIFFE X.509-SVID from $SOCKET_PATH" >&2
exit 1

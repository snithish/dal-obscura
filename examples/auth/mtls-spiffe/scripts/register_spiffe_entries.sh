#!/bin/sh
set -eu

SERVER_SOCKET=${SPIRE_SERVER_SOCKET:-/run/spire/server/private/api.sock}
SPIRE_SERVER_BIN=${SPIRE_SERVER_BIN:-/usr/local/bin/spire-server}
JOIN_TOKEN_FILE=${SPIRE_JOIN_TOKEN_FILE:-/run/spire/shared/join-token}
AGENT_ID=${SPIRE_AGENT_ID:-spiffe://example.org/ns/default/sa/spire-agent}
SERVER_ID=${SPIFFE_SERVER_ID:-spiffe://example.org/ns/default/sa/dal-obscura-server}
CLIENT_ID=${SPIFFE_CLIENT_ID:-spiffe://example.org/ns/default/sa/dal-obscura-client}

mkdir -p "$(dirname "$JOIN_TOKEN_FILE")"

for _ in $(seq 1 60); do
  if "$SPIRE_SERVER_BIN" healthcheck -socketPath "$SERVER_SOCKET" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

TOKEN_OUTPUT=$(
  "$SPIRE_SERVER_BIN" token generate \
    -socketPath "$SERVER_SOCKET" \
    -spiffeID "$AGENT_ID" \
    -output json
)
printf '%s\n' "$TOKEN_OUTPUT" | sed -n 's/.*"value"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' > "$JOIN_TOKEN_FILE"
if [ ! -s "$JOIN_TOKEN_FILE" ]; then
  printf '%s\n' "$TOKEN_OUTPUT" | sed -n 's/.*"token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' > "$JOIN_TOKEN_FILE"
fi
if [ ! -s "$JOIN_TOKEN_FILE" ]; then
  echo "Unable to parse SPIRE join token: $TOKEN_OUTPUT" >&2
  exit 1
fi

"$SPIRE_SERVER_BIN" entry create \
  -socketPath "$SERVER_SOCKET" \
  -parentID "$AGENT_ID" \
  -spiffeID "$SERVER_ID" \
  -selector unix:uid:1001 \
  -dns dal-obscura >/dev/null

"$SPIRE_SERVER_BIN" entry create \
  -socketPath "$SERVER_SOCKET" \
  -parentID "$AGENT_ID" \
  -spiffeID "$CLIENT_ID" \
  -selector unix:uid:1002 >/dev/null

touch /run/spire/shared/entries.done

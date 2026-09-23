#!/usr/bin/env sh
set -eu
umask 077

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
RUNTIME_DIR="$ROOT/.runtime"
TOKEN_FILE="$RUNTIME_DIR/pf-enable-token"
ANCHOR="com.apple/dal-obscura-demo"

enable() {
  if ! grep -Fq 'rdr-anchor "com.apple/*"' /etc/pf.conf; then
    echo "The active macOS PF configuration lacks the com.apple wildcard anchor; refusing to change its main ruleset." >&2
    return 1
  fi
  mkdir -p "$RUNTIME_DIR"

  if [ -s "$TOKEN_FILE" ] && sudo pfctl -s info 2>/dev/null | grep -q 'Status: Enabled'; then
    sudo pfctl -a "$ANCHOR" -f "$ROOT/pf-anchor.conf"
    echo "Podman localhost ports enabled (PF anchor $ANCHOR)."
    return
  fi

  rm -f "$TOKEN_FILE"
  output="$(sudo pfctl -E 2>&1)" || {
    printf '%s\n' "$output" >&2
    return 1
  }
  token="$(printf '%s\n' "$output" | sed -n 's/.*Token : *\([0-9][0-9]*\).*/\1/p' | head -n 1)"
  if [ -z "$token" ]; then
    printf '%s\n' "$output" >&2
    echo "Could not capture PF enable token; refusing to leave untracked firewall state." >&2
    return 1
  fi

  printf '%s\n' "$token" >"$TOKEN_FILE"
  if ! sudo pfctl -a "$ANCHOR" -f "$ROOT/pf-anchor.conf"; then
    sudo pfctl -X "$token" >/dev/null 2>&1 || true
    rm -f "$TOKEN_FILE"
    return 1
  fi
  echo "Podman localhost ports enabled. Rules redirect loopback-only ports 80/443 to 18080/18443."
}

disable() {
  if [ -s "$TOKEN_FILE" ]; then
    token="$(cat "$TOKEN_FILE")"
    sudo pfctl -a "$ANCHOR" -F all >/dev/null 2>&1 || true
    sudo pfctl -X "$token" >/dev/null 2>&1 || true
    rm -f "$TOKEN_FILE"
  else
    sudo pfctl -a "$ANCHOR" -F all >/dev/null 2>&1 || true
  fi
  echo "Podman localhost port redirects removed."
}

case "${1:-}" in
  enable) enable ;;
  disable) disable ;;
  *) echo "Usage: $0 enable|disable" >&2; exit 2 ;;
esac

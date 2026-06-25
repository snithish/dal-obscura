#!/bin/sh
set -eu

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

api_base_url="$(json_escape "${DAL_OBSCURA_API_BASE_URL:-}")"

cat > /usr/share/caddy/config.json <<EOF
{"apiBaseUrl":"${api_base_url}"}
EOF

exec caddy run --config /etc/caddy/Caddyfile --adapter caddyfile

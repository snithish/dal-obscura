#!/bin/sh
set -eu

template=/etc/nginx/obscura/default.conf.template
destination=/etc/nginx/conf.d/default.conf
resolver="$(awk '$1 == "nameserver" && $2 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ { print $2; exit }' /etc/resolv.conf)"

case "$resolver" in
  ''|*[!0-9.]*|.*|*..*|*.)
    echo "No usable IPv4 DNS resolver found in /etc/resolv.conf" >&2
    exit 1
    ;;
esac

sed "s/__OBSCURA_NGINX_RESOLVER__/$resolver/g" "$template" > "$destination.tmp"
mv "$destination.tmp" "$destination"

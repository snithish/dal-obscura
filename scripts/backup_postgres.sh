#!/usr/bin/env sh
set -eu

: "${DAL_OBSCURA_DATABASE_URL:?set DAL_OBSCURA_DATABASE_URL}"
: "${DAL_OBSCURA_BACKUP_RECIPIENT:?set DAL_OBSCURA_BACKUP_RECIPIENT}"

if [ "$#" -ne 1 ]; then
  echo "usage: DAL_OBSCURA_DATABASE_URL=... DAL_OBSCURA_BACKUP_RECIPIENT=age1... $0 OUTPUT.age" >&2
  exit 2
fi

command -v pg_dump >/dev/null 2>&1 || { echo "pg_dump is required" >&2; exit 2; }
command -v age >/dev/null 2>&1 || { echo "age is required" >&2; exit 2; }
output=$1
checksum="${output}.sha256"
backup_name=$(basename -- "$output")
if [ -e "$output" ] || [ -e "$checksum" ]; then
  echo "refusing to overwrite existing backup or checksum: $output" >&2
  exit 2
fi
command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum is required" >&2; exit 2; }
parent=$(dirname -- "$output")
mkdir -p "$parent"
umask 077
temporary=$(mktemp "${TMPDIR:-/tmp}/dal-obscura-backup.XXXXXX")
dump_temporary=$(mktemp "${TMPDIR:-/tmp}/dal-obscura-backup-dump.XXXXXX")
checksum_temporary=$(mktemp "${TMPDIR:-/tmp}/dal-obscura-backup-checksum.XXXXXX")
cleanup() { rm -f "$temporary" "$dump_temporary" "$checksum_temporary"; }
trap cleanup EXIT HUP INT TERM

pg_dump --format=custom --no-owner --no-acl --dbname="$DAL_OBSCURA_DATABASE_URL" \
  > "$dump_temporary"
test -s "$dump_temporary" || { echo "PostgreSQL dump is empty" >&2; exit 1; }
age --encrypt --recipient "$DAL_OBSCURA_BACKUP_RECIPIENT" --output "$temporary" \
  < "$dump_temporary"
test -s "$temporary" || { echo "encrypted backup is empty" >&2; exit 1; }
digest=$(sha256sum "$temporary" | awk '{print $1}')
test -n "$digest" || { echo "could not compute backup checksum" >&2; exit 1; }
printf '%s  %s\n' "$digest" "$backup_name" > "$checksum_temporary"
test -s "$checksum_temporary" || { echo "backup checksum is empty" >&2; exit 1; }
mv -- "$temporary" "$output"
mv -- "$checksum_temporary" "$checksum"
trap - EXIT HUP INT TERM
printf 'encrypted backup written: %s\nchecksum written: %s\n' "$output" "$checksum"

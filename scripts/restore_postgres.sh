#!/usr/bin/env sh
set -eu

: "${DAL_OBSCURA_DATABASE_URL:?set DAL_OBSCURA_DATABASE_URL}"
: "${DAL_OBSCURA_AGE_IDENTITY:?set DAL_OBSCURA_AGE_IDENTITY}"
: "${DAL_OBSCURA_RESTORE_CONFIRM:?set DAL_OBSCURA_RESTORE_CONFIRM}"

if [ "$DAL_OBSCURA_RESTORE_CONFIRM" != "I_UNDERSTAND_ISOLATED_RESTORE" ]; then
  echo "refusing restore: set DAL_OBSCURA_RESTORE_CONFIRM=I_UNDERSTAND_ISOLATED_RESTORE" >&2
  exit 2
fi
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: DAL_OBSCURA_DATABASE_URL=... DAL_OBSCURA_AGE_IDENTITY=... $0 BACKUP.age [CELL_ID]" >&2
  exit 2
fi

command -v age >/dev/null 2>&1 || { echo "age is required" >&2; exit 2; }
command -v pg_restore >/dev/null 2>&1 || { echo "pg_restore is required" >&2; exit 2; }
command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum is required" >&2; exit 2; }
command -v dal-obscura-maintenance >/dev/null 2>&1 || {
  echo "dal-obscura-maintenance is required" >&2
  exit 2
}
backup=$1
cell_id=${2:-}
test -r "$backup" || { echo "backup is not readable: $backup" >&2; exit 2; }
checksum="${backup}.sha256"
if [ -e "$checksum" ]; then
  test -r "$checksum" || { echo "backup checksum is not readable: $checksum" >&2; exit 2; }
  checksum_directory=$(dirname -- "$checksum")
  checksum_name=$(basename -- "$checksum")
  (
    cd -- "$checksum_directory"
    sha256sum --check "$checksum_name" >/dev/null
  ) || {
    echo "backup checksum verification failed: $backup" >&2
    exit 1
  }
fi
test -r "$DAL_OBSCURA_AGE_IDENTITY" || {
  echo "age identity is not readable: $DAL_OBSCURA_AGE_IDENTITY" >&2
  exit 2
}
umask 077
temporary=$(mktemp "${TMPDIR:-/tmp}/dal-obscura-restore.XXXXXX")
cleanup() { rm -f "$temporary"; }
trap cleanup EXIT HUP INT TERM

age --decrypt --identity "$DAL_OBSCURA_AGE_IDENTITY" "$backup" > "$temporary"
test -s "$temporary" || { echo "decrypted backup is empty" >&2; exit 1; }
pg_restore --single-transaction --clean --if-exists --no-owner --no-acl \
  --dbname="$DAL_OBSCURA_DATABASE_URL" "$temporary"

if [ -n "$cell_id" ]; then
  dal-obscura-maintenance invalidate-access --database-url "$DAL_OBSCURA_DATABASE_URL" --cell-id "$cell_id"
else
  dal-obscura-maintenance invalidate-access --database-url "$DAL_OBSCURA_DATABASE_URL"
fi
printf 'restore completed and replayable access invalidated; keep ingress closed until reconciliation\n'

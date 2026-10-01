#!/usr/bin/env sh
set -eu

: "${DAL_OBSCURA_DATABASE_URL:?set DAL_OBSCURA_DATABASE_URL}"
: "${DAL_OBSCURA_AGE_IDENTITY:?set DAL_OBSCURA_AGE_IDENTITY}"
: "${DAL_OBSCURA_RESTORE_CONFIRM:?set DAL_OBSCURA_RESTORE_CONFIRM}"

if [ "$DAL_OBSCURA_RESTORE_CONFIRM" != "I_UNDERSTAND_ISOLATED_RESTORE" ]; then
  echo "refusing restore: set DAL_OBSCURA_RESTORE_CONFIRM=I_UNDERSTAND_ISOLATED_RESTORE" >&2
  exit 2
fi
if [ "$#" -ne 1 ]; then
  echo "usage: DAL_OBSCURA_DATABASE_URL=... DAL_OBSCURA_AGE_IDENTITY=... $0 BACKUP.age" >&2
  exit 2
fi

command -v age >/dev/null 2>&1 || { echo "age is required" >&2; exit 2; }
command -v pg_restore >/dev/null 2>&1 || { echo "pg_restore is required" >&2; exit 2; }
command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum is required" >&2; exit 2; }
command -v dal-obscura-maintenance >/dev/null 2>&1 || {
  echo "dal-obscura-maintenance is required" >&2
  exit 2
}

require_owner_only_secret() {
  path=$1
  mode=$(stat -c '%a' "$path" 2>/dev/null || stat -f '%Lp' "$path" 2>/dev/null || true)
  case "$mode" in
    400|600) ;;
    *)
      echo "Secret file must be owner-only (mode 400 or 600): $path" >&2
      exit 2
      ;;
  esac
}

backup=$1
test -r "$backup" || { echo "backup is not readable: $backup" >&2; exit 2; }
checksum="${backup}.sha256"
if [ -e "$checksum" ]; then
  test -r "$checksum" || { echo "backup checksum is not readable: $checksum" >&2; exit 2; }
  checksum_directory=$(dirname -- "$checksum")
  backup_name=$(basename -- "$backup")
  expected_digest=$(awk '$1 ~ /^[[:xdigit:]]{64}$/ {print $1; exit}' "$checksum")
  verification_checksum=$(mktemp "${TMPDIR:-/tmp}/dal-obscura-restore-checksum.XXXXXX")
  printf '%s  %s\n' "$expected_digest" "$backup_name" > "$verification_checksum"
  checksum_valid=0
  if (
    cd -- "$checksum_directory"
    sha256sum --check "$verification_checksum" >/dev/null
  ); then
    checksum_valid=1
  fi
  rm -f -- "$verification_checksum"
  if [ "$checksum_valid" -ne 1 ]; then
    echo "backup checksum verification failed: $backup" >&2
    exit 1
  fi
fi
test -r "$DAL_OBSCURA_AGE_IDENTITY" || {
  echo "age identity is not readable: $DAL_OBSCURA_AGE_IDENTITY" >&2
  exit 2
}
require_owner_only_secret "$DAL_OBSCURA_AGE_IDENTITY"
umask 077
temporary=$(mktemp "${TMPDIR:-/tmp}/dal-obscura-restore.XXXXXX")
cleanup() { rm -f "$temporary"; }
trap cleanup EXIT HUP INT TERM

age --decrypt --identity "$DAL_OBSCURA_AGE_IDENTITY" "$backup" > "$temporary"
test -s "$temporary" || { echo "decrypted backup is empty" >&2; exit 1; }
pg_restore --single-transaction --clean --if-exists --no-owner --no-acl \
  --dbname="$DAL_OBSCURA_DATABASE_URL" "$temporary"

dal-obscura-maintenance invalidate-access --database-url "$DAL_OBSCURA_DATABASE_URL"
printf 'restore completed and replayable access invalidated; keep ingress closed until reconciliation\n'

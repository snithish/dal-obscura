#!/usr/bin/env sh
set -eu

# Keep one production image for both planes. The first argument selects the
# process role; any other command is executed directly for operational tooling.
case "${1:-data-plane}" in
  data-plane)
    shift || true
    exec dal-obscura "$@"
    ;;
  control-plane)
    shift || true
    exec dal-obscura-control-plane "$@"
    ;;
  dal-obscura | dal-obscura-control-plane)
    exec "$@"
    ;;
  *)
    exec "$@"
    ;;
esac

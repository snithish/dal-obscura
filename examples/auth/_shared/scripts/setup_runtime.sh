#!/usr/bin/env sh
set -eu

rm -f /workspace/runtime/setup.done

for pre_setup in "$@"; do
  case "$pre_setup" in
    *.py)
      /workspace/.venv/bin/python "$pre_setup"
      ;;
    *.sh)
      sh "$pre_setup"
      ;;
    *)
      echo "Unsupported setup step: $pre_setup" >&2
      exit 1
      ;;
  esac
done

/workspace/.venv/bin/python examples/auth/_shared/scripts/build_runtime.py
touch /workspace/runtime/setup.done
tail -f /dev/null

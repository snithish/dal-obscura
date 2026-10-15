"""Internal container command transport for image inspection and verification."""

import json
import sys

from dal_obscura.bootstrap.plugin_image import run_plugin_image_command


def main() -> None:
    result = run_plugin_image_command(sys.argv[1:])
    print(json.dumps(result, sort_keys=sys.argv[1] == "inspect"))


if __name__ == "__main__":
    main()

# Shared Auth Example Support

This directory contains the reusable pieces that keep the public example
`compose.yaml` files focused on the auth mechanism being demonstrated.

## Files

- `Dockerfile`: thin helper image used by `setup`, `client`, and small helper
  services. It derives from `DAL_OBSCURA_IMAGE`, which defaults to
  `ghcr.io/snithish/dal-obscura:latest`, and adds only example-only helper
  dependencies.
- `scripts/build_runtime.py`: creates the sample Iceberg table, provisions auth
  providers and policy through the control-plane API, publishes the data-plane
  snapshot, and writes `/workspace/runtime/data-plane.env`.
- `scripts/setup_runtime.sh`: shared setup entrypoint. Examples can pass one or
  more local setup scripts before `build_runtime.py` runs.
- `scripts/start_data_plane.sh`: starts `dal-obscura` from the published runtime
  env and optionally reads TLS certificate/key/CA files from file-path env vars.
- `scripts/run_client_session.sh`: sources the runtime env and starts the demo
  client session.
- `scripts/flight_client.py`: interactive read helper installed in the client
  image as `dal-obscura-example-read`.

## Reading Order

For a new user, read an example directory first, then come here only when you
want to understand how the sample table and published config are created. The
scripts are shared implementation support, not the main teaching surface.

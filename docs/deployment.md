# Deployment

The image runs Flight and HTTP as separate processes over one migrated and
published database. Use the production Compose profile in `deployment/production/`
and inspect its `.env.example` for deployment inputs. Pin image digests, provide
TLS material and worker object-storage credentials, and mount an artifact lock
built from the exact installed wheels. Secrets use the startup backend described
in [configuration](configuration.md).

Run migrations explicitly before serving. Configuration is published through the
HTTP control plane; process startup neither migrates nor substitutes unpublished
records. Bootstrap access belongs to disposable development profiles. Disable it
for normal production administration.

```sh
uv run --no-sync dal-obscura-migrate upgrade
uv run --no-sync dal-obscura-control-plane
uv run --no-sync dal-obscura
```

The operator CLI prepares plugin images and deployment targets. Use
`dal-obscura --help` for its commands. Build plugin locks from installed artifacts:

```sh
uv run --no-sync python scripts/build_plugin_lock.py --output /tmp/plugin-lock.json \
  --plugin catalog:iceberg.catalog --plugin table_format:iceberg \
  --plugin secret_backend:environment --plugin secret_backend:mounted_file
```

For a coordinated SDK/configuration change, rebuild all affected provider and
reader wheels, qualify the candidate image, and regenerate its lock before
restarting workers and HTTP. Replan tickets whose passive envelope or artifact
identity no longer matches. Preserve published policies and revisions through
supported database migrations. Document any deployment-specific rollback procedure
with the deployed artifact set.

Local HTTPS/mTLS and Keycloak examples live under `deployment/local-secure/` and
`examples/demo/keycloak/`. Their runners expose `--help`. Use fresh project names
and disposable databases for qualification; leave user demo services untouched.

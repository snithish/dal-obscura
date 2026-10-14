# Dal Obscura

Dal Obscura serves governed Arrow Flight reads. Policies combine row filters,
column selection and SQL masking. An authenticated governance UI publishes
catalogs, schemas, policies and identity attribute mappings.

The service captures an immutable configuration for planning and issues signed
passive scan tickets. Independent workers execute admitted plugin tasks against
the captured snapshot. Authorization and output validation remain in the service.

## Development

Use Python 3.14, uv, the reader package's Rust toolchain in
`packages/iceberg-reader/rust-toolchain.toml`, and the Node and
pnpm versions in [the UI package](apps/governance-ui/package.json).

```sh
uv sync --no-editable --all-packages --dev --extra server --extra postgres --frozen
uv run --no-sync dal-obscura-migrate --help
uv run --no-sync dal-obscura --help
uv run --no-sync dal-obscura-control-plane --help
pnpm --dir apps/governance-ui install --frozen-lockfile
```

Set `DAL_OBSCURA_DATABASE_URL`, run `dal-obscura-migrate upgrade`, and publish
configuration through the control plane before starting Flight. Services require
an existing database schema. Use disposable databases and development profiles
for local work. See [configuration](docs/configuration.md) and
[deployment](docs/deployment.md) before starting services.

## Guides

- [Architecture and execution contracts](docs/architecture.md)
- [Plugin SDK](packages/plugin-api/README.md)
- [Plugin qualification tools](packages/plugin-conformance/README.md)
- [Testing and release qualification](docs/testing.md)
- [Governance UI and identity](docs/governance-ui.md)

Core tests live in `tests/` under their owning service boundary. Package behavior
and package test support belong with the package or the conformance kit. See the
[testing guide](docs/testing.md) for the separate native, provider and UI lanes.

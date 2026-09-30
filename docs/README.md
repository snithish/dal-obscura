# dal-obscura Documentation

The authenticated control plane manages catalogs, assets, owners, and live policies.
The data plane exposes Arrow Flight reads and enforces row filters and column masks.

## Get started

- [Quickstart](quickstart.md): start a local environment and verify governed reads.
- [Keycloak demo](../examples/demo/keycloak/README.md): runnable local IAM,
  Postgres, Iceberg, control plane, and Flight environment.
- [Concepts](concepts.md): assets, policies, identity, and tickets.

## Govern and operate

- [Policy authoring](policy-authoring.md): rules, filters, masks, policy tests, and revocation.
- [Operators](operators.md): deployment, startup settings, health, and risks.
- [Operator runbook](operators-runbook.md): readiness, restart, reset, and triage.
- [Security](security.md): trust boundaries, authentication, secrets, and tickets.
- [Live configuration](live-configuration.md): snapshots, revisions, provider reuse, and restarts.
- [Production deployment](../deployment/production/README.md) and
  [secure local deployment](../deployment/local-secure/README.md): profile-specific setup.

## Integrate clients

- [Control-plane API](control-plane-api.md): authentication, preconditions, errors, and contracts.
- [Connectors](connectors.md): governed client reads and streaming behavior.
- [Compatibility](compatibility.md): tested consumers and unsupported cases.
- [JVM connector workspace](../connectors/README.md): Java/Spark setup and protocol.

## Develop and extend

- [Development](development.md): setup, test ownership, checks, and extension rules.
- [Architecture atlas](architecture/architecture-atlas.html) and
  [Mermaid source](architecture/architecture-atlas.md): system boundaries and read flow.
- [Read execution invariants](read-execution-invariants.md): guarantees, tests, and resource limits.
- [Plugin SDK](../packages/plugin-api/README.md) and
  [conformance kit](../packages/plugin-conformance/README.md): contracts and admission.
- [Governance UI](../apps/governance-ui/README.md): frontend development and checks.
- [Capacity benchmarks](../evaluation/capacity/README.md): repeatable measurements.

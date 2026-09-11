# dal-obscura Documentation

dal-obscura is a governed analytical data access layer. It exposes Arrow Flight
reads, applies policy-based row filters and masks, and uses an operator CLI to
manage immutable publication generations.

Use this page as the map. The docs are grouped by what you are trying to do.

## Contents

- [Start Here](#start-here)
- [Docs By Need](#docs-by-need)
- [Core Guides](#core-guides)
- [Mental Model](#mental-model)

## Start Here

| Reader | First read | Next read |
| --- | --- | --- |
| Evaluator | [Quickstart](quickstart.md) | [Concepts](concepts.md) |
| Data consumer | [Quickstart](quickstart.md) | [Connectors](connectors.md) and [Compatibility](compatibility.md) |
| Operator | [Policy Authoring](policy-authoring.md) | [Concepts](concepts.md) |
| Platform operator | [Operators](operators.md) | [Operator Runbook](operators-runbook.md) |
| Security reviewer | [Security](security.md) | [Policy Authoring](policy-authoring.md) |
| Contributor | [Development](development.md) | [New UI/UX Plan](ui-v2/README.md) |

## Docs By Need

### Try The Service

- [Quickstart](quickstart.md): run the local demo, verify governed Flight reads,
  and learn the manifest/CLI service shape.
- [Local Keycloak Demo](../examples/demo/keycloak/README.md): complete laptop
  environment with Keycloak, Postgres, Iceberg, and Flight reads.

### Understand The Model

- [Concepts](concepts.md): catalogs, assets, owners, policy versions, tickets,
  and the control-plane/data-plane split.
- [Security](security.md): identity providers, ticket lifecycle, secret
  handling, and fail-closed behavior.

### Configure And Operate

- [Operators](operators.md): deployment shape, required decisions, startup
  order, health checks, and operational risks.
- [Operator Runbook](operators-runbook.md): readiness, restart, reset, and fast
  triage checklist.
- [OIDC Example](../examples/auth/keycloak-oidc/README.md): runnable Docker
  Compose fixture for the supported reader identity provider.

### Govern Data

- [Policy Authoring](policy-authoring.md): grant rules, row filters, masks,
  preview, and asset-scoped policy-version publishing.

### Integrate Clients

- [Connectors](connectors.md): client surfaces and read-path overview.
- [Compatibility](compatibility.md): tested Python, Arrow, Java, and Spark
  versions plus unsupported consumer cells.
- [Connector Workspace](../connectors/README.md): JVM modules, Spark options,
  and protocol v1 details.

### Build And Contribute

- [New UI/UX Plan](ui-v2/README.md): fresh policy authoring and management experience, implementation packages, and acceptance gates. Proposed, not shipped.

- [Development](development.md): repo layout, common checks, test guidance, and
  release-oriented checklist.

## Core Guides

```mermaid
flowchart LR
    start["Run demo"] --> concepts["Understand model"]
    concepts --> policy["Author policy"]
    concepts --> ops["Operate service"]
    policy --> read["Verify reads"]
    ops --> security["Review security"]
    read --> connectors["Use connectors"]
```

Use the quickstart for a first working environment. Use the operator and
security guides before exposing a shared environment. Use connector docs only
after you have at least one governed asset and one allowed read persona.

## Mental Model

```mermaid
flowchart LR
    catalog["Catalog discovery"] --> asset["Governed asset"]
    asset --> policy["Manifest policy"]
    policy --> version["Published generation"]
    version --> publish["Compare-and-swap publish"]
    publish --> read["Governed Flight reads"]
```

The public product model is asset-first. Internal runtime details such as
tenant and cell identifiers are kept out of normal user workflows.

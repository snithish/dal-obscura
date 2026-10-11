# Operator plugin deployment

The operator CLI prepares plugins and catalog instances, then publishes one immutable
service image containing the installed plugin packages and their admission lock:

```sh
dal-obscura plugin add acme-delta-plugin
dal-obscura plugin configure acme.delta
dal-obscura deploy
```

These are operator commands. Running `dal-obscura` without arguments still starts
Flight. The CLI hides entry-point discovery and lock construction; it prints every
selected plugin ID, distribution, exact version, API version, descriptor digest and
installed-artifact digest. `deploy` compares admission with the last verified release
and asks for approval after preparing a concrete release. `--yes` explicitly approves
that printed admission for automated deployments.

## Configure the target once

This first implementation targets an **existing, initialized Compose deployment**
with `control-plane` and `data-plane` services. It does not create a database, migrate,
seed policies, provision secrets, configure ingress, or start dependent services.
Use the [production reference](../deployment/production/README.md) for first installation.
Docker with BuildKit, Compose supporting `up --wait`, registry authentication and
an immutable worker base image are prerequisites. Build the base from this revision;
its service package must include the operator image tooling.

Create `.dal-obscura-deploy/target.json`. Replace both digest placeholders with verified
SHA-256 digests; mutable base/tool tags are rejected. Paths for `compose_file` and
`env_file` are relative to this workspace. Smoke certificate paths are relative to the
operator's current directory, or absolute. Match `project_name` to the existing project.

```json
{
  "base_image": "registry.example/dal-obscura@sha256:REPLACE_WITH_VERIFIED_DIGEST",
  "uv_image": "ghcr.io/astral-sh/uv@sha256:REPLACE_WITH_VERIFIED_DIGEST",
  "image_repository": "registry.example/dal-obscura",
  "compose_file": "../deployment/production/compose.yaml",
  "env_file": "../deployment/production/.env",
  "project_name": "dal-obscura-production",
  "control_url": "https://governance.example",
  "admin_token_env": "DAL_OBSCURA_DEPLOY_ADMIN_TOKEN",
  "smoke": {
    "uri": "grpc+tls://flight.example:443",
    "token_env": "DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN",
    "ca_file": "/secure/tls/ca.crt",
    "cert_file": "/secure/tls/client.crt",
    "key_file": "/secure/tls/client.key",
    "catalog": "synthetic",
    "target": "default.deployment_check",
    "columns": ["id"],
    "expected_rows": [{"id": 1}]
  }
}
```

Use an OIDC platform-admin bearer token for the control API and an appropriately
scoped bearer token for the synthetic Flight read. Load them into the named environment
variables through your credential tooling. The CLI never stores these token values.
Control HTTP and unencrypted Flight are permitted only on loopback. Control API
redirects are rejected so bearer credentials cannot be forwarded to another origin.

A different workspace is supported with a global flag before the command:

```sh
dal-obscura --workspace /secure/operator-workspace plugin add acme-delta-plugin==1.2.3
```

## Add and configure

`plugin add` accepts a package name with optional version constraints. It installs
binary wheels in a disposable build based on the worker image, using a pinned `uv`
image as build tooling. Existing base distributions are constrained to their installed
versions; an incompatible plugin fails instead of upgrading the service or SDK silently.
No package manager is added to the final worker image. Previously selected packages
remain version-pinned when another package is added.

The CLI reads static descriptors without importing factories, checks admission through
the existing registry, and discovers plugin IDs only from explicitly added distributions.
Plugins in transitive dependencies are not automatically admitted. Add their distributions
explicitly if they supply required catalog output formats. All IDs exposed by an explicitly
added package are included in the candidate and displayed for review. The candidate is
replaced atomically only after successful installation and inspection.

`plugin configure` selects a catalog plugin and prompts for a named instance, its
non-secret form fields, and secret names/scopes. Several instances may use the same
plugin. Provision the actual secrets and `catalog:<instance>` grants through the existing
secret provider. The CLI stores references such as `{"secret":"catalog-token",
"scope":"catalog:warehouse"}` and never resolves them. Format plugins bind table handles
and do not have catalog-instance configuration.

For noninteractive configuration, or descriptors without form fields:

```sh
dal-obscura plugin configure acme.delta --name warehouse --options /secure/options.json
```

The options file must contain non-secret options and explicit secret references. Shared
validation rejects inline credentials, credential-bearing URIs, unknown declared fields,
missing required fields and wrong types. The live API also enforces its egress policy.
Drafts initially expect catalog revision zero. Successful deployment updates their saved
revision; unchanged published configurations are idempotent. A concurrent revision conflict
requires review and reconfiguration with `--expected-revision <current revision>`.

## Prepare, publish and verify

```sh
dal-obscura deploy --prepare
dal-obscura deploy
```

`--prepare` builds a release and checks Compose configuration without pushing images,
changing running services or publishing catalogs. Normal `deploy` prepares its own
release, authenticates to the control API, displays admission and asks for approval.

The release embeds the lock with mode `0444`, inspects the final installed artifacts
again, and rejects any difference from the candidate. Deploy pushes that exact image,
resolves its registry digest, and starts both application services by digest through a
saved Compose override. Both services use `/app/plugin-lock.json`; routine deployment
uses `--no-deps` and never invokes migration jobs.

Every running control-plane and data-plane replica must have the selected image identity,
embedded lock path and exact lock bytes. An in-container verifier reruns static admission
and queries that replica's readiness endpoint. Only then does deploy publish catalog
drafts through one authenticated `POST /v1/catalogs/publish` change set, with explicit
revision fences on every changed instance. Releases contain at most 100 catalog drafts;
this limit is checked before container preparation. The existing HTTP request-byte budget
and catalog option validation also apply. An unchanged published configuration is skipped.

The server applies changed instances in deterministic name order inside one database
transaction, including their audit records. A revision conflict, admission failure, or
invalid option on any instance rolls back the entire change set. Successful catalog
publication is atomic across these changes. A committed publication followed by a failed
smoke read still needs recovery; image rollout and catalog publication are separate stages.
Retry reads current revisions and recognizes identical published configurations.

Finally, a real authenticated Flight read checks schema/plan/batch agreement with metadata,
exact synthetic values and duplicate counts. Row order may differ across tasks. The probe
uses one 30-second deadline and bounds endpoints, chunks and expected rows (at most 1,000).
It cancels active readers on success and failure. Use a small synthetic table with stable
expected rows; do not put customer data in release configuration.

Only successful verification advances `.dal-obscura-deploy/verified.json`. Each release
retains its image identity, package selections, lock, catalog snapshot, Dockerfile and
Compose override in `releases/<id>/`. Local release artifacts are ignored by Git.

## Failure and recovery

A failed install leaves the previous candidate intact. A failed publish, worker check,
catalog update or smoke read returns nonzero and leaves the last verified receipt intact.
Running services and already published catalogs are not automatically rolled back. Inspect
the failed release and correct its cause before retrying; no success is reported prematurely.

Keep the previous verified image in the registry and local engine. Its saved `compose.json`
override pins the previous published digest and can be combined with the original Compose
file and environment file to restore application images. Reconcile any catalog changes
before reopening traffic; an image rollback alone does not restore live database state.

Changing a plugin artifact can make outstanding tickets incompatible with new workers.
Drain those tickets before a coordinated plugin upgrade, as required by the
[existing cutover contract](plugin-rebuild.md). Ticket draining and ingress control are
not automated by this first Compose implementation. It also does not provide Kubernetes
rollouts, registry signing, automatic rollback or cross-platform image builds.

Qualification: local CLI/admission tests, real disposable Flight transport checks and
installed-wheel checks are separate evidence from live build/push/Compose deployment.
The latter still requires a working container engine and disposable deployment target.

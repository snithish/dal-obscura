# Architecture and edge-case review — 2026-10-02

The feature ownership and governed-read design remain appropriate. This audit
checked identity freshness, request snapshots, revision conflicts, ticket
integrity/exchange reservations, policy and nested projection enforcement,
passive scan decoding, plugin lifetimes, and streaming resource limits. It found
specific failure-path gaps rather than a reason to introduce another architecture.

## Corrections

- **Signing-key freshness.** The remote JWKS cache previously refreshed only when
  a token used an unknown key ID. A removed known key could remain trusted beyond
  the configured interval, and a replacement using the same ID stayed invisible.
  Authentication now refreshes the entire set when due, serializes concurrent
  refreshes, and denies stale-key authentication after refresh failure. Failed
  attempts are rate-limited; successful retries restore access. Static JWKS stays
  static. Tests cover removal, same-ID replacement, interval boundaries, provider
  failure, retry suppression, and recovery.
- **Iterator construction cleanup.** Runtime planning and conformance planning/
  output now own the iterable before calling `iter()`. If construction fails,
  cleanup still runs and preserves the original failure even when `close()` fails.
- **Shutdown cleanup.** Releasing the last active provider lease after shutdown
  preserves an existing request error. A cleanup failure still surfaces when the
  request itself succeeded, and the cache slot is released in either case.
- **Conformance byte accounting.** Output checks now reject both oversized logical
  batches and small slices retaining oversized parent buffers, matching runtime
  admission semantics.
- **Malformed envelope errors.** Excessive JSON nesting is rejected through the
  scan decoder's normal invalid-payload error rather than leaking a parser
  `RecursionError`.

Regression tests reproduced each changed failure before its fix. They live at
the identity, provider lifecycle, scan decoder, runtime adapter, and independent
conformance boundaries that own these behaviors.

## What replaced pickle

`sources/task_codec.py` encodes a versioned JSON envelope containing the exact
admitted plugin identity, immutable SDK table handle, original and projected Arrow
schemas encoded as base64 IPC, storage path roots, and an immutable SDK `ScanTask`.
The SDK task accepts bounded passive JSON objects and recursively freezes them.

Native Iceberg now uses the same SDK envelope as other formats. Its task contains
projected column names, admitted file membership, bounded native parallelism and
an optional validated SQL hint. DuckDB interprets snapshots and delete semantics;
there is no separate PyIceberg task codec or reconstruction of provider objects.
The factory is selected from the admitted registry and must match the captured
artifact identity.

This full envelope stays in the existing server-side ticket database. Flight
clients receive only an HMAC-signed reference containing ticket ID, nonce, and
expiry. Fetch verifies that reference, stored payload integrity, principal context,
exchange admission, and revocation before executing the captured task. Production
source and bundled plugin code contain no pickle/cloudpickle/dill path. Tests
retain hostile legacy pickle examples to prove rejection.

JSON prevents executable object deserialization; it does not sandbox admitted
Python plugins, Arrow IPC, or trusted provider libraries. Plugin installation and
artifact admission remain a separate trust boundary.

## Architecture evidence and remaining limits

- Configuration uses one repeatable database snapshot and releases its connection
  before provider IO. PostgreSQL concurrent-writer and resource CAS cases passed.
- Authorization, DuckDB SQL, and emitted schemas share their policy/projection
  owner. Hidden filter dependencies, full filters before masks, NULLs, nested
  ancestor masks, maps/lists, literal dotted names, and metadata remain covered.
- Task validation precedes atomic ticket persistence. Fetch uses captured policy
  and checks expiry/revocation before each emitted batch. Exchange exhaustion
  blocks new fetches while preserving already reserved streams.
- Sources retain explicit ownership and bounded provider reuse. Exact installed
  artifacts remain required for plugin restoration; no compatibility decoder or
  arbitrary module-loading path was added.
- Planning memory still scales with file count. Delete buffers depend on one
  data file's associated deletes. Worker admission and byte checks are not a global
  RSS cap. Blocking provider IO requires provider-specific timeouts. Revocation
  queries per emitted batch need deployment concurrency measurement. PyIceberg
  private APIs still require upgrade conformance.
- Remote JWKS authentication now fails closed during an expired-cache provider
  outage. This is an intentional availability tradeoff, not an automatic claim
  that existing browser sessions or issued JWTs receive immediate IdP revocation.

## Qualification

- Full Python suite: **1,016 passed, zero skipped**, using disposable PostgreSQL
  and all real consumer backends through the no-skips wrapper.
- All five wheels rebuilt and installed in a fresh environment. With repository
  source paths disabled: **91 package checks** and **39 runtime routing checks**
  passed. Lock generation, CLI startup/help, and packaged migration upgrade/check
  passed.
- Ruff lint/format, Ty, and all-file pre-commit passed.
- Ticket-to-response benchmark: 16.8649 ms mean over 31 rounds. Two streaming
  probes covered 35 million rows combined and passed chunking, exact output, and
  RSS assertions. This is fresh regression evidence, not a paired speedup claim.

These checks establish regression evidence for the reviewed boundaries. They do
not prove the absence of every possible edge case or deployment-specific fault.
See [execution invariants](../read-execution-invariants.md),
[security guide](../security.md), and [architecture atlas](architecture-atlas.md).

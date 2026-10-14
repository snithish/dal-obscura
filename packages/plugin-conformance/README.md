# Plugin conformance

The kit qualifies catalog discovery, immutable handles, exact projected schemas,
passive tasks, output values and declared capabilities through SDK boundaries.
Expected rows and schemas must be supplied independently of the implementation.
Core admission, HTTP authorization and signed-ticket tests remain in the service.

`run_catalog_checks` and `run_format_checks` return qualification reports.
`testing.py` provides separate-process task readers for disjoint work and exact
coverage checks. Production capacity claims require additional representative
streaming and RSS measurements.

Real-provider fixture tools create snapshots through maintained writers. Delta
MoR deletes and updates use native Spark SQL; delta-rs owns COW mutations. Iceberg
uses Apache Java delete writers and RowDelta snapshot construction. A small
fixture-only bridge publishes the resulting snapshot through PyIceberg's public
catalog commit API with UUID and parent-snapshot requirements because PyIceberg
has no public MoR writer. The bridge does not encode manifests or delete semantics.

Native fixture lanes need Java 17, Maven, Spark and provider dependencies from the
workspace development environment. They use disposable services and directories,
close drivers and streams, and pin JVM artifacts in the fixture tools. Protocol
hand edits are reserved for negative corruption tests. Package tests never import
core `tests.support`.

See [qualification commands](../../docs/testing.md) for the no-skips, real-provider,
installed-wheel and separate S3-worker lanes.

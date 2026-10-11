# Captured read evidence

[Nested Delta and Iceberg report](nested-mor.html) embeds the full-scale execution
results and source excerpts captured before the repository cleanup. Its
[provenance manifest](nested-mor-provenance.json) identifies the exact source and
artifact hashes used for those runs. Subsequent cleanup can change source hashes;
the captured timings and excerpts are intentionally preserved as historical evidence.

See [current review qualification](../code-review.md) for the later revision's
checks and [reproduction commands](../../examples/nested-mor/README.md) to generate
new evidence. Neither format demonstrated partition IO pushdown in the captured
runs; Delta demonstrated projected-column IO reduction. Exact results and
disjoint worker assignments were verified for both.

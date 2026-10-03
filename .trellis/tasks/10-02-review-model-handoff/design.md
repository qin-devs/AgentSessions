# Design
Follow parent decisions and revised finding semantics.

## Exclusive ownership
application/candle_embedding.rs and handoff_pack.rs; ports/handoff.rs only if necessary. Main owns docs, task artifacts, commit and push; never revert another worker.

## Approach
Require complete required-content coverage in manifest, not self-hash; reject empty/missing/duplicate/unsafe entries. Preserve actual parsing and size/hash checks. No partial bundle bypass. Process-local failures need explicit bounded recovery semantics; preserve success caching. Enforce handoff content bytes across all retained fields; error if mandatory content cannot fit; recompute confidence after evidence trim. Versioned tagged ID encoding distinguishes since/until and None/empty. Do not invent repo/sidechain fields. Test tiny budgets, long query/tool metadata, no evidence and ID collisions. Coordinate loader diagnostics with CLI owner.

## Compatibility
Retain contracts except specified corrections. Document migration/cache changes. Revert isolated commits only after accounting for new data; never remove validation to roll back.

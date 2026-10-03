# Design
Follow parent decisions and revised finding semantics.

## Exclusive ownership
SQLite crate; ports/lib.rs; application/lib.rs and coupled tests. Main owns docs, task artifacts, commit and push; never revert another worker.

## Approach
Repo/facet-filtered ownership uses matching placement/session deterministically. Generation/search/payload/ownership use one connection/read transaction; no process-only lock. Same-source latest replaces old text; cross-source deterministic merge/removal uses live projections. Preserve full/incomplete claimant semantics. Alias candidates include entities losing this source but retained by another. Invalidate vectors on final content change; remove orphans; model/dimension readiness matches query. Context activity errors propagate. Preserve upstream batch scope. Additive migration cannot invent historical per-source content.

## Compatibility
Retain contracts except specified corrections. Document migration/cache changes. Revert isolated commits only after accounting for new data; never remove validation to roll back.

## Main-session baseline design review
Verified on 2edf2dc: commit_source_batches_if_changed still merges stored payload into incoming text regardless of source authority; alias regeneration collects candidates only after membership replacement; semantic readiness has no dimension condition. These are not fixed merely by upstream batch-scoping.

Persisted per-source projection evidence is necessary for deterministic source removal; the present merged payload cannot reconstruct it. Prefer an additive v18-to-next migration with an explicit missing-evidence legacy state, not fabricated copies of catalog content per source. Force one source reparse with PARSER_SEMANTIC_VERSION 3 -> 4 (also needed by provider corrections), then let current-source observations become authoritative. Keep conservative legacy/incomplete behavior until every needed live claimant has current evidence; document this limit and ensure no stale same-source merge after its replacement. Integrate projection persistence with verified durable batch/outbox transaction, no out-of-band write.

Snapshot scope should start before generation and cover all read orchestration including payload/ownership/resume metadata. A port-owned RAII read unit can share SQLite's connection without holding its RefCell borrow across calls; consider nested use and release on errors. BEGIN alone does not pin until the first read. In production every data port must use that same store. Use a two-connection WAL test with an injected writer between generation/query/payload; test next request sees new data and failed requests release snapshot. Do not hold snapshot across model loading, prompts or network waits.

Composition audit at ee7c6fa: every AppRequest is read-only and CLI resume_app/resume_semantic_app share one store across all data ports. Query embedding/model preparation happens before App dispatch. Human search attaches resume/date rows after App returns; CLI and MCP handoff resolve source locations, tool activities and message facts after Search returns. Those composition paths need a nested outer scope through their final DB read, rather than independent per-call snapshots. Doctor bypasses App and reads multiple catalog counters; protect that projection directly. Resume process execution and acknowledgement must remain outside read scopes.

All-request consistency, source authority, alias cleanup and vector changes may be split internally but must retain one correct final integration. Notify main before public schema changes beyond these approved corrections. Ports/app lib are owned here; CLI owns loader diagnostics, model worker owns candle/handoff.

## Source-projection implementation checkpoints (rechecked at 0395394)

These checkpoints constrain the upcoming P0-6/P0-7/P0-8 implementation; they are not claims that a migration exists.

- `commit_source_batches_if_changed` currently folds incoming source payloads and then folds the stored aggregate again. A shorter same-source correction therefore competes with historical text. Distinguish replacement of a source observation from cross-source reconciliation; changing the longer-text tie-break alone cannot fix source authority.
- `SourceReplacementManifest` currently seals membership, placements, activity/usage IDs, scan metadata, resume claims and installation, but not original per-source payloads. New projection evidence must participate in the durable manifest and phase-2 transaction, including changes that leave the aggregate unchanged. Otherwise an apparently harmless source update can be lost and affect a later deletion.
- Both early `sources_are_current` and late `source_batches_are_current` shortcuts must compare required projection evidence, not merely the current aggregate. Parser-version invalidation alone is not sufficient if either content-level shortcut still skips evidence persistence.
- Migration must leave existing source observations explicitly unknown. Never copy the catalog aggregate into every claimant's new projection row. Complete re-ingestion can supply missing evidence; incomplete scans preserve unobserved claims without inventing payload provenance. Tests must distinguish new complete stores from mixed legacy evidence.
- Regeneration currently gathers candidates from memberships inside `regenerate_compatibility_aliases_in_tx`. Removal needs the union of before/after affected entities and placements, collected before replacement; retain batch-scoped SQL rather than restoring whole-catalog maps.
- Test separate-source and same-batch updates in both orders, shorter and equal-length corrections, removal of the winning source, last-source tombstones, incomplete retention, true no-op, manifest tampering, failure rollback and reopen. Compare canonical payload, FTS, aliases and vectors, not only hit counts.
- Rollback is restore-from-consistent-backup or a compatible forward correction, not an automatic schema downgrade. New evidence tables and source reparses must not be discarded by pretending the older binary understands the new authority model.

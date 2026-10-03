# Execution
- [ ] Read curated specs and parent baseline.
- [ ] Recheck P0-4/P0-5/P0-6/P0-7/P0-8; application part P1-10; P3-1/P3-2 at 2edf2dc; add failing regression.
- [ ] Implement smallest root-cause fix in exclusive scope.
- [ ] Run package tests/Clippy and report exact commands/results.
- [ ] Independent check and shared-boundary integration.

No child commit, push or recursive agents. Coordinate shared Cargo target builds.

## 2026-10-03 bounded execution
- Current integration base: 0395394 (first remediation batch pushed; 1895 passing workspace tests).
- First slice: P0-4 filtered search ownership in SQLite and P1-10 Context port error propagation in application, with disjoint implementation ownership and serialized Cargo tests.
- P0-5/P0-6/P0-7/P0-8 are not implicitly included or completed by this slice. Migration checkpoints were refreshed in design.md before attempting source-authority changes.
- SQLite reproduced incorrect repo-filtered ownership before the fix; 12 focused filtered tests passed after the first patch. The strengthened matrix also covers lexical fallback, mixed main/sidechain MainOnly exclusion, no-placement legacy behavior, metadata representatives and grouped results. Final full integration is pending.
- Independent review identified a related existing mismatch in application current-repo boost. It now uses the selected hit owner, preserving None fallback and existing ranking weights. Red tests proved owner mismatch and pre-fix cursor acceptance; all three owner/cursor tests then passed. The search digest revision is now `search-v2-rrf60-signals-v3-matched-owner`; old search cursors are rejected, not silently resumed. List cursors are unchanged.
- Application all-feature suite: 313 passed; scoped all-feature/all-target Clippy passed. Main workspace fmt/Clippy passed and full tests/MSRV are running. Source-authority migration remains design-only.
- Context counterexample was executed with the original `unwrap_or_default`: 2 focused tests passed and `context_activity_read_errors_propagate` failed because it received successful empty activity output. Restoring `?` made all 3 pass; usage errors already propagated and legitimate absent projections remain successful empty output.
- Final independent static review passed the bounded slice, including matched-owner scoring, old-cursor rejection, fallback and MainOnly compatibility. The reviewer did not run Cargo; full integration evidence belongs to main's gate below.
- Main final gate passed: 1904 workspace tests, 0 failures, 22 ignored; fmt, all-target/all-feature Clippy and Rust 1.90 workspace check passed (offline/locked). Python 21/11/54 tests (two skips in total), Node 11, privacy scan and real synthetic debug smoke 10/10 passed. No schema migration or source-projection implementation in this checkpoint.

## P0-5 snapshot slice (base ee7c6fa)
- CatalogStore now exposes a request ReadSnapshot guard and forwards it through shared references; App::handle acquires it for every current read-only request.
- SQLite pins on the first real store_metadata SELECT before returning the first guard; nested guards share the connection without retaining a RefCell borrow. Last guard rollback restores the constructor's query_only=OFF state; writes inside the scope are refused, and cleanup failure poisons future acquisition (current Drop cannot return an error).
- Actual red controls disabled the App guard: a writer before search changed the hit set under old generation; a writer before payload/ownership returned new text under old generation. Restoring the guard passed both. Seven regressions cover eager pin, nested/non-LIFO lifetime, errors/unwind, failed pin, unrelated transaction refusal, write refusal and cleanup poisoning.
- Scoped all-feature tests: SQLite 287 plus 4 process tests, application 313, ports 72 passed; scoped Clippy passed. Main added Human search/handoff/doctor and MCP handoff outer scopes; two success/error release tests passed.
- Independent review caught CLI App construction invoking Git while a view was pinned. App construction was moved before acquisition, and duplicate search dispatch branches unified; lexical mode still skips semantic readiness/model reads. Main is rerunning full gates after that correction.
- No source-authority, vector lifecycle or schema migration is implemented by this slice. Persistent custom adapters/forwarding wrappers must override the default fixture-only no-op snapshot; production port slots use the same SqliteStore.
- Final post-review gate: 1913 passed, 0 failed, 22 ignored; fmt, workspace all-target/all-feature Clippy and Rust 1.90 all-target/all-feature check passed. Python 21/11/54 (two skips), Node 11, privacy/diff checks and real debug-binary smoke 10/10 passed. The child remains in progress for P0-6 through P0-8 and performance work.

## Source-authority slice (base b480627)
- Main reproduced two actual end-to-end defects with the built baseline binary and synthetic temporary JSONL sources: a shorter same-source correction retains the old longer text; deleting the winning source retains that deleted source's text instead of the surviving source. No source corpus or user catalog was touched.
- Added CLI regression `source_projection_corrections_and_deleted_winner_converge`, including lexical removal and a repeated no-op assertion. It is pending the storage implementation, not yet a passing result.
- Hermes' existing parser-version-2 reparse regression now expects the exported current parser semantic version rather than hardcoding 3; it still verifies one reparse then no-op.
- Operator rollout requires a consistent catalog backup before the first writer upgrade and a compatible binary for the resulting schema. An additive migration is not an automatic downgrade path; never restore by merely lowering user_version. Exact source-authority convergence requires trustworthy per-source observations, not invented historical backfill.
- First implementation passed 1925 workspace tests and MSRV, but independent review identified missing negative cases: incomplete rescans could erase generated aliases; legacy intrinsic corrections could conflict in no-op probes; public unscoped writes could diverge from source evidence. These are being reproduced/corrected, so the first green run is not final approval.
- Preserve the accepted one-canonical-manifest optimization (F6). Projection evidence must remain sealed in the durable manifest and CAS checks; reserializing the whole immutable private input inside phase-2 solely to defend private-call misuse is not a new external trust boundary and is not required by this slice.
- Review corrections now preserve aliases on incomplete scans, allow legacy intrinsic corrections to no-op/reconcile, and reject unscoped public writes to source-owned IDs. Scoped tests passed (302 SQLite unit + 4 process); final independent review and integration are running.
- Source descriptors now seal full typed ID and labeled BLAKE3 hashes of original payload/text rather than serializing transcript bytes as JSON number arrays. A descriptor-size regression failed before the change and passed after: a 16,384-fold body increase leaves descriptor size unchanged (<512 bytes in that fixture). This is a bounded encoding test, not a throughput or memory benchmark. Raw source rows still have storage cost.
- Final independent review passed after the three corrections. Main full gate: 1929 passed, 0 failed, 22 ignored; fmt, all-target/all-feature Clippy and Rust 1.90 workspace check passed. Python suites 21/11/54 (two skips), Node 11, privacy/diff checks and real debug-binary smoke 10/10 passed.
- P0-6/P0-7 are verified for this slice. P0-8 remains partial (transactional invalidation implemented; readiness/dimension, historical orphans and remaining write-path audit pending). No scale-performance claim or full child completion.

## Vector-readiness slice (base b6f3d48)
- The storage worker reproduced four behavioral failures: dimension mismatch reported semantic rather than fallback; put retained a changed-body vector; corrections beyond the FTS cap retained vectors across five write routes; explicit rebuild retained historical orphans.
- Added dimension to the SemanticIndex readiness port and exact reference forwarding; App skips readiness for missing embeddings and pure lexical requests, while backend/corrupt-vector errors remain failures.
- All invalidation paths compare full canonical message bodies, not the bounded FTS projection. Put invalidation is in its catalog/FTS/generation transaction; batch/source paths retain source-owned authority checks. Historical orphan cleanup runs only inside the existing writer rebuild transaction and preserves live vectors.
- Worker runtime failed after intermediate green tests; main continued directly. Main verified SQLite 13, application 7, ports 1 semantic-focused tests plus a real CLI readiness/cleanup/recovery e2e before the final review correction.
- Independent review found fallback-to-fallback cursor input drift. Main reproduced an actual failing regression, then retained requested semantic/hybrid embedding in the digest regardless of effective fallback. Eight application semantic tests now pass, including changed dimension, changed same-dimension contents and identical continuation.
- Initial full gate before the cursor regression: 1938 passed, 0 failed, 22 ignored. Final post-fix gate is in progress, not inferred from this earlier result. Historical stale-but-live vectors require explicit `index embeddings`; no provenance backfill or schema bump is claimed.

- Final post-review gate: 1939 passed, 0 failed, 22 ignored; workspace fmt/Clippy and Rust 1.90 checks passed. Python 21/11/54 (two skips), Node 11, privacy/diff checks and real smoke 10/10 passed. The independent reviewer confirmed the corrected fallback digest and tests.
- P0-4 through P0-8 and the application part of P1-10 now have verified scoped implementation. Keep this child in progress for P3-1/P3-2 scale/performance validation; historical stale-but-live embeddings need explicit maintenance, not an automatic correctness claim.

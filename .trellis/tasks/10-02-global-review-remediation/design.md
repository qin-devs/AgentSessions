# Design

## Decisions carried from the approved plan
D0 uses freshly fetched main `2edf2dc3bd6052f54fa1e95070daab462eaf4d86`; old local checkout is 61 commits behind. D4 retains Rust 1.90 and upstream SQLite fixes. Preserve upstream batch-scoped SQL and tail/partial diagnostics.
D1 keeps Human interaction and refuses machine shared-terminal execution; shared ID validation, stateless HTTP preview, POST unsupported.
D2 requires same-source latest and deterministic cross-source authority including removal. Persist source projections if necessary through additive migration; never label historical merged payload as original per-source evidence. Document re-ingestion and rollback limitations.
D3/D5/D6/D9 retain current fail-closed tail, ranking, linear Pi and Experimental defaults; alternatives deferred. D7 aligns schema with actual 1.1, no speculative 1.2. D8 allows CI/artifact validation, not automatic release.

## Partition
Boundaries owns CLI lib/resume/protocol/MCP and CLI e2e. Model/handoff owns candle_embedding/handoff_pack and ports/handoff. Providers owns provider crates. Storage owns SQLite, ports/lib and application/lib. Read surfaces owns serve/Web/TUI/source_fs and schema. Release owns workflows/release scripts. Main owns task/spec/public docs, integration, commit and push. Coordinate cross-owned APIs; never revert another worker.

## Validation and rollback
Add regression before fix, then package tests, independent check and integration gates. Schema/cache/ID changes need compatibility notes. Revert isolated commits, preserving new data; never remove validation or reuse stale vectors to roll back. Tests never modify source corpora.

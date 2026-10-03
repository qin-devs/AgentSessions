# Design
Follow parent decisions and revised finding semantics.

## Exclusive ownership
workflows; release scripts; CI/privacy regression tests. Main owns docs, task artifacts, commit and push; never revert another worker.

## Approach
Retain Rust 1.90 and upstream deps; add actual MSRV gate if missing. Resolve immutable source_commit first; quality/build/manifest use identical SHA. Binary archive distribution remains. Artifact-only rehearsal may use a validated immutable ref with read permissions; no automatic official tag/release. Keep pinned actions. Test trigger/target mismatch cannot bypass quality gate. Privacy expansion uses configurable synthetic identifiers and boundary/false-positive cases, never real usernames. No automatic advisory ignore or removal of semantic features.

## Compatibility
Retain contracts except specified corrections. Document migration/cache changes. Revert isolated commits only after accounting for new data; never remove validation to roll back.

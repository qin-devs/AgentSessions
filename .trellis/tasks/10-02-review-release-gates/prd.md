# release-gates

## Goal
Close P0-11/P2-10/P3-4; A7/A8 from the owner-approved combined plan. Baseline 2edf2dc; approval and constraints are in the parent PRD.

## Requirements
Retain Rust 1.90 and upstream deps; add actual MSRV gate if missing. Resolve immutable source_commit first; quality/build/manifest use identical SHA. Binary archive distribution remains. Artifact-only rehearsal may use a validated immutable ref with read permissions; no automatic official tag/release. Keep pinned actions. Test trigger/target mismatch cannot bypass quality gate. Privacy expansion uses configurable synthetic identifiers and boundary/false-positive cases, never real usernames. No automatic advisory ignore or removal of semantic features.

## Acceptance
- [ ] Baseline counterexamples and regressions cover described input/error boundaries.
- [ ] Package tests/lint pass; no swallowed error or unrequested expansion.
- [ ] Independent integration check verifies shared contracts.
- [ ] Evidence and untested limits recorded honestly.

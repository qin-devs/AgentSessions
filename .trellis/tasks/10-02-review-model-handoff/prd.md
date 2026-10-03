# model-handoff

## Goal
Close P0-9/P0-10; model-cache part P1-10 from the owner-approved combined plan. Baseline 2edf2dc; approval and constraints are in the parent PRD.

## Requirements
Require complete required-content coverage in manifest, not self-hash; reject empty/missing/duplicate/unsafe entries. Preserve actual parsing and size/hash checks. No partial bundle bypass. Process-local failures need explicit bounded recovery semantics; preserve success caching. Enforce handoff content bytes across all retained fields; error if mandatory content cannot fit; recompute confidence after evidence trim. Versioned tagged ID encoding distinguishes since/until and None/empty. Do not invent repo/sidechain fields. Test tiny budgets, long query/tool metadata, no evidence and ID collisions. Coordinate loader diagnostics with CLI owner.

## Acceptance
- [ ] Baseline counterexamples and regressions cover described input/error boundaries.
- [ ] Package tests/lint pass; no swallowed error or unrequested expansion.
- [ ] Independent integration check verifies shared contracts.
- [ ] Evidence and untested limits recorded honestly.

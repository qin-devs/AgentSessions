# storage-consistency

## Goal
Close P0-4/P0-5/P0-6/P0-7/P0-8; application part P1-10; P3-1/P3-2 from the owner-approved combined plan. Baseline 2edf2dc; approval and constraints are in the parent PRD.

## Requirements
Repo/facet-filtered ownership uses matching placement/session deterministically. Generation/search/payload/ownership use one connection/read transaction; no process-only lock. Same-source latest replaces old text; cross-source deterministic merge/removal uses live projections. Preserve full/incomplete claimant semantics. Alias candidates include entities losing this source but retained by another. Invalidate vectors on final content change; remove orphans; model/dimension readiness matches query. Context activity errors propagate. Preserve upstream batch scope. Additive migration cannot invent historical per-source content.

## Acceptance
- [ ] Baseline counterexamples and regressions cover described input/error boundaries.
- [ ] Package tests/lint pass; no swallowed error or unrequested expansion.
- [ ] Independent integration check verifies shared contracts.
- [ ] Evidence and untested limits recorded honestly.

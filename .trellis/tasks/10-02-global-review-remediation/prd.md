# Verified global review remediation

## Goal
Implement the confirmed corrections in the owner-approved combined review without repeating upstream fixes or implementing withdrawn findings.

## Approval
On 2026-10-02 the owner explicitly approved implementing the revised combined plan and authorized commits, pushes and isolated worktrees. Source: AgentSessions combined review dated 2026-10-02, SHA256 `701684235607a327788a0b5d231c144b16100d2d070b9c3e8f5dfc138626d6f6`. This is the subsequent execution approval for that reviewed plan, not an initial feature request.

## Requirements
- Close confirmed safety, data authority, model/handoff, provider fidelity and entrypoint contract defects with regressions.
- Track all 37 original IDs, C1/C2 and A1-A8, distinguishing repairs, upstream revalidation, deferred decisions and withdrawn diagnoses.
- Bind release quality/build/provenance to one immutable commit; preserve read-only HTTP and binary archive distribution.
- Preserve live source claims, canonical identities and privacy; use synthetic fixtures.
- Retain existing fail-closed UTF-8, ranking, Pi lineage and provider defaults. Optional alternatives and pruning require separate approval.
- Preserve the dirty original checkout. No force push, history rewrite, official release/tag, removal of other worktrees, maturity promotion or unsafe bypass.

## Acceptance
- [ ] Each approved defect has a baseline counterexample and passing regression, or evidence-backed non-applicability.
- [ ] Final fmt, Clippy, workspace all-features tests, Rust 1.90 locked check, Python/Node and privacy gates pass.
- [ ] Migration/rollback, provider variants and protocol identity cases are tested; untested platforms remain explicit.
- [ ] Build/smoke/packaging and remote CI results are recorded; no unpublished release is claimed.
- [ ] Every child/optional decision has an honest state; no broad task is closed from one successful batch.

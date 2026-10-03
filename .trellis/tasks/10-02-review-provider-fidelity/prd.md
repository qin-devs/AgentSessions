# provider-fidelity

## Goal
Close P1-2/P1-3/P1-4/P1-5/P1-6/P1-7/P2-5/P2-6 from the owner-approved combined plan. Baseline 2edf2dc; approval and constraints are in the parent PRD.

## Requirements
Normalize format-proven epoch units in Cursor/Cline and single-document BOM; never numeric timestamp to empty string. Grok promptId is not durable session ID; retain whitespace/rewind. OpenClaw thinking-only loss gets skipped/diagnostics without indexing reasoning. Codex payload.id is SID only in session_meta, reject conflicting aliases. Kimi probe accepts supported prompt/steer discriminator, not arbitrary JSON. Qoder SID/cwd must come from one authoritative record/session. Temporary DB files are exclusive/private; only successful creation owns cleanup; conflict never deletes another file. Cover Cursor/OpenCode and check new Hermes for same pattern. Preserve Pi and provider maturity.

## Acceptance
- [ ] Baseline counterexamples and regressions cover described input/error boundaries.
- [ ] Package tests/lint pass; no swallowed error or unrequested expansion.
- [ ] Independent integration check verifies shared contracts.
- [ ] Evidence and untested limits recorded honestly.

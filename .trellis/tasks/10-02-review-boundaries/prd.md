# boundaries

## Goal
Close P0-1/P0-2/P0-3; CLI part P1-10 from the owner-approved combined plan. Baseline 2edf2dc; approval and constraints are in the parent PRD.

## Requirements
Reject leading-option native IDs using checked-ID rules shared by preview/execute; keep valid formats and quoting. Human interaction remains; JSON/JSONL/Robot refuses shared-terminal execution with structured errors, no fake success or null stdio. Preserve first-preview confirmation. Normalize unknown command and redact free-text diagnostic/error/details without changing request_id/cursors/stable IDs. Prove with fake providers that rejected inputs and machine mode never spawn. Model selection distinguishes absent, broken and vector-not-ready states; do not swallow loader errors. Coordinate a stateless preview API with serve owner.

## Acceptance
- [ ] Baseline counterexamples and regressions cover described input/error boundaries.
- [ ] Package tests/lint pass; no swallowed error or unrequested expansion.
- [ ] Independent integration check verifies shared contracts.
- [ ] Evidence and untested limits recorded honestly.

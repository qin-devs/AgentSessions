# Design
Follow parent decisions and revised finding semantics.

## Exclusive ownership
CLI lib.rs/protocol.rs/mcp.rs plus application/src/resume.rs and tests/e2e.rs. Main owns docs, task artifacts, commit and push; never revert another worker.

## Approach
Reject leading-option native IDs using checked-ID rules shared by preview/execute; keep valid formats and quoting. Human interaction remains; JSON/JSONL/Robot refuses shared-terminal execution with structured errors, no fake success or null stdio. Preserve first-preview confirmation. Normalize unknown command and redact free-text diagnostic/error/details without changing request_id/cursors/stable IDs. Prove with fake providers that rejected inputs and machine mode never spawn. Model selection distinguishes absent, broken and vector-not-ready states; do not swallow loader errors. Coordinate a stateless preview API with serve owner.

## Compatibility
Retain contracts except specified corrections. Document migration/cache changes. Revert isolated commits only after accounting for new data; never remove validation to roll back.

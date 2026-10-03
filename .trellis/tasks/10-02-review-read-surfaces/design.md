# Design
Follow parent decisions and revised finding semantics.

## Exclusive ownership
CLI serve.rs/web/tui and web tests; adapters-sqlite/src/source_fs.rs for C2; schemas/robot; no lib.rs/e2e without coordination. Main owns docs, task artifacts, commit and push; never revert another worker.

## Approach
GET resume preview never creates/consumes CLI ack; POST remains unsupported. Forward existing handoff provider/time/evidence options and budgets; HTTP Context supports max_bytes. Align schema with actual 1.1 and validate real frames. Show raw/talks/summary and structured truncation, preserving warnings/messages. TUI literal m/k input works alongside discoverable modified shortcuts; inject current_repo. Do not equate historical tool result with request Outcome. Reproduce conditional symlink/short-read/hint defects before repair; retain deletion safety.

## Compatibility
Retain contracts except specified corrections. Document migration/cache changes. Revert isolated commits only after accounting for new data; never remove validation to roll back.

# Baseline
- Approval: explicit implementation of the revised plan with commit/push/worktree permission.
- Source: `2edf2dc3bd6052f54fa1e95070daab462eaf4d86`, fetched main on 2026-10-02.
- Original: `5b232cdedbff33a251e7fb266558be7af8496e1a`, 61 commits behind; 8 dirty entries preserved.
- Branch: `fix/global-review-remediation-20261002`, isolated worktree.
- Retain upstream: 27a39e7/ada5f21 MSRV/Clippy; fd918f1/525a0c6/5ba9939/748c720 SQL scope; 6979aa9/107578d tail/partial; newer Cursor/Hermes and Node24 actions.
- No CodeGraph index in new worktree; do not treat the old index as current source. Use ast-grep outline and targeted reads; do not create an index.
- Research: sqlite.org/isolation.html and sqlite.org/wal.html describe first-read snapshot pinning. Command uses argv but leading hyphens remain options; no unverified universal -- or null stdio.
- Fresh tests pending. Prior 1774-test result belongs to the old checkout.

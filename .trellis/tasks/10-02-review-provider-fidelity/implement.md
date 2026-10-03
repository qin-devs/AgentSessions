# Execution
- [x] Read curated specs and parent baseline.
- [ ] Recheck P1-2/P1-3/P1-4/P1-5/P1-6/P1-7/P2-5/P2-6 at 2edf2dc; add failing regression.
- [ ] Implement smallest root-cause fix in exclusive scope.
- [ ] Run package tests/Clippy and report exact commands/results.
- [ ] Independent check and shared-boundary integration.

No child commit, push or recursive agents. Coordinate shared Cargo target builds.

## Timestamp/BOM slice (base df9746f)
- Cursor P1-2 and Cline P1-3 are implemented as disjoint provider slices with serialized Cargo gates. No dedicated Cursor/Cline spec indexes exist; actual generic ports/testkit specs and committed provenance guide this work, not Codex-only field rules.
- Cursor must cover legacy ItemTable chatdata/prompts and current disk-kv. Cline must keep single-document BOM handling local to its reader/probe/parser, not duplicate the existing JSONL reader's first-line handling.
- Main owns CLI time-boundary/reparse tests and parser semantic version 4 -> 5. Existing unchanged sources must be reparsed once; schema version stays 19.
- Conversion must use demonstrated field/variant units, retain equivalent instants, preserve native identity and source bytes, and keep the existing inclusive-since/exclusive-until contract. Cross-source and reconstructed-ID upgrade implications must be checked explicitly.
- Temp DB ownership, other provider fidelity and maturity changes are not part of this slice.


## Timestamp/BOM integration evidence
- Existing Cursor/Cline fixtures and provider tests reproduce the original unit,
  absent/empty-time and leading-BOM defects; evidence is pinned in each provider's
  golden PROVENANCE. Cline fixture revision is 2; all other providers remain 1.
- SQLite's original two-copy ItemTable test failed on the decimal/RFC timestamp
  conflict, then passed with the proven aggregate-only alias. Expanded tests
  exposed missing batch-scoped Document proof for an unscanned claimant; loading
  only referenced missing proofs fixes that without broadening aggregate IDs.
- The five `cursor_timestamp_upgrade_*` storage regressions now pass: both orders,
  1ns differences, three-source null folding, per-claimant document proof, complete
  removal versus incomplete retention, exact raw evidence, no-op and other stable
  field conflicts. The final CLI tests also exercise actual identical snapshots,
  both rolling upgrade orders, exact raw observations, source bytes, stable IDs,
  half-open millisecond/nanosecond filters and unchanged-source no-op behavior.
- Independent final read-only review covered production, specs and all new CLI
  regression assertions; no blocking finding remained. The reviewer independently
  ran rustfmt/diff checks, not Cargo; Cargo results below were executed by main.
- Final Windows all-feature workspace gate: **1966 passed, 0 failed, 23 ignored**.
  The additional ignored case is a manual Cline golden-output printer, not a
  disabled regression; the new golden assertion and all timestamp tests ran.
  All-target/all-feature Clippy with -D warnings, fmt and Rust 1.90 locked/offline
  all-target/all-feature checks passed. Python 21/11/54 (two skips), Node 11,
  privacy/diff/context checks and actual debug-binary smoke 10/10 passed.
- P1-2/P1-3 scoped implementation is verified locally. Remote checks must bind the
  pushed SHA; prior df9746f results do not certify this slice. Other provider work
  and the parent remediation task remain open; no archive, release or promotion.

### Root-cause prevention
- Categories: cross-layer representation contract, test coverage gap, and implicit
  numeric-unit assumptions. Provider-only green tests cannot prove catalog rolling
  upgrade safety; copied sources share IDs even when no native ID is adopted.
- A pairwise null-converging fold can conceal disagreements among non-null original
  timestamps. Prove units per claimant and compare originals before folding.
- Float-token decoding can destroy the evidence needed to reject fractional values.
  Test raw numeric lexemes, not already rounded Values. Shared ports/storage specs
  now pin these contracts; no broader provider or parser fallback is introduced.


Implementation checkpoint: `9a36ad6` (provider code, storage compatibility, CLI
regressions, specifications and CHANGELOG).

### Final verification commands
```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --offline --locked -- -D warnings
cargo test --workspace --all-features --offline --locked --no-fail-fast -- --test-threads=2
cargo +1.90.0 check --workspace --all-targets --all-features --offline --locked
python -m unittest discover -s scripts -p 'test_*.py'
python -m unittest discover -s scripts/release -p 'test_*.py'
python -m unittest discover -s scripts/evidence -p 'test_*.py'
node --test crates/agent-session-grep-cli/tests/web_ui.test.cjs
python scripts/evidence/privacy_scan.py --repo .
python .trellis/scripts/task.py validate 10-02-review-provider-fidelity
python scripts/verify-release.py --asg <built-debug-binary>
git diff --check
```
All source inputs were synthetic and source-byte preservation was asserted.
The existing expected Cursor JSON has only six semantic timestamp differences;
its generated output is now LF to satisfy ordinary diff checks without a
whitespace override. The three existing Cursor binary fixtures remain unchanged.

# Changelog

All notable changes to agent-session-grep are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

> Planned first public version: `0.1.0`. No tag or release has been published.

### Added

- Cursor `state.vscdb` gains a second, additive variant (`cursor/disk-kv-v1`)
  covering the `cursorDiskKV` table (`composerData:<id>` metadata +
  `bubbleId:<composerId>:<bubbleId>` bodies), a surface the ItemTable-only
  adapter never read. Variant selection is decided by the bytes alone
  (`cursorDiskKV` plus a `composerData:` key vs. ItemTable `chatdata`/`prompts`):
  the two variants are mutually exclusive, and a double claim or a broken shape
  is refused instead of guessed. Messages follow `fullConversationHeadersOnly`
  strictly - never KV key, rowid or insertion order (pinned by two synthetic
  fixtures whose KV insertion orders are shuffled) - and a missing, NULL,
  malformed, wrong-shape or invalid-UTF-8 bubble keeps its slot, is counted in
  `skipped` and is named with its exact storage key instead of silently
  disappearing. Tool input prefers `rawArgs` and falls back to `params`, accepts
  object and JSON/plain-string encodings, and is only ever treated as text
  (never executed). Every database is read through one pinned read transaction
  on a private copy of the captured snapshot: the parser never opens the source
  file, so source DB/WAL/SHM stay untouched and a concurrent commit cannot
  change one parse's observation. Messages report no adopted native id
  (`bubbleId` is a composer-scoped storage-key component and Cursor's private
  storage has no official version contract), so canonical message identity
  stays document-scoped; verbatim composer/bubble ids appear only as
  observations. Composers, headers, cells and total bytes are bounded, and
  exceeding a bound fails the source explicitly instead of truncating it.
  Evidence: BLAKE3-pinned synthetic goldens plus adversarial unit tests in
  `crates/agent-session-grep-provider-cursor/` and CLI ingest/search/bad-bubble
  end-to-end tests in `crates/agent-session-grep-cli/tests/cursor_disk_kv.rs`.
  Maturity stays Experimental: no official versioned storage contract was
  located, so only the pinned Wake implementation and the synthetic probes
  support the format (recorded in the crate's golden PROVENANCE).
- Hermes `state.db` sessions are now indexed by a second, additive variant
  (`hermes/sqlite-state-v1`) covering `~/.hermes/state.db` and
  `~/.hermes/profiles/<name>/state.db`, a surface the JSON-only adapter never
  read. Variant selection is decided by the bytes alone (SQLite header plus the
  `sessions`/`messages` key columns vs. a JSON object document), so the two
  variants are mutually exclusive and a double claim is refused instead of
  guessed. Each database is read through one pinned read transaction on a
  private copy of the captured snapshot: the parser never opens the source file
  (the capture step's read-only WAL attach can still write `-shm` read marks, as
  recorded in the crate's golden PROVENANCE), so a concurrent committed write
  cannot change what one parse observes. Sessions are enumerated per database, messages follow
  `ORDER BY timestamp, id`, REAL unix seconds are normalized to millisecond UTC
  instants, and NULL timestamps stay absent instead of being back-filled from
  `started_at`. Both documented tool-call shapes (`{name,arguments}` and
  `{id,function:{...}}`) are decoded, but every call/result association stays
  non-authoritative: no native call id is synthesized, no parent edge and no
  tool activity is emitted, and unmatched or ambiguous associations are reported
  as bounded diagnostics. Message rowids are not adopted as canonical identity
  (they are per-database and reused), so messages keep document-scoped ids — the
  same decision recorded for Pi. Rows, cells, total bytes and tool calls are
  bounded, and exceeding a bound fails the source explicitly instead of
  truncating it. Evidence: a BLAKE3-pinned synthetic `state.db` golden plus unit
  tests for ordering, NULL handling, bad rows, both tool-call shapes, budgets
  and WAL/source immutability under
  `crates/agent-session-grep-provider-hermes/`, and a CLI end-to-end
  profile-isolation and search test in
  `crates/agent-session-grep-cli/tests/hermes_state_db.rs`. Known gaps: no
  discovery root for `state.db` yet, and an already registered shallower
  installation root currently absorbs a profile database — both recorded in
  `docs/product/PROVIDER-BETA-READINESS.md`.
- The loopback `serve` URL now carries its session token in the URL **fragment**
  (`http://<addr>/#token=…`) instead of the query string. A fragment is never
  sent to the server, so the token cannot reach a request log, an access log,
  or the `Referer` header of a later navigation; the embedded UI reads it from
  `location.hash`, still accepts a query-string token for a hand-typed or
  bookmarked URL, and strips the token from the visible URL either way so it
  does not persist in browser history. Idea from cc-sessions-viewer's
  `web-server-mode.md` (that repository has no LICENSE file — spec-level idea
  only; the implementation matches our existing token handling).

- `kimi-code` now indexes the user's own prompts. Kimi writes them as
  `turn.prompt` (and `turn.steer` for a mid-turn correction) with the text in a
  top-level `input` block array, not as `context.append_message`; the adapter
  parsed only the latter, so the highest-value text in a Kimi session — what the
  user actually asked — never entered the index, and the golden fixture carried
  no such record for a test to catch it. Both record types now emit as user
  messages in file order, with the record shape taken from the upstream ctx
  adapter's real-shape fixture. `context.append_loop_event` step/tool events
  stay unparsed on purpose: they are tool activity rather than messages, and
  wire.jsonl carries no per-message native id to anchor them to.

- Pi session-tree lineage is now reported instead of silently flattened. Pi
  format v2/v3 files are a parent-linked tree (`version` on the `session`
  header, per-record `id` + `parentId`; two records sharing one `parentId` are
  a retried/abandoned branch). The adapter still indexes **every** branch's
  text in file order — search completeness comes first — but the parse report
  now carries an explicit diagnostic naming the declared version and the number
  of lineage-bearing records, and `probe` reports both as matched evidence. No
  parent edges are emitted: canonical message identity adopts a provider native
  id verbatim and without a provider namespace, while real Pi record ids are
  8 hex characters scoped to one file, so promoting them would merge messages
  from different sessions onto one entity. `context` therefore stays
  Unsupported with the reason recorded per row — the blocker is
  document-scoped native message identity in the composition root, not a
  missing format fact. Pinned by a new synthetic branch fixture
  (`crates/agent-session-grep-provider-pi/tests/golden/v3-branched.jsonl`,
  BLAKE3-pinned, both branches indexed) plus positive/negative diagnostic
  guards in the golden and property suites.
- Repo filter parity across the machine surfaces: the MCP `search_sessions`
  tool takes `repo` (declared in its tool schema with the same length bound
  the derivation uses, enforced at runtime), the Web/loopback search routes
  forward a `repo` query parameter, and `hook <event>` takes `--repo` — all
  with the CLI `--repo` semantics (verbatim three-segment `host/owner/name`
  slug; blank values are `invalid_request`, never a silent "no filter").
  `generate_handoff` keeps no repo dimension on either surface.
- Repo identity (schema v16 `session_repo_slugs` projection): sessions are
  grouped by the `host/owner/name` slug derived from their pair-observed
  working directory via local git detection (`git rev-parse --show-toplevel`
  + `remote get-url origin`, cached per directory — Recall's repo identity
  and sessiongrep's `find_repo_root` patterns). Only the three-segment slug
  is stored — absolute paths never enter the projection (privacy contract).
  A stored row means detection succeeded; no row means unknown (honest
  degradation — directories that no longer exist, non-git directories, or
  remotes without `origin` are never guessed). The projection rebuilds in
  the same transaction as `session_fts` (affected-session commits and
  `index rebuild`), and is removed with its session. `search --repo <slug>`
  filters hits to sessions of that repo; `status` reports per-repo session
  counts (`repos` key, sessions desc + slug asc). Detection failures never
  fail sync or index.
- Token usage tracking (usage dimension, schema v15 `usage_events` +
  `usage_event_membership` projection): only numbers the provider format
  explicitly gives are recorded — never estimated from text length or any
  other proxy. `claude-code` extracts observed per-message `message.usage`
  (input/output/cache_read/cache_write, anchored to the assistant record's
  uuid); `codex` derives per-event increments from `event_msg/token_count`
  cumulative totals under monotonic validation (98% stale-regression guard,
  fork-child inherited baseline absorbed, per Recall's derivation rules).
  Session-level events (no message anchor) are stored with a NULL message id —
  anchors are never invented. A stored row means "the provider reported
  usage" (coverage marker: absence of rows means unknown, not zero).
- `status` reports usage totals (`usage.sessions` + five bucket sums +
  observed/derived event counts); zero-session usage is reported as "no
  usage facts", distinct from a missing projection. `doctor` reports
  `usage_storage: true` and orphaned usage projection counts, purged by
  `index purge-activities` in the same transaction.
- Per-provider `usage` capability column (capability matrix + robot
  `providers` envelope): `claude-code` Native, `codex` Derived; the other 12
  implemented providers honestly report Unsupported with per-format evidence
  comments (pi/kimi-code/tencent-codebuddy/cline/opencode/cursor/grok-build
  formats carry usage fields but their adapters lack per-message native ids
  or field parsing; aider/hermes/antigravity/openclaw/qoder formats carry no
  usage facts).

- 16-provider capability matrix (`agent-session-grep-ports`) with deferred
  provider rows (`deepseek-harness`, `zcode`) and per-provider maturity grading.
- Provider adapters for the 14 implemented, Experimental providers:
  `claude-code`, `codex`, `grok-build`, `antigravity`, `opencode`, `pi`,
  `hermes`, `cursor`, `kimi-code`, `openclaw`, `qoder`, `tencent-codebuddy`,
  `cline`, and `aider`. Each adapter has evidence-backed synthetic fixtures;
  DeepSeek Harness and ZCode remain deferred because no transcript evidence is
  available.
- Source installers install both `agent-session-grep` and `asg`: Windows uses
  two executable copies; Unix uses a managed symlink or wrapper. Upgrade and
  uninstall are idempotent and refuse unrelated aliases.
- `handoff <query>` CLI subcommand — deterministic handoff-pack/v1 generation
  with evidence/inference separation and budget truncation.
- `resume <session-id>` CLI subcommand — dry-run by default (prints the
  provider command, original working directory, and permission mode);
  `--yes` spawns the provider in that directory. Providers whose resume
  command is unverified report `available:false` rather than a fabricated
  command.
- `search --mode lexical|semantic|hybrid` — retrieval mode selection. The
  default vector mode uses bigram hashes for fuzzy lexical matching, not a
  semantic model; hybrid combines lexical and vector rankings with RRF.
  Semantic and hybrid metrics remain informational and carry no release
  threshold or quality claim. A real local semantic backend is available
  through the optional `semantic-candle` cargo feature (off by default).
- `hook <session-start|user-prompt-submit>` CLI subcommand — Claude Code hook
  integration, disabled by default. Reads the hook payload from stdin and emits
  the `hookSpecificOutput.additionalContext` contract; nothing is injected
  unless `--enable` is passed. Injected text is redacted (ADR-0009).
- `serve --port <n>` CLI subcommand — loopback HTTP server (random bearer
  token, Host loopback check, embedded Web UI, JSON API).
- MCP tools: `search_sessions`, `get_session_context`, `get_session_resume`, `get_message`, `list_sessions`, `generate_handoff`, `list_providers`, `get_status`, and `doctor` (9 total).
- `list_sessions` Peek Bundle (borrowed from hstry): every session entry
  carries a `peek` object with `first_user_text` / `last_user_text` (≤200
  chars each, char-boundary truncation; null without user messages), capped at
  1 KiB serialized per session with the cap and over-limit truncation guarded
  by tests. Peek bytes are charged to the `max_response_bytes` gate.
- `list_sessions` derived session titles (borrowing list #6): session entries
  carry a `title` string (≤80 chars, char-boundary truncation; omitted when no
  candidate exists) derived by the chain custom title (`title` field) > AI
  summary (`summary` field) > first valid user message. Injected noise is
  filtered at provider parse time, so the first committed user message is the
  first valid one. Titles live in a rebuildable schema v13 `session_titles`
  projection maintained in the same transaction as the session FTS rows, and
  title bytes are charged to the `max_response_bytes` gate like peek bytes.
  The human list renderer shows the title instead of the raw payload preview.
- Parser-semantic version in `sync` unchanged detection (borrowed from
  Recall's parser-version incremental sync): schema v14 adds
  `source_scans.parser_version` (migration default `0`), and the unchanged
  judgment now compares `(len_bytes, fingerprint, parser_version)` against
  the binary's `PARSER_SEMANTIC_VERSION` constant. When parsing semantics
  change (the constant is bumped), sources with unchanged bytes are
  re-parsed once on the next `sync` — targeted backfill, with the reparse
  reason reported through the sync warnings channel — instead of keeping
  stale parsed content until a manual `index rebuild` or a source-file
  change. No full rebuild is required.
- Cross-boundary output redaction (ADR-0009): Robot JSON/JSONL, MCP, HTTP API,
  Handoff Pack, and Web UI redact standalone and prose-embedded secrets
  (AWS keys, GitHub PATs, OpenAI/Anthropic/xAI keys, Bearer tokens, PEM
  private keys, secret-named JSON fields). Human CLI/TUI output stays
  unredacted (ADR-0004).
- Bounded ingestion (RFC-0002 §7): `ingest`/`sync` read every source through
  `ReadOnlySource`/`BoundedLineReader` with per-format caps (JSONL 8 MiB per
  record, JSON-family 32 MiB, SQLite 128 MiB); an oversized source or record
  fails closed (`SourceTooLarge`/`RecordTooLarge`) instead of loading the whole
  file into memory.
- Global `--offline` flag: fails closed on any future network-dependent
  capability (`capability_not_supported`, exit 7); `doctor`/`hook` report the
  flag. The default build carries no HTTP client dependency and the only socket
  is `serve`'s loopback listener, enforced by `tests/network_egress.rs` and a
  `security-audit` CI step.
- `hook` gains `--provider` (repeatable) and `--decay-days` filters, wired into
  the same `SearchFilters` the CLI and MCP use.
- Session metadata search (schema v11 `session_fts`): search by resolved
  provider session id, pair-observed original working directory, or the first
  user request in a session, without indexing absolute source paths.
- `serve` hardening: bounded worker pool, request size limits, Host/Origin
  loopback checks, forwarded-header rejection, a POST CSRF gate, and an
  `/api/projection/search` alias shared with the consistency harness.
- `tui --snapshot-json <query>`: headless structural search projection over the
  same Application path, so the release harness needs no terminal automation.
- 12 additional provider golden fixtures (grok, antigravity, opencode, pi,
  hermes, cursor, kimi, openclaw, qoder, codebuddy, cline, aider): synthetic
  transcript plus `PROVENANCE.md`, pinned canonical output, span round-trip,
  and a read-only checksum regression, alongside the existing Claude/Codex
  golden evidence.
- Seeded randomized property suites for six more provider adapters
  (`kimi-code`, `openclaw`, `qoder`, `tencent-codebuddy`, `cline`, `aider`),
  closing the coverage gap that previously left randomized property tests to
  Claude/Codex. Fixed-seed xorshift64* transcripts assert span/seq/count/
  metadata invariants against independent ground truth; deterministic
  golden-source mutations (truncate/insert/delete/byte-flip, split and
  shuffled lines) must never panic, never mutate the source bytes, never
  report a partial commit, and stay byte-identical across re-parses; and
  `probe` over arbitrary bytes must never panic and must report only the
  adapter's own variant with non-ambiguous confidence
  (`crates/agent-session-grep-provider-*/tests/properties.rs`).
- Resume execution contract: the first run forces a preview acknowledgement
  before any real spawn, the provider binary is preflighted, and a drift test
  keeps the capability matrix and the resume command builder aligned.
- `scripts/evidence/privacy_scan.py`: scans tracked text for personal or
  machine-specific absolute paths, alongside
  `docs/operations/PUBLIC-HISTORY-SCRUB.md` for the history decision.
- `scripts/verify-release.py` expanded to 10 checks (binary/version, sync,
  lexical search, get, context, resume metadata and dry-run, deterministic
  handoff, semantic/hybrid effective modes, hook default-off, provider matrix).
- `scripts/rehearsal/compare_entrypoints.py`: the five-entry-point consistency
  harness now directly compares all five surfaces (CLI, MCP, Robot, Web, TUI)
  for the canonical search operation — Web launches a real loopback `serve`
  process and TUI drives `--snapshot-json`. An unimplemented entry point fails
  the harness instead of being recorded as a skip.
- Optional local semantic backend (`semantic-candle` cargo feature, default
  off): Candle 0.10 + pinned
  `intfloat-multilingual-e5-small@614241f6-candle-f32-meanpool-l2-qpass-v1`
  (384 dims, mean pooling, L2, `query:`/`passage:` prefixes). `model import
  --dir <bundle>` verifies every declared SHA-256 and atomically publishes the
  bundle into the local model cache (never downloads); `model status` reports
  whether the default E5 bundle is imported. Default builds stay
  bigram-hash / lexical-only, and missing weights keep the explicit
  `lexical_fallback` behavior. See `docs/operations/SEMANTIC-MODEL-BUNDLE.md`.
- Handoff packs project authoritative per-message facts: each mainline entry
  now carries `role` and `is_sidechain` (missing facts render `unknown` /
  `false`, never fabricated), and the pack carries the catalog `tool_activity`
  list for its hit messages (`handoff-pack/v1` schema updated).
- TUI search facet controls: `m` cycles the sidechain facet
  (include → main-only → subagent-only) and `k` cycles the tool-kind facet
  (any → file → command → web → query → unknown) with an empty input,
  reissuing the search through the same `SearchFacets` contract as CLI/MCP.
- Context responses (`context` CLI, `get_session_context` MCP) project
  `tool_activities` for the assembled messages via the ContextGraphStore batch
  read; empty when nothing is stored, never fabricated.
- Provider Beta readiness ledger (`docs/product/PROVIDER-BETA-READINESS.md`):
  per-provider local vs external Beta blockers, so no provider is promoted
  from code existence alone.
- Seeded randomized property suites (`tests/properties.rs`) for `grok-build`,
  `antigravity`, `opencode`, `pi`, `hermes`, and `cursor`, mirroring the
  existing claude-code/codex coverage: deterministic xorshift64* corpora pin
  span round-trips, seq/committed self-consistency, parse determinism,
  read-only sources (RFC-0002 §7), probe never-panics on arbitrary bytes, and
  no-panic legal output under seeded mutations of each golden fixture
  (truncate / insert / delete / flip / split / shuffle). The readiness ledger
  gains a `property` column guarded in both directions against each suite's
  existence (`beta_readiness_property_column_matches_properties_test_existence`).
- Resume command matrix extended with authoritative upstream evidence:
  `antigravity` → `agy --conversation <id>` (fast-resume adapter +
  agent-sessions builder), `opencode` → `opencode <directory> --session <id>`
  with the directory positional omitted when unknown (fast-resume adapter;
  cc-switch/agf corroborate the `--session`/`-s` flag), `kimi-code` →
  `kimi --session <id>` (fast-resume adapter, same wire.jsonl source face),
  and `tencent-codebuddy` → `codebuddy --resume <id>` (AgentRecall, same
  `~/.codebuddy/projects` JSONL face). Capability matrix resume columns move
  to Derived for these four; `hermes` (conflicting `--resume`/`--session`/no
  CLI claims across reference projects), `qoder` (no resume evidence), and
  `cursor` (CLI `agent --resume` belongs to a different store.db surface)
  stay Unknown rather than fabricate commands.
- Honest `tool_activity` accounting for the seven providers whose formats
  were reviewed for structured tool-call records: `grok-build` (ACP
  `_meta.bashCommand` meta chunks), `antigravity` (`tool_calls`),
  `qoder` (`tool_use`/`tool_result` record types), and `kimi-code` (loop
  `step`/`tool` events) do carry structured tool records, but none of the
  seven formats carries a durable per-message native id — messages report
  empty native ids, so activities cannot anchor (staging fail-closed drop,
  R5.3); `pi`, `openclaw`, and `tencent-codebuddy` documented format
  knowledge carries no structured tool-call records. All seven stay
  `Unsupported` in the capability matrix, with the reason recorded in the
  Beta readiness ledger and pinned by new golden-corpus drift tests in each
  provider crate.
- Pseudo-user noise filtering in the provider parse layer: `claude-code`
  skips harness-injected user-role records by explicit envelope shape only
  (`<system-reminder>`, `<local-command-caveat>`/`Caveat:`, exact
  `[Request interrupted by user]` markers, empty-arg `<command-name>`
  envelopes, `<local-command-stdout>`/`<local-command-stderr>`,
  `<bash-input>`/`<bash-stdout>`/`<bash-stderr>`,
  `<user-prompt-submit-hook>`, `<task-notification>`, and whole-line
  `[Image: …]` isMeta image references — every rule cites its format
  evidence), and `codex` skips `# AGENTS.md` title and
  `<environment_context>` user injections (cc-switch rollout evidence).
  Filtered records count into `ParseReport.skipped` with a diagnostic and
  never enter the index; command envelopes carrying user arguments stay.
  The twelve formats without such injection shapes gain pinning tests
  asserting verbatim passthrough of noise-shaped user text, so a filter
  borrowed from another format cannot land silently.
- Rank signals for lexical search hits: displayed score is now
  `max(0, RRF × recency_decay − sidechain_penalty + current_repo_boost)`
  with a 30-day exponential half-life (future timestamps clamp to full score)
  and a 0.3 decay floor so old messages are never fully suppressed; sidechain
  hits pay a fixed `0.25/61` score penalty (same relevance ranks them after mainline
  hits) and hits whose session belongs to the repository the command runs in
  gain a fixed `0.5/61` boost (sessiongrep's current-repository preference,
  scaled to RRF with k=60). Penalty and boost are composed
  before the clamp, so a hit clamped to zero cannot be revived by the
  preference; a caller with no derivable repository identity (not in a git
  work tree, no `origin`, or an entry point without working-directory
  semantics) leaves every score bit-identical. Constants live in one module
  (`application::ranking`) and every parameter is pinned by unit tests;
  ranking uses the injected application clock at the first search request;
  subsequent pages retain that scoring time and original 15-minute expiry
  (tests inject a fixed clock, and `ASG_CURRENT_REPO` fixes the repository signal for
  end-to-end tests). Applies to pure lexical retrieval (including lexical
  fallback) only — semantic hits and hybrid RRF fusion are unchanged.
- CJK single-character query recall: the FTS token stream now also carries a
  unigram for every Han character alongside the ADR-0007 bigrams
  (`application::cjk::fts_tokens_cjk`, shared by the index and query sides),
  so single-character queries such as `search 了` match sentences containing
  the character instead of returning nothing. Bigram recall for two-character
  and longer queries is unchanged; worst-case token growth over raw CJK text
  is about 4x (ADR-0007 §后果 updated). Existing databases pick up the new
  tokens via `index rebuild` (no schema change). `bigram_cjk` stays bigram-
  only and remains the guidance evidence source.

### Changed

- The public-facing security and readiness documents now state two limits they
  previously glossed over. `SECURITY.md` records that key-name redaction covers
  string values only (numbers/booleans under a secret-looking key pass through —
  `handoff-pack/v1`'s integer `max_tokens`/`used_tokens` were being emitted as
  the string `"[redacted]"`), and that absolute paths are not secrets to the
  ruleset: `original_working_directory` crosses machine boundaries verbatim
  because `resume` needs it, while `source_path`/`transcript_path` are omitted
  from that shape by design. `THREAT-MODEL.md` §7 no longer claims the
  data-root-locking spike "confirmed the lease does not support NFS" — the spike
  says network filesystems were *not verified* — and both remaining open
  decisions (path privacy mode, network data-root policy) now carry their fact
  basis and a recommended ruling for the owner to sign (§7.1/§7.2).
  `README.md` corrects three claims that did not survive checking: "every hit
  carries a source span" (four of fourteen adapters honestly report none), "CJK
  bigram" tokenization (unigrams + bigrams since the v17 projection), and the
  Kimi format cell now names `turn.prompt`/`turn.steer`. The owner release
  checklist was re-verified against the current tree: verify-release 10/10,
  privacy scan 0, `cargo deny` all ok, public-tree export clean, and the last
  three stale CLI `main.rs` references from the lib split repointed.

- Tool activity extraction now follows the record shapes real `claude-code` and
  `codex` transcripts write (structure census over local corpora: record types,
  tool names and argument key names only, never content). `codex` registers
  `function_call` (previously ignored — 88% of observed tool calls) alongside
  `custom_tool_call`, pairs both `function_call_output` and
  `custom_tool_call_output` by `call_id` (12805/12805 observed outputs pair, 0
  orphans; previously the two halves were crossed so nothing ever paired and
  status stayed `unknown`), reads the string `input` that `custom_tool_call`
  actually carries instead of an absent `arguments` object, and resolves an
  `apply_patch` envelope to the first `*** {Add,Update,Delete} File:` path
  instead of storing patch text. The kind closed set gained the names the
  census found (`PowerShell`, `shell_command`, `exec_command`, `apply_patch`,
  `Agent`, `spawn_agent`, `web_fetch`, `web_search`, `view_image`); everything
  outside it — MCP tools, orchestration/plan tools, user plugins — still fails
  closed to `kind = Unknown` with no target. Codex tool outputs carry no
  failure marker in any observed record, so status stays success-or-unknown and
  is never inferred from output text. `TOOL_ACTIVITY_TARGET_MAX_CHARS` moved to
  ports as the single owner of the 512-character target bound.
- Claude Code `tool_use` blocks now render as `Name(target)` summaries in the
  canonical message projection (idea from cc-switch's `extract_text_from_item`,
  MIT; implementation is our own). Claude Code stores tool calls inside
  assistant messages and those blocks have no `text` field, so 2610 of 4226
  observed assistant messages projected to an empty body — indexed, counted as
  committed, and matchable by no query. The summary makes "which tool touched
  which file" searchable. Only provider-recorded strings are used, under a
  fixed template, bounded by the same target cap as the store, so a body and
  its stored activity target agree verbatim; the catalog payload still holds
  the provider record in full and spans still point at source bytes
  (ADR-0004/THREAT-MODEL: the catalog never rewrites source text — this is the
  canonical projection, which already canonicalizes local-command envelopes).
  Those pinned canonicalizations keep priority over summaries.
- Message FTS body retention (borrowing list #3, ctx text-retention policy):
  a message's searchable projection is now bounded to 16 000 characters
  (`application::retention::MESSAGE_FTS_MAX_CHARS`, char-boundary truncation)
  at the index write paths, the payload-reprojection path
  (`searchable_text` — rebuild/merge/put), and the ingest entry construction.
  The catalog payload keeps the full provider text (ADR-0004/THREAT-MODEL:
  the catalog never rewrites source text), so `get-message` and context views
  are unchanged; only the FTS index volume is bounded. The same cap applies
  on every path so current-detection compares the same bounded text and
  idempotent re-sync still reports no change. Existing databases converge
  automatically: an oversized row is rewritten (bounded) on its next sync;
  `index rebuild` re-projects the whole index from the catalog.

### Fixed

- Cursor ItemTable message timestamps now normalize proven Unix milliseconds
  to millisecond-precise UTC. Disk-kv bubble strings preserve their offset and
  fraction; unsupported numeric bubble times are diagnosed rather than guessed.
  Cline reads its native integer `ts` milliseconds and one leading document BOM,
  rejects float/exponent timestamp tokens without rounding, preserves valid
  legacy string timestamps, and diagnoses numeric compatibility fields whose
  unit is unknown. Invalid timestamp metadata does not invent an empty time or
  discard otherwise valid Cline conversation text.
  **Upgrade:** parser version 5 re-ingests unchanged sources once; schema remains
  19. Sequential re-ingestion of shared Cursor ItemTable messages reconciles
  equal decimal/UTC instants only with per-source Document proof, without changing
  original evidence or stable IDs. Different instants still conflict, even one
  nanosecond apart or separated by a null observation. Removing the sole updated
  claimant restores the surviving observations. Source files and provider
  maturity are unchanged. To downgrade parsing semantics, use a matching catalog
  backup or re-ingest into a fresh catalog; lowering a parser marker alone is
  not a rollback. `index rebuild` only refreshes projections, not parser semantics.
- Semantic/hybrid readiness now matches the selected model, query-vector
  dimension and live catalog rows. Full message-body changes (including beyond
  the FTS text cap) invalidate vectors atomically on every public catalog/batch
  write path. Fallback cursors retain the requested vector and dimension, so
  changing either cannot silently continue an old page.
  Existing `index rebuild` removes historical orphan vectors without touching
  live vectors; use `index embeddings` to regenerate stale-but-live legacy
  caches. Reads never perform cleanup. No schema/parser bump is required.
- Source updates now replace their own original projection before deterministic
  cross-source aggregation. Removing a source restores the surviving projection
  and regenerates affected compatibility aliases. Source evidence is stored
  atomically with its durable batch; final text changes/deletion retire vectors
  in that same transaction.
  **Upgrade:** catalog schema 19 adds per-source observations; parser version 4
  reparses unchanged sources. Make a consistent catalog backup before the first
  writer upgrade. Legacy evidence is not fabricated: old aggregates remain until
  all live contributors have been re-ingested. An older binary cannot open the
  upgraded schema; restore a verified pre-upgrade backup rather than lowering
  the schema version. Historical vector caches require explicit maintenance
  as described above; per-source migration does not prove their input provenance.
  Unscoped storage writes now reject source-owned IDs; update or remove them
  through source replacement so raw evidence and catalog authority cannot diverge.
- Read requests now pin one SQLite view across generation, search, payload,
  ownership and resume reads. Human search rows, handoff evidence and doctor
  counters share the same view; nested scopes release on errors/unwind and
  refuse writes. Model loading, Git repository discovery and output waits stay
  outside read scopes. No persisted schema migration is required.
- Filtered search results now use a Session from a placement matching the
  requested repository/provider/sidechain conditions, including semantic and
  hybrid results; Context activity-read failures no longer appear as empty
  successful activity output.
  Current-repository scoring uses the same matched Session without changing
  signal weights. Existing search cursors must restart after this correction;
  list cursors are unchanged.
- Resume rejects option-like/unchecked provider session IDs before constructing
  argv; machine-output execution is refused rather than sharing an interactive
  provider's terminal streams. Human confirmation and interaction remain.
  HTTP resume previews no longer change the CLI acknowledgement marker.
- Model integrity verification now requires a unique size/hash entry for every
  required content file. Incomplete old manifests must be regenerated and
  reimported; verification alone does not prove model inference works.
- Handoff generation enforces its content-byte budget after all metadata is
  finalized, reports Low confidence without retained evidence, and rejects
  irreducible overflow. Tagged identity hashing separates previously colliding
  filter fields; generated pack IDs change, while the v1 pack schema remains.
- SQLite source classification reads the complete signature across short reads
  so a segmented read cannot bypass logical WAL snapshotting.
- Release quality/build/assembly now check out the same resolved commit;
  reusable CI includes an explicit Rust 1.90 all-feature compatibility gate.
- Machine diagnostic warnings retain accurate redaction status while Human
  diagnostics keep their original text. Synthetic release verification isolates
  user model caches and checks disabled hooks for empty stdout.

- Live OpenCode/Cursor SQLite ingestion reads committed WAL state through
  bounded read-only backups and preserves distinct sessions and resume claims.
  Parser semantic version 2 reparses prior scans. SQL failures abort ingestion;
  malformed role records count as skipped and cannot cause tombstones.
  Private parser databases now clean their WAL/SHM files after successful
  reads and errors, with connection handles closed before unlinking.
- Read commands no longer create or migrate catalogs. Stale schemas require
  explicit writer-leased maintenance; no-op writes still validate every fact.
- Semantic/hybrid search applies metadata, facet and visibility filters before
  pagination, propagates readiness errors, rejects non-finite vectors and uses
  the shared semantic application path in MCP. Provider filters cover all 14
  implemented providers; Web declares and validates its full search parameter set.
- The embedded Web client exposes the supported search filters, invalidates
  pagination when inputs change, and prevents obsolete search/context/preview
  responses from overwriting current state. Loading/error feedback, token
  rotation, bilingual results and responsive layout retain offline deployment.
- Message/session FTS ranks now merge by RRF k=60 with canonical wire-ID ties.
  Search cursors bind mode, model, vector and ranking context, retain first-page
  scoring time and never refresh their original expiry; old search cursors
  fail explicitly. Semantic top-k memory and metadata exclusion SQL are bounded;
  embedding rebuild uses atomic keyset batches with complete failure rollback.
- Unicode-safe secret redaction, checked native message/parent IDs, valid calendar
  dates and checked time arithmetic prevent panics and identity collisions.
  Hook budget detection/truncation uses the same character unit; Grok rewind
  indices no longer truncate on 32-bit targets.
- Updated Ratatui to 0.30.2 and Crossterm to 0.29, removing both lru unsound
  advisories. One `paste` unmaintained warning remains in the optional semantic
  dependency tree; no advisory suppression was added.
- Added explicit CLI-only installation relocation. Schema v18 persists opaque
  installation namespaces, current/retired locations, source bindings and
  relocation receipts; preview is read-only, apply requires a fresh plan,
  writer lease and verified SQLite backup. Relocation updates every live source
  locator atomically while preserving canonical Session/Message/Document/
  Placement IDs, resume observations, usage and tool activity. Retired aliases
  default to 90 days (1..365), never expire canonical IDs, and never silently
  reactivate an old source root.

- **`sync` could not finish while any coding agent was running.** Every source
  was staged, then every snapshot verified, then one commit ran; the
  verification loop propagated the first `SnapshotChanged` with `?`, so a single
  file that grew during the scan discarded the entire run — nothing committed
  for any source. That is the normal case, not an edge case: the agent is
  appending to its own transcript, often the very session the user is typing in.
  Measured against a local 170k-entity library, `sync --discover` failed every
  attempt with exit 5 while Claude Code was open, leaving the catalog untouched.
  A drifted source is now **deferred** — dropped from the commit batch, reported
  through a per-source diagnostic and a new `deferred` count, with its
  previously indexed content left exactly as it was — and every other source
  commits. Not a byte of a deferred source is written, so a torn snapshot
  remains impossible; `emitted` excludes their staged messages so
  `INV-NO-PARSE-LOSS` stays exact. Only `SnapshotChanged` defers: a genuine I/O
  failure still aborts, and `ingest` of a single source still reports exit 5
  because no other source can carry the run.

- **The two message-conflict refusals were indistinguishable.** A stored payload
  that is not canonical JSON and two sources that disagree about a stable field
  produced the same sentence, which the CLI then masks into `catalog_error`
  ("database internal error", remedy: check the `--db` path). The two causes
  need opposite remedies, so a real conflict on a real library could not be
  diagnosed even with `ASG_DEBUG_ERRORS=1`. The field-mismatch refusal now names
  the differing key and the non-canonical one says so. Neither names an entity:
  a provider native id is adopted verbatim into the wire id, so ids are
  untrusted content in an error string.

- **One write open could delete the entire repo dimension and mark it
  converged.** The index-projection self-heal inside `open_for_write` called the
  same whole-store reprojection as `index rebuild`, which empties
  `session_repo_slugs` and re-derives every row from the injected
  `RepoSlugResolver`. Self-heal runs *before* the composition root can inject
  that resolver, so it re-derived from nothing, then stamped
  `index_projection_version` current in the same transaction. After that,
  `search --repo` returned zero hits and `status` reported no repositories,
  with no signal that anything had been lost. The projection is the only one
  that cannot be rebuilt from the catalog — a slug exists only as a live probe
  of the local git checkout, since no absolute path is ever stored — so
  "reproject from the authoritative catalog" has no authority over it. The two
  callers are now distinct: explicit `index rebuild` re-derives, self-heal
  preserves (it never changes the session set, so it cannot orphan a row) while
  still sweeping rows whose session has left the catalog. A slug-rule change is
  therefore converged by an explicit `index rebuild`, which is now stated in
  `docs/operations/rebuild-and-migration-runbook.md`.

- **Ten defects in the embedded web UI**, each now pinned by a guard that fails
  on the pre-fix markup: pasting a fresh `…/#token=…` into an already-open tab
  did nothing (a fragment-only change is a same-document navigation, so the
  script never re-ran — which is exactly what the gate's own error text tells
  you to do after the token rotates); a token the server had already refused
  stayed in `localStorage`, so every reload painted the authenticated shell for
  one round-trip before falling back to the gate; the hit counter reported the
  last page instead of everything loaded (three pages of two hits read "2");
  switching language left the page half-translated, since `applyI18n()` only
  rewrites `[data-i18n]` markup while the status line, hit cards, context title
  and per-message buttons come from `t()` at render time; a resume preview
  stayed on screen after another session was opened, attributing the wrong
  `--resume` command to it; closing the context pane left the previous hit
  highlighted; the filter controls needed 308px inside a 248px sidebar and the
  page-size input was sliced at the edge; one unbreakable run — an absolute
  path, URL or hash — stretched a message to 3200px and scrolled the whole list
  sideways; `frame-ancestors` logged a CSP error on every load because it is
  ignored in a meta-delivered policy (the HTTP header already carries it); and
  with no icon declared the browser probed `/favicon.ico`, which is not the UI
  page and so answered 401 every load.

- **A resume preview whose path ended in `\` could not be run.**
  `quote_preview_token` rejected the characters that expand inside double
  quotes but not a trailing backslash: quoted, `"C:\ws\"` ends in `\"`, a
  literal quote in both POSIX shells and Windows argv parsing, so the closing
  quote never closed and `&& claude --resume …` was swallowed into the
  directory argument; bare, POSIX read it as a line continuation. A trailing
  separator is an ordinary way to write a Windows path. Only the trailing
  position is refused, so ordinary Windows paths still preview.

- **One non-UTF-8 byte on the MCP server's stdin killed the whole session.**
  `serve` read frames with `BufRead::lines()`, which yields `Err` for a line
  that is not valid UTF-8; the `?` turned that into a `source_io` failure and
  the process exited 5. The request on the bad line and *every request after
  it* went unanswered — the client sees a hang, not an error. Verified against
  the release binary: `{"jsonrpc":"2.0","id":2,"method":"ping","x":"<0xFF
  0xFE>"}` followed by a valid `ping` for id 3 produced zero response frames
  and exit 5. A byte that is not UTF-8 is a malformed frame, not an I/O
  failure: the reader now takes bytes with `read_until(b'\n')`, answers the
  offending line with JSON-RPC `-32700`, and keeps serving. Genuine stdin I/O
  errors still surface as `source_io`.

- **Declared MCP input-schema bounds were not enforced at runtime.**
  `search_sessions`/`generate_handoff` published `providers.maxItems: 2` and
  nothing checked it: `providers: ["claude","codex","claude-code","claude",
  "codex"]` was accepted. The declared `2` was also tighter than the three
  spellings the same schema's `enum` accepts, so a client that named every
  legal value was violating a bound the server never applied. `tool_name` went
  the other way — the only string parameter with no `maxLength` at all and no
  runtime cap, so a 100 KB value was accepted and echoed back verbatim in
  `data.facets.tool_name`. Both directions are a schema that lies to an AI
  client that cannot see the code. The accepted provider values now live in one
  `PROVIDER_FILTER_VALUES` constant that feeds the schema `enum`, the schema
  `maxItems`, and the runtime length check; `tool_name` declares and enforces a
  128-character bound. A regression test asserts each declared bound rejects
  `bound + 1` at runtime and that every declared `enum` value is really
  accepted.

- **The MCP `doctor` tool answered with strictly less than the CLI `doctor`.**
  Both sides hand-wrote their own `json!`, and the MCP copy was missing six
  fields: `offline`, `semantic_feature`, `tool_activity_storage`,
  `usage_storage`, `orphaned_usage_events`, and `orphaned_usage_memberships`.
  An AI agent asking "why did semantic search fall back to lexical?" or "why is
  usage empty?" through MCP could not see the answer that the same command
  gives on the CLI. The release consistency harness compares exactly one
  operation (`search`), so no test ever put the two `doctor` projections side
  by side. Both entry points now project through one
  `doctor_store_data(store, offline)`; the MCP server takes the global
  `--offline` intent so it reports that field honestly too. A new e2e test
  compares the MCP `doctor` data against the real CLI `--robot doctor`
  envelope field for field, rather than against a hand-copied list.

- **`SECURITY.md` described `--offline` as an active control.** It read as
  though the flag rejects network-requiring capabilities, but no shipped
  capability requires the network (`NETWORK_REQUIRING_SUBCOMMANDS` is empty), so
  the flag rejects nothing today and only changes the `offline` field that
  `doctor` and `hook` report — which is what `--help` already said. The policy
  now says so, and its absolute-path scope limit names `config paths` alongside
  `original_working_directory` instead of implying the working directory is the
  only verbatim-path route across a machine boundary.

- **Cross-boundary redaction only masked the first sixteen secrets in a value.**
  The shared span replacer stopped after 16 substitutions per string, so span 17
  onward was emitted verbatim. One transcript message holding a pasted `.env`
  dump with more than sixteen credentials therefore crossed *every* machine
  boundary in the clear — Robot JSON `search`/`context`/`get`/`show`/`list`, MCP
  `content` and `structuredContent`, the Web `/api/*` bodies, and the injected
  hook context — while `redaction.status` still reported `applied`, so the
  envelope claimed the value had been cleaned. The cap existed only because the
  old implementation called `String::replace_range` once per span, which is
  quadratic when secrets are dense; the replacer now accumulates into a single
  output buffer, which is linear, so the cap could go without adding cost.
  Reproduced with a 40-span synthetic dump and a mixed-shape variant, pinned in
  both the ports engine and the CLI JSON-tree walker.

- **The resume dry-run preview let a transcript inject shell commands.** The
  preview string was built by raw interpolation of `(cd {dir} && {binary}
  {args})`, and both the recorded working directory and the provider session id
  come from a provider transcript — untrusted input per `THREAT-MODEL` §2. A
  session whose recorded `cwd` ended in `...\ws" && <command> && cd "...`
  rendered as a preview that runs `<command>` first, and that string is exactly
  what the human preview invites an operator to copy and what an MCP/Robot
  client may hand to a shell. Preview tokens are now quoted when they need it,
  and the whole string is withheld (`command: null`, with the human renderer
  saying why) when a token carries a character that can still escape or expand
  inside double quotes in cmd.exe, PowerShell or a POSIX shell — no single
  quoting style is safe across all three, so refusing beats emitting something
  plausible. Ordinary paths with spaces now preview correctly instead of
  splitting. Execution was never affected: `resume --yes` spawns argv with
  `current_dir` and never goes through a shell.

- **MCP tool results redacted secrets but never said so.** Every successful
  `tools/call` result runs the payload through cross-boundary redaction
  (ADR-0009), and `handle_tools_call` threw the resulting `RedactionStatus`
  away: `structuredContent` carried only `{outcome, data, warnings, page}`
  while the Robot envelope for the same operation carried a `redaction` block.
  An AI client that received `"text": "leaky [redacted:api_key] here"` could
  not tell whether the server had masked a secret or the transcript literally
  said that, and had no `redacted_count` to reason about how much was removed —
  the exact failure mode MCP exists to avoid. The tool payload now carries the
  same `redaction` block as the Robot envelope (`mode`, `status`,
  `ruleset_version`, `redacted_count`, `audit_id`), built by one shared
  `protocol::redaction_block` so the two boundaries cannot drift, and the
  status is attached after redaction so it is not itself rescanned. This was
  a required part of the privacy-hooks design ("attaches `redaction` to
  `structuredContent`") that was never implemented; no test asserted the key
  set, so the whole suite stayed green.

- **Lexical search pages silently duplicated hits and hid the strongest one**
  (rank-window pagination). After the rank-signals wave, the fetch window was
  `offset + page + 1` — but re-ranking sorts the whole window, so the pinned
  ordering depended on which page you asked for. Page 1 ranked a small window's
  top slice; page 2 ranked a larger window where deep-bm25 items with strong
  recency could leapfrog what page 1 had already shown. Concatenated pages
  could repeat a hit and never show the true top hit at all, with exit code 0
  throughout. Found by dogfooding against a real index; reproduced with a
  4-hit fixture whose recency order is the reverse of its index order (pages
  returned `[hit2, hit1, hit1, hit0]` against a truth of
  `[hit3, hit2, hit1, hit0]`). Re-ranked paths (pure lexical, plus
  semantic/hybrid degraded to lexical fallback) now fetch one fixed
  `RANK_SCAN_WINDOW` (512; ×16 for `--group-by-session`) independent of the
  offset, so every page ranks the same set and slices one total order; paging
  past the window ends with `has_more=false` — a ranking horizon, not a
  defect. Semantic/hybrid paths keep the offset-based over-fetch because
  their order is a stable prefix of the retrieval order. The window-shape
  predicate is computed once before the fetch and reused as the rank decision,
  so the two cannot drift.

- **`serve`'s slowloris-isolation test no longer asserts a wall-clock budget.**
  `integration_slowloris_does_not_block_a_fast_get` measured that a fast `GET`
  finished within 250 ms while a partial-header client was parked on the pool.
  That budget measures how loaded the host is, not whether the server
  serialises: the test passed 5/5 in isolation and failed inside a full
  `cargo test --workspace` run on a busy machine, which is exactly how it would
  flake on a shared CI runner. The property is now expressed structurally — the
  server's read timeout is set far beyond the request so the parked connection
  provably cannot be reaped first, meaning a `200` can only come from
  concurrent service, and a server that serialised would stall past the
  client's own read timeout and fail loudly. The reaping half moved to
  `integration_stalled_headers_are_answered_408`, which asserts the `408` with
  no timing assertion at all. Server behaviour is unchanged.

- **Silently wrong search results on stores built by an older binary**
  (index-projection versioning, schema v17). The FTS token transform is a
  contract between index time and query time; when it changed (pure CJK
  bigrams → unigram + bigram) nothing invalidated the tokens already on disk,
  and `PARSER_SEMANTIC_VERSION` does not cover this axis — it only triggers a
  re-parse when *parse* semantics change. Measured on a real 170,468-entity
  library (generation 9) built by the previous binary: MCP `search_sessions`
  returned **0 hits** for `配置备份` and `备份` while `clippy` matched and
  scored normally. An error would have been visible; a partially empty hit set
  was not. Now: `INDEX_PROJECTION_VERSION` is persisted as the store-level
  `store_metadata.index_projection_version`; write opens (`sync`/`ingest`/
  `index`) reproject from the authoritative catalog automatically (no
  re-parse — catalog payloads are authoritative for content) and advance the
  generation once; read-only paths fail closed with `schema_incompatible`
  (exit 9) naming `index rebuild` instead of returning a wrong hit set; and
  `doctor` reports `index_projection_version` /
  `index_projection_expected` / `index_projection_stale`. The version's
  contract covers every projection input — CJK tokenization,
  `MESSAGE_FTS_MAX_CHARS` retention truncation, `searchable_text`, the
  `session_fts` field set, and the `session_titles` / `session_repo_slugs`
  derivation rules (all reprojected by the same rebuild). The v17 migration
  stamps from an observable fact, so a fresh data root is never reported stale
  and never churns a rebuild on first open. `catalog`, `list`, and `get` are
  unaffected: the catalog is authoritative and the projection is derived.
  Measured on that library: v15 → v17 migration 0.19 s, one whole-store
  reprojection of 170,468 entities ≈ 140 s, after which `配置备份` and `备份`
  both recall again.
- Message edge relation classification (session-tree lineage, borrowed from
  hstry's `fork_type` three-way classification): a sidechain message's parent
  edge is now stored as `subagent` instead of `reply`. Only the claude-code
  format carries a provable edge type (`isSidechain` = subagent/branch flag),
  so this is the honest subset — fork/retry/continuation have no explicit
  format field and keep `reply` rather than being guessed. Edge relations are
  deterministic and re-derived on re-ingest, so existing databases converge
  on their next sync.
- serve query-string routing: `/api/search?q=...` no longer 404s.
- Smoke scripts assert the actual 9 MCP tools (was 7 after the provider wave;
  `generate_handoff` brought it to 9).
- `verify-release.py` runs with `--db` and a committed gate fixture.
- Redaction now covers secrets embedded inside prose (previously only
  whole-string secrets were matched).
- Error output at the Robot/JSON boundary now follows the same redaction
  discipline as success envelopes: error messages and details go through the
  shared redaction engine, and `ProviderError::Io` OS text (which can embed
  real absolute transcript paths on Windows) is masked at conversion, like
  `PortError::SourceIo`. MCP error frames share the same masking.
- The entry-point consistency e2e test skips (instead of failing the suite)
  when no Python interpreter is available.
- CI now runs the `semantic-candle` feature test suite, so that module is
  covered by the quality gate.

### Planned for 0.1.0

- First public release: CLI, Robot JSON protocol, MCP server, TUI, and Web UI
  adapters sharing one Application ADT. Full cross-entry release rehearsal is
  still required before publication.
- Session search over the implemented provider set (lexical FTS5 plus ranked
  fuzzy-lexical retrieval), resume metadata extraction, and handoff packs.
- Installer scripts for Windows (PowerShell) and Unix (Bash), release
  rehearsal automation, and an evidence-backed open-source gate manifest.
- Core Beta evidence harness: lexical recall at 10 = 1.00 and parse loss =
  0.00 on the committed synthetic gate fixture.

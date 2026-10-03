# agentsessions-cli — Backend Guidelines

> The composition root. The only crate that binds abstract ports to concrete
> adapters and owns the human/robot-facing surface.

---

## Role in the architecture

`agentsessions-cli` is the **composition root** (see the hexagonal layering in
the plan §5): it is the single place that `new`s a concrete `SqliteStore` and
injects it into `App`. Every other crate is backend-agnostic. This crate also
owns the CLI argument parsing, the Robot v1 protocol envelope, and the mapping
from `AppError` to canonical error codes / exit codes.

Real files:
- `src/main.rs` — arg parsing, command dispatch, composition root, platform paths
- `src/protocol.rs` — Robot v1 envelope, `CanonicalCode` error catalog, output modes
- `src/human.rs` — human-mode success renderer (text lines, no envelope)
- `src/mcp.rs` — stdio MCP server (JSON-RPC 2.0, contract §8 tools)
- `src/tui/` — interactive read-only TUI Preview (`core.rs` pure reducer/view-model, `mod.rs` terminal glue)

---

## Pre-Development Checklist

Before writing code in this crate:

- [ ] Argument parsing is **hand-written**, no third-party CLI framework. Match
      the existing style in `main.rs` (`parse_db_flag`, `command_name`,
      `arg(rest, i, usage)`, `extract_flag` for value flags); do not introduce
      clap/structopt. Boolean flags use `take_bool_flag` (MCP uses `opt_bool`);
      every new flag must be registered in every prefix scanner that skips
      value-bearing flags (`command_name`, request-id/output-mode scans),
      otherwise a value can hide a later `--robot`/`--output` flag.
- [ ] Every new subcommand goes through `dispatch()` and returns
      `(&'static str command, Outcome, serde_json::Value, protocol::Page, Vec<String> warnings)`.
      Errors return `CliError`, never `panic!` / `unwrap` on user input.
      App responses are projected by `render()` — truncated results become
      `Outcome::Partial` (process exit 10, contract §5); pagination tokens fill
      the envelope `page.next_cursor` / `page.has_more`; honest degradations
      (e.g. unknown-precision evidence) become `warnings` entries.
- [ ] Output truth table (contract §6): `emit_result()` is the single success
      exit — Human mode renders via `human::render_success` (text lines, no
      envelope, warnings to stderr); Json/Jsonl emit one envelope. ALL protocol
      stdout goes through `protocol::write_stdout_line` (EPIPE → silent exit 0,
      other write errors → stderr + exit 5) — never bare `println!` for frames.
- [ ] Progress frames (`protocol::progress_frame`) are emitted ONLY under
      `--output jsonl` (never Json/`--robot`/Human). `diagnostic` frames are
      schema-defined but v1 emits none. Progress identifies inputs by bounded
      source ordinal/count only; it never includes a source path or native ID.
- [ ] `--request-id` is validated by `protocol::valid_request_id`
      (`^[A-Za-z0-9._:-]+$`, 1-128) and echoed verbatim in every frame;
      invalid values are usage errors, never silently replaced.
- [ ] Source staging de-duplicates stable Message entities but retains every
      `MessagePlacement` and `MessageEdge`. Native IDs are preferred; the
      no-native fallback is `Unstable` and derives from provider, variant,
      content-addressed document, and ordinal — never source path.
- [ ] Preserve the complete provider `ParseReport`: `committed` must equal
      emitted occurrences, `skipped` is reported honestly, and only zero-skipped
      sources are relation-complete. Parent/sidechain/span/session payload keys
      are compatibility projections; Application context consumes typed edges.
- [ ] Read paths use `SqliteStore::open`; write paths (`index`/`ingest`/`sync`)
      use `SqliteStore::open_for_write` to take the writer lease. Do not open a
      write connection for a read-only command.
- [ ] stdout carries **only** protocol data; human diagnostics go to stderr
      (plan §7.5). `protocol::emit` no longer exists — success goes through
      `emit_result`, errors through `error_envelope`, every frame through
      `write_stdout_line`.
- [ ] MCP (`mcp` subcommand, `src/mcp.rs`, contract §8): stdout carries only
      JSON-RPC frames (same `write_stdout_line` exit; EOF → exit 0). Handlers
      are thin — validate protocol, build `AppRequest`, project via the same
      `render()`; never duplicate search/branch/pagination/budget rules and
      never expose file read/SQL/exec. Error split: protocol problems
      (malformed JSON, batch arrays, unknown method/tool, invalid params,
      not-initialized) → JSON-RPC errors (`-32602` carries
      `data.canonical_code = invalid_request`); business failures after a
      well-formed `AppRequest` → `isError: true` tool results carrying
      `structuredContent.error.canonical_code`. `initialize`/`ping` bypass the
      initialize gate; supported protocol versions are pinned in
      `SUPPORTED_PROTOCOL_VERSIONS` (negotiation never lies). Tool set is
      frozen: search_sessions / get_session_context / get_message /
      list_sessions / list_providers / get_status / doctor.
- [ ] MCP legacy compatibility duplicates a successful payload in
      `structuredContent` and `content[0].text`. Application/Robot byte estimates
      cover one rendered payload, not the complete duplicated JSON-RPC frame;
      do not claim a hard MCP frame-byte limit until the MCP boundary enforces
      and tests that separate contract.
- [ ] TUI (`tui` subcommand, `src/tui/`): all state transitions and rendering
      decisions live in the PURE `core.rs` (no ratatui/crossterm/store/App
      imports; unit-tested without a terminal); `mod.rs` is thin glue only.
      Data access goes through `AppRequest::{Search, MessageContexts, Context}`
      and the shared `render()` projection — no SQL, compatibility-alias
      session selection, cursor construction, or branch logic in the TUI.
      Reverse candidates are grouped by distinct Session: zero is index-only,
      one opens, several are explicit ambiguity. Evidence aligns by
      placement/occurrence ID. Non-tty stdout → usage error before
      raw mode; terminal restore covers normal, error, and panic paths; App
      errors become status-line text, never a crash. The TUI is the only
      place the `ratatui`/`crossterm` workspace deps may be used.
- [ ] New error conditions map to an existing `CanonicalCode`. If you need a new
      code, register it in the `CanonicalCode` enum, its `as_str`, `exit_code`,
      `retryable`, **and** the published `schemas/robot/v1/error-catalog.json`
      (the schema-drift test enforces they stay in sync).
- [ ] Error envelopes and human diagnostics must preserve canonical category
      while keeping messages bounded and path/native-ID-free. Never relay raw
      provider, SQLite, or conflict payloads to stdout or stderr.

---

## Scenario: CLI-only installation relocation

### 1. Scope / Trigger
The owner explicitly reconnects a previously indexed provider root after moving
unchanged source files to another directory or drive.

### 2. Signatures
`asg --db <catalog> relocate --provider <provider> --from <old-root>
--to <new-root> [--alias-ttl-days 90]` previews.
Apply additionally requires `--apply --plan <opaque-plan> --backup <new-file>`.
The command is registered in every value-flag/prefix scanner and help/hint list.

### 3. Contracts
Validate complete scalar/flag combinations before opening the catalog. Preview
uses `SqliteStore::open`; apply uses `open_for_relocation`, which does not create,
migrate or reproject before the required backup. An old catalog needs an
explicit `index rebuild` first. Canonicalize provider aliases through the shared
registry. The source root need not exist; the destination must be host-local
and absolute. No provider files/configuration are moved or rewritten.

Production staging injects the persisted/reserved installation namespace before
deriving native Sessions, then commits the reservation with source facts.
Do not rederive from the new path. Explicit discovery supplements canonical
provider roots with registered current roots, uses consistent locator spelling
for fingerprint caching and refuses retired-location reuse. Independent catalogs
with independently allocated namespaces need not have equal Session IDs; reads
within one catalog and after relocation must preserve every canonical ID.

Robot relocation responses contain only status, opaque plan, counts, generations
and TTL. Errors stay path/native-ID/content-free. Capability metadata explicitly
names CLI as the only mutation interface; MCP/Web must not advertise or accept
new relocation mutation tools. Existing read/search/resume projections remain
consistent after relocation.

### 4. Validation & Error Matrix
Invalid/duplicate/missing scalar or incompatible flag combination ->
`invalid_request`, before catalog access. Preserve `source_changed`,
`generation_mismatch`, `writer_busy`, `schema_incompatible`, source/backend I/O
categories. An unchanged result does not advance generation or create a backup.

### 5. Good/Base/Bad Cases
Good: preview a moved synthetic provider root, apply its plan with a new backup,
then re-sync/discover without identity changes. Base: help describes the same
flags the parser accepts. Bad: open a writer for an invalid request, or compare
random per-catalog namespace IDs as if they were globally reconstructible.

### 6. Tests Required
Assert no-create/no-migrate preview and invalid requests; exact Robot privacy
shape; source/ID/resume/context preservation (generation advances once); occupied
target refusal; repeated no-op and incremental discovery; capability metadata,
MCP mutation rejection and installed-artifact smoke using only synthetic data.

### 7. Wrong vs Correct
Wrong: use ordinary `open_for_write` before verifying relocation prerequisites.
Correct: parse first, use a non-mutating read or dedicated existing-catalog lease,
then let the storage operation validate, back up and activate atomically.

## Quality Check

Before proposing a commit for this crate:

- [ ] `cargo fmt --all --check` clean.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo test -p agent-session-grep-cli` green, including `tests/e2e.rs` and
      `tests/mcp_e2e.rs` (both drive the real compiled binary end-to-end).
- [ ] Positive legacy re-ingest fixtures use the historical installation seed
      and `StableId::native_session_scoped`, and assert unchanged Session IDs.
      Keep a separate unprovable-ID refusal with unchanged catalog/registry/
      generation. Windows raw-locator coverage must not be hidden by seeding
      every fixture with already-normalized source paths.
- [ ] Resume subprocess cwd assertions canonicalize both filesystem paths and
      compare native `PathBuf` values. Exercise a Unix symlink directory while
      retaining first-run preview, actual spawn, and native argument assertions;
      `/var` versus `/private/var` spellings alone do not indicate a wrong cwd.

- [ ] Any protocol/envelope change is reflected in both `protocol.rs` tests and
      the `tests/e2e.rs` envelope-shape assertions.
- [ ] Exit codes match the error catalog (invalid_request/cursor_invalid/
      cursor_expired → 2, not_found → 4, source_io/source_changed → 5,
      catalog/writer_busy → 6, provider → 7, schema_incompatible/
      generation_mismatch → 9, partial success → 10, internal → 70).
- [ ] Surface changes are mirrored in `scripts/install/smoke.{ps1,sh}`. That
      script is the installed-artifact contract check (CLI envelopes, exit
      codes, MCP handshake) and runs on all three OS targets in the `installer`
      CI job; a new command or exit-code rule that it does not assert is
      untested against a real install.

---

## Operational surfaces and evidence

- **Install scripts** (`scripts/install/`) build from source with `--locked` and
  install exactly two command files into a user-level prefix:
  `agent-session-grep[.exe]` and `asg[.exe]`. Windows uses an executable copy;
  Unix prefers a relative symlink and may use a marked wrapper fallback. The
  scripts must never modify `PATH`, the registry, or shell profiles, never
  request elevation, and never download anything beyond what `cargo build`
  fetches. Upgrade replaces only the managed command paths. `uninstall`
  removes exactly those two managed files, is idempotent (a second run
  reports "not installed" and exits 0), and never deletes a directory
  recursively.
- **Smoke script** never builds. It requires an already-built binary and fails
  if absent, so a build failure can never masquerade as a smoke pass. Its
  fixture is synthetic and inlined; it must not read real transcripts.
- **Real-data regression** (`scripts/evidence/real_data_regression.py`) runs
  only locally against a throwaway temp store. Its report is a closed field set
  of aggregate counts and invariant verdicts — no message text, source paths,
  native ids, fingerprints, usernames, or hostnames. Reports default under the
  gitignored `evidence-output/`; a report generated from real data is never
  committed. Adding a report field means extending `validate_report` and the
  privacy test in the same change.
- **No-loss accounting.** Provider-emitted source occurrences must equal
  persisted source-placement claims and skipped records must be zero. Stable
  Message count is a separate de-duplicated census; legal shared identity is
  not parse loss.
- **Evidence honesty.** Rows in `docs/operations/core-beta-evidence-matrix.md`
  follow that file's status vocabulary literally: `ci_configured_only` until a
  specific successful run is named, and a failing local run is recorded as a
  failing run. Neither a passing smoke script nor a green CI job is
  clean-machine, minimum-OS, or signed-release evidence — hosted runners ship a
  preinstalled toolchain.
- **Historical blocker (recorded 2026-07-27).** Real corpora proved stable
  Message identity is shared while session/document/span/parent are contextual.
  The v7 implementation now stages placements/edges and removes those fields
  from stable conflict authority without weakening role/text/timestamp
  conflicts. This is code/process evidence only until the authorized aggregate
  real-data regression is rerun successfully; providers remain Experimental.
- **Resume protocol (ADR-0009, recorded 2026-08-14).** `get-session-resume`
  CLI command and `get_session_resume` MCP tool return ONLY the fixed nullable
  fields: `session_id`, `provider_id`, `resume_available`,
  `provider_session_id`, `original_working_directory`, `unavailable_reason`.
  Never emit `command`, `resume_command`, `source_path`, or `transcript_path`
  (asserted in `tests/mcp_e2e.rs` and `tests/e2e.rs`). Resume Metadata is
  isolated in its own port struct and SQLite table; it never enters FTS text,
  opaque session payload, diagnostics, progress frames, or error messages.
  Explicit per-message session observations produce separate per-session claims;
  ambiguous report-level observations still fail closed. Canonical `session_id` (`ses_v1_*`) is the
  catalog identity; the Provider-native ID is Resume Metadata only — the two
  are not interchangeable and `ses_v1_*` cannot be reversed to the native ID.
- **Scoped canonical Session identity (2026-08-14).** Native Session IDs are
  namespaced by `SessionIdentityNamespace` (provider + installation
  namespace) before hashing (`StableId::native_session_scoped`). The
  composition-root `installation_namespace` derives the namespace from the
  provider data-root prefix path (`.claude`/`.codex`) or, for unknown
  providers, the source's parent directory. Known RFC-0001 §5.1 debt: this
  derives from absolute path and lacks a persisted registry, `id_alias` table,
  and path case normalization; relocation does not preserve Session identity.
  The domain layer is correct; the gap is composition-root only.
  Follow-up: `.trellis/tasks/09-25-session-relocation-aliases/`; preserve
  historical `ses_v1` during ordinary sync.
- **Human session table (2026-08-14).** Human search renders a frozen
  five-column table: `日期 | Provider | 会话标题 | 工作目录 | Session ID`.
  Provider and Session ID are never truncated; title uses tail ellipsis;
  working directory uses middle collapse; missing renders `—`. 日期 is the
  per-Session latest message timestamp truncated to `YYYY-MM-DD` (batched
  `MAX(json_extract(catalog.payload,'$.timestamp'))`, no N+1); 会话标题 is the
  highest-relevance hit `text` on the current page. Robot/MCP output is
  unchanged (Human-mode only projection).

---

## Scenario: Cross-entry search and safe ingest boundaries

### 1. Scope / Trigger
CLI, MCP, Web, hook and handoff search; provider staging and vector rebuild.

### 2. Signatures
`prepare_search_embedding` owns shared local model selection/vectorization;
semantic requests use `resume_semantic_app`. Web `/api/status` adds
`data.web_capabilities`; search accepts `max_bytes`, sidechain/tool facets and
repeated `provider` parameters in addition to existing search fields.

### 3. Contracts
Provider filters and MCP enums derive from implemented searchable capability
rows (14 currently) plus the `claude` alias. Web unknown/empty parameters,
duplicate scalars and invalid boolean/percent encoding fail explicitly; repeated
providers form the same OR set as CLI/MCP. Web `limit` sets both page size and
item budget; this difference is declared in capability metadata.
Default bigram-hash remains fuzzy lexical vectorization, not a semantic-model
quality claim. Source message/parent IDs are checked before normalization;
explicit session observations survive staging. SQL/schema/corruption errors
abort OpenCode ingestion; nontext roles are reported as skipped, making the
scan incomplete and preventing missing-record tombstones.
Hook truncation uses one Unicode character cutoff for both detection and
output (the existing approximate 4 characters/token contract).

### 4. Validation & Error Matrix
Unknown/deferred provider, malformed search fields, unsafe native IDs -> invalid
request. Store/schema/vector failures preserve canonical business errors. A
read command on an absent catalog returns `catalog_error` and creates no DB.

### 5. Good/Base/Bad Cases
Good: indexed Grok data stays filtered in CLI/MCP semantic/hybrid results.
Base: missing vectors cause explicit fallback. Bad: create an embedding and
then call an App with `NoSemanticIndex`, or flag an uncut CJK hook as truncated.

### 6. Tests Required
Assert positive cross-entry hits, all advertised provider filters, invalid Web
parameters, budgets/facets, Unicode hooks, atomic native-ID refusal and source
multi-session accounting. Installer smoke covers no-create reads, explicit sync,
registry filters and CLI/MCP effective semantic modes using synthetic data.

### 7. Wrong vs Correct
Wrong: duplicate embedding/model/provider-selection logic per entrypoint.
Correct: share the composition helpers and keep protocol adapters thin.

## Scenario: Embedded Web request state

### 1. Scope / Trigger
The offline `src/web/index.html` client changes filters, pages, sessions,
context policy, preview or authentication while earlier requests are pending.

### 2. Signatures
`beginRequest(kind) -> AbortController`, `cancelRequest(kind)` and
`currentRequest(kind, controller) -> boolean` own the `search`, `context`,
`preview` and `init` request slots. `api(path, signal)` preserves shared error
projection; `invalidateSearch(messageKey)` clears the current search state.

### 3. Contracts
Every search input participates in the serialized request signature. Changes
clear the cursor, loaded hits, selection and handoff state; they cancel search
and close context. Pagination may append only for the unchanged signature,
and duplicate in-flight page loads are ignored. Aborting saves work, but
controller identity also rejects responses queued before cancellation.
Context success/error/finally paths must still belong to the active controller
and Session. Closing context cancels context and preview work. An old 401 must
not clear a newer accepted token. Render payloads with `textContent`; preserve
CSP, single-file offline deployment and bilingual loaded-result state.

### 4. Validation & Error Matrix
Obsolete or aborted response -> no visible state change. Current request
failure -> visible error and cleared busy state. Empty query or invalid numeric
input -> no search request. Search input change -> pagination invalidated with
accessible feedback. Current unauthorized response -> authentication gate.

### 5. Good/Base/Bad Cases
Good: query B completes before query A; B remains visible after A succeeds or
fails. Base: a second page appends to the same search and updates the total.
Bad: an old context response reopens a closed panel or replaces another Session.

### 6. Tests Required
Run `node --test crates/agent-session-grep-cli/tests/web_ui.test.cjs`. Assert
all filters and capability-driven providers, out-of-order success/error,
input/cursor invalidation, duplicate paging, empty-query/error reset, Session
switch/close/policy changes, old-token 401, editable filter keys and language
changes preserving every loaded page. These tests execute the shipped script
with delayed synthetic responses and no npm dependencies. Browser layout
inspection remains separate from the DOM interaction harness.

### 7. Wrong vs Correct
```javascript
// Wrong: any completed request can replace the current context.
const body = await api('/api/context?' + params);
```

```javascript
// Correct: only the current controller and Session may project the response.
const controller = beginRequest('context');
const session = activeSession;
const body = await api('/api/context?' + params, controller.signal);
if (!currentRequest('context', controller) || activeSession !== session) return;
```

## Scenario: Empty sources and record-stream tail health

### 1. Scope / Trigger
`ingest`/`sync` open a source at zero bytes, or rescan a known source whose
bytes may or may not be a line-delimited record stream.

### 2. Signatures
`provider_is_record_stream(provider_id) -> Option<bool>` (derived from the
adapter manifest's `StreamingSupport`, so the registry stays the single source
of truth); `empty_source_batch(path, fingerprint, discovered_provider_id) ->
SourceBatch`; `SqliteStore::source_fingerprints(&[String])`.

### 3. Contracts
Zero-byte input is triaged before any provider probe. A first unknown empty
source is a warned no-op: no provider binding, no scan row, no placeholder
entity, generation and committed counters stay 0. An empty source that already
has a scan row commits an honest whole-source replacement batch (empty
entries/placements/edges/activities/usage, `relation_complete: true`,
`len_bytes: Some(0)`, provider id only from discovery) so the existing
replacement inference tombstones the old rows while provider ownership is
resolved from the already proven installation binding. Repeating the same
empty fingerprint/parser version is a no-op; refilling the file goes through
the ordinary parse path.
JSONL truncated-tail retention (keep the previous index, do not advance the
fingerprint) applies only when the manifest declares
`StreamingSupport::RecordStream`. Whole-source formats (SQLite, whole JSON,
Markdown, disk KV) must reach their own adapter validation; the line-wise JSON
health probe would otherwise read a legitimate edit as a truncated tail and
retain a stale index forever.
When a parsed source reports `skipped > 0`, both `ingest` and `sync` prepend the
same bounded partial-source warning: previously indexed history is retained
where present, and old/new document-scoped copies may temporarily coexist until
a complete rescan. Emit it once per response ahead of per-row diagnostics so
the existing diagnostic warning cap cannot hide it; include it in
`data.diagnostics` and apply the existing count/character budgets. The warning
contains no source path or native identifier. Accounting alone is not a
user-visible explanation of retained history.

### 4. Validation & Error Matrix
Unknown/provider-less source id -> `None` -> not a record stream -> no tail
retention. Empty known source -> empty replacement may only tombstone, never
invent a provider. Empty unknown source -> exit 0 with a diagnostic warning.
Unreadable or changed source -> existing capture/verify failure paths, nothing
committed.

### 5. Good/Base/Bad Cases
Good: emptying a known JSONL source tombstones its messages and a later refill
re-indexes them under the proven provider.
Base: repeating sync on an empty source reports unchanged and commits nothing.
Bad: a rewritten SQLite/whole-JSON source keeps its previous index because a
synthetic JSONL health check called it truncated.

### 6. Tests Required
`cargo --offline --locked test -p agent-session-grep-cli --test e2e` covering
first-empty no-op, empty replacement and refill for canonical and standalone
paths, whole-source update visibility, and truncated-tail and partial-row
retention. Cover the empty lifecycle across ingest/sync, canonical/standalone
sources, and native/fallback identity, including unchanged generation on the
repeated empty scan and stable Session identity after refill.
Hermes integration must exercise partial ingest and sync, require the bounded
retention/coexistence warning, preserve shared-source history across complete
recovery, and reparse an unchanged parser-version-2 snapshot exactly once.

### 7. Wrong vs Correct
Wrong: gate tail health on a file extension or a hard-coded provider list, or
emit a zero-byte batch before checking whether the source was ever scanned.
Correct: ask the adapter manifest what the provider streams, and let the stored
scan rows decide whether an empty source is a no-op or a replacement.

**Language**: write all guideline docs in **English**.

## Scenario: Resume boundary and immutable release quality

### 1. Scope / Trigger
CLI/HTTP resume previews, machine execution, and reusable release CI.

### 2. Signatures
`preview_resume(&SqliteStore, &str)` returns the ordinary outcome/data/page/warnings tuple without acknowledgement. CLI resume shares `load_resume_preview`; machine --yes execution is rejected. Reusable ci.yml accepts optional string `source_commit` and resolves a full SHA in the source job.

### 3. Contracts
Validate native session IDs through the checked domain contract and reject leading hyphens before descriptor construction. Human execution retains its terminal and first-preview gate; do not route it through null stdio. HTTP GET neither creates nor consumes CLI acknowledgement, and HTTP POST remains unsupported. Unknown CLI command names are canonicalized to unknown; machine diagnostics/progress free text is redacted before bounding. Preserve request correlation/page tokens and existing local Human rendering policy. Diagnostic redaction must retain its actual status/count through envelope construction; scanning already-redacted text again cannot recover that accounting. Test both the machine envelope metadata and Human diagnostics, not only a redaction helper.
Release prepare resolves source_commit; quality, build and assemble consume that same immutable commit. All reusable quality jobs depend on source and checkout its SHA. MSRV is explicitly 1.90.0, including all features/targets. Existing-tag dispatch remains artifact-only; no tag-free rehearsal or automatic official release is implied.
Synthetic release verification isolates platform home/config/data/cache paths in each child process, not just the catalog, so installed user models cannot change its bigram-hash expectations. Never change the parent environment or user configuration. Hook verification observes the bare hook protocol: disabled hooks must exit successfully with exactly empty stdout, not a JSON envelope; diagnostic stderr is allowed.

### 4. Validation & Error Matrix
Option-like native ID -> unavailable descriptor with no executable argv. Machine --yes -> structured invalid_request without spawn. GET preview -> no ack state change. Invalid non-SHA quality input -> source job fails before consumers. Build/assembly never re-resolve a mutable tag.

### 5. Good/Base/Bad Cases
Good: Human fake-provider round trip uses exact argv after preview. Base: machine caller previews only. Bad: GET acknowledges a CLI confirmation, or green trigger-branch tests bless a different tag's build.

### 6. Tests Required
Boundary units and fake-provider e2e cover no-spawn, valid argv, mode isolation and free-text redaction/correlation. HTTP regression proves repeatable GET and separate CLI acknowledgement; existing POST/CSRF checks remain. Workflow tests enforce quality/build/assemble/source checkout and the MSRV command. Controlled remote mismatch rehearsal is a separate pending release gate.

### 7. Wrong vs Correct
Wrong: infer safe argv from shell quoting, or use the dispatch branch for release quality.
Correct: validate operands before preview/execute and bind every release consumer to prepare's SHA.

## Scenario: Composite read response snapshots
1. **Scope:** Human search rows, CLI/MCP handoff evidence and doctor counters extend beyond a single App call.
2. **Signature:** use the store's nestable `begin_read_snapshot` around App plus all subsequent DB projection reads.
3. **Contract:** prepare model/query embeddings first. Human search retains its view through resume/date rows; handoff retains it through source locators, activities and message facts, then releases before pack formatting. Doctor protects its direct multi-counter projection. Ordinary App calls rely on App's guard.
4. **Errors:** map acquisition errors through existing protocol/business mappings. Early failures release the view before the next command.
5. **Cases:** generation and evidence refer to one view; resume confirmation, child process execution and output/transport waits never retain a guard.
6. **Tests:** CLI Human search/handoff/doctor and MCP handoff success/error paths permit a subsequent write; core WAL tests prove nested view consistency.
7. **Wrong/right:** wrapping App alone leaves post-response DB reads unprotected; extend only the composite read region, never the whole command/server lifetime.

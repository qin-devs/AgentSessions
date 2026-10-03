# agentsessions-adapters-sqlite — Backend Guidelines

> SQLite + FTS5 storage adapter. Implements the catalog store and search index
> ports. This is the only crate that talks to `rusqlite`. The catalog is the
> source of truth; the FTS index is a rebuildable projection.

---

## Role in the architecture

`agentsessions-adapters-sqlite` is a **driven adapter**: it implements the
storage/search/context-graph ports declared in `agentsessions-ports`. It owns
the SQLite schema, migrations, writer lease, durable outbox, generation
tracking, source entity/placement claims, relation completeness, tombstones,
and the FTS5 projection.

Core invariant: **`catalog` is authoritative for payload; `fts` + `fts_ids`
are derived and must be fully rebuildable from `catalog` at any time.**

---

## Pre-Development Checklist

- [ ] Am I preserving the catalog-as-truth / FTS-as-projection split? Never
      make search the source of record.
- [ ] Schema change? Bump `PRAGMA user_version`, add a forward migration, and
      keep it non-destructive. Never drop/recreate to dodge a migration.
- [ ] Does the write go through the durable outbox two-phase path
      (`begin_index_batch` → `commit_index_batch`) so a crash leaves only a
      side-effect-free `building` row that `recover_interrupted` converts to
      `aborted`?
- [ ] Generation advance: is it guarded by CAS on `store_metadata`? A failed
      transaction must not pollute the active generation.
- [ ] Identity fidelity: does the code read `fts_ids.id_json` first and only
      fall back to `StableId::from_wire` (which degrades to Unstable) when the
      sidecar is absent? The catalog wire key does NOT encode stability.
- [ ] Source read-only: nothing here writes back to a provider source file.

---

## Key patterns (real code)

- `verify_pending_in_tx(...)` — pre-commit validation inside the transaction:
  checks active-generation CAS + durable intent state + digest/manifest.
- `rebuild_index()` — clears `fts`/`fts_ids` and re-projects the whole catalog
  in one transaction, advancing generation; rolls back cleanly on failure.
- `interrupted_batch_count()` — read-only doctor signal for pending `building`
  intents.
- `WriterLease::try_acquire(...)` — accepts only `fs4` `Ok(true)`, retains the
  authoritative locked handle, and maps contention to path-redacted WriterBusy.
- `searchable_text(payload)` — extracts the searchable body from the canonical
  payload for FTS.
- Filtered FTS search — uses prepared queries with `fts MATCH` plus provider,
  timestamp, facet and visibility predicates before `LIMIT`. Each corpus uses
  BM25 then canonical wire ID; message/session ranks combine through RRF k=60,
  never by comparing their raw BM25 scores. SQLite FTS5
  auxiliary functions and `MATCH` name the real virtual table (`fts`), not a
  table alias.
- Filtered hit ownership is occurrence-aware: repo, provider and applicable
  sidechain predicates must hold on the same placement. Select the smallest
  matching Session wire ID deterministically and return it with lexical and
  semantic hits so hybrid/grouping/display do not fall back to an unrelated
  compatibility alias. Metadata representatives must satisfy the same placement
  predicates. Preserve MainOnly's existing message-wide exclusion of any
  sidechain history; this ownership correction does not redefine that facet.
- `ContextGraphStore` reads — return typed Domain messages/documents/placements/
  edges, group reverse candidates by distinct Session, and expose aggregate
  placement/claim counts. They never return SQL rows or compatibility JSON.
- `merge_session_payloads(...)` / `merge_message_payloads(...)` — union the two
  projections of one entity that arrive from different sources. Real corpora
  need this: one logical session spans many transcript files, and resuming or
  forking a conversation copies its history into the new file, so the same
  entity legitimately arrives more than once per batch.

## Cross-source entity merging

`commit_source_batches_if_changed` folds an entity that several sources claim
instead of rejecting the batch. Which fields may differ is deliberately narrow:

| Entity | Unioned fields | Everything else |
|---|---|---|
| Session | `messages` (append-order), `documents` (sorted) | fields not in the union set (`document`, `documents`, `messages`) are not preserved by the merge |
| Message | contextual compatibility keys (`session(s)`, `span(s)`, parent provenance, sidechain, seq); text uses the existing deterministic longer-projection rule | stable role and unknown intrinsic fields must match across live sources; timestamp has only the deliberate compatibility exceptions below |

- **Timestamp exception (Codex)**: `timestamp` may differ as `string` vs `null`
  across projections of the same stable message — the old Codex adapter stored
  the occurrence-local outer envelope timestamp, the current one emits no
  stable timestamp, so re-ingesting an old catalog must not conflict. The
  merged value converges deterministically on `null` regardless of merge
  order (no stable timestamp exists for the entity).

- **Timestamp spelling exception (Cursor ItemTable)**: the same proven
  millisecond instant may arrive as an old decimal string and a new UTC string
  during parser-version re-ingestion. This is allowed only for final claimants
  whose own associated Document observation proves provider `cursor` and variant
  `cursor/vscdb-chat-v1`. See the rolling-upgrade scenario below; the generic
  pairwise merge and other providers are not broadened.

- Each unioned field keeps a **singular alias** (`document`, `session`, `span`)
  holding the first entry, so readers written against the pre-union shape keep
  working. On a shared entity the alias names *one* contributor, not all of
  them — treat it as a compatibility shim, never as the complete answer.
- Schema 19 retains original observations in `source_entity_projections`, keyed
  by source and entity. A complete scan replaces that source's observations;
  an incomplete scan updates observed entries but retains unobserved evidence.
  Aggregate payloads are recomputed from final live claimants, not folded with
  historical aggregate text. Thus a same-source shorter/equal-length correction
  replaces its predecessor, while cross-source reconciliation remains deterministic.
- Migration creates an empty evidence table: missing legacy evidence stays
  unknown. Preserve an existing aggregate until every live claimant has actual
  observations; if no aggregate exists to preserve, fail with re-ingest guidance.
  Parser version 4 introduced this re-ingestion; current version 5 also reparses
  unchanged sources for timestamp/BOM fidelity. Re-ingesting all contributors
  supplies evidence and converges; never manufacture per-source payloads from
  the historical aggregate.
- Both no-op checks include original identity/payload/text evidence. Evidence
  changes must commit even when the aggregate does not change, since a later
  source deletion may reveal that observation.
- Once every known contributing source is relation-complete and has evidence, compatibility
  aliases are regenerated subtractively from placements/edges/claims:
  divergent parents/sidechain values become `null`, spans name exact placement
  and document identities, and zero-message Session documents survive through
  source entity membership. Mixed legacy/incomplete state preserves existing
  aliases and never pretends they are complete.
- Alias candidates include memberships/placements from both before and after
  source replacement, including entities retained only by another source. Keep
  all candidate/evidence loads batch-scoped. Relocation moves the new table's
  live source locator alongside existing source tables.
- Incomplete scans preserve existing compatibility aliases while allowing
  observed intrinsic payload corrections. An unobserved raw projection must not
  overwrite previously generated spans/parents/session aliases. No-op probes
  compare authoritative state without treating a retained legacy aggregate as a
  live original claimant; intrinsic corrections can repeat and later converge.
- Unscoped public writes (`put`, ordinary batch and index-batch APIs) reject
  IDs claimed by source membership, including unscoped deletions. Use source
  replacement to change those facts. Validate before creating intent and again
  at public commit boundaries; standalone unclaimed entities remain writable.
- Source manifest descriptors retain full typed identity and labeled BLAKE3
  payload/text digests, not JSON numeric arrays or another copy of transcript
  bodies. Original bytes remain in the projection table. Reuse the one sealed
  canonical manifest during private phase-2 verification; CAS and persisted
  durable-manifest comparisons remain mandatory.

## SQLite v7 relation commit

- Migration v6→v7 is one explicit transaction. The four relation tables,
  relation indexes, three default-empty outbox manifest columns, and
  `PRAGMA user_version = 7` commit or roll back together. No legacy aliases are
  backfilled into fabricated placements or completeness markers.
- Durable intent hashes entity changes plus relation upserts/deletes and the
  final source replacement state: entity memberships (including document
  attribution), placement claims, and relation marker state. An honestly empty
  replacement is still manifest data.
- A complete scan replaces claims and may delete only facts with no surviving
  source claim. An incomplete scan unions observed entities/placements/edges,
  derives no missing-record tombstones, and clears its completeness marker.
  Observed relation changes are allowed only when every final claimant observes
  the same placement/edge/root fact.
- Catalog, FTS, identity sidecar, source claims, relations, active generation,
  and outbox activation are one phase-2 transaction. Failure leaves the
  durable `building` intent but no data or generation side effect.
- Every ordinary and source-batch write path validates incoming StableId
  metadata against persisted `fts_ids.id_json` before taking a no-op shortcut
  or creating an outbox intent. A cross-batch metadata conflict fails without
  changing generation, claims, catalog, or index state.
- FTS rebuild reprojects `fts`/`fts_ids` and removes historical vector rows
  with no catalog entity inside its writer transaction. Live vectors remain;
  relation rows, claims, completeness, context graphs and aggregate context
  stats must remain byte-for-byte stable. This does not repair stale-but-live
  legacy vectors; those require `index embeddings`.
- Context reads fail `SchemaIncompatible` with a bounded re-ingest action when
  any known contributor lacks a v7 completeness marker. They never parse
  compatibility aliases as graph authority.

## Common mistakes

- Assuming `StableId::from_wire` restores Native/Reconstructed — it returns
  Unstable. Full identity lives only in `fts_ids.id_json`.
- Advancing generation outside the committing transaction.
- Adding an `ALTER`-free destructive "migration".
- Treating a per-source fact (session, span, parent) as intrinsic to a message.
  Message *identity* is shared across files; its *position* is not.
- Treating `skipped > 0` as a complete replacement or retaining an old
  relation-complete marker after an incomplete re-scan.

---

## Resume Metadata claims (ADR-0009, schema v8)

`source_session_resume_claims` holds source-scoped Resume Metadata written
atomically with source replacement and cleared on source removal (tombstone).
The canonical `ses_v1_*` is the catalog identity; the Provider-native ID and
Original Working Directory are isolated Resume Metadata, never entering FTS
text, opaque Session payload, diagnostics, progress, or errors.

- **Pair association**: `original_working_directory` is returned only when
  `pair_observed = 1` AND `original_working_directory_state = 'resolved'`.
  A cwd seen without a co-observed Provider Session ID is suppressed even
  when the ID itself is resolved. Implemented in `resume_metadata_from_claim`.
- **Fail-closed conflicts**: when the same canonical Session has conflicting
  claims across Sources, `resume_of` returns `resume_available = false` with
  `unavailable_reason = "conflicting resume metadata claims"` and discloses
  none of the conflicting values. It never picks by source path or order.
- **No reverse derivation**: there is no path from `ses_v1_*` back to the
  Provider-native ID. The native ID lives only in the claim row.
- **Explicit multi-session observations**: one Source can carry independent
  claims keyed by canonical Session. An ambiguous report-level observation
  still fails closed; per-message session observations are not conflated.
- **Batched reads**: `resume_of` chunks via `BATCH_IN_CHUNK` (no N+1) and
  short-circuits empty input with zero SQL statements. The `session_id` lookup
  is backed by index `source_session_resume_claims_session`, not a full scan
  (asserted by an EXPLAIN QUERY PLAN test).
- **Latest activity**: `latest_activity_ymd_for_sessions` batches
  `MAX(json_extract(catalog.payload, '$.timestamp'))` per canonical Session,
  truncated to `YYYY-MM-DD`, for the Human table 日期 column. No port/DTO/schema
  change; Robot/MCP output is unaffected (Human-mode only projection).

## Session metadata search projection (schema v11)

`schema v11` (`migrate_v10_to_v11`) adds `session_fts` (FTS5,
`session_wire UNINDEXED, text`) and `session_fts_ids` (session wire ↔ FTS
rowid sidecar) as a rebuildable, privacy-safe search projection over Session
metadata. It is a derived projection like the message `fts`, populated from
`source_session_resume_claims` + catalog; never authoritative.

- **Indexed fields**: resolved Provider-native Session ID
  (`provider_session_id_state = 'resolved'`), Original Working Directory only
  when `pair_observed = 1` AND resolved, and the first chronological valid
  user request as the deterministic title-like field. Each field is bounded by
  `SESSION_SEARCH_FIELD_CHARS` (4096 chars). Provider custom title/summary are
  not yet part of the Canonical provider contract and remain explicitly
  deferred.
- **Fail-closed conflicts**: when the same canonical Session has conflicting
  claims across Sources (any field disagrees), `session_search_text` returns
  `None` for the claim block — none of that Session's claim values are
  indexed, matching the `resume_of` conflict rule. It never picks by source
  path or order.
- **Never indexed**: `source_path`, transcript path, native IDs from other
  providers, or anything outside the fixed privacy-filtered metadata shape.
- **Maintenance**: `commit_index_batch_with_relations` collects affected
  Sessions (upserts/deletes, placement moves, source replacements and claim
  rows) and rebuilds their `session_fts` rows in the same transaction;
  `rebuild_index` repopulates the projection entirely from catalog + claims.
  Delete is rowid-scoped via the `session_fts_ids` sidecar.
- **Query merge**: `query_filtered` runs the message-FTS query and a second
  `session_fts MATCH` with the same `safe_fts_query`, merges deterministically
  (score desc, wire id asc) and truncates to the limit. A Session already
  represented by a matching non-system Message hit is deduplicated; a
  metadata-only Session returns its canonical identity (or its first
  non-system Message as representative). Candidate exclusions use a single
  JSON parameter (`json_each`) rather than one SQL expression per message;
  final merging uses deterministic RRF and canonical wire IDs.

## Scenario: Explicit installation relocation (schema v18)

### 1. Scope / Trigger
Indexing a new installation, upgrading v17 provenance, or reconnecting an
unchanged installation at an explicitly selected new directory/drive.

### 2. Signatures
`resolve_or_allocate_installation_namespace(provider, source, legacy_seed)`
returns a staging namespace. `relocation_preview(provider, from, to, ttl)` is
read-only. `open_for_relocation(path)` acquires a writer lease without schema
migration or projection rebuild. `apply_relocation(provider, from, to, ttl,
plan, backup)` returns the bounded `RelocationResult`; `backup_to(path)` never
overwrites a destination or its SQLite sidecars.

### 3. Contracts
`installation_namespaces` freezes the exact legacy seed or a new opaque seed;
`installation_locations` stores flat current/retired root ownership;
`source_installations` binds source locators; `installation_relocations` stores
private committed receipts. Migration skips unverifiable/ambiguous provenance
without rekeying historical entities. New namespace reservations live only in
memory until their source replacement activates. The durable source manifest
includes the assignment; a failed source commit leaves no registry pollution.
Before selecting or reserving a namespace for an unregistered input,
`validate_unbound_source_locator` checks existing unbound scan locators using
`normalize_absolute_path`. Include NULL provider metadata. A different stored
spelling with the same lexical key is `InvalidRequest`, even if an exact-text
row also exists; preserve the original locator, all tables and generation.
An exact unique legacy locator still requires complete native-session proof.
Registered current bindings keep their indexed path. The unbound cold path
streams rows with bounded memory, but may scan legacy candidates per input;
it is not an indexed or batched time-complexity optimization.


Relocation extends the existing outbox manifest and activation transaction.
Move every live source locator in `source_scans`, `source_membership`,
`source_placement_membership`, `source_relation_scans`,
`source_session_resume_claims`, `tool_activity_membership`,
`usage_event_membership` and `source_installations`, plus registry ownership,
receipt, generation and intent state together. Keep canonical payloads,
identity sidecars, intrinsic relations, native observations, original CWD and
historical outbox manifests unchanged. Validate destination logical snapshots
(including committed SQLite WAL) before and immediately before activation.

Preview pins a read transaction, streams source enumeration in 128-row pages
and retains bounded mapping metadata (up to 65,536 sources), never transcript
payloads or embeddings. A verified Backup-API snapshot is published to a new
file before creating the relocation intent. Only the private backup switches
out of WAL; its temporary sidecars are cleaned after connections close.
Read-only and stale-schema opens cannot create or migrate catalogs. Ordinary
write-open auto-reprojection must not run before a relocation backup.

The compatibility alias defaults to 90 days (1..365). It never authorizes an
ordinary scan of the retired installation. Expiry permits a new independent
installation, never namespace reuse from a stale staging reservation. It does
not expire canonical IDs. Repeated mappings are unchanged only while the
complete selected ownership/source set still matches the committed receipt.

### 4. Validation & Error Matrix
Malformed/occupied/ambiguous roots, unsupported provider or invalid policy ->
`InvalidRequest`; stale generation -> `GenerationMismatch`; unreadable source
-> `SourceIo`; changed content -> `SnapshotChanged`; missing lease ->
`WriterBusy`; stale schema -> `SchemaIncompatible`; backup/SQL failure ->
bounded backend error. All failures preserve live tables and generation;
a building intent may remain for ordinary abort recovery. Public diagnostics
and result fields contain counts/digests, never paths/native IDs/content.

### 5. Good/Base/Bad Cases
Good: several legacy namespace groups under one moved root retain distinct
native Sessions. Base: re-syncing the new root is a no-op. Bad: using the new
path as namespace input, accepting a stale receipt after a sibling installation
was added, or allowing one provider's retired root to block another provider.

### 6. Tests Required
Use only synthetic sources. Assert full table/identity/context/resume/backup
snapshots, not just counts; inject failure at every locator table, registry,
receipt and activation. Cover legacy migration rollback, opaque allocation,
independent same-native-ID installations, source deletion, WAL-only change,
plan lifetime/mapping/generation, target conflicts, reverse moves, retired
expiry, casing/separators/Unicode, read-only preview and no-clobber backup.
Cover provider-less legacy Windows locators with raw and normalized inputs,
ambiguous equivalent rows, exact legacy proof, and preservation on refusal.
The namespace guard is shared by ingest, explicit sync and discovery; tests
must include those routes rather than relying on CLI input normalization.


### 7. Wrong vs Correct
Wrong: persist a namespace during parsing or update only `source_scans`.
Correct: stage the assignment, then atomically activate all authoritative facts
through the same durable manifest and generation CAS used by source commits.

## Scenario: Read-only lifecycle and logical source snapshots

### 1. Scope / Trigger
Opening catalogs, ingesting live SQLite sources, and rebuilding embeddings.

### 2. Signatures
`SqliteStore::open` uses `SQLITE_OPEN_READ_ONLY`; `open_for_write` acquires the
writer lease before migrations. `SourceBatch.resume_claims` is a vector of
per-session claims. `rebuild_embeddings_from_catalog(model_id, dimension,
batch_size, encode)` returns indexed/skipped/cleared counts.

### 3. Contracts
Read opens never create/migrate a catalog and use a finite 1-second busy timeout.
SQLite source capture uses Backup under a pinned read transaction, with a
128 MiB logical-size ceiling and guarded temporary destination. Never checkpoint
or write the provider source database or its WAL; attaching a read-only
connection to a live WAL source does let SQLite write reader marks into the
`-shm` wal-index (measured 2026-09-29), so the executable immutability claim is
"database + WAL bytes unchanged, and the provider parser never opens the source
at all" rather than "all three files are byte-identical". Logical fingerprints
include committed WAL data and remain current across an external checkpoint
without logical changes.
Per-session resume claims share the existing `(source_path, session_id)` key;
no schema migration is required. Parser semantic version 2 forces old source
scans to reparse. Provider parser `TempDb` keeps `conn` before `_guard` so
SQLite closes before cleanup. The guard owns the private database plus its
`-wal`/`-shm` files; the last read-only connection does not guarantee their
removal. Register that guard before writing bytes, and keep the file handle
inside its lifetime so write/sync failure also cleans the copy. Never use
this cleanup on original provider sources or enumerate unrelated temp files. Validate duplicate facts/claims/StableId metadata before any
no-op shortcut. Old building intents are aborted on writer recovery.
Embedding rebuild uses bounded wire-ID keysets (batch 1..512), finite vectors,
and one transaction for replacement plus generation. Encoder failure rolls back.
Semantic query scans rows but retains only bounded top-k candidates; it is an
exact scan, not ANN and not a production latency guarantee.

### 4. Validation & Error Matrix
Absent read catalog -> backend error without creating a file. Wrong schema
-> `SchemaIncompatible`, with explicit writer-maintenance action. Source changes
-> `SnapshotChanged`; oversized snapshots -> `SourceIo`. Invalid batch facts,
vectors or encoding failures -> error without changing authoritative data.

### 5. Good/Base/Bad Cases
Good: two sessions in one live WAL DB retain separate placements/resume claims.
Base: repeated unchanged logical snapshot is a no-op. Bad: copying only the
main DB, silently merging sessions, or clearing old vectors before encoding.

### 6. Tests Required
Assert absent/stale/read-under-writer catalog behavior, unchanged source
database/WAL bytes, WAL-only updates/checkpoints, per-session claims, invalid
no-op batches, encoder rollback, keyset query plans, finite scores and bounded
top-k equivalence. Each SQLite-reading provider crate (`opencode`,
`cursor`, `hermes/sqlite-state-v1`) must create real temporary WAL/SHM files
while parsing its private read-only copy, then assert all owned files disappear
after successful reads and query errors (the hermes and cursor copy guards
also remove a stray `-journal`).

### 7. Wrong vs Correct
Wrong: call migration from a read command or return early before batch validation.
Correct: lease writes explicitly and validate all incoming facts before no-op.

---

## Scenario: Batch-scoped commit state (measured 2026-09-29)

### 1. Scope / Trigger
Initial ingest at 10k-1M messages: latency and peak RSS of one `sync` commit.

### 2. Signatures
`commit_source_batches_if_changed` reads only the batch's own sources, the ids
they observe, and the claim rows of those ids (`CatalogStateSnapshot`).
`begin_index_batch_with_manifest` / `verify_pending_in_tx(..., manifest)` take
one `CanonicalBatchManifest` per commit. Write connections run
`PRAGMA cache_size = -131072` (session-level; `synchronous`/`journal_mode`
unchanged).

### 3. Contracts
- Do not reintroduce whole-catalog maps in the commit path: membership and
  relation rows must be loaded per batch sources plus candidate ids. Tables
  without a candidate-index (e.g. `source_membership.message_id`) may be
  scanned but must keep only candidate rows in memory (O(batch)).
- The durable manifest is computed once per commit; `verify_pending_in_tx`
  must still compare it against the `index_batches` row and reject tampering.
  Accepted trade-off (2026-09-29): the pre-optimization "recompute the manifest
  from the transaction inputs" defence was dropped - `verify_pending_in_tx`
  (`crates/agent-session-grep-adapters-sqlite/src/lib.rs:6036-6043,6114-6127`)
  now compares the caller-supplied manifest against the stored row instead of
  recomputing it; behaviour is equivalent and the durable-intent tamper tests
  still reject DB-row tampering. Optional hardening: a debug assertion or an
  opt-in recompute.
- An unchanged (fingerprint-matched) source produced no commit batch, so the
  post-read `verify_snapshot` re-read/re-hash must be skipped for it; sources
  that were actually parsed keep the full verification.
- `cache_size` is a performance knob only: never trade `synchronous`, journal
  mode, or single-commit atomicity for throughput.

### 4. Validation & Error Matrix
Whole-catalog loads are not a correctness signal; removing them must not change
any stored row. Evidence boundaries (verified 2026-09-29):

- `research/scripts/verify_equiv.py` compares 15 tables + `active_generation`
  only; its membership coverage is 2 tables (`source_membership`,
  `source_placement_membership`), the synthetic corpus has no tool_use/usage
  records so `tool_activities`/`usage_events` (and their membership tables)
  compare 0 rows, it does not exercise no-op / shrink-replacement / tombstone
  scenarios, and it excludes the `index_batches` manifest/replacement columns.
  Its historical runs were not persisted as artifacts.
- Sealed extended evidence:
  `research/results/auxiliary/equiv-extended-10k.json` from
  `research/scripts/verify_equiv_extended.py` runs initial -> no-op -> shrink
  replacement -> post-replacement no-op -> empty-source tombstone -> second
  no-op with both binaries against the same absolute source paths, and compares
  25 non-shadow tables (including all four membership tables) by row-content
  hash plus `active_generation`: 0 mismatches (generation 3 == 3). FTS5 shadow
  tables and the per-fresh-catalog installation namespace id/wall-clock columns
  are excluded with recorded reasons; the tool_use/usage tables remain 0-row
  empty comparisons.

### 5. Good/Base/Bad Cases
Good: a 200k-message batch against a 1M catalog loads ~200k candidate rows and
commits in one transaction. Base: an unchanged re-sync skips parsing, the
double read, and the commit. Bad: rebuilding catalog-sized claimer maps or
re-reading every unchanged source.

### 6. Tests Required
- Adapter suite (outbox/CAS/generation, identity fidelity, relation
  completeness, durable-intent tamper rejection) stays green.
- Multi-batch ingest equivalence: the same corpus split into many batches must
  produce identical catalog/projection/relation rows as a single batch.

### 7. Wrong vs Correct
Wrong: load `source_membership`/`message_placements`/`message_edges` whole and
build catalog-sized maps on every commit (the 6th 200k batch against a 1M
catalog spent 23.417 s in `load_catalog_state` and loaded 1,000,000 placement +
900,000 edge rows; sealed trace
`research/results/auxiliary/state-load-diagnostic-before.jsonl`).
Correct: load the batch's sources + candidate ids, scan without retaining, and
keep the durable manifest to one computation.

## Scenario: Historical empty-placeholder repair (parser version 3)

### 1. Scope / Trigger
A source was first ingested at zero bytes by the historical first-empty path,
which persisted an `empty` provider namespace/location for it, and a later parse
now proves a real provider (the CLI-side lifecycle is owned by the CLI).

### 2. Signatures
`SqliteStore::repairable_empty_placeholder(conn, source_path, namespace_id) ->
PortResult<bool>`;
`SqliteStore::authorized_placeholder_rebinds(tx, sources) ->
PortResult<BTreeSet<String>>`;
`persist_installation_in_tx(..., authorized_placeholder_rebind: bool)`;
`PARSER_SEMANTIC_VERSION = 3`.

### 3. Contracts
`resolve_or_allocate_installation_namespace` may return a different namespace
for a source only when the persisted binding is a provable empty placeholder:
the namespace provider is exactly `empty`, the current scan is zero bytes with
the empty-blake3 fingerprint and its provider is NULL or `empty`, and there are
no message/placement/activity/usage claims. Every surviving claim must match
the exact historical empty document/session derivation, its full typed
`fts_ids.id_json` identity, its catalog payload, and its membership document
attribution. A Reconstructed stability label alone is not placeholder proof:
real containers can have that label too. Re-derivation only checks existing
proof and never creates or rewrites a native identity.
Resume metadata may only describe that exact placeholder session under provider
`empty`, with both native-session and working-directory values NULL, both
states exactly `missing`, and `pair_observed = 0`. Foreign identities, cwd facts,
unknown states, missing payload/sidecar evidence, aliases or relocation
provenance fail closed. Anything unprovable fails closed with
`source belongs to another provider installation`; the placeholder path never
runs legacy provenance recovery and never reconstructs identity.
Authorization is re-evaluated inside the write transaction by
`authorized_placeholder_rebinds`, before the batch replaces the proof
(`source_scans`, memberships, catalog identities); `persist_installation_in_tx`
then only re-checks that authorization flag plus `prior_provider == "empty"`.
A pre-parse reservation alone never authorizes a rebind, and the repair is
scoped to that source path: other sources sharing the placeholder
namespace/location keep their rows. `PARSER_SEMANTIC_VERSION = 3` makes sync
treat sources stored under an older parser version as changed, which is how the
corrected staging reaches already-scanned sources without byte changes.

### 4. Validation & Error Matrix
Provable placeholder + real provider -> binding replaced once, placeholder
entities retired by the same replacement batch. Real claim, unresolved
identity, ambiguous provenance, non-empty scan, or foreign fingerprint -> hard
`invalid_request` conflict, prior binding and rows untouched. Transaction
failure or a racing writer -> nothing persisted (writer lease and CAS/outbox
unchanged).

### 5. Good/Base/Bad Cases
Good: a legacy zero-byte `empty` binding is repaired to `claude-code` and the
old placeholder documents disappear from search.
Base: an empty binding rebuilt by the fixed binary is never created again, and
unrelated sources keep their bindings.
Bad: a forged `msg_v1_*` claim on the placeholder source blocks the rebind.

### 6. Tests Required
`cargo --offline --locked test -p agent-session-grep-cli --test e2e` (repair
success, forged-claim refusal, standalone empty replacement) plus the SQLite
adapter unit tests for the proof predicate and version-stale reparse. Required
negative controls mutate one proof surface at a time: exact container identity,
sidecar, catalog payload, document attribution, scan provider, native-session
metadata, cwd metadata, and observed-pair state. A refused rebind must leave the
live catalog, memberships, installation registry and generation unchanged.
Exercise shared placeholders until the last claimant is replaced, proof changes
after pre-parse reservation, transaction rollback on failed activation, and
real-provider/retired-alias boundaries. The unchanged parser-version-2 Hermes
snapshot must be reparsed once by version 3, preserve partial history, then no-op.

### 7. Wrong vs Correct
Wrong: authorize the rebind from the pre-parse reservation, or read the proof
after the batch has already replaced the scan row and memberships.
Correct: verify exact historical container and metadata proof at the top of the
write transaction, pass the resulting authorization into the persist step, and
keep everything else fail-closed; neither an `empty` namespace nor a
Reconstructed sidecar label is sufficient on its own.

## Quality Check

- `catalog` remains authoritative; `fts` fully rebuildable from it.
- All multi-write operations are transactional and go through the outbox.
- Migrations are additive/non-destructive and gated on `user_version`.
- `cargo fmt --all --check` + `cargo clippy --workspace --all-targets -- -D warnings`
  + `cargo test --workspace` (SQLite adapter tests included).

---

**Language**: All documentation in **English**.

## Scenario: Complete SQLite source signature reads

### 1. Scope / Trigger
Classifying a source before choosing ordinary byte fingerprinting or a logical SQLite/WAL snapshot.

### 2. Signatures
`source_fs::capture(&Path) -> PortResult<SourceSnapshot>` uses the private `has_sqlite_header(&mut dyn Read)` classifier.

### 3. Contracts
Read exactly 16 signature bytes or observe definite EOF. A successful short Read is not EOF and cannot decide the format. Classification must not consume payload bytes beyond the header. Existing logical backup and read-only source guarantees remain unchanged.

### 4. Validation & Error Matrix
Exact SQLite signature -> logical backup; short file/nonmatching signature -> ordinary file; other I/O error -> SourceIo. Do not downgrade a real I/O error into a text-file classification.

### 5. Good/Base/Bad Cases
Good: one-byte chunks still identify SQLite. Base: empty/short text is a regular file. Bad: one short read skips committed WAL data.

### 6. Tests Required
`source_fs` tests cover chunks 1..16, EOF/nonmatch, error after partial input and the existing WAL-only change/source-immutability regression.

### 7. Wrong vs Correct
Wrong: classify from one read's byte count. Correct: `read_exact` with explicit UnexpectedEof handling and other errors propagated.

## Scenario: Pinned SQLite request views
1. **Scope:** multi-read requests on one SqliteStore, including write-open stores used for tests/composition.
2. **Signature:** `begin_read_snapshot() -> PortResult<Box<dyn ReadSnapshot + '_>>` implements the catalog port.
3. **Contract:** first guard starts a deferred transaction and reads store_metadata before returning; BEGIN or SELECT 1 alone is insufficient. Nested guards share a counter without retaining a RefCell borrow. query_only refuses writes within the scope. Constructors leave query_only OFF; the private connection is not externally configurable. Final guard rolls back and restores OFF without changing SQLite open flags or migrating schema.
4. **Errors:** do not join an unrelated active transaction. Failed pin cleans up; failed rollback/reset poisons subsequent snapshot acquisition with a reopen-required backend error. Drop cannot return a cleanup error for the current response; it must not panic during unwinding.
5. **Cases:** concurrent WAL commit is invisible to the current view and visible to the next. Read-only source/catalog permissions stay unchanged.
6. **Tests:** writer before query and before payload/ownership, eager pin, non-LIFO nesting, early error/unwind, failed pin, write refusal and cleanup poisoning.
7. **Wrong/right:** a process mutex cannot provide cross-process consistency; one pinned connection shared by every request data port can.

## Scenario: Dimension-aware vector validity
1. **Scope:** semantic/hybrid reads, canonical body updates, and explicit index maintenance.
2. **Signatures:** `SemanticIndex::is_ready(query_dimension: usize)`; private `message_body`, `invalidate_changed_vectors_in_tx`; existing `rebuild_index`/`index embeddings` maintenance.
3. **Contracts:** readiness checks selected model, query dimension and live catalog within the request snapshot. Invalidation compares complete canonical message bodies, not the FTS-capped projection, on both `put` and batch/source write paths. Unchanged body/alias-only updates preserve valid vectors. Index rebuild removes only historical orphans; reads never prune.
4. **Errors:** corruption in a matching vector and backend failures propagate; only an actual not-ready result permits explicit lexical fallback. Invalidations/pruning roll back together with catalog/index/generation on failure.
5. **Cases:** a body change beyond the FTS cap still retires a vector; wrong-dimension-only vectors do not make an index ready. Live legacy vectors may be stale and require explicit embedding rebuild; do not promise automatic historical repair.
6. **Tests:** model/dimension/live-row matrix; malformed matching vector; put plus every public batch/source path; unchanged-body retention; transaction rollback; read-only orphan retention and writer rebuild cleanup; WAL writer between readiness/query/payload.
7. **Wrong/right:** comparing capped FTS text misses changes used by the vectorizer; compare full input and keep the original FTS cap unchanged. No schema or parser bump is required for these derived-cache corrections.


## Scenario: Cursor ItemTable timestamp rolling upgrade

### 1. Scope / Trigger
Parser 5 can replace a decimal millisecond string with a UTC string while an
unscanned copy still claims the same Message. Source replacement, not a schema
migration or a read, reconciles this representation-only change. Schema stays 19.

### 2. Signatures
`commit_source_batches_if_changed(&[SourceBatch]) -> PortResult<bool>`;
`cursor_itemtable_timestamp_alias(...) -> PortResult<Option<String>>` and
`cursor_aggregate_payload(...) -> PortResult<Cow<[u8]>>` are private helpers.
`source_membership.document_id` selects the proving Document in that source's
`source_entity_projections`, never in the merged catalog payload.

### 3. Contracts
- Every final claimant must have an associated Document observation naming
  `cursor` / `cursor/vscdb-chat-v1`. A correct but unrelated Document does not
  authorize reinterpretation. Load referenced proofs outside aggregate candidates
  in a batch-scoped query; do not expand the merge set or scan the whole catalog.
- Compare all non-null original timestamp observations before pairwise folding.
  Interpret only this proven old spelling as signed i64 Unix milliseconds and
  compare the complete `(unix_seconds, nanosecond)` tuple to parsed date-times.
  Do not round to milliseconds, guess units by digit count, or let null hide a
  non-null disagreement within the proven ItemTable set.
- Use an actually observed UTC/offset spelling only while aggregating. Never
  mutate `SourceReplacementManifest.projections`, original payload/text evidence,
  identity, or exact no-op comparisons. Null still converges to null.
- Recompute from final surviving observations: removing the sole date-time
  claimant restores the remaining decimal spelling; an incomplete scan does not
  remove that claimant. Generic role/unknown intrinsic conflicts stay strict.
- Parser-version rollback is not an inverse data migration. Do not merely lower
  markers; restore a matching catalog backup or re-ingest into a fresh catalog
  with the chosen compatible binary before a semantic downgrade. `index rebuild`
  alone does not reparse original provider sources.

### 4. Validation & Error Matrix
Missing/wrong provenance does not authorize the exception. Different instants,
unsupported proven observations, and other intrinsic conflicts remain bounded
`PortError::Backend` failures; no generation, catalog or original observation
changes commit. Missing legacy source evidence keeps the existing schema-19
re-ingestion policy rather than fabricating proof.

### 5. Good / Base / Bad Cases
Good: `1000` and `1970-01-01T01:00:01.000+01:00` under proven ItemTable claims.
Base: one live decimal claimant retains its original spelling.
Bad: `1000`, null and `1970-01-01T00:00:01.000000001Z` must not be silently folded.

### 6. Tests Required
The `cursor_timestamp_upgrade_*` storage tests cover both replacement orders,
1-nanosecond differences, three-source null folding, per-claimant association,
unrelated/wrong/missing Document proof, source removal versus incomplete scans,
raw evidence equality, no-op behavior and other intrinsic fields. CLI tests must
also exercise identical snapshot copies, stable IDs, parser-4 to parser-5
re-ingestion and unchanged source bytes.

### 7. Wrong vs Correct
Wrong: normalize timestamps globally in `merge_message_payloads` or overwrite
old source observations to make them compare equal.
Correct: prove the variant per claimant, compare exact instants first, then use
a temporary aggregate-only alias that disappears with its last live claimant.

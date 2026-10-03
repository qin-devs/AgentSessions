# agentsessions-ports — Backend Guidelines

> Hexagonal port layer. Defines the trait boundaries between the application
> core and concrete adapters. No I/O, no concrete backend, no `std::fs`.

---

## Role in the architecture

`agentsessions-ports` is the **interface layer** of the hexagon. It declares the
traits (ports) that the application core depends on and that adapters
(`adapters-sqlite`, `provider-claude`, `provider-codex`) implement. It is the
narrowest crate on purpose: it names capabilities, it does not perform them.

Depends only on `agentsessions-domain`. Never depends on any adapter crate,
`rusqlite`, `serde_json`, or the filesystem.

---

## Pre-Development Checklist

- [ ] Am I adding a *capability the core needs*, or leaking an adapter detail?
      Ports describe what the core requires, not how SQLite/FS provides it.
- [ ] Does the new method belong on an existing port, or is it a genuinely new
      seam? Prefer extending an existing trait when there is one caller.
- [ ] Does the signature use domain types (`StableId`, `Session`, `Message`)
      and `PortResult<T>` — never `rusqlite::Error` or raw `io::Error`?
- [ ] If I add a `PortError` variant, does it map to a canonical protocol code
      in the CLI's `protocol.rs` `From<PortError>`? Adding a variant without
      updating that mapping breaks the error contract.
- [ ] Structured event params (e.g. `MessageEvent`) — should this be a struct
      rather than a positional tuple, so fields can be added without breaking
      every call site?

---

## Key types

- `PortError` / `PortResult<T>` — the error currency crossing every port.
  Variants: `Backend`, `SourceIo`, `SchemaIncompatible`, `NotFound`,
  `SnapshotChanged`, `WriterBusy`. Each maps to a canonical code downstream.
- `CanonicalEventSink` + `MessageEvent<'a>` — structured sink for provider
  parse output. `MessageEvent` is a struct (native_id / parent_native_id /
  role / text / timestamp / is_sidechain / span) precisely so new fields don't
  break callers. `span` is `(start, end)` byte offsets **into the verified
  snapshot bytes**, end exclusive, newline excluded; the unit is deliberately
  "snapshot bytes" (not "file bytes") so a future row-level source (SQLite
  provider) can satisfy the same contract by making the extracted row payload
  the snapshot. `None` means the provider cannot attribute a contiguous span —
  never fabricate one. `MessageEvent.session` optionally carries a
  `ProviderSessionIdentity { source_key, observation }` per message. The
  source-local key distinguishes native-less sessions and is never exposed
  as a provider-native ID. Report-level session fields are compatibility
  observations for messages without an explicit session.
- `ProviderAdapter` — `probe(bytes) -> Probe` + `parse(bytes, sink)`. The
  probe/select contract lives here; the selection *policy* lives in the
  application core.
- `SearchQuery<'a>` — literal query text plus normalized `SearchFilters`.
  Providers are validated canonical values from the capability matrix; time bounds use a normalized
  `(unix_seconds, nanosecond)` instant. Adapters must apply filters before
  storage `LIMIT`, not post-filter paginated hits.
- `SearchHit` — ranked stable identity plus additive text/session/guidance
  projection fields. `occurrences` is 1 on the default (non-grouped) path and
  the per-session collapse count on the grouped path. Guidance is assembled by
  Application; storage leaves `why_matched` and `suggested_next_commands`
  empty.
- `ContextGraphStore` — typed contextual reads:
  `load_session_graph(&SessionId)`, `message_contexts(&MessageId)`, and
  `context_stats()`. It returns Domain graphs, distinct-session candidates, and
  aggregate counts; no SQL row or JSON payload shape crosses the port.

## Scenario: Context graph reads

### 1. Scope / Trigger

Use this port when Application needs Message placement, edge, or reverse
session-membership facts that cannot be represented by `CatalogStore::get`.

### 2. Signatures

```rust
fn load_session_graph(&self, session_id: &StableId)
    -> PortResult<SessionContextGraph>;
fn message_contexts(&self, message_id: &StableId)
    -> PortResult<Vec<MessageContextCandidate>>;
fn context_stats(&self) -> PortResult<ContextStats>;
```

### 3. Contracts

- `message_contexts` groups by distinct Session. Several placements in one
  Session produce one candidate with several sorted `placement_ids`.
- `ContextStats` exposes only aggregate placement and source-claim counts.
- Implement `ContextGraphStore for &T` whenever a concrete store implements it,
  so one shared store reference can fill Application port slots.

### 4. Validation & Error Matrix

- Unknown Session → `PortError::NotFound`.
- Migrated legacy relation state that is not complete →
  `PortError::SchemaIncompatible`.
- Backend corruption/query failure → `PortError::Backend`.
- Ambiguous graph topology is Domain selection output, not a SQLite error.

### 5. Good/Base/Bad Cases

- Good: one Message has two placements in one Session → one candidate.
- Base: one complete Session graph → typed graph with no compatibility parsing.
- Bad: return raw SQL rows or JSON blobs → port-layer violation.

### 6. Tests Required

- `&T` blanket forwarding.
- Candidate grouping by Session.
- Not-found and schema-incompatible classification.
- Aggregate counts without source paths or payload content.

### 7. Wrong vs Correct

```rust
// Wrong: leaks persistence shape.
fn load_edges(&self, session: &str) -> Vec<rusqlite::Row<'_>>;

// Correct: backend-independent Domain values.
fn load_session_graph(&self, session: &StableId)
    -> PortResult<SessionContextGraph>;
```

---

## Scenario: Shared retrieval and source contracts

### 1. Scope / Trigger
Search ports and multi-session source events cross application/adapter boundaries.

### 2. Signatures
`SemanticIndex::is_ready(query_dimension: usize) -> PortResult<bool>`;
`semantic_model_id() -> PortResult<Option<String>>`;
`query_semantic_filtered(&[f32], usize, &SearchFilters, &SearchFacets, bool)`
returns `PortResult<Vec<SearchHit>>`. `SearchIndex::query_with_policy` accepts
the same facets and `include_system` visibility policy for lexical search.

### 3. Contracts
Apply filters/facets/visibility before top-k. Only `Ok(false)` readiness permits
explicit lexical fallback; errors retain their port classification. Readiness
requires vectors for the selected model, query dimension and live catalog; zero
dimension is not ready. Shared references must forward the exact dimension. Reject
non-finite vectors/scores. `SearchProvider` is a private canonical wrapper:
accepted IDs come from implemented searchable capability rows, with the
historical `claude` alias. Source fingerprints are opaque: file BLAKE3 hex or
`sqlite:<logical-backup-BLAKE3>`; source paths remain locators.

### 4. Validation & Error Matrix
Unknown/deferred provider -> boundary invalid request. Missing semantic index
-> explicit fallback. Backend/schema/busy failures -> errors, never absence.

### 5. Good/Base/Bad Cases
Good: two sessions in one source retain different observations. Base: absent
event session uses the report observation. Bad: treating a source-local key
as a native resume ID, or filtering an already truncated semantic result.

### 6. Tests Required
Assert registry/runtime/schema parity, trait forwarding, readiness error
propagation, metadata/facet visibility, and staged session preservation.

### 7. Wrong vs Correct
Wrong: `is_ready().unwrap_or(false)` or a second hard-coded provider list.
Correct: propagate readiness errors and derive accepted provider values from
the capability registry.

## Scenario: Bounded relocation contracts

### 1. Scope / Trigger
Adapters expose relocation results and errors to the composition root.

### 2. Signatures
`relocation::{RelocationResult, RelocationStatus}`;
`validate_alias_ttl_days(u32)`; `PortError::{InvalidRequest, GenerationMismatch}`.

### 3. Contracts
Result status is `planned|applied|unchanged`, with an optional opaque plan,
source/Session/installation/namespace counts, current/previous generation and
alias TTL. No public DTO contains physical paths, backup locations, native IDs
or transcript text. Defaults/ranges/plan lifetime are shared constants, not
repeated CLI literals. This is CLI-only mutation; no MCP/Web write port is added.

### 4. Validation & Error Matrix
Alias days outside 1..365 -> bounded invalid request. Storage and protocol keep
source-change, generation, lease and schema failures distinct. Every added
PortError has an exhaustive canonical-protocol mapping and internal kind label.

### 5. Good/Base/Bad Cases
Good: Robot emits an opaque plan and aggregate counts. Base: `unchanged` omits a
new plan. Bad: DTO serialization publishes private mapping or receipt fields.

### 6. Tests Required
Assert serialization shape/counts, default and boundary policies, and every
new error mapping without echoing caller-controlled details.

### 7. Wrong vs Correct
Wrong: expose SQLite rows or paths as a public relocation result.
Correct: expose typed counts/status and keep private mappings in the adapter.

## Quality Check

- No `use rusqlite`, no `use std::fs`, no `serde_json` in this crate.
- Every port method returns `PortResult<_>`; concrete adapter errors are
  wrapped into `PortError` by the adapter, never surfaced raw.
- New `PortError` variants have a matching arm in the CLI protocol mapping.
- Traits stay object-safe where the composition root needs `&dyn Trait`
  (e.g. `ProviderAdapter` is used as `&dyn ProviderAdapter` in the registry).
- `cargo fmt --all --check` + `cargo clippy --workspace --all-targets -- -D warnings`.

---

**Language**: All documentation in **English**.

## Scenario: Request read snapshots
1. **Scope:** one read request across catalog/search/context/resume ports.
2. **Signature:** `CatalogStore::begin_read_snapshot() -> PortResult<Box<dyn ReadSnapshot + '_>>`; `&T` forwards it.
3. **Contract:** returned guards represent an already pinned view; nested guards share it until the last drops, including non-LIFO release. All mutable ports must use the same backend session. The default no-op is only for immutable/in-memory fixtures; persistent adapters and forwarding wrappers must override it.
4. **Errors:** acquisition failure propagates as a port error; early return/unwind must release the guard. Adapter cleanup failure must fail closed on later acquisition, never silently reuse a stale snapshot.
5. **Cases:** good: search IDs and their payloads share a generation; bad: independently reopened connections or a RefCell borrow retained through port calls.
6. **Tests:** actual WAL writer interleavings, reference forwarding, nested scope and error/unwind cleanup.
7. **Boundary:** this is a read capability, not an exposed SQLite transaction. Never hold it across writes, model loading, prompts or network/output waits.


## Scenario: Provider timestamp and single-document BOM fidelity

### 1. Scope / Trigger
Provider parsing changes timestamp representations without changing source bytes,
message identity, traversal order or provider maturity. Unit evidence is local to
the provider field and variant, not a rule for every numeric timestamp.

### 2. Signatures
`MessageEvent.timestamp: Option<&str>` and `ParseReport::{skipped, diagnostics}`;
`ProviderAdapter::{probe, parse}` and the default bounded source entry points.
No new port, public DTO field or dependency is introduced.

### 3. Contracts
- Cursor ItemTable `timingInfo.startTime` and prompt `createdAt` integers are
  Unix milliseconds. Normalize to UTC with exact millisecond precision; sort on
  the original integers. Disk-kv bubble `createdAt` strings retain their spelling,
  offset and fractional precision. Numeric bubble values have no proven unit and
  must not borrow the composer's timestamp or a magnitude-based unit heuristic.
- Native Cline `ts` is authoritative when present and accepts only integer JSON
  tokens representable as i64. Do not use f64 or `fract() == 0`: deserialization
  may already have rounded fractional tokens or underflowed them to zero.
  Without `ts`, valid legacy `timestamp` date-time strings retain their spelling;
  numeric compatibility values have no proven unit. Null/missing time stays absent.
- Integer formatting is limited to four-digit years 0000..9999. Invalid Cline
  timestamp metadata, out-of-range ItemTable integers and unsupported disk-kv
  bubble metadata omit the time with a content-free diagnostic, retaining the
  message. Cline aggregates its metadata-loss count; `skipped` counts lost
  messages, not lost fields. ItemTable's existing whole-value type errors remain
  distinct from this representable-integer conversion rule.
- Cline probe and parse strip exactly one UTF-8 BOM at byte zero of the single
  JSON document. Repeated/mid-document BOM stays invalid; U+FEFF within text is
  preserved. This is not a change to shared JSONL readers or byte-span authority.
- Parser 5 re-ingests unchanged sources once; schema stays 19. Since is inclusive,
  until exclusive. Source identities and bytes remain unchanged. Cursor/Cline
  stay Experimental; pinned implementation evidence is not format certification.

### 4. Validation & Error Matrix
Missing/null metadata -> no invented timestamp or warning. Unsupported timestamp
metadata -> bounded field-loss diagnostic, not an empty string or a guessed time.
Invalid single-document JSON/BOM placement -> existing probe/parse refusal.
Cross-source intrinsic conflicts retain storage authority; only proven Cursor
ItemTable spelling upgrades get the narrow aggregate-only storage exception.

### 5. Good / Base / Bad Cases
Good: native Cline `ts: -1` -> `1969-12-31T23:59:59.999Z`.
Base: no timestamp remains `None`.
Bad: numeric legacy `timestamp`, `1e-400` or `1735689600123.00001` must not become
an invented integral instant; a Cline `ts` error must not fall back to another field.

### 6. Tests Required
Provider timestamp suites use raw JSON bytes for precision-loss counterexamples,
plus pre-epoch/range/leap, missing/null, text/order/ID, BOM placement and bounded
Cline diagnostic cases. Golden provenance pins units and source hashes. CLI
covers millisecond/offset/nanosecond filters, one-time reparse, unchanged source
bytes, duplicate-Cursor rolling replacement and the fixture revision matrix.

### 7. Wrong vs Correct
Wrong: `numeric.as_f64().filter(|v| v.fract() == 0.0)` or treating every `createdAt`
as the same unit. Correct: validate the original integer token and the exact
field/variant evidence, preserving unknown metadata as absent with diagnostics.

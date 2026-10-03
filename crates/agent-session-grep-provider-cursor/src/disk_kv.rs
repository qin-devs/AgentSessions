//! Cursor `cursorDiskKV` SQLite variant (`cursor/disk-kv-v1`).
//!
//! Reads the newer Cursor IDE chat surface: a `cursorDiskKV(key, value)` table
//! inside `state.vscdb`, where `composerData:<composerId>` holds one
//! conversation's metadata (`fullConversationHeadersOnly`) and
//! `bubbleId:<composerId>:<bubbleId>` holds one message body. This variant is
//! additive next to `cursor/vscdb-chat-v1` (ItemTable `chatdata`/`prompts`) and
//! the two are mutually exclusive: a stream that satisfies both is refused as
//! ambiguous instead of guessed.
//!
//! Deliberate boundaries (each one is a decision, not an oversight):
//!
//! * **Header order is the only order.** Messages come out strictly in the
//!   order of `fullConversationHeadersOnly`; KV key order, rowid order and
//!   insertion order are never used for messages. Duplicate `bubbleId`s keep
//!   independent header ordinals, and composers are enumerated in byte-wise
//!   (`COLLATE BINARY`) key order, which is also insertion-order independent.
//! * **Bad slots stay accounted for.** A missing row, SQL NULL, malformed JSON,
//!   non-object body, invalid UTF-8, non-text `text`, malformed tool envelope or
//!   unknown header role is *not* silently dropped: the slot keeps its ordinal
//!   in a diagnostic, the state is counted per composer, and one `skipped` is
//!   reported. A broken bubble never discards its session.
//! * **Tool input is text, never an execution.** The tool input prefers
//!   `rawArgs`, then `params`; an object, a JSON string or plain text are all
//!   accepted and the selected value is only ever rendered as text. A bubble
//!   with no usable text of its own falls back to that rendered input; nothing
//!   is ever executed.
//! * **Identity is not fabricated.** `bubbleId` is a storage-key component
//!   scoped to its composer, and Cursor's private storage has no official
//!   versioned contract, so no global stability can be proven. Messages
//!   therefore report an empty `native_id` (the composition root derives
//!   document-scoped ids), and verbatim composer/bubble ids appear only as
//!   observations: the session identity uses the composer id, and diagnostics
//!   name the exact storage key a slot resolved to - never rewritten,
//!   normalized, filtered or hashed.
//! * **Bounds are honest.** Composer rows, header slots, one cell and the total
//!   materialized cell bytes are capped; exceeding any cap fails the source
//!   explicitly instead of truncating it.
//! * **Read-only snapshot.** The adapter never opens the source database: it
//!   reads a private read-only copy of the captured bytes under one pinned read
//!   transaction (see the crate-level `open_readonly_from_bytes`), so
//!   `state.vscdb`/`-wal`/`-shm` are untouched and a concurrent committed write
//!   cannot change what one parse observes.
//!
//! Format evidence: the frozen synthetic probe suite and the pinned Wake
//! implementation are cited in `tests/golden/PROVENANCE.md`; Cursor's private
//! storage has no located official version contract, so nothing here is a
//! compatibility certification.

use agent_session_grep_ports::{
    CanonicalEventSink, Confidence, MessageEvent, MetadataResolution, ParseReport, ProbeResult,
    ProviderError, ProviderSessionIdentity, ProviderSessionObservation, SQLITE_MAX_SOURCE_BYTES,
};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OptionalExtension, params};

/// Variant id surfaced in probe results.
pub(crate) const VARIANT_ID: &str = "cursor/disk-kv-v1";

/// `state.vscdb` table holding the disk key/value chat surface.
const TABLE: &str = "cursorDiskKV";

/// Key prefix of one conversation's metadata row: `composerData:<composerId>`.
pub(crate) const COMPOSER_PREFIX: &str = "composerData:";

/// Key prefix of one message body row: `bubbleId:<composerId>:<bubbleId>`.
pub(crate) const BUBBLE_PREFIX: &str = "bubbleId:";

/// Columns this variant requires to identify the format.
pub(crate) const REQUIRED_COLUMNS: &[&str] = &["key", "value"];

/// One diagnostic explaining why messages carry no adopted native id.
const IDENTITY_NOTE: &str = "cursorDiskKV: messages report no adopted native id (`bubbleId` is a \
    composer-scoped storage-key component and Cursor's private storage has no official versioned \
    contract), so the composition root derives document-scoped ids; verbatim composer/bubble ids \
    appear only as observations in these diagnostics";

/// Bounds for one probe/parse.
///
/// The defaults are the production limits. They stay injectable so the failure
/// path of every bound is pinned by a small synthetic fixture instead of a
/// multi-megabyte one - a bound that is never exercised is not a bound.
#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) max_source_bytes: u64,
    pub(crate) max_composers: u64,
    pub(crate) max_headers_per_composer: u64,
    pub(crate) max_total_headers: u64,
    pub(crate) max_cell_bytes: u64,
    pub(crate) max_total_cell_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_source_bytes: SQLITE_MAX_SOURCE_BYTES,
            max_composers: 4_096,
            // 100_000 header slots in one conversation is far past any observed
            // composer; the cap exists so one row cannot materialize the world.
            max_headers_per_composer: 100_000,
            max_total_headers: 250_000,
            // 8 MiB per cell: the same order as the record-stream cap for line
            // formats, and larger than any single composerData/bubbleId body.
            max_cell_bytes: 8 * 1024 * 1024,
            max_total_cell_bytes: 64 * 1024 * 1024,
        }
    }
}

/// A table name is a real table (not index/view) plus its column list.
fn table_columns(conn: &Connection, table: &str) -> Result<Option<Vec<String>>, ProviderError> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1 COLLATE BINARY LIMIT 1",
            [table],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if exists.is_none() {
        return Ok(None);
    }
    let mut statement = conn
        .prepare("SELECT name FROM pragma_table_info(?1)")
        .map_err(sql_error)?;
    let names = statement
        .query_map([table], |row| row.get::<_, String>(0))
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    Ok(Some(names))
}

fn missing_columns(present: &[String], required: &[&str]) -> Vec<String> {
    required
        .iter()
        .filter(|column| !present.iter().any(|name| name == *column))
        .map(|column| (*column).to_string())
        .collect()
}

fn sql_error(error: rusqlite::Error) -> ProviderError {
    // SQLite errors here name only the private temp copy this adapter created;
    // the captured source path never reaches this connection.
    ProviderError::StructuralFatal(format!("cursorDiskKV query failed: {error}"))
}

/// Validate the `cursorDiskKV` shape on an already-open snapshot.
///
/// A missing table is a structural fatal for this variant (callers decide
/// relevance via [`claims`]); a table carrying the Cursor name with the wrong
/// columns is always a structural fatal, never a silent non-match.
fn require_schema(conn: &Connection) -> Result<Vec<String>, ProviderError> {
    let Some(columns) = table_columns(conn, TABLE)? else {
        return Err(ProviderError::StructuralFatal(format!(
            "cursorDiskKV has no `{TABLE}` table"
        )));
    };
    let missing = missing_columns(&columns, REQUIRED_COLUMNS);
    if !missing.is_empty() {
        return Err(ProviderError::StructuralFatal(format!(
            "cursorDiskKV table is missing key column(s): {}",
            missing.join(", ")
        )));
    }
    Ok(columns)
}

/// True when any key starts (byte-wise) with `prefix`; reads no value cells.
///
/// `GLOB` is case-sensitive and its pattern language uses `*`/`?`/`[...]` - the
/// caller's literal prefixes contain none of those, so the pattern is exact for
/// every id (including Unicode and storage separators).
fn prefix_exists(conn: &Connection, prefix: &str) -> Result<bool, ProviderError> {
    let pattern = format!("{prefix}*");
    conn.prepare("SELECT 1 FROM cursorDiskKV WHERE key GLOB ?1 LIMIT 1")
        .and_then(|mut statement| statement.exists([pattern]))
        .map_err(sql_error)
}

/// Whether this stream carries the disk-kv surface: the `cursorDiskKV` table
/// with its `key`/`value` columns plus at least one `composerData:` key.
///
/// A `cursorDiskKV` table with the Cursor name but a broken column shape is a
/// structural fatal, never a silent non-match.
pub(crate) fn claims(conn: &Connection) -> Result<bool, ProviderError> {
    let Some(columns) = table_columns(conn, TABLE)? else {
        return Ok(false);
    };
    let missing = missing_columns(&columns, REQUIRED_COLUMNS);
    if !missing.is_empty() {
        return Err(ProviderError::StructuralFatal(format!(
            "cursorDiskKV table is missing key column(s): {}",
            missing.join(", ")
        )));
    }
    prefix_exists(conn, COMPOSER_PREFIX)
}

/// Count rows whose key starts with `prefix`, capped at `cap + 1`.
fn bounded_prefix_count(conn: &Connection, prefix: &str, cap: u64) -> Result<u64, ProviderError> {
    let sql = "SELECT count(*) FROM (SELECT 1 FROM cursorDiskKV WHERE key GLOB ?1 LIMIT ?2)";
    let count: i64 = conn
        .query_row(
            sql,
            params![format!("{prefix}*"), cap.saturating_add(1) as i64],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(count.max(0) as u64)
}

/// One raw cell value, materialized under the byte budget.
enum Cell {
    Null,
    Integer,
    Real,
    Bytes(Vec<u8>),
}

/// Materialization budget for one database.
struct Materialization {
    limits: Limits,
    cell_bytes: u64,
}

impl Materialization {
    fn new(limits: &Limits) -> Self {
        Self {
            limits: *limits,
            cell_bytes: 0,
        }
    }

    fn charge_cell(&mut self, actual: u64) -> Result<(), ProviderError> {
        if actual > self.limits.max_cell_bytes {
            return Err(ProviderError::RecordTooLarge {
                actual,
                max: self.limits.max_cell_bytes,
            });
        }
        self.cell_bytes = self.cell_bytes.saturating_add(actual);
        if self.cell_bytes > self.limits.max_total_cell_bytes {
            return Err(ProviderError::SourceTooLarge {
                actual: self.cell_bytes,
                max: self.limits.max_total_cell_bytes,
            });
        }
        Ok(())
    }
}

/// Read one cell, charging its byte length before copying it.
fn read_cell(
    row: &rusqlite::Row<'_>,
    index: usize,
    budget: &mut Materialization,
) -> Result<Cell, ProviderError> {
    let value = row.get_ref(index).map_err(sql_error)?;
    let len = match value {
        ValueRef::Null => 0,
        ValueRef::Integer(_) | ValueRef::Real(_) => 8,
        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => bytes.len() as u64,
    };
    budget.charge_cell(len)?;
    Ok(match value {
        ValueRef::Null => Cell::Null,
        ValueRef::Integer(_) => Cell::Integer,
        ValueRef::Real(_) => Cell::Real,
        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => Cell::Bytes(bytes.to_vec()),
    })
}

/// Text cell -> UTF-8 string. NULL is absent; numbers are a wrong SQLite
/// type; invalid UTF-8 is its own explicit state.
fn text_cell(cell: &Cell) -> Result<Option<&str>, SlotState> {
    match cell {
        Cell::Null => Ok(None),
        Cell::Bytes(bytes) => std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|_| SlotState::InvalidUtf8),
        Cell::Integer | Cell::Real => Err(SlotState::InvalidTextType),
    }
}

/// Every per-slot and per-composer state this variant reports.
///
/// The names are the frozen probe-suite vocabulary (`ok`, `missing_row`,
/// `null_value`, `malformed_json`, `invalid_bubble_shape`, `invalid_utf8`, ...)
/// plus the composer-row states this parser has to distinguish.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotState {
    Ok,
    InvalidHeader,
    UnknownRole,
    MissingRow,
    NullValue,
    DuplicateRow,
    InvalidUtf8,
    InvalidTextType,
    MalformedJson,
    InvalidBubbleShape,
    InvalidToolShape,
    EmptyBubble,
    InvalidComposerShape,
    MissingHeaders,
    InvalidHeaders,
}

impl SlotState {
    /// Rendering order of the per-composer state counts; also the exhaustive
    /// list every summary line draws from.
    const ALL: [SlotState; 15] = [
        SlotState::Ok,
        SlotState::InvalidHeader,
        SlotState::UnknownRole,
        SlotState::MissingRow,
        SlotState::NullValue,
        SlotState::DuplicateRow,
        SlotState::InvalidUtf8,
        SlotState::InvalidTextType,
        SlotState::MalformedJson,
        SlotState::InvalidBubbleShape,
        SlotState::InvalidToolShape,
        SlotState::EmptyBubble,
        SlotState::InvalidComposerShape,
        SlotState::MissingHeaders,
        SlotState::InvalidHeaders,
    ];

    fn as_str(self) -> &'static str {
        match self {
            SlotState::Ok => "ok",
            SlotState::InvalidHeader => "invalid_header",
            SlotState::UnknownRole => "unknown_role",
            SlotState::MissingRow => "missing_row",
            SlotState::NullValue => "null_value",
            SlotState::DuplicateRow => "duplicate_row",
            SlotState::InvalidUtf8 => "invalid_utf8",
            SlotState::InvalidTextType => "invalid_text_type",
            SlotState::MalformedJson => "malformed_json",
            SlotState::InvalidBubbleShape => "invalid_bubble_shape",
            SlotState::InvalidToolShape => "invalid_tool_shape",
            SlotState::EmptyBubble => "empty_bubble",
            SlotState::InvalidComposerShape => "invalid_composer_shape",
            SlotState::MissingHeaders => "missing_headers",
            SlotState::InvalidHeaders => "invalid_headers",
        }
    }

    fn index(self) -> usize {
        SlotState::ALL
            .iter()
            .position(|state| *state == self)
            .expect("every SlotState is listed in SlotState::ALL")
    }
}

/// Per-state counts of one composer's header slots.
#[derive(Clone, Copy)]
struct StateCounts([u64; 15]);

impl StateCounts {
    fn new() -> Self {
        Self([0; 15])
    }

    fn record(&mut self, state: SlotState) {
        self.0[state.index()] += 1;
    }

    /// `ok=3 missing_row=1 ...` - every state that occurred, in a fixed order.
    /// `ok` is always rendered so a clean composer cannot look like a composer
    /// with no accounting at all.
    fn render(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        for state in SlotState::ALL {
            let count = self.0[state.index()];
            if count > 0 || state == SlotState::Ok {
                parts.push(format!("{}={count}", state.as_str()));
            }
        }
        parts.join(" ")
    }
}

/// Outcome of walking one composer: what it emitted, what it skipped, and the
/// diagnostics that name the slots it could not use.
struct ComposerOutcome {
    counts: StateCounts,
    emitted: u64,
    skipped: u64,
    notes: Vec<String>,
}

impl ComposerOutcome {
    fn new() -> Self {
        Self {
            counts: StateCounts::new(),
            emitted: 0,
            skipped: 0,
            notes: Vec::new(),
        }
    }

    /// Record one defective composer row or header slot.
    fn skip(mut self, state: SlotState) -> Self {
        self.counts.record(state);
        self.skipped += 1;
        self
    }

    fn total_slots(&self) -> u64 {
        self.counts.0.iter().sum()
    }
}

/// Mutable projection state for one parse: the sink plus the single
/// monotonically increasing `seq` and committed counter covering the document.
struct WalkState<'a> {
    sink: &'a mut dyn CanonicalEventSink,
    seq: u32,
    committed: usize,
}

/// Probe the disk-kv surface on an already-open snapshot.
pub(crate) fn probe(
    conn: &Connection,
    source_len: u64,
    limits: &Limits,
) -> Result<ProbeResult, ProviderError> {
    if source_len > limits.max_source_bytes {
        return Err(ProviderError::SourceTooLarge {
            actual: source_len,
            max: limits.max_source_bytes,
        });
    }
    let columns = require_schema(conn)?;

    let mut matched = vec!["SQLite magic header detected".to_string()];
    let mut unmatched = Vec::new();
    matched.push(format!("`{TABLE}` table ({} columns)", columns.len()));
    matched.push("cursorDiskKV(key, value) present".to_string());

    let composers = bounded_prefix_count(conn, COMPOSER_PREFIX, limits.max_composers)?;
    if composers == 0 {
        return Err(ProviderError::AmbiguousVariant(
            "cursorDiskKV has no `composerData:` key - not a Cursor disk KV chat surface".into(),
        ));
    }
    matched.push(format!("at most {composers} composerData: key(s)"));

    let bubbles = bounded_prefix_count(conn, BUBBLE_PREFIX, limits.max_total_headers)?;
    if bubbles == 0 {
        unmatched.push(
            "no `bubbleId:` keys (a composer may legitimately hold no stored bubbles)".into(),
        );
    } else {
        matched.push(format!("at most {bubbles} bubbleId: key(s)"));
    }

    Ok(ProbeResult {
        variant_id: VARIANT_ID.to_string(),
        confidence: Confidence::Confirmed,
        matched_evidence: matched,
        unmatched_evidence: unmatched,
    })
}

/// One exact-key lookup outcome.
enum Lookup {
    Missing,
    Duplicate,
    Null,
    Text(String),
    InvalidUtf8,
    InvalidType,
}

/// Look up one exact storage key, byte-for-byte (`COLLATE BINARY`, no
/// normalization, no prefix wildcards) and materialize its value cell.
///
/// Two rows under one key are ambiguous, so they are reported as
/// [`Lookup::Duplicate`] rather than silently picking one.
fn lookup_value(
    conn: &Connection,
    key: &str,
    budget: &mut Materialization,
) -> Result<Lookup, ProviderError> {
    let mut statement = conn
        .prepare("SELECT value FROM cursorDiskKV WHERE key = ?1 COLLATE BINARY LIMIT 2")
        .map_err(sql_error)?;
    let mut rows = statement.query([key]).map_err(sql_error)?;
    let Some(row) = rows.next().map_err(sql_error)? else {
        return Ok(Lookup::Missing);
    };
    let cell = read_cell(row, 0, budget)?;
    if rows.next().map_err(sql_error)?.is_some() {
        return Ok(Lookup::Duplicate);
    }
    Ok(match cell {
        Cell::Null => Lookup::Null,
        Cell::Integer | Cell::Real => Lookup::InvalidType,
        Cell::Bytes(bytes) => match std::str::from_utf8(&bytes) {
            Ok(text) => Lookup::Text(text.to_string()),
            Err(_) => Lookup::InvalidUtf8,
        },
    })
}

/// Parse one JSON document and require a top-level object.
fn json_object(raw: &str, wrong_shape: SlotState) -> Result<serde_json::Value, SlotState> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| SlotState::MalformedJson)?;
    if !value.is_object() {
        return Err(wrong_shape);
    }
    Ok(value)
}

/// Render one tool-input candidate as text; `None` when it is absent, JSON
/// `null` or blank text.
///
/// A string is kept verbatim (never parsed and never executed); an object,
/// array, number or boolean is rendered as compact JSON so the searchable text
/// is deterministic.
fn input_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(text) if text.trim().is_empty() => None,
        serde_json::Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

/// Selected tool input, `rawArgs` first and `params` second.
fn selected_input(tool: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    ["rawArgs", "params"]
        .into_iter()
        .find_map(|field| tool.get(field).and_then(input_text))
}

/// Validate the `toolFormerData` envelope and return its selected input text.
///
/// `Ok(None)` means the bubble has no tool envelope at all; `Ok(Some(""))`
/// means the envelope is valid but has no usable input. `Err` is the
/// `invalid_tool_shape` slot state.
fn tool_input(
    body: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<String>, SlotState> {
    let Some(raw) = body.get("toolFormerData") else {
        return Ok(None);
    };
    let Some(tool) = raw.as_object() else {
        return Err(SlotState::InvalidToolShape);
    };
    if !matches!(tool.get("name"), Some(serde_json::Value::String(_))) {
        return Err(SlotState::InvalidToolShape);
    }
    if let Some(call_id) = tool.get("toolCallId")
        && !call_id.is_null()
        && !call_id.is_string()
    {
        return Err(SlotState::InvalidToolShape);
    }
    Ok(Some(selected_input(tool).unwrap_or_default()))
}

/// Session identity of one composer: the composer id is the observation and
/// the source-local key; an empty id falls back to an anonymous source key,
/// exactly like the ItemTable variant's id-less sessions.
fn composer_identity(composer_id: &str, ordinal: usize) -> (ProviderSessionIdentity, bool) {
    let named = !composer_id.is_empty();
    let observation = ProviderSessionObservation {
        provider_session_id: if named {
            MetadataResolution::Resolved(composer_id.to_string())
        } else {
            MetadataResolution::Missing
        },
        ..Default::default()
    };
    let identity = ProviderSessionIdentity {
        source_key: if named {
            composer_id.to_string()
        } else {
            format!("anonymous-composer-{ordinal}")
        },
        observation,
    };
    (identity, !named)
}

/// Walk one composer in `fullConversationHeadersOnly` order.
///
/// Every header index produces exactly one outcome: an emitted message, or a
/// counted `skipped` slot named by its ordinal in `notes`.
fn walk_composer(
    conn: &Connection,
    composer_key: &str,
    ordinal: usize,
    limits: &Limits,
    budget: &mut Materialization,
    state: &mut WalkState<'_>,
) -> Result<ComposerOutcome, ProviderError> {
    let composer_id = composer_key
        .strip_prefix(COMPOSER_PREFIX)
        .unwrap_or(composer_key);
    let (identity, anonymous) = composer_identity(composer_id, ordinal);
    let mut outcome = ComposerOutcome::new();
    if anonymous {
        outcome.notes.push(format!(
            "cursorDiskKV composer slot {ordinal}: key `{composer_key}` has an empty suffix; \
             it uses the source-local key `{}`",
            identity.source_key
        ));
    }

    let composer = match lookup_value(conn, composer_key, budget)? {
        Lookup::Text(text) => text,
        Lookup::Missing => {
            return Ok(outcome
                .note(format!(
                    "cursorDiskKV composer `{composer_id}`: composer row missing; composer skipped"
                ))
                .skip(SlotState::MissingRow));
        }
        Lookup::Duplicate => {
            return Ok(outcome
                .note(format!(
                    "cursorDiskKV composer `{composer_id}`: duplicate composer rows; composer \
                     skipped"
                ))
                .skip(SlotState::DuplicateRow));
        }
        Lookup::Null => {
            return Ok(outcome
                .note(format!(
                    "cursorDiskKV composer `{composer_id}`: composer row has NULL value; composer \
                     skipped"
                ))
                .skip(SlotState::NullValue));
        }
        Lookup::InvalidUtf8 => {
            return Ok(outcome
                .note(format!(
                    "cursorDiskKV composer `{composer_id}`: composer row is not valid UTF-8; \
                     composer skipped"
                ))
                .skip(SlotState::InvalidUtf8));
        }
        Lookup::InvalidType => {
            return Ok(outcome
                .note(format!(
                    "cursorDiskKV composer `{composer_id}`: composer row has a non-text SQLite \
                     type; composer skipped"
                ))
                .skip(SlotState::InvalidTextType));
        }
    };

    let data = match json_object(&composer, SlotState::InvalidComposerShape) {
        Ok(data) => data,
        Err(state) => {
            return Ok(outcome
                .note(format!(
                    "cursorDiskKV composer `{composer_id}`: composer row {}; composer skipped",
                    state.as_str()
                ))
                .skip(state));
        }
    };
    let data = data.as_object().expect("json_object returned an object");
    let Some(headers) = data.get("fullConversationHeadersOnly") else {
        return Ok(outcome
            .note(format!(
                "cursorDiskKV composer `{composer_id}`: `fullConversationHeadersOnly` is missing; \
                 composer skipped"
            ))
            .skip(SlotState::MissingHeaders));
    };
    let Some(headers) = headers.as_array() else {
        return Ok(outcome
            .note(format!(
                "cursorDiskKV composer `{composer_id}`: `fullConversationHeadersOnly` is not an \
                 array; composer skipped"
            ))
            .skip(SlotState::InvalidHeaders));
    };
    if headers.len() as u64 > limits.max_headers_per_composer {
        return Err(ProviderError::RecordTooLarge {
            actual: headers.len() as u64,
            max: limits.max_headers_per_composer,
        });
    }

    for (slot, header) in headers.iter().enumerate() {
        let Some(bubble_id) = header
            .as_object()
            .and_then(|header| header.get("bubbleId"))
            .and_then(|value| value.as_str())
            .filter(|id| !id.is_empty())
        else {
            outcome.counts.record(SlotState::InvalidHeader);
            outcome.skipped += 1;
            outcome.notes.push(format!(
                "cursorDiskKV composer `{composer_id}`: header slot {slot}: invalid_header"
            ));
            continue;
        };

        // The storage key is the composer id and the bubble id joined by `:`,
        // verbatim. No Unicode normalization, no delimiter splitting: the
        // lookup can only ever resolve what the storage key itself spells.
        let key = format!("{BUBBLE_PREFIX}{composer_id}:{bubble_id}");
        let body = match lookup_value(conn, &key, budget)? {
            Lookup::Text(text) => text,
            Lookup::Missing => {
                outcome = outcome.defect(slot, &key, SlotState::MissingRow, composer_id);
                continue;
            }
            Lookup::Duplicate => {
                outcome = outcome.defect(slot, &key, SlotState::DuplicateRow, composer_id);
                continue;
            }
            Lookup::Null => {
                outcome = outcome.defect(slot, &key, SlotState::NullValue, composer_id);
                continue;
            }
            Lookup::InvalidUtf8 => {
                outcome = outcome.defect(slot, &key, SlotState::InvalidUtf8, composer_id);
                continue;
            }
            Lookup::InvalidType => {
                outcome = outcome.defect(slot, &key, SlotState::InvalidTextType, composer_id);
                continue;
            }
        };

        let body = match json_object(&body, SlotState::InvalidBubbleShape) {
            Ok(body) => body,
            Err(state) => {
                outcome = outcome.defect(slot, &key, state, composer_id);
                continue;
            }
        };
        let body = body.as_object().expect("json_object returned an object");

        // Roles come from the header's `type` only (1=user, 2=assistant).
        // An unknown role is never silently folded into `assistant`.
        let role = match header.get("type").and_then(|value| value.as_i64()) {
            Some(1) => "user",
            Some(2) => "assistant",
            _ => {
                outcome = outcome.defect(slot, &key, SlotState::UnknownRole, composer_id);
                continue;
            }
        };

        let text = match body.get("text") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(text)) => Some(text.clone()),
            Some(_) => {
                outcome = outcome.defect(slot, &key, SlotState::InvalidTextType, composer_id);
                continue;
            }
        };

        let input = match tool_input(body) {
            Ok(input) => input,
            Err(state) => {
                outcome = outcome.defect(slot, &key, state, composer_id);
                continue;
            }
        };

        // The canonical stream has no tool-call slot: the bubble's own text is
        // the message text, and a tool-only bubble falls back to the selected
        // input text so the input stays searchable. Never executed, only text.
        let message_text = text
            .filter(|text| !text.trim().is_empty())
            .or_else(|| input.filter(|input| !input.trim().is_empty()));
        let Some(message_text) = message_text else {
            outcome = outcome.defect(slot, &key, SlotState::EmptyBubble, composer_id);
            continue;
        };

        // The proven bubble field is an ISO string, not the composer's
        // numeric epoch metadata. Preserve the source spelling (including
        // offset and fractional precision); never infer a unit or invent a
        // timestamp from the composer when this field is absent.
        let timestamp = match body.get("createdAt") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(value)) if !value.trim().is_empty() => {
                Some(value.as_str())
            }
            Some(_) => {
                outcome.notes.push(format!(
                    "cursorDiskKV composer `{composer_id}`: header slot {slot}: \
                     timestamp omitted (createdAt must be a non-empty ISO string)"
                ));
                None
            }
        };

        state
            .sink
            .emit_message(MessageEvent {
                session: Some(&identity),
                seq: state.seq,
                // See the module docs: a bubble id is a composer-scoped storage-key
                // component, not a proven globally stable native id.
                native_id: "",
                parent_native_id: None,
                role,
                text: &message_text,
                timestamp,
                is_sidechain: false,
                span: None, // SQLite rows have no byte spans in the verified snapshot
            })
            .map_err(|error| ProviderError::StructuralFatal(error.to_string()))?;
        state.seq = state.seq.checked_add(1).ok_or_else(|| {
            ProviderError::StructuralFatal("too many cursorDiskKV messages".into())
        })?;
        state.committed += 1;
        outcome.counts.record(SlotState::Ok);
        outcome.emitted += 1;
    }

    Ok(outcome)
}

impl ComposerOutcome {
    fn note(mut self, note: String) -> Self {
        self.notes.push(note);
        self
    }

    /// Record one defective header slot and name the exact storage key it
    /// resolved to (the verbatim observation of the composer/bubble ids).
    fn defect(mut self, slot: usize, key: &str, state: SlotState, composer_id: &str) -> Self {
        self.counts.record(state);
        self.skipped += 1;
        self.notes.push(format!(
            "cursorDiskKV composer `{composer_id}`: header slot {slot} (`{key}`): {}",
            state.as_str()
        ));
        self
    }
}

/// Parse one `cursorDiskKV` snapshot into canonical messages.
///
/// Composers are walked in byte-wise key order; messages inside one composer
/// strictly follow `fullConversationHeadersOnly`. One monotonically increasing
/// source-local `seq` covers the whole document.
pub(crate) fn parse(
    conn: &Connection,
    source_len: u64,
    sink: &mut dyn CanonicalEventSink,
    limits: &Limits,
) -> Result<ParseReport, ProviderError> {
    if source_len > limits.max_source_bytes {
        return Err(ProviderError::SourceTooLarge {
            actual: source_len,
            max: limits.max_source_bytes,
        });
    }
    require_schema(conn)?;

    let mut budget = Materialization::new(limits);
    let mut report = ParseReport::default();
    report.diagnostics.push(IDENTITY_NOTE.to_string());

    // Composer keys: byte-wise distinct and ordered, so neither insertion order
    // nor rowid order can influence the walk. `GLOB` is exact for the literal
    // prefix and never treats `%`/`_` in ids as wildcards.
    let mut composer_keys: Vec<String> = Vec::new();
    {
        let mut statement = conn
            .prepare(
                "SELECT DISTINCT key COLLATE BINARY FROM cursorDiskKV WHERE key GLOB ?1 ORDER BY 1",
            )
            .map_err(sql_error)?;
        let mut rows = statement
            .query([format!("{COMPOSER_PREFIX}*")])
            .map_err(sql_error)?;
        while let Some(row) = rows.next().map_err(sql_error)? {
            if composer_keys.len() as u64 >= limits.max_composers {
                return Err(ProviderError::SourceTooLarge {
                    actual: composer_keys.len() as u64 + 1,
                    max: limits.max_composers,
                });
            }
            let cell = read_cell(row, 0, &mut budget)?;
            match text_cell(&cell) {
                Ok(Some(key)) => composer_keys.push(key.to_string()),
                Ok(None) => {
                    report.skipped += 1;
                    report.diagnostics.push(
                        "cursorDiskKV: a composerData: key row with a NULL key was skipped".into(),
                    );
                }
                Err(state) => {
                    report.skipped += 1;
                    report.diagnostics.push(format!(
                        "cursorDiskKV: a composerData: key was skipped ({})",
                        state.as_str()
                    ));
                }
            }
        }
    }

    match composer_keys.len() {
        0 => report
            .diagnostics
            .push("cursorDiskKV: no `composerData:` keys found; nothing to parse".into()),
        1 => {
            let id = composer_keys[0]
                .strip_prefix(COMPOSER_PREFIX)
                .unwrap_or(&composer_keys[0]);
            if !id.is_empty() {
                report.session_native_id = Some(id.to_string());
                report.session_observation.provider_session_id =
                    MetadataResolution::Resolved(id.to_string());
            }
        }
        _ => {
            // Several composers in one database: each message carries its own
            // membership and the report level fails closed instead of claiming
            // one native session for the source (ADR-0009).
            let first = composer_keys
                .iter()
                .filter_map(|key| key.strip_prefix(COMPOSER_PREFIX))
                .find(|id| !id.is_empty());
            report.session_native_id = first.map(str::to_string);
            report.session_observation.provider_session_id = MetadataResolution::Ambiguous;
            report.session_observation.multi_session = true;
        }
    }

    let mut walk = WalkState {
        sink,
        seq: 0,
        committed: 0,
    };
    let mut total_headers: u64 = 0;
    for (ordinal, composer_key) in composer_keys.iter().enumerate() {
        let outcome = walk_composer(
            conn,
            composer_key,
            ordinal + 1,
            limits,
            &mut budget,
            &mut walk,
        )?;
        total_headers = total_headers.saturating_add(outcome.total_slots());
        if total_headers > limits.max_total_headers {
            return Err(ProviderError::SourceTooLarge {
                actual: total_headers,
                max: limits.max_total_headers,
            });
        }
        let composer_id = composer_key
            .strip_prefix(COMPOSER_PREFIX)
            .unwrap_or(composer_key);
        report.skipped += outcome.skipped as usize;
        if outcome.skipped > 0 || outcome.emitted == 0 {
            report.diagnostics.push(format!(
                "cursorDiskKV composer `{composer_id}`: {} header slot(s), {} message(s) \
                 emitted, {} slot(s) skipped; states: {}",
                outcome.total_slots(),
                outcome.emitted,
                outcome.skipped,
                outcome.counts.render(),
            ));
        }
        report.diagnostics.extend(outcome.notes);
    }
    report.committed = walk.committed;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// One captured canonical message: seq, session source key, native id,
    /// role and text.
    #[derive(Debug, PartialEq, Eq)]
    struct Event {
        seq: u32,
        session: Option<String>,
        native_id: String,
        role: String,
        text: String,
    }

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<Event>,
    }

    impl CanonicalEventSink for RecordingSink {
        fn emit_message(
            &mut self,
            event: MessageEvent<'_>,
        ) -> agent_session_grep_ports::PortResult<()> {
            self.events.push(Event {
                seq: event.seq,
                session: event.session.map(|identity| identity.source_key.clone()),
                native_id: event.native_id.to_string(),
                role: event.role.to_string(),
                text: event.text.to_string(),
            });
            Ok(())
        }
    }

    /// Serialize an in-memory database to bytes via VACUUM INTO.
    fn vacuum_to_bytes(conn: &Connection) -> Vec<u8> {
        let path = crate::temp_db_path("test");
        conn.execute_batch(&format!("VACUUM INTO '{}'", path.display()))
            .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        bytes
    }

    enum FixtureValue {
        Text(String),
        Blob(Vec<u8>),
        Integer(i64),
        Null,
    }

    /// Build a synthetic `state.vscdb` with a `cursorDiskKV` table; row order
    /// is the insertion order.
    fn cursor_db(rows: Vec<(String, FixtureValue)>) -> Vec<u8> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value BLOB);")
            .unwrap();
        for (key, value) in rows {
            match value {
                FixtureValue::Text(text) => {
                    conn.execute(
                        "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                        params![key, text],
                    )
                    .unwrap();
                }
                FixtureValue::Blob(bytes) => {
                    conn.execute(
                        "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                        params![key, bytes],
                    )
                    .unwrap();
                }
                FixtureValue::Integer(value) => {
                    conn.execute(
                        "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                        params![key, value],
                    )
                    .unwrap();
                }
                FixtureValue::Null => {
                    conn.execute(
                        "INSERT INTO cursorDiskKV (key, value) VALUES (?1, NULL)",
                        params![key],
                    )
                    .unwrap();
                }
            }
        }
        vacuum_to_bytes(&conn)
    }

    /// Build a synthetic database whose `cursorDiskKV` has no unique key, so
    /// duplicate rows can exist.
    fn duplicate_db(rows: Vec<(String, String)>) -> Vec<u8> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE cursorDiskKV (key TEXT, value BLOB);")
            .unwrap();
        for (key, value) in rows {
            conn.execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![key, value],
            )
            .unwrap();
        }
        vacuum_to_bytes(&conn)
    }

    fn composer_key(id: &str) -> String {
        format!("{COMPOSER_PREFIX}{id}")
    }

    fn bubble_key(composer_id: &str, bubble_id: &str) -> String {
        format!("{BUBBLE_PREFIX}{composer_id}:{bubble_id}")
    }

    fn header(bubble_id: &str, role: i64) -> serde_json::Value {
        json!({ "bubbleId": bubble_id, "type": role })
    }

    fn composer_json(headers: &[serde_json::Value]) -> String {
        json!({ "fullConversationHeadersOnly": headers }).to_string()
    }

    fn text_bubble(text: &str) -> String {
        json!({ "text": text }).to_string()
    }

    fn tool_bubble(tool: serde_json::Value) -> String {
        json!({ "toolFormerData": tool }).to_string()
    }

    fn parse_with(
        bytes: &[u8],
        sink: &mut RecordingSink,
        limits: &Limits,
    ) -> Result<ParseReport, ProviderError> {
        let db = crate::open_readonly_from_bytes(bytes).expect("open snapshot");
        parse(&db.conn, bytes.len() as u64, sink, limits)
    }

    fn parse_ok(bytes: &[u8]) -> (ParseReport, RecordingSink) {
        let mut sink = RecordingSink::default();
        let report = parse_with(bytes, &mut sink, &Limits::default()).expect("parse succeeds");
        (report, sink)
    }

    fn probe_with(bytes: &[u8], limits: &Limits) -> Result<ProbeResult, ProviderError> {
        let db = crate::open_readonly_from_bytes(bytes).expect("open snapshot");
        probe(&db.conn, bytes.len() as u64, limits)
    }

    fn diagnostics(report: &ParseReport) -> String {
        report.diagnostics.join("\n")
    }

    #[test]
    fn probe_confirms_the_synthetic_disk_kv_schema() {
        let bytes = cursor_db(vec![
            (
                composer_key("c1"),
                FixtureValue::Text(composer_json(&[header("b1", 1)])),
            ),
            (
                bubble_key("c1", "b1"),
                FixtureValue::Text(text_bubble("synthetic")),
            ),
        ]);
        let result = probe_with(&bytes, &Limits::default()).expect("probe succeeds");
        assert_eq!(result.variant_id, VARIANT_ID);
        assert_eq!(result.confidence, Confidence::Confirmed);
        assert!(
            result
                .matched_evidence
                .iter()
                .any(|line| line.contains("cursorDiskKV(key, value)")),
            "{:?}",
            result.matched_evidence
        );
        assert!(
            result
                .matched_evidence
                .iter()
                .any(|line| line.contains("composerData:")),
            "{:?}",
            result.matched_evidence
        );
    }

    #[test]
    fn claims_rejects_a_cursor_named_table_with_the_wrong_columns() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE cursorDiskKV (k TEXT, v BLOB);")
            .unwrap();
        let error = claims(&conn).unwrap_err();
        assert!(matches!(error, ProviderError::StructuralFatal(_)));

        // No `cursorDiskKV` table at all: not this variant, but not an error.
        let other = Connection::open_in_memory().unwrap();
        other
            .execute_batch("CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT);")
            .unwrap();
        assert!(!claims(&other).unwrap());
    }

    #[test]
    fn probe_rejects_a_table_without_any_composer_key() {
        let bytes = cursor_db(vec![(
            bubble_key("c1", "b1"),
            FixtureValue::Text(text_bubble("synthetic")),
        )]);
        let error = probe_with(&bytes, &Limits::default()).unwrap_err();
        assert!(matches!(error, ProviderError::AmbiguousVariant(_)));
    }

    #[test]
    fn parse_orders_by_header_not_by_key_or_insertion_order() {
        let headers = [header("z:💡", 1), header("a:雪", 2)];
        let build = |reversed: bool| {
            let mut rows = vec![(
                composer_key("c1"),
                FixtureValue::Text(composer_json(&headers)),
            )];
            let mut bodies = vec![
                ("z:💡", "first in header order"),
                ("a:雪", "second in header order"),
            ];
            if reversed {
                bodies.reverse();
            }
            for (id, text) in bodies {
                rows.push((bubble_key("c1", id), FixtureValue::Text(text_bubble(text))));
            }
            cursor_db(rows)
        };

        let (report, sink) = parse_ok(&build(false));
        let (reversed_report, reversed_sink) = parse_ok(&build(true));

        let texts: Vec<&str> = sink.events.iter().map(|e| e.text.as_str()).collect();
        assert_eq!(
            texts,
            ["first in header order", "second in header order"],
            "messages must follow fullConversationHeadersOnly"
        );
        assert_eq!(sink.events[0].role, "user");
        assert_eq!(sink.events[1].role, "assistant");
        assert_eq!(sink.events[0].seq, 0);
        assert_eq!(sink.events[1].seq, 1);
        assert_eq!(
            report, reversed_report,
            "a shuffled KV insertion order must not change the report"
        );
        assert_eq!(
            sink.events, reversed_sink.events,
            "a shuffled KV insertion order must not change the output"
        );
    }

    #[test]
    fn parse_keeps_a_slot_and_state_for_every_bad_bubble() {
        let headers = [
            header("ok", 1),
            header("bad-json", 2),
            header("missing", 2),
            header("null", 2),
            header("array", 2),
            header("utf8", 2),
        ];
        let bytes = cursor_db(vec![
            (
                composer_key("c1"),
                FixtureValue::Text(composer_json(&headers)),
            ),
            (
                bubble_key("c1", "ok"),
                FixtureValue::Text(text_bubble("synthetic ok")),
            ),
            (
                bubble_key("c1", "bad-json"),
                FixtureValue::Text("{".to_string()),
            ),
            (bubble_key("c1", "null"), FixtureValue::Null),
            (
                bubble_key("c1", "array"),
                FixtureValue::Text("[]".to_string()),
            ),
            (bubble_key("c1", "utf8"), FixtureValue::Blob(vec![0xff])),
        ]);

        let (report, sink) = parse_ok(&bytes);
        assert_eq!(report.committed, 1);
        assert_eq!(report.skipped, 5);
        assert_eq!(sink.events.len(), 1);
        assert_eq!(sink.events[0].text, "synthetic ok");

        let notes = diagnostics(&report);
        for state in [
            "malformed_json",
            "missing_row",
            "null_value",
            "invalid_bubble_shape",
            "invalid_utf8",
        ] {
            assert!(notes.contains(state), "missing state {state} in {notes}");
        }
        assert!(notes.contains("header slot 1"), "{notes}");
        assert!(notes.contains("header slot 5"), "{notes}");
        assert!(notes.contains("`bubbleId:c1:bad-json`"), "{notes}");
        assert!(
            notes.contains(
                "states: ok=1 missing_row=1 null_value=1 invalid_utf8=1 \
                            malformed_json=1 invalid_bubble_shape=1"
            ),
            "{notes}"
        );
    }

    #[test]
    fn parse_selects_tool_input_raw_args_then_params_without_executing() {
        let headers = [
            header("raw", 2),
            header("params", 2),
            header("plain", 2),
            header("blank", 2),
            header("null-text", 2),
        ];
        let bytes = cursor_db(vec![
            (
                composer_key("c1"),
                FixtureValue::Text(composer_json(&headers)),
            ),
            (
                bubble_key("c1", "raw"),
                FixtureValue::Text(tool_bubble(json!({
                    "name": "synthetic_tool",
                    "toolCallId": "call:雪/#%",
                    "rawArgs": "{\"path\":\"synthetic/雪\"}",
                    "params": { "ignored": true }
                }))),
            ),
            (
                bubble_key("c1", "params"),
                FixtureValue::Text(tool_bubble(json!({
                    "name": "synthetic_tool",
                    "params": { "path": "synthetic/params" }
                }))),
            ),
            (
                bubble_key("c1", "plain"),
                FixtureValue::Text(tool_bubble(json!({
                    "name": "synthetic_tool",
                    "rawArgs": "not a command: synthetic"
                }))),
            ),
            (
                bubble_key("c1", "blank"),
                FixtureValue::Text(tool_bubble(json!({
                    "name": "synthetic_tool",
                    "rawArgs": "  ",
                    "params": "{\"n\":2}"
                }))),
            ),
            (
                bubble_key("c1", "null-text"),
                FixtureValue::Text(tool_bubble(json!({
                    "name": "synthetic_tool",
                    "rawArgs": "null",
                    "params": { "must_not_replace_null": true }
                }))),
            ),
        ]);

        let (report, sink) = parse_ok(&bytes);
        assert_eq!(report.committed, 5);
        assert_eq!(report.skipped, 0);
        let texts: Vec<&str> = sink.events.iter().map(|e| e.text.as_str()).collect();
        assert_eq!(
            texts,
            [
                // rawArgs JSON string: kept verbatim, never executed.
                "{\"path\":\"synthetic/雪\"}",
                // params object: rendered as compact JSON.
                "{\"path\":\"synthetic/params\"}",
                // rawArgs plain text: kept as text.
                "not a command: synthetic",
                // blank rawArgs falls back to params.
                "{\"n\":2}",
                // A rawArgs that reads `null` is still a selection: it must not
                // be replaced by params.
                "null",
            ]
        );
        assert!(sink.events.iter().all(|event| event.role == "assistant"));
    }

    #[test]
    fn parse_reports_text_type_tool_shape_and_composer_row_states() {
        // Every remaining slot state in one compact database: a non-text
        // SQLite cell, both malformed tool envelopes, a non-string `text`
        // field, the three composer-row defects, and the empty-composer-id
        // fallback. Defects must keep their slot (and their session), never
        // discard a whole conversation.
        let bytes = cursor_db(vec![
            (
                composer_key(""),
                FixtureValue::Text(composer_json(&[header("anon", 1)])),
            ),
            (
                bubble_key("", "anon"),
                FixtureValue::Text(text_bubble("synthetic anonymous composer")),
            ),
            (
                composer_key("slots"),
                FixtureValue::Text(composer_json(&[
                    header("int-cell", 1),
                    header("tool-not-object", 2),
                    header("tool-bad-name", 2),
                    header("bad-text", 2),
                    header("ok", 1),
                ])),
            ),
            (bubble_key("slots", "int-cell"), FixtureValue::Integer(7)),
            (
                bubble_key("slots", "tool-not-object"),
                FixtureValue::Text(json!({ "toolFormerData": 42 }).to_string()),
            ),
            (
                bubble_key("slots", "tool-bad-name"),
                FixtureValue::Text(tool_bubble(json!({ "name": 7 }))),
            ),
            (
                bubble_key("slots", "bad-text"),
                FixtureValue::Text(json!({ "text": 42 }).to_string()),
            ),
            (
                bubble_key("slots", "ok"),
                FixtureValue::Text(text_bubble("synthetic surviving slot")),
            ),
            (composer_key("shape"), FixtureValue::Text("[]".to_string())),
            (
                composer_key("no-headers"),
                FixtureValue::Text("{}".to_string()),
            ),
            (
                composer_key("bad-headers"),
                FixtureValue::Text(json!({ "fullConversationHeadersOnly": {} }).to_string()),
            ),
        ]);

        let (report, sink) = parse_ok(&bytes);

        // Two conversations survive: the anonymous composer and the intact
        // slot of the composer whose four sibling slots were defective.
        assert_eq!(report.committed, 2);
        assert_eq!(sink.events.len(), 2);
        assert_eq!(report.skipped, 7);
        let sessions: Vec<Option<&str>> = sink
            .events
            .iter()
            .map(|event| event.session.as_deref())
            .collect();
        assert_eq!(
            sessions,
            [Some("anonymous-composer-1"), Some("slots")],
            "a defective slot or composer row must not discard its session"
        );
        assert_eq!(
            sink.events
                .iter()
                .map(|event| event.text.as_str())
                .collect::<Vec<_>>(),
            ["synthetic anonymous composer", "synthetic surviving slot"]
        );

        let notes = diagnostics(&report);
        // Per-slot states of composer `slots`: one ok, two non-text cells and
        // two malformed tool envelopes; every defective slot keeps its ordinal.
        assert!(
            notes.contains(
                "cursorDiskKV composer `slots`: 5 header slot(s), 1 message(s) emitted, \
                 4 slot(s) skipped; states: ok=1 invalid_text_type=2 invalid_tool_shape=2"
            ),
            "{notes}"
        );
        for slot in 0..4 {
            assert!(
                notes.contains(&format!("composer `slots`: header slot {slot} ")),
                "missing defect line for header slot {slot}: {notes}"
            );
        }
        assert!(
            notes.contains("header slot 1 (`bubbleId:slots:tool-not-object`): invalid_tool_shape")
                && notes
                    .contains("header slot 2 (`bubbleId:slots:tool-bad-name`): invalid_tool_shape"),
            "{notes}"
        );
        // Composer-row defects: each is counted, named, and skips only itself.
        assert!(
            notes.contains(
                "cursorDiskKV composer `shape`: composer row invalid_composer_shape; composer \
                 skipped"
            ),
            "{notes}"
        );
        assert!(
            notes.contains(
                "cursorDiskKV composer `no-headers`: `fullConversationHeadersOnly` is missing; \
                 composer skipped"
            ),
            "{notes}"
        );
        assert!(
            notes.contains(
                "cursorDiskKV composer `bad-headers`: `fullConversationHeadersOnly` is not an \
                 array; composer skipped"
            ),
            "{notes}"
        );
        for state in [
            "invalid_composer_shape=1",
            "missing_headers=1",
            "invalid_headers=1",
        ] {
            assert!(notes.contains(state), "missing state {state} in {notes}");
        }
        // The empty-id composer is an observation-only fallback, named verbatim.
        assert!(
            notes.contains("key `composerData:` has an empty suffix")
                && notes.contains("source-local key `anonymous-composer-1`"),
            "{notes}"
        );
    }

    #[test]
    fn parse_reports_unknown_role_and_invalid_header_slots() {
        let headers = [json!({ "type": 1 }), header("x", 77)];
        let bytes = cursor_db(vec![
            (
                composer_key("c1"),
                FixtureValue::Text(composer_json(&headers)),
            ),
            (
                bubble_key("c1", "x"),
                FixtureValue::Text(text_bubble("role must not be guessed")),
            ),
        ]);

        let (report, sink) = parse_ok(&bytes);
        assert_eq!(report.committed, 0);
        assert!(sink.events.is_empty());
        assert_eq!(report.skipped, 2);
        let notes = diagnostics(&report);
        assert!(notes.contains("invalid_header"), "{notes}");
        assert!(notes.contains("unknown_role"), "{notes}");
        assert!(notes.contains("states: ok=0"), "{notes}");
        assert!(notes.contains("header slot 0"), "{notes}");
        assert!(notes.contains("header slot 1"), "{notes}");
    }

    #[test]
    fn parse_keeps_duplicate_bubble_ids_as_separate_ordinals() {
        let headers = [header("same", 1), header("same", 2)];
        let bytes = cursor_db(vec![
            (
                composer_key("c1"),
                FixtureValue::Text(composer_json(&headers)),
            ),
            (
                bubble_key("c1", "same"),
                FixtureValue::Text(text_bubble("synthetic repeated")),
            ),
        ]);

        let (report, sink) = parse_ok(&bytes);
        assert_eq!(report.committed, 2);
        assert_eq!(report.skipped, 0);
        assert_eq!(sink.events.len(), 2);
        assert_eq!(sink.events[0].text, sink.events[1].text);
        assert_eq!(sink.events[0].seq, 0);
        assert_eq!(sink.events[1].seq, 1);
        assert_eq!(sink.events[0].role, "user");
        assert_eq!(sink.events[1].role, "assistant");
    }

    #[test]
    fn parse_reports_multi_composer_sessions_with_per_message_identity() {
        // Inserted out of key order: the walk must still be byte-wise key order.
        let bytes = cursor_db(vec![
            (
                composer_key("c2"),
                FixtureValue::Text(composer_json(&[header("b", 2)])),
            ),
            (
                bubble_key("c2", "b"),
                FixtureValue::Text(text_bubble("second composer")),
            ),
            (
                composer_key("c1"),
                FixtureValue::Text(composer_json(&[header("a", 1)])),
            ),
            (
                bubble_key("c1", "a"),
                FixtureValue::Text(text_bubble("first composer")),
            ),
        ]);

        let (report, sink) = parse_ok(&bytes);
        assert_eq!(report.committed, 2);
        assert_eq!(report.session_native_id.as_deref(), Some("c1"));
        assert!(report.session_observation.multi_session);
        assert_eq!(
            report.session_observation.provider_session_id,
            MetadataResolution::Ambiguous
        );
        let sessions: Vec<Option<&str>> = sink
            .events
            .iter()
            .map(|event| event.session.as_deref())
            .collect();
        assert_eq!(sessions, [Some("c1"), Some("c2")]);
        assert_eq!(
            sink.events
                .iter()
                .map(|event| event.text.as_str())
                .collect::<Vec<_>>(),
            ["first composer", "second composer"]
        );
        // No native message id is adopted: the ids stay observations.
        assert!(sink.events.iter().all(|event| event.native_id.is_empty()));
    }

    #[test]
    fn parse_treats_two_native_tuples_that_spell_one_key_as_the_storage_cell_it_is() {
        // `composerData:a:b` + bubble `c` and `composerData:a` + bubble `b:c`
        // both spell `bubbleId:a:b:c`. The storage format cannot tell them
        // apart, and this adapter must not pretend it can (no delimiter
        // splitting, no normalization).
        let bytes = cursor_db(vec![
            (
                composer_key("a:b"),
                FixtureValue::Text(composer_json(&[header("c", 1)])),
            ),
            (
                composer_key("a"),
                FixtureValue::Text(composer_json(&[header("b:c", 1)])),
            ),
            (
                bubble_key("a:b", "c"),
                FixtureValue::Text(text_bubble("shared storage cell")),
            ),
        ]);

        let (report, sink) = parse_ok(&bytes);
        assert_eq!(report.committed, 2);
        assert_eq!(report.skipped, 0);
        assert_eq!(
            sink.events
                .iter()
                .map(|event| event.text.as_str())
                .collect::<Vec<_>>(),
            ["shared storage cell", "shared storage cell"]
        );
    }

    #[test]
    fn parse_reports_duplicate_rows_explicitly() {
        let composer = composer_json(&[header("b1", 1)]);
        let bytes = duplicate_db(vec![
            (composer_key("c1"), composer.clone()),
            (composer_key("c1"), composer.clone()),
        ]);
        let (report, sink) = parse_ok(&bytes);
        assert_eq!(sink.events.len(), 0);
        assert_eq!(report.skipped, 1);
        assert!(diagnostics(&report).contains("duplicate_row"), "{report:?}");

        let body = text_bubble("synthetic duplicate row");
        let bytes = duplicate_db(vec![
            (composer_key("c1"), composer),
            (bubble_key("c1", "b1"), body.clone()),
            (bubble_key("c1", "b1"), body),
        ]);
        let (report, sink) = parse_ok(&bytes);
        assert_eq!(sink.events.len(), 0);
        assert_eq!(report.skipped, 1);
        let notes = diagnostics(&report);
        assert!(notes.contains("duplicate_row"), "{notes}");
        assert!(notes.contains("header slot 0"), "{notes}");
    }

    #[test]
    fn parse_fails_explicitly_when_a_bound_is_exceeded() {
        let one_slot = composer_json(&[header("b1", 1)]);
        let two_slots = composer_json(&[header("b1", 1), header("b2", 2)]);
        let two_composers = cursor_db(vec![
            (
                composer_key("c1"),
                FixtureValue::Text(composer_json(&[header("b1", 1)])),
            ),
            (
                bubble_key("c1", "b1"),
                FixtureValue::Text(text_bubble("synthetic a")),
            ),
            (
                composer_key("c2"),
                FixtureValue::Text(composer_json(&[header("b2", 2)])),
            ),
            (
                bubble_key("c2", "b2"),
                FixtureValue::Text(text_bubble("synthetic b")),
            ),
        ]);
        let mut sink = RecordingSink::default();
        assert!(matches!(
            parse_with(
                &two_composers,
                &mut sink,
                &Limits {
                    max_composers: 1,
                    ..Limits::default()
                }
            ),
            Err(ProviderError::SourceTooLarge { actual: 2, max: 1 })
        ));

        let mut sink = RecordingSink::default();
        assert!(matches!(
            parse_with(
                &two_composers,
                &mut sink,
                &Limits {
                    max_total_headers: 1,
                    ..Limits::default()
                }
            ),
            Err(ProviderError::SourceTooLarge { actual: 2, max: 1 })
        ));

        let two_slots = cursor_db(vec![
            (composer_key("c1"), FixtureValue::Text(two_slots)),
            (
                bubble_key("c1", "b1"),
                FixtureValue::Text(text_bubble("synthetic a")),
            ),
            (
                bubble_key("c1", "b2"),
                FixtureValue::Text(text_bubble("synthetic b")),
            ),
        ]);
        let mut sink = RecordingSink::default();
        assert!(matches!(
            parse_with(
                &two_slots,
                &mut sink,
                &Limits {
                    max_headers_per_composer: 1,
                    ..Limits::default()
                }
            ),
            Err(ProviderError::RecordTooLarge { actual: 2, max: 1 })
        ));

        let one_slot = cursor_db(vec![(composer_key("c1"), FixtureValue::Text(one_slot))]);
        let mut sink = RecordingSink::default();
        assert!(matches!(
            parse_with(
                &one_slot,
                &mut sink,
                &Limits {
                    max_cell_bytes: 4,
                    ..Limits::default()
                }
            ),
            Err(ProviderError::RecordTooLarge { max: 4, .. })
        ));

        let mut sink = RecordingSink::default();
        assert!(matches!(
            parse_with(
                &one_slot,
                &mut sink,
                &Limits {
                    max_total_cell_bytes: 4,
                    ..Limits::default()
                }
            ),
            Err(ProviderError::SourceTooLarge { max: 4, .. })
        ));

        // The whole-source and probe caps are checked before any row is read.
        let db = crate::open_readonly_from_bytes(&one_slot).expect("open snapshot");
        let mut sink = RecordingSink::default();
        assert!(matches!(
            parse(
                &db.conn,
                64,
                &mut sink,
                &Limits {
                    max_source_bytes: 16,
                    ..Limits::default()
                }
            ),
            Err(ProviderError::SourceTooLarge {
                actual: 64,
                max: 16
            })
        ));
        assert!(matches!(
            probe(
                &db.conn,
                64,
                &Limits {
                    max_source_bytes: 16,
                    ..Limits::default()
                }
            ),
            Err(ProviderError::SourceTooLarge {
                actual: 64,
                max: 16
            })
        ));
    }

    #[test]
    fn private_snapshot_copy_is_removed_after_use() {
        let bytes = cursor_db(vec![(
            composer_key("c1"),
            FixtureValue::Text(composer_json(&[])),
        )]);
        let leaked_path = {
            let db = crate::open_readonly_from_bytes(&bytes).unwrap();
            let path = db.temp_path().to_path_buf();
            assert!(path.exists(), "temp copy must exist while the db is open");
            path
        };
        assert!(
            !leaked_path.exists(),
            "temp copy must be unlinked after the connection is dropped"
        );
    }

    /// Best-effort cleanup for the WAL test's owned files.
    struct ScratchFiles {
        paths: Vec<std::path::PathBuf>,
    }

    impl Drop for ScratchFiles {
        fn drop(&mut self) {
            for path in &self.paths {
                for suffix in ["", "-wal", "-shm", "-journal"] {
                    let mut owned = path.as_os_str().to_os_string();
                    owned.push(suffix);
                    let _ = std::fs::remove_file(std::path::Path::new(&owned));
                }
            }
        }
    }

    /// `(len, mtime, BLAKE3)` per source file; `None` once it does not exist.
    fn source_state(
        main: &std::path::Path,
        wal: &std::path::Path,
        shm: &std::path::Path,
    ) -> Vec<Option<(u64, std::time::SystemTime, String)>> {
        [main, wal, shm]
            .into_iter()
            .map(|path| {
                let meta = std::fs::metadata(path).ok()?;
                let bytes = std::fs::read(path).ok()?;
                Some((
                    meta.len(),
                    meta.modified().expect("mtime"),
                    blake3::hash(&bytes).to_hex().to_string(),
                ))
            })
            .collect()
    }

    #[test]
    fn source_database_files_stay_byte_identical_across_a_concurrent_wal_commit() {
        let source_path = crate::temp_db_path("wal-source");
        let snapshot_path = crate::temp_db_path("wal-snapshot");
        let _scratch = ScratchFiles {
            paths: vec![source_path.clone(), snapshot_path.clone()],
        };

        let writer = Connection::open(&source_path).expect("writer");
        writer
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA wal_autocheckpoint = 0;
                 CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value BLOB);",
            )
            .expect("wal schema");
        writer
            .execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![composer_key("c1"), composer_json(&[header("old", 1)])],
            )
            .expect("composer row");
        writer
            .execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![bubble_key("c1", "old"), text_bubble("before commit")],
            )
            .expect("bubble row");

        let wal_path = {
            let mut path = source_path.as_os_str().to_os_string();
            path.push("-wal");
            std::path::PathBuf::from(path)
        };
        let shm_path = {
            let mut path = source_path.as_os_str().to_os_string();
            path.push("-shm");
            std::path::PathBuf::from(path)
        };
        assert!(
            wal_path.exists() && shm_path.exists(),
            "the synthetic writer must keep a live WAL and SHM"
        );

        // One logical snapshot, exactly like production capture: SQLite writes
        // the currently committed database (WAL frames included) to a single
        // new file, so no racy main/WAL/SHM copy is involved.
        writer
            .execute_batch(&format!("VACUUM INTO '{}'", snapshot_path.display()))
            .expect("logical snapshot");
        let snapshot_bytes = std::fs::read(&snapshot_path).expect("snapshot bytes");

        let before = source_state(&source_path, &wal_path, &shm_path);
        let (report, sink) = parse_ok(&snapshot_bytes);
        assert_eq!(report.committed, 1);
        assert_eq!(sink.events[0].text, "before commit");
        let after = source_state(&source_path, &wal_path, &shm_path);
        assert_eq!(before, after, "probe/parse must not touch the source files");

        // A concurrent commit changes the source, but the snapshot taken before
        // it keeps observing exactly the same database state.
        writer
            .execute(
                "UPDATE cursorDiskKV SET value = ?2 WHERE key = ?1",
                params![
                    composer_key("c1"),
                    composer_json(&[header("old", 1), header("new", 2)])
                ],
            )
            .expect("update composer row");
        writer
            .execute(
                "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                params![bubble_key("c1", "new"), text_bubble("after commit")],
            )
            .expect("second bubble row");

        let (again_report, again_sink) = parse_ok(&snapshot_bytes);
        assert_eq!(again_report.committed, report.committed);
        assert_eq!(again_sink.events.len(), 1);
        assert_eq!(again_sink.events[0].text, "before commit");

        // A fresh snapshot does see the new row: the writer's commit is real,
        // only this parse's observation is pinned.
        std::fs::remove_file(&snapshot_path).expect("drop old snapshot");
        writer
            .execute_batch(&format!("VACUUM INTO '{}'", snapshot_path.display()))
            .expect("second logical snapshot");
        let fresh = std::fs::read(&snapshot_path).expect("fresh snapshot bytes");
        let (fresh_report, _) = parse_ok(&fresh);
        assert_eq!(fresh_report.committed, 2);
    }
}

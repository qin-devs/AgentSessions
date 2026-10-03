//! Cursor provider adapter.
//!
//! Two mutually exclusive variants:
//!
//! * `cursor/vscdb-chat-v1` (legacy): `state.vscdb` ItemTable KV keys
//!   `workbench.panel.aichat.view.aichat.chatdata` (tabs -> bubbles) and
//!   `aiService.prompts` (flat prompt/response history).
//! * `cursor/disk-kv-v1` (see [`disk_kv`]): the newer `cursorDiskKV` table
//!   (`composerData:<id>` metadata + `bubbleId:<composerId>:<bubbleId>` bodies).
//!
//! Variant dispatch is decided by the bytes alone: a SQLite stream can claim
//! the ItemTable surface, the disk-kv surface, or neither, and a stream that
//! satisfies both is refused as ambiguous instead of guessed. Everything the
//! ItemTable variant accepted before retains its text, identity and order;
//! its numeric message timestamps are normalized from the proven millisecond
//! unit. The disk-kv path is additive.
//!
//! Both variants receive the SQLite file as a byte stream, write it to a
//! private temporary file, and open that copy read-only
//! (SQLITE_OPEN_READONLY + busy_timeout + `query_only`, one pinned read
//! transaction); the source database is never opened.
//!
//! Format evidence: hstry (MIT) `adapters/cursor/adapter.ts`:
//! - key `workbench.panel.aichat.view.aichat.chatdata` → JSON document with
//!   `tabs`, each tab holding `bubbles` (type/text/rawText/timingInfo.startTime)
//! - key `aiService.prompts` → JSON array of `{prompt, response, createdAt,
//!   conversationId}` records, grouped into conversations by conversationId
//! - the `cursorDiskKV` surface: pinned Wake implementation plus the frozen
//!   synthetic probe suite, cited in `tests/golden/PROVENANCE.md`

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use agent_session_grep_ports::{
    AdapterManifest, CanonicalEventSink, Confidence, MessageEvent, MetadataResolution, ParseReport,
    ProbeResult, ProviderAdapter, ProviderError, manifest_for,
};
use rusqlite::{Connection, OpenFlags, params};

mod disk_kv;

/// Variant id surfaced in probe results.
const VARIANT_ID: &str = "cursor/vscdb-chat-v1";

/// SQLite magic header: every SQLite database starts with "SQLite format 3\0".
const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";

/// ItemTable key holding the chat tabs JSON document.
const CHAT_DATA_KEY: &str = "workbench.panel.aichat.view.aichat.chatdata";

/// ItemTable key holding the flat prompts history JSON array.
const PROMPTS_KEY: &str = "aiService.prompts";

/// Cursor adapter: parses `state.vscdb` (SQLite ItemTable KV store).
///
/// The adapter writes the byte stream to a temp file and opens it read-only,
/// because rusqlite requires a file path (no in-memory deserialize in 0.40).
/// The temp file is cleaned up by the OS.
pub struct CursorAdapter;

impl CursorAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CursorAdapter {
    fn default() -> Self {
        Self::new()
    }
}

/// `workbench.panel.aichat.view.aichat.chatdata` payload.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChatData {
    #[serde(default)]
    tabs: Vec<Tab>,
}

/// One chat tab: a conversation with its message bubbles.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Tab {
    id: Option<String>,
    created_at: Option<i64>,
    #[serde(default)]
    bubbles: Vec<Bubble>,
}

/// One chat bubble: a single user/assistant message.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Bubble {
    r#type: Option<String>,
    text: Option<String>,
    raw_text: Option<String>,
    timing_info: Option<TimingInfo>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimingInfo {
    start_time: Option<i64>,
}

/// One `aiService.prompts` history record: a prompt/response exchange.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Prompt {
    prompt: Option<String>,
    response: Option<String>,
    created_at: Option<i64>,
    conversation_id: Option<String>,
}

impl ProviderAdapter for CursorAdapter {
    fn provider_id(&self) -> &str {
        "cursor"
    }

    fn manifest(&self) -> AdapterManifest {
        manifest_for(
            self.provider_id(),
            Some(1),
            &[
                "SQLite source has no byte spans",
                "chatdata/prompts are multi-generation formats; version layering is not yet implemented",
                "native message ids are not preserved (ids are derived, not native)",
                "the additive `cursor/disk-kv-v1` variant reads cursorDiskKV composerData/bubbleId rows through a private read-only snapshot (one pinned read transaction); the capability row above still advertises only the ItemTable variant",
                "disk-kv messages report no adopted native id: a bubbleId is a composer-scoped storage-key component with no official version contract, so canonical message identity stays document-scoped (the same decision as hermes rowids); verbatim composer/bubble ids appear only as observations in diagnostics and the session identity",
                "disk-kv bubbles with no usable text or tool input, and composers whose own row is malformed, are counted in `skipped` with per-state diagnostics; a broken bubble never discards its session and a broken composer row never discards the source's other composers",
                "disk-kv rows/headers/cells are bounded (4_096 composers, 100_000 headers per composer, 250_000 headers per database, 8 MiB per cell, 64 MiB per database); exceeding a bound fails the source instead of truncating it",
            ],
        )
    }

    fn probe(&self, bytes: &[u8]) -> Result<ProbeResult, ProviderError> {
        let mut matched = Vec::new();
        let unmatched = Vec::new();

        // Quick check: SQLite files start with a magic header.
        if bytes.len() < SQLITE_MAGIC.len() || &bytes[..SQLITE_MAGIC.len()] != SQLITE_MAGIC {
            return Err(ProviderError::AmbiguousVariant(
                "not a SQLite database (missing magic header)".into(),
            ));
        }
        matched.push("SQLite magic header detected".into());

        // Open one read-only snapshot and decide which Cursor surface the
        // bytes carry. `db` unlinks the temp copy when it goes out of scope
        // (see `TempDb`).
        let db = open_readonly_from_bytes(bytes)
            .map_err(|e| ProviderError::StructuralFatal(format!("failed to open SQLite: {e}")))?;
        let conn = &db.conn;
        let item_table = item_table_claim(conn);
        let disk_kv = disk_kv::claims(conn)?;
        if item_table && disk_kv {
            return Err(ProviderError::AmbiguousVariant(
                "bytes match both `cursor/disk-kv-v1` and `cursor/vscdb-chat-v1`".into(),
            ));
        }
        if disk_kv {
            // Re-validates the schema and checks the adapter's own source cap.
            return disk_kv::probe(conn, bytes.len() as u64, &disk_kv::Limits::default());
        }
        if !item_table {
            return Err(ProviderError::AmbiguousVariant(
                "no Cursor state surface found (no ItemTable chatdata/prompts key and no \
                 `cursorDiskKV` composerData: key)"
                    .into(),
            ));
        }
        matched.push("ItemTable found".into());

        // Cursor chat history lives under either of two ItemTable keys.
        let has_chat_data = key_exists(conn, CHAT_DATA_KEY);
        let has_prompts = key_exists(conn, PROMPTS_KEY);
        if has_chat_data {
            matched.push("Cursor chatdata key found".into());
        }
        if has_prompts {
            matched.push("Cursor prompts key found".into());
        }

        Ok(ProbeResult {
            variant_id: VARIANT_ID.to_string(),
            confidence: Confidence::Confirmed,
            matched_evidence: matched,
            unmatched_evidence: unmatched,
        })
    }

    fn parse(
        &self,
        bytes: &[u8],
        sink: &mut dyn CanonicalEventSink,
    ) -> Result<ParseReport, ProviderError> {
        // `db` unlinks the temp copy when it goes out of scope (see `TempDb`).
        let db = open_readonly_from_bytes(bytes)
            .map_err(|e| ProviderError::StructuralFatal(format!("failed to open SQLite: {e}")))?;
        let conn = &db.conn;

        // The same surface decision the probe makes, so a byte stream can only
        // ever be parsed as the variant its probe selected.
        let item_table = item_table_claim(conn);
        let disk_kv = disk_kv::claims(conn)?;
        if item_table && disk_kv {
            return Err(ProviderError::AmbiguousVariant(
                "bytes match both `cursor/disk-kv-v1` and `cursor/vscdb-chat-v1`".into(),
            ));
        }
        if disk_kv {
            return disk_kv::parse(conn, bytes.len() as u64, sink, &disk_kv::Limits::default());
        }
        parse_item_table(conn, sink)
    }
}

/// The `cursor/vscdb-chat-v1` surface: the ItemTable plus at least one of the
/// two Cursor chat keys. Table/column problems are not an error here - they
/// simply cannot claim this surface.
fn item_table_claim(conn: &Connection) -> bool {
    table_exists(conn, "ItemTable")
        && (key_exists(conn, CHAT_DATA_KEY) || key_exists(conn, PROMPTS_KEY))
}

/// Parse the ItemTable surface (`cursor/vscdb-chat-v1`).
fn parse_item_table(
    conn: &Connection,
    sink: &mut dyn CanonicalEventSink,
) -> Result<ParseReport, ProviderError> {
    let mut report = ParseReport::default();
    let mut seq: u32 = 0;
    let mut session_count = 0usize;

    // Chat tabs first, then the flat prompts history (hstry order).
    match read_key(conn, CHAT_DATA_KEY) {
        Ok(Some(value)) if !value.trim().is_empty() => {
            parse_chat_data(&value, sink, &mut report, &mut seq, &mut session_count)?;
        }
        Ok(_) => {}
        Err(e) => {
            report.skipped += 1;
            report
                .diagnostics
                .push(format!("failed to read chatdata value: {e}"));
        }
    }
    match read_key(conn, PROMPTS_KEY) {
        Ok(Some(value)) if !value.trim().is_empty() => {
            parse_prompts(&value, sink, &mut report, &mut seq, &mut session_count)?;
        }
        Ok(_) => {}
        Err(e) => {
            report.skipped += 1;
            report
                .diagnostics
                .push(format!("failed to read prompts value: {e}"));
        }
    }

    // Fail closed for multi-session sources (ADR-0009): no single native id
    // may be claimed authoritative when the file holds several sessions.
    if session_count > 1 {
        report.session_observation.multi_session = true;
        report.session_observation.provider_session_id = MetadataResolution::Ambiguous;
    }

    Ok(report)
}

/// Parse the chatdata JSON document: each tab is one session, each bubble one
/// message ordered by `timingInfo.startTime` (tabs by `createdAt`).
fn parse_chat_data(
    value: &str,
    sink: &mut dyn CanonicalEventSink,
    report: &mut ParseReport,
    seq: &mut u32,
    session_count: &mut usize,
) -> Result<(), ProviderError> {
    let data: ChatData = match serde_json::from_str(value) {
        Ok(data) => data,
        Err(e) => {
            report.skipped += 1;
            report
                .diagnostics
                .push(format!("chatdata value is not valid JSON, skipped: {e}"));
            return Ok(());
        }
    };

    let mut tabs = data.tabs;
    // Sessions in chronological order of tab creation.
    tabs.sort_by_key(|tab| tab.created_at.unwrap_or(i64::MAX));

    for tab in tabs {
        // Keep bubbles with a type and non-empty text (text ?? rawText).
        let mut bubbles: Vec<(String, String, Option<i64>)> = Vec::new();
        for bubble in tab.bubbles {
            let Some(r#type) = bubble.r#type else {
                continue;
            };
            let text = bubble.text.or(bubble.raw_text).unwrap_or_default();
            if r#type.is_empty() || text.trim().is_empty() {
                continue;
            }
            let start = bubble.timing_info.as_ref().and_then(|t| t.start_time);
            let role = if r#type == "user" {
                "user"
            } else {
                "assistant"
            };
            bubbles.push((text, role.to_string(), start));
        }
        if bubbles.is_empty() {
            continue;
        }

        let session = register_session(report, session_count, tab.id.as_deref());
        // Chronological order within the tab by bubble start time.
        bubbles.sort_by_key(|(_, _, start)| start.unwrap_or(i64::MAX));
        for (text, role, start) in bubbles {
            emit_message(sink, report, seq, &role, &text, start, &session)?;
        }
    }
    Ok(())
}

/// Parse the prompts JSON array: prompts grouped by conversationId, each
/// group one session, each prompt/response pair a user + assistant message.
fn parse_prompts(
    value: &str,
    sink: &mut dyn CanonicalEventSink,
    report: &mut ParseReport,
    seq: &mut u32,
    session_count: &mut usize,
) -> Result<(), ProviderError> {
    let prompts: Vec<Prompt> = match serde_json::from_str(value) {
        Ok(prompts) => prompts,
        Err(e) => {
            report.skipped += 1;
            report
                .diagnostics
                .push(format!("prompts value is not valid JSON, skipped: {e}"));
            return Ok(());
        }
    };

    // Group by conversationId as-is (real data stores "" when absent).
    let mut grouped: std::collections::BTreeMap<String, Vec<Prompt>> =
        std::collections::BTreeMap::new();
    for prompt in prompts {
        grouped
            .entry(prompt.conversation_id.clone().unwrap_or_default())
            .or_default()
            .push(prompt);
    }

    // Conversations in chronological order of their first prompt.
    let mut groups: Vec<(String, Vec<Prompt>)> = grouped.into_iter().collect();
    groups.sort_by_key(|(_, prompts)| {
        prompts
            .iter()
            .filter_map(|p| p.created_at)
            .min()
            .unwrap_or(i64::MAX)
    });

    for (conversation_id, mut prompts) in groups {
        // Chronological order within the conversation.
        prompts.sort_by_key(|p| p.created_at.unwrap_or(i64::MAX));

        let mut messages: Vec<(String, String, Option<i64>)> = Vec::new();
        for prompt in prompts {
            if let Some(text) = prompt.prompt.filter(|t| !t.trim().is_empty()) {
                messages.push(("user".to_string(), text, prompt.created_at));
            }
            if let Some(text) = prompt.response.filter(|t| !t.trim().is_empty()) {
                messages.push(("assistant".to_string(), text, prompt.created_at));
            }
        }
        if messages.is_empty() {
            continue;
        }

        let id = (!conversation_id.trim().is_empty()).then_some(conversation_id.as_str());
        let session = register_session(report, session_count, id);
        for (role, text, created_at) in messages {
            emit_message(sink, report, seq, &role, &text, created_at, &session)?;
        }
    }
    Ok(())
}

/// Count one more session and, when it is the first with a native id, claim
/// it as the document session id (overridden to ambiguous at the end when
/// the document turns out to hold multiple sessions).
fn register_session(
    report: &mut ParseReport,
    session_count: &mut usize,
    native_id: Option<&str>,
) -> agent_session_grep_ports::ProviderSessionIdentity {
    *session_count += 1;
    let native_id = native_id.filter(|s| !s.trim().is_empty());
    let observation = agent_session_grep_ports::ProviderSessionObservation {
        provider_session_id: native_id
            .map(|id| MetadataResolution::Resolved(id.to_string()))
            .unwrap_or_default(),
        ..Default::default()
    };
    if report.session_native_id.is_none() {
        report.session_native_id = native_id.map(str::to_string);
        report.session_observation = observation.clone();
    }
    agent_session_grep_ports::ProviderSessionIdentity {
        source_key: native_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("anonymous-session-{}", session_count)),
        observation,
    }
}

/// Format a proven ItemTable epoch-millisecond field without floating-point
/// rounding or magnitude-based unit inference. The civil-from-days algorithm
/// is the inverse of the Gregorian day calculation used by the time readers.
/// Use the four-digit ISO year form; the caller diagnoses values outside it.
fn format_epoch_millis_utc(millis: i64) -> Option<String> {
    let seconds = millis.div_euclid(1_000);
    let subsecond = millis.rem_euclid(1_000);
    let day_seconds = seconds.rem_euclid(86_400);
    let days = seconds.div_euclid(86_400) + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let march_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * march_month + 2) / 5 + 1;
    let month = march_month + if march_month < 10 { 3 } else { -9 };
    let year = era * 400 + year_of_era + i64::from(month <= 2);
    if !(0..=9999).contains(&year) {
        return None;
    }
    let (hour, minute, second) = (day_seconds / 3_600, day_seconds / 60 % 60, day_seconds % 60);
    Some(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{subsecond:03}Z"
    ))
}

/// Emit one ItemTable message without inventing a native id. Raw epoch values
/// have already determined its ordinal; formatting must never change that
/// order or omit the message when a timestamp cannot be represented.
fn emit_message(
    sink: &mut dyn CanonicalEventSink,
    report: &mut ParseReport,
    seq: &mut u32,
    role: &str,
    text: &str,
    timestamp: Option<i64>,
    session: &agent_session_grep_ports::ProviderSessionIdentity,
) -> Result<(), ProviderError> {
    let timestamp = timestamp.and_then(|millis| {
        let formatted = format_epoch_millis_utc(millis);
        if formatted.is_none() {
            report
                .diagnostics
                .push("cursor ItemTable: out-of-range epoch-millisecond timestamp omitted".into());
        }
        formatted
    });
    sink.emit_message(MessageEvent {
        session: Some(session),
        seq: *seq,
        native_id: "",
        parent_native_id: None,
        role,
        text,
        timestamp: timestamp.as_deref(),
        is_sidechain: false,
        span: None, // SQLite has no byte spans
    })
    .map_err(|e| ProviderError::StructuralFatal(e.to_string()))?;
    *seq += 1;
    report.committed += 1;
    Ok(())
}

/// Deletes the owned temp SQLite database and sidecars after its connection closes.
/// A final read-only connection may leave WAL/SHM files behind.
struct TempDbGuard {
    path: std::path::PathBuf,
}

impl Drop for TempDbGuard {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut owned_file = self.path.as_os_str().to_os_string();
            owned_file.push(suffix);
            let _ = std::fs::remove_file(std::path::Path::new(&owned_file));
        }
    }
}

/// A read-only connection over a temp copy of the source bytes, bundled with the
/// guard that unlinks that copy.
///
/// **Field order is load-bearing.** Struct fields drop in declaration order, so
/// `conn` closes the database before `_guard` unlinks the file. Windows refuses
/// to delete a file that is still open and `remove_file`'s error is discarded, so
/// the reverse order leaks every temp copy silently. A tuple binding
/// (`let (conn, guard) = ...`) drops the *later* binding first — i.e. the guard
/// while the connection is still open — which is exactly the broken order this
/// struct exists to prevent.
struct TempDb {
    conn: Connection,
    _guard: TempDbGuard,
}

impl TempDb {
    /// Path of the temp copy backing this connection.
    #[cfg(test)]
    fn temp_path(&self) -> &std::path::Path {
        &self._guard.path
    }
}

/// Open a SQLite database from bytes, read-only.
///
/// Writes bytes to a temp file, opens with SQLITE_OPEN_READONLY + busy_timeout,
/// and returns a [`TempDb`] that deletes the temp copy once it goes out of scope.
fn open_readonly_from_bytes(bytes: &[u8]) -> Result<TempDb, String> {
    let temp_path = temp_db_path("parse");
    let guard = TempDbGuard { path: temp_path };
    let mut file = std::fs::File::create(&guard.path).map_err(|e| e.to_string())?;
    file.write_all(bytes).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    drop(file);

    let conn = Connection::open_with_flags(
        &guard.path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| e.to_string())?;
    conn.busy_timeout(std::time::Duration::from_secs(1))
        .map_err(|e| e.to_string())?;
    // Pin one read transaction (`BEGIN` + a first read): every later statement
    // in this connection sees that same snapshot, and `query_only` makes a
    // write attempt fail instead of silently mutating the copy.
    conn.execute_batch(
        "PRAGMA query_only = ON; BEGIN; SELECT rootpage FROM sqlite_schema LIMIT 1;",
    )
    .map_err(|e| e.to_string())?;

    Ok(TempDb {
        conn,
        _guard: guard,
    })
}

/// Unique temp file path for a purpose: process id + atomic counter prevent
/// concurrent parses (and parallel tests) from colliding on the same name.
///
/// The pid is load-bearing: the counter alone restarts at 0 in every process, so
/// two concurrent `asg` runs would both pick `asg-cursor-0-parse.db` and
/// `File::create` would truncate the other's copy mid-parse.
fn temp_db_path(tag: &str) -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "asg-cursor-{}-{}-{tag}.db",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Check if a table exists in the database.
fn table_exists(conn: &Connection, table_name: &str) -> bool {
    conn.prepare(&format!(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    ))
    .and_then(|mut stmt| stmt.exists([]))
    .unwrap_or(false)
}

/// Check whether an ItemTable row with the given key exists.
fn key_exists(conn: &Connection, key: &str) -> bool {
    conn.prepare("SELECT key FROM ItemTable WHERE key = ?1")
        .and_then(|mut stmt| stmt.exists(params![key]))
        .unwrap_or(false)
}

/// Read an ItemTable value by key; `Ok(None)` when the key is absent.
fn read_key(conn: &Connection, key: &str) -> Result<Option<String>, String> {
    let mut stmt = conn
        .prepare("SELECT value FROM ItemTable WHERE key = ?1")
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query(params![key]).map_err(|e| e.to_string())?;
    match rows.next().map_err(|e| e.to_string())? {
        Some(row) => row.get(0).map(Some).map_err(|e| e.to_string()),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_matches_provider_matrix() {
        let adapter = CursorAdapter::new();
        let manifest = adapter.manifest();
        assert_eq!(manifest.provider_id, adapter.provider_id());
        assert_eq!(manifest.supported_variants, vec![VARIANT_ID.to_string()]);
        assert_eq!(manifest.capabilities.provider_id, adapter.provider_id());
        assert_eq!(manifest.capabilities.variant_id, VARIANT_ID);
        assert!(manifest.last_certified_targets.is_empty());
        assert_eq!(manifest.fixture_revision, Some(1));
    }

    fn assert_wal_temp_copy_cleanup(query: &str, should_fail: bool) {
        let fixture = TempDbGuard {
            path: temp_db_path("wal-fixture"),
        };
        let writer = Connection::open(&fixture.path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 CREATE TABLE cleanup_fixture (value INTEGER);
                 INSERT INTO cleanup_fixture VALUES (7);",
            )
            .unwrap();
        drop(writer);
        let bytes = std::fs::read(&fixture.path).unwrap();
        let mut copy_path = None;
        let result = (|| -> rusqlite::Result<i64> {
            let db = open_readonly_from_bytes(&bytes).unwrap();
            let path = db.temp_path().to_path_buf();
            // A real read materializes the private WAL/SHM files.
            db.conn
                .query_row("SELECT value FROM cleanup_fixture", [], |row| {
                    row.get::<_, i64>(0)
                })?;
            for suffix in ["-wal", "-shm"] {
                let mut sidecar = path.as_os_str().to_os_string();
                sidecar.push(suffix);
                assert!(std::path::Path::new(&sidecar).exists());
            }
            copy_path = Some(path);
            // The error case returns while TempDb is still a local owner.
            db.conn.query_row(query, [], |row| row.get(0))
        })();
        assert_eq!(result.is_err(), should_fail);
        let path = copy_path.unwrap();
        let mut remaining = Vec::new();
        for suffix in ["", "-wal", "-shm"] {
            let mut owned_file = path.as_os_str().to_os_string();
            owned_file.push(suffix);
            let owned_file = std::path::Path::new(&owned_file);
            if owned_file.exists() {
                remaining.push(suffix);
                // Do not leave this test's files behind when the assertion fails.
                std::fs::remove_file(owned_file).unwrap();
            }
        }
        assert!(
            remaining.is_empty(),
            "temporary SQLite files leaked: {remaining:?}"
        );
    }

    #[test]
    fn wal_temp_copy_cleans_sidecars_after_success() {
        assert_wal_temp_copy_cleanup("SELECT value FROM cleanup_fixture", false);
    }

    #[test]
    fn wal_temp_copy_cleans_sidecars_after_query_error() {
        assert_wal_temp_copy_cleanup("SELECT missing_column FROM cleanup_fixture", true);
    }

    #[test]
    fn temp_copy_is_unlinked_once_the_connection_goes_out_of_scope() {
        // Regression: this adapter used to return the bare `Connection` with no
        // guard at all, so every probe/parse left its temp copy in the temp dir.
        // `TempDb`'s field order is what makes the unlink run after the database
        // is closed; this test fails if the pairing is ever unbundled again.
        let db_bytes = create_cursor_db(None, Some("[]"));
        let leaked_path = {
            let db = open_readonly_from_bytes(&db_bytes).unwrap();
            let path = db.temp_path().to_path_buf();
            assert!(path.exists(), "temp copy must exist while the db is open");
            path
        };
        assert!(
            !leaked_path.exists(),
            "temp copy must be unlinked after the connection is dropped"
        );
    }

    #[test]
    fn temp_paths_are_scoped_to_this_process() {
        // The name must carry the pid: two concurrent processes both starting at
        // counter 0 would otherwise pick `asg-cursor-0-parse.db` and
        // `File::create` would truncate the other process's database mid-parse.
        let path = temp_db_path("parse");
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        assert!(
            name.contains(&format!("asg-cursor-{}-", std::process::id())),
            "temp file name must be scoped by pid, got `{name}`"
        );
    }

    /// Records (seq, role, text, timestamp) of every emitted message.
    struct RecordingSink {
        events: Vec<(u32, String, String, Option<String>)>,
    }
    impl CanonicalEventSink for RecordingSink {
        fn emit_message(
            &mut self,
            event: MessageEvent<'_>,
        ) -> agent_session_grep_ports::PortResult<()> {
            self.events.push((
                event.seq,
                event.role.to_string(),
                event.text.to_string(),
                event.timestamp.map(str::to_string),
            ));
            Ok(())
        }
    }

    fn sink() -> RecordingSink {
        RecordingSink { events: Vec::new() }
    }

    /// Serialize an in-memory database to bytes via VACUUM INTO.
    fn vacuum_to_bytes(conn: &Connection) -> Vec<u8> {
        let temp_path = temp_db_path("test");
        conn.execute_batch(&format!("VACUUM INTO '{}'", temp_path.display()))
            .unwrap();
        let bytes = std::fs::read(&temp_path).unwrap();
        let _ = std::fs::remove_file(&temp_path);
        bytes
    }

    /// Build a synthetic Cursor `state.vscdb` with only a `cursorDiskKV` table.
    fn create_disk_kv_db() -> Vec<u8> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value BLOB);")
            .unwrap();
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params![
                "composerData:c1",
                r#"{"fullConversationHeadersOnly":[{"bubbleId":"b1","type":1}]}"#
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params!["bubbleId:c1:b1", r#"{"text":"synthetic disk kv"}"#],
        )
        .unwrap();
        vacuum_to_bytes(&conn)
    }

    /// Build a synthetic database that carries both Cursor surfaces.
    fn create_dual_surface_db() -> Vec<u8> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT);
             CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value BLOB);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO ItemTable (key, value) VALUES (?1, ?2)",
            params![CHAT_DATA_KEY, r#"{"tabs":[]}"#],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            params!["composerData:c1", r#"{"fullConversationHeadersOnly":[]}"#],
        )
        .unwrap();
        vacuum_to_bytes(&conn)
    }

    /// Build a synthetic Cursor `state.vscdb` (ItemTable + given key values).
    fn create_cursor_db(chatdata: Option<&str>, prompts: Option<&str>) -> Vec<u8> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT);")
            .unwrap();
        if let Some(value) = chatdata {
            conn.execute(
                "INSERT INTO ItemTable (key, value) VALUES (?1, ?2)",
                params![CHAT_DATA_KEY, value],
            )
            .unwrap();
        }
        if let Some(value) = prompts {
            conn.execute(
                "INSERT INTO ItemTable (key, value) VALUES (?1, ?2)",
                params![PROMPTS_KEY, value],
            )
            .unwrap();
        }
        vacuum_to_bytes(&conn)
    }

    #[test]
    fn probe_rejects_non_sqlite_bytes() {
        let adapter = CursorAdapter::new();
        let err = adapter.probe(b"not a sqlite file").unwrap_err();
        assert!(matches!(err, ProviderError::AmbiguousVariant(_)));
    }

    #[test]
    fn probe_rejects_empty_sqlite_db() {
        let adapter = CursorAdapter::new();
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE unrelated (id INTEGER);")
            .unwrap();
        let db_bytes = vacuum_to_bytes(&conn);
        let err = adapter.probe(&db_bytes).unwrap_err();
        assert!(matches!(err, ProviderError::AmbiguousVariant(_)));
    }

    #[test]
    fn probe_rejects_itemtable_without_cursor_keys() {
        let adapter = CursorAdapter::new();
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT);
             INSERT INTO ItemTable VALUES ('some.other.key', 'x');",
        )
        .unwrap();
        let db_bytes = vacuum_to_bytes(&conn);
        let err = adapter.probe(&db_bytes).unwrap_err();
        assert!(matches!(err, ProviderError::AmbiguousVariant(_)));
    }

    #[test]
    fn probe_confirms_cursor_state_db() {
        let adapter = CursorAdapter::new();
        let db_bytes = create_cursor_db(Some(r#"{"tabs":[{"id":"tab-1","bubbles":[]}]}"#), None);
        let result = adapter.probe(&db_bytes).unwrap();
        assert_eq!(result.variant_id, VARIANT_ID);
        assert_eq!(result.confidence, Confidence::Confirmed);
        assert!(
            result
                .matched_evidence
                .iter()
                .any(|e| e.contains("chatdata"))
        );
    }

    #[test]
    fn probe_confirms_with_prompts_only() {
        let adapter = CursorAdapter::new();
        let db_bytes = create_cursor_db(None, Some(r#"[]"#));
        let result = adapter.probe(&db_bytes).unwrap();
        assert_eq!(result.confidence, Confidence::Confirmed);
        assert!(
            result
                .matched_evidence
                .iter()
                .any(|e| e.contains("prompts"))
        );
    }

    #[test]
    fn probe_confirms_disk_kv_only_database() {
        let adapter = CursorAdapter::new();
        let result = adapter.probe(&create_disk_kv_db()).unwrap();
        assert_eq!(result.variant_id, "cursor/disk-kv-v1");
        assert_eq!(result.confidence, Confidence::Confirmed);
    }

    #[test]
    fn probe_rejects_a_stream_that_matches_both_cursor_variants() {
        let adapter = CursorAdapter::new();
        let err = adapter.probe(&create_dual_surface_db()).unwrap_err();
        assert!(
            matches!(err, ProviderError::AmbiguousVariant(_)),
            "a double claim must be refused, got {err:?}"
        );
    }

    #[test]
    fn parse_dispatches_to_the_disk_kv_surface_and_refuses_a_double_claim() {
        let adapter = CursorAdapter::new();
        let mut sink = sink();
        let report = adapter.parse(&create_disk_kv_db(), &mut sink).unwrap();
        assert_eq!(report.committed, 1);
        assert_eq!(sink.events[0].2, "synthetic disk kv");

        let err = adapter
            .parse(&create_dual_surface_db(), &mut sink)
            .unwrap_err();
        assert!(matches!(err, ProviderError::AmbiguousVariant(_)));
    }

    #[test]
    fn parse_extracts_chatdata_messages() {
        let adapter = CursorAdapter::new();
        let chatdata = r#"{"tabs":[{"id":"tab-1","title":"Test","createdAt":100,"lastUpdatedAt":200,
            "bubbles":[
              {"type":"user","text":"hello","rawText":"ignored","timingInfo":{"startTime":101}},
              {"type":"assistant","text":"hi there","rawText":"alt","timingInfo":{"startTime":102}}
            ]}]}"#;
        let db_bytes = create_cursor_db(Some(chatdata), None);
        let mut sink = sink();
        let report = adapter.parse(&db_bytes, &mut sink).unwrap();

        assert_eq!(report.committed, 2);
        assert_eq!(report.skipped, 0);
        assert_eq!(report.session_native_id.as_deref(), Some("tab-1"));
        assert_eq!(
            report.session_observation.provider_session_id,
            MetadataResolution::Resolved("tab-1".into())
        );
        assert!(!report.session_observation.multi_session);
        assert_eq!(sink.events.len(), 2);
        // user message, text wins over rawText.
        assert_eq!(sink.events[0].0, 0);
        assert_eq!(sink.events[0].1, "user");
        assert_eq!(sink.events[0].2, "hello");
        assert_eq!(
            sink.events[0].3.as_deref(),
            Some("1970-01-01T00:00:00.101Z")
        );
        assert_eq!(sink.events[1].0, 1);
        assert_eq!(sink.events[1].1, "assistant");
        assert_eq!(sink.events[1].2, "hi there");
        assert_eq!(
            sink.events[1].3.as_deref(),
            Some("1970-01-01T00:00:00.102Z")
        );
    }

    #[test]
    fn parse_filters_invalid_bubbles_and_orders_by_time() {
        let adapter = CursorAdapter::new();
        let chatdata = r#"{"tabs":[{"id":"tab-1","createdAt":1,
            "bubbles":[
              {"type":"assistant","text":"later","timingInfo":{"startTime":200}},
              {"type":"user","text":"first","timingInfo":{"startTime":100}},
              {"type":"user","rawText":"raw only","timingInfo":{"startTime":150}},
              {"type":"","text":"no type"},
              {"type":"assistant","text":"  "},
              {"type":"user","text":"no timing"}
            ]}]}"#;
        let db_bytes = create_cursor_db(Some(chatdata), None);
        let mut sink = sink();
        let report = adapter.parse(&db_bytes, &mut sink).unwrap();

        assert_eq!(report.committed, 4);
        // Ordered by startTime; the untimed bubble sorts last.
        assert_eq!(sink.events[0].2, "first");
        assert_eq!(sink.events[1].2, "raw only");
        assert_eq!(sink.events[2].2, "later");
        assert_eq!(sink.events[3].2, "no timing");
        assert_eq!(sink.events[3].3, None);
        assert_eq!(sink.events[0].1, "user");
        assert_eq!(sink.events[2].1, "assistant");
    }

    #[test]
    fn parse_groups_prompts_by_conversation_id() {
        let adapter = CursorAdapter::new();
        let prompts = r#"[{"id":"p1","prompt":"q1","response":"a1","createdAt":100,"conversationId":"conv-a"},
            {"id":"p2","prompt":"q2","response":"","createdAt":200,"conversationId":"conv-a"},
            {"id":"p3","prompt":"","response":"a3","createdAt":300,"conversationId":"conv-b"}]"#;
        let db_bytes = create_cursor_db(None, Some(prompts));
        let mut sink = sink();
        let report = adapter.parse(&db_bytes, &mut sink).unwrap();

        // conv-a: user q1, assistant a1, user q2; conv-b: assistant a3.
        assert_eq!(report.committed, 4);
        assert_eq!(report.session_native_id.as_deref(), Some("conv-a"));
        assert!(report.session_observation.multi_session);
        assert_eq!(
            report.session_observation.provider_session_id,
            MetadataResolution::Ambiguous
        );
        assert!(report.diagnostics.is_empty());
        // conv-a (earliest 100) emits before conv-b (300).
        let roles: Vec<&str> = sink.events.iter().map(|e| e.1.as_str()).collect();
        assert_eq!(roles, ["user", "assistant", "user", "assistant"]);
        assert_eq!(
            sink.events.iter().map(|e| e.2.as_str()).collect::<Vec<_>>(),
            ["q1", "a1", "q2", "a3"]
        );
        assert_eq!(
            sink.events[1].3.as_deref(),
            Some("1970-01-01T00:00:00.100Z")
        );
        assert_eq!(
            sink.events[3].3.as_deref(),
            Some("1970-01-01T00:00:00.300Z")
        );
    }

    #[test]
    fn parse_single_prompt_conversation_resolves_session_id() {
        let adapter = CursorAdapter::new();
        let prompts = r#"[{"id":"p1","prompt":"q1","response":"a1","createdAt":100,"conversationId":"only-conv"}]"#;
        let db_bytes = create_cursor_db(None, Some(prompts));
        let mut sink = sink();
        let report = adapter.parse(&db_bytes, &mut sink).unwrap();

        assert_eq!(report.committed, 2);
        assert_eq!(report.session_native_id.as_deref(), Some("only-conv"));
        assert_eq!(
            report.session_observation.provider_session_id,
            MetadataResolution::Resolved("only-conv".into())
        );
        assert!(!report.session_observation.multi_session);
    }

    #[test]
    fn parse_missing_native_ids_leave_session_claims_missing() {
        let adapter = CursorAdapter::new();
        // Tab without id + prompts with empty conversationId: two sessions,
        // neither carrying a native id.
        let chatdata =
            r#"{"tabs":[{"bubbles":[{"type":"user","text":"hi","timingInfo":{"startTime":1}}]}]}"#;
        let prompts =
            r#"[{"id":"p1","prompt":"q","response":"a","createdAt":2,"conversationId":""}]"#;
        let db_bytes = create_cursor_db(Some(chatdata), Some(prompts));
        let mut sink = sink();
        let report = adapter.parse(&db_bytes, &mut sink).unwrap();

        assert_eq!(report.committed, 3);
        assert_eq!(report.session_native_id, None);
        // Multi-session fail-closes to ambiguous even without any native id.
        assert_eq!(
            report.session_observation.provider_session_id,
            MetadataResolution::Ambiguous
        );
        assert!(report.session_observation.multi_session);
    }

    #[test]
    fn parse_passes_noise_shaped_user_text_through_verbatim() {
        // 钉住测试：Cursor chatdata bubble 没有 system-reminder / AGENTS.md /
        // 环境上下文等注入概念（user bubble 的 text 就是用户原文，无任何内容
        // 过滤）。形似噪声的文本必须逐字透传，防止将来把别家格式的过滤规则
        // 盲目搬来造成 silent drift。
        let adapter = CursorAdapter::new();
        let chatdata = r##"{"tabs":[{"id":"tab-1","title":"Synthetic","createdAt":100,"lastUpdatedAt":200,
            "bubbles":[
              {"type":"user","text":"<system-reminder>reminder text</system-reminder>","timingInfo":{"startTime":101}},
              {"type":"user","text":"# AGENTS.md instructions","timingInfo":{"startTime":102}}
            ]}]}"##;
        let db_bytes = create_cursor_db(Some(chatdata), None);
        let mut sink = sink();
        let report = adapter.parse(&db_bytes, &mut sink).unwrap();

        assert_eq!(report.committed, 2);
        assert_eq!(report.skipped, 0);
        assert_eq!(
            sink.events[0].2,
            "<system-reminder>reminder text</system-reminder>"
        );
        assert_eq!(sink.events[1].2, "# AGENTS.md instructions");
    }
}

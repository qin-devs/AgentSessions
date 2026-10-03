//! Synthetic timestamp regressions for both proven Cursor storage variants.
//! See golden/PROVENANCE.md for field-unit evidence and fixture provenance.

use std::sync::atomic::{AtomicU64, Ordering};

use agent_session_grep_ports::{
    CanonicalEventSink, Confidence, MessageEvent, ParseReport, PortResult, ProviderAdapter,
    ProviderSessionIdentity,
};
use agent_session_grep_provider_cursor::CursorAdapter;
use agent_session_grep_testkit::assert_read_only;
use agent_session_grep_testkit::golden::CapturingSink;
use rusqlite::{Connection, params};
use serde_json::{Value, json};

const CHAT_DATA_KEY: &str = "workbench.panel.aichat.view.aichat.chatdata";
const PROMPTS_KEY: &str = "aiService.prompts";

#[derive(Default)]
struct TimestampSink {
    inner: CapturingSink,
    sessions: Vec<Option<ProviderSessionIdentity>>,
}

impl CanonicalEventSink for TimestampSink {
    fn emit_message(&mut self, event: MessageEvent<'_>) -> PortResult<()> {
        self.sessions.push(event.session.cloned());
        self.inner.emit_message(event)
    }
}

fn fixture(table: &str) -> Value {
    let mut fixture: Value = serde_json::from_str(include_str!("golden/timestamps.json")).unwrap();
    assert_eq!(fixture["fixture_revision"], 1);
    fixture[table].take()
}

/// Materialize only synthetic row payloads, like the existing property suite.
fn database(table: &str, rows: &Value) -> Vec<u8> {
    assert!(matches!(table, "ItemTable" | "cursorDiskKV"));
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "CREATE TABLE {table} (key TEXT PRIMARY KEY, value BLOB);"
    ))
    .unwrap();
    for (key, value) in rows.as_object().unwrap() {
        conn.execute(
            &format!("INSERT INTO {table} (key, value) VALUES (?1, ?2)"),
            params![key, value.to_string()],
        )
        .unwrap();
    }
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "asg-cursor-timestamps-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    conn.execute("VACUUM INTO ?1", [path.to_str().unwrap()])
        .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    bytes
}

fn parse(table: &str, rows: &Value) -> (ParseReport, TimestampSink) {
    let bytes = database(table, rows);
    let adapter = CursorAdapter::new();
    let probe = assert_read_only(&bytes, |source| adapter.probe(source)).unwrap();
    assert_eq!(probe.confidence, Confidence::Confirmed);
    assert_eq!(
        probe.variant_id,
        if table == "ItemTable" {
            "cursor/vscdb-chat-v1"
        } else {
            "cursor/disk-kv-v1"
        }
    );
    let mut sink = TimestampSink::default();
    let report = assert_read_only(&bytes, |source| adapter.parse(source, &mut sink)).unwrap();
    for (seq, message) in sink.inner.messages.iter().enumerate() {
        assert_eq!(message.seq as usize, seq);
        assert!(message.native_id.is_empty());
        assert!(message.parent_native_id.is_none());
        assert!(message.span.is_none());
        assert!(!message.is_sidechain);
    }
    assert_eq!(report.committed, sink.inner.messages.len());
    (report, sink)
}

#[test]
fn item_table_epoch_millis_preserve_fraction_order_and_missing_values() {
    let (report, sink) = parse("ItemTable", &fixture("ItemTable"));
    let projection: Vec<_> = sink
        .inner
        .messages
        .iter()
        .map(|message| (message.text.as_str(), message.timestamp.as_deref()))
        .collect();
    assert_eq!(
        projection,
        [
            ("epoch before", Some("1969-12-31T23:59:59.999Z")),
            ("epoch zero", Some("1970-01-01T00:00:00.000Z")),
            ("epoch after", Some("1970-01-01T00:00:00.001Z")),
            ("chat before", Some("2024-01-01T00:00:00.122Z")),
            ("chat since", Some("2024-01-01T00:00:00.123Z")),
            ("chat until", Some("2024-01-01T00:00:00.124Z")),
            ("chat null", None),
            ("chat missing", None),
            ("prompt before", Some("2024-01-01T00:00:00.122Z")),
            ("prompt since", Some("2024-01-01T00:00:00.123Z")),
            ("response since", Some("2024-01-01T00:00:00.123Z")),
            ("prompt until", Some("2024-01-01T00:00:00.124Z")),
            ("prompt null", None),
            ("prompt missing", None),
        ]
    );
    assert_eq!(report.skipped, 0);
    assert!(report.diagnostics.is_empty());
    assert!(report.session_observation.multi_session);
    for (index, session) in sink.sessions.iter().enumerate() {
        assert_eq!(
            session.as_ref().unwrap().source_key,
            if index < 8 {
                "timestamp-tab"
            } else {
                "timestamp-prompts"
            }
        );
    }
}

#[test]
fn disk_kv_preserves_bubble_iso_precision_and_header_order_without_fallback() {
    let (report, sink) = parse("cursorDiskKV", &fixture("cursorDiskKV"));
    let projection: Vec<_> = sink
        .inner
        .messages
        .iter()
        .map(|message| (message.text.as_str(), message.timestamp.as_deref()))
        .collect();
    assert_eq!(
        projection,
        [
            ("disk until", Some("2024-01-01T00:00:00.124Z")),
            (
                "  disk precise 雪  ",
                Some("2024-01-01T01:00:00.123456789+01:00")
            ),
            ("disk since", Some("2024-01-01T00:00:00.123Z")),
            ("disk since", Some("2024-01-01T00:00:00.123Z")),
            ("disk before", Some("2024-01-01T00:00:00.122Z")),
            ("disk null", None),
            ("disk missing", None),
        ]
    );
    assert_eq!(report.skipped, 0);
    assert!(
        sink.sessions
            .iter()
            .all(|session| session.as_ref().unwrap().source_key == "timestamp-composer")
    );
}

#[test]
fn item_table_rejects_non_integer_epoch_fields_without_guessing_units() {
    // These unsupported field shapes retain the existing recoverable JSON-value
    // failure contract; they must never turn into Some("") or a guessed epoch.
    for invalid in [
        json!(true),
        json!("1704067200123"),
        json!([]),
        json!({}),
        json!(1.5),
    ] {
        for key in [CHAT_DATA_KEY, PROMPTS_KEY] {
            let payload = if key == CHAT_DATA_KEY {
                json!({"tabs": [{"bubbles": [{
                    "type": "user", "text": "synthetic invalid timestamp",
                    "timingInfo": {"startTime": invalid}
                }]}]})
            } else {
                json!([{"prompt": "synthetic invalid timestamp", "createdAt": invalid}])
            };
            let (report, sink) = parse("ItemTable", &json!({key: payload}));
            assert!(sink.inner.messages.is_empty(), "{key}: {invalid}");
            assert_eq!(report.skipped, 1);
            assert_eq!(report.diagnostics.len(), 1);
            assert!(report.diagnostics[0].contains("not valid JSON"));
        }
    }
}

#[test]
fn item_table_out_of_range_epoch_keeps_the_message_and_reports_timestamp_loss() {
    for epoch in [i64::MIN, i64::MAX] {
        for key in [CHAT_DATA_KEY, PROMPTS_KEY] {
            let payload = if key == CHAT_DATA_KEY {
                json!({"tabs": [{"bubbles": [{
                    "type": "user", "text": "synthetic out of range",
                    "timingInfo": {"startTime": epoch}
                }]}]})
            } else {
                json!([{"prompt": "synthetic out of range", "createdAt": epoch}])
            };
            let (report, sink) = parse("ItemTable", &json!({key: payload}));
            assert_eq!(sink.inner.messages.len(), 1);
            assert_eq!(sink.inner.messages[0].timestamp, None);
            assert_eq!(report.skipped, 0);
            assert_eq!(report.diagnostics.len(), 1);
            assert!(report.diagnostics[0].contains("timestamp"));
            assert!(!report.diagnostics[0].contains(&epoch.to_string()));
        }
    }
}

#[test]
fn disk_kv_does_not_guess_units_for_unsupported_bubble_timestamp_values() {
    for invalid in [
        json!(1735689600000_i64),
        json!(1),
        json!(true),
        json!([]),
        json!({}),
        json!(""),
        json!("   "),
    ] {
        let rows = json!({
            "composerData:synthetic": {
                "createdAt": 1735689600000_i64,
                "fullConversationHeadersOnly": [{"bubbleId": "one", "type": 1}]
            },
            "bubbleId:synthetic:one": {"text": "synthetic", "createdAt": invalid}
        });
        let (report, sink) = parse("cursorDiskKV", &rows);
        assert_eq!(report.committed, 1);
        assert_eq!(report.skipped, 0);
        assert_eq!(sink.inner.messages[0].timestamp, None);
        assert!(
            report
                .diagnostics
                .iter()
                .any(|note| note.contains("timestamp omitted"))
        );
        assert!(
            !report
                .diagnostics
                .iter()
                .any(|note| note.contains("1735689600000"))
        );
    }
}

#[test]
fn item_table_epoch_millis_calendar_and_range_edges_are_exact() {
    for (millis, expected) in [
        (-62_167_219_200_001_i64, None),
        (-62_167_219_200_000, Some("0000-01-01T00:00:00.000Z")),
        (-1_001, Some("1969-12-31T23:59:58.999Z")),
        (1_001, Some("1970-01-01T00:00:01.001Z")),
        (951_782_400_007, Some("2000-02-29T00:00:00.007Z")),
        (4_107_542_400_000, Some("2100-03-01T00:00:00.000Z")),
        (253_402_300_799_999, Some("9999-12-31T23:59:59.999Z")),
        (253_402_300_800_000, None),
    ] {
        let (report, sink) = parse(
            "ItemTable",
            &json!({
                PROMPTS_KEY: [{"prompt": "synthetic calendar edge", "createdAt": millis}]
            }),
        );
        assert_eq!(sink.inner.messages.len(), 1);
        assert_eq!(
            sink.inner.messages[0].timestamp.as_deref(),
            expected,
            "{millis}"
        );
        assert_eq!(report.skipped, 0);
        assert_eq!(report.diagnostics.len(), usize::from(expected.is_none()));
    }
}

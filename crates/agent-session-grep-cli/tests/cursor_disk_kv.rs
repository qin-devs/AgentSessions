//! End-to-end coverage for the additive `cursor/disk-kv-v1` variant.
//!
//! Two facts can only be proven through the real composition root and cannot be
//! seen from the adapter alone:
//!
//! 1. A `state.vscdb` `cursorDiskKV` source is ingested, committed and
//!    searchable through the real CLI (`sync <path>` -> probe -> parse ->
//!    commit -> search), with each composer becoming its own Session.
//! 2. A broken bubble does not discard its session: the remaining slots are
//!    committed, the defect is counted in `skipped` and named in diagnostics.

use rusqlite::Connection;
use std::process::{Command, Output};

/// The freshly built binary (Cargo injects this path at compile time).
const BIN: &str = env!("CARGO_BIN_EXE_agent-session-grep");

/// Fixed clock, matching the other e2e suites so ranking stays deterministic.
const E2E_CLOCK_MS: &str = "1787616000000";

fn run(db: &str, args: &[&str]) -> Output {
    Command::new(BIN)
        .arg("--db")
        .arg(db)
        .arg("--robot")
        .args(args)
        .env("ASG_CLOCK_MS", E2E_CLOCK_MS)
        .output()
        .expect("failed to spawn agent-session-grep binary")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn parse_first_line(output: &Output) -> serde_json::Value {
    let text = stdout(output);
    let line = text.lines().next().expect("output must have a line");
    serde_json::from_str(line).unwrap_or_else(|error| panic!("not JSON: {error}\n{line}"))
}

fn temp_db(tag: &str) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(format!("{tag}.db"));
    let db = path.to_string_lossy().into_owned();
    let store = agent_session_grep_adapters_sqlite::SqliteStore::open_for_write(&db)
        .expect("initialize empty catalog fixture under writer lease");
    drop(store);
    (dir, db)
}

/// One synthetic composer: its id, its `fullConversationHeadersOnly` entries
/// and its bubble rows (a `None` body becomes a SQL NULL value).
struct Composer {
    id: &'static str,
    headers: Vec<serde_json::Value>,
    bodies: Vec<(&'static str, Option<serde_json::Value>)>,
}

/// Write one synthetic `state.vscdb` with a `cursorDiskKV` surface.
///
/// A `headers` entry is `{"bubbleId": id, "type": 1|2}` and `bodies` maps a
/// bubble id to its JSON body (or `None` for a SQL NULL value).
fn write_state_vscdb(path: &std::path::Path, composers: &[Composer]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create state.vscdb parent");
    }
    let conn = Connection::open(path).expect("open synthetic state.vscdb");
    conn.execute_batch("CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value BLOB);")
        .expect("create cursorDiskKV");
    for composer in composers {
        let metadata = serde_json::json!({ "fullConversationHeadersOnly": composer.headers });
        conn.execute(
            "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
            rusqlite::params![
                format!("composerData:{}", composer.id),
                metadata.to_string()
            ],
        )
        .expect("insert composer row");
        for (bubble_id, body) in &composer.bodies {
            let key = format!("bubbleId:{}:{bubble_id}", composer.id);
            match body {
                Some(body) => {
                    conn.execute(
                        "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
                        rusqlite::params![key, body.to_string()],
                    )
                    .expect("insert bubble row");
                }
                None => {
                    conn.execute(
                        "INSERT INTO cursorDiskKV (key, value) VALUES (?1, NULL)",
                        rusqlite::params![key],
                    )
                    .expect("insert NULL bubble row");
                }
            }
        }
    }
    drop(conn);
}

/// Distinct `session_id` values from a Robot `search` frame.
fn hit_sessions(frame: &serde_json::Value) -> Vec<String> {
    let mut sessions: Vec<String> = frame["data"]["hits"]
        .as_array()
        .expect("search hits array")
        .iter()
        .filter_map(|hit| hit["session_id"].as_str().map(str::to_string))
        .collect();
    sessions.sort_unstable();
    sessions.dedup();
    sessions
}

const USER: i64 = 1;
const ASSISTANT: i64 = 2;

#[test]
fn disk_kv_is_ingested_by_explicit_path_and_searchable() {
    let (dir, db) = temp_db("cursor-disk-kv");
    let source = dir.path().join("workspace/state.vscdb");
    write_state_vscdb(
        &source,
        &[
            Composer {
                id: "composer-a",
                headers: vec![
                    serde_json::json!({ "bubbleId": "a1", "type": USER }),
                    serde_json::json!({ "bubbleId": "a2", "type": ASSISTANT }),
                ],
                bodies: vec![
                    (
                        "a1",
                        Some(serde_json::json!({ "text": "synthetic disk kv token one" })),
                    ),
                    (
                        "a2",
                        Some(serde_json::json!({ "text": "synthetic disk kv reply" })),
                    ),
                ],
            },
            Composer {
                id: "composer-b",
                headers: vec![serde_json::json!({ "bubbleId": "b1", "type": USER })],
                bodies: vec![(
                    "b1",
                    Some(serde_json::json!({ "text": "synthetic disk kv token two" })),
                )],
            },
        ],
    );
    let path = source.to_string_lossy().into_owned();

    let sync = run(&db, &["sync", &path]);
    assert!(sync.status.success(), "sync failed: {}", stdout(&sync));
    let frame = parse_first_line(&sync);
    assert_eq!(frame["data"]["committed"], 3, "{frame}");
    assert_eq!(frame["data"]["skipped"], 0, "{frame}");
    // Exactly one diagnostic: the identity note explaining that bubble ids are
    // not adopted as canonical message ids.
    assert_eq!(frame["data"]["diagnostics"], 1, "{frame}");

    let search = run(&db, &["search", "synthetic disk kv"]);
    assert!(
        search.status.success(),
        "search failed: {}",
        stdout(&search)
    );
    let frame = parse_first_line(&search);
    let hits = frame["data"]["hits"].as_array().expect("hits");
    assert!(!hits.is_empty(), "{frame}");
    assert_eq!(
        hit_sessions(&frame).len(),
        2,
        "each composer must stay its own Session: {frame}"
    );

    // A second sync of the unchanged snapshot is a content-level no-op.
    let resync = run(&db, &["sync", &path]);
    assert!(
        resync.status.success(),
        "resync failed: {}",
        stdout(&resync)
    );
    let frame = parse_first_line(&resync);
    assert_eq!(frame["data"]["committed"], 0, "{frame}");
    // `unchanged` counts messages that were already current (three here).
    assert_eq!(frame["data"]["unchanged"], 3, "{frame}");
}

#[test]
fn a_broken_bubble_does_not_discard_its_session() {
    let (dir, db) = temp_db("cursor-disk-kv-bad-bubble");
    let source = dir.path().join("workspace/state.vscdb");
    write_state_vscdb(
        &source,
        &[Composer {
            id: "composer-a",
            headers: vec![
                serde_json::json!({ "bubbleId": "ok", "type": USER }),
                serde_json::json!({ "bubbleId": "gone", "type": ASSISTANT }),
            ],
            bodies: vec![(
                "ok",
                Some(serde_json::json!({ "text": "synthetic disk kv surviving token" })),
            )],
        }],
    );
    let path = source.to_string_lossy().into_owned();

    let sync = run(&db, &["sync", &path]);
    assert!(sync.status.success(), "sync failed: {}", stdout(&sync));
    let frame = parse_first_line(&sync);
    assert_eq!(frame["data"]["committed"], 1, "{frame}");
    assert_eq!(frame["data"]["skipped"], 1, "{frame}");
    // Shared retention warning + identity note + per-composer slot accounting
    // + the defect line naming the exact missing storage key.
    assert_eq!(frame["data"]["diagnostics"], 4, "{frame}");
    let warnings = frame["warnings"].as_array().expect("warnings");
    assert_eq!(warnings.len(), 4, "{frame}");
    for expected in [
        "messages report no adopted native id",
        "2 header slot(s), 1 message(s) emitted, 1 slot(s) skipped",
        "header slot 1 (`bubbleId:composer-a:gone`): missing_row",
    ] {
        assert!(
            warnings
                .iter()
                .any(|warning| warning.as_str().is_some_and(|text| text.contains(expected))),
            "original provider diagnostic must remain visible ({expected}): {frame}"
        );
    }
    let retention = warnings[0].as_str().expect("retention warning");
    assert!(
        retention.starts_with("partial source scan:")
            && retention.contains("history is retained")
            && retention.contains("may temporarily coexist")
            && retention.contains("complete rescan")
            && retention.chars().count() <= 512,
        "bounded retention/coexistence warning must be first: {frame}"
    );
    assert_eq!(
        warnings
            .iter()
            .filter(|warning| warning
                .as_str()
                .is_some_and(|text| text.starts_with("partial source scan:")))
            .count(),
        1,
        "one retention warning per response: {frame}"
    );

    let search = run(&db, &["search", "surviving token"]);
    assert!(
        search.status.success(),
        "search failed: {}",
        stdout(&search)
    );
    let frame = parse_first_line(&search);
    assert_eq!(hit_sessions(&frame).len(), 1, "{frame}");
}

#[test]
fn disk_kv_timestamp_filters_preserve_offsets_and_nanosecond_precision() {
    let (dir, db) = temp_db("cursor-disk-kv-timestamps");
    let source = dir.path().join("workspace/state.vscdb");
    let records = [
        (
            "after",
            "diskkvprecision after",
            "2025-01-01T00:00:00.123456790Z",
        ),
        (
            "before",
            "diskkvprecision before",
            "2025-01-01T01:00:00.123456788+01:00",
        ),
        (
            "exact",
            "diskkvprecision exact",
            "2025-01-01T01:00:00.123456789+01:00",
        ),
    ];
    write_state_vscdb(
        &source,
        &[Composer {
            id: "time-composer",
            headers: records
                .iter()
                .map(|(id, _, _)| serde_json::json!({"bubbleId":id,"type":USER}))
                .collect(),
            bodies: records
                .iter()
                .map(|(id, text, timestamp)| {
                    (
                        *id,
                        Some(serde_json::json!({"text":text,"createdAt":timestamp})),
                    )
                })
                .collect(),
        }],
    );
    let original = std::fs::read(&source).unwrap();
    let output = run(&db, &["sync", source.to_str().unwrap()]);
    assert!(output.status.success(), "{}", stdout(&output));
    let synced = parse_first_line(&output);
    assert_eq!(synced["data"]["committed"], 3);
    for (since, until, expected) in [
        (
            "2025-01-01T00:00:00.123456789Z",
            "2025-01-01T00:00:00.123456790Z",
            "diskkvprecision exact",
        ),
        (
            "2025-01-01T00:00:00.123456790Z",
            "2025-01-01T00:00:00.123456791Z",
            "diskkvprecision after",
        ),
    ] {
        let output = run(
            &db,
            &[
                "search",
                "diskkvprecision",
                "--provider",
                "cursor",
                "--since",
                since,
                "--until",
                until,
            ],
        );
        assert!(output.status.success(), "{}", stdout(&output));
        let frame = parse_first_line(&output);
        let hits = frame["data"]["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 1, "{frame}");
        assert_eq!(hits[0]["text"], expected);
    }
    let conn = Connection::open(&db).unwrap();
    let mut stmt = conn
        .prepare("SELECT payload FROM catalog WHERE id LIKE 'msg_v1_%'")
        .unwrap();
    let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0)).unwrap();
    let stored: std::collections::BTreeMap<String, String> = rows
        .map(|row| {
            let payload: serde_json::Value = serde_json::from_slice(&row.unwrap()).unwrap();
            (
                payload["text"].as_str().unwrap().to_owned(),
                payload["timestamp"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(stored.len(), 3);
    for (_, text, timestamp) in records {
        assert_eq!(
            stored[text], timestamp,
            "retain original offset/fraction spelling"
        );
    }
    let repeated = run(&db, &["sync", source.to_str().unwrap()]);
    assert!(repeated.status.success(), "{}", stdout(&repeated));
    assert_eq!(
        parse_first_line(&repeated)["data"]["generation"],
        synced["data"]["generation"]
    );
    assert_eq!(std::fs::read(source).unwrap(), original);
}

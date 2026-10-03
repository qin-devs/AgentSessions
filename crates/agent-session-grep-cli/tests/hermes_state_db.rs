//! End-to-end coverage for the additive `hermes/sqlite-state-v1` variant.
//!
//! Two facts can only be proven through the real composition root and cannot be
//! seen from the adapter alone:
//!
//! 1. `~/.hermes/state.db` and `~/.hermes/profiles/<name>/state.db` are separate
//!    *sources*, and the profile path feeds the installation namespace - so the
//!    same native session id in two profiles stays two canonical Sessions
//!    instead of collapsing into one entity.
//! 2. A state.db source is ingested, committed and searchable through the real
//!    CLI (`sync <path>` -> probe -> parse -> commit -> search).

use rusqlite::Connection;
use std::process::{Command, Output};

/// The freshly built binary (Cargo injects this path at compile time).
const BIN: &str = env!("CARGO_BIN_EXE_agent-session-grep");

/// Fixed clock, matching the other e2e suites so ranking stays deterministic.
const E2E_CLOCK_MS: &str = "1787616000000";

const SCHEMA: &str = "CREATE TABLE sessions (id TEXT PRIMARY KEY, started_at REAL);\n\
     CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT, content TEXT, \
     tool_calls TEXT, tool_call_id TEXT, tool_name TEXT, reasoning TEXT, timestamp REAL);\n";

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

/// Write one synthetic Hermes `state.db` with a single session and one message.
fn write_state_db(path: &std::path::Path, session_id: &str, text: &str, timestamp: f64) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create state.db parent");
    }
    let conn = Connection::open(path).expect("open synthetic state.db");
    conn.execute_batch(&format!(
        "{SCHEMA}INSERT INTO sessions VALUES ('{session_id}', 1700000000.0);\n\
         INSERT INTO messages (id, session_id, role, content, timestamp) \
         VALUES (1, '{session_id}', 'user', '{text}', {timestamp});\n"
    ))
    .expect("populate synthetic state.db");
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

#[test]
fn state_db_is_ingested_by_explicit_path_and_searchable() {
    let (dir, db) = temp_db("hermes-state-db");
    let source = dir.path().join(".hermes/state.db");
    write_state_db(
        &source,
        "hermes-state-sess-1",
        "synthetic hermes state probe text",
        1700000001.5,
    );
    let path = source.to_string_lossy().into_owned();

    let sync = run(&db, &["sync", &path]);
    assert!(sync.status.success(), "sync failed: {}", stdout(&sync));
    let frame = parse_first_line(&sync);
    assert_eq!(frame["data"]["committed"], 1, "{frame}");
    // Exactly one diagnostic: the identity note explaining that rowids are not
    // adopted as canonical message ids.
    assert_eq!(frame["data"]["diagnostics"], 1, "{frame}");
    assert_eq!(frame["data"]["skipped"], 0, "{frame}");

    let search = run(&db, &["search", "synthetic hermes state"]);
    assert!(
        search.status.success(),
        "search failed: {}",
        stdout(&search)
    );
    let frame = parse_first_line(&search);
    let hits = frame["data"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 1, "{frame}");
    assert_eq!(hit_sessions(&frame).len(), 1, "{frame}");

    // A second sync of the unchanged snapshot is a content-level no-op.
    let resync = run(&db, &["sync", &path]);
    assert!(
        resync.status.success(),
        "resync failed: {}",
        stdout(&resync)
    );
    let frame = parse_first_line(&resync);
    assert_eq!(frame["data"]["committed"], 0, "{frame}");
    assert_eq!(frame["data"]["unchanged"], 1, "{frame}");
}

/// A snapshot whose message rows survive without their session row is an
/// incomplete scan: the omitted rows must be counted as skipped, the previous
/// claims must stay searchable, and restoring the session must converge without
/// duplicate hits.
#[test]
fn partial_orphan_snapshot_counts_skips_and_retains_prior_claims() {
    let (dir, db) = temp_db("hermes-partial-orphan");
    let source = dir.path().join(".hermes/state.db");
    write_state_db(
        &source,
        "hermes-partial-session",
        "synthetic partial orphan needle",
        1700000030.0,
    );
    let path = source.to_string_lossy().into_owned();

    let first = run(&db, &["ingest", &path]);
    assert!(first.status.success(), "ingest failed: {}", stdout(&first));
    let frame = parse_first_line(&first);
    assert_eq!(frame["data"]["committed"], 1, "{frame}");
    assert_eq!(frame["data"]["skipped"], 0, "{frame}");

    // The message row remains, but nothing links it to a session any more.
    let conn = Connection::open(&source).expect("open state.db");
    conn.execute("DELETE FROM sessions", [])
        .expect("drop session row");
    drop(conn);

    let partial = run(&db, &["ingest", &path]);
    assert!(
        partial.status.success(),
        "partial ingest failed: {}",
        stdout(&partial)
    );
    let frame = parse_first_line(&partial);
    assert_eq!(frame["data"]["committed"], 0, "{frame}");
    assert!(
        frame["data"]["skipped"]
            .as_u64()
            .is_some_and(|skipped| skipped >= 1),
        "unread message rows must be counted as skipped: {frame}"
    );
    let search = run(&db, &["search", "orphan needle"]);
    let frame = parse_first_line(&search);
    assert_eq!(
        frame["data"]["hits"].as_array().expect("hits").len(),
        1,
        "an incomplete scan must retain prior claims: {frame}"
    );

    // Restoring the session row returns the source to a complete scan; the
    // original document identity is observed again and nothing is duplicated.
    let conn = Connection::open(&source).expect("open state.db");
    conn.execute(
        "INSERT INTO sessions (id, started_at) VALUES ('hermes-partial-session', 1700000030.0)",
        [],
    )
    .expect("restore session row");
    drop(conn);
    let restored = run(&db, &["ingest", &path]);
    assert!(
        restored.status.success(),
        "restore failed: {}",
        stdout(&restored)
    );
    let frame = parse_first_line(&restored);
    assert_eq!(frame["data"]["skipped"], 0, "{frame}");
    let search = run(&db, &["search", "orphan needle"]);
    let frame = parse_first_line(&search);
    assert_eq!(
        frame["data"]["hits"].as_array().expect("hits").len(),
        1,
        "a complete rescan must converge without duplicates: {frame}"
    );

    // A message that is genuinely gone from a complete scan is an obsolete
    // claim and must be retired; only partial scans retain unseen history.
    let conn = Connection::open(&source).expect("open state.db");
    conn.execute("DELETE FROM messages", [])
        .expect("drop message row");
    drop(conn);
    let cleared = run(&db, &["ingest", &path]);
    assert!(
        cleared.status.success(),
        "complete empty rescan failed: {}",
        stdout(&cleared)
    );
    let frame = parse_first_line(&cleared);
    assert_eq!(frame["data"]["skipped"], 0, "{frame}");
    let search = run(&db, &["search", "orphan needle"]);
    let frame = parse_first_line(&search);
    assert_eq!(
        frame["data"]["hits"].as_array().expect("hits").len(),
        0,
        "a complete scan must retire the removed claim: {frame}"
    );
}

#[test]
fn partial_state_db_rescans_warn_and_preserve_shared_history() {
    for command in ["ingest", "sync"] {
        for defect in ["orphan", "blank", "malformed"] {
            let (dir, db) = temp_db("hermes-shared-partial");
            let source = dir.path().join(".hermes/state.db");
            write_state_db(&source, "first-session", "retainedfirst body", 1700000030.0);
            let conn = Connection::open(&source).unwrap();
            conn.execute_batch(
                "INSERT INTO sessions VALUES ('second-session', 1700000031.0);
                 INSERT INTO messages (id, session_id, role, content, timestamp)
                 VALUES (2, 'second-session', 'user', 'retainedsecond body', 1700000031.0);",
            )
            .unwrap();
            drop(conn);
            let path = source.to_string_lossy().into_owned();
            let first = run(&db, &[command, &path]);
            assert!(first.status.success(), "{}", stdout(&first));
            let hits = parse_first_line(&run(&db, &["search", "retainedfirst"]));
            let session = hit_sessions(&hits).pop().unwrap();
            let shared = dir.path().join("shared/state.db");
            std::fs::create_dir_all(shared.parent().unwrap()).unwrap();
            std::fs::copy(&source, &shared).unwrap();
            let shared_path = shared.to_string_lossy().into_owned();
            let copy = run(&db, &[command, &shared_path]);
            assert!(copy.status.success(), "{}", stdout(&copy));

            let hit_count = |query: &str| {
                let output = run(&db, &["search", query]);
                assert!(output.status.success(), "{}", stdout(&output));
                parse_first_line(&output)["data"]["hits"]
                    .as_array()
                    .unwrap()
                    .len()
            };
            assert_eq!(
                hit_count("retainedfirst"),
                1,
                "shared identity is not parse loss"
            );
            let conn = Connection::open(&source).unwrap();
            conn.execute_batch(match defect {
                "orphan" => "DELETE FROM sessions WHERE id='second-session';",
                "blank" => {
                    "UPDATE sessions SET id='' WHERE id='second-session';
                            UPDATE messages SET session_id='' WHERE id=2;"
                }
                "malformed" => {
                    "UPDATE sessions SET id=CAST(x'ff' AS TEXT) WHERE id='second-session';
                                UPDATE messages SET session_id=CAST(x'ff' AS TEXT) WHERE id=2;"
                }
                _ => unreachable!(),
            })
            .unwrap();
            drop(conn);
            let partial = run(&db, &[command, &path]);
            assert!(
                partial.status.success(),
                "{command}/{defect}: {}",
                stdout(&partial)
            );
            let frame = parse_first_line(&partial);
            assert_eq!(frame["data"]["committed"], 1, "{frame}");
            assert_eq!(frame["data"]["skipped"], 1, "{frame}");
            let warnings = frame["warnings"].as_array().unwrap();
            assert!(
                warnings.iter().any(|warning| warning
                    .as_str()
                    .is_some_and(|text| text.contains("may temporarily coexist")
                        && text.contains("complete rescan"))),
                "partial retention and accepted copies need an explicit bounded warning: {frame}"
            );
            assert!(warnings.iter().all(|warning| {
                let text = warning.as_str().unwrap();
                text.chars().count() <= 513 && !text.contains(&path) && !text.contains(&shared_path)
            }));
            assert_eq!(
                hit_count("retainedfirst"),
                2,
                "old/new document copies are accepted"
            );
            assert_eq!(
                hit_count("retainedsecond"),
                1,
                "omitted history stays searchable"
            );
            let context = run(&db, &["context", &session]);
            assert_eq!(
                parse_first_line(&context)["error"]["code"],
                "schema_incompatible"
            );

            let conn = Connection::open(&source).unwrap();
            conn.execute_batch(
                "DELETE FROM sessions WHERE id<>'first-session';
                 INSERT INTO sessions VALUES ('second-session', 1700000031.0);
                 UPDATE messages SET session_id='second-session', content='recoveredsecond body'
                 WHERE id=2;",
            )
            .unwrap();
            drop(conn);
            let recovered = run(&db, &[command, &path]);
            assert!(recovered.status.success(), "{}", stdout(&recovered));
            assert_eq!(parse_first_line(&recovered)["data"]["skipped"], 0);
            assert_eq!(
                hit_count("retainedfirst"),
                2,
                "only shared old and complete new copies survive"
            );
            assert_eq!(
                hit_count("retainedsecond"),
                1,
                "the shared source still owns old history"
            );
            assert_eq!(hit_count("recoveredsecond"), 1);
            let context = run(&db, &["context", &session]);
            assert!(context.status.success(), "{}", stdout(&context));

            let conn = Connection::open(&shared).unwrap();
            conn.execute("DELETE FROM messages", []).unwrap();
            drop(conn);
            let removed = run(&db, &[command, &shared_path]);
            assert!(removed.status.success(), "{}", stdout(&removed));
            assert_eq!(hit_count("retainedfirst"), 1);
            assert_eq!(
                hit_count("retainedsecond"),
                0,
                "no source owns the obsolete history now"
            );
            let repeated = run(&db, &["sync", &path]);
            assert!(repeated.status.success(), "{}", stdout(&repeated));
            assert_eq!(parse_first_line(&repeated)["data"]["committed"], 0);
        }
    }
}

#[test]
fn unchanged_version_two_state_db_reparses_completeness_once() {
    let (dir, db) = temp_db("hermes-parser-version");
    let source = dir.path().join(".hermes/state.db");
    write_state_db(
        &source,
        "version-session",
        "versionvisible body",
        1700000030.0,
    );
    let conn = Connection::open(&source).unwrap();
    conn.execute(
        "INSERT INTO messages (id, session_id, role, content) VALUES (2, 'orphan', 'user', 'omitted')",
        [],
    )
    .unwrap();
    drop(conn);
    let path = source.to_string_lossy().into_owned();
    let initial = run(&db, &["sync", &path]);
    assert!(initial.status.success(), "{}", stdout(&initial));
    let generation = parse_first_line(&initial)["data"]["generation"]
        .as_u64()
        .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute("UPDATE source_scans SET parser_version=2", [])
        .unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO source_relation_scans(source_path, relation_schema_version)
         SELECT source_path, 7 FROM source_scans",
        [],
    )
    .unwrap();
    drop(conn);
    let reparsed = run(&db, &["sync", &path]);
    assert!(reparsed.status.success(), "{}", stdout(&reparsed));
    let frame = parse_first_line(&reparsed);
    assert_eq!(
        frame["data"]["emitted"], 1,
        "unchanged bytes must still reparse: {frame}"
    );
    assert_eq!(frame["data"]["skipped"], 1, "{frame}");
    assert_eq!(frame["data"]["generation"], generation + 1, "{frame}");
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        conn.query_row("SELECT parser_version FROM source_scans", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        i64::from(agent_session_grep_adapters_sqlite::PARSER_SEMANTIC_VERSION)
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM source_relation_scans", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    drop(conn);
    let repeated = run(&db, &["sync", &path]);
    assert!(repeated.status.success(), "{}", stdout(&repeated));
    let frame = parse_first_line(&repeated);
    assert_eq!(frame["data"]["emitted"], 0, "{frame}");
    assert_eq!(frame["data"]["committed"], 0, "{frame}");
    assert_eq!(frame["data"]["generation"], generation + 1, "{frame}");
}
#[test]
fn profiles_with_the_same_native_session_id_stay_separate_sources() {
    let (dir, db) = temp_db("hermes-state-profiles");
    // Same native session id in both profile databases and the same message
    // rowid: only the profile path separates them.
    let profile_a = dir.path().join(".hermes/profiles/a/state.db");
    let profile_b = dir.path().join(".hermes/profiles/b/state.db");
    write_state_db(
        &profile_a,
        "shared-native-session",
        "synthetic shared token from profile a",
        1700000010.0,
    );
    write_state_db(
        &profile_b,
        "shared-native-session",
        "synthetic shared token from profile b",
        1700000011.0,
    );

    for source in [&profile_a, &profile_b] {
        let path = source.to_string_lossy().into_owned();
        let sync = run(&db, &["sync", &path]);
        assert!(sync.status.success(), "sync failed: {}", stdout(&sync));
        let frame = parse_first_line(&sync);
        assert_eq!(frame["data"]["committed"], 1, "{frame}");
    }

    let search = run(&db, &["search", "synthetic shared token"]);
    assert!(
        search.status.success(),
        "search failed: {}",
        stdout(&search)
    );
    let frame = parse_first_line(&search);
    let sessions = hit_sessions(&frame);
    assert_eq!(
        sessions.len(),
        2,
        "equal native session ids in two profiles must stay two Sessions: {frame}"
    );
    let texts: Vec<&str> = frame["data"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .filter_map(|hit| hit["text"].as_str())
        .collect();
    assert_eq!(texts.len(), 2, "{frame}");
    for expected in ["from profile a", "from profile b"] {
        assert!(
            texts.iter().any(|text| text.contains(expected)),
            "missing {expected} in {frame}"
        );
    }
}

/// Known limitation, reported to the owner rather than hidden: the composition
/// root's installation resolution is *not* part of this adapter, and an already
/// registered shallower installation root absorbs a deeper source that lives
/// under it. Syncing `~/.hermes/state.db` first therefore makes
/// `~/.hermes/profiles/<name>/state.db` join the main installation, and the
/// same native session id in both databases collapses into one Session.
///
/// Fixing it is an identity-layer decision (a single-file SQLite source needs a
/// file-level installation boundary, variant-aware and with migration
/// evidence), not an adapter change - the variant-aware probe/dispatch in this
/// crate cannot see the source path at all. This test pins the current
/// behaviour so such a change cannot land silently.
#[test]
fn known_limitation_main_database_absorbs_a_profile_source() {
    let (dir, db) = temp_db("hermes-state-absorb");
    let main_db = dir.path().join(".hermes/state.db");
    let profile_db = dir.path().join(".hermes/profiles/work/state.db");
    write_state_db(
        &main_db,
        "shared-native-session",
        "synthetic shared token from main",
        1700000010.0,
    );
    write_state_db(
        &profile_db,
        "shared-native-session",
        "synthetic shared token from profile",
        1700000011.0,
    );

    for source in [&main_db, &profile_db] {
        let path = source.to_string_lossy().into_owned();
        let sync = run(&db, &["sync", &path]);
        assert!(sync.status.success(), "sync failed: {}", stdout(&sync));
    }

    let search = run(&db, &["search", "synthetic shared token"]);
    assert!(
        search.status.success(),
        "search failed: {}",
        stdout(&search)
    );
    let frame = parse_first_line(&search);
    assert_eq!(
        hit_sessions(&frame).len(),
        1,
        "documented limitation: the shallower installation root currently owns \
         the deeper profile source, so both databases share one Session: {frame}"
    );
}

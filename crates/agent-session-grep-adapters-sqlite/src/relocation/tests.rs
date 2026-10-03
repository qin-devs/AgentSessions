//! Synthetic relocation regressions. Every filesystem mutation is contained
//! inside an owned temporary directory; provider transcripts are never used.

use super::*;
use agent_session_grep_domain::{
    IdKind, MessageRelation, TokenSource, ToolActivityActor, ToolActivityKind, ToolActivityStatus,
};
use agent_session_grep_ports::{CatalogStore, ContextGraphStore, ResumeClaimsStore};
use rusqlite::types::Value as SqlValue;
use std::fs;
use std::path::Path;

const NOW_MS: i64 = 1_700_000_000_000;
const TTL_DAYS: u32 = 90;
// Independent of the implementation inventory: adding a live source locator
// table must update the regression contract as well as the migration itself.
const EXPECTED_LOCATOR_TABLES: [&str; 9] = [
    "source_entity_projections",
    "source_installations",
    "source_membership",
    "source_placement_membership",
    "source_relation_scans",
    "source_scans",
    "source_session_resume_claims",
    "tool_activity_membership",
    "usage_event_membership",
];

fn clock_now() -> PortResult<i64> {
    Ok(NOW_MS)
}
fn clock_expired_plan() -> PortResult<i64> {
    Ok(NOW_MS + 15 * 60 * 1000)
}
fn clock_expired_alias() -> PortResult<i64> {
    Ok(NOW_MS + i64::from(TTL_DAYS) * 86_400_000)
}

fn locator(path: &Path) -> String {
    path.to_str()
        .expect("synthetic fixture paths are UTF-8")
        .replace('\\', "/")
}

#[derive(Clone, Debug, PartialEq)]
struct TableState {
    columns: Vec<String>,
    rows: Vec<Vec<SqlValue>>,
}
type State = BTreeMap<String, TableState>;

fn table_state(conn: &Connection, table: &str) -> TableState {
    let quoted = table.replace('"', "\"\"");
    let mut statement = conn
        .prepare(&format!("SELECT * FROM \"{quoted}\""))
        .unwrap();
    let columns = statement
        .column_names()
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    let width = statement.column_count();
    let mut rows: Vec<Vec<SqlValue>> = statement
        .query_map([], |row| (0..width).map(|index| row.get(index)).collect())
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    rows.sort_by_cached_key(|row| format!("{row:?}"));
    TableState { columns, rows }
}

fn connection_state(conn: &Connection) -> State {
    let mut statement = conn.prepare(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).unwrap();
    let tables: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    tables
        .into_iter()
        .map(|table| {
            let state = table_state(conn, &table);
            (table, state)
        })
        .collect()
}

fn state(store: &SqliteStore) -> State {
    connection_state(&store.conn.borrow())
}

fn live_state(store: &SqliteStore) -> State {
    let mut state = state(store);
    state.remove("index_batches"); // An aborted building intent is recoverable bookkeeping.
    state
}

fn canonical_state(store: &SqliteStore) -> State {
    let mut result = state(store);
    result.retain(|name, _| {
        !EXPECTED_LOCATOR_TABLES.contains(&name.as_str())
            && ![
                "index_batches",
                "store_metadata",
                "installation_locations",
                "installation_relocations",
            ]
            .contains(&name.as_str())
    });
    result
}

fn count(store: &SqliteStore, table: &str) -> usize {
    table_state(&store.conn.borrow(), table).rows.len()
}

fn column<'a>(row: &'a [SqlValue], table: &TableState, name: &str) -> &'a SqlValue {
    &row[table
        .columns
        .iter()
        .position(|column| column == name)
        .unwrap()]
}

fn assert_locator_remapping(before: &State, after: &State, mappings: &[(&str, &str)]) {
    let actual_tables: Vec<_> = before
        .iter()
        .filter(|(_, table)| table.columns.iter().any(|column| column == "source_path"))
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(actual_tables, EXPECTED_LOCATOR_TABLES);
    for name in EXPECTED_LOCATOR_TABLES {
        let mut expected = before[name].clone();
        assert!(!expected.rows.is_empty(), "fixture must exercise {name}");
        let path_column = expected
            .columns
            .iter()
            .position(|column| column == "source_path")
            .unwrap();
        let key_column = expected
            .columns
            .iter()
            .position(|column| column == "source_key");
        for row in &mut expected.rows {
            let SqlValue::Text(old) = &row[path_column] else {
                panic!("source path must be text")
            };
            let (_, new) = mappings
                .iter()
                .find(|(path, _)| *path == old)
                .expect("every fixture source is mapped");
            row[path_column] = SqlValue::Text((*new).to_owned());
            if let Some(index) = key_column {
                row[index] = SqlValue::Text(normalize_absolute_path(new).unwrap());
            }
        }
        expected.rows.sort_by_cached_key(|row| format!("{row:?}"));
        assert_eq!(after[name], expected, "complete locator state for {name}");
    }
}

#[derive(Clone)]
struct PreparedSource {
    batch: SourceBatch,
    namespace: String,
    sessions: Vec<StableId>,
    natives: Vec<String>,
}

fn batch_from_snapshot(
    path: &str,
    provider: &str,
    namespace: &str,
    natives: &[&str],
    snapshot: &SourceSnapshot,
) -> PreparedSource {
    let document = StableId::derive(
        IdKind::Document,
        Stability::Reconstructed,
        &[snapshot.fingerprint.as_bytes()],
    );
    let mut batch = SourceBatch {
        source_path: path.into(),
        entries: vec![(
            document.clone(),
            serde_json::to_vec(&serde_json::json!({
                "provider": provider, "variant": format!("{provider}/synthetic-v1"),
                "fingerprint": snapshot.fingerprint, "len": snapshot.len,
            }))
            .unwrap(),
            String::new(),
        )],
        placements: Vec::new(),
        edges: Vec::new(),
        activities: Vec::new(),
        usage_events: Vec::new(),
        relation_complete: true,
        len_bytes: Some(snapshot.len.try_into().unwrap()),
        fingerprint: Some(snapshot.fingerprint.clone()),
        provider_id: Some(provider.into()),
        resume_claims: Vec::new(),
    };
    let mut sessions = Vec::new();
    for (session_index, native) in natives.iter().enumerate() {
        let session = StableId::native_session_scoped(
            &SessionIdentityNamespace {
                provider_id: provider,
                installation_namespace: namespace,
            },
            native,
        );
        let mut messages: Vec<StableId> = Vec::new();
        for (ordinal, role) in ["user", "assistant"].into_iter().enumerate() {
            let text = format!("synthetic {native} {role} body");
            let message = StableId::derive(
                IdKind::Message,
                Stability::Reconstructed,
                &[session.as_str().as_bytes(), text.as_bytes()],
            );
            batch.entries.push((
                message.clone(),
                serde_json::to_vec(&serde_json::json!({
                    "role": role, "text": text, "timestamp": "2026-09-01T00:00:00Z",
                    "session": session.as_str(), "sessions": [session.as_str()],
                    "parent": null, "parent_native_id": null, "is_sidechain": false,
                    "span": null, "spans": [],
                }))
                .unwrap(),
                text,
            ));
            let placement = MessagePlacement::new(
                session.clone(),
                document.clone(),
                message.clone(),
                (session_index * 2 + ordinal).try_into().unwrap(),
                false,
                None,
            );
            if let Some(parent) = messages.first() {
                batch.edges.push(MessageEdge {
                    child_placement_id: placement.id.clone(),
                    parent_message_id: parent.clone(),
                    parent_native_id: Some("synthetic-parent".into()),
                    relation: MessageRelation::Reply,
                });
            }
            batch.placements.push(placement);
            messages.push(message);
        }
        batch.entries.push((
            session.clone(),
            serde_json::to_vec(&serde_json::json!({
                "document": document.as_str(), "documents": [document.as_str()],
                "messages": messages.iter().map(StableId::as_str).collect::<Vec<_>>(),
            }))
            .unwrap(),
            String::new(),
        ));
        batch.activities.push(SourceActivity {
            message_id: messages[1].clone(),
            activity: ToolActivity {
                kind: ToolActivityKind::Command,
                actor: ToolActivityActor::Main,
                name: "shell".into(),
                target: Some("echo synthetic".into()),
                status: ToolActivityStatus::Success,
            },
        });
        batch.usage_events.push(SourceUsage {
            session_id: session.clone(),
            message_id: Some(messages[1].clone()),
            usage: UsageObservation {
                input_tokens: 17,
                output_tokens: 9,
                cache_read_tokens: 3,
                cache_write_tokens: 2,
                reasoning_tokens: 1,
                token_source: TokenSource::Observed,
            },
        });
        batch.resume_claims.push(SourceResumeClaim {
            provider_id: provider.into(),
            session_id: session.as_str().into(),
            provider_session_id: Some((*native).into()),
            provider_session_id_state: "resolved".into(),
            original_working_directory: Some("/synthetic/original-project".into()),
            original_working_directory_state: "resolved".into(),
            pair_observed: true,
        });
        sessions.push(session);
    }
    PreparedSource {
        batch,
        namespace: namespace.into(),
        sessions,
        natives: natives.iter().map(|value| (*value).into()).collect(),
    }
}

fn stage_existing(
    store: &SqliteStore,
    path: &str,
    provider: &str,
    natives: &[&str],
) -> PreparedSource {
    let namespace = store
        .resolve_or_allocate_installation_namespace(
            provider,
            path,
            &legacy_installation_namespace(path, provider),
        )
        .unwrap();
    let snapshot = capture(Path::new(path)).unwrap();
    batch_from_snapshot(path, provider, &namespace, natives, &snapshot)
}

struct Fixture {
    store: SqliteStore,
    db: String,
    from: String,
    to: String,
    provider: &'static str,
    temporary: tempfile::TempDir,
}

impl Fixture {
    fn new(provider: &'static str) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let marker = match provider {
            "claude-code" => ".claude",
            "codex" => ".codex",
            _ => "installation",
        };
        let from = locator(&temporary.path().join("original").join(marker));
        let to = locator(&temporary.path().join("moved").join(marker));
        fs::create_dir_all(&from).unwrap();
        let db = locator(&temporary.path().join("catalog").join("index.sqlite"));
        let store = SqliteStore::open_for_write(&db)
            .unwrap()
            .with_relocation_clock(clock_now);
        Self {
            store,
            db,
            from,
            to,
            provider,
            temporary,
        }
    }

    fn path(&self, name: &str) -> String {
        locator(&self.temporary.path().join(name))
    }

    fn stage(&self, relative: &str, natives: &[&str]) -> PreparedSource {
        let path = locator(&Path::new(&self.from).join(relative));
        fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
        fs::write(&path, format!("synthetic source {}\n", natives.join(","))).unwrap();
        stage_existing(&self.store, &path, self.provider, natives)
    }

    fn commit(&self, sources: &[&PreparedSource]) {
        let batches: Vec<_> = sources.iter().map(|source| source.batch.clone()).collect();
        assert!(
            self.store
                .commit_source_batches_if_changed(&batches)
                .unwrap()
        );
    }

    fn destination(&self, source: &PreparedSource) -> String {
        let relative = Path::new(&source.batch.source_path)
            .strip_prefix(&self.from)
            .unwrap();
        locator(&Path::new(&self.to).join(relative))
    }

    fn move_source(&self, source: &PreparedSource) -> String {
        let destination = self.destination(source);
        fs::create_dir_all(Path::new(&destination).parent().unwrap()).unwrap();
        fs::rename(&source.batch.source_path, &destination).unwrap();
        destination
    }

    fn preview(&self) -> RelocationResult {
        self.store
            .relocation_preview(self.provider, &self.from, &self.to, TTL_DAYS)
            .unwrap()
    }

    fn apply(&self, token: &str, backup: &str) -> PortResult<RelocationResult> {
        self.store.apply_relocation(
            self.provider,
            &self.from,
            &self.to,
            TTL_DAYS,
            token,
            &self.path(backup),
        )
    }
}

#[test]
fn reservations_are_private_until_valid_source_activation() {
    let fixture = Fixture::new("claude-code");
    let before = state(&fixture.store);
    let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
    let second = fixture.stage("sessions/b.jsonl", &["session-b"]);
    assert_eq!(source.namespace, second.namespace);
    assert!(source.namespace.starts_with("installation-v1:"));
    assert_eq!(
        state(&fixture.store),
        before,
        "resolving namespaces must not persist reservations"
    );
    let mut invalid = source.batch.clone();
    invalid.entries.push(invalid.entries[0].clone());
    assert!(
        fixture
            .store
            .commit_source_batches_if_changed(&[invalid])
            .is_err()
    );
    assert_eq!(
        state(&fixture.store),
        before,
        "invalid stage cannot pollute registry or intents"
    );
    fixture.commit(&[&source, &second]);
    assert_eq!(count(&fixture.store, "installation_namespaces"), 1);
    assert_eq!(count(&fixture.store, "source_installations"), 2);
    assert_eq!(
        fixture
            .store
            .active_installation_roots("claude-code")
            .unwrap(),
        [fixture.from]
    );
}

#[test]
fn invalid_namespace_or_provider_claim_rolls_back_every_registry_table() {
    let fixture = Fixture::new("codex");
    let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
    let before = state(&fixture.store);
    let mut wrong_namespace = source.batch.clone();
    wrong_namespace.resume_claims[0].provider_session_id = Some("different-native-session".into());
    assert!(
        fixture
            .store
            .commit_source_batches_if_changed(&[wrong_namespace])
            .is_err()
    );
    assert_eq!(state(&fixture.store), before);
    let mut wrong_provider = source.batch;
    wrong_provider.provider_id = Some("claude-code".into());
    assert!(
        fixture
            .store
            .commit_source_batches_if_changed(&[wrong_provider])
            .is_err()
    );
    assert_eq!(state(&fixture.store), before);
}

#[test]
fn relocation_preserves_all_locator_claims_identities_context_and_observations() {
    let fixture = Fixture::new("claude-code");
    let first = fixture.stage("projects/一/a.jsonl", &["session-a", "session-b"]);
    let second = fixture.stage("projects/二/b.jsonl", &["session-b", "session-c"]);
    fixture.commit(&[&first, &second]);
    let mut sessions = first.sessions.clone();
    sessions.push(second.sessions[1].clone());
    let contexts: Vec<_> = sessions
        .iter()
        .map(|id| fixture.store.load_session_graph(id).unwrap())
        .collect();
    let resume = fixture.store.resume_of(&sessions).unwrap();
    let before = state(&fixture.store);
    let canonical = canonical_state(&fixture.store);
    let generation = fixture.store.active_generation().unwrap();
    let first_to = fixture.move_source(&first);
    let second_to = fixture.move_source(&second);
    let plan = fixture.preview();
    assert_eq!(plan.status, RelocationStatus::Planned);
    assert_eq!(
        (plan.source_count, plan.session_count, plan.namespace_count),
        (2, 3, 1)
    );
    assert_eq!(plan.generation, generation);
    assert_eq!(state(&fixture.store), before, "preview is read-only");
    let result = fixture
        .apply(plan.plan.as_deref().unwrap(), "verified-backup.sqlite")
        .unwrap();
    assert_eq!(result.status, RelocationStatus::Applied);
    assert_eq!(result.previous_generation, generation);
    assert_eq!(result.generation, generation + 1);
    assert_locator_remapping(
        &before,
        &state(&fixture.store),
        &[
            (&first.batch.source_path, &first_to),
            (&second.batch.source_path, &second_to),
        ],
    );
    assert_eq!(canonical_state(&fixture.store), canonical);
    assert_eq!(fixture.store.resume_of(&sessions).unwrap(), resume);
    for (id, expected) in sessions.iter().zip(contexts) {
        assert_eq!(fixture.store.load_session_graph(id).unwrap(), expected);
        assert!(
            fixture.store.get(id).unwrap().is_some(),
            "old canonical lookup remains direct"
        );
    }
    let backup = SqliteStore::open(&fixture.path("verified-backup.sqlite")).unwrap();
    assert_eq!(
        state(&backup),
        before,
        "verified backup is the complete pre-relocation catalog"
    );
    let after_intents = state(&fixture.store).remove("index_batches").unwrap();
    assert!(
        before["index_batches"]
            .rows
            .iter()
            .all(|row| after_intents.rows.contains(row)),
        "historical durable manifests must remain byte-for-byte immutable"
    );
    assert_eq!(
        fixture
            .store
            .active_installation_roots("claude-code")
            .unwrap(),
        std::slice::from_ref(&fixture.to)
    );
    let natives: Vec<_> = first.natives.iter().map(String::as_str).collect();
    let rescanned = stage_existing(&fixture.store, &first_to, fixture.provider, &natives);
    assert_eq!(rescanned.namespace, first.namespace);
    assert_eq!(rescanned.sessions, first.sessions);
    assert!(
        !fixture
            .store
            .commit_source_batches_if_changed(&[rescanned.batch])
            .unwrap()
    );
    assert_eq!(fixture.store.active_generation().unwrap(), generation + 1);
}

#[test]
fn same_native_session_id_in_independent_installations_remains_distinct() {
    let fixture = Fixture::new("claude-code");
    let first = fixture.stage("sessions/a.jsonl", &["same-native"]);
    let other_path = fixture.path("independent/.claude/sessions/a.jsonl");
    fs::create_dir_all(Path::new(&other_path).parent().unwrap()).unwrap();
    fs::write(&other_path, b"synthetic independent installation\n").unwrap();
    let other = stage_existing(
        &fixture.store,
        &other_path,
        fixture.provider,
        &["same-native"],
    );
    assert_ne!(first.namespace, other.namespace);
    assert_ne!(first.sessions, other.sessions);
    fixture.commit(&[&first, &other]);
    let unrelated_graph = fixture
        .store
        .load_session_graph(&other.sessions[0])
        .unwrap();
    fixture.move_source(&first);
    let plan = fixture.preview();
    fixture
        .apply(plan.plan.as_deref().unwrap(), "backup.sqlite")
        .unwrap();
    assert_eq!(
        fixture
            .store
            .load_session_graph(&other.sessions[0])
            .unwrap(),
        unrelated_graph
    );
    assert!(fixture.store.get(&first.sessions[0]).unwrap().is_some());
    assert_eq!(count(&fixture.store, "installation_namespaces"), 2);
}

#[test]
fn occupied_destination_cannot_merge_independent_installations() {
    let fixture = Fixture::new("claude-code");
    let first = fixture.stage("sessions/a.jsonl", &["same-native"]);
    let destination = fixture.destination(&first);
    fs::create_dir_all(Path::new(&destination).parent().unwrap()).unwrap();
    fs::copy(&first.batch.source_path, &destination).unwrap();
    let occupied = stage_existing(
        &fixture.store,
        &destination,
        fixture.provider,
        &["same-native"],
    );
    fixture.commit(&[&first, &occupied]);
    let before = state(&fixture.store);
    assert!(matches!(
        fixture
            .store
            .relocation_preview(fixture.provider, &fixture.from, &fixture.to, TTL_DAYS),
        Err(PortError::InvalidRequest(_))
    ));
    assert_eq!(state(&fixture.store), before);
    assert_ne!(first.sessions, occupied.sessions);
}

#[test]
fn stale_generation_fails_before_missing_new_source_or_backup_creation() {
    let fixture = Fixture::new("codex");
    let first = fixture.stage("sessions/a.jsonl", &["session-a"]);
    fixture.commit(&[&first]);
    fixture.move_source(&first);
    let plan = fixture.preview();
    let second = fixture.stage("sessions/b.jsonl", &["session-b"]);
    fixture.commit(&[&second]);
    let before = state(&fixture.store);
    assert!(matches!(
        fixture.apply(plan.plan.as_deref().unwrap(), "must-not-exist.sqlite"),
        Err(PortError::GenerationMismatch(_))
    ));
    assert_eq!(state(&fixture.store), before);
    assert!(!Path::new(&fixture.path("must-not-exist.sqlite")).exists());
}

#[test]
fn changed_and_missing_destination_sources_leave_the_catalog_unchanged() {
    for missing in [false, true] {
        let fixture = Fixture::new("claude-code");
        let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
        fixture.commit(&[&source]);
        let destination = fixture.move_source(&source);
        let plan = fixture.preview();
        let before = state(&fixture.store);
        if missing {
            fs::remove_file(&destination).unwrap();
        } else {
            fs::write(
                &destination,
                b"changed synthetic content with another length\n",
            )
            .unwrap();
        }
        let error = fixture
            .apply(plan.plan.as_deref().unwrap(), "must-not-exist.sqlite")
            .unwrap_err();
        if missing {
            assert!(matches!(error, PortError::SourceIo(_)));
        } else {
            assert!(matches!(error, PortError::SnapshotChanged(_)));
        }
        assert_eq!(state(&fixture.store), before);
        assert!(!Path::new(&fixture.path("must-not-exist.sqlite")).exists());
    }
}

#[test]
fn every_live_locator_write_failure_rolls_back_all_tables_and_generation() {
    for table in EXPECTED_LOCATOR_TABLES {
        let fixture = Fixture::new("claude-code");
        let source = fixture.stage("sessions/a.jsonl", &["session-a", "session-b"]);
        fixture.commit(&[&source]);
        fixture.move_source(&source);
        let plan = fixture.preview();
        fixture.store.conn.borrow().execute_batch(&format!(
            "CREATE TRIGGER synthetic_relocation_failure BEFORE UPDATE OF source_path ON {table}
             WHEN NEW.source_path <> OLD.source_path BEGIN SELECT RAISE(ABORT, 'synthetic relocation failure'); END;"
        )).unwrap();
        let before = live_state(&fixture.store);
        let before_intents = count(&fixture.store, "index_batches");
        let generation = fixture.store.active_generation().unwrap();
        assert!(
            fixture
                .apply(plan.plan.as_deref().unwrap(), "backup.sqlite")
                .is_err(),
            "{table} must be updated"
        );
        assert_eq!(
            live_state(&fixture.store),
            before,
            "all live state must roll back after {table}"
        );
        assert_eq!(fixture.store.active_generation().unwrap(), generation);
        assert_eq!(count(&fixture.store, "index_batches"), before_intents + 1);
        assert_eq!(fixture.store.interrupted_batch_count().unwrap(), 1);
        fixture.store.recover_interrupted().unwrap();
        assert_eq!(fixture.store.interrupted_batch_count().unwrap(), 0);
        assert_eq!(
            live_state(&fixture.store),
            before,
            "recovery changes bookkeeping only"
        );
    }
}

fn file_names(root: &Path) -> Vec<String> {
    fn visit(root: &Path, directory: &Path, names: &mut Vec<String>) {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &entry.path(), names);
            } else {
                names.push(locator(entry.path().strip_prefix(root).unwrap()));
            }
        }
    }
    let mut names = Vec::new();
    visit(root, root, &mut names);
    names.sort();
    names
}

#[test]
fn read_only_preview_under_a_writer_lease_creates_no_files_or_live_state() {
    let fixture = Fixture::new("claude-code");
    let source = fixture.stage("sessions/a.jsonl", &["synthetic-private-native-marker"]);
    fixture.commit(&[&source]);
    let destination = fixture.move_source(&source);
    let source_bytes = fs::read(&destination).unwrap();
    let before = state(&fixture.store);
    let names = file_names(fixture.temporary.path());
    let reader = SqliteStore::open(&fixture.db)
        .unwrap()
        .with_relocation_clock(clock_now);
    let plan = reader
        .relocation_preview(fixture.provider, &fixture.from, &fixture.to, TTL_DAYS)
        .unwrap();
    assert_eq!(plan.status, RelocationStatus::Planned);
    assert_eq!(state(&fixture.store), before);
    assert_eq!(fs::read(&destination).unwrap(), source_bytes);
    assert_eq!(file_names(fixture.temporary.path()), names);
    let rendered = serde_json::to_string(&plan).unwrap();
    for private in [&fixture.from, &fixture.to, &source.natives[0]] {
        assert!(
            !rendered.contains(private),
            "public preview must contain aggregates and opaque tokens only"
        );
    }
    let error = reader
        .apply_relocation(
            fixture.provider,
            &fixture.from,
            &fixture.to,
            TTL_DAYS,
            plan.plan.as_deref().unwrap(),
            &fixture.path("no-writer-backup.sqlite"),
        )
        .unwrap_err();
    assert!(matches!(error, PortError::WriterBusy(_)));
    assert_eq!(state(&fixture.store), before);
    assert_eq!(file_names(fixture.temporary.path()), names);
    let missing = fixture.path("absent-catalog/index.sqlite");
    assert!(SqliteStore::open(&missing).is_err());
    assert!(
        !Path::new(&missing).parent().unwrap().exists(),
        "read-open must not create a catalog directory"
    );
}

#[test]
fn overlap_unknown_provider_and_relative_roots_are_refused_without_writes() {
    let fixture = Fixture::new("claude-code");
    let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
    fixture.commit(&[&source]);
    let nested = locator(&Path::new(&fixture.from).join("nested"));
    let before = state(&fixture.store);
    for (provider, from, to) in [
        (fixture.provider, fixture.from.as_str(), nested.as_str()),
        (fixture.provider, nested.as_str(), fixture.from.as_str()),
        (fixture.provider, "relative", fixture.to.as_str()),
        (fixture.provider, fixture.from.as_str(), "relative"),
        ("not-a-provider", fixture.from.as_str(), fixture.to.as_str()),
    ] {
        assert!(matches!(
            fixture
                .store
                .relocation_preview(provider, from, to, TTL_DAYS),
            Err(PortError::InvalidRequest(_))
        ));
        assert_eq!(state(&fixture.store), before);
    }
}

#[test]
fn tampered_expired_or_different_retention_plans_do_not_create_backups() {
    let mut fixture = Fixture::new("codex");
    let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
    fixture.commit(&[&source]);
    fixture.move_source(&source);
    let token = fixture.preview().plan.unwrap();
    let before = state(&fixture.store);
    let mut corrupted = token.clone().into_bytes();
    corrupted[0] = if corrupted[0] == b'A' { b'B' } else { b'A' };
    assert!(matches!(
        fixture.apply(&String::from_utf8(corrupted).unwrap(), "tampered.sqlite"),
        Err(PortError::InvalidRequest(_))
    ));
    assert!(matches!(
        fixture.store.apply_relocation(
            fixture.provider,
            &fixture.from,
            &fixture.to,
            TTL_DAYS + 1,
            &token,
            &fixture.path("retention.sqlite")
        ),
        Err(PortError::InvalidRequest(_))
    ));
    fixture.store = fixture.store.with_relocation_clock(clock_expired_plan);
    assert!(matches!(
        fixture.apply(&token, "expired.sqlite"),
        Err(PortError::InvalidRequest(_))
    ));
    assert_eq!(state(&fixture.store), before);
    for name in ["tampered.sqlite", "retention.sqlite", "expired.sqlite"] {
        assert!(!Path::new(&fixture.path(name)).exists());
    }
}

#[test]
fn backup_collisions_and_failures_preserve_catalog_sources_and_existing_files() {
    for mode in [
        "existing",
        "directory",
        "missing-parent",
        "wal",
        "shm",
        "catalog",
        "provider",
    ] {
        let fixture = Fixture::new("claude-code");
        let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
        fixture.commit(&[&source]);
        let destination = fixture.move_source(&source);
        let token = fixture.preview().plan.unwrap();
        let mut backup = fixture.path("backup.sqlite");
        let sentinel = b"owned synthetic backup sentinel";
        let preserved_file = match mode {
            "existing" => {
                fs::write(&backup, sentinel).unwrap();
                Some(backup.clone())
            }
            "directory" => {
                fs::create_dir(&backup).unwrap();
                None
            }
            "missing-parent" => {
                backup = fixture.path("absent/backup.sqlite");
                None
            }
            "wal" | "shm" => {
                let sidecar = format!("{backup}-{mode}");
                fs::write(&sidecar, sentinel).unwrap();
                Some(sidecar)
            }
            "catalog" => {
                backup.clone_from(&fixture.db);
                None
            }
            "provider" => {
                backup.clone_from(&destination);
                None
            }
            _ => unreachable!(),
        };
        let before = state(&fixture.store);
        let names = file_names(fixture.temporary.path());
        let provider_bytes = fs::read(&destination).unwrap();
        let error = fixture
            .store
            .apply_relocation(
                fixture.provider,
                &fixture.from,
                &fixture.to,
                TTL_DAYS,
                &token,
                &backup,
            )
            .unwrap_err();
        assert!(matches!(error, PortError::Backend(_)), "{mode}: {error}");
        assert_eq!(state(&fixture.store), before, "backup failure in {mode}");
        assert_eq!(fs::read(&destination).unwrap(), provider_bytes);
        assert_eq!(
            file_names(fixture.temporary.path()),
            names,
            "backup failure must clean owned temporary files"
        );
        if let Some(path) = preserved_file {
            assert_eq!(fs::read(path).unwrap(), sentinel);
        }
    }
}

#[test]
fn replay_is_a_no_op_with_no_new_backup_or_generation() {
    let fixture = Fixture::new("claude-code");
    let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
    fixture.commit(&[&source]);
    fixture.move_source(&source);
    let token = fixture.preview().plan.unwrap();
    let applied = fixture.apply(&token, "first.sqlite").unwrap();
    let before = state(&fixture.store);
    let replay = fixture.apply(&token, "must-not-exist.sqlite").unwrap();
    assert_eq!(replay.status, RelocationStatus::Unchanged);
    assert_eq!(replay.generation, applied.generation);
    assert_eq!(state(&fixture.store), before);
    assert!(!Path::new(&fixture.path("must-not-exist.sqlite")).exists());
    let preview = fixture.preview();
    assert_eq!(preview.status, RelocationStatus::Unchanged);
    assert!(preview.plan.is_none());
    assert_eq!(state(&fixture.store), before);
}

#[test]
fn reverse_move_requires_a_fresh_plan_and_retains_flat_namespace_locations() {
    let fixture = Fixture::new("claude-code");
    let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
    fixture.commit(&[&source]);
    let canonical = canonical_state(&fixture.store);
    let destination = fixture.move_source(&source);
    let forward_token = fixture.preview().plan.unwrap();
    let applied = fixture.apply(&forward_token, "forward.sqlite").unwrap();
    fs::rename(&destination, &source.batch.source_path).unwrap();
    let before = state(&fixture.store);
    assert!(
        fixture
            .store
            .apply_relocation(
                fixture.provider,
                &fixture.to,
                &fixture.from,
                TTL_DAYS,
                &forward_token,
                &fixture.path("wrong-plan.sqlite")
            )
            .is_err()
    );
    assert_eq!(state(&fixture.store), before);
    assert!(!Path::new(&fixture.path("wrong-plan.sqlite")).exists());
    let reverse = fixture
        .store
        .relocation_preview(fixture.provider, &fixture.to, &fixture.from, TTL_DAYS)
        .unwrap();
    assert_eq!(reverse.status, RelocationStatus::Planned);
    let result = fixture
        .store
        .apply_relocation(
            fixture.provider,
            &fixture.to,
            &fixture.from,
            TTL_DAYS,
            reverse.plan.as_deref().unwrap(),
            &fixture.path("reverse.sqlite"),
        )
        .unwrap();
    assert_eq!(result.status, RelocationStatus::Applied);
    assert_eq!(result.generation, applied.generation + 1);
    assert_eq!(canonical_state(&fixture.store), canonical);
    assert_eq!(
        fixture
            .store
            .active_installation_roots(fixture.provider)
            .unwrap(),
        std::slice::from_ref(&fixture.from)
    );
    let locations = table_state(&fixture.store.conn.borrow(), "installation_locations");
    let namespace_ids: BTreeSet<_> = locations
        .rows
        .iter()
        .map(|row| format!("{:?}", column(row, &locations, "namespace_id")))
        .collect();
    assert_eq!(
        namespace_ids.len(),
        1,
        "both locations point directly to the same namespace"
    );
    assert_eq!(locations.rows.len(), 2);
    assert_eq!(count(&fixture.store, "installation_relocations"), 2);
    assert_eq!(
        fixture
            .store
            .resolve_or_allocate_installation_namespace(
                fixture.provider,
                &source.batch.source_path,
                &legacy_installation_namespace(&source.batch.source_path, fixture.provider)
            )
            .unwrap(),
        source.namespace
    );
}

#[test]
fn alias_expiry_does_not_expire_canonical_ids_or_reuse_the_moved_namespace() {
    let mut fixture = Fixture::new("claude-code");
    let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
    fixture.commit(&[&source]);
    let graph = fixture
        .store
        .load_session_graph(&source.sessions[0])
        .unwrap();
    let destination = fixture.move_source(&source);
    let token = fixture.preview().plan.unwrap();
    fixture.apply(&token, "backup.sqlite").unwrap();
    let before = state(&fixture.store);
    let legacy = legacy_installation_namespace(&source.batch.source_path, fixture.provider);
    assert!(matches!(
        fixture.store.resolve_or_allocate_installation_namespace(
            fixture.provider,
            &source.batch.source_path,
            &legacy
        ),
        Err(PortError::InvalidRequest(_))
    ));
    fixture.store = fixture.store.with_relocation_clock(clock_expired_alias);
    let current = fixture
        .store
        .resolve_or_allocate_installation_namespace(
            fixture.provider,
            &destination,
            &legacy_installation_namespace(&destination, fixture.provider),
        )
        .unwrap();
    assert_eq!(current, source.namespace);
    let reused_location = fixture
        .store
        .resolve_or_allocate_installation_namespace(
            fixture.provider,
            &source.batch.source_path,
            &legacy,
        )
        .unwrap();
    assert_ne!(
        reused_location, source.namespace,
        "an expired physical location is a new installation"
    );
    assert_eq!(
        fixture
            .store
            .load_session_graph(&source.sessions[0])
            .unwrap(),
        graph
    );
    assert!(fixture.store.get(&source.sessions[0]).unwrap().is_some());
    assert_eq!(
        state(&fixture.store),
        before,
        "time and reservations never delete canonical entities or mutate the registry"
    );
}

fn removed_source(path: &str, provider: &str) -> SourceBatch {
    SourceBatch {
        source_path: path.into(),
        entries: Vec::new(),
        placements: Vec::new(),
        edges: Vec::new(),
        activities: Vec::new(),
        usage_events: Vec::new(),
        relation_complete: true,
        len_bytes: None,
        fingerprint: None,
        provider_id: Some(provider.into()),
        resume_claims: Vec::new(),
    }
}

#[test]
fn source_deletion_after_relocation_preserves_shared_facts_until_the_last_claim() {
    let fixture = Fixture::new("claude-code");
    let first = fixture.stage("sessions/a.jsonl", &["shared-session"]);
    let second = fixture.stage("sessions/b.jsonl", &["shared-session"]);
    fixture.commit(&[&first, &second]);
    assert_eq!(first.sessions, second.sessions);
    assert_eq!(count(&fixture.store, "usage_events"), 1);
    assert_eq!(count(&fixture.store, "tool_activities"), 1);
    let first_to = fixture.move_source(&first);
    let second_to = fixture.move_source(&second);
    let token = fixture.preview().plan.unwrap();
    fixture.apply(&token, "backup.sqlite").unwrap();
    fs::remove_file(&first_to).unwrap();
    assert!(
        fixture
            .store
            .commit_source_batches_if_changed(&[removed_source(&first_to, fixture.provider)])
            .unwrap()
    );
    assert!(fixture.store.get(&first.sessions[0]).unwrap().is_some());
    assert_eq!(count(&fixture.store, "usage_events"), 1);
    assert_eq!(count(&fixture.store, "tool_activities"), 1);
    assert_eq!(count(&fixture.store, "source_installations"), 1);
    assert!(fixture.store.resume_of(&first.sessions).unwrap()[0].resume_available);
    fs::remove_file(&second_to).unwrap();
    assert!(
        fixture
            .store
            .commit_source_batches_if_changed(&[removed_source(&second_to, fixture.provider)])
            .unwrap()
    );
    assert!(fixture.store.get(&first.sessions[0]).unwrap().is_none());
    assert_eq!(count(&fixture.store, "usage_events"), 0);
    assert_eq!(count(&fixture.store, "tool_activities"), 0);
    assert_eq!(count(&fixture.store, "source_installations"), 0);
    assert_eq!(
        count(&fixture.store, "installation_namespaces"),
        1,
        "identity registry is not a content tombstone"
    );
}

#[test]
fn several_parent_grouped_namespaces_keep_their_relative_roots() {
    let fixture = Fixture::new("cursor");
    let first = fixture.stage("workspace-a/state.jsonl", &["same-native"]);
    let second = fixture.stage("workspace-b/state.jsonl", &["same-native"]);
    assert_ne!(first.namespace, second.namespace);
    assert_ne!(first.sessions, second.sessions);
    fixture.commit(&[&first, &second]);
    let first_to = fixture.move_source(&first);
    let second_to = fixture.move_source(&second);
    let before = state(&fixture.store);
    let plan = fixture.preview();
    assert_eq!(
        (
            plan.source_count,
            plan.session_count,
            plan.installation_count,
            plan.namespace_count
        ),
        (2, 2, 2, 2)
    );
    fixture
        .apply(plan.plan.as_deref().unwrap(), "backup.sqlite")
        .unwrap();
    assert_locator_remapping(
        &before,
        &state(&fixture.store),
        &[
            (&first.batch.source_path, &first_to),
            (&second.batch.source_path, &second_to),
        ],
    );
    let expected_roots = [
        locator(Path::new(&first_to).parent().unwrap()),
        locator(Path::new(&second_to).parent().unwrap()),
    ];
    assert_eq!(
        fixture
            .store
            .active_installation_roots(fixture.provider)
            .unwrap(),
        expected_roots
    );
}

#[test]
fn unicode_roots_preserve_existing_ids_and_component_spelling() {
    let mut fixture = Fixture::new("codex");
    fixture.from = fixture.path("项目-é/原始/.codex");
    fixture.to = fixture.path("归档-e\u{301}/搬迁/.codex");
    let source = fixture.stage("sessions/会话.jsonl", &["session-a"]);
    fixture.commit(&[&source]);
    let canonical = canonical_state(&fixture.store);
    let destination = fixture.move_source(&source);
    let token = fixture.preview().plan.unwrap();
    fixture.apply(&token, "backup.sqlite").unwrap();
    assert_eq!(canonical_state(&fixture.store), canonical);
    assert_eq!(
        fixture
            .store
            .active_installation_roots(fixture.provider)
            .unwrap(),
        [fixture.to]
    );
    assert!(Path::new(&destination).exists());
    assert!(fixture.store.get(&source.sessions[0]).unwrap().is_some());
}

#[cfg(windows)]
#[test]
fn windows_case_separator_equivalence_does_not_duplicate_claims_or_generation() {
    let fixture = Fixture::new("claude-code");
    let source = fixture.stage("sessions/KeepCase.jsonl", &["session-a"]);
    fixture.commit(&[&source]);
    let before = state(&fixture.store);
    let alternate_root = fixture.from.to_ascii_uppercase().replace('/', "\\");
    let preview = fixture
        .store
        .relocation_preview(fixture.provider, &fixture.from, &alternate_root, TTL_DAYS)
        .unwrap();
    assert_eq!(preview.status, RelocationStatus::Unchanged);
    assert!(preview.plan.is_none());
    let alternate_source = source
        .batch
        .source_path
        .to_ascii_uppercase()
        .replace('/', "\\");
    let restaged = stage_existing(
        &fixture.store,
        &alternate_source,
        fixture.provider,
        &["session-a"],
    );
    assert_eq!(restaged.namespace, source.namespace);
    assert_eq!(restaged.sessions, source.sessions);
    assert!(
        !fixture
            .store
            .commit_source_batches_if_changed(&[restaged.batch])
            .unwrap()
    );
    assert_eq!(state(&fixture.store), before);
}

// Build the historical boundary using only an empty synthetic catalog. The
// fixture-only DDL is deliberately unavailable to production migration paths.
fn make_empty_v17_fixture(store: &SqliteStore) {
    for table in [
        "installation_relocations",
        "source_installations",
        "installation_locations",
        "installation_namespaces",
    ] {
        assert_eq!(count(store, table), 0);
    }
    store
        .conn
        .borrow()
        .execute_batch(
            "DROP TABLE installation_relocations;
         DROP TABLE source_installations;
         DROP TABLE installation_locations;
         DROP TABLE installation_namespaces;
         ALTER TABLE index_batches DROP COLUMN relocation_json;
         PRAGMA user_version=17;",
        )
        .unwrap();
}

fn legacy_store() -> SqliteStore {
    let store = SqliteStore::open_in_memory()
        .unwrap()
        .with_relocation_clock(clock_now);
    make_empty_v17_fixture(&store);
    store
}

fn seed_legacy_source(
    store: &SqliteStore,
    path: &str,
    provider: &str,
    native: &str,
    resolved: bool,
) -> PreparedSource {
    let bytes = format!("synthetic legacy source {native}\n");
    let snapshot = SourceSnapshot {
        path: path.into(),
        len: bytes.len() as u64,
        mtime_ms: NOW_MS,
        fingerprint: blake3::hash(bytes.as_bytes()).to_hex().to_string(),
    };
    let namespace = legacy_installation_namespace(path, provider);
    let source = batch_from_snapshot(path, provider, &namespace, &[native], &snapshot);
    let conn = store.conn.borrow();
    let tx = conn.unchecked_transaction().unwrap();
    let document = source
        .batch
        .entries
        .iter()
        .find(|(id, _, _)| id.kind() == IdKind::Document)
        .unwrap()
        .0
        .as_str();
    for (id, payload, _) in &source.batch.entries {
        tx.execute(
            "INSERT INTO catalog(id,payload) VALUES(?1,?2)",
            rusqlite::params![id.as_str(), payload],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO fts_ids(wire_id,id_json,fts_rowid) VALUES(?1,?2,NULL)",
            rusqlite::params![id.as_str(), serde_json::to_string(id).unwrap()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO source_membership(source_path,message_id,document_id) VALUES(?1,?2,?3)",
            rusqlite::params![path, id.as_str(), document],
        )
        .unwrap();
    }
    tx.execute("INSERT INTO source_scans(source_path,scanned_at_ms,len_bytes,fingerprint,provider_id,parser_version) VALUES(?1,?2,?3,?4,?5,2)",
        rusqlite::params![path, NOW_MS, snapshot.len as i64, snapshot.fingerprint, provider]).unwrap();
    if resolved {
        let claim = &source.batch.resume_claims[0];
        tx.execute(
            "INSERT INTO source_session_resume_claims(source_path,session_id,provider_id,provider_session_id,provider_session_id_state,
             original_working_directory,original_working_directory_state,pair_observed) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            rusqlite::params![path, claim.session_id, claim.provider_id, claim.provider_session_id, claim.provider_session_id_state,
                claim.original_working_directory, claim.original_working_directory_state, i64::from(claim.pair_observed)]
        ).unwrap();
    }
    tx.commit().unwrap();
    source
}

fn schema_state(conn: &Connection) -> Vec<(String, String, Option<String>)> {
    conn.prepare("SELECT type,name,sql FROM sqlite_schema ORDER BY type,name")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn schema_version(store: &SqliteStore) -> i64 {
    store
        .conn
        .borrow()
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn v17_migration_preserves_exact_legacy_namespace_ids_and_every_existing_table() {
    let store = legacy_store();
    let source = seed_legacy_source(
        &store,
        r"C:\RecordedCase\.claude\projects\session.jsonl",
        "claude-code",
        "native-a",
        true,
    );
    let before = state(&store);
    SqliteStore::migrate_v17_to_v18_inner(&store.conn.borrow(), false).unwrap();
    assert_eq!(schema_version(&store), 18);
    let after = state(&store);
    for (name, old) in &before {
        if name == "index_batches" {
            assert!(after[name].columns.starts_with(&old.columns));
            assert!(after[name].rows.is_empty());
        } else {
            assert_eq!(after[name], *old, "v18 migration must not rewrite {name}");
        }
    }
    assert_eq!(source.namespace, "claude-code:C:/RecordedCase/.claude");
    let assignment =
        SqliteStore::assignment_for_source(&store.conn.borrow(), &source.batch.source_path)
            .unwrap()
            .unwrap();
    assert_eq!(assignment.namespace_input, source.namespace);
    assert_eq!(assignment.origin, "legacy-v1");
    assert_eq!(assignment.root_key, "c:/recordedcase/.claude");
    assert_eq!(count(&store, "installation_namespaces"), 1);
    assert_eq!(count(&store, "source_installations"), 1);
    assert_eq!(
        SqliteStore::stable_id_from_store(&store.conn.borrow(), source.sessions[0].as_str())
            .unwrap(),
        source.sessions[0]
    );
    assert!(store.get(&source.sessions[0]).unwrap().is_some());
    let claim = store.resume_of(&source.sessions).unwrap().remove(0);
    assert_eq!(claim.provider_session_id.as_deref(), Some("native-a"));
    assert_eq!(
        claim.original_working_directory.as_deref(),
        Some("/synthetic/original-project")
    );
}

#[test]
fn v17_case_ambiguous_or_unverifiable_provenance_stays_unregistered_and_readable() {
    let store = legacy_store();
    let first = seed_legacy_source(
        &store,
        "C:/CaseRoot/.claude/a.jsonl",
        "claude-code",
        "native-a",
        true,
    );
    let second = seed_legacy_source(
        &store,
        "c:/caseroot/.claude/b.jsonl",
        "claude-code",
        "native-b",
        true,
    );
    let missing = seed_legacy_source(
        &store,
        "C:/Unresolved/.codex/session.jsonl",
        "codex",
        "native-c",
        false,
    );
    assert_ne!(first.namespace, second.namespace);
    let catalog = table_state(&store.conn.borrow(), "catalog");
    SqliteStore::migrate_v17_to_v18_inner(&store.conn.borrow(), false).unwrap();
    assert_eq!(count(&store, "installation_namespaces"), 0);
    assert_eq!(count(&store, "source_installations"), 0);
    assert_eq!(table_state(&store.conn.borrow(), "catalog"), catalog);
    for source in [&first, &second, &missing] {
        assert!(store.get(&source.sessions[0]).unwrap().is_some());
    }
    let destination = tempfile::tempdir().unwrap();
    assert!(matches!(
        store.relocation_preview(
            "claude-code",
            "C:/CaseRoot/.claude",
            &locator(destination.path()),
            TTL_DAYS
        ),
        Err(PortError::InvalidRequest(_))
    ));
    assert!(matches!(
        store.resolve_or_allocate_installation_namespace(
            "codex",
            &missing.batch.source_path,
            &missing.namespace
        ),
        Err(PortError::InvalidRequest(_))
    ));
}

#[test]
fn unbound_legacy_source_aliases_fail_before_namespace_allocation() {
    for recorded in [
        r"C:\RecordedCase\.claude\session.jsonl",
        r"\\server\share\RecordedCase\.claude\session.jsonl",
    ] {
        let store = SqliteStore::open_in_memory().unwrap();
        seed_legacy_source(&store, recorded, "claude-code", "native-a", false);
        store
            .conn
            .borrow()
            .execute(
                "UPDATE source_scans SET provider_id=NULL, len_bytes=NULL, fingerprint=NULL",
                [],
            )
            .unwrap();
        let before = state(&store);
        for input in [
            recorded.to_owned(),
            recorded.replace('\\', "/"),
            recorded.replace('\\', "/").to_ascii_lowercase(),
        ] {
            assert!(matches!(
                store.resolve_or_allocate_installation_namespace(
                    "claude-code",
                    &input,
                    &legacy_installation_namespace(&input, "claude-code"),
                ),
                Err(PortError::InvalidRequest(_))
            ));
            assert_eq!(state(&store), before);
            assert!(store.pending_installations.borrow().is_empty());
        }
    }
}

#[test]
fn ambiguous_unbound_legacy_source_keys_refuse_even_an_exact_locator() {
    let store = SqliteStore::open_in_memory().unwrap();
    let paths = [
        r"C:\RecordedCase\.claude\session.jsonl",
        "C:/RecordedCase/.claude/session.jsonl",
    ];
    for (path, native) in paths.iter().zip(["native-a", "native-b"]) {
        seed_legacy_source(&store, path, "claude-code", native, true);
    }
    let before = state(&store);
    for path in paths {
        assert!(matches!(
            store.resolve_or_allocate_installation_namespace(
                "claude-code",
                path,
                &legacy_installation_namespace(path, "claude-code"),
            ),
            Err(PortError::InvalidRequest(_))
        ));
        assert_eq!(state(&store), before);
        assert!(store.pending_installations.borrow().is_empty());
    }
}

#[test]
fn unbound_legacy_source_alias_refuses_registered_and_pending_root_reuse() {
    for registered in [false, true] {
        let store = SqliteStore::open_in_memory().unwrap();
        let recorded = r"C:\RecordedCase\.claude\session.jsonl";
        seed_legacy_source(&store, recorded, "claude-code", "native-a", false);
        store
            .conn
            .borrow()
            .execute(
                "UPDATE source_scans SET provider_id=NULL WHERE source_path=?1",
                [recorded],
            )
            .unwrap();
        let sibling = seed_legacy_source(
            &store,
            r"C:\RecordedCase\.claude\sibling.jsonl",
            "claude-code",
            "native-b",
            true,
        );
        assert_eq!(
            store
                .resolve_or_allocate_installation_namespace(
                    "claude-code",
                    &sibling.batch.source_path,
                    &sibling.namespace,
                )
                .unwrap(),
            sibling.namespace,
        );
        if registered {
            store
                .commit_source_batches_if_changed(&[sibling.batch])
                .unwrap();
        }
        let before = state(&store);
        let pending_keys: Vec<_> = store
            .pending_installations
            .borrow()
            .keys()
            .cloned()
            .collect();
        let alias = "c:/recordedcase/.claude/session.jsonl";
        assert!(matches!(
            store.resolve_or_allocate_installation_namespace(
                "claude-code",
                alias,
                &legacy_installation_namespace(alias, "claude-code"),
            ),
            Err(PortError::InvalidRequest(_))
        ));
        assert_eq!(state(&store), before);
        assert_eq!(
            store
                .pending_installations
                .borrow()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            pending_keys,
        );
    }
}

#[test]
fn exact_legacy_proof_preserves_ids_and_registered_source_aliases() {
    let store = SqliteStore::open_in_memory().unwrap();
    let recorded = r"C:\RecordedCase\.claude\session.jsonl";
    let source = seed_legacy_source(&store, recorded, "claude-code", "native-a", true);
    let before = state(&store);
    assert_eq!(
        store
            .resolve_or_allocate_installation_namespace("claude-code", recorded, &source.namespace,)
            .unwrap(),
        source.namespace,
    );
    assert_eq!(state(&store), before);
    assert!(
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source.batch))
            .unwrap()
    );
    let after = state(&store);
    let alias = "c:/recordedcase/.claude/session.jsonl";
    assert_eq!(
        store
            .resolve_or_allocate_installation_namespace(
                "claude-code",
                alias,
                &legacy_installation_namespace(alias, "claude-code"),
            )
            .unwrap(),
        source.namespace,
    );
    let mut restaged = source.batch;
    restaged.source_path = alias.into();
    assert!(!store.commit_source_batches_if_changed(&[restaged]).unwrap());
    assert_eq!(state(&store), after);
    for id in source.sessions {
        assert_eq!(
            SqliteStore::stable_id_from_store(&store.conn.borrow(), id.as_str()).unwrap(),
            id
        );
    }
}

#[test]
fn legacy_locator_guard_does_not_guess_posix_case_or_relative_paths() {
    for (recorded, input) in [
        (
            "/synthetic/CaseRoot/session.jsonl",
            "/synthetic/caseroot/session.jsonl",
        ),
        (
            "relative/session.jsonl",
            "/synthetic/relative/session.jsonl",
        ),
    ] {
        let store = SqliteStore::open_in_memory().unwrap();
        store
            .conn
            .borrow()
            .execute(
                "INSERT INTO source_scans(source_path,scanned_at_ms) VALUES(?1,0)",
                [recorded],
            )
            .unwrap();
        let before = state(&store);
        let namespace = store
            .resolve_or_allocate_installation_namespace(
                "claude-code",
                input,
                &legacy_installation_namespace(input, "claude-code"),
            )
            .unwrap();
        assert!(namespace.starts_with("installation-v1:"));
        assert_eq!(state(&store), before);
    }
}

#[test]
fn v18_migration_failure_rolls_back_schema_registry_and_legacy_data() {
    let store = legacy_store();
    seed_legacy_source(
        &store,
        "/synthetic/legacy/.codex/session.jsonl",
        "codex",
        "native-a",
        true,
    );
    let before = state(&store);
    let schema = schema_state(&store.conn.borrow());
    assert!(SqliteStore::migrate_v17_to_v18_inner(&store.conn.borrow(), true).is_err());
    assert_eq!(schema_version(&store), 17);
    assert_eq!(state(&store), before);
    assert_eq!(schema_state(&store.conn.borrow()), schema);
    SqliteStore::migrate_v17_to_v18_inner(&store.conn.borrow(), false).unwrap();
    assert_eq!(schema_version(&store), 18);
    assert_eq!(count(&store, "source_installations"), 1);
}

#[test]
fn read_open_of_v17_catalog_does_not_migrate_or_modify_it() {
    let fixture = Fixture::new("claude-code");
    make_empty_v17_fixture(&fixture.store);
    seed_legacy_source(
        &fixture.store,
        "/synthetic/legacy/.claude/session.jsonl",
        "claude-code",
        "native-a",
        true,
    );
    let before = state(&fixture.store);
    let schema = schema_state(&fixture.store.conn.borrow());
    assert!(matches!(
        SqliteStore::open(&fixture.db),
        Err(PortError::SchemaIncompatible(_))
    ));
    assert_eq!(schema_version(&fixture.store), 17);
    assert_eq!(state(&fixture.store), before);
    assert_eq!(schema_state(&fixture.store.conn.borrow()), schema);
}

#[test]
fn relocation_writer_does_not_reproject_stale_catalog_before_its_backup() {
    let fixture = Fixture::new("claude-code");
    let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
    fixture.commit(&[&source]);
    fixture.move_source(&source);
    fixture
        .store
        .conn
        .borrow()
        .execute("UPDATE store_metadata SET index_projection_version=0", [])
        .unwrap();
    let before = state(&fixture.store);
    let Fixture {
        store,
        db,
        from,
        to,
        provider,
        temporary,
    } = fixture;
    drop(store);
    let writer = SqliteStore::open_for_relocation(&db)
        .unwrap()
        .with_relocation_clock(clock_now);
    assert_eq!(
        state(&writer),
        before,
        "taking the relocation lease must not repair projections"
    );
    let plan = writer
        .relocation_preview(provider, &from, &to, TTL_DAYS)
        .unwrap();
    let backup = locator(&temporary.path().join("stale-projection-backup.sqlite"));
    writer
        .apply_relocation(
            provider,
            &from,
            &to,
            TTL_DAYS,
            plan.plan.as_deref().unwrap(),
            &backup,
        )
        .unwrap();
    let copied =
        Connection::open_with_flags(backup, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(
        connection_state(&copied),
        before,
        "backup must contain the original projection metadata and contents"
    );
}

fn stage_wal_source(fixture: &Fixture) -> (PreparedSource, Connection) {
    let path = locator(&Path::new(&fixture.from).join("sessions.sqlite"));
    let writer = Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
         CREATE TABLE conversations(native_id TEXT PRIMARY KEY, body TEXT NOT NULL);
         INSERT INTO conversations VALUES('wal-session-a','synthetic first conversation');
         PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
    let main_before = fs::read(&path).unwrap();
    writer
        .execute(
            "INSERT INTO conversations VALUES('wal-session-b','synthetic second conversation')",
            [],
        )
        .unwrap();
    assert_eq!(
        fs::read(&path).unwrap(),
        main_before,
        "second session must exist only in committed WAL frames"
    );
    assert!(Path::new(&format!("{path}-wal")).exists());
    let source = stage_existing(
        &fixture.store,
        &path,
        fixture.provider,
        &["wal-session-a", "wal-session-b"],
    );
    assert!(
        source
            .batch
            .fingerprint
            .as_deref()
            .unwrap()
            .starts_with("sqlite:")
    );
    (source, writer)
}

fn copy_wal_destination(
    fixture: &Fixture,
    source: &PreparedSource,
    writer: &Connection,
) -> (String, Connection) {
    let destination = fixture.destination(source);
    fs::create_dir_all(Path::new(&destination).parent().unwrap()).unwrap();
    writer
        .backup(rusqlite::MAIN_DB, Path::new(&destination), None)
        .unwrap();
    let target = Connection::open(&destination).unwrap();
    target.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let captured = capture(Path::new(&destination)).unwrap();
    assert_eq!(Some(captured.fingerprint), source.batch.fingerprint);
    (destination, target)
}

#[test]
fn multi_session_wal_source_preserves_all_claims_and_does_not_write_provider_files() {
    let fixture = Fixture::new("cursor");
    let (source, source_writer) = stage_wal_source(&fixture);
    fixture.commit(&[&source]);
    let (destination, _destination_writer) =
        copy_wal_destination(&fixture, &source, &source_writer);
    let main_before = fs::read(&source.batch.source_path).unwrap();
    let wal_path = format!("{}-wal", source.batch.source_path);
    let wal_before = fs::read(&wal_path).unwrap();
    let contexts: Vec<_> = source
        .sessions
        .iter()
        .map(|id| fixture.store.load_session_graph(id).unwrap())
        .collect();
    let resume = fixture.store.resume_of(&source.sessions).unwrap();
    let before = state(&fixture.store);
    let plan = fixture.preview();
    assert_eq!((plan.source_count, plan.session_count), (1, 2));
    fixture
        .apply(plan.plan.as_deref().unwrap(), "backup.sqlite")
        .unwrap();
    assert_locator_remapping(
        &before,
        &state(&fixture.store),
        &[(&source.batch.source_path, &destination)],
    );
    assert_eq!(fixture.store.resume_of(&source.sessions).unwrap(), resume);
    for (id, graph) in source.sessions.iter().zip(contexts) {
        assert_eq!(fixture.store.load_session_graph(id).unwrap(), graph);
    }
    assert_eq!(fs::read(&source.batch.source_path).unwrap(), main_before);
    assert_eq!(
        fs::read(&wal_path).unwrap(),
        wal_before,
        "provider WAL must not be checkpointed or rewritten"
    );
}

#[test]
fn wal_only_destination_change_invalidates_the_plan_without_touching_catalog() {
    let fixture = Fixture::new("cursor");
    let (source, writer) = stage_wal_source(&fixture);
    fixture.commit(&[&source]);
    let (destination, target) = copy_wal_destination(&fixture, &source, &writer);
    let token = fixture.preview().plan.unwrap();
    let before = state(&fixture.store);
    let main_before = fs::read(&destination).unwrap();
    target.execute("UPDATE conversations SET body='synthetic committed WAL edit' WHERE native_id='wal-session-b'", []).unwrap();
    assert_eq!(
        fs::read(&destination).unwrap(),
        main_before,
        "content mutation must be WAL-only"
    );
    assert!(matches!(
        fixture.apply(&token, "must-not-exist.sqlite"),
        Err(PortError::SnapshotChanged(_))
    ));
    assert_eq!(state(&fixture.store), before);
    assert!(!Path::new(&fixture.path("must-not-exist.sqlite")).exists());
}

#[test]
fn registry_receipt_or_activation_failure_also_rolls_back_every_live_table() {
    for trigger in [
        "CREATE TRIGGER synthetic_failure BEFORE UPDATE OF state ON installation_locations BEGIN SELECT RAISE(ABORT,'synthetic location failure'); END;",
        "CREATE TRIGGER synthetic_failure BEFORE INSERT ON installation_relocations BEGIN SELECT RAISE(ABORT,'synthetic receipt failure'); END;",
        "CREATE TRIGGER synthetic_failure BEFORE UPDATE OF state ON index_batches WHEN NEW.state='activated' AND NEW.relocation_json <> 'null' BEGIN SELECT RAISE(ABORT,'synthetic activation failure'); END;",
    ] {
        let fixture = Fixture::new("claude-code");
        let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
        fixture.commit(&[&source]);
        fixture.move_source(&source);
        let token = fixture.preview().plan.unwrap();
        fixture.store.conn.borrow().execute_batch(trigger).unwrap();
        let before = live_state(&fixture.store);
        assert!(fixture.apply(&token, "backup.sqlite").is_err());
        assert_eq!(
            live_state(&fixture.store),
            before,
            "locator, registry and generation activation share one transaction"
        );
        assert_eq!(fixture.store.interrupted_batch_count().unwrap(), 1);
    }
}

#[test]
fn review_receipt_rechecks_the_complete_selected_roots() {
    for destination_scope in [false, true] {
        for (registered, populated) in [(false, true), (true, true), (true, false)] {
            let fixture = Fixture::new("cursor");
            let empty = fixture.stage("empty-group/state.jsonl", &["deleted-session"]);
            fixture.commit(&[&empty]);
            assert!(
                fixture
                    .store
                    .commit_source_batches_if_changed(&[removed_source(
                        &empty.batch.source_path,
                        fixture.provider
                    )])
                    .unwrap()
            );
            let original = fixture.stage("group-a/state.jsonl", &["original-session"]);
            fixture.commit(&[&original]);
            fixture.move_source(&original);
            let token = fixture.preview().plan.unwrap();
            fixture.apply(&token, "first.sqlite").unwrap();

            let outside_path = fixture.path("unrelated/state.jsonl");
            fs::create_dir_all(Path::new(&outside_path).parent().unwrap()).unwrap();
            fs::write(&outside_path, b"synthetic unrelated source\n").unwrap();
            let outside = stage_existing(
                &fixture.store,
                &outside_path,
                fixture.provider,
                &["unrelated-session"],
            );
            fixture.commit(&[&outside]);
            let before_unrelated_replay = state(&fixture.store);
            assert_eq!(fixture.preview().status, RelocationStatus::Unchanged);
            assert_eq!(
                fixture
                    .apply(&token, "unrelated-replay.sqlite")
                    .unwrap()
                    .status,
                RelocationStatus::Unchanged,
                "a generation change outside both roots does not invalidate an exact receipt"
            );
            assert_eq!(state(&fixture.store), before_unrelated_replay);
            assert!(!Path::new(&fixture.path("unrelated-replay.sqlite")).exists());

            let root = if destination_scope {
                &fixture.to
            } else {
                &fixture.from
            };
            let added_path = locator(&Path::new(root).join("group-b/state.jsonl"));
            fs::create_dir_all(Path::new(&added_path).parent().unwrap()).unwrap();
            fs::write(&added_path, b"synthetic newly indexed sibling source\n").unwrap();
            let added = if registered {
                stage_existing(
                    &fixture.store,
                    &added_path,
                    fixture.provider,
                    &["added-session"],
                )
            } else {
                batch_from_snapshot(
                    &added_path,
                    fixture.provider,
                    &legacy_installation_namespace(&added_path, fixture.provider),
                    &["added-session"],
                    &capture(Path::new(&added_path)).unwrap(),
                )
            };
            fixture.commit(&[&added]);
            if !populated {
                assert!(
                    fixture
                        .store
                        .commit_source_batches_if_changed(&[removed_source(
                            &added_path,
                            fixture.provider
                        )])
                        .unwrap()
                );
            }
            let before = state(&fixture.store);
            assert!(
                matches!(
                    fixture.store.relocation_preview(
                        fixture.provider,
                        &fixture.from,
                        &fixture.to,
                        TTL_DAYS
                    ),
                    Err(PortError::InvalidRequest(_))
                ),
                "changed root ownership must not return an old unchanged receipt"
            );
            assert!(matches!(
                fixture.apply(&token, "must-not-exist.sqlite"),
                Err(PortError::GenerationMismatch(_))
            ));
            assert_eq!(state(&fixture.store), before);
            assert!(!Path::new(&fixture.path("must-not-exist.sqlite")).exists());
        }
    }
}

#[test]
fn review_relocation_can_replace_an_expired_alias_of_another_installation() {
    let mut fixture = Fixture::new("claude-code");
    let first = fixture.stage("sessions/a.jsonl", &["same-native"]);
    let other_root = fixture.path("independent/.claude");
    let other_path = locator(&Path::new(&other_root).join("sessions/b.jsonl"));
    fs::create_dir_all(Path::new(&other_path).parent().unwrap()).unwrap();
    fs::write(&other_path, b"synthetic independent installation\n").unwrap();
    let other = stage_existing(
        &fixture.store,
        &other_path,
        fixture.provider,
        &["same-native"],
    );
    fixture.commit(&[&first, &other]);
    assert_ne!(first.namespace, other.namespace);
    assert_ne!(first.sessions, other.sessions);
    let first_destination = fixture.move_source(&first);
    let first_token = fixture.preview().plan.unwrap();
    fixture.apply(&first_token, "first.sqlite").unwrap();
    let first_graph = fixture
        .store
        .load_session_graph(&first.sessions[0])
        .unwrap();
    let other_graph = fixture
        .store
        .load_session_graph(&other.sessions[0])
        .unwrap();
    let canonical = canonical_state(&fixture.store);

    let reused_path = locator(&Path::new(&fixture.from).join("sessions/b.jsonl"));
    fs::rename(&other_path, &reused_path).unwrap();
    let before = state(&fixture.store);
    assert!(matches!(
        fixture
            .store
            .relocation_preview(fixture.provider, &other_root, &fixture.from, TTL_DAYS),
        Err(PortError::InvalidRequest(_))
    ));
    assert_eq!(state(&fixture.store), before);
    fixture.store = fixture.store.with_relocation_clock(clock_expired_alias);
    let preview = fixture
        .store
        .relocation_preview(fixture.provider, &other_root, &fixture.from, TTL_DAYS)
        .unwrap();
    assert_eq!(preview.status, RelocationStatus::Planned);
    let generation = fixture.store.active_generation().unwrap();
    let result = fixture
        .store
        .apply_relocation(
            fixture.provider,
            &other_root,
            &fixture.from,
            TTL_DAYS,
            preview.plan.as_deref().unwrap(),
            &fixture.path("second.sqlite"),
        )
        .unwrap();
    assert_eq!(result.status, RelocationStatus::Applied);
    assert_eq!(result.generation, generation + 1);
    assert_eq!(canonical_state(&fixture.store), canonical);
    assert_locator_remapping(
        &before,
        &state(&fixture.store),
        &[
            (&first_destination, &first_destination),
            (&other_path, &reused_path),
        ],
    );
    assert_eq!(
        fixture
            .store
            .load_session_graph(&first.sessions[0])
            .unwrap(),
        first_graph
    );
    assert_eq!(
        fixture
            .store
            .load_session_graph(&other.sessions[0])
            .unwrap(),
        other_graph
    );
    let rebound = stage_existing(
        &fixture.store,
        &reused_path,
        fixture.provider,
        &["same-native"],
    );
    assert_eq!(rebound.namespace, other.namespace);
    assert_eq!(rebound.sessions, other.sessions);
    assert!(
        !fixture
            .store
            .commit_source_batches_if_changed(&[rebound.batch])
            .unwrap()
    );
}

#[test]
fn review_retired_location_does_not_block_another_provider() {
    let fixture = Fixture::new("cursor");
    let moved = fixture.stage("shared/a.jsonl", &["moved-session"]);
    let retained_path = locator(&Path::new(&fixture.from).join("shared/b.jsonl"));
    fs::write(&retained_path, b"synthetic retained provider source\n").unwrap();
    let retained = stage_existing(
        &fixture.store,
        &retained_path,
        "aider",
        &["retained-session"],
    );
    fixture.commit(&[&moved, &retained]);
    fixture.move_source(&moved);
    let token = fixture.preview().plan.unwrap();
    fixture.apply(&token, "backup.sqlite").unwrap();
    let moved_graph = fixture
        .store
        .load_session_graph(&moved.sessions[0])
        .unwrap();
    let before = state(&fixture.store);
    let unchanged = stage_existing(
        &fixture.store,
        &retained_path,
        "aider",
        &["retained-session"],
    );
    assert_eq!(unchanged.namespace, retained.namespace);
    assert!(
        !fixture
            .store
            .commit_source_batches_if_changed(&[unchanged.batch])
            .unwrap()
    );
    assert_eq!(state(&fixture.store), before);

    fs::write(
        &retained_path,
        b"synthetic retained provider source with appended content\n",
    )
    .unwrap();
    let changed = stage_existing(
        &fixture.store,
        &retained_path,
        "aider",
        &["retained-session"],
    );
    assert_eq!(changed.namespace, retained.namespace);
    assert_eq!(changed.sessions, retained.sessions);
    let generation = fixture.store.active_generation().unwrap();
    fixture.commit(&[&changed]);
    assert_eq!(fixture.store.active_generation().unwrap(), generation + 1);
    assert_eq!(
        fixture
            .store
            .load_session_graph(&moved.sessions[0])
            .unwrap(),
        moved_graph
    );
    assert!(fixture.store.get(&retained.sessions[0]).unwrap().is_some());
    assert!(matches!(
        fixture.store.resolve_or_allocate_installation_namespace(
            fixture.provider,
            &moved.batch.source_path,
            &legacy_installation_namespace(&moved.batch.source_path, fixture.provider),
        ),
        Err(PortError::InvalidRequest(_))
    ));
}

#[test]
fn review_explicit_scan_must_prove_all_unresolved_legacy_native_sessions() {
    let temporary = tempfile::tempdir().unwrap();
    let path = locator(&temporary.path().join(".codex/a.jsonl"));
    let second_path = locator(&temporary.path().join(".codex/b.jsonl"));
    let sibling_path = locator(&temporary.path().join(".codex/c.jsonl"));
    fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
    fs::write(&path, b"synthetic legacy source native-a\n").unwrap();
    fs::write(&sibling_path, b"synthetic legacy source native-c\n").unwrap();
    let store = legacy_store();
    let first = seed_legacy_source(&store, &path, "codex", "native-a", false);
    let second = seed_legacy_source(&store, &second_path, "codex", "native-b", false);
    let sibling = seed_legacy_source(&store, &sibling_path, "codex", "native-c", false);
    // A v17 source can own several Native Sessions without resume observations.
    store
        .conn
        .borrow()
        .execute(
            "UPDATE source_membership SET source_path=?1 WHERE source_path=?2",
            rusqlite::params![path, second_path],
        )
        .unwrap();
    store
        .conn
        .borrow()
        .execute(
            "DELETE FROM source_scans WHERE source_path=?1",
            [&second_path],
        )
        .unwrap();
    SqliteStore::migrate_v17_to_v18_inner(&store.conn.borrow(), false).unwrap();
    assert_eq!(count(&store, "installation_namespaces"), 0);
    assert_eq!(count(&store, "source_installations"), 0);
    let before = state(&store);
    let namespace = store
        .resolve_or_allocate_installation_namespace("codex", &path, &first.namespace)
        .unwrap();
    assert_eq!(namespace, first.namespace);
    assert_eq!(
        state(&store),
        before,
        "a provisional exact legacy seed is not a registration"
    );
    let snapshot = capture(Path::new(&path)).unwrap();
    for natives in [&["native-a"][..], &["native-a", "wrong-native"][..]] {
        let incomplete = batch_from_snapshot(&path, "codex", &namespace, natives, &snapshot);
        assert!(matches!(
            store.commit_source_batches_if_changed(&[incomplete.batch]),
            Err(PortError::InvalidRequest(_))
        ));
        assert_eq!(
            state(&store),
            before,
            "unproven membership cannot mutate any table or intent"
        );
    }
    let verified = batch_from_snapshot(
        &path,
        "codex",
        &namespace,
        &["native-a", "native-b"],
        &snapshot,
    );
    let mut no_claims = verified.batch.clone();
    no_claims.resume_claims.clear();
    assert!(matches!(
        store.commit_source_batches_if_changed(&[no_claims]),
        Err(PortError::InvalidRequest(_))
    ));
    assert_eq!(state(&store), before);
    assert!(
        store
            .commit_source_batches_if_changed(&[verified.batch])
            .unwrap()
    );
    assert_eq!(
        verified.sessions,
        [first.sessions[0].clone(), second.sessions[0].clone()]
    );
    assert_eq!(count(&store, "installation_namespaces"), 1);
    assert_eq!(count(&store, "source_installations"), 1);
    let reparsed_sibling = stage_existing(&store, &sibling_path, "codex", &["native-c"]);
    assert_eq!(reparsed_sibling.namespace, namespace);
    assert_eq!(reparsed_sibling.sessions, sibling.sessions);
    assert!(
        store
            .commit_source_batches_if_changed(&[reparsed_sibling.batch])
            .unwrap()
    );
    assert_eq!(count(&store, "installation_namespaces"), 1);
    assert_eq!(count(&store, "source_installations"), 2);
    for session in [
        &first.sessions[0],
        &second.sessions[0],
        &sibling.sessions[0],
    ] {
        assert!(store.get(session).unwrap().is_some());
        assert_eq!(
            SqliteStore::stable_id_from_store(&store.conn.borrow(), session.as_str()).unwrap(),
            *session
        );
    }
}

#[test]
fn review_source_change_during_activation_rolls_back_every_live_table() {
    let fixture = Fixture::new("codex");
    let source = fixture.stage("sessions/a.jsonl", &["session-a"]);
    fixture.commit(&[&source]);
    let destination = fixture.move_source(&source);
    let token = fixture.preview().plan.unwrap();
    fixture
        .store
        .conn
        .borrow()
        .create_scalar_function(
            "rewrite_synthetic_destination",
            0,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            move |_| {
                fs::write(
                    &destination,
                    b"synthetic destination changed during activation\n",
                )
                .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
                Ok(1)
            },
        )
        .unwrap();
    fixture.store.conn.borrow().execute_batch(
        "CREATE TRIGGER change_source_during_relocation AFTER UPDATE OF source_path ON source_scans
         WHEN NEW.source_path <> OLD.source_path BEGIN SELECT rewrite_synthetic_destination(); END;"
    ).unwrap();
    let before = live_state(&fixture.store);
    let generation = fixture.store.active_generation().unwrap();
    let intents = count(&fixture.store, "index_batches");
    assert!(matches!(
        fixture.apply(&token, "backup.sqlite"),
        Err(PortError::SnapshotChanged(_))
    ));
    assert_eq!(live_state(&fixture.store), before);
    assert_eq!(fixture.store.active_generation().unwrap(), generation);
    assert_eq!(count(&fixture.store, "index_batches"), intents + 1);
    assert_eq!(fixture.store.interrupted_batch_count().unwrap(), 1);
    assert!(Path::new(&fixture.path("backup.sqlite")).exists());
    fixture.store.recover_interrupted().unwrap();
    assert_eq!(live_state(&fixture.store), before);
    assert_eq!(fixture.store.interrupted_batch_count().unwrap(), 0);
}

//! SQLite 适配器：在单个 SQLite 文件上同时落实 `CatalogStore` 与 `SearchIndex`
//! 两个端口（见 ADR-0001：FTS5 单存主线）。
//!
//! 本 crate 是 hexagonal 架构里的 driven adapter——只依赖 domain + ports 的抽象，
//! 把端口契约翻译成具体的 SQLite/FTS5 SQL，绝不反向依赖 application。
//!
//! 共享纯策略：CJK n-gram transform（ADR-0007，单字 + bigram）与 RFC3339/ISO-8601
//! 时间戳解析按约定
//! 放在 application crate（`cjk` 模块 / `parse_search_instant`），由本 crate 在 FTS
//! 写入/查询两侧与时间过滤谓词的标量函数中调用（索引与查询必须共享同一 transform、
//! 过滤谓词与请求边界必须共享同一解析才能一致）；relocation 模块同样复用
//! Application 的纯路径/计划策略。SQL、文件读取及持久化仍只在适配器中。

mod cas;
mod lease;
mod relocation;
mod source_fs;
mod trace;

pub use cas::{cas_activate, read_current, write_current};
pub use lease::WriterLease;
pub use source_fs::{
    FileSource, SnapshotFs, capture, open_snapshot_source, read_verified, verify_snapshot,
};

use agent_session_grep_application::{bounded_index_text, fts_tokens_cjk, parse_search_instant};
use agent_session_grep_domain::{
    EvidenceSpan, IdKind, Message, MessageEdge, MessagePlacement, MessageRelation, PlacementId,
    Role, SessionContextGraph, SourceDocument, StableId, ToolActivity, UsageObservation,
};
use agent_session_grep_ports::{
    CatalogEntry, CatalogStore, ContextGraphStore, ContextStats, MessageContextCandidate,
    PortError, PortResult, ReadSnapshot, RepoTotals, ResumeClaimsStore, SearchFacets, SearchHit,
    SearchIndex, SearchQuery, SemanticIndex, SessionResumeMetadata, SidechainFacet,
    SourcePlacement, SourceResumeClaim, TOOL_ACTIVITY_TARGET_MAX_CHARS, UsageTotals,
};
use relocation::{InstallationAssignment, RelocationManifest};
use rusqlite::{Connection, OptionalExtension};
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Translate adapter failures into the stable port error vocabulary.
///
/// SQLite BUSY/LOCKED conditions are expected writer contention and therefore
/// retryable. The diagnostic is intentionally generic so backend paths or raw
/// SQLite messages cannot escape through the protocol boundary.
fn backend<E: std::fmt::Display + 'static>(e: E) -> PortError {
    let any = &e as &dyn Any;
    if any.downcast_ref::<rusqlite::Error>().is_some_and(|error| {
        matches!(
            error,
            rusqlite::Error::SqliteFailure(sqlite, _)
                if matches!(
                    sqlite.code,
                    rusqlite::ErrorCode::DatabaseBusy
                        | rusqlite::ErrorCode::DatabaseLocked
                )
        )
    }) {
        PortError::WriterBusy("SQLite storage is busy or locked by another writer".into())
    } else {
        PortError::Backend(e.to_string())
    }
}

/// 把一行里的非负 INTEGER 列读成 u64（schema CHECK 约束保证非负；
/// 损坏行 fail-closed 报错，绝不静默 wrap——usage 五桶专用）。
fn row_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(0);

/// 批量 `IN (...)` 查询的单块 id 上限。SQLite 的变量上限是 999（旧版）/
/// 32766（3.32+），一个大 batch 的 placement/entity 数远超此限，必须分块。
const BATCH_IN_CHUNK: usize = 500;
/// Session 元数据搜索投影（`session_fts.text`）单字段的字符上限（schema v11）。
const SESSION_SEARCH_FIELD_CHARS: usize = 4096;
/// 会话标题显示投影（`session_titles.title`）的字符上限（schema v13，
/// 借鉴清单 #6：≤80 字符，char 边界截断）。
const SESSION_TITLE_MAX_CHARS: usize = 80;

/// 批量 INSERT 每块行数。
///
/// 借鉴 hstry `bulk_insert_messages_in_tx`（MIT，
/// hstry/crates/hstry-core/src/db.rs:2990）：同一事务内用多行 VALUES 语句
/// 替代逐行 prepared execute，把往返次数从 N 压到 N/CHUNK。块内参数数
/// 不得超过 SQLite 默认的 SQLITE_MAX_VARIABLE_NUMBER（999）；本文件最宽的
/// 批量语句是 message_placements 的 8 列，8 × 100 = 800，与 hstry 的
/// `COLS * ROWS_PER_CHUNK <= 950` 编译期断言保持同一保守上限（db.rs:3058）。
const BULK_INSERT_ROWS_PER_CHUNK: usize = 100;
const _: () = assert!(8 * BULK_INSERT_ROWS_PER_CHUNK <= 950);

type PlacementInsertRow<'a> = (
    &'a str,
    &'a str,
    &'a str,
    &'a str,
    i64,
    i64,
    Option<i64>,
    Option<i64>,
);

/// 生成多行 VALUES 元组串：`rows` 个 `(?,?,...)`（每元组 `cols` 个占位符）。
fn multi_row_values(rows: usize, cols: usize) -> String {
    let mut sql = String::new();
    for row in 0..rows {
        if row > 0 {
            sql.push(',');
        }
        sql.push('(');
        for col in 0..cols {
            if col > 0 {
                sql.push(',');
            }
            sql.push('?');
        }
        sql.push(')');
    }
    sql
}

/// 生成 `IN (...)` 子句的占位符串：`count` 个逗号分隔的 `?`。
fn in_placeholders(count: usize) -> String {
    vec!["?"; count].join(",")
}

/// 把 id 列表切成不超过 [`BATCH_IN_CHUNK`] 的块（每块一个 `IN (...)` 查询）。
fn chunk_ids<T: AsRef<str>>(ids: &[T]) -> Vec<&[T]> {
    ids.chunks(BATCH_IN_CHUNK).collect()
}

fn unix_ms() -> PortResult<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(backend)?
        .as_millis();
    i64::try_from(millis).map_err(backend)
}

fn operation_id() -> PortResult<String> {
    let seq = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
    Ok(format!(
        "op_v1_{}_{}_{}",
        unix_ms()?,
        std::process::id(),
        seq
    ))
}

/// Batch-manifest digest 的后端域分隔串。**字节值已冻结**：它参与
/// `index_batches.operation_digest`，改动会与既有 data root 中已存摘要不符。
/// 与 [`INDEX_PROJECTION_VERSION`] 无关——后者是可演进的投影版本，本常量只是
/// 摘要域标签。
const INDEX_BATCH_DIGEST_DOMAIN: &[u8] = b"sqlite-fts5-v1";

/// 从存储的 catalog payload 投影出可检索正文——rebuild 的规范投影函数。
///
/// 约定：现代 ingest/sync 写入的是完整 JSON payload（`{"role":..,"text":..,..}`），
/// 先尝试解析 JSON 取 `text` 字段；解析失败再回退历史格式——payload 里首个制表符
/// 之前是 role 前缀、之后是消息正文，无制表符则整体即正文（切片期 `index` 命令写入的
/// 无前缀纯文本）。因此仅凭 catalog 即可无损重建 FTS 投影，无需依赖可能已损坏/丢失的
/// 旧 FTS 内容。
///
/// 投影统一施加 [`bounded_index_text`] 截断（借鉴清单 #3：ctx 文本保留策略，
/// [`agent_session_grep_application::MESSAGE_FTS_MAX_CHARS`]）：catalog payload 保留
/// provider 原文全文，FTS 投影有界——rebuild/merge/put 与直接写入路径必须产出
/// 同一有界文本，否则 current 判定两侧分叉、重同步幂等失效。
///
/// 已知限制：切片期 `index` 命令若写入本身含制表符的正文，历史格式投影会截断到首个
/// 制表符之后——该命令仅供切片期测试，真实数据均经 ingest/sync 以 JSON payload 写入。
fn searchable_text(payload: &[u8]) -> String {
    bounded_index_text(&message_body(payload))
}

/// Full canonical body: vector validity must not use the truncated FTS projection.
fn message_body(payload: &[u8]) -> String {
    // Modern shape: `{"role":...,"text":...,...}`. Indexing the raw JSON would
    // let structural tokens (`user`, `null`, `sessions`) match every message.
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) {
        if let Some(text) = value.get("text").and_then(serde_json::Value::as_str) {
            return text.to_owned();
        }
        // JSON that lacks a string `text` field must not fall back to
        // indexing the raw JSON (structural-token pollution). It carries no
        // searchable body.
        return String::new();
    }
    let text = String::from_utf8_lossy(payload);
    match text.split_once('\t') {
        Some((_role, body)) => body.to_owned(),
        None => text.into_owned(),
    }
}

fn hash_field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

/// Union two projections of the same message entity.
///
/// Claude Code copies a conversation's history into the new transcript when a
/// session is resumed or forked, so one message legitimately belongs to several
/// sessions. The `session` back-reference becomes a union (`sessions`, sorted,
/// with `session` kept as a single-value alias). The content projection
/// (`text`) is exempt from conflict authority too: a copy may carry a different
/// number of content blocks than the original (e.g. a truncated tool_result),
/// so the merged value deterministically keeps the longer projection and no
/// retrieved content is lost. `span`/`spans` are unioned keyed by contributing
/// document, and the contextual aliases (`parent`, `parent_native_id`,
/// `is_sidechain`, `seq`) may differ because they are regenerated from v7
/// relations once every contributing source is relation-complete. Only the
/// remaining stable fields must still agree byte-for-byte; a message whose
/// stable projection depends on which file it came from is a real
/// inconsistency and is still rejected.
fn merge_message_payloads(_wire: &str, left: &[u8], right: &[u8]) -> PortResult<Vec<u8>> {
    let parse = |bytes: &[u8]| -> PortResult<serde_json::Map<String, serde_json::Value>> {
        match serde_json::from_slice::<serde_json::Value>(bytes) {
            Ok(serde_json::Value::Object(map)) => Ok(map),
            // Slice-era rows hold bare text rather than canonical JSON. Those
            // cannot be reconciled field by field, so the conflict stands.
            // The two refusals below must stay distinguishable: the caller masks
            // the detail into `catalog_error`, and "an old row predates the
            // canonical payload" and "two sources disagree about a stable field"
            // need opposite remedies. Neither text may name an entity — a
            // provider native id is adopted verbatim into the wire id, so it is
            // untrusted content (see `conflicting_message_projections_are_still_
            // rejected`).
            _ => Err(PortError::Backend(
                "message has conflicting projections across sources \
                 (a stored projection is not canonical JSON — a pre-canonical row \
                 cannot be reconciled field by field)"
                    .into(),
            )),
        }
    };

    let left_map = parse(left)?;
    let right_map = parse(right)?;

    let mut sessions: BTreeSet<String> = BTreeSet::new();
    // Spans are keyed by contributing document because the same message text
    // sits at different byte offsets in each file that carries it. Keying by
    // document also makes a re-sync idempotent: the same file always maps to
    // the same entry rather than appending a duplicate.
    let mut spans: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for map in [&left_map, &right_map] {
        if let Some(session) = map.get("session").and_then(|v| v.as_str()) {
            sessions.insert(session.to_string());
        }
        if let Some(list) = map.get("sessions").and_then(|v| v.as_array()) {
            for entry in list {
                if let Some(session) = entry.as_str() {
                    sessions.insert(session.to_string());
                }
            }
        }
        if let Some(list) = map.get("spans").and_then(|v| v.as_array()) {
            for entry in list {
                let Some(document) = entry.get("document").and_then(|v| v.as_str()) else {
                    return Err(PortError::Backend(
                        "message has a span without a document reference".into(),
                    ));
                };
                let document = document.to_string();
                // 同一 document 的多次出现按字段联合（右侧覆盖左侧）：保留左侧
                // 独有的字段——最典型的是 v7 再生回写的 `placement_id`——使 merge
                // 结果与再生后的 stored 逐字节一致，重同步才能收敛为内容级 no-op；
                // 偏移更新仍生效（右侧的 start/end 覆盖左侧）。
                match spans.get(&document).cloned() {
                    Some(mut existing) if entry.is_object() && existing.is_object() => {
                        let existing_obj = existing.as_object_mut().expect("checked is_object");
                        for (key, value) in entry.as_object().expect("checked is_object") {
                            existing_obj.insert(key.clone(), value.clone());
                        }
                        spans.insert(document, existing);
                    }
                    _ => {
                        spans.insert(document, entry.clone());
                    }
                }
            }
        }
    }

    // A row written before spans carried document attribution has only the
    // singular `span`. It cannot be keyed by document, so it is kept verbatim as
    // the alias rather than dropped — losing it would silently downgrade the
    // evidence for that message from byte precision to unknown.
    let legacy_span = [&left_map, &right_map]
        .into_iter()
        .find_map(|map| map.get("span").filter(|value| value.is_object()).cloned());

    // Only stable Message fields are conflict authority. Contextual compatibility
    // aliases may differ and are regenerated from v7 relations once every known
    // contributing source is relation-complete.
    for (a, b) in [(&left_map, &right_map), (&right_map, &left_map)] {
        for (key, value) in a {
            if matches!(
                key.as_str(),
                "session"
                    | "sessions"
                    | "span"
                    | "spans"
                    | "parent"
                    | "parent_native_id"
                    | "is_sidechain"
                    | "seq"
                    | "text"
            ) {
                continue;
            }
            if b.get(key) != Some(value) {
                // Codex's old adapter stored the occurrence-local outer
                // envelope timestamp as the message timestamp; the current
                // adapter emits no stable timestamp (different occurrences
                // carry different envelope timestamps). Re-ingesting such a
                // source therefore compares a string against null for the
                // same stable message, which must not be a conflict: the
                // merged value is null (no stable timestamp exists). A
                // missing key is treated like an explicit null for this
                // convergence.
                if key == "timestamp"
                    && matches!(
                        (value, b.get(key)),
                        (serde_json::Value::String(_), Some(serde_json::Value::Null))
                            | (serde_json::Value::Null, Some(serde_json::Value::String(_)))
                            | (serde_json::Value::String(_), None)
                            | (serde_json::Value::Null, None)
                    )
                {
                    continue;
                }
                return Err(PortError::Backend(format!(
                    "message has conflicting projections across sources \
                     (stable field `{key}` differs)"
                )));
            }
        }
    }

    // Timestamp is occurrence-local for Codex: an old adapter wrote the outer
    // envelope timestamp, the current one emits none. When projections disagree
    // on it (string vs null), converge deterministically on null regardless of
    // which projection happens to be on the left. A missing key is treated like
    // an explicit null for this convergence (the conflict check above already
    // treats them the same), so (string, missing) also converges to null
    // instead of keeping the string.
    let timestamp_states: [Option<bool>; 2] =
        [&left_map, &right_map].map(|map| map.get("timestamp").map(serde_json::Value::is_null));
    let timestamp_converges_to_null = timestamp_states
        .iter()
        .any(|state| matches!(state, Some(false)))
        && timestamp_states
            .iter()
            .any(|state| state.is_none_or(|is_null| is_null));

    let mut merged = left_map;
    if timestamp_converges_to_null {
        merged.insert("timestamp".to_string(), serde_json::Value::Null);
    }
    // `text` is a content projection, not a stable identity field: Claude Code
    // copies a conversation's history into the new transcript when a session is
    // resumed or forked, and a copy may carry a different number of content
    // blocks than the original (e.g. a truncated tool_result). The projections
    // legitimately differ, so text is exempt from conflict authority; the
    // merged value deterministically keeps the longer projection so no
    // retrieved content is lost.
    match (&merged.get("text"), right_map.get("text")) {
        (Some(left_text), Some(right_text)) => {
            let left_len = left_text.as_str().map_or(0, |s| s.len());
            let right_len = right_text.as_str().map_or(0, |s| s.len());
            if right_len > left_len {
                merged.insert("text".to_string(), right_text.clone());
            }
        }
        (None, Some(right_text)) => {
            merged.insert("text".to_string(), right_text.clone());
        }
        _ => {}
    }
    let sessions: Vec<String> = sessions.into_iter().collect();
    merged.insert(
        "session".to_string(),
        sessions
            .first()
            .cloned()
            .map_or(serde_json::Value::Null, serde_json::Value::String),
    );
    merged.insert("sessions".to_string(), serde_json::json!(sessions));

    let spans: Vec<serde_json::Value> = spans.into_values().collect();
    // `span` stays as a single-value alias holding the first contributing
    // document's offsets, so evidence assembly written against the pre-union
    // shape keeps reporting byte precision. On a message shared by several
    // files it names one location, not all of them.
    merged.insert(
        "span".to_string(),
        match spans.first() {
            Some(first) => serde_json::json!({
                "start": first.get("start").cloned().unwrap_or(serde_json::Value::Null),
                "end": first.get("end").cloned().unwrap_or(serde_json::Value::Null),
            }),
            None => legacy_span.unwrap_or(serde_json::Value::Null),
        },
    );
    merged.insert("spans".to_string(), serde_json::json!(spans));
    serde_json::to_vec(&serde_json::Value::Object(merged)).map_err(backend)
}

/// Check every non-null original observation before pairwise folding can hide
/// a disagreement behind null. Only the proven Cursor ItemTable caller uses this
/// legacy integer-milliseconds interpretation; raw source evidence is untouched.
fn cursor_itemtable_timestamp_alias<'a>(
    payloads: impl IntoIterator<Item = &'a [u8]>,
) -> PortResult<Option<String>> {
    let mut expected = None;
    let mut canonical = None;
    for payload in payloads {
        let value: serde_json::Value = serde_json::from_slice(payload).map_err(backend)?;
        let Some(timestamp) = value.get("timestamp").filter(|value| !value.is_null()) else {
            continue;
        };
        let timestamp = timestamp.as_str().ok_or_else(|| {
            PortError::Backend("Cursor ItemTable timestamp observation is not a string".into())
        })?;
        let instant = if let Ok(millis) = timestamp.parse::<i64>() {
            (
                millis.div_euclid(1000),
                (millis.rem_euclid(1000) as u32) * 1_000_000,
            )
        } else {
            let instant = parse_search_instant(timestamp).ok_or_else(|| {
                PortError::Backend(
                    "Cursor ItemTable timestamp observation has an unproven format".into(),
                )
            })?;
            canonical.get_or_insert_with(|| timestamp.to_string());
            (instant.unix_seconds, instant.nanosecond)
        };
        if expected.is_some_and(|previous| previous != instant) {
            return Err(PortError::Backend(
                "message has conflicting projections across sources (stable field `timestamp` differs)".into()));
        }
        expected = Some(instant);
    }
    Ok(canonical)
}

/// Use an actually observed RFC3339 alias for the aggregate only. Never
/// overwrite the raw per-source observation or keep a removed claimant's alias.
fn cursor_aggregate_payload<'a>(
    payload: &'a [u8],
    canonical: Option<&str>,
) -> PortResult<std::borrow::Cow<'a, [u8]>> {
    let Some(canonical) = canonical else {
        return Ok(std::borrow::Cow::Borrowed(payload));
    };
    let mut value: serde_json::Value = serde_json::from_slice(payload).map_err(backend)?;
    if value
        .get("timestamp")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|timestamp| timestamp.parse::<i64>().is_ok())
    {
        value["timestamp"] = serde_json::json!(canonical);
        Ok(std::borrow::Cow::Owned(
            serde_json::to_vec(&value).map_err(backend)?,
        ))
    } else {
        Ok(std::borrow::Cow::Borrowed(payload))
    }
}

/// Union two projections of the same session container entity.
///
/// One logical session is routinely split across many transcript files, so each
/// source contributes only the members it actually carries. Merging appends the
/// right side's new members after the left side's and unions the contributing
/// documents. Both inputs must be canonical session JSON; a malformed stored
/// payload is a real inconsistency and is reported rather than silently
/// discarded.
///
/// Determinism comes from the caller: `sync` rejects duplicate source paths and
/// the CLI passes sources in a fixed order, so the same corpus yields the same
/// merged bytes and an unchanged re-sync still registers as a content-level
/// no-op.
fn merge_session_payloads(_wire: &str, left: &[u8], right: &[u8]) -> PortResult<Vec<u8>> {
    fn parse(bytes: &[u8]) -> PortResult<serde_json::Value> {
        serde_json::from_slice(bytes).map_err(|error| {
            PortError::Backend(format!("session payload is not canonical JSON: {error}"))
        })
    }

    let left_value = parse(left)?;
    let right_value = parse(right)?;

    // Member order is load-bearing: readers treat a member's position in this
    // array as its in-session sequence number, and branch selection picks the
    // highest-sequence non-sidechain leaf. Sorting by wire id would therefore
    // scramble conversation order for real provider-native ids, so the union is
    // append-only. `documents` carries no such meaning and is sorted.
    let mut members: Vec<String> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut documents: BTreeSet<String> = BTreeSet::new();

    for value in [&left_value, &right_value] {
        // `document` (single) is the pre-union shape; `documents` (array) is what
        // a merged payload carries. Accept both so a store written by an older
        // binary merges cleanly instead of losing its attribution.
        if let Some(document) = value.get("document").and_then(|v| v.as_str()) {
            documents.insert(document.to_string());
        }
        if let Some(list) = value.get("documents").and_then(|v| v.as_array()) {
            for entry in list {
                if let Some(document) = entry.as_str() {
                    documents.insert(document.to_string());
                }
            }
        }
        let list = value
            .get("messages")
            .and_then(|v| v.as_array())
            .ok_or_else(|| PortError::Backend("session payload lacks a messages array".into()))?;
        for entry in list {
            let member = entry.as_str().ok_or_else(|| {
                PortError::Backend("session has a non-string message member".into())
            })?;
            if seen.insert(member.to_string()) {
                members.push(member.to_string());
            }
        }
    }

    let documents: Vec<String> = documents.into_iter().collect();
    let merged = serde_json::json!({
        // `document` stays as a single-value alias for the first contributing
        // document so readers written against the pre-union shape keep working.
        // On a multi-document session it names one contributor, not all of them.
        "document": documents.first().cloned(),
        "documents": documents,
        "messages": members,
    });
    serde_json::to_vec(&merged).map_err(backend)
}

/// One relation row to insert or replace in a relation-aware batch.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RelationUpsertManifest {
    Placement(MessagePlacement),
    Edge(MessageEdge),
    Activity(StoredActivity),
    Usage(StoredUsage),
}

impl RelationUpsertManifest {
    fn canonical_key(&self) -> String {
        match self {
            Self::Placement(placement) => format!("placement:{}", placement.id.as_str()),
            Self::Edge(edge) => format!("edge:{}", edge.child_placement_id.as_str()),
            Self::Activity(activity) => format!("activity:{}", activity.activity_id),
            Self::Usage(usage) => format!("usage:{}", usage.usage_id),
        }
    }

    fn canonical_value(&self) -> serde_json::Value {
        match self {
            Self::Placement(placement) => serde_json::json!({
                "kind": "message_placement",
                "placement": placement,
            }),
            Self::Edge(edge) => serde_json::json!({
                "kind": "message_edge",
                "edge": edge,
            }),
            Self::Activity(activity) => serde_json::json!({
                "kind": "tool_activity",
                "activity": {
                    "activity_id": activity.activity_id,
                    "message_id": activity.message_id,
                    "kind": activity.kind,
                    "actor": activity.actor,
                    "name": activity.name,
                    "target": activity.target,
                    "status": activity.status,
                },
            }),
            Self::Usage(usage) => serde_json::json!({
                "kind": "usage_event",
                "usage": {
                    "usage_id": usage.usage_id,
                    "session_id": usage.session_id,
                    "message_id": usage.message_id,
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cache_read_tokens": usage.cache_read_tokens,
                    "cache_write_tokens": usage.cache_write_tokens,
                    "reasoning_tokens": usage.reasoning_tokens,
                    "token_source": usage.token_source,
                },
            }),
        }
    }
}

/// One relation row to delete in a relation-aware batch.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RelationDeleteManifest {
    Placement(PlacementId),
    Edge(PlacementId),
    Activity(String),
    Usage(String),
}

impl RelationDeleteManifest {
    fn canonical_key(&self) -> String {
        match self {
            Self::Placement(id) => format!("placement:{}", id.as_str()),
            Self::Edge(id) => format!("edge:{}", id.as_str()),
            Self::Activity(activity_id) => format!("activity:{activity_id}"),
            Self::Usage(usage_id) => format!("usage:{usage_id}"),
        }
    }

    fn canonical_value(&self) -> serde_json::Value {
        match self {
            Self::Placement(id) => serde_json::json!({
                "kind": "message_placement",
                "placement_id": id,
            }),
            Self::Edge(id) => serde_json::json!({
                "kind": "message_edge",
                "child_placement_id": id,
            }),
            Self::Activity(activity_id) => serde_json::json!({
                "kind": "tool_activity",
                "activity_id": activity_id,
            }),
            Self::Usage(usage_id) => serde_json::json!({
                "kind": "usage_event",
                "usage_id": usage_id,
            }),
        }
    }
}

/// One durable source-to-entity membership row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SourceEntityMembershipManifest {
    entity_id: String,
    document_id: Option<String>,
}

impl SourceEntityMembershipManifest {
    fn canonical_value(&self) -> serde_json::Value {
        serde_json::json!({
            "entity_id": self.entity_id,
            "document_id": self.document_id,
        })
    }
}

/// Context fields derived from relation evidence, not intrinsic entity content.
fn compatibility_keys(kind: IdKind) -> &'static [&'static str] {
    match kind {
        IdKind::Message => &[
            "session",
            "sessions",
            "span",
            "spans",
            "parent",
            "parent_native_id",
            "is_sidechain",
            "seq",
        ],
        IdKind::Session => &["document", "documents", "messages"],
        _ => &[],
    }
}

/// Compare intrinsic content without making the historical aggregate a claimant.
fn intrinsic_payloads_equal(kind: IdKind, left: &[u8], right: &[u8]) -> bool {
    if left == right {
        return true;
    }
    let (Ok(serde_json::Value::Object(mut left)), Ok(serde_json::Value::Object(mut right))) =
        (serde_json::from_slice(left), serde_json::from_slice(right))
    else {
        return false;
    };
    for key in compatibility_keys(kind) {
        left.remove(*key);
        right.remove(*key);
    }
    left == right
}

/// Original observed identity, payload bytes, and indexed text for one source.
type SourceProjection = (StableId, Vec<u8>, String);
type SourceProjections = BTreeMap<String, SourceProjection>;

/// Complete source-scoped state after applying one scan.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceReplacementManifest {
    source_path: String,
    entity_memberships: Vec<SourceEntityMembershipManifest>,
    /// Original observations only. Missing members explicitly lack legacy evidence.
    projections: SourceProjections,
    placement_ids: Vec<PlacementId>,
    /// 该源声明的工具活动 id（v12；按 activity_id 排序去重）。
    activity_ids: Vec<String>,
    /// 该源声明的 token 用量事件 id（v15；按 usage_id 排序去重）。
    usage_ids: Vec<String>,
    relation_complete: bool,
    /// 捕获时源字节长度与内容指纹（source-scan 指纹缓存）。
    len_bytes: Option<i64>,
    fingerprint: Option<String>,
    /// 该 source 的 provider id；写入 `source_scans.provider_id` 供 discover diff。
    provider_id: Option<String>,
    /// Source-scoped Resume Metadata 声明（ADR-0009）：随本 source replacement
    /// 同事务原子写入；空列表表示清除该 source 的旧声明。
    resume_claims: Vec<SourceResumeClaim>,
    installation: Option<InstallationAssignment>,
}

impl SourceReplacementManifest {
    fn canonical_value(&self) -> serde_json::Value {
        let mut entity_memberships = self.entity_memberships.clone();
        entity_memberships.sort();
        let entity_memberships: Vec<_> = entity_memberships
            .iter()
            .map(SourceEntityMembershipManifest::canonical_value)
            .collect();
        let mut placement_ids = self.placement_ids.clone();
        placement_ids.sort();
        let mut activity_ids = self.activity_ids.clone();
        activity_ids.sort();
        let mut usage_ids = self.usage_ids.clone();
        usage_ids.sort();
        let mut claims: Vec<_> = self.resume_claims.iter().collect();
        claims.sort_by_key(|claim| &claim.session_id);
        let claims: Vec<_> = claims.into_iter().map(resume_claim_value).collect();
        // The outbox seals evidence; it is not a replay copy of transcript
        // bodies. Labels distinguish the two byte domains, and the full typed
        // identity remains part of the descriptor sealed by the batch digest.
        let projections: BTreeMap<_, _> = self
            .projections
            .iter()
            .map(|(wire, (id, payload, text))| {
                (
                    wire,
                    serde_json::json!({
                        "id": id,
                        "payload_blake3": blake3::hash(payload).to_hex().to_string(),
                        "text_blake3": blake3::hash(text.as_bytes()).to_hex().to_string(),
                    }),
                )
            })
            .collect();
        let mut value = serde_json::json!({
            "source_path": self.source_path,
            "entity_memberships": entity_memberships,
            "projections": projections,
            "placement_ids": placement_ids,
            "activity_ids": activity_ids,
            "usage_ids": usage_ids,
            "relation_complete": self.relation_complete,
            "len_bytes": self.len_bytes,
            "fingerprint": self.fingerprint,
            "provider_id": self.provider_id,
            "resume_claims": claims,
        });
        if let Some(installation) = &self.installation {
            value["installation"] = installation.canonical_value();
        }
        value
    }
}

/// Canonical JSON value of one [`SourceResumeClaim`]（含入 source replacement
/// 的 durable manifest：声明随 durable intent 一起哈希，改写声明即改写 digest）。
fn resume_claim_value(claim: &SourceResumeClaim) -> serde_json::Value {
    serde_json::json!({
        "provider_id": claim.provider_id,
        "session_id": claim.session_id,
        "provider_session_id": claim.provider_session_id,
        "provider_session_id_state": claim.provider_session_id_state,
        "original_working_directory": claim.original_working_directory,
        "original_working_directory_state": claim.original_working_directory_state,
        "pair_observed": claim.pair_observed,
    })
}

/// 持久化的 resume claim 行（`source_session_resume_claims` 表，ADR-0009）。
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredResumeClaim {
    session_id: String,
    provider_id: String,
    provider_session_id: Option<String>,
    provider_session_id_state: String,
    original_working_directory: Option<String>,
    original_working_directory_state: String,
    pair_observed: bool,
}

impl StoredResumeClaim {
    fn from_claim(claim: &SourceResumeClaim) -> Self {
        Self {
            session_id: claim.session_id.clone(),
            provider_id: claim.provider_id.clone(),
            provider_session_id: claim.provider_session_id.clone(),
            provider_session_id_state: claim.provider_session_id_state.clone(),
            original_working_directory: claim.original_working_directory.clone(),
            original_working_directory_state: claim.original_working_directory_state.clone(),
            pair_observed: claim.pair_observed,
        }
    }
}

/// Read all per-session resume claims for one source in deterministic order.
fn stored_resume_claim(
    conn: &Connection,
    source_path: &str,
) -> PortResult<BTreeMap<String, StoredResumeClaim>> {
    let mut stmt = conn
        .prepare(
            "SELECT session_id, provider_id, provider_session_id, provider_session_id_state,
                original_working_directory, original_working_directory_state, pair_observed
         FROM source_session_resume_claims WHERE source_path = ?1 ORDER BY session_id",
        )
        .map_err(backend)?;
    let rows = stmt
        .query_map([source_path], |row| {
            let claim = StoredResumeClaim {
                session_id: row.get(0)?,
                provider_id: row.get(1)?,
                provider_session_id: row.get(2)?,
                provider_session_id_state: row.get(3)?,
                original_working_directory: row.get(4)?,
                original_working_directory_state: row.get(5)?,
                pair_observed: row.get(6)?,
            };
            Ok((claim.session_id.clone(), claim))
        })
        .map_err(backend)?;
    rows.collect::<Result<_, _>>().map_err(backend)
}

/// 把一条声明行解析为固定形状的 [`SessionResumeMetadata`]（fail closed）：
/// 只有 `provider_session_id_state == "resolved"` 且有值才算可恢复；missing/
/// ambiguous/无值一律不可恢复并给出简短 reason。字段取值以 state 为准——
/// 未 resolved 的字段即使携带值也按 None 输出，绝不把歧义值当权威值。
fn resume_metadata_from_claim(id: &StableId, claim: &StoredResumeClaim) -> SessionResumeMetadata {
    let available =
        claim.provider_session_id_state == "resolved" && claim.provider_session_id.is_some();
    let (provider_session_id, original_working_directory, unavailable_reason) = if available {
        let directory =
            if claim.original_working_directory_state == "resolved" && claim.pair_observed {
                claim.original_working_directory.clone()
            } else {
                None
            };
        (claim.provider_session_id.clone(), directory, None)
    } else {
        let reason = match claim.provider_session_id_state.as_str() {
            "missing" => "provider session id not observed",
            "ambiguous" => "ambiguous provider session id",
            "resolved" => "provider session id value missing",
            _ => "provider session id state unresolved",
        };
        (None, None, Some(reason.into()))
    };
    SessionResumeMetadata {
        session_id: id.clone(),
        provider_id: Some(claim.provider_id.clone()),
        resume_available: available,
        provider_session_id,
        original_working_directory,
        unavailable_reason,
    }
}

/// Canonical relation/source manifests stored beside the entity manifest.
#[derive(Debug, Clone, Default)]
struct RelationManifests {
    relation_upserts: Vec<RelationUpsertManifest>,
    relation_deletes: Vec<RelationDeleteManifest>,
    source_replacements: Vec<SourceReplacementManifest>,
    relocation: Option<RelocationManifest>,
}

impl RelationManifests {
    fn validate(&self) -> PortResult<()> {
        if let Some(relocation) = &self.relocation {
            if !self.relation_upserts.is_empty()
                || !self.relation_deletes.is_empty()
                || !self.source_replacements.is_empty()
            {
                return Err(PortError::Backend(
                    "relocation cannot include source replacements".into(),
                ));
            }
            relocation.validate()?;
        }
        for upsert in &self.relation_upserts {
            match upsert {
                RelationUpsertManifest::Placement(placement) => {
                    validate_placement(placement)?;
                }
                RelationUpsertManifest::Edge(edge) => validate_edge(edge)?,
                // StoredActivity/StoredUsage 在构造（stored_activity_from /
                // stored_usage_from）时已完成领域校验与边界截断；此处只做清单
                // 结构校验（键唯一等）。
                RelationUpsertManifest::Activity(_) | RelationUpsertManifest::Usage(_) => {}
            }
        }
        let mut upsert_keys: Vec<_> = self
            .relation_upserts
            .iter()
            .map(RelationUpsertManifest::canonical_key)
            .collect();
        upsert_keys.sort();
        if upsert_keys.windows(2).any(|window| window[0] == window[1]) {
            return Err(PortError::Backend(
                "index batch contains duplicate relation upserts".into(),
            ));
        }

        let mut delete_keys: Vec<_> = self
            .relation_deletes
            .iter()
            .map(RelationDeleteManifest::canonical_key)
            .collect();
        delete_keys.sort();
        if delete_keys.windows(2).any(|window| window[0] == window[1]) {
            return Err(PortError::Backend(
                "index batch contains duplicate relation deletes".into(),
            ));
        }
        if upsert_keys
            .iter()
            .any(|key| delete_keys.binary_search(key).is_ok())
        {
            return Err(PortError::Backend(
                "index batch cannot upsert and delete the same relation".into(),
            ));
        }

        let mut source_paths: Vec<_> = self
            .source_replacements
            .iter()
            .map(|replacement| replacement.source_path.as_str())
            .collect();
        source_paths.sort_unstable();
        if source_paths.windows(2).any(|window| window[0] == window[1]) {
            return Err(PortError::Backend(
                "index batch contains duplicate source replacements".into(),
            ));
        }

        for replacement in &self.source_replacements {
            let mut entity_ids: Vec<_> = replacement
                .entity_memberships
                .iter()
                .map(|membership| membership.entity_id.as_str())
                .collect();
            entity_ids.sort_unstable();
            if entity_ids.windows(2).any(|window| window[0] == window[1]) {
                return Err(PortError::Backend(
                    "source replacement contains duplicate entity memberships".into(),
                ));
            }
            for membership in &replacement.entity_memberships {
                StableId::from_wire(&membership.entity_id).ok_or_else(|| {
                    PortError::Backend("source replacement has an invalid entity id".into())
                })?;
                if let Some(document_id) = &membership.document_id {
                    let document_id = StableId::from_wire(document_id).ok_or_else(|| {
                        PortError::Backend("source replacement has an invalid document id".into())
                    })?;
                    if document_id.kind() != IdKind::Document {
                        return Err(PortError::Backend(
                            "source membership document id has wrong kind".into(),
                        ));
                    }
                }
            }

            let mut placement_ids = replacement.placement_ids.clone();
            placement_ids.sort();
            if placement_ids
                .windows(2)
                .any(|window| window[0] == window[1])
            {
                return Err(PortError::Backend(
                    "source replacement contains duplicate placement claims".into(),
                ));
            }

            let mut activity_ids = replacement.activity_ids.clone();
            activity_ids.sort();
            if activity_ids.windows(2).any(|window| window[0] == window[1]) {
                return Err(PortError::Backend(
                    "source replacement contains duplicate activity claims".into(),
                ));
            }

            for claim in &replacement.resume_claims {
                let claim_session = StableId::from_wire(&claim.session_id).ok_or_else(|| {
                    PortError::Backend(
                        "source replacement has an invalid resume claim session id".into(),
                    )
                })?;
                if claim_session.kind() != IdKind::Session {
                    return Err(PortError::Backend(
                        "source resume claim session id has wrong kind".into(),
                    ));
                }
                if !replacement
                    .entity_memberships
                    .iter()
                    .any(|membership| membership.entity_id == claim.session_id)
                {
                    return Err(PortError::Backend(
                        "source resume claim session does not belong to the source replacement"
                            .into(),
                    ));
                }
                if claim.provider_id.is_empty() {
                    return Err(PortError::Backend(
                        "source resume claim has an empty provider id".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn canonical_json(&self) -> PortResult<(String, String, String)> {
        self.validate()?;
        let mut upserts: Vec<_> = self.relation_upserts.iter().collect();
        upserts.sort_by_key(|item| item.canonical_key());
        let upserts: Vec<_> = upserts
            .into_iter()
            .map(RelationUpsertManifest::canonical_value)
            .collect();

        let mut deletes: Vec<_> = self.relation_deletes.iter().collect();
        deletes.sort_by_key(|item| item.canonical_key());
        let deletes: Vec<_> = deletes
            .into_iter()
            .map(RelationDeleteManifest::canonical_value)
            .collect();

        let mut replacements: Vec<_> = self.source_replacements.iter().collect();
        replacements.sort_by(|left, right| left.source_path.cmp(&right.source_path));
        let replacements: Vec<_> = replacements
            .into_iter()
            .map(SourceReplacementManifest::canonical_value)
            .collect();

        Ok((
            serde_json::to_string(&upserts).map_err(backend)?,
            serde_json::to_string(&deletes).map_err(backend)?,
            serde_json::to_string(&replacements).map_err(backend)?,
        ))
    }
}

/// Canonical durable representation of one generation change set.
struct CanonicalBatchManifest {
    upsert_ids: Vec<String>,
    delete_ids: Vec<String>,
    relation_upserts_json: String,
    relation_deletes_json: String,
    source_replacements_json: String,
    relocation_json: String,
    operation_digest: String,
}

/// Canonicalize and fingerprint one generation change set.
///
/// Sorting by wire ID makes the digest independent of discovery order. Duplicate IDs and
/// upsert/delete overlap are rejected so the journal always describes an unambiguous set.
fn batch_manifest(
    upserts: &[(StableId, Vec<u8>, String)],
    deletes: &[StableId],
    relations: &RelationManifests,
) -> PortResult<CanonicalBatchManifest> {
    let mut ordered_upserts: Vec<_> = upserts.iter().collect();
    ordered_upserts.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    let mut ordered_deletes: Vec<_> = deletes.iter().collect();
    ordered_deletes.sort_by(|a, b| a.as_str().cmp(b.as_str()));

    let upsert_ids: Vec<String> = ordered_upserts
        .iter()
        .map(|(id, _, _)| id.as_str().to_string())
        .collect();
    let delete_ids: Vec<String> = ordered_deletes
        .iter()
        .map(|id| id.as_str().to_string())
        .collect();
    if upsert_ids.windows(2).any(|w| w[0] == w[1]) || delete_ids.windows(2).any(|w| w[0] == w[1]) {
        return Err(PortError::Backend(
            "index batch contains duplicate entity ids".into(),
        ));
    }
    if upsert_ids
        .iter()
        .any(|id| delete_ids.binary_search(id).is_ok())
    {
        return Err(PortError::Backend(
            "index batch cannot upsert and delete the same entity".into(),
        ));
    }

    let mut hasher = blake3::Hasher::new();
    // 数据兼容性：分隔串刻意保留旧名 `agentsessions`——operation_digest 持久化在
    // index_batches 表并与既有 data root 中已存摘要交叉比对，改名会破坏 v7 数据兼容。
    hash_field(&mut hasher, b"agentsessions-index-batch-v1");
    hash_field(&mut hasher, INDEX_BATCH_DIGEST_DOMAIN);
    for (id, payload, text) in ordered_upserts {
        hash_field(&mut hasher, b"upsert");
        hash_field(&mut hasher, id.as_str().as_bytes());
        hash_field(&mut hasher, payload);
        hash_field(&mut hasher, text.as_bytes());
    }
    for id in ordered_deletes {
        hash_field(&mut hasher, b"delete");
        hash_field(&mut hasher, id.as_str().as_bytes());
    }

    let (relation_upserts_json, relation_deletes_json, source_replacements_json) =
        relations.canonical_json()?;
    hash_field(&mut hasher, b"relation_upserts");
    hash_field(&mut hasher, relation_upserts_json.as_bytes());
    hash_field(&mut hasher, b"relation_deletes");
    hash_field(&mut hasher, relation_deletes_json.as_bytes());
    hash_field(&mut hasher, b"source_replacements");
    hash_field(&mut hasher, source_replacements_json.as_bytes());
    let relocation_json = serde_json::to_string(
        &relations
            .relocation
            .as_ref()
            .map(RelocationManifest::canonical_value),
    )
    .map_err(backend)?;
    if relations.relocation.is_some() {
        hash_field(&mut hasher, b"installation_relocation_v1");
        hash_field(&mut hasher, relocation_json.as_bytes());
    }

    Ok(CanonicalBatchManifest {
        upsert_ids,
        delete_ids,
        relation_upserts_json,
        relation_deletes_json,
        source_replacements_json,
        relocation_json,
        operation_digest: hasher.finalize().to_hex().to_string(),
    })
}

/// Durable outbox row for an index-generation operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexBatch {
    pub operation_id: String,
    pub base_generation: u64,
    pub target_generation: u64,
    pub state: String,
    pub operation_digest: String,
    pub upsert_ids: Vec<String>,
    pub delete_ids: Vec<String>,
    pub relation_upserts: Vec<serde_json::Value>,
    pub relation_deletes: Vec<serde_json::Value>,
    pub source_replacements: Vec<serde_json::Value>,
    pub durable_point: String,
    pub error_code: Option<String>,
}

/// Handle returned after an outbox intent reaches its first durable point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingIndexBatch {
    pub operation_id: String,
    pub base_generation: u64,
    pub target_generation: u64,
    pub operation_digest: String,
}

/// 一个 source 完整成功 scan 后的全部消息条目。
///
/// `sync`/`ingest` 为每个只读源构造一个 `SourceBatch`，store 据此推导：本次出现的
/// message id 是 upsert；该源上次成功 scan 有、本次没有的 id 是 tombstone（删除）。
/// 只有整批全部源都 stage 成功后才提交；missing/tombstone 只能由完整成功 scan 确认。
#[derive(Clone)]
pub struct SourceBatch {
    /// 该源的稳定标识（当前用其只读路径字符串）。
    pub source_path: String,
    /// 本次 scan 得到的全部 (message id, catalog payload, 索引正文)。
    ///
    /// 自 v6 起，条目不限于消息：组合根把该源派生的 session（`ses_v1_*`）与
    /// document（`doc_v1_*`）目录实体放进同一批 entries，随消息走同一事务提交、
    /// 同一 membership/tombstone 推导——源消失时容器实体随消息一起退役。
    pub entries: Vec<(StableId, Vec<u8>, String)>,
    /// 本次 source scan 观察到的全部 contextual message occurrences。
    ///
    /// B1 只携带数据；B2 才会把这些关系写入 v7 表。
    pub placements: Vec<MessagePlacement>,
    /// 本次 source scan 观察到的全部 contextual parent edges。
    pub edges: Vec<MessageEdge>,
    /// 本次 source scan 观察到的全部工具活动（v12 投影；默认为空）。
    ///
    /// 活动锚定在 `message_id` 上；同一活动事实 + 同一锚点在不同源里派生同一
    /// activity_id（跨源副本去重，claims 计数决定行生命周期）。
    pub activities: Vec<SourceActivity>,
    /// 本次 source scan 观察到的全部 token 用量事件（v15 投影；默认为空）。
    ///
    /// 事件锚定在 `session_id` 上（`message_id` 可空：provider 逐消息给用量时
    /// 挂消息，session 级累计事件挂会话）。同一事实 + 同一锚点跨源派生同一
    /// usage_id（去重，claims 计数决定行生命周期）。
    pub usage_events: Vec<SourceUsage>,
    /// 该 source 是否完成了零 skipped 的 relation scan。
    ///
    /// B1 不提交 completeness marker；B2 将据此替换或撤销 marker。
    pub relation_complete: bool,
    /// 捕获时的源字节长度与内容指纹（source-scan 指纹缓存，用于跳过
    /// 未变化源的重复解析）。None = 未提供（测试/旧调用方）。
    pub len_bytes: Option<i64>,
    pub fingerprint: Option<String>,
    /// Provider id supplied only by canonical-root discovery. Explicit sync batches
    /// leave this NULL so discover never tombstones paths outside its known root.
    pub provider_id: Option<String>,
    /// Source-scoped Resume Metadata 声明（ADR-0009）；随 source 事务原子写入，
    /// source 移除时同事务清除。每个 canonical Session 至多一个声明。
    pub resume_claims: Vec<SourceResumeClaim>,
}

/// 一条锚定在稳定消息上的工具活动（v12）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceActivity {
    pub message_id: StableId,
    pub activity: ToolActivity,
}

/// 一条锚定在稳定会话上的 token 用量事件（v15）。
///
/// `message_id` 为 `None` 表示 session 级观察（如 Codex `token_count` 累计
/// 事件没有消息关联）；`Some` 表示 provider 逐消息给出（如 Claude Code 的
/// `message.usage` 锚在 assistant 记录上）。绝不臆造锚点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceUsage {
    pub session_id: StableId,
    pub message_id: Option<StableId>,
    pub usage: UsageObservation,
}

/// `usage_events` 表行 + 用量 id 的存储视图。
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredUsage {
    usage_id: String,
    session_id: String,
    message_id: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_write_tokens: u64,
    reasoning_tokens: u64,
    token_source: String,
}

impl StoredUsage {
    fn matches(&self, other: &StoredUsage) -> bool {
        self.session_id == other.session_id
            && self.message_id == other.message_id
            && self.input_tokens == other.input_tokens
            && self.output_tokens == other.output_tokens
            && self.cache_read_tokens == other.cache_read_tokens
            && self.cache_write_tokens == other.cache_write_tokens
            && self.reasoning_tokens == other.reasoning_tokens
            && self.token_source == other.token_source
    }
}

/// 内容寻址的用量 id：`use_v1_<hex16(blake3("usage-event-v1" || …))>`。
///
/// 同一 (session_id, message_id, 五桶, token_source) 派生同一 id——跨源副本
/// 天然去重；message_id 为 None 时以空串参与哈希（与 Some("") 无歧义）。
fn usage_id_for(session_id: &str, message_id: Option<&str>, usage: &UsageObservation) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"usage-event-v1");
    hasher.update(session_id.as_bytes());
    hasher.update(&[0]);
    hasher.update(message_id.unwrap_or("").as_bytes());
    hasher.update(&[0]);
    for bucket in [
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_read_tokens,
        usage.cache_write_tokens,
        usage.reasoning_tokens,
    ] {
        hasher.update(&bucket.to_le_bytes());
        hasher.update(&[0]);
    }
    hasher.update(usage.token_source.as_str().as_bytes());
    let hex = hasher.finalize().to_hex();
    format!("use_v1_{}", &hex.as_str()[..16])
}

/// 把领域用量观察规范化为存储行（fail-closed：锚点种类校验）。
fn stored_usage_from(
    session_id: &StableId,
    message_id: Option<&StableId>,
    usage: &UsageObservation,
) -> PortResult<StoredUsage> {
    if session_id.kind() != IdKind::Session {
        return Err(PortError::Backend(
            "usage event session anchor has the wrong kind".into(),
        ));
    }
    if let Some(message_id) = message_id
        && message_id.kind() != IdKind::Message
    {
        return Err(PortError::Backend(
            "usage event message anchor has the wrong kind".into(),
        ));
    }
    let row = StoredUsage {
        usage_id: usage_id_for(session_id.as_str(), message_id.map(StableId::as_str), usage),
        session_id: session_id.as_str().to_string(),
        message_id: message_id.map(|id| id.as_str().to_string()),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cache_read_tokens,
        cache_write_tokens: usage.cache_write_tokens,
        reasoning_tokens: usage.reasoning_tokens,
        token_source: usage.token_source.as_str().to_string(),
    };
    Ok(row)
}

/// `tool_activities` 表行 + 活动 id 的存储视图。
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredActivity {
    activity_id: String,
    message_id: String,
    kind: String,
    actor: String,
    name: String,
    target: Option<String>,
    status: String,
}

impl StoredActivity {
    fn matches(&self, other: &StoredActivity) -> bool {
        self.message_id == other.message_id
            && self.kind == other.kind
            && self.actor == other.actor
            && self.name == other.name
            && self.target == other.target
            && self.status == other.status
    }
}

/// 工具名存储上限（字符数）：显式截断，防止 provider 失控的工具名膨胀存储。
const TOOL_ACTIVITY_NAME_MAX_CHARS: usize = 128;
// 工具 target 存储上限（字符数）由 ports 的
// `TOOL_ACTIVITY_TARGET_MAX_CHARS` 单一持有（provider 的可检索正文投影用同一
// 常量），此处直接引用，避免两侧数值漂移。

/// 内容寻址的活动 id：`act_v1_<hex16(blake3("tool-activity-v1" || …))>`。
///
/// 同一 (message_id, kind, actor, name, target, status) 派生同一 id——跨源副本
/// 天然去重；facts 在派生前已按存储上限截断（归一化点唯一，两侧一致）。
fn activity_id_for(message_id: &str, facts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"tool-activity-v1");
    hasher.update(message_id.as_bytes());
    hasher.update(&[0]);
    for fact in facts {
        hasher.update(fact.as_bytes());
        hasher.update(&[0]);
    }
    let hex = hasher.finalize().to_hex();
    format!("act_v1_{}", &hex.as_str()[..16])
}

/// 把领域活动规范化为存储行（边界：显式截断 name/target；fail-closed 校验）。
fn stored_activity_from(
    message_id: &StableId,
    activity: &ToolActivity,
) -> PortResult<StoredActivity> {
    activity.validate().map_err(|error| {
        PortError::Backend(format!("tool activity violates domain invariants: {error}"))
    })?;
    if message_id.kind() != IdKind::Message {
        return Err(PortError::Backend(
            "tool activity message anchor has the wrong kind".into(),
        ));
    }
    let name: String = activity
        .name
        .chars()
        .take(TOOL_ACTIVITY_NAME_MAX_CHARS)
        .collect();
    let target = activity.target.as_deref().map(|target| {
        target
            .chars()
            .take(TOOL_ACTIVITY_TARGET_MAX_CHARS)
            .collect()
    });
    let row = StoredActivity {
        activity_id: activity_id_for(
            message_id.as_str(),
            &[
                activity.kind.as_str(),
                activity.actor.as_str(),
                &name,
                target.as_deref().unwrap_or(""),
                activity.status.as_str(),
            ],
        ),
        message_id: message_id.as_str().to_string(),
        kind: activity.kind.as_str().to_string(),
        actor: activity.actor.as_str().to_string(),
        name,
        target,
        status: activity.status.as_str().to_string(),
    };
    Ok(row)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredPlacement {
    session_id: String,
    document_id: String,
    message_id: String,
    source_ordinal: u32,
    is_sidechain: bool,
    span: Option<(u64, u64)>,
}

impl StoredPlacement {
    fn matches(&self, placement: &MessagePlacement) -> bool {
        self.session_id == placement.session_id.as_str()
            && self.document_id == placement.source_document_id.as_str()
            && self.message_id == placement.message_id.as_str()
            && self.source_ordinal == placement.source_ordinal
            && self.is_sidechain == placement.is_sidechain
            && self.span == placement.span.as_ref().map(|span| (span.start, span.end))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredEdge {
    parent_message_id: String,
    parent_native_id: Option<String>,
    relation: String,
}

impl StoredEdge {
    fn matches(&self, edge: &MessageEdge) -> bool {
        self.parent_message_id == edge.parent_message_id.as_str()
            && self.parent_native_id == edge.parent_native_id
            && self.relation == edge.relation.as_str()
    }
}

struct PreparedSource {
    relation_complete: bool,
    prior_entity_memberships: BTreeMap<String, Option<String>>,
    prior_placement_ids: BTreeSet<String>,
    prior_activity_ids: BTreeSet<String>,
    prior_usage_ids: BTreeSet<String>,
    observed_placements: BTreeMap<String, MessagePlacement>,
    observed_edges: BTreeMap<String, MessageEdge>,
    replacement: SourceReplacementManifest,
}

fn validate_placement(placement: &MessagePlacement) -> PortResult<()> {
    if placement.session_id.kind() != IdKind::Session
        || placement.source_document_id.kind() != IdKind::Document
        || placement.message_id.kind() != IdKind::Message
    {
        return Err(PortError::Backend(
            "message placement contains an entity id with the wrong kind".into(),
        ));
    }
    let expected = PlacementId::derive(
        &placement.session_id,
        &placement.source_document_id,
        &placement.message_id,
        placement.source_ordinal,
    );
    if placement.id != expected {
        return Err(PortError::Backend(
            "message placement id does not match its contextual facts".into(),
        ));
    }
    if let Some(span) = &placement.span {
        if span.end < span.start {
            return Err(PortError::Backend(
                "message placement span end precedes start".into(),
            ));
        }
        i64::try_from(span.start).map_err(backend)?;
        i64::try_from(span.end).map_err(backend)?;
    }
    Ok(())
}

fn validate_edge(edge: &MessageEdge) -> PortResult<()> {
    if edge.parent_message_id.kind() != IdKind::Message {
        return Err(PortError::Backend(
            "message edge parent id has the wrong kind".into(),
        ));
    }
    Ok(())
}

fn stored_role(value: &str) -> PortResult<Role> {
    match value {
        "user" => Ok(Role::User),
        "assistant" => Ok(Role::Assistant),
        "system" => Ok(Role::System),
        // Codex's authoritative conversation role for the system/permission
        // layer; the codex adapter emits it verbatim (see provider-codex
        // is_conversational_role), so the read path must accept it.
        "developer" => Ok(Role::Developer),
        "tool" => Ok(Role::Tool),
        _ => Err(PortError::Backend(
            "stored message has an unsupported role".into(),
        )),
    }
}

fn stored_relation(value: &str) -> PortResult<MessageRelation> {
    match value {
        "reply" => Ok(MessageRelation::Reply),
        "retry" => Ok(MessageRelation::Retry),
        "fork" => Ok(MessageRelation::Fork),
        "continuation" => Ok(MessageRelation::Continuation),
        "subagent" => Ok(MessageRelation::Subagent),
        "tool_result" => Ok(MessageRelation::ToolResult),
        _ => Err(PortError::Backend(
            "stored message edge has an unsupported relation".into(),
        )),
    }
}

/// Repo slug 解析器（schema v16）：cwd → 宿主仓 `host/owner/name` 三段 slug。
///
/// 写入路径由组合根注入真实实现（CLI 的 git 检测；借鉴 Recall 的
/// repo_identity），测试注入确定性的 fake。解析器是环境事实探测器：
/// 检测失败一律 `None`（诚实降级），绝不报错、绝不猜——sync/index 永远
/// 不因 git 不可用而失败。
pub trait RepoSlugResolver {
    /// cwd → repo slug；任何一步失败（目录不存在/非 git 仓库/无 origin/
    /// URL 形状不认识）返回 None。
    fn resolve(&self, cwd: &str) -> Option<String>;
}

/// 默认解析器：不探测（repo identity 投影关闭）。未注入解析器时
/// `session_repo_slugs` 恒为空——"无行 = 未知"，与"有解析器但检测失败"
/// 同义，读取侧无需区分。
pub struct NoopRepoSlugResolver;

impl RepoSlugResolver for NoopRepoSlugResolver {
    fn resolve(&self, _cwd: &str) -> Option<String> {
        None
    }
}

/// 重投影时对 repo 身份投影（schema v16 `session_repo_slugs`）的处置。
///
/// 该表是本适配器唯一**不可从 catalog 重建**的派生投影：slug 只能由注入的
/// [`RepoSlugResolver`] 现场探测本机 git 得到，catalog 里没有任何字节能还原它
/// （按设计——绝对路径不进这张表）。因此"从权威 catalog 全量重投影"这个动作
/// 对它没有权威：若无条件清表重派生，一次解析器缺席（打开时自愈尚未装配组合根
/// 注入的解析器，或 git 临时不可用）就会把整条 repo 维度删空，同时把
/// `index_projection_version` 盖成当前——此后 `search --repo` 恒 0 命中、
/// `status` 恒无仓库，而没有任何信号会报告这次丢失。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepoIdentityRebuild {
    /// 重派生：显式 `index rebuild` 与增量提交路径。调用方已装配解析器，
    /// 且会话集合可能变化（退役会话必须同事务删除其 slug 行）。
    Rederive,
    /// 原样保留：打开时的投影版本自愈（[`SqliteStore::ensure_index_projection_current`]）。
    /// 自愈只重投影 catalog 可重建的词元流与显示投影，完全不改变会话集合，
    /// 因此保留既有 slug 行不会留下孤儿。slug 派生规则本身变化时的收敛动作
    /// 是显式 `index rebuild`（它会重新探测 git），不是打开时自愈。
    Preserve,
}

/// SQLite 支撑的存储：catalog 表存规范化实体负载，FTS5 表提供全文检索。
///
/// 单连接 + `RefCell` 内部可变：端口 trait 以 `&self` 取用，而 rusqlite 的写操作
/// 需要可变连接。首个垂直切片单线程使用，故不引入连接池。
///
/// 可选持有 [`WriterLease`]：经 [`SqliteStore::open_for_write`] 打开时，
/// lease 与 store 同生命周期，Drop store 时释放 data-root 写锁。
pub struct SqliteStore {
    conn: RefCell<Connection>,
    read_snapshot_count: Cell<usize>,
    read_snapshot_failed: Cell<bool>,
    /// 写入路径持有的 data-root 独占 lease；只读打开时为 None。
    _lease: Option<WriterLease>,
    /// 当前语义模型 id（#3）：`None` 表示未配置语义检索，`SemanticIndex`
    /// 全部方法降级为空/未就绪。设置它是调用方声明"这些向量属于哪个模型"，
    /// 换模型后旧维度向量因 model_id 不匹配自然被排除。
    semantic_model_id: RefCell<Option<String>>,
    /// Repo slug 解析器（schema v16）：写路径由组合根注入真实 git 实现；
    /// 默认 [`NoopRepoSlugResolver`]（投影关闭）。解析器是环境事实探测器，
    /// 失败一律 None。
    repo_slug_resolver: RefCell<Box<dyn RepoSlugResolver>>,
    /// Staging reservations only; persisted with the matching source replacement.
    pending_installations: RefCell<BTreeMap<String, InstallationAssignment>>,
    relocation_clock: fn() -> PortResult<i64>,
}

struct SqliteReadSnapshot<'a> {
    store: &'a SqliteStore,
}

impl ReadSnapshot for SqliteReadSnapshot<'_> {}

impl Drop for SqliteReadSnapshot<'_> {
    fn drop(&mut self) {
        let remaining = self.store.read_snapshot_count.get() - 1;
        self.store.read_snapshot_count.set(remaining);
        if remaining == 0 {
            // No writes belong in this scope. Rollback also releases read locks.
            // A failed cleanup poisons future snapshots rather than silently
            // reusing an old view. Drop itself must not panic during unwinding.
            let released = self.store.conn.try_borrow().is_ok_and(|conn| {
                let rollback = conn.execute_batch("ROLLBACK");
                let reset = conn.execute_batch("PRAGMA query_only = OFF");
                rollback.is_ok() && reset.is_ok()
            });
            if !released {
                self.store.read_snapshot_failed.set(true);
            }
        }
    }
}

/// 一批源路径的指纹缓存项：捕获时长度与内容指纹。
/// 一条 source-scan 指纹缓存行：`(len_bytes, fingerprint, parser_version)`。
///
/// parser_version 是解析语义版本（[`PARSER_SEMANTIC_VERSION`] 的存储镜像）：
/// CLI 的 unchanged 判定必须三者同时匹配——版本落后即视为需要重解析，
/// 否则解析逻辑升级后源文件未变化的库永远不重解析。
pub type SourceFingerprint = (Option<i64>, Option<String>, i64);

/// 一条 `source_scans` 行的 current 判定视图：
/// `(len_bytes, fingerprint, provider_id, parser_version)`。
type StoredSourceScan = (Option<i64>, Option<String>, Option<String>, i64);

/// 全库关系/成员视图，提交开始时读取一次，供后续 no-op 探测复用。
struct CatalogStateSnapshot<'a> {
    entities_by_source: &'a BTreeMap<String, BTreeMap<String, Option<String>>>,
    placements_by_source: &'a BTreeMap<String, BTreeSet<String>>,
    activities_by_source: &'a BTreeMap<String, BTreeSet<String>>,
    usages_by_source: &'a BTreeMap<String, BTreeSet<String>>,
    placements: &'a BTreeMap<String, StoredPlacement>,
    edges: &'a BTreeMap<String, StoredEdge>,
    activities: &'a BTreeMap<String, StoredActivity>,
    usages: &'a BTreeMap<String, StoredUsage>,
}

impl SqliteStore {
    /// Pin this connection's read view; nested callers share it until the last
    /// guard drops. This never creates, migrates or writes a catalog.
    pub fn begin_read_snapshot(&self) -> PortResult<Box<dyn ReadSnapshot + '_>> {
        if self.read_snapshot_failed.get() {
            return Err(PortError::Backend(
                "read snapshot cleanup failed; reopen catalog".into(),
            ));
        }
        let count = self.read_snapshot_count.get();
        if count == 0 {
            let conn = self.conn.borrow();
            if !conn.is_autocommit() {
                return Err(PortError::InvalidRequest(
                    "read snapshot cannot join a write transaction".into(),
                ));
            }
            // All constructors leave query_only OFF. Enforce the read-only
            // scope even on a write-open store; never accept writes then silently
            // discard them on guard cleanup. SQLITE_OPEN_READ_ONLY is unchanged.
            conn.execute_batch("PRAGMA query_only = ON")
                .map_err(backend)?;
            if let Err(error) = conn.execute_batch("BEGIN DEFERRED") {
                if conn.execute_batch("PRAGMA query_only = OFF").is_err() {
                    self.read_snapshot_failed.set(true);
                }
                return Err(backend(error));
            }
            // BEGIN alone does not pin SQLite's view. Read a real main-database
            // table before returning (SELECT 1 would not suffice).
            let pinned = conn.query_row(
                "SELECT active_generation FROM store_metadata WHERE singleton = 1",
                [],
                |row| row.get::<_, i64>(0),
            );
            if let Err(error) = pinned {
                let rollback = conn.execute_batch("ROLLBACK");
                let reset = conn.execute_batch("PRAGMA query_only = OFF");
                if rollback.is_err() || reset.is_err() {
                    self.read_snapshot_failed.set(true);
                }
                return Err(backend(error));
            }
        }
        self.read_snapshot_count.set(count + 1);
        Ok(Box::new(SqliteReadSnapshot { store: self }))
    }

    /// 只读打开（不抢 writer lease）。供 search/get/doctor 等读路径。
    pub fn open(path: &str) -> PortResult<Self> {
        let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(backend)?;
        conn.busy_timeout(std::time::Duration::from_secs(1))
            .map_err(backend)?;
        let current: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(backend)?;
        if current != SCHEMA_VERSION {
            return Err(PortError::SchemaIncompatible(format!(
                "catalog schema version {current} differs from supported {SCHEMA_VERSION}; \
                 run index rebuild with a compatible agent-session-grep version"
            )));
        }
        Self::register_scalar_functions(&conn)?;
        Ok(SqliteStore {
            conn: RefCell::new(conn),
            read_snapshot_count: Cell::new(0),
            read_snapshot_failed: Cell::new(false),
            _lease: None,
            semantic_model_id: RefCell::new(None),
            repo_slug_resolver: RefCell::new(Box::new(NoopRepoSlugResolver)),
            pending_installations: RefCell::new(BTreeMap::new()),
            relocation_clock: unix_ms,
        })
    }

    /// 写入路径打开：先在 db 所在目录获取 data-root writer lease，再打开库，
    /// 最后收敛派生投影。
    ///
    /// 若另一进程已持 lease，立即失败（不阻塞）。lease 随本 store 存活，
    /// Drop 时释放，以维持每个 data root 单写者不变量。
    ///
    /// 注意本函数在返回**之前**就可能重投影派生索引（投影版本自愈），此时
    /// 组合根还没机会 [`set_repo_slug_resolver`](Self::set_repo_slug_resolver)。
    /// 因此自愈路径刻意不重派生 repo 身份投影——见 [`RepoIdentityRebuild`]。
    pub fn open_for_write(path: &str) -> PortResult<Self> {
        let db_path = Path::new(path);
        // 裸相对文件名（如 "catalog.db"）的 parent() 是空串 ""，create_dir_all("")
        // 会报错；空 parent 按当前工作目录处理（与只读 open 一致，目录解析交给
        // Connection::open / WriterLease）。
        let data_root = match db_path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        let lease = WriterLease::try_acquire(data_root)?;
        let conn = Connection::open(path).map_err(backend)?;
        Self::init(&conn)?;
        let store = SqliteStore {
            conn: RefCell::new(conn),
            read_snapshot_count: Cell::new(0),
            read_snapshot_failed: Cell::new(false),
            _lease: Some(lease),
            semantic_model_id: RefCell::new(None),
            repo_slug_resolver: RefCell::new(Box::new(NoopRepoSlugResolver)),
            pending_installations: RefCell::new(BTreeMap::new()),
            relocation_clock: unix_ms,
        };
        // lease 已到手，当前进程是唯一写者；安全收敛上次崩溃留下的无副作用 intent。
        store.recover_interrupted()?;
        // 投影版本自愈（schema v17）：本二进制的投影变换与库中现存词元流失配时，
        // 从权威 catalog 重投影（无需 reparse）。放在 lease 之后——重投影是写操作，
        // 必须由唯一写者执行；读路径（`open`）无 lease 不写，改为查询期 fail-closed。
        store.ensure_index_projection_current()?;
        Ok(store)
    }

    /// 打开内存存储（测试用，无 lease）。
    pub fn open_in_memory() -> PortResult<Self> {
        let conn = Connection::open_in_memory().map_err(backend)?;
        Self::init(&conn)?;
        Ok(SqliteStore {
            conn: RefCell::new(conn),
            read_snapshot_count: Cell::new(0),
            read_snapshot_failed: Cell::new(false),
            _lease: None,
            semantic_model_id: RefCell::new(None),
            repo_slug_resolver: RefCell::new(Box::new(NoopRepoSlugResolver)),
            pending_installations: RefCell::new(BTreeMap::new()),
            relocation_clock: unix_ms,
        })
    }

    /// 打开并把 schema 迁移到当前版本，为版本化 migration 与可重建索引奠基。
    fn init(conn: &Connection) -> PortResult<()> {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA cache_size = -131072;",
        )
        .map_err(backend)?;
        Self::migrate(conn)?;
        Self::register_scalar_functions(conn)
    }

    /// Register the ISO-8601 timestamp parser used by filtered search pushdown.
    ///
    /// `asg_instant_sort_key(text)` maps a timezone-qualified RFC3339/ISO-8601
    /// string to the 12-byte [`SearchInstant::sort_key`] BLOB (byte order ==
    /// instant order). Non-string inputs (e.g. Codex's modern `null`) and
    /// unparseable timestamps yield NULL so rows compare out of every
    /// half-open `[since, until)` predicate instead of erroring the query.
    fn register_scalar_functions(conn: &Connection) -> PortResult<()> {
        conn.create_scalar_function(
            "asg_instant_sort_key",
            1,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8
                | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
            |ctx| {
                let value = ctx.get::<rusqlite::types::Value>(0)?;
                let rusqlite::types::Value::Text(text) = value else {
                    return Ok(None);
                };
                Ok(parse_search_instant(&text).map(|instant| instant.sort_key().to_vec()))
            },
        )
        .map_err(backend)
    }

    /// 按 `PRAGMA user_version` 门控的顺序迁移。
    ///
    /// 每次 schema 变更追加一个版本步骤并递增 [`SCHEMA_VERSION`]；旧库打开时
    /// 从其记录的版本逐步升级。`user_version` 是 SQLite 内建的每库整数，
    /// 不占额外表，正是 migration 追踪的标准落点。
    fn migrate(conn: &Connection) -> PortResult<()> {
        let current: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(backend)?;
        if current > SCHEMA_VERSION {
            // 库比本二进制更新——拒绝而非静默降级，避免按旧 schema 误读新数据。
            return Err(PortError::SchemaIncompatible(format!(
                "catalog schema version {current} is newer than supported {SCHEMA_VERSION}; \
                 upgrade agent-session-grep or rebuild the data root"
            )));
        }
        let legacy_tx = if current < 6 {
            // v1-v6 are one atomic migration unit. A crash mid-way (e.g. after
            // source_membership's v5 rename but before user_version=6) must
            // roll back instead of stranding a store that can never reopen.
            Some(conn.unchecked_transaction().map_err(backend)?)
        } else {
            None
        };
        // Legacy steps run on the transaction when one was opened (its Deref
        // exposes the same Connection API); v7 migration keeps its own
        // transaction on the raw connection.
        let mig = legacy_tx.as_deref().unwrap_or(conn);
        if current < 1 {
            // v1：catalog（按 id 主键存 payload）+ contentful-id FTS5（id 回带，text 索引）。
            mig.execute_batch(
                "CREATE TABLE catalog (
                     id      TEXT PRIMARY KEY,
                     payload BLOB NOT NULL
                 );
                 CREATE VIRTUAL TABLE fts USING fts5(id UNINDEXED, text);",
            )
            .map_err(backend)?;
        }
        if current < 2 {
            // v2：活动 generation + durable outbox。FTS5 与 catalog 同事务提交，
            // journal 记录 intent、激活结果以及崩溃恢复结论。
            mig.execute_batch(
                "CREATE TABLE IF NOT EXISTS store_metadata (
                     singleton         INTEGER PRIMARY KEY CHECK(singleton = 1),
                     active_generation INTEGER NOT NULL CHECK(active_generation >= 0),
                     index_projection_version INTEGER NOT NULL DEFAULT 0
                 );
                 INSERT OR IGNORE INTO store_metadata(singleton, active_generation)
                 VALUES(1, 0);
                 CREATE TABLE IF NOT EXISTS index_batches (
                     operation_id     TEXT PRIMARY KEY,
                     base_generation  INTEGER NOT NULL,
                     target_generation INTEGER NOT NULL,
                     state            TEXT NOT NULL CHECK(state IN (
                         'building', 'search_built', 'activated', 'aborted',
                         'superseded', 'cleanup_pending'
                     )),
                     operation_digest TEXT NOT NULL,
                     upsert_ids_json  TEXT NOT NULL,
                     delete_ids_json  TEXT NOT NULL,
                     durable_point    TEXT NOT NULL,
                     created_at_ms    INTEGER NOT NULL,
                     committed_at_ms  INTEGER,
                     error_code       TEXT,
                     CHECK(target_generation = base_generation + 1)
                 );
                 CREATE INDEX IF NOT EXISTS index_batches_state
                 ON index_batches(state);",
            )
            .map_err(backend)?;
        }
        if current < 3 {
            // v3：以 wire id 为唯一索引键，隔离稳定性元数据，保证外部 wire round-trip
            // 形成的 Unstable id 也能删除原有 Native/Reconstructed FTS 行。
            mig.execute_batch(
                "CREATE TABLE IF NOT EXISTS fts_ids (
                     wire_id TEXT PRIMARY KEY,
                     id_json TEXT NOT NULL UNIQUE,
                     fts_rowid INTEGER
                 );",
            )
            .map_err(backend)?;
            let mut stmt = mig.prepare("SELECT id FROM fts").map_err(backend)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            let mut ids = Vec::new();
            for row in rows {
                let id_json = row.map_err(backend)?;
                let id: StableId = serde_json::from_str(&id_json).map_err(backend)?;
                ids.push((id.as_str().to_string(), id_json));
            }
            drop(stmt);
            for (wire_id, id_json) in ids {
                mig.execute(
                    "INSERT OR REPLACE INTO fts_ids(wire_id, id_json) VALUES(?1, ?2)",
                    rusqlite::params![wire_id, id_json],
                )
                .map_err(backend)?;
            }
        }
        if current < 4 {
            // v4：记录每个 source 最近一次完整成功 scan 的 message membership，
            // 只有完整 scan 成功后才可安全推导删除/tombstone。
            mig.execute_batch(
                "CREATE TABLE IF NOT EXISTS source_membership (
                     source_path TEXT NOT NULL,
                     message_id  TEXT PRIMARY KEY
                 );
                 CREATE INDEX IF NOT EXISTS source_membership_source
                 ON source_membership(source_path);",
            )
            .map_err(backend)?;
        }
        if current < 5 {
            // v5：空 source scan 也必须留下“已成功扫描”的证据；membership 改为
            // 多对多主键，避免删除一个 source 时误删仍被其它 source 引用的实体。
            mig.execute_batch(
                "DROP INDEX IF EXISTS source_membership_source;
                 ALTER TABLE source_membership RENAME TO source_membership_v4;
                 CREATE TABLE source_membership (
                     source_path TEXT NOT NULL,
                     message_id  TEXT NOT NULL,
                     PRIMARY KEY(source_path, message_id)
                 );
                 INSERT INTO source_membership(source_path, message_id)
                 SELECT source_path, message_id FROM source_membership_v4;
                 DROP TABLE source_membership_v4;
                 CREATE INDEX source_membership_source
                 ON source_membership(source_path);
                 CREATE TABLE IF NOT EXISTS source_scans (
                     source_path   TEXT PRIMARY KEY,
                     scanned_at_ms INTEGER NOT NULL,
                     len_bytes     INTEGER,
                     fingerprint   TEXT,
                     provider_id   TEXT,
                     parser_version INTEGER NOT NULL DEFAULT 0
                 );",
            )
            .map_err(backend)?;
        }
        if current < 6 {
            // v6：source_membership 增加可空 document_id——记录各 source 所属文档实体的
            // wire id，使 source 消失的 tombstone 清理能同步退役其 session/document 目录行。
            // 旧行保持 NULL（v6 前的 membership 无文档归属信息）。
            let has_document_id = mig
                .prepare("PRAGMA table_info(source_membership)")
                .map_err(backend)?
                .query_map([], |row| row.get::<_, String>(1))
                .map_err(backend)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(backend)?
                .iter()
                .any(|name| name == "document_id");
            if !has_document_id {
                mig.execute_batch("ALTER TABLE source_membership ADD COLUMN document_id TEXT;")
                    .map_err(backend)?;
            }
            // Commit the versioned steps before the atomic v6->v7 step, which
            // opens its own transaction on the raw connection.
            mig.execute_batch("PRAGMA user_version = 6;")
                .map_err(backend)?;
            if let Some(tx) = legacy_tx {
                tx.commit().map_err(backend)?;
            }
        }
        if current < 7 {
            Self::migrate_v6_to_v7(conn)?;
        }
        if current < 8 {
            Self::migrate_v7_to_v8(conn)?;
        }
        if current < 9 {
            Self::migrate_v8_to_v9(conn)?;
        }
        if current < 10 {
            Self::migrate_v9_to_v10(conn)?;
        }
        if current < 11 {
            Self::migrate_v10_to_v11(conn)?;
        }
        if current < 12 {
            Self::migrate_v11_to_v12(conn)?;
        }
        if current < 13 {
            Self::migrate_v12_to_v13(conn)?;
        }
        if current < 14 {
            Self::migrate_v13_to_v14(conn)?;
        }
        if current < 15 {
            Self::migrate_v14_to_v15(conn)?;
        }
        if current < 16 {
            Self::migrate_v15_to_v16(conn)?;
        }
        if current < 17 {
            Self::migrate_v16_to_v17(conn)?;
        }
        if current < 18 {
            Self::migrate_v17_to_v18(conn)?;
        }
        if current < 19 {
            let tx = conn.unchecked_transaction().map_err(backend)?;
            tx.execute_batch(
                "CREATE TABLE source_entity_projections (
                    source_path TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    id_json TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    text TEXT NOT NULL,
                    PRIMARY KEY(source_path, entity_id)
                 );
                 CREATE INDEX source_entity_projections_entity
                    ON source_entity_projections(entity_id, source_path);
                 PRAGMA user_version = 19;",
            )
            .map_err(backend)?;
            // Never manufacture original observations from the catalog aggregate.
            tx.commit().map_err(backend)?;
        }
        // 不随 user_version 门控：旧 v7 库（本列存在前建成的）打开时同样需要。
        Self::ensure_fts_ids_rowid(conn)?;
        Ok(())
    }

    /// 确保 `fts_ids` 边车携带 `fts_rowid` 列（v7 内的加法扩展，`user_version` 不变）。
    ///
    /// FTS5 表的 `id` 列是内容列而非 rowid，旧删除语句按内容比较会整表扫描
    /// （`SCAN fts VIRTUAL TABLE INDEX 0`），每批提交成本 O(全库)。本列把
    /// fts5 行的 rowid 回写到边车，删除改按 rowid 定位（O(1)）。
    /// 新库在 v3 建表时已带本列，此处直接短路——open（含只读 open）不再为
    /// 新库执行 ALTER+回填事务；只有 fts_rowid 列加入前建成的旧 v7 库首次
    /// 打开时走 ALTER+回填：`fts` 与 `fts_ids` 自 v3 起同事务写入、一一对应，
    /// 用 `id_json` 连接即可把 fts5 已分配的行 rowid 抄进边车；session/document
    /// 实体无 fts 行，保持 NULL（删除按 NULL 定位即无操作）。
    /// ALTER 与回填在同一事务内，崩溃不留半成品；重跑因列已存在直接短路。
    /// 并发打开旧库时 ALTER/回填可能遇 BUSY/LOCKED——经 [`backend`] 归一为
    /// 可重试的 WriterBusy，调用方应重试而非当作锁损坏。
    fn ensure_fts_ids_rowid(conn: &Connection) -> PortResult<()> {
        let has_fts_rowid = conn
            .prepare("PRAGMA table_info(fts_ids)")
            .map_err(backend)?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
            .iter()
            .any(|name| name == "fts_rowid");
        if has_fts_rowid {
            return Ok(());
        }
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch("ALTER TABLE fts_ids ADD COLUMN fts_rowid INTEGER;")
            .map_err(backend)?;
        let rows: Vec<(String, i64)> = {
            let mut stmt = tx
                .prepare(
                    "SELECT fi.wire_id, f.rowid
                     FROM fts f JOIN fts_ids fi ON fi.id_json = f.id",
                )
                .map_err(backend)?;
            let mapped = stmt
                .query_map([], |row| {
                    let wire: String = row.get(0)?;
                    let rid: i64 = row.get(1)?;
                    Ok((wire, rid))
                })
                .map_err(backend)?;
            let mut out = Vec::new();
            for row in mapped {
                out.push(row.map_err(backend)?);
            }
            out
        };
        for (wire_id, fts_rowid) in rows {
            tx.execute(
                "UPDATE fts_ids SET fts_rowid = ?2 WHERE wire_id = ?1",
                rusqlite::params![wire_id, fts_rowid],
            )
            .map_err(backend)?;
        }
        tx.commit().map_err(backend)?;
        Ok(())
    }

    /// Add the v7 relational schema in one explicit transaction.
    ///
    /// Legacy catalog and source-membership rows are retained byte-for-byte.
    /// No placement, edge, source claim, or relation-complete marker can be
    /// reconstructed safely from v6 aliases, so all new relation tables start
    /// empty. `user_version = 7` is part of the same transaction as the DDL.
    fn migrate_v6_to_v7(conn: &Connection) -> PortResult<()> {
        Self::migrate_v6_to_v7_inner(conn, false)
    }

    fn migrate_v6_to_v7_inner(conn: &Connection, inject_failure: bool) -> PortResult<()> {
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "CREATE TABLE message_placements (
                 placement_id   TEXT PRIMARY KEY,
                 session_id     TEXT NOT NULL,
                 document_id    TEXT NOT NULL,
                 message_id     TEXT NOT NULL,
                 source_ordinal INTEGER NOT NULL CHECK(source_ordinal >= 0),
                 is_sidechain   INTEGER NOT NULL CHECK(is_sidechain IN (0, 1)),
                 byte_start     INTEGER,
                 byte_end       INTEGER,
                 CHECK(
                     (byte_start IS NULL AND byte_end IS NULL)
                     OR (byte_start >= 0 AND byte_end >= byte_start)
                 ),
                 UNIQUE(session_id, document_id, source_ordinal)
             );
             CREATE TABLE message_edges (
                 child_placement_id TEXT PRIMARY KEY,
                 parent_message_id  TEXT NOT NULL,
                 parent_native_id   TEXT,
                 relation           TEXT NOT NULL
             );
             CREATE TABLE source_placement_membership (
                 source_path  TEXT NOT NULL,
                 placement_id TEXT NOT NULL,
                 PRIMARY KEY(source_path, placement_id)
             );
             CREATE TABLE source_relation_scans (
                 source_path             TEXT PRIMARY KEY,
                 relation_schema_version INTEGER NOT NULL
                     CHECK(relation_schema_version >= 7)
             );
             CREATE INDEX message_placements_session_order
             ON message_placements(session_id, document_id, source_ordinal, placement_id);
             CREATE INDEX message_placements_message
             ON message_placements(message_id);
             CREATE INDEX message_placements_document
             ON message_placements(document_id);
             CREATE INDEX source_placement_membership_placement
             ON source_placement_membership(placement_id);
             ALTER TABLE index_batches
             ADD COLUMN relation_upserts_json TEXT NOT NULL DEFAULT '[]';
             ALTER TABLE index_batches
             ADD COLUMN relation_deletes_json TEXT NOT NULL DEFAULT '[]';
             ALTER TABLE index_batches
             ADD COLUMN source_replacements_json TEXT NOT NULL DEFAULT '[]';
             PRAGMA user_version = 7;",
        )
        .map_err(backend)?;

        // Source-scan fingerprint cache columns (additive within v7): used by
        // the CLI to skip re-parsing sources whose bytes are unchanged.
        // Idempotent for catalogs that reached v7 before these columns
        // existed.
        let has_len_bytes: bool = tx
            .prepare("PRAGMA table_info(source_scans)")
            .map_err(backend)?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
            .iter()
            .any(|name| name == "len_bytes");
        if !has_len_bytes {
            tx.execute_batch(
                "ALTER TABLE source_scans ADD COLUMN len_bytes INTEGER;
                 ALTER TABLE source_scans ADD COLUMN fingerprint TEXT;",
            )
            .map_err(backend)?;
        }

        if inject_failure {
            return Err(PortError::Backend(
                "injected v6-to-v7 migration failure".into(),
            ));
        }

        tx.commit().map_err(backend)
    }

    /// Add the v8 resume-claims schema in one explicit transaction.
    ///
    /// Source-scoped Resume Metadata claims (ADR-0009)：键为
    /// `(source_path, session_id)`，随派生它们的 source replacement 同事务
    /// 原子写入/替换，source 移除时同事务清除。声明是独立的只读解析源，
    /// 不进入 FTS 正文；legacy 库迁到 v8 后表为空，所有 session 读到
    /// "no resume metadata claims"（不可恢复），直至 re-sync 回填声明。
    /// `user_version = 8` 与 DDL 在同一事务内。
    fn migrate_v7_to_v8(conn: &Connection) -> PortResult<()> {
        Self::migrate_v7_to_v8_inner(conn, false)
    }

    fn migrate_v7_to_v8_inner(conn: &Connection, inject_failure: bool) -> PortResult<()> {
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "CREATE TABLE source_session_resume_claims (
                 source_path                      TEXT NOT NULL,
                 session_id                       TEXT NOT NULL,
                 provider_id                      TEXT NOT NULL,
                 provider_session_id              TEXT,
                 provider_session_id_state        TEXT NOT NULL,
                 original_working_directory       TEXT,
                 original_working_directory_state TEXT NOT NULL,
                 pair_observed                    INTEGER NOT NULL
                     CHECK(pair_observed IN (0, 1)),
                 PRIMARY KEY(source_path, session_id)
             );
             CREATE INDEX source_session_resume_claims_session
             ON source_session_resume_claims(session_id, source_path);
             PRAGMA user_version = 8;",
        )
        .map_err(backend)?;

        if inject_failure {
            return Err(PortError::Backend(
                "injected v7-to-v8 migration failure".into(),
            ));
        }

        tx.commit().map_err(backend)
    }

    /// Add the v9 `provider_id` column to `source_scans` (additive, non-destructive).
    ///
    /// `sync --discover` 用 `provider_id` 按 provider diff 已存源路径：找出某个
    /// provider 下曾被扫描、本次未在磁盘上出现的源，合成空批 tombstone（仅在
    /// 完整扫描时）。旧行保持 NULL（直到该源被再次扫描时回填）。列存在即无害：
    /// 未升级的 v8 代码路径忽略它。
    fn migrate_v8_to_v9(conn: &Connection) -> PortResult<()> {
        let has_provider_id = conn
            .prepare("PRAGMA table_info(source_scans)")
            .map_err(backend)?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
            .iter()
            .any(|name| name == "provider_id");
        if has_provider_id {
            // 已有列（可能是本迁移重跑或新库建表时已带）；只对齐 user_version。
            conn.execute_batch("PRAGMA user_version = 9;")
                .map_err(backend)?;
            return Ok(());
        }
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "ALTER TABLE source_scans ADD COLUMN provider_id TEXT;
             PRAGMA user_version = 9;",
        )
        .map_err(backend)?;
        tx.commit().map_err(backend)
    }

    /// v9→v10：语义向量边车表（#3）。
    ///
    /// `message_vec` 与 `fts` 同级——都是 catalog 的可重建投影，不是权威数据。
    /// 向量以 little-endian f32 blob 存储（`dimension` 显式记录，避免读回时
    /// 靠 blob 长度推断）；`model_id` 让换模型后的旧向量可被识别并清理，而不是
    /// 与新维度向量混在一张表里静默产生垃圾相似度。
    /// 主键是 wire_id：与 `fts_ids` 同一连接键，删除路径无需第二套 id 映射。
    fn migrate_v9_to_v10(conn: &Connection) -> PortResult<()> {
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS message_vec (
                 wire_id   TEXT PRIMARY KEY,
                 model_id  TEXT NOT NULL,
                 dimension INTEGER NOT NULL CHECK(dimension > 0),
                 embedding BLOB NOT NULL
             );
             CREATE INDEX IF NOT EXISTS message_vec_model ON message_vec(model_id);
             PRAGMA user_version = 10;",
        )
        .map_err(backend)?;
        tx.commit().map_err(backend)
    }

    /// Add the v11 privacy-safe Session metadata search projection.
    ///
    /// Both tables are derived state: `session_fts` holds only bounded, resolved
    /// Session metadata and `session_fts_ids` maps the canonical Session wire to
    /// the FTS rowid. Existing catalog, message FTS, claims, and relations remain
    /// untouched; rebuild or the next affected source commit populates the new
    /// projection for migrated databases.
    fn migrate_v10_to_v11(conn: &Connection) -> PortResult<()> {
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS session_fts USING fts5(session_wire UNINDEXED, text);
             CREATE TABLE IF NOT EXISTS session_fts_ids (
                 session_wire TEXT PRIMARY KEY,
                 fts_rowid    INTEGER NOT NULL
             );
             PRAGMA user_version = 11;",
        )
        .map_err(backend)?;
        tx.commit().map_err(backend)
    }

    /// Add the v12 tool-activity projection in one explicit transaction
    /// (additive, non-destructive).
    ///
    /// `tool_activities` stores typed tool-call observations anchored to stable
    /// message wire ids; `tool_activity_membership` records per-source claims so
    /// the lifecycle mirrors `message_placements` (complete-scan replace,
    /// incomplete-scan union, tombstone via claims). The step depends only on
    /// v7+ tables, so it runs cleanly on any catalog at v7..=11 — merge-safe
    /// with parallel schema branches. `user_version = 12` commits with the DDL.
    fn migrate_v11_to_v12(conn: &Connection) -> PortResult<()> {
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS tool_activities (
                 activity_id TEXT PRIMARY KEY,
                 message_id  TEXT NOT NULL,
                 kind        TEXT NOT NULL,
                 actor       TEXT NOT NULL,
                 name        TEXT NOT NULL,
                 target      TEXT,
                 status      TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS tool_activities_message ON tool_activities(message_id);
             CREATE INDEX IF NOT EXISTS tool_activities_kind ON tool_activities(kind);
             CREATE INDEX IF NOT EXISTS tool_activities_name ON tool_activities(name);
             CREATE TABLE IF NOT EXISTS tool_activity_membership (
                 source_path TEXT NOT NULL,
                 activity_id TEXT NOT NULL,
                 PRIMARY KEY(source_path, activity_id)
             );
             CREATE INDEX IF NOT EXISTS tool_activity_membership_activity
             ON tool_activity_membership(activity_id);
             PRAGMA user_version = 12;",
        )
        .map_err(backend)?;
        tx.commit().map_err(backend)
    }

    /// Add the v13 session-title display projection in one explicit
    /// transaction (additive, non-destructive).
    ///
    /// `session_titles` stores the derived display title per canonical Session
    /// （借鉴清单 #6 的 custom-title > ai-title > 首条有效 user 派生链，
    /// ≤[`SESSION_TITLE_MAX_CHARS`] 字符）。与 `session_fts` 同属"catalog +
    /// claims 可重建投影"：旧库迁到 v13 后表为空，由 rebuild 或后续 affected
    /// source 提交回填。`user_version = 13` 与 DDL 同事务。
    fn migrate_v12_to_v13(conn: &Connection) -> PortResult<()> {
        Self::migrate_v12_to_v13_inner(conn, false)
    }

    fn migrate_v12_to_v13_inner(conn: &Connection, inject_failure: bool) -> PortResult<()> {
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS session_titles (
                 session_wire TEXT PRIMARY KEY,
                 title        TEXT NOT NULL
             );
             PRAGMA user_version = 13;",
        )
        .map_err(backend)?;

        if inject_failure {
            return Err(PortError::Backend(
                "injected v12-to-v13 migration failure".into(),
            ));
        }

        tx.commit().map_err(backend)
    }

    /// Add the v14 `parser_version` column to `source_scans` (additive,
    /// non-destructive).
    ///
    /// 借鉴 Recall 的 parser_version 增量同步（usage/event parser_version
    /// 三层判断）：任何改变已索引内容的解析语义升级都递增
    /// [`PARSER_SEMANTIC_VERSION`]，sync 的 unchanged 判定把存储的
    /// parser_version 纳入比较——版本落后的源即使字节未变也走 targeted
    /// backfill（重跑 parse + commit），不再依赖手动 `index rebuild` 或源
    /// 文件变化。旧行 DEFAULT 0——0 永不等于当前版本（≥1），因此迁移后
    /// 第一次 sync 自动 backfill 全部已扫源。列存在即无害：未升级的 v13
    /// 代码路径忽略它。新库在 v5 建表 DDL 已带本列（v9 provider_id 同一
    /// 模式），此处短路只对齐 user_version。`user_version = 14` 与 DDL
    /// 同事务。
    fn migrate_v13_to_v14(conn: &Connection) -> PortResult<()> {
        Self::migrate_v13_to_v14_inner(conn, false)
    }

    fn migrate_v13_to_v14_inner(conn: &Connection, inject_failure: bool) -> PortResult<()> {
        let has_parser_version = conn
            .prepare("PRAGMA table_info(source_scans)")
            .map_err(backend)?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
            .iter()
            .any(|name| name == "parser_version");
        if has_parser_version {
            // 已有列（新库建表时已带或本迁移重跑）；只对齐 user_version。
            conn.execute_batch("PRAGMA user_version = 14;")
                .map_err(backend)?;
            return Ok(());
        }
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "ALTER TABLE source_scans ADD COLUMN parser_version INTEGER NOT NULL DEFAULT 0;
             PRAGMA user_version = 14;",
        )
        .map_err(backend)?;

        if inject_failure {
            return Err(PortError::Backend(
                "injected v13-to-v14 migration failure".into(),
            ));
        }

        tx.commit().map_err(backend)
    }

    /// Add the v15 token-usage projection in one explicit transaction
    /// (additive, non-destructive).
    ///
    /// `usage_events` stores typed token-usage observations anchored to stable
    /// session wire ids (`message_id` nullable：provider 逐消息给出时挂消息，
    /// session 级累计事件挂会话）；`usage_event_membership` records per-source
    /// claims so the lifecycle mirrors `tool_activities`（complete-scan replace,
    /// incomplete-scan union, tombstone via claims）。五桶非负、`token_source`
    /// 只允许 observed/derived（覆盖标记：行存在 = provider 报过用量，真 0 与
    /// 未知可区分）。步骤只依赖 v7+ 表，在 v7..=14 的任意 catalog 上都能干净
    /// 运行——与并行 schema 分支 merge-safe。`user_version = 15` 与 DDL 同事务。
    fn migrate_v14_to_v15(conn: &Connection) -> PortResult<()> {
        Self::migrate_v14_to_v15_inner(conn, false)
    }

    fn migrate_v14_to_v15_inner(conn: &Connection, inject_failure: bool) -> PortResult<()> {
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS usage_events (
                 usage_id           TEXT PRIMARY KEY,
                 session_id         TEXT NOT NULL,
                 message_id         TEXT,
                 input_tokens       INTEGER NOT NULL CHECK(input_tokens >= 0),
                 output_tokens      INTEGER NOT NULL CHECK(output_tokens >= 0),
                 cache_read_tokens  INTEGER NOT NULL CHECK(cache_read_tokens >= 0),
                 cache_write_tokens INTEGER NOT NULL CHECK(cache_write_tokens >= 0),
                 reasoning_tokens   INTEGER NOT NULL CHECK(reasoning_tokens >= 0),
                 token_source       TEXT NOT NULL
                     CHECK(token_source IN ('observed', 'derived'))
             );
             CREATE INDEX IF NOT EXISTS usage_events_session ON usage_events(session_id);
             CREATE INDEX IF NOT EXISTS usage_events_message ON usage_events(message_id);
             CREATE TABLE IF NOT EXISTS usage_event_membership (
                 source_path TEXT NOT NULL,
                 usage_id    TEXT NOT NULL,
                 PRIMARY KEY(source_path, usage_id)
             );
             CREATE INDEX IF NOT EXISTS usage_event_membership_usage
             ON usage_event_membership(usage_id);
             PRAGMA user_version = 15;",
        )
        .map_err(backend)?;

        if inject_failure {
            return Err(PortError::Backend(
                "injected v14-to-v15 migration failure".into(),
            ));
        }

        tx.commit().map_err(backend)
    }

    /// Add the v16 repo-identity projection in one explicit transaction
    /// (additive, non-destructive).
    ///
    /// `session_repo_slugs` stores the privacy-safe `host/owner/name` slug
    /// derived from each Session's pair-observed working directory via the
    /// injected [`RepoSlugResolver`]（git rev-parse --show-toplevel +
    /// remote get-url origin，Recall 同款）。绝对路径绝不落此表——只有三段
    /// slug。行存在 = 检测成功；无行 = 未知/未派生（诚实降级，不猜）。
    /// 生命周期与 `session_fts` 同一重建批次（affected-session commit +
    /// rebuild 同事务），session 退役时同事务删除。旧库迁到 v16 后表为空，
    /// 由 rebuild 或后续 affected source 提交回填。步骤只依赖 v7+ 表，在
    /// v7..=15 的任意 catalog 上都能干净运行——与并行 schema 分支
    /// merge-safe。`user_version = 16` 与 DDL 同事务。
    fn migrate_v15_to_v16(conn: &Connection) -> PortResult<()> {
        Self::migrate_v15_to_v16_inner(conn, false)
    }

    fn migrate_v15_to_v16_inner(conn: &Connection, inject_failure: bool) -> PortResult<()> {
        let tx = conn.unchecked_transaction().map_err(backend)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS session_repo_slugs (
                 session_wire TEXT PRIMARY KEY,
                 repo_slug    TEXT NOT NULL
             );
             PRAGMA user_version = 16;",
        )
        .map_err(backend)?;

        if inject_failure {
            return Err(PortError::Backend(
                "injected v15-to-v16 migration failure".into(),
            ));
        }

        tx.commit().map_err(backend)
    }

    /// Add the v17 `store_metadata.index_projection_version` column (additive,
    /// non-destructive) and stamp it honestly for this catalog.
    ///
    /// 该列是**库级**投影属性（见 [`INDEX_PROJECTION_VERSION`]）：它回答
    /// "现存 FTS 词元流与派生投影是哪个变换写的"。它不是 per-source 事实
    /// （不像 `source_scans.parser_version`）——一次重投影重写整库的每一行，
    /// 部分迁移状态没有可自洽的答案，故落在 singleton `store_metadata`，与
    /// `active_generation` 同表同语义层级。
    ///
    /// 迁移期的标记取自**可观测事实**而非库版本：投影为空（`fts` 与
    /// `session_fts` 都无行）说明没有任何旧变换写下的词元，直接标记当前版本
    /// （新库/未索引库因此不会在第一次打开时被判失配）；投影非空的旧库保持
    /// DEFAULT 0——0 永不等于当前版本（≥1），因此写路径打开时自动重投影
    /// （[`SqliteStore::ensure_index_projection_current`]），读路径 fail-closed
    /// 而不是静默返回错误命中集。`user_version = 17` 与 DDL 同事务。
    fn migrate_v16_to_v17(conn: &Connection) -> PortResult<()> {
        Self::migrate_v16_to_v17_inner(conn, false)
    }

    fn migrate_v16_to_v17_inner(conn: &Connection, inject_failure: bool) -> PortResult<()> {
        let has_column = conn
            .prepare("PRAGMA table_info(store_metadata)")
            .map_err(backend)?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)?
            .iter()
            .any(|name| name == "index_projection_version");
        let tx = conn.unchecked_transaction().map_err(backend)?;
        if !has_column {
            // 新库在 v2 建表 DDL 已带本列（v5 parser_version / v9 provider_id
            // 同一模式）；旧库在此加法扩展。
            tx.execute_batch(
                "ALTER TABLE store_metadata
                 ADD COLUMN index_projection_version INTEGER NOT NULL DEFAULT 0;",
            )
            .map_err(backend)?;
        }
        if Self::index_projection_is_empty_in_tx(&tx)? {
            tx.execute(
                "UPDATE store_metadata SET index_projection_version = ?1 WHERE singleton = 1",
                [i64::from(INDEX_PROJECTION_VERSION)],
            )
            .map_err(backend)?;
        }
        tx.execute_batch("PRAGMA user_version = 17;")
            .map_err(backend)?;

        if inject_failure {
            return Err(PortError::Backend(
                "injected v16-to-v17 migration failure".into(),
            ));
        }

        tx.commit().map_err(backend)
    }

    /// True when no derived FTS projection row exists（`fts` 与 `session_fts`
    /// 都为空）：此时不存在任何旧变换写下的词元，投影版本可无条件标记为当前。
    fn index_projection_is_empty_in_tx(conn: &Connection) -> PortResult<bool> {
        let empty: bool = conn
            .query_row(
                "SELECT NOT EXISTS(SELECT 1 FROM fts)
                        AND NOT EXISTS(SELECT 1 FROM session_fts)",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        Ok(empty)
    }

    /// 声明语义向量归属的模型 id（#3）。未设置时 `SemanticIndex` 全部方法
    /// 视为未配置：`is_ready` 为 false、查询返回空、写入报错——这样"忘了配模型"
    /// 不会变成往表里写无归属向量。
    pub fn set_semantic_model(&self, model_id: impl Into<String>) {
        *self.semantic_model_id.borrow_mut() = Some(model_id.into());
    }

    /// 注入 repo slug 解析器（schema v16）。写路径（sync/index）在提交前
    /// 注入真实 git 实现；未注入时投影恒为空（诚实降级，不猜）。
    ///
    /// 可以在 [`open_for_write`](Self::open_for_write) 之后注入：打开时的投影
    /// 版本自愈不重派生该投影（见 [`RepoIdentityRebuild`]），因此注入时机不会
    /// 让已探测出的 repo 身份被删空。
    pub fn set_repo_slug_resolver(&self, resolver: Box<dyn RepoSlugResolver>) {
        *self.repo_slug_resolver.borrow_mut() = resolver;
    }

    /// 清除当前模型下的全部向量（换模型或 rebuild 语义索引时使用）。
    /// 返回删除行数。向量表是投影而非权威数据，清除永不影响 catalog。
    pub fn clear_embeddings(&self, model_id: &str) -> PortResult<usize> {
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(backend)?;
        let n = tx
            .execute("DELETE FROM message_vec WHERE model_id = ?1", [model_id])
            .map_err(backend)?;
        if n > 0 {
            Self::advance_generation_in_tx(&tx)?;
        }
        tx.commit().map_err(backend)?;
        Ok(n)
    }

    /// Atomically replace one model's projection from bounded catalog keyset
    /// batches. The encoder is called only for Messages and must not access
    /// this store while its transaction is open. Returns indexed/skipped/cleared.
    pub fn rebuild_embeddings_from_catalog(
        &self,
        model_id: &str,
        dimension: usize,
        batch_size: usize,
        mut encode: impl FnMut(&CatalogEntry) -> PortResult<Option<Vec<f32>>>,
    ) -> PortResult<(usize, usize, usize)> {
        if model_id.is_empty() || dimension == 0 || !(1..=512).contains(&batch_size) {
            return Err(PortError::Backend(
                "invalid embedding rebuild configuration".into(),
            ));
        }
        let dimension = i64::try_from(dimension).map_err(backend)?;
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(backend)?;
        let cleared = tx
            .execute("DELETE FROM message_vec WHERE model_id = ?1", [model_id])
            .map_err(backend)?;
        let mut indexed = 0;
        let mut skipped = 0;
        {
            let mut read = tx
                .prepare("SELECT id, payload FROM catalog WHERE id > ?1 ORDER BY id LIMIT ?2")
                .map_err(backend)?;
            let mut write = tx
                .prepare(
                    "INSERT INTO message_vec(wire_id, model_id, dimension, embedding)
                 VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT(wire_id) DO UPDATE SET
                     model_id = excluded.model_id,
                     dimension = excluded.dimension,
                     embedding = excluded.embedding",
                )
                .map_err(backend)?;
            let mut after = String::new();
            loop {
                let batch = read
                    .query_map(rusqlite::params![after, batch_size as i64], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })
                    .map_err(backend)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(backend)?;
                if batch.is_empty() {
                    break;
                }
                for (wire, payload) in batch {
                    let id = StableId::from_wire(&wire).ok_or_else(|| {
                        PortError::Backend("catalog contains an invalid entity id".into())
                    })?;
                    after = wire;
                    if id.kind() != IdKind::Message {
                        skipped += 1;
                        continue;
                    }
                    let Some(vector) = encode(&CatalogEntry { id, payload })? else {
                        skipped += 1;
                        continue;
                    };
                    if vector.len() != dimension as usize
                        || vector.iter().any(|value| !value.is_finite())
                    {
                        return Err(PortError::Backend(
                            "embedding has invalid dimension or non-finite values".into(),
                        ));
                    }
                    write
                        .execute(rusqlite::params![
                            after,
                            model_id,
                            dimension,
                            f32_slice_to_bytes(&vector)
                        ])
                        .map_err(backend)?;
                    indexed += 1;
                }
            }
        }
        Self::advance_generation_in_tx(&tx)?;
        tx.commit().map_err(backend)?;
        Ok((indexed, skipped, cleared))
    }

    /// Batch-load role + sidechain facts for the given message wire ids.
    ///
    /// Role comes from the canonical message payload; sidechain is true when
    /// the message has at least one `message_placements.is_sidechain = 1` row.
    /// Missing messages are omitted (caller treats absence as unknown/false).
    pub fn message_facts_for(
        &self,
        message_ids: &[StableId],
    ) -> PortResult<Vec<(String, String, bool)>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.borrow();
        let wires: Vec<&str> = message_ids.iter().map(|id| id.as_str()).collect();
        let mut out = Vec::new();
        for chunk in chunk_ids(&wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            // Role from catalog payload JSON; sidechain via EXISTS on placements.
            // `json_valid` 门必须在 `json_extract` 之前：catalog 里合法存在
            // 非 JSON payload（切片期 `index <fact> <text>` 写入的裸文本），
            // 直接 json_extract 会让整条语句以 "malformed JSON" 失败。
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT c.id,
                            COALESCE(
                                CASE WHEN json_valid(c.payload)
                                     THEN json_extract(c.payload, '$.role') END,
                                'unknown'
                            ),
                            EXISTS(
                              SELECT 1 FROM message_placements mp
                              WHERE mp.message_id = c.id AND mp.is_sidechain = 1
                            )
                     FROM catalog c
                     WHERE c.id IN ({placeholders})
                     ORDER BY c.id"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)? != 0,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                out.push(row.map_err(backend)?);
            }
        }
        Ok(out)
    }

    /// Batch-load tool activities for the given message wire ids (handoff pack).
    ///
    /// Returns one JSON object per activity, stable-ordered by
    /// `(message_id, activity_id)`. Unknown message ids contribute nothing.
    pub fn tool_activities_for_messages(
        &self,
        message_ids: &[StableId],
    ) -> PortResult<Vec<serde_json::Value>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.borrow();
        let wires: Vec<&str> = message_ids.iter().map(|id| id.as_str()).collect();
        let mut out = Vec::new();
        for chunk in chunk_ids(&wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT activity_id, message_id, kind, actor, name, target, status
                     FROM tool_activities
                     WHERE message_id IN ({placeholders})
                     ORDER BY message_id, activity_id"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (activity_id, message_id, kind, actor, name, target, status) =
                    row.map_err(backend)?;
                out.push(serde_json::json!({
                    "activity_id": activity_id,
                    "message_id": message_id,
                    "kind": kind,
                    "actor": actor,
                    "name": name,
                    "target": target,
                    "status": status,
                }));
            }
        }
        Ok(out)
    }

    /// 全库 token 用量聚合（usage 维度只读投影，status 展示用）。
    ///
    /// 覆盖标记原则：`sessions == 0` 表示库中没有任何 usage 事实（未知），
    /// 而不是"用量为零"——真 0 与未知必须可区分（agentsview has_*_tokens
    /// 同义）。事件计数按 token_source 分列（observed/derived）。
    pub fn usage_totals(&self) -> PortResult<Option<UsageTotals>> {
        let conn = self.conn.borrow();
        // usage_events 表只在 v15 迁移后存在；无投影返回 None（诚实区分
        // "无投影"与"有投影但零事实"）。
        let has_table: bool = conn
            .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='usage_events'")
            .map_err(backend)?
            .query_row([], |_| Ok(true))
            .optional()
            .map_err(backend)?
            .unwrap_or(false);
        if !has_table {
            return Ok(None);
        }
        let totals: (i64, i64, i64, i64, i64, i64, i64, i64) = conn
            .query_row(
                "SELECT COUNT(DISTINCT session_id),
                        COALESCE(SUM(input_tokens), 0),
                        COALESCE(SUM(output_tokens), 0),
                        COALESCE(SUM(cache_read_tokens), 0),
                        COALESCE(SUM(cache_write_tokens), 0),
                        COALESCE(SUM(reasoning_tokens), 0),
                        COALESCE(SUM(CASE WHEN token_source = 'observed' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE WHEN token_source = 'derived' THEN 1 ELSE 0 END), 0)
                 FROM usage_events",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .map_err(backend)?;
        let to_u64 = |value: i64| u64::try_from(value.max(0)).map_err(backend);
        Ok(Some(UsageTotals {
            sessions: to_u64(totals.0)?,
            input_tokens: to_u64(totals.1)?,
            output_tokens: to_u64(totals.2)?,
            cache_read_tokens: to_u64(totals.3)?,
            cache_write_tokens: to_u64(totals.4)?,
            reasoning_tokens: to_u64(totals.5)?,
            observed_events: to_u64(totals.6)?,
            derived_events: to_u64(totals.7)?,
        }))
    }

    /// 全库 repo 身份聚合（schema v16 只读投影，status 展示用）。
    ///
    /// 每 slug 一行 `(repo_slug, sessions)`，会话数降序、slug 升序
    /// tiebreak（确定性）。无投影行 → 空列表（未知 ≠ 零——绝不把
    /// "没有 repo 事实"说成"零个仓库"）。
    pub fn repo_totals(&self) -> PortResult<Vec<RepoTotals>> {
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare(
                "SELECT repo_slug, COUNT(*)
                 FROM session_repo_slugs
                 GROUP BY repo_slug
                 ORDER BY COUNT(*) DESC, repo_slug ASC",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(backend)?;
        let mut totals = Vec::new();
        for row in rows {
            let (repo_slug, sessions) = row.map_err(backend)?;
            totals.push(RepoTotals {
                repo_slug,
                sessions: u64::try_from(sessions.max(0)).map_err(backend)?,
            });
        }
        Ok(totals)
    }

    /// v12 工具活动投影的孤儿扫描（只读，doctor/维护证据）：
    /// 返回 `(孤儿活动行数, 孤儿成员行数)`。
    ///
    /// - 孤儿活动：`tool_activities` 行没有对应的 catalog 消息行。活动是消息
    ///   的投影，消息退役时其活动由 claims 推导同事务删除（见
    ///   [`commit_source_batches_if_changed`]）；残余行是投影漂移证据。
    /// - 孤儿成员：`tool_activity_membership` 行指向不存在的活动（悬空 claim）。
    ///
    /// 两类行都无法再从 catalog 重建，是确定性修剪
    /// （[`purge_orphaned_activities`](Self::purge_orphaned_activities)）的输入。
    pub fn orphaned_activity_counts(&self) -> PortResult<(u64, u64)> {
        let conn = self.conn.borrow();
        let activities: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_activities ta
                 WHERE NOT EXISTS (
                     SELECT 1 FROM catalog c WHERE c.id = ta.message_id
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        let memberships: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_activity_membership m
                 WHERE NOT EXISTS (
                     SELECT 1 FROM tool_activities ta
                     WHERE ta.activity_id = m.activity_id
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        Ok((
            u64::try_from(activities).map_err(backend)?,
            u64::try_from(memberships).map_err(backend)?,
        ))
    }

    /// v15 usage 投影的孤儿扫描（只读，doctor/维护证据）：
    /// 返回 `(孤儿用量行数, 孤儿成员行数)`。
    ///
    /// - 孤儿用量：`usage_events` 行没有对应的 catalog 会话行。用量是会话
    ///   的投影，会话退役时其事件由 claims 推导同事务删除；残余行是投影
    ///   漂移证据。
    /// - 孤儿成员：`usage_event_membership` 行指向不存在的事件（悬空 claim）。
    pub fn orphaned_usage_counts(&self) -> PortResult<(u64, u64)> {
        let conn = self.conn.borrow();
        let events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_events ue
                 WHERE NOT EXISTS (
                     SELECT 1 FROM catalog c WHERE c.id = ue.session_id
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        let memberships: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_event_membership m
                 WHERE NOT EXISTS (
                     SELECT 1 FROM usage_events ue
                     WHERE ue.usage_id = m.usage_id
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        Ok((
            u64::try_from(events).map_err(backend)?,
            u64::try_from(memberships).map_err(backend)?,
        ))
    }

    /// 确定性修剪孤儿工具活动行（v12 保留策略的维护路径）。
    ///
    /// 活动是 catalog 的投影：正常写入路径里，source 退役与消息 tombstone 会
    /// 在同一事务内推导并删除其活动与 claim；本方法只删除漂移残余——没有
    /// catalog 消息的活动行、指向不存在活动的成员行——绝不触碰仍锚定在
    /// catalog 消息上的活动及其合法 claim，也不触碰 catalog/FTS/session 元数据。
    ///
    /// 与 [`rebuild_index`](Self::rebuild_index) 同一 writer 纪律：durable
    /// intent（CAS base generation）→ 单事务校验 + 删除 + 推进 generation +
    /// activate，outbox 行即本次维护操作的审计记录。返回 `(删除活动行数,
    /// 删除成员行数)`——成员行数含随孤儿活动删除而级联清除的 claim。无孤儿时
    /// 不写库（返回 `(0, 0)`，generation 不动）——修剪是收敛操作，空跑不
    /// 产生 journal churn。
    ///
    /// v15 起同事务一并修剪孤儿 usage 投影行（会话退役后的漂移残余，同一
    /// 维护语义）：删除没有 catalog 会话的 `usage_events` 行与悬空
    /// `usage_event_membership` claim。返回值仍只报告活动行数（对外契约
    /// 不变）；usage 修剪结果经 [`orphaned_usage_counts`](Self::orphaned_usage_counts)
    /// 复核。
    pub fn purge_orphaned_activities(&self) -> PortResult<(u64, u64)> {
        let (activities, memberships) = self.orphaned_activity_counts()?;
        let (orphaned_usages, orphaned_usage_memberships) = self.orphaned_usage_counts()?;
        if activities == 0
            && memberships == 0
            && orphaned_usages == 0
            && orphaned_usage_memberships == 0
        {
            return Ok((0, 0));
        }
        // 空变更集的 durable intent：与 rebuild 同一条 CAS 前置条件
        // （active_generation == base），防止并发写者抢先推进后误修剪。
        let pending = self.begin_index_batch(&[], &[])?;
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(backend)?;
        let relations = RelationManifests::default();
        let manifest = batch_manifest(&[], &[], &relations)?;
        Self::verify_pending_in_tx(&tx, &pending, &[], &[], &relations, &manifest)?;
        // 确定性删除：谓词自包含，只删事务时刻仍然悬空的行（与扫描同一谓词）。
        // 先删孤儿活动行，再清悬空 claim——claim 的悬空定义是"指向不存在的
        // 活动"，第二个语句同时覆盖预先悬空的 claim 与刚删活动的 claim；
        // 反序会在删除活动后留下新悬空 claim。
        let removed_activities = tx
            .execute(
                "DELETE FROM tool_activities
                 WHERE NOT EXISTS (
                     SELECT 1 FROM catalog c WHERE c.id = tool_activities.message_id
                 )",
                [],
            )
            .map_err(backend)?;
        let removed_memberships = tx
            .execute(
                "DELETE FROM tool_activity_membership
                 WHERE activity_id NOT IN (SELECT activity_id FROM tool_activities)",
                [],
            )
            .map_err(backend)?;
        // v15 usage 投影修剪：先删没有 catalog 会话的事件行，再清悬空 claim
        // （与活动同一顺序语义——先删事件行，claim 的悬空定义才会同时覆盖
        // 预先悬空与刚删行的 claim）。
        tx.execute(
            "DELETE FROM usage_events
             WHERE NOT EXISTS (
                 SELECT 1 FROM catalog c WHERE c.id = usage_events.session_id
             )",
            [],
        )
        .map_err(backend)?;
        tx.execute(
            "DELETE FROM usage_event_membership
             WHERE usage_id NOT IN (SELECT usage_id FROM usage_events)",
            [],
        )
        .map_err(backend)?;
        tx.execute(
            "UPDATE store_metadata SET active_generation = ?1 WHERE singleton = 1",
            [pending.target_generation as i64],
        )
        .map_err(backend)?;
        tx.execute(
            "UPDATE index_batches
             SET state = 'activated', durable_point = 'activated', committed_at_ms = ?2
             WHERE operation_id = ?1",
            rusqlite::params![pending.operation_id, unix_ms()?],
        )
        .map_err(backend)?;
        tx.commit().map_err(backend)?;
        Ok((
            u64::try_from(removed_activities).map_err(backend)?,
            u64::try_from(removed_memberships).map_err(backend)?,
        ))
    }

    /// 当前存储读回的 schema 版本（供 doctor/诊断）。
    pub fn schema_version(&self) -> PortResult<i64> {
        self.conn
            .borrow()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(backend)
    }

    /// 按 wire id 升序分页列出 catalog 实体；`kind` 为 `Some` 时只返回该 kind
    /// 的实体（SQL 前缀过滤，使 offset/limit 作用于过滤后的集合——见
    /// `CatalogStore::list_sessions` 的分页语义约束）。
    fn list_filtered(
        conn: &RefCell<Connection>,
        kind: Option<IdKind>,
        limit: usize,
    ) -> PortResult<Vec<CatalogEntry>> {
        let conn = conn.borrow();
        // 前缀是受控常量（`ses_v1_` 等），拼进 SQL 不会引入注入面；避免按
        // `? IS NULL OR id LIKE ?` 形式传参，让查询规划器对两种形状都走索引。
        let sql = match kind {
            Some(kind) => format!(
                "SELECT id, payload FROM catalog WHERE id LIKE '{}%' ORDER BY id ASC LIMIT ?1",
                kind.prefix()
            ),
            None => "SELECT id, payload FROM catalog ORDER BY id ASC LIMIT ?1".to_string(),
        };
        let mut stmt = conn.prepare(&sql).map_err(backend)?;
        let rows = stmt
            .query_map([limit as i64], |row| {
                let wire: String = row.get(0)?;
                let payload: Vec<u8> = row.get(1)?;
                Ok((wire, payload))
            })
            .map_err(backend)?;
        let mut entries = Vec::new();
        for row in rows {
            let (wire, payload) = row.map_err(backend)?;
            let id = StableId::from_wire(&wire).ok_or_else(|| {
                PortError::Backend("catalog contains an invalid entity id".into())
            })?;
            entries.push(CatalogEntry { id, payload });
        }
        Ok(entries)
    }

    /// 读取一批源路径的指纹缓存（source_scans 的 len/fingerprint 列 +
    /// parser_version）。
    ///
    /// 返回 `path -> (len_bytes, fingerprint, parser_version)`；从未扫描过的
    /// 源不在 map 中。CLI 用它跳过未变化源的重复解析（capture 后先比指纹与
    /// 解析语义版本，相同则不再 parse，直接按 no-op 处理）。
    pub fn source_fingerprints(
        &self,
        paths: &[String],
    ) -> PortResult<BTreeMap<String, SourceFingerprint>> {
        let conn = self.conn.borrow();
        let mut out = BTreeMap::new();
        for path in paths {
            let row: Option<SourceFingerprint> = conn
                .query_row(
                    "SELECT len_bytes, fingerprint, parser_version
                     FROM source_scans WHERE source_path = ?1",
                    [path],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(backend)?;
            if let Some(row) = row {
                out.insert(path.clone(), row);
            }
        }
        Ok(out)
    }

    /// 读取一批源路径已提交的 message 实体数（membership 中 msg_v1_ 行数）。
    ///
    /// CLI 在指纹缓存命中、跳过 parse 时用它上报 unchanged 消息数，保持
    /// `unchanged` 与 `emitted` 同单位（消息数）。
    pub fn source_message_counts(&self, paths: &[String]) -> PortResult<BTreeMap<String, usize>> {
        let conn = self.conn.borrow();
        let mut out = BTreeMap::new();
        for path in paths {
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM source_membership
                     WHERE source_path = ?1 AND message_id LIKE 'msg_v1_%'",
                    [path],
                    |row| row.get(0),
                )
                .map_err(backend)?;
            out.insert(path.clone(), count as usize);
        }
        Ok(out)
    }

    /// 列出某 provider 在 `source_scans` 中已记录的全部源路径（按路径升序）。
    ///
    /// `sync --discover` 用它做"prior-path diff"：先取该 provider 的已存路径，
    /// 与本次发现的路径比对——存在于已存但本次未出现在磁盘上的，说明源已被删除，
    /// 合成空批（`relation_complete = true`）即可触发 tombstone（R2）。
    ///
    /// `provider_id IS NULL` 的旧行（v8 前或未走 discover 的显式 sync）不会被
    /// 返回——它们对 discover 不可见，不会被误 tombstone（安全保守）。
    pub fn source_paths_for_provider(&self, provider_id: &str) -> PortResult<Vec<String>> {
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare(
                "SELECT source_path FROM source_scans
                 WHERE provider_id = ?1
                 ORDER BY source_path ASC",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([provider_id], |row| row.get::<_, String>(0))
            .map_err(backend)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(backend)?);
        }
        Ok(out)
    }

    /// Associate previously explicit-synced sources with providers resolved by
    /// canonical-root discovery. Existing associations are immutable.
    pub fn backfill_source_provider_ids(&self, sources: &[(String, String)]) -> PortResult<usize> {
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(backend)?;
        let changed = {
            let mut stmt = tx
                .prepare(
                    "UPDATE source_scans
                     SET provider_id = ?2
                     WHERE source_path = ?1 AND provider_id IS NULL",
                )
                .map_err(backend)?;
            let mut changed = 0usize;
            for (source_path, provider_id) in sources {
                changed += stmt.execute([source_path, provider_id]).map_err(backend)?;
            }
            changed
        };
        tx.commit().map_err(backend)?;
        Ok(changed)
    }

    /// 列出指定路径中缺少完整 relation marker 的源。
    ///
    /// `sync --discover` 在部分 root scan 后再次遇到同一字节源时，必须重新
    /// stage 它来恢复 `relation_complete`；否则普通 fingerprint skip 会让缺失
    /// marker 永远无法回填。
    pub fn source_paths_requiring_relation_scan(
        &self,
        paths: &[String],
    ) -> PortResult<BTreeSet<String>> {
        let conn = self.conn.borrow();
        let mut out = BTreeSet::new();
        for path in paths {
            let missing: bool = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM source_scans ss
                         WHERE ss.source_path = ?1
                           AND NOT EXISTS(
                               SELECT 1 FROM source_relation_scans rs
                               WHERE rs.source_path = ss.source_path
                           )
                     )",
                    [path],
                    |row| row.get(0),
                )
                .map_err(backend)?;
            if missing {
                out.insert(path.clone());
            }
        }
        Ok(out)
    }

    fn stable_id_from_store(conn: &Connection, wire: &str) -> PortResult<StableId> {
        let id_json: Option<String> = conn
            .query_row(
                "SELECT id_json FROM fts_ids WHERE wire_id = ?1",
                [wire],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        let id = match id_json {
            Some(json) => serde_json::from_str::<StableId>(&json).map_err(backend)?,
            None => StableId::from_wire(wire).ok_or_else(|| {
                PortError::Backend("catalog contains an invalid entity id".into())
            })?,
        };
        if id.as_str() != wire {
            return Err(PortError::Backend(
                "stored identity sidecar does not match its catalog key".into(),
            ));
        }
        Ok(id)
    }

    /// 批量加载场景下的身份解析：优先用已加载的 fts_ids 映射，缺失回退
    /// `from_wire`（与 `stable_id_from_store` 语义一致，避免逐条查询）。
    fn stable_id_from_wire(
        wire: &str,
        id_json_by_wire: &BTreeMap<String, String>,
    ) -> PortResult<StableId> {
        let id = match id_json_by_wire.get(wire) {
            Some(json) => serde_json::from_str::<StableId>(json).map_err(backend)?,
            None => StableId::from_wire(wire).ok_or_else(|| {
                PortError::Backend("catalog contains an invalid entity id".into())
            })?,
        };
        if id.as_str() != wire {
            return Err(PortError::Backend(
                "stored identity sidecar does not match its catalog key".into(),
            ));
        }
        Ok(id)
    }

    fn ensure_stored_identity_metadata_matches(
        &self,
        entries: &[(StableId, Vec<u8>, String)],
    ) -> PortResult<()> {
        let conn = self.conn.borrow();
        // prepare 提升到循环外：200K 实体 × 每次 prepare/finalize 的常数因子
        // 在批量提交里会被放大（见 commit_index_batch_with_relations 的同类 hoist）。
        let mut stmt = conn
            .prepare("SELECT id_json FROM fts_ids WHERE wire_id = ?1")
            .map_err(backend)?;
        for (id, _, _) in entries {
            let id_json: Option<String> = stmt
                .query_row([id.as_str()], |row| row.get(0))
                .optional()
                .map_err(backend)?;
            let Some(id_json) = id_json else {
                continue;
            };
            let stored_id: StableId = serde_json::from_str(&id_json)
                .map_err(|_| PortError::Backend("stored identity sidecar is not valid".into()))?;
            if &stored_id != id {
                return Err(PortError::Backend(
                    "entity has conflicting identity metadata with stored catalog".into(),
                ));
            }
        }
        Ok(())
    }

    fn relation_sources_for_session(
        conn: &Connection,
        session_id: &str,
    ) -> PortResult<BTreeSet<String>> {
        let mut stmt = conn
            .prepare(
                "SELECT source_path FROM source_membership WHERE message_id = ?1
                 UNION
                 SELECT claims.source_path
                 FROM source_placement_membership AS claims
                 JOIN message_placements AS placements
                   ON placements.placement_id = claims.placement_id
                 WHERE placements.session_id = ?1",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([session_id], |row| row.get::<_, String>(0))
            .map_err(backend)?;
        let mut sources = BTreeSet::new();
        for row in rows {
            sources.insert(row.map_err(backend)?);
        }
        Ok(sources)
    }

    fn relation_sources_for_message(
        conn: &Connection,
        message_id: &str,
    ) -> PortResult<BTreeSet<String>> {
        let mut stmt = conn
            .prepare(
                "SELECT source_path FROM source_membership WHERE message_id = ?1
                 UNION
                 SELECT claims.source_path
                 FROM source_placement_membership AS claims
                 JOIN message_placements AS placements
                   ON placements.placement_id = claims.placement_id
                 WHERE placements.message_id = ?1",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([message_id], |row| row.get::<_, String>(0))
            .map_err(backend)?;
        let mut sources = BTreeSet::new();
        for row in rows {
            sources.insert(row.map_err(backend)?);
        }
        Ok(sources)
    }

    fn require_relation_complete_sources(
        conn: &Connection,
        sources: &BTreeSet<String>,
        require_known_source: bool,
        subject: &str,
    ) -> PortResult<()> {
        if require_known_source && sources.is_empty() {
            return Err(PortError::SchemaIncompatible(format!(
                "{subject} contextual relations are unavailable; re-ingest required"
            )));
        }
        for source_path in sources {
            let complete: bool = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM source_relation_scans WHERE source_path = ?1
                     )",
                    [source_path],
                    |row| row.get(0),
                )
                .map_err(backend)?;
            if !complete {
                return Err(PortError::SchemaIncompatible(format!(
                    "{subject} contextual relations are incomplete; re-ingest required"
                )));
            }
        }
        Ok(())
    }

    /// 以 durable outbox 包裹一批 upsert，再原子提交 catalog + FTS + generation。
    ///
    /// 阶段一先持久化 intent；阶段二在单个 SQLite 事务中应用所有实体并激活目标
    /// generation。任一实体失败都会回滚整批数据；若进程在两阶段之间终止，下一次
    /// 写打开会把无副作用的 `building` intent 标记为 `aborted`。
    pub fn commit_batch(&self, entries: &[(StableId, Vec<u8>, String)]) -> PortResult<()> {
        self.commit_batch_if_changed(entries).map(|_| ())
    }

    /// Unscoped writes cannot change source-owned entities. Only a source
    /// replacement can update their observations and membership coherently.
    fn reject_source_owned_writes(
        conn: &Connection,
        upserts: &[SourceProjection],
        deletes: &[StableId],
    ) -> PortResult<()> {
        let ids: Vec<_> = upserts
            .iter()
            .map(|(id, _, _)| id.as_str())
            .chain(deletes.iter().map(StableId::as_str))
            .collect();
        for chunk in chunk_ids(&ids) {
            let claimed: bool = conn
                .query_row(
                    &format!(
                        "SELECT EXISTS(SELECT 1 FROM source_membership WHERE message_id IN ({}))",
                        in_placeholders(chunk.len())
                    ),
                    rusqlite::params_from_iter(chunk.iter()),
                    |row| row.get(0),
                )
                .map_err(backend)?;
            if claimed {
                return Err(PortError::InvalidRequest(
                    "source-owned entities must be updated through a source replacement".into(),
                ));
            }
        }
        Ok(())
    }

    /// Durable batch commit that reports whether a new generation was activated.
    ///
    /// `false` means every catalog payload and indexed text already matched the batch;
    /// no outbox row or generation was created.
    pub fn commit_batch_if_changed(
        &self,
        entries: &[(StableId, Vec<u8>, String)],
    ) -> PortResult<bool> {
        if entries.is_empty() {
            return Ok(false);
        }
        // Validate the complete change set before the no-op shortcut; duplicate IDs must
        // never be silently accepted just because the first copy is already current.
        Self::reject_source_owned_writes(&self.conn.borrow(), entries, &[])?;
        batch_manifest(entries, &[], &RelationManifests::default())?;
        self.ensure_stored_identity_metadata_matches(entries)?;
        if self.batch_is_current(entries)? {
            return Ok(false);
        }
        let pending = self.begin_index_batch(entries, &[])?;
        self.commit_index_batch(&pending, entries, &[])?;
        Ok(true)
    }

    fn batch_is_current(&self, entries: &[(StableId, Vec<u8>, String)]) -> PortResult<bool> {
        self.batch_is_current_with_derived_context(entries, false)
    }

    fn batch_is_current_with_derived_context(
        &self,
        entries: &[(StableId, Vec<u8>, String)],
        contextual_payloads_are_derived: bool,
    ) -> PortResult<bool> {
        let conn = self.conn.borrow();
        for (id, payload, text) in entries {
            let catalog_payload: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT payload FROM catalog WHERE id = ?1",
                    [id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            let Some(catalog_payload) = catalog_payload else {
                return Ok(false);
            };
            let payload_is_current = if contextual_payloads_are_derived
                && matches!(id.kind(), IdKind::Message | IdKind::Session)
            {
                intrinsic_payloads_equal(id.kind(), &catalog_payload, payload)
            } else {
                catalog_payload.as_slice() == payload.as_slice()
            };
            if !payload_is_current {
                return Ok(false);
            }
            let id_json: Option<String> = conn
                .query_row(
                    "SELECT id_json FROM fts_ids WHERE wire_id = ?1",
                    [id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            let Some(id_json) = id_json else {
                return Ok(false);
            };
            let stored_id: StableId = serde_json::from_str(&id_json)
                .map_err(|_| PortError::Backend("stored identity sidecar is not valid".into()))?;
            if &stored_id != id {
                return Ok(false);
            }
            // 非 Message 实体不进 fts 全文表（见 commit_index_batch_with_relations），
            // 其"内容一致"只看 catalog payload 与 fts_ids 身份边车。
            if id.kind() != IdKind::Message {
                continue;
            }
            // fts 的 `id` 是内容列（UNINDEXED），按它比较会让每条消息的 current
            // 判定整表扫描（B1 路径 O(N²)）；改经 fts_ids 边车的 fts_rowid 按
            // rowid O(1) 定位。边车缺行或 fts_rowid 为 NULL 时按“无 fts 行”处理
            // （rowid = NULL 匹配不到行）→ 不 current，提交路径会重建该行。
            let indexed_text: Option<String> = conn
                .query_row(
                    "SELECT text FROM fts
                     WHERE rowid = (SELECT fts_rowid FROM fts_ids WHERE wire_id = ?1)",
                    [id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            // fts 存的是 CJK n-gram（单字 + bigram）变换后的正文（见 fts 写入侧），
            // current 判定必须对同一 text 施加同一 transform 再比较，否则已同步
            // 的源每次重同步都被误判为 not-current、反复推进 generation。
            let expected = fts_tokens_cjk(text);
            if indexed_text.as_deref() != Some(expected.as_str()) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Commit source-owned entity and relation facts, reporting generation change.
    ///
    /// Complete relation scans replace both entity and placement claims and may
    /// derive tombstones. Incomplete scans union observed claims, derive no
    /// tombstones, and clear the source relation-completeness marker.
    pub fn commit_source_batches_if_changed(&self, sources: &[SourceBatch]) -> PortResult<bool> {
        let canonical_sources = self.canonicalize_source_batches(sources)?;
        let mut ordered_sources: Vec<&SourceBatch> = canonical_sources
            .iter()
            .map(|source| source.as_ref())
            .collect();
        ordered_sources.sort_by(|left, right| left.source_path.cmp(&right.source_path));
        let paths: Vec<&str> = ordered_sources
            .iter()
            .map(|source| source.source_path.as_str())
            .collect();
        if paths.windows(2).any(|window| window[0] == window[1]) {
            return Err(PortError::Backend(
                "source batch contains duplicate source paths".into(),
            ));
        }

        let mut trace_stages: Vec<(&'static str, std::time::Duration)> = Vec::new();
        let trace_source_count = ordered_sources.len();
        let trace_started = trace::begin();
        // A no-op still promises a valid batch. Validate incoming identities
        // and multiplicities before set/map comparison can erase duplicates.
        for source in &ordered_sources {
            self.installation_for_source_commit(source)?;
            batch_manifest(&source.entries, &[], &RelationManifests::default())?;
            self.ensure_stored_identity_metadata_matches(&source.entries)?;
            let mut placements = BTreeSet::new();
            let mut slots = BTreeSet::new();
            for placement in &source.placements {
                validate_placement(placement)?;
                if !placements.insert(placement.id.as_str())
                    || !slots.insert((
                        placement.session_id.as_str(),
                        placement.source_document_id.as_str(),
                        placement.source_ordinal,
                    ))
                {
                    return Err(PortError::Backend(
                        "source batch contains duplicate placement facts".into(),
                    ));
                }
            }
            let mut children = BTreeSet::new();
            for edge in &source.edges {
                validate_edge(edge)?;
                if !placements.contains(edge.child_placement_id.as_str())
                    || !children.insert(edge.child_placement_id.as_str())
                {
                    return Err(PortError::Backend(
                        "source batch contains invalid or duplicate edge children".into(),
                    ));
                }
            }
            let mut claim_sessions = BTreeSet::new();
            for claim in &source.resume_claims {
                if !claim_sessions.insert(&claim.session_id) {
                    return Err(PortError::Backend(
                        "source batch contains duplicate resume claims".into(),
                    ));
                }
            }
        }

        // Cheap no-op check: building the merged view, claimer graph,
        // and manifest below costs O(whole catalog). When every source in
        // this batch is already current (entries, relations, membership,
        // claims, scans), skip all of it and report no generation change.
        // The per-batch cost is then proportional to the batch, not the
        // catalog — this is what makes an unchanged re-sync fast.
        trace::add(&mut trace_stages, "validate_incoming", trace_started);
        let trace_started = trace::begin();
        let batch_current = self.sources_are_current(&ordered_sources)?;
        trace::add(&mut trace_stages, "noop_probe", trace_started);
        if batch_current {
            trace::emit(
                "adapter:catalog",
                &trace_stages,
                &format!("noop=true sources={trace_source_count}"),
            );
            return Ok(false);
        }

        let trace_started = trace::begin();
        let scanned_paths: BTreeSet<String> = paths.into_iter().map(str::to_string).collect();
        // Batch-scoped state: only this batch's own sources plus the ids they
        // observe/claim. Whole-catalog maps cost ~23 s and multi-GiB peak RSS on
        // the 6th 200k-message batch against a 1M catalog (measured 2026-09-29).
        let mut entity_candidates = BTreeSet::<String>::new();
        let mut placement_candidates = BTreeSet::<String>::new();
        let mut activity_candidates = BTreeSet::<String>::new();
        let mut usage_candidates = BTreeSet::<String>::new();
        for source in &ordered_sources {
            for (id, _, _) in &source.entries {
                entity_candidates.insert(id.as_str().to_string());
            }
            for placement in &source.placements {
                placement_candidates.insert(placement.id.as_str().to_string());
            }
            for activity in &source.activities {
                let stored = stored_activity_from(&activity.message_id, &activity.activity)?;
                activity_candidates.insert(stored.activity_id);
            }
            for usage in &source.usage_events {
                let stored =
                    stored_usage_from(&usage.session_id, usage.message_id.as_ref(), &usage.usage)?;
                usage_candidates.insert(stored.usage_id);
            }
        }
        let mut current_entities_by_source =
            self.source_entity_membership_state_for_sources(&scanned_paths)?;
        for memberships in current_entities_by_source.values() {
            for entity_id in memberships.keys() {
                entity_candidates.insert(entity_id.clone());
            }
        }
        self.extend_entity_claims_for_candidates(
            &mut current_entities_by_source,
            &entity_candidates,
            &scanned_paths,
        )?;
        let mut current_placements_by_source =
            self.source_placement_membership_state_for_sources(&scanned_paths)?;
        for placement_ids in current_placements_by_source.values() {
            for placement_id in placement_ids {
                placement_candidates.insert(placement_id.clone());
            }
        }
        self.extend_placement_claims_for_candidates(
            &mut current_placements_by_source,
            &placement_candidates,
            &scanned_paths,
        )?;
        let mut current_activities_by_source =
            self.source_activity_membership_state_for_sources(&scanned_paths)?;
        for activity_ids in current_activities_by_source.values() {
            for activity_id in activity_ids {
                activity_candidates.insert(activity_id.clone());
            }
        }
        self.extend_activity_claims_for_candidates(
            &mut current_activities_by_source,
            &activity_candidates,
            &scanned_paths,
        )?;
        let mut current_usages_by_source =
            self.source_usage_membership_state_for_sources(&scanned_paths)?;
        for usage_ids in current_usages_by_source.values() {
            for usage_id in usage_ids {
                usage_candidates.insert(usage_id.clone());
            }
        }
        self.extend_usage_claims_for_candidates(
            &mut current_usages_by_source,
            &usage_candidates,
            &scanned_paths,
        )?;
        let mut projections =
            Self::load_source_projections(&self.conn.borrow(), &entity_candidates)?;
        // An unscanned claimant may attribute the affected message to a document
        // outside the aggregate candidates. Load only those referenced proofs;
        // do not broaden the merge set or infer evidence from catalog aliases.
        let proof_candidates = current_entities_by_source
            .values()
            .flat_map(|memberships| memberships.values().flatten())
            .filter(|document| !entity_candidates.contains(*document))
            .cloned()
            .collect();
        for (source, documents) in
            Self::load_source_projections(&self.conn.borrow(), &proof_candidates)?
        {
            projections.entry(source).or_default().extend(documents);
        }
        let stored_placements = self.stored_placements_for(&placement_candidates)?;
        let stored_edges = self.stored_edges_for(&placement_candidates)?;
        let stored_activities = self.stored_activities_for(&activity_candidates)?;
        let stored_usages = self.stored_usages_for(&usage_candidates)?;
        trace::add(&mut trace_stages, "load_catalog_state", trace_started);

        let mut merged = BTreeMap::<String, (StableId, Vec<u8>, String)>::new();
        let mut observed_placements = BTreeMap::<String, MessagePlacement>::new();
        let mut observed_edges = BTreeMap::<String, MessageEdge>::new();
        let mut observed_activities = BTreeMap::<String, StoredActivity>::new();
        let mut observed_usages = BTreeMap::<String, StoredUsage>::new();
        let mut prepared_sources = BTreeMap::<String, PreparedSource>::new();

        let trace_started = trace::begin();

        for source in ordered_sources {
            let present: BTreeSet<&str> = source
                .entries
                .iter()
                .map(|(id, _, _)| id.as_str())
                .collect();
            if present.len() != source.entries.len() {
                return Err(PortError::Backend(
                    "source batch contains duplicate message ids".into(),
                ));
            }

            let document_id = source
                .entries
                .iter()
                .find(|(id, _, _)| id.kind() == IdKind::Document)
                .map(|(id, _, _)| id.as_str().to_string());
            let incoming_entities: BTreeMap<String, Option<String>> = source
                .entries
                .iter()
                .map(|(id, _, _)| (id.as_str().to_string(), document_id.clone()))
                .collect();
            let prior_entity_memberships = current_entities_by_source
                .get(&source.source_path)
                .cloned()
                .unwrap_or_default();
            let mut final_entities = if source.relation_complete {
                BTreeMap::new()
            } else {
                prior_entity_memberships.clone()
            };
            final_entities.extend(incoming_entities);

            let mut source_placements = BTreeMap::new();
            let mut placement_slots = BTreeSet::new();
            for placement in &source.placements {
                validate_placement(placement)?;
                let placement_id = placement.id.as_str().to_string();
                if source_placements
                    .insert(placement_id.clone(), placement.clone())
                    .is_some()
                {
                    return Err(PortError::Backend(
                        "source batch contains duplicate placement ids".into(),
                    ));
                }
                let slot = (
                    placement.session_id.as_str().to_string(),
                    placement.source_document_id.as_str().to_string(),
                    placement.source_ordinal,
                );
                if !placement_slots.insert(slot) {
                    return Err(PortError::Backend(
                        "source batch contains duplicate placement ordinals".into(),
                    ));
                }
                if let Some(existing) = observed_placements.get(&placement_id) {
                    if existing != placement {
                        return Err(PortError::Backend(format!(
                            "placement {placement_id} has conflicting projections across sources"
                        )));
                    }
                } else {
                    observed_placements.insert(placement_id, placement.clone());
                }
            }

            let mut source_edges = BTreeMap::new();
            for edge in &source.edges {
                validate_edge(edge)?;
                let placement_id = edge.child_placement_id.as_str().to_string();
                if !source_placements.contains_key(&placement_id) {
                    return Err(PortError::Backend(
                        "source batch edge does not belong to an observed placement".into(),
                    ));
                }
                if source_edges
                    .insert(placement_id.clone(), edge.clone())
                    .is_some()
                {
                    return Err(PortError::Backend(
                        "source batch contains duplicate edge children".into(),
                    ));
                }
                if let Some(existing) = observed_edges.get(&placement_id) {
                    if existing != edge {
                        return Err(PortError::Backend(format!(
                            "edge {placement_id} has conflicting projections across sources"
                        )));
                    }
                } else {
                    observed_edges.insert(placement_id, edge.clone());
                }
            }

            let prior_placement_ids = current_placements_by_source
                .get(&source.source_path)
                .cloned()
                .unwrap_or_default();
            let mut final_placement_ids = if source.relation_complete {
                BTreeSet::new()
            } else {
                prior_placement_ids.clone()
            };
            final_placement_ids.extend(source_placements.keys().cloned());

            // 工具活动（v12）：派生活动 id、校验锚点，跨源事实冲突拒绝。
            let mut source_activities = BTreeMap::new();
            for source_activity in &source.activities {
                let stored =
                    stored_activity_from(&source_activity.message_id, &source_activity.activity)?;
                // 活动 id 对全部事实内容寻址（activity_id_for），所以同 id 蕴含同事实：
                // 一条消息里两次完全相同的工具调用（同 kind/actor/name/target/status，
                // 例如连续读同一文件、或 target 截断后相同）本就是同一检索面事实的
                // 重复观察，按 id 去重而非当作冲突——这与跨源副本走同一去重规则。
                // 仅当同 id 行事实不同（哈希碰撞或派生逻辑漂移）才 fail-closed。
                if let Some(existing) = source_activities.get(&stored.activity_id) {
                    if existing != &stored {
                        return Err(PortError::Backend(format!(
                            "activity {} has conflicting facts within one source",
                            stored.activity_id
                        )));
                    }
                } else {
                    source_activities.insert(stored.activity_id.clone(), stored.clone());
                }
                if let Some(existing) = observed_activities.get(&stored.activity_id) {
                    if existing != &stored {
                        return Err(PortError::Backend(format!(
                            "activity {} has conflicting projections across sources",
                            stored.activity_id
                        )));
                    }
                } else {
                    observed_activities.insert(stored.activity_id.clone(), stored);
                }
            }
            let prior_activity_ids = current_activities_by_source
                .get(&source.source_path)
                .cloned()
                .unwrap_or_default();
            let mut final_activity_ids = if source.relation_complete {
                BTreeSet::new()
            } else {
                prior_activity_ids.clone()
            };
            final_activity_ids.extend(source_activities.keys().cloned());

            // token 用量事件（v15）：派生活用 id、校验锚点，跨源事实冲突拒绝。
            // 事件行对 (session, message, 五桶, source) 内容寻址，同 id 即同事实；
            // 仅当同 id 行事实不同（哈希碰撞或派生逻辑漂移）才 fail-closed。
            let mut source_usages = BTreeMap::new();
            for source_usage in &source.usage_events {
                let stored = stored_usage_from(
                    &source_usage.session_id,
                    source_usage.message_id.as_ref(),
                    &source_usage.usage,
                )?;
                if let Some(existing) = source_usages.get(&stored.usage_id) {
                    if existing != &stored {
                        return Err(PortError::Backend(format!(
                            "usage event {} has conflicting facts within one source",
                            stored.usage_id
                        )));
                    }
                } else {
                    source_usages.insert(stored.usage_id.clone(), stored.clone());
                }
                if let Some(existing) = observed_usages.get(&stored.usage_id) {
                    if existing != &stored {
                        return Err(PortError::Backend(format!(
                            "usage event {} has conflicting projections across sources",
                            stored.usage_id
                        )));
                    }
                } else {
                    observed_usages.insert(stored.usage_id.clone(), stored);
                }
            }
            let prior_usage_ids = current_usages_by_source
                .get(&source.source_path)
                .cloned()
                .unwrap_or_default();
            let mut final_usage_ids = if source.relation_complete {
                BTreeSet::new()
            } else {
                prior_usage_ids.clone()
            };
            final_usage_ids.extend(source_usages.keys().cloned());

            let evidence = projections.entry(source.source_path.clone()).or_default();
            if source.relation_complete {
                evidence.clear();
            }
            for entry in &source.entries {
                evidence.insert(entry.0.as_str().to_string(), entry.clone());
            }
            let replacement = SourceReplacementManifest {
                projections: evidence.clone(),
                source_path: source.source_path.clone(),
                entity_memberships: final_entities
                    .into_iter()
                    .map(|(entity_id, document_id)| SourceEntityMembershipManifest {
                        entity_id,
                        document_id,
                    })
                    .collect(),
                placement_ids: final_placement_ids
                    .iter()
                    .map(|wire| {
                        PlacementId::from_wire(wire).ok_or_else(|| {
                            PortError::Backend(format!("invalid placement claim id: {wire}"))
                        })
                    })
                    .collect::<PortResult<Vec<_>>>()?,
                activity_ids: final_activity_ids.into_iter().collect(),
                usage_ids: final_usage_ids.into_iter().collect(),
                relation_complete: source.relation_complete,
                len_bytes: source.len_bytes,
                fingerprint: source.fingerprint.clone(),
                provider_id: source.provider_id.clone(),
                resume_claims: source.resume_claims.clone(),
                installation: self.installation_for_source_commit(source)?,
            };
            prepared_sources.insert(
                source.source_path.clone(),
                PreparedSource {
                    relation_complete: source.relation_complete,
                    prior_entity_memberships,
                    prior_placement_ids,
                    prior_activity_ids,
                    prior_usage_ids,
                    observed_placements: source_placements,
                    observed_edges: source_edges,
                    replacement,
                },
            );
        }

        trace::add(&mut trace_stages, "merge_sources", trace_started);
        let trace_started = trace::begin();
        // Cache document proof once, scoped to the already loaded candidate
        // projections. A source's unrelated Cursor document is not proof for a
        // message: the final source/message membership must name that document.
        let cursor_documents: BTreeSet<(&str, &str)> = projections
            .iter()
            .flat_map(|(source, rows)| {
                rows.iter().filter_map(move |(wire, (id, payload, _))| {
                    if id.kind() != IdKind::Document {
                        return None;
                    }
                    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
                    (value.get("provider").and_then(serde_json::Value::as_str) == Some("cursor")
                        && value.get("variant").and_then(serde_json::Value::as_str)
                            == Some("cursor/vscdb-chat-v1"))
                    .then_some((source.as_str(), wire.as_str()))
                })
            })
            .collect();
        let mut cursor_epoch_claims = BTreeMap::<String, usize>::new();
        let mut final_entity_claimers = BTreeMap::<String, BTreeSet<String>>::new();
        for (source_path, memberships) in &current_entities_by_source {
            if scanned_paths.contains(source_path) {
                continue;
            }
            for (entity_id, document_id) in memberships {
                if entity_id.starts_with("msg_v1_")
                    && document_id.as_deref().is_some_and(|document| {
                        cursor_documents.contains(&(source_path.as_str(), document))
                    })
                {
                    *cursor_epoch_claims.entry(entity_id.clone()).or_default() += 1;
                }
                final_entity_claimers
                    .entry(entity_id.clone())
                    .or_default()
                    .insert(source_path.clone());
            }
        }
        let mut final_placement_claimers = BTreeMap::<String, BTreeSet<String>>::new();
        for (source_path, placement_ids) in &current_placements_by_source {
            if scanned_paths.contains(source_path) {
                continue;
            }
            for placement_id in placement_ids {
                final_placement_claimers
                    .entry(placement_id.clone())
                    .or_default()
                    .insert(source_path.clone());
            }
        }
        for (source_path, prepared) in &prepared_sources {
            for membership in &prepared.replacement.entity_memberships {
                if membership.entity_id.starts_with("msg_v1_")
                    && membership.document_id.as_deref().is_some_and(|document| {
                        cursor_documents.contains(&(source_path.as_str(), document))
                    })
                {
                    *cursor_epoch_claims
                        .entry(membership.entity_id.clone())
                        .or_default() += 1;
                }
                final_entity_claimers
                    .entry(membership.entity_id.clone())
                    .or_default()
                    .insert(source_path.clone());
            }
            for placement_id in &prepared.replacement.placement_ids {
                final_placement_claimers
                    .entry(placement_id.as_str().to_string())
                    .or_default()
                    .insert(source_path.clone());
            }
        }
        let mut final_activity_claimers = BTreeMap::<String, BTreeSet<String>>::new();
        for (source_path, activity_ids) in &current_activities_by_source {
            if scanned_paths.contains(source_path) {
                continue;
            }
            for activity_id in activity_ids {
                final_activity_claimers
                    .entry(activity_id.clone())
                    .or_default()
                    .insert(source_path.clone());
            }
        }
        for (source_path, prepared) in &prepared_sources {
            for activity_id in &prepared.replacement.activity_ids {
                final_activity_claimers
                    .entry(activity_id.clone())
                    .or_default()
                    .insert(source_path.clone());
            }
        }
        let mut final_usage_claimers = BTreeMap::<String, BTreeSet<String>>::new();
        for (source_path, usage_ids) in &current_usages_by_source {
            if scanned_paths.contains(source_path) {
                continue;
            }
            for usage_id in usage_ids {
                final_usage_claimers
                    .entry(usage_id.clone())
                    .or_default()
                    .insert(source_path.clone());
            }
        }
        for (source_path, prepared) in &prepared_sources {
            for usage_id in &prepared.replacement.usage_ids {
                final_usage_claimers
                    .entry(usage_id.clone())
                    .or_default()
                    .insert(source_path.clone());
            }
        }

        // Recompute only the affected entities from final live observations. A
        // source update replaces its prior evidence even when its text shrinks.
        let missing_ids: Vec<StableId> = final_entity_claimers
            .iter()
            .filter(|(id, claimers)| {
                claimers.iter().any(|source| {
                    !projections
                        .get(source)
                        .is_some_and(|rows| rows.contains_key(*id))
                })
            })
            .filter_map(|(id, _)| StableId::from_wire(id))
            .collect();
        let legacy_payloads: BTreeMap<_, _> = self
            .get_many(&missing_ids)?
            .into_iter()
            .filter_map(|(id, payload)| payload.map(|p| (id.as_str().to_string(), p)))
            .collect();
        for (wire, claimers) in &final_entity_claimers {
            if claimers.iter().any(|source| {
                !projections
                    .get(source)
                    .is_some_and(|rows| rows.contains_key(wire))
            }) {
                // A mixed legacy entity cannot yet be reconstructed. Retain its
                // aggregate, not a fabricated source projection. Evidence still
                // commits, so the last required re-ingest converges.
                if !legacy_payloads.contains_key(wire) {
                    return Err(PortError::SchemaIncompatible(
                        "source projection evidence is missing; completely re-ingest all contributing sources".into()));
                }
                continue;
            }
            let cursor_timestamp =
                if claimers.len() > 1 && cursor_epoch_claims.get(wire) == Some(&claimers.len()) {
                    cursor_itemtable_timestamp_alias(
                        claimers
                            .iter()
                            .map(|source| projections[source][wire].1.as_slice()),
                    )?
                } else {
                    None
                };
            for source in claimers {
                let (id, raw_payload, text) = &projections[source][wire];
                let payload = cursor_aggregate_payload(raw_payload, cursor_timestamp.as_deref())?;
                if let Some((old_id, old_payload, old_text)) = merged.get(wire) {
                    if old_id != id {
                        return Err(PortError::Backend(
                            "entity has conflicting identity metadata across sources".into(),
                        ));
                    }
                    if old_payload.as_slice() == payload.as_ref() && old_text == text {
                        continue;
                    }
                    let union = match id.kind() {
                        IdKind::Session => {
                            merge_session_payloads(wire, old_payload, payload.as_ref())?
                        }
                        IdKind::Message => {
                            merge_message_payloads(wire, old_payload, payload.as_ref())?
                        }
                        _ => {
                            return Err(PortError::Backend(
                                "entity has conflicting projections across sources".into(),
                            ));
                        }
                    };
                    let text = if id.kind() == IdKind::Message {
                        searchable_text(&union)
                    } else {
                        text.clone()
                    };
                    merged.insert(wire.clone(), (id.clone(), union, text));
                } else {
                    merged.insert(
                        wire.clone(),
                        (id.clone(), payload.into_owned(), text.clone()),
                    );
                }
            }
        }

        let claimant_sources: Vec<_> = final_entity_claimers
            .values()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut complete_sources = BTreeSet::new();
        {
            let conn = self.conn.borrow();
            for chunk in chunk_ids(&claimant_sources) {
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT source_path FROM source_relation_scans WHERE source_path IN ({})",
                        in_placeholders(chunk.len())
                    ))
                    .map_err(backend)?;
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(backend)?;
                for row in rows {
                    complete_sources.insert(row.map_err(backend)?);
                }
            }
        }
        for (source, prepared) in &prepared_sources {
            if prepared.relation_complete {
                complete_sources.insert(source.clone());
            } else {
                complete_sources.remove(source);
            }
        }
        let retain_alias_ids: Vec<_> = merged
            .values()
            .filter(|(id, _, _)| {
                !compatibility_keys(id.kind()).is_empty()
                    && final_entity_claimers[id.as_str()]
                        .iter()
                        .any(|source| !complete_sources.contains(source))
            })
            .map(|(id, _, _)| id.clone())
            .collect();
        for (id, old_payload) in self.get_many(&retain_alias_ids)? {
            let Some(old_payload) = old_payload else {
                continue;
            };
            let (_, payload, _) = merged.get_mut(id.as_str()).expect("selected merged entity");
            // An incomplete relation set cannot regenerate subtractive aliases.
            // Keep only the old compatibility fields, never old role/text/etc.
            let (Ok(serde_json::Value::Object(old)), Ok(serde_json::Value::Object(mut new))) = (
                serde_json::from_slice(&old_payload),
                serde_json::from_slice(payload),
            ) else {
                continue;
            };
            for key in compatibility_keys(id.kind()) {
                if let Some(value) = old.get(*key) {
                    new.insert((*key).into(), value.clone());
                }
            }
            *payload = serde_json::to_vec(&new).map_err(backend)?;
        }

        let mut deletes = BTreeMap::new();
        let mut placement_delete_ids = BTreeSet::new();
        let mut activity_delete_ids = BTreeSet::new();
        let mut usage_delete_ids = BTreeSet::new();
        // 失败/不完整扫描不变量（与 fast-resume `failed_incremental_scan` 同一
        // 原则：任何 IO/解析/目录错误都不删除已索引内容）：relation_complete=false
        // 的源绝不推导 tombstone——"这次没看到"不是"已被删除"，只有完整成功的
        // 扫描才能确认缺席。CLI 层对截断尾源（Invalid 健康度）直接 retain 旧索引
        // （不提交批次），同样不会走到这里。
        for prepared in prepared_sources.values() {
            if !prepared.relation_complete {
                continue;
            }
            let final_entity_ids: BTreeSet<&str> = prepared
                .replacement
                .entity_memberships
                .iter()
                .map(|membership| membership.entity_id.as_str())
                .collect();
            for prior in prepared.prior_entity_memberships.keys() {
                if !final_entity_ids.contains(prior.as_str())
                    && !final_entity_claimers.contains_key(prior)
                {
                    let id = StableId::from_wire(prior).ok_or_else(|| {
                        PortError::Backend(format!("invalid membership id: {prior}"))
                    })?;
                    deletes.insert(prior.clone(), id);
                }
            }

            let final_placement_ids: BTreeSet<&str> = prepared
                .replacement
                .placement_ids
                .iter()
                .map(PlacementId::as_str)
                .collect();
            for prior in &prepared.prior_placement_ids {
                if !final_placement_ids.contains(prior.as_str())
                    && !final_placement_claimers.contains_key(prior)
                    && stored_placements.contains_key(prior)
                {
                    placement_delete_ids.insert(prior.clone());
                }
            }

            let final_activity_ids: BTreeSet<&str> = prepared
                .replacement
                .activity_ids
                .iter()
                .map(String::as_str)
                .collect();
            for prior in &prepared.prior_activity_ids {
                if !final_activity_ids.contains(prior.as_str())
                    && !final_activity_claimers.contains_key(prior)
                    && stored_activities.contains_key(prior)
                {
                    activity_delete_ids.insert(prior.clone());
                }
            }

            let final_usage_ids: BTreeSet<&str> = prepared
                .replacement
                .usage_ids
                .iter()
                .map(String::as_str)
                .collect();
            for prior in &prepared.prior_usage_ids {
                if !final_usage_ids.contains(prior.as_str())
                    && !final_usage_claimers.contains_key(prior)
                    && stored_usages.contains_key(prior)
                {
                    usage_delete_ids.insert(prior.clone());
                }
            }
        }

        for (placement_id, placement) in &observed_placements {
            let Some(stored) = stored_placements.get(placement_id) else {
                continue;
            };
            if stored.matches(placement) {
                continue;
            }
            let claimers = final_placement_claimers
                .get(placement_id)
                .cloned()
                .unwrap_or_default();
            let all_claimers_observed_same_placement = !claimers.is_empty()
                && claimers.iter().all(|source_path| {
                    prepared_sources.get(source_path).is_some_and(|prepared| {
                        prepared.observed_placements.get(placement_id) == Some(placement)
                    })
                });
            if !all_claimers_observed_same_placement {
                return Err(PortError::Backend(format!(
                    "placement {placement_id} conflicts with a source that did not observe the same placement"
                )));
            }
        }

        for (placement_id, edge) in &observed_edges {
            let current_matches = stored_edges
                .get(placement_id)
                .is_some_and(|stored| stored.matches(edge));
            let claimers = final_placement_claimers
                .get(placement_id)
                .cloned()
                .unwrap_or_default();
            if !current_matches {
                let all_claimers_observed_same_edge = !claimers.is_empty()
                    && claimers.iter().all(|source_path| {
                        prepared_sources.get(source_path).is_some_and(|prepared| {
                            prepared.observed_edges.get(placement_id) == Some(edge)
                        })
                    });
                if !all_claimers_observed_same_edge {
                    return Err(PortError::Backend(format!(
                        "edge {placement_id} conflicts with a source that did not observe the same edge"
                    )));
                }
            }
        }

        let mut edge_delete_ids: BTreeSet<String> = placement_delete_ids
            .iter()
            .filter(|placement_id| stored_edges.contains_key(*placement_id))
            .cloned()
            .collect();
        for prepared in prepared_sources.values() {
            for placement_id in prepared.observed_placements.keys() {
                if prepared.observed_edges.contains_key(placement_id)
                    || !stored_edges.contains_key(placement_id)
                {
                    continue;
                }
                if observed_edges.contains_key(placement_id) {
                    return Err(PortError::Backend(format!(
                        "edge {placement_id} has inconsistent complete-source claims"
                    )));
                }
                let claimers = final_placement_claimers
                    .get(placement_id)
                    .cloned()
                    .unwrap_or_default();
                let all_claimers_observed_root = !claimers.is_empty()
                    && claimers.iter().all(|source_path| {
                        prepared_sources.get(source_path).is_some_and(|claimer| {
                            claimer.observed_placements.contains_key(placement_id)
                                && !claimer.observed_edges.contains_key(placement_id)
                        })
                    });
                if !all_claimers_observed_root {
                    return Err(PortError::Backend(format!(
                        "edge {placement_id} conflicts with a source that did not observe the same root"
                    )));
                }
                edge_delete_ids.insert(placement_id.clone());
            }
        }

        trace::add(&mut trace_stages, "tombstones", trace_started);

        let upserts: Vec<(StableId, Vec<u8>, String)> = merged.into_values().collect();
        let deletes: Vec<StableId> = deletes.into_values().collect();
        let relations = RelationManifests {
            relation_upserts: observed_placements
                .into_values()
                .map(RelationUpsertManifest::Placement)
                .chain(
                    observed_edges
                        .into_values()
                        .map(RelationUpsertManifest::Edge),
                )
                .chain(
                    observed_activities
                        .into_values()
                        .map(RelationUpsertManifest::Activity),
                )
                .chain(
                    observed_usages
                        .into_values()
                        .map(RelationUpsertManifest::Usage),
                )
                .collect(),
            relation_deletes: edge_delete_ids
                .into_iter()
                .map(|wire| {
                    PlacementId::from_wire(&wire)
                        .map(RelationDeleteManifest::Edge)
                        .ok_or_else(|| {
                            PortError::Backend(format!("invalid edge tombstone id: {wire}"))
                        })
                })
                .chain(placement_delete_ids.into_iter().map(|wire| {
                    PlacementId::from_wire(&wire)
                        .map(RelationDeleteManifest::Placement)
                        .ok_or_else(|| {
                            PortError::Backend(format!("invalid placement tombstone id: {wire}"))
                        })
                }))
                .chain(
                    activity_delete_ids
                        .into_iter()
                        .map(|id| Ok(RelationDeleteManifest::Activity(id))),
                )
                .chain(
                    usage_delete_ids
                        .into_iter()
                        .map(|id| Ok(RelationDeleteManifest::Usage(id))),
                )
                .collect::<PortResult<Vec<_>>>()?,
            source_replacements: prepared_sources
                .into_values()
                .map(|prepared| prepared.replacement)
                .collect(),
            relocation: None,
        };

        let trace_started = trace::begin();
        let manifest = batch_manifest(&upserts, &deletes, &relations)?;
        self.ensure_stored_identity_metadata_matches(&upserts)?;
        let batch_current = self.source_batches_are_current(
            &upserts,
            &relations,
            &CatalogStateSnapshot {
                entities_by_source: &current_entities_by_source,
                placements_by_source: &current_placements_by_source,
                activities_by_source: &current_activities_by_source,
                usages_by_source: &current_usages_by_source,
                placements: &stored_placements,
                edges: &stored_edges,
                activities: &stored_activities,
                usages: &stored_usages,
            },
        )?;
        trace::add(&mut trace_stages, "manifest", trace_started);
        if batch_current {
            trace::emit(
                "adapter:catalog",
                &trace_stages,
                &format!("noop=late sources={trace_source_count}"),
            );
            return Ok(false);
        }

        let trace_started = trace::begin();
        let pending =
            self.begin_index_batch_with_manifest(&manifest, &upserts, &deletes, &relations)?;
        trace::add(&mut trace_stages, "outbox_intent", trace_started);
        let trace_started = trace::begin();
        self.commit_index_batch_with_relations(
            &pending, &upserts, &deletes, &relations, &manifest,
        )?;
        trace::add(&mut trace_stages, "apply_commit", trace_started);
        let trace_started = trace::begin();
        self.clear_installation_reservations(
            relations
                .source_replacements
                .iter()
                .map(|source| source.source_path.as_str()),
        );
        trace::add(&mut trace_stages, "finalize", trace_started);
        if trace::enabled() {
            // Approximate retained bytes of the full-catalog maps that this
            // batch loaded; the harness measures the process peak externally.
            let payload_bytes: usize = upserts
                .iter()
                .map(|(_, payload, text)| payload.len() + text.len())
                .sum();
            let stored_payload_bytes: usize = legacy_payloads
                .iter()
                .map(|(id, payload)| id.len() + payload.len() + 48)
                .sum();
            let placement_bytes: usize = stored_placements
                .iter()
                .map(|(id, row)| {
                    id.len()
                        + row.session_id.len()
                        + row.document_id.len()
                        + row.message_id.len()
                        + 72
                })
                .sum();
            let edge_bytes: usize = stored_edges
                .iter()
                .map(|(id, row)| {
                    id.len()
                        + row.parent_message_id.len()
                        + row.parent_native_id.as_deref().map_or(0, str::len)
                        + row.relation.len()
                        + 64
                })
                .sum();
            let activity_bytes: usize = stored_activities.keys().map(|id| id.len() + 160).sum();
            let usage_bytes: usize = stored_usages.keys().map(|id| id.len() + 96).sum();
            let entity_membership_bytes: usize = current_entities_by_source
                .iter()
                .map(|(source, rows)| {
                    source.len()
                        + rows
                            .iter()
                            .map(|(id, document)| {
                                id.len() + document.as_deref().map_or(0, str::len) + 48
                            })
                            .sum::<usize>()
                })
                .sum();
            let placement_membership_bytes: usize = current_placements_by_source
                .iter()
                .map(|(source, rows)| {
                    source.len() + rows.iter().map(|id| id.len() + 32).sum::<usize>()
                })
                .sum();
            trace::emit(
                "adapter:bytes",
                &[],
                &format!(
                    "upserts={} upsert_bytes~{} stored_payloads={} stored_payload_bytes~{} \
                     stored_placements={} placement_bytes~{} stored_edges={} edge_bytes~{} \
                     stored_activities={} activity_bytes~{} stored_usages={} usage_bytes~{} \
                     entity_membership_bytes~{} placement_membership_bytes~{}",
                    upserts.len(),
                    payload_bytes,
                    legacy_payloads.len(),
                    stored_payload_bytes,
                    stored_placements.len(),
                    placement_bytes,
                    stored_edges.len(),
                    edge_bytes,
                    stored_activities.len(),
                    activity_bytes,
                    stored_usages.len(),
                    usage_bytes,
                    entity_membership_bytes,
                    placement_membership_bytes
                ),
            );
        }
        trace::emit(
            "adapter:catalog",
            &trace_stages,
            &format!(
                "noop=false sources={trace_source_count} upserts={} deletes={} relation_upserts={} relation_deletes={} source_replacements={}",
                upserts.len(),
                deletes.len(),
                relations.relation_upserts.len(),
                relations.relation_deletes.len(),
                relations.source_replacements.len()
            ),
        );
        Ok(true)
    }

    fn source_projections_for_path(conn: &Connection, path: &str) -> PortResult<SourceProjections> {
        let mut stmt = conn.prepare("SELECT entity_id, id_json, payload, text FROM source_entity_projections WHERE source_path=?1").map_err(backend)?;
        let rows = stmt
            .query_map([path], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(backend)?;
        let mut result = BTreeMap::new();
        for row in rows {
            let (wire, id, payload, text) = row.map_err(backend)?;
            result.insert(
                wire,
                (serde_json::from_str(&id).map_err(backend)?, payload, text),
            );
        }
        Ok(result)
    }

    fn load_source_projections(
        conn: &Connection,
        candidates: &BTreeSet<String>,
    ) -> PortResult<BTreeMap<String, SourceProjections>> {
        let ids: Vec<_> = candidates.iter().collect();
        let mut result: BTreeMap<String, SourceProjections> = BTreeMap::new();
        for chunk in chunk_ids(&ids) {
            let mut stmt = conn.prepare(&format!("SELECT source_path, entity_id, id_json, payload, text FROM source_entity_projections WHERE entity_id IN ({})", in_placeholders(chunk.len()))).map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (source, wire, id, payload, text) = row.map_err(backend)?;
                result.entry(source).or_default().insert(
                    wire,
                    (serde_json::from_str(&id).map_err(backend)?, payload, text),
                );
            }
        }
        Ok(result)
    }

    /// True when every source in the batch is already fully current: catalog
    /// entries (payload + fts text), placements, edges, entity membership,
    /// placement claims, scan record, and relation-completeness marker all
    /// match the stored state. Called before any heavy merge/manifest work
    /// so an unchanged re-sync costs O(batch), not O(whole catalog). Every
    /// query is scoped to this batch's sources/ids — no full-table loads.
    fn sources_are_current(&self, ordered_sources: &[&SourceBatch]) -> PortResult<bool> {
        let conn = self.conn.borrow();
        for source in ordered_sources {
            let evidence = Self::source_projections_for_path(&conn, &source.source_path)?;
            if source
                .entries
                .iter()
                .any(|entry| evidence.get(entry.0.as_str()) != Some(entry))
            {
                return Ok(false);
            }
            if !self.installation_is_current(&conn, source)? {
                return Ok(false);
            }
            // A source that has never been scanned cannot be current; skip the
            // per-entity queries (which dominate on first ingest of an
            // empty catalog) and go straight to the heavy path.
            let stored_scan: Option<StoredSourceScan> = conn
                .query_row(
                    "SELECT len_bytes, fingerprint, provider_id, parser_version
                     FROM source_scans WHERE source_path = ?1",
                    [&source.source_path],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(backend)?;
            let Some((stored_len, stored_fingerprint, stored_provider_id, stored_parser_version)) =
                stored_scan
            else {
                return Ok(false);
            };
            // 指纹缓存参与 current 判定：len/fingerprint 任一变说明源字节已变而
            // 缓存未更新，必须重解析并重写缓存。只查扫描行存在会让缓存永不收敛，
            // CLI 每次运行都重解析全部源。
            if source.len_bytes != stored_len
                || source.fingerprint.as_deref() != stored_fingerprint.as_deref()
            {
                return Ok(false);
            }
            // 解析语义版本参与 current 判定（借鉴 Recall 的 parser_version 增量
            // 同步）：版本落后说明本二进制解析语义已升级，字节未变也必须走提交
            // 路径重写 source_scans（targeted backfill）——否则重解析结果与库
            // 一致时 no-op 短路会让版本永不收敛、每次 sync 都重复解析。
            if stored_parser_version != i64::from(PARSER_SEMANTIC_VERSION) {
                return Ok(false);
            }
            // provider_id 回填同样参与 current 判定：discover 发现的源可能携带
            // provider_id，而已存行为 NULL（先显式 sync 后 discovery 的场景）。
            // 仅当 incoming 是 Some 且与 stored 不同时才判 not-current——
            // incoming None（显式 sync）不覆盖已有 provider_id，保持一致。
            if let Some(incoming_provider_id) = source.provider_id.as_deref()
                && stored_provider_id.as_deref() != Some(incoming_provider_id)
            {
                return Ok(false);
            }

            // Catalog existence: batch-scoped ID reads, chunked under the
            // SQLite variable limit.
            let ids: Vec<&str> = source
                .entries
                .iter()
                .map(|(id, _, _)| id.as_str())
                .collect();
            let mut catalog_ids = BTreeSet::new();
            for chunk in chunk_ids(&ids) {
                let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT id FROM catalog WHERE id IN ({placeholders})"
                    ))
                    .map_err(backend)?;
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(backend)?;
                for row in rows {
                    catalog_ids.insert(row.map_err(backend)?);
                }
            }
            if source
                .entries
                .iter()
                .any(|(id, _, _)| !catalog_ids.contains(id.as_str()))
            {
                return Ok(false);
            }
            // Original observations were checked against source evidence above.
            // The historical aggregate may intentionally differ while another
            // legacy claimant is still unknown; it is never a competing source.
            // Indexed text: batched reads mapping wire_id -> fts text.
            let mut fts_text: BTreeMap<String, String> = BTreeMap::new();
            for chunk in chunk_ids(&ids) {
                let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT fi.wire_id, f.text FROM fts f
                         JOIN fts_ids fi ON fi.id_json = f.id
                         WHERE fi.wire_id IN ({placeholders})"
                    ))
                    .map_err(backend)?;
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(backend)?;
                for row in rows {
                    let (wire_id, text) = row.map_err(backend)?;
                    fts_text.insert(wire_id, text);
                }
            }
            for (id, _payload, text) in &source.entries {
                // 非 Message 实体不进 fts 全文表（见 batch_upsert_fts_in_tx），
                // 恒无 fts 行；拿 fts_tokens_cjk(text) 与“无行”比较会让任何
                // 含 session/document 条目的批次——即每个真实 ingest 批次——
                // 永远判为 not-current，快路径整体失效，重同步退化为 O(全库)。
                // 与 batch_is_current_with_derived_context 的同一判定保持一致。
                if id.kind() != IdKind::Message {
                    continue;
                }
                // 与写入侧同一 transform：fts 正文存的是 fts_tokens_cjk(text)。
                let expected = fts_tokens_cjk(text);
                if fts_text.get(id.as_str()).map(String::as_str) != Some(expected.as_str()) {
                    return Ok(false);
                }
            }
            // Entity membership for this source only.
            let mut stmt = conn
                .prepare(
                    "SELECT message_id, document_id FROM source_membership
                     WHERE source_path = ?1 ORDER BY message_id",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([&source.source_path], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })
                .map_err(backend)?;
            let mut stored_membership = BTreeMap::new();
            for row in rows {
                let (entity_id, document_id) = row.map_err(backend)?;
                stored_membership.insert(entity_id, document_id);
            }
            let document_id = source
                .entries
                .iter()
                .find(|(id, _, _)| id.kind() == IdKind::Document)
                .map(|(id, _, _)| id.as_str().to_string());
            let incoming_entities: BTreeMap<String, Option<String>> = source
                .entries
                .iter()
                .map(|(id, _, _)| (id.as_str().to_string(), document_id.clone()))
                .collect();
            if stored_membership != incoming_entities {
                return Ok(false);
            }
            // Placement claims for this source only.
            let mut stmt = conn
                .prepare(
                    "SELECT placement_id FROM source_placement_membership
                     WHERE source_path = ?1 ORDER BY placement_id",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([&source.source_path], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            let stored_placements: BTreeSet<String> =
                rows.collect::<Result<_, _>>().map_err(backend)?;
            let expected_placements: BTreeSet<String> = source
                .placements
                .iter()
                .map(|p| p.id.as_str().to_string())
                .collect();
            if stored_placements != expected_placements {
                return Ok(false);
            }
            // Stored placements for this source's ids (batched, chunked).
            let mut stored_placements: BTreeMap<String, StoredPlacement> = BTreeMap::new();
            let pids: Vec<&str> = source.placements.iter().map(|p| p.id.as_str()).collect();
            for chunk in chunk_ids(&pids) {
                let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT placement_id, session_id, document_id, message_id,
                                source_ordinal, is_sidechain, byte_start, byte_end
                         FROM message_placements WHERE placement_id IN ({placeholders})"
                    ))
                    .map_err(backend)?;
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                        let start: Option<i64> = row.get(6)?;
                        let end: Option<i64> = row.get(7)?;
                        Ok((
                            row.get::<_, String>(0)?,
                            StoredPlacement {
                                session_id: row.get(1)?,
                                document_id: row.get(2)?,
                                message_id: row.get(3)?,
                                source_ordinal: row.get(4)?,
                                is_sidechain: row.get(5)?,
                                span: match (start, end) {
                                    (Some(start), Some(end)) => Some((start as u64, end as u64)),
                                    _ => None,
                                },
                            },
                        ))
                    })
                    .map_err(backend)?;
                for row in rows {
                    let (pid, stored) = row.map_err(backend)?;
                    stored_placements.insert(pid, stored);
                }
            }
            for placement in &source.placements {
                if !stored_placements
                    .get(placement.id.as_str())
                    .is_some_and(|stored| stored.matches(placement))
                {
                    return Ok(false);
                }
            }
            // Stored edges for this source's ids (batched, chunked).
            let mut stored_edges: BTreeMap<String, (String, Option<String>, String)> =
                BTreeMap::new();
            let cids: Vec<&str> = source
                .edges
                .iter()
                .map(|e| e.child_placement_id.as_str())
                .collect();
            for chunk in chunk_ids(&cids) {
                let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT child_placement_id, parent_message_id, parent_native_id, relation
                         FROM message_edges WHERE child_placement_id IN ({placeholders})"
                    ))
                    .map_err(backend)?;
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            (
                                row.get::<_, String>(1)?,
                                row.get::<_, Option<String>>(2)?,
                                row.get::<_, String>(3)?,
                            ),
                        ))
                    })
                    .map_err(backend)?;
                for row in rows {
                    let (cid, edge) = row.map_err(backend)?;
                    stored_edges.insert(cid, edge);
                }
            }
            for edge in &source.edges {
                let matches = stored_edges
                    .get(edge.child_placement_id.as_str())
                    .is_some_and(|(parent, native, relation)| {
                        parent == edge.parent_message_id.as_str()
                            && native.as_deref() == edge.parent_native_id.as_deref()
                            && relation == edge.relation.as_str()
                    });
                if !matches {
                    return Ok(false);
                }
            }
            // Tool-activity claims for this source only.
            let mut stmt = conn
                .prepare(
                    "SELECT activity_id FROM tool_activity_membership
                     WHERE source_path = ?1 ORDER BY activity_id",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([&source.source_path], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            let stored_activity_claims: BTreeSet<String> =
                rows.collect::<Result<_, _>>().map_err(backend)?;
            let expected_activity_claims: BTreeSet<String> = source
                .activities
                .iter()
                .map(|source_activity| {
                    stored_activity_from(&source_activity.message_id, &source_activity.activity)
                        .map(|stored| stored.activity_id)
                })
                .collect::<PortResult<BTreeSet<_>>>()?;
            if stored_activity_claims != expected_activity_claims {
                return Ok(false);
            }
            // Stored activity rows for this source's ids (batched, chunked).
            let mut stored_rows: BTreeMap<String, StoredActivity> = BTreeMap::new();
            let activity_ids: Vec<String> = expected_activity_claims.into_iter().collect();
            for chunk in chunk_ids(&activity_ids) {
                let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT activity_id, message_id, kind, actor, name, target, status
                         FROM tool_activities WHERE activity_id IN ({placeholders})"
                    ))
                    .map_err(backend)?;
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                        let id: String = row.get(0)?;
                        Ok((
                            id.clone(),
                            StoredActivity {
                                activity_id: id,
                                message_id: row.get(1)?,
                                kind: row.get(2)?,
                                actor: row.get(3)?,
                                name: row.get(4)?,
                                target: row.get(5)?,
                                status: row.get(6)?,
                            },
                        ))
                    })
                    .map_err(backend)?;
                for row in rows {
                    let (id, stored) = row.map_err(backend)?;
                    stored_rows.insert(id, stored);
                }
            }
            for source_activity in &source.activities {
                let expected =
                    stored_activity_from(&source_activity.message_id, &source_activity.activity)?;
                if !stored_rows
                    .get(&expected.activity_id)
                    .is_some_and(|stored| stored.matches(&expected))
                {
                    return Ok(false);
                }
            }
            // Usage-event claims for this source only（v15）。
            let mut stmt = conn
                .prepare(
                    "SELECT usage_id FROM usage_event_membership
                     WHERE source_path = ?1 ORDER BY usage_id",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([&source.source_path], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            let stored_usage_claims: BTreeSet<String> =
                rows.collect::<Result<_, _>>().map_err(backend)?;
            let expected_usage_claims: BTreeSet<String> = source
                .usage_events
                .iter()
                .map(|source_usage| {
                    stored_usage_from(
                        &source_usage.session_id,
                        source_usage.message_id.as_ref(),
                        &source_usage.usage,
                    )
                    .map(|stored| stored.usage_id)
                })
                .collect::<PortResult<BTreeSet<_>>>()?;
            if stored_usage_claims != expected_usage_claims {
                return Ok(false);
            }
            // Stored usage rows for this source's ids (batched, chunked).
            let mut stored_usage_rows: BTreeMap<String, StoredUsage> = BTreeMap::new();
            let usage_ids: Vec<String> = expected_usage_claims.into_iter().collect();
            for chunk in chunk_ids(&usage_ids) {
                let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT usage_id, session_id, message_id, input_tokens, output_tokens,
                                cache_read_tokens, cache_write_tokens, reasoning_tokens,
                                token_source
                         FROM usage_events WHERE usage_id IN ({placeholders})"
                    ))
                    .map_err(backend)?;
                let rows = stmt
                    .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                        let id: String = row.get(0)?;
                        Ok((
                            id.clone(),
                            StoredUsage {
                                usage_id: id,
                                session_id: row.get(1)?,
                                message_id: row.get(2)?,
                                input_tokens: row_u64(row, 3)?,
                                output_tokens: row_u64(row, 4)?,
                                cache_read_tokens: row_u64(row, 5)?,
                                cache_write_tokens: row_u64(row, 6)?,
                                reasoning_tokens: row_u64(row, 7)?,
                                token_source: row.get(8)?,
                            },
                        ))
                    })
                    .map_err(backend)?;
                for row in rows {
                    let (id, stored) = row.map_err(backend)?;
                    stored_usage_rows.insert(id, stored);
                }
            }
            for source_usage in &source.usage_events {
                let expected = stored_usage_from(
                    &source_usage.session_id,
                    source_usage.message_id.as_ref(),
                    &source_usage.usage,
                )?;
                if !stored_usage_rows
                    .get(&expected.usage_id)
                    .is_some_and(|stored| stored.matches(&expected))
                {
                    return Ok(false);
                }
            }
            // Completeness marker (scan record already checked at loop head).
            let complete: bool = conn
                .query_row(
                    "SELECT 1 FROM source_relation_scans WHERE source_path = ?1",
                    [&source.source_path],
                    |_| Ok(()),
                )
                .optional()
                .map_err(backend)?
                .is_some();
            if complete != source.relation_complete {
                return Ok(false);
            }
            // Resume Metadata 声明（ADR-0009）同样参与 no-op 判定：声明变化
            // （含 None↔Some）必须走提交路径原子替换/清除，不能因其余字节
            // 未变而跳过。
            let stored_resume = stored_resume_claim(&conn, &source.source_path)?;
            let expected_resume = source
                .resume_claims
                .iter()
                .map(|claim| {
                    (
                        claim.session_id.clone(),
                        StoredResumeClaim::from_claim(claim),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            if stored_resume != expected_resume {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(test)]
    fn source_entity_membership_state(
        &self,
    ) -> PortResult<BTreeMap<String, BTreeMap<String, Option<String>>>> {
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare(
                "SELECT source_path, message_id, document_id
                 FROM source_membership ORDER BY source_path, message_id",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(backend)?;
        let mut state = BTreeMap::<String, BTreeMap<String, Option<String>>>::new();
        for row in rows {
            let (source_path, entity_id, document_id) = row.map_err(backend)?;
            state
                .entry(source_path)
                .or_default()
                .insert(entity_id, document_id);
        }
        Ok(state)
    }

    #[cfg(test)]
    fn source_message_ids(&self, source_path: &str) -> PortResult<Vec<String>> {
        Ok(self
            .source_entity_membership_state()?
            .remove(source_path)
            .unwrap_or_default()
            .into_keys()
            .collect())
    }

    /// 本批来源自身的实体成员行（按 `source_path` 索引，只读本批 source）。
    fn source_entity_membership_state_for_sources(
        &self,
        sources: &BTreeSet<String>,
    ) -> PortResult<BTreeMap<String, BTreeMap<String, Option<String>>>> {
        let conn = self.conn.borrow();
        let mut state = BTreeMap::<String, BTreeMap<String, Option<String>>>::new();
        for chunk in chunk_ids(&sources.iter().cloned().collect::<Vec<_>>()) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT source_path, message_id, document_id FROM source_membership
                     WHERE source_path IN ({placeholders}) ORDER BY source_path, message_id"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (source_path, entity_id, document_id) = row.map_err(backend)?;
                state
                    .entry(source_path)
                    .or_default()
                    .insert(entity_id, document_id);
            }
        }
        Ok(state)
    }

    /// 追加候选实体的跨来源 claim 行。`source_membership` 没有 message_id 索引，
    /// 只能整表扫描；扫描成本 O(catalog)，但只保留候选行（内存 O(batch)）。
    fn extend_entity_claims_for_candidates(
        &self,
        state: &mut BTreeMap<String, BTreeMap<String, Option<String>>>,
        candidates: &BTreeSet<String>,
        scanned: &BTreeSet<String>,
    ) -> PortResult<()> {
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare("SELECT source_path, message_id, document_id FROM source_membership")
            .map_err(backend)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(backend)?;
        for row in rows {
            let (source_path, entity_id, document_id) = row.map_err(backend)?;
            if scanned.contains(&source_path) || !candidates.contains(&entity_id) {
                continue;
            }
            state
                .entry(source_path)
                .or_default()
                .insert(entity_id, document_id);
        }
        Ok(())
    }

    /// 本批来源自身的 placement 成员行。
    fn source_placement_membership_state_for_sources(
        &self,
        sources: &BTreeSet<String>,
    ) -> PortResult<BTreeMap<String, BTreeSet<String>>> {
        let conn = self.conn.borrow();
        let mut state = BTreeMap::<String, BTreeSet<String>>::new();
        for chunk in chunk_ids(&sources.iter().cloned().collect::<Vec<_>>()) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT source_path, placement_id FROM source_placement_membership
                     WHERE source_path IN ({placeholders}) ORDER BY source_path, placement_id"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (source_path, placement_id) = row.map_err(backend)?;
                state.entry(source_path).or_default().insert(placement_id);
            }
        }
        Ok(state)
    }

    /// 追加候选 placement 的跨来源 claim 行（按 placement_id 索引分块查询）。
    fn extend_placement_claims_for_candidates(
        &self,
        state: &mut BTreeMap<String, BTreeSet<String>>,
        candidates: &BTreeSet<String>,
        scanned: &BTreeSet<String>,
    ) -> PortResult<()> {
        let conn = self.conn.borrow();
        let ids: Vec<String> = candidates.iter().cloned().collect();
        for chunk in chunk_ids(&ids) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT source_path, placement_id FROM source_placement_membership
                     WHERE placement_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (source_path, placement_id) = row.map_err(backend)?;
                if scanned.contains(&source_path) {
                    continue;
                }
                state.entry(source_path).or_default().insert(placement_id);
            }
        }
        Ok(())
    }

    /// 本批来源自身的工具活动成员行。
    fn source_activity_membership_state_for_sources(
        &self,
        sources: &BTreeSet<String>,
    ) -> PortResult<BTreeMap<String, BTreeSet<String>>> {
        let conn = self.conn.borrow();
        let mut state = BTreeMap::<String, BTreeSet<String>>::new();
        for chunk in chunk_ids(&sources.iter().cloned().collect::<Vec<_>>()) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT source_path, activity_id FROM tool_activity_membership
                     WHERE source_path IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (source_path, activity_id) = row.map_err(backend)?;
                state.entry(source_path).or_default().insert(activity_id);
            }
        }
        Ok(state)
    }

    /// 追加候选活动的跨来源 claim 行（按 activity_id 索引分块查询）。
    fn extend_activity_claims_for_candidates(
        &self,
        state: &mut BTreeMap<String, BTreeSet<String>>,
        candidates: &BTreeSet<String>,
        scanned: &BTreeSet<String>,
    ) -> PortResult<()> {
        let conn = self.conn.borrow();
        let ids: Vec<String> = candidates.iter().cloned().collect();
        for chunk in chunk_ids(&ids) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT source_path, activity_id FROM tool_activity_membership
                     WHERE activity_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (source_path, activity_id) = row.map_err(backend)?;
                if scanned.contains(&source_path) {
                    continue;
                }
                state.entry(source_path).or_default().insert(activity_id);
            }
        }
        Ok(())
    }

    /// 本批来源自身的用量事件成员行。
    fn source_usage_membership_state_for_sources(
        &self,
        sources: &BTreeSet<String>,
    ) -> PortResult<BTreeMap<String, BTreeSet<String>>> {
        let conn = self.conn.borrow();
        let mut state = BTreeMap::<String, BTreeSet<String>>::new();
        for chunk in chunk_ids(&sources.iter().cloned().collect::<Vec<_>>()) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT source_path, usage_id FROM usage_event_membership
                     WHERE source_path IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (source_path, usage_id) = row.map_err(backend)?;
                state.entry(source_path).or_default().insert(usage_id);
            }
        }
        Ok(state)
    }

    /// 追加候选用量事件的跨来源 claim 行（按 usage_id 索引分块查询）。
    fn extend_usage_claims_for_candidates(
        &self,
        state: &mut BTreeMap<String, BTreeSet<String>>,
        candidates: &BTreeSet<String>,
        scanned: &BTreeSet<String>,
    ) -> PortResult<()> {
        let conn = self.conn.borrow();
        let ids: Vec<String> = candidates.iter().cloned().collect();
        for chunk in chunk_ids(&ids) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT source_path, usage_id FROM usage_event_membership
                     WHERE usage_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (source_path, usage_id) = row.map_err(backend)?;
                if scanned.contains(&source_path) {
                    continue;
                }
                state.entry(source_path).or_default().insert(usage_id);
            }
        }
        Ok(())
    }

    /// 只读取候选 id 的关系行（主键分块查询，替代整表加载）。
    fn stored_placements_for(
        &self,
        candidates: &BTreeSet<String>,
    ) -> PortResult<BTreeMap<String, StoredPlacement>> {
        let conn = self.conn.borrow();
        let ids: Vec<String> = candidates.iter().cloned().collect();
        let mut placements = BTreeMap::new();
        for chunk in chunk_ids(&ids) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT placement_id, session_id, document_id, message_id,
                            source_ordinal, is_sidechain, byte_start, byte_end
                     FROM message_placements WHERE placement_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (
                    placement_id,
                    session_id,
                    document_id,
                    message_id,
                    ordinal,
                    sidechain,
                    start,
                    end,
                ) = row.map_err(backend)?;
                let span = match (start, end) {
                    (None, None) => None,
                    (Some(start), Some(end)) => Some((
                        u64::try_from(start).map_err(backend)?,
                        u64::try_from(end).map_err(backend)?,
                    )),
                    _ => {
                        return Err(PortError::Backend(
                            "stored placement has a partial span".into(),
                        ));
                    }
                };
                placements.insert(
                    placement_id,
                    StoredPlacement {
                        session_id,
                        document_id,
                        message_id,
                        source_ordinal: u32::try_from(ordinal).map_err(backend)?,
                        is_sidechain: sidechain != 0,
                        span,
                    },
                );
            }
        }
        Ok(placements)
    }

    /// 只读取候选 placement 的边行。
    fn stored_edges_for(
        &self,
        candidates: &BTreeSet<String>,
    ) -> PortResult<BTreeMap<String, StoredEdge>> {
        let conn = self.conn.borrow();
        let ids: Vec<String> = candidates.iter().cloned().collect();
        let mut edges = BTreeMap::new();
        for chunk in chunk_ids(&ids) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT child_placement_id, parent_message_id, parent_native_id, relation
                     FROM message_edges WHERE child_placement_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (child, parent_message_id, parent_native_id, relation) =
                    row.map_err(backend)?;
                edges.insert(
                    child,
                    StoredEdge {
                        parent_message_id,
                        parent_native_id,
                        relation,
                    },
                );
            }
        }
        Ok(edges)
    }

    /// 测试专用：整表读取（生产提交路径只读候选 id，见 §Batch-scoped commit state）。
    #[cfg(test)]
    fn stored_placements(&self) -> PortResult<BTreeMap<String, StoredPlacement>> {
        let conn = self.conn.borrow();
        Self::stored_placements_from(&conn)
    }

    /// 测试专用：整表读取（生产提交路径只读候选 id）。
    #[cfg(test)]
    fn stored_edges(&self) -> PortResult<BTreeMap<String, StoredEdge>> {
        let conn = self.conn.borrow();
        Self::stored_edges_from(&conn)
    }

    fn stored_placements_from(conn: &Connection) -> PortResult<BTreeMap<String, StoredPlacement>> {
        let mut stmt = conn
            .prepare(
                "SELECT placement_id, session_id, document_id, message_id,
                        source_ordinal, is_sidechain, byte_start, byte_end
                 FROM message_placements ORDER BY placement_id",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                ))
            })
            .map_err(backend)?;
        let mut placements = BTreeMap::new();
        for row in rows {
            let (placement_id, session_id, document_id, message_id, ordinal, sidechain, start, end) =
                row.map_err(backend)?;
            let span = match (start, end) {
                (None, None) => None,
                (Some(start), Some(end)) => Some((
                    u64::try_from(start).map_err(backend)?,
                    u64::try_from(end).map_err(backend)?,
                )),
                _ => {
                    return Err(PortError::Backend(
                        "stored placement has a partial span".into(),
                    ));
                }
            };
            placements.insert(
                placement_id,
                StoredPlacement {
                    session_id,
                    document_id,
                    message_id,
                    source_ordinal: u32::try_from(ordinal).map_err(backend)?,
                    is_sidechain: sidechain != 0,
                    span,
                },
            );
        }
        Ok(placements)
    }

    fn stored_activities_for(
        &self,
        candidates: &BTreeSet<String>,
    ) -> PortResult<BTreeMap<String, StoredActivity>> {
        let conn = self.conn.borrow();
        let ids: Vec<String> = candidates.iter().cloned().collect();
        let mut activities = BTreeMap::new();
        for chunk in chunk_ids(&ids) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT activity_id, message_id, kind, actor, name, target, status
                     FROM tool_activities WHERE activity_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok(StoredActivity {
                        activity_id: row.get(0)?,
                        message_id: row.get(1)?,
                        kind: row.get(2)?,
                        actor: row.get(3)?,
                        name: row.get(4)?,
                        target: row.get(5)?,
                        status: row.get(6)?,
                    })
                })
                .map_err(backend)?;
            for row in rows {
                let activity = row.map_err(backend)?;
                activities.insert(activity.activity_id.clone(), activity);
            }
        }
        Ok(activities)
    }

    fn stored_usages_for(
        &self,
        candidates: &BTreeSet<String>,
    ) -> PortResult<BTreeMap<String, StoredUsage>> {
        let conn = self.conn.borrow();
        let ids: Vec<String> = candidates.iter().cloned().collect();
        let mut usages = BTreeMap::new();
        for chunk in chunk_ids(&ids) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT usage_id, session_id, message_id, input_tokens, output_tokens,
                            cache_read_tokens, cache_write_tokens, reasoning_tokens, token_source
                     FROM usage_events WHERE usage_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        StoredUsage {
                            usage_id: String::new(),
                            session_id: row.get(1)?,
                            message_id: row.get(2)?,
                            input_tokens: row_u64(row, 3)?,
                            output_tokens: row_u64(row, 4)?,
                            cache_read_tokens: row_u64(row, 5)?,
                            cache_write_tokens: row_u64(row, 6)?,
                            reasoning_tokens: row_u64(row, 7)?,
                            token_source: row.get(8)?,
                        },
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (usage_id, mut usage) = row.map_err(backend)?;
                usage.usage_id = usage_id.clone();
                usages.insert(usage_id, usage);
            }
        }
        Ok(usages)
    }

    fn stored_edges_from(conn: &Connection) -> PortResult<BTreeMap<String, StoredEdge>> {
        let mut stmt = conn
            .prepare(
                "SELECT child_placement_id, parent_message_id, parent_native_id, relation
                 FROM message_edges ORDER BY child_placement_id",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    StoredEdge {
                        parent_message_id: row.get(1)?,
                        parent_native_id: row.get(2)?,
                        relation: row.get(3)?,
                    },
                ))
            })
            .map_err(backend)?;
        let mut edges = BTreeMap::new();
        for row in rows {
            let (placement_id, edge) = row.map_err(backend)?;
            edges.insert(placement_id, edge);
        }
        Ok(edges)
    }

    fn alias_candidates(
        conn: &Connection,
        batch_sources: &[String],
    ) -> PortResult<BTreeSet<String>> {
        let mut candidate_entities = BTreeSet::<String>::new();
        for chunk in chunk_ids(batch_sources) {
            let placeholders = in_placeholders(chunk.len());
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT message_id FROM source_membership
                     WHERE source_path IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    row.get::<_, String>(0)
                })
                .map_err(backend)?;
            for row in rows {
                candidate_entities.insert(row.map_err(backend)?);
            }
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT placements.session_id, placements.document_id,
                            placements.message_id
                     FROM source_placement_membership AS claims
                     JOIN message_placements AS placements
                       ON placements.placement_id = claims.placement_id
                     WHERE claims.source_path IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (session_id, document_id, message_id) = row.map_err(backend)?;
                candidate_entities.insert(session_id);
                candidate_entities.insert(document_id);
                candidate_entities.insert(message_id);
            }
        }
        Ok(candidate_entities)
    }

    fn regenerate_compatibility_aliases_in_tx(
        tx: &rusqlite::Transaction<'_>,
        batch_sources: &[String],
        in_memory_payloads: &BTreeMap<&str, &[u8]>,
        mut candidate_entities: BTreeSet<String>,
    ) -> PortResult<()> {
        candidate_entities.extend(Self::alias_candidates(tx, batch_sources)?);
        if candidate_entities.is_empty() {
            return Ok(());
        }

        let complete_sources = {
            let mut stmt = tx
                .prepare("SELECT source_path FROM source_relation_scans")
                .map_err(backend)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            let mut sources = BTreeSet::new();
            for row in rows {
                sources.insert(row.map_err(backend)?);
            }
            sources
        };

        let mut claimers_by_entity = BTreeMap::<String, BTreeSet<String>>::new();
        let mut session_document_claims = BTreeMap::<String, BTreeSet<String>>::new();
        {
            let mut stmt = tx
                .prepare(
                    "SELECT source_path, message_id, document_id
                     FROM source_membership",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (source_path, entity_id, document_id) = row.map_err(backend)?;
                if !candidate_entities.contains(&entity_id) {
                    continue;
                }
                claimers_by_entity
                    .entry(entity_id.clone())
                    .or_default()
                    .insert(source_path);
                if entity_id.starts_with(IdKind::Session.prefix())
                    && let Some(document_id) = document_id
                {
                    session_document_claims
                        .entry(entity_id)
                        .or_default()
                        .insert(document_id);
                }
            }
        }
        {
            let mut stmt = tx
                .prepare(
                    "SELECT claims.source_path, placements.session_id,
                            placements.document_id, placements.message_id
                     FROM source_placement_membership AS claims
                     JOIN message_placements AS placements
                       ON placements.placement_id = claims.placement_id",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (source_path, session_id, document_id, message_id) = row.map_err(backend)?;
                for entity_id in [session_id, document_id, message_id] {
                    if !candidate_entities.contains(&entity_id) {
                        continue;
                    }
                    claimers_by_entity
                        .entry(entity_id)
                        .or_default()
                        .insert(source_path.clone());
                }
            }
        }

        let evidence = Self::load_source_projections(tx, &candidate_entities)?;
        let fully_complete_entities: BTreeSet<String> = claimers_by_entity
            .into_iter()
            .filter_map(|(entity_id, claimers)| {
                (!claimers.is_empty()
                    && claimers.iter().all(|source_path| {
                        complete_sources.contains(source_path)
                            && evidence
                                .get(source_path)
                                .is_some_and(|rows| rows.contains_key(&entity_id))
                    }))
                .then_some(entity_id)
            })
            .collect();
        if fully_complete_entities.is_empty() {
            return Ok(());
        }

        let placements = Self::stored_placements_from(tx)?;
        let edges = Self::stored_edges_from(tx)?;
        let mut placements_by_message = BTreeMap::<String, Vec<(String, StoredPlacement)>>::new();
        let mut placements_by_session = BTreeMap::<String, Vec<(String, StoredPlacement)>>::new();
        for (placement_id, placement) in placements {
            placements_by_message
                .entry(placement.message_id.clone())
                .or_default()
                .push((placement_id.clone(), placement.clone()));
            placements_by_session
                .entry(placement.session_id.clone())
                .or_default()
                .push((placement_id, placement));
        }
        for placements in placements_by_message
            .values_mut()
            .chain(placements_by_session.values_mut())
        {
            placements.sort_by(|left, right| {
                (
                    left.1.document_id.as_str(),
                    left.1.source_ordinal,
                    left.0.as_str(),
                )
                    .cmp(&(
                        right.1.document_id.as_str(),
                        right.1.source_ordinal,
                        right.0.as_str(),
                    ))
            });
        }

        for entity_id in fully_complete_entities {
            let Some(id) = StableId::from_wire(&entity_id) else {
                return Err(PortError::Backend(
                    "source membership contains an invalid entity id".into(),
                ));
            };
            if !matches!(id.kind(), IdKind::Message | IdKind::Session) {
                continue;
            }
            let owned_payload: Option<Vec<u8>> =
                if in_memory_payloads.contains_key(entity_id.as_str()) {
                    None
                } else {
                    tx.query_row(
                        "SELECT payload FROM catalog WHERE id = ?1",
                        [&entity_id],
                        |row| row.get::<_, Option<Vec<u8>>>(0),
                    )
                    .optional()
                    .map_err(backend)?
                    .flatten()
                };
            let payload: Option<&[u8]> = match in_memory_payloads.get(entity_id.as_str()) {
                Some(payload) => Some(*payload),
                None => owned_payload.as_deref(),
            };
            let Some(payload) = payload else {
                continue;
            };
            let mut map = match serde_json::from_slice::<serde_json::Value>(payload) {
                Ok(serde_json::Value::Object(map)) => map,
                _ => continue,
            };

            match id.kind() {
                IdKind::Message => {
                    let placements = placements_by_message
                        .get(&entity_id)
                        .cloned()
                        .unwrap_or_default();
                    if placements.is_empty() {
                        continue;
                    }
                    let sessions: Vec<String> = placements
                        .iter()
                        .map(|(_, placement)| placement.session_id.clone())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect();
                    map.insert(
                        "session".into(),
                        sessions
                            .first()
                            .cloned()
                            .map_or(serde_json::Value::Null, serde_json::Value::String),
                    );
                    map.insert("sessions".into(), serde_json::json!(sessions));

                    let spans: Vec<serde_json::Value> = placements
                        .iter()
                        .filter_map(|(placement_id, placement)| {
                            placement.span.map(|(start, end)| {
                                serde_json::json!({
                                    "placement_id": placement_id,
                                    "document": placement.document_id,
                                    "start": start,
                                    "end": end,
                                })
                            })
                        })
                        .collect();
                    map.insert(
                        "span".into(),
                        spans.first().map_or(serde_json::Value::Null, |span| {
                            serde_json::json!({
                                "start": span.get("start").cloned().unwrap_or(serde_json::Value::Null),
                                "end": span.get("end").cloned().unwrap_or(serde_json::Value::Null),
                            })
                        }),
                    );
                    map.insert("spans".into(), serde_json::json!(spans));

                    let parent_facts: Vec<Option<String>> = placements
                        .iter()
                        .map(|(placement_id, _)| {
                            edges
                                .get(placement_id)
                                .map(|edge| edge.parent_message_id.clone())
                        })
                        .collect();
                    let parent = match parent_facts.first() {
                        Some(first) if parent_facts.iter().all(|fact| fact == first) => {
                            first.clone()
                        }
                        _ => None,
                    };
                    map.insert(
                        "parent".into(),
                        parent.map_or(serde_json::Value::Null, serde_json::Value::String),
                    );

                    let parent_native_facts: Vec<Option<String>> = placements
                        .iter()
                        .map(|(placement_id, _)| {
                            edges
                                .get(placement_id)
                                .and_then(|edge| edge.parent_native_id.clone())
                        })
                        .collect();
                    let parent_native_id = match parent_native_facts.first() {
                        Some(first) if parent_native_facts.iter().all(|fact| fact == first) => {
                            first.clone()
                        }
                        _ => None,
                    };
                    map.insert(
                        "parent_native_id".into(),
                        parent_native_id.map_or(serde_json::Value::Null, serde_json::Value::String),
                    );

                    let is_sidechain = match placements.first() {
                        Some((_, first))
                            if placements.iter().all(|(_, placement)| {
                                placement.is_sidechain == first.is_sidechain
                            }) =>
                        {
                            serde_json::Value::Bool(first.is_sidechain)
                        }
                        _ => serde_json::Value::Null,
                    };
                    map.insert("is_sidechain".into(), is_sidechain);
                }
                IdKind::Session => {
                    let placements = placements_by_session
                        .get(&entity_id)
                        .cloned()
                        .unwrap_or_default();
                    if placements.is_empty()
                        && map
                            .get("messages")
                            .and_then(serde_json::Value::as_array)
                            .is_some_and(|messages| !messages.is_empty())
                    {
                        continue;
                    }
                    let mut seen_messages = BTreeSet::new();
                    let mut messages = Vec::new();
                    let mut documents = session_document_claims
                        .remove(&entity_id)
                        .unwrap_or_default();
                    for (_, placement) in placements {
                        documents.insert(placement.document_id);
                        if seen_messages.insert(placement.message_id.clone()) {
                            messages.push(placement.message_id);
                        }
                    }
                    let documents: Vec<String> = documents.into_iter().collect();
                    map.insert(
                        "document".into(),
                        documents
                            .first()
                            .cloned()
                            .map_or(serde_json::Value::Null, serde_json::Value::String),
                    );
                    map.insert("documents".into(), serde_json::json!(documents));
                    map.insert("messages".into(), serde_json::json!(messages));
                }
                _ => unreachable!(),
            }

            let payload = serde_json::to_vec(&serde_json::Value::Object(map)).map_err(backend)?;
            // Skip the write when the rebuilt aliases equal the stored bytes:
            // regeneration must not rewrite the catalog (and inflate the WAL)
            // on every commit once aliases are stable.
            let stored_owned: Option<Vec<u8>>;
            let stored: Option<&[u8]> = match in_memory_payloads.get(entity_id.as_str()) {
                Some(stored) => Some(*stored),
                None => {
                    stored_owned = tx
                        .query_row(
                            "SELECT payload FROM catalog WHERE id = ?1",
                            [&entity_id],
                            |row| row.get::<_, Option<Vec<u8>>>(0),
                        )
                        .optional()
                        .map_err(backend)?
                        .flatten();
                    stored_owned.as_deref()
                }
            };
            if stored != Some(payload.as_slice()) {
                tx.execute(
                    "UPDATE catalog SET payload = ?2 WHERE id = ?1",
                    rusqlite::params![entity_id, payload],
                )
                .map_err(backend)?;
            }
        }
        Ok(())
    }

    fn source_batches_are_current(
        &self,
        upserts: &[(StableId, Vec<u8>, String)],
        relations: &RelationManifests,
        state: &CatalogStateSnapshot<'_>,
    ) -> PortResult<bool> {
        {
            let conn = self.conn.borrow();
            for replacement in &relations.source_replacements {
                if Self::source_projections_for_path(&conn, &replacement.source_path)?
                    != replacement.projections
                {
                    return Ok(false);
                }
            }
        }
        if !self.batch_is_current_with_derived_context(upserts, true)? {
            return Ok(false);
        }
        let stored_placements = state.placements;
        let stored_edges = state.edges;
        let stored_activities = state.activities;
        let stored_usages = state.usages;
        for upsert in &relations.relation_upserts {
            let current = match upsert {
                RelationUpsertManifest::Placement(placement) => stored_placements
                    .get(placement.id.as_str())
                    .is_some_and(|stored| stored.matches(placement)),
                RelationUpsertManifest::Edge(edge) => stored_edges
                    .get(edge.child_placement_id.as_str())
                    .is_some_and(|stored| stored.matches(edge)),
                RelationUpsertManifest::Activity(activity) => stored_activities
                    .get(&activity.activity_id)
                    .is_some_and(|stored| stored.matches(activity)),
                RelationUpsertManifest::Usage(usage) => stored_usages
                    .get(&usage.usage_id)
                    .is_some_and(|stored| stored.matches(usage)),
            };
            if !current {
                return Ok(false);
            }
        }
        for delete in &relations.relation_deletes {
            let exists = match delete {
                RelationDeleteManifest::Placement(id) => {
                    stored_placements.contains_key(id.as_str())
                }
                RelationDeleteManifest::Edge(id) => stored_edges.contains_key(id.as_str()),
                RelationDeleteManifest::Activity(activity_id) => {
                    stored_activities.contains_key(activity_id)
                }
                RelationDeleteManifest::Usage(usage_id) => stored_usages.contains_key(usage_id),
            };
            if exists {
                return Ok(false);
            }
        }

        let entity_state = state.entities_by_source;
        let placement_state = state.placements_by_source;
        let activity_state = state.activities_by_source;
        let usage_state = state.usages_by_source;
        let conn = self.conn.borrow();
        for replacement in &relations.source_replacements {
            if replacement.entity_memberships.is_empty()
                && replacement.fingerprint.is_none()
                && replacement.len_bytes.is_none()
                && Self::assignment_for_source(&conn, &replacement.source_path)?.is_some()
            {
                return Ok(false);
            }
            if let Some(installation) = &replacement.installation
                && Self::assignment_for_source(&conn, &replacement.source_path)?.as_ref()
                    != Some(installation)
            {
                return Ok(false);
            }
            let expected_entities: BTreeMap<String, Option<String>> = replacement
                .entity_memberships
                .iter()
                .map(|membership| (membership.entity_id.clone(), membership.document_id.clone()))
                .collect();
            if entity_state
                .get(&replacement.source_path)
                .cloned()
                .unwrap_or_default()
                != expected_entities
            {
                return Ok(false);
            }
            let expected_placements: BTreeSet<String> = replacement
                .placement_ids
                .iter()
                .map(|id| id.as_str().to_string())
                .collect();
            if placement_state
                .get(&replacement.source_path)
                .cloned()
                .unwrap_or_default()
                != expected_placements
            {
                return Ok(false);
            }
            let expected_activity_ids: BTreeSet<String> =
                replacement.activity_ids.iter().cloned().collect();
            if activity_state
                .get(&replacement.source_path)
                .cloned()
                .unwrap_or_default()
                != expected_activity_ids
            {
                return Ok(false);
            }
            let expected_usage_ids: BTreeSet<String> =
                replacement.usage_ids.iter().cloned().collect();
            if usage_state
                .get(&replacement.source_path)
                .cloned()
                .unwrap_or_default()
                != expected_usage_ids
            {
                return Ok(false);
            }
            let stored_scan: Option<StoredSourceScan> = conn
                .query_row(
                    "SELECT len_bytes, fingerprint, provider_id, parser_version
                     FROM source_scans WHERE source_path = ?1",
                    [&replacement.source_path],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(backend)?;
            let Some((stored_len, stored_fingerprint, stored_provider_id, stored_parser_version)) =
                stored_scan
            else {
                return Ok(false);
            };
            // 与 sources_are_current 同口径：指纹缓存参与 no-op 判定。字节已变而
            // 缓存未更新的源必须走提交路径重写 source_scans，否则指纹缓存永不收敛。
            if replacement.len_bytes != stored_len
                || replacement.fingerprint.as_deref() != stored_fingerprint.as_deref()
            {
                return Ok(false);
            }
            // 与 sources_are_current 同口径：解析语义版本落后（升级后未
            // backfill）同样必须走提交路径写回当前版本，否则版本永不收敛。
            if stored_parser_version != i64::from(PARSER_SEMANTIC_VERSION) {
                return Ok(false);
            }
            if let Some(incoming_provider_id) = replacement.provider_id.as_deref()
                && stored_provider_id.as_deref() != Some(incoming_provider_id)
            {
                return Ok(false);
            }
            let relation_complete: bool = conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM source_relation_scans WHERE source_path = ?1
                     )",
                    [&replacement.source_path],
                    |row| row.get(0),
                )
                .map_err(backend)?;
            if relation_complete != replacement.relation_complete {
                return Ok(false);
            }
            // 与 sources_are_current 同口径：声明变化同样必须走提交路径。
            let stored_resume = stored_resume_claim(&conn, &replacement.source_path)?;
            let expected_resume = replacement
                .resume_claims
                .iter()
                .map(|claim| {
                    (
                        claim.session_id.clone(),
                        StoredResumeClaim::from_claim(claim),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            if stored_resume != expected_resume {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// 读回当前活动 generation（v2 起可用）。
    pub fn active_generation(&self) -> PortResult<u64> {
        let conn = self.conn.borrow();
        let g: i64 = conn
            .query_row(
                "SELECT active_generation FROM store_metadata WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        u64::try_from(g).map_err(backend)
    }

    /// 读回本库现存投影的索引投影版本（schema v17；见
    /// [`INDEX_PROJECTION_VERSION`]）。`0` 表示"未知/旧变换写的"——由 v17
    /// 迁移为投影非空的旧库留下的哨兵值。
    pub fn index_projection_version(&self) -> PortResult<i64> {
        let conn = self.conn.borrow();
        Self::stored_index_projection_version(&conn)
    }

    /// 现存投影是否由本二进制的投影变换写成。false ⇒ FTS 词元流与查询侧
    /// 词元不可比，任何 MATCH 结果都不可信（doctor 据此如实报告）。
    pub fn index_projection_is_current(&self) -> PortResult<bool> {
        Ok(self.index_projection_version()? == i64::from(INDEX_PROJECTION_VERSION))
    }

    fn stored_index_projection_version(conn: &Connection) -> PortResult<i64> {
        conn.query_row(
            "SELECT index_projection_version FROM store_metadata WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(backend)
    }

    /// FTS 查询前的投影版本闸门（读路径 fail-closed）。
    ///
    /// 索引期与查询期的词元变换必须同版本。失配时**绝不返回命中集**——旧词元
    /// 与新查询词元对不上会静默漏掉真实命中（实测：一个由旧二进制建立的
    /// 170 468 实体真实库上，中文查询 0 命中而 ASCII 查询正常）。归到
    /// [`PortError::SchemaIncompatible`]（error catalog `schema_incompatible`，
    /// operator_action 已是"升级程序或重建派生数据根"），消息给出确切修复命令。
    fn assert_index_projection_current(conn: &Connection) -> PortResult<()> {
        let stored = Self::stored_index_projection_version(conn)?;
        if stored == i64::from(INDEX_PROJECTION_VERSION) {
            return Ok(());
        }
        Err(PortError::SchemaIncompatible(format!(
            "search index projection version {stored} does not match this binary's \
             {INDEX_PROJECTION_VERSION}: the stored FTS tokens were written by a different \
             projection and cannot be matched against this binary's query tokens; run \
             `index rebuild` to reproject from the catalog (a write-mode `sync` reprojects \
             automatically)"
        )))
    }

    /// 收敛索引投影版本（写路径自愈，[`SqliteStore::open_for_write`] 调用）。
    ///
    /// 已是当前版本 → no-op。否则：
    /// - 投影为空（无 `fts`/`session_fts` 行）→ 没有旧词元可纠正，只标记版本，
    ///   **不推进 generation、不写 outbox 行**（新库/空库不该因此产生 churn）；
    /// - 投影非空 → 从权威 catalog 全量重投影（等价 `index rebuild`）。catalog
    ///   payload 对内容权威，**不需要回到 provider reparse**——这正是本轴与
    ///   [`PARSER_SEMANTIC_VERSION`] 的区别。重投影推进 generation（投影内容
    ///   确实变了，旧 cursor 必须失效）。
    ///
    /// 自愈的重投影**不重派生 repo 身份投影**（[`RepoIdentityRebuild::Preserve`]）：
    /// 该投影不可从 catalog 重建，而本方法在 `open_for_write` 内、组合根注入
    /// 解析器之前就会跑；无条件清表重派生会用一次自愈把整条 repo 维度删空
    /// 并同时盖上"投影已收敛"的戳。自愈不改变会话集合，保留既有行不产生孤儿。
    ///
    /// 返回是否执行了重投影。
    pub fn ensure_index_projection_current(&self) -> PortResult<bool> {
        if self.index_projection_is_current()? {
            return Ok(false);
        }
        let projection_is_empty = {
            let conn = self.conn.borrow();
            Self::index_projection_is_empty_in_tx(&conn)?
        };
        if projection_is_empty {
            let conn = self.conn.borrow();
            conn.execute(
                "UPDATE store_metadata SET index_projection_version = ?1 WHERE singleton = 1",
                [i64::from(INDEX_PROJECTION_VERSION)],
            )
            .map_err(backend)?;
            return Ok(false);
        }
        self.reproject_from_catalog(RepoIdentityRebuild::Preserve)?;
        Ok(true)
    }

    /// 阶段一（durable intent）：写入一条 `building` outbox 行并提交。
    ///
    /// Durable outbox 状态机的第一个 durable point——在任何 catalog/FTS
    /// 变更落盘之前，先持久化"应该构建什么"（upsert/delete 集合 + digest）。
    /// 此后崩溃，恢复只会看到一条无副作用的 `building` 行并将其 `aborted`。
    ///
    /// `base_generation` 必须等于当前活动 generation，否则说明并发写者抢先推进过
    /// generation，本次 intent 作废（CAS 前置条件）。
    pub fn begin_index_batch(
        &self,
        upserts: &[(StableId, Vec<u8>, String)],
        deletes: &[StableId],
    ) -> PortResult<PendingIndexBatch> {
        Self::reject_source_owned_writes(&self.conn.borrow(), upserts, deletes)?;
        self.begin_index_batch_with_relations(upserts, deletes, &RelationManifests::default())
    }

    fn begin_index_batch_with_relations(
        &self,
        upserts: &[(StableId, Vec<u8>, String)],
        deletes: &[StableId],
        relations: &RelationManifests,
    ) -> PortResult<PendingIndexBatch> {
        let manifest = batch_manifest(upserts, deletes, relations)?;
        self.begin_index_batch_with_manifest(&manifest, upserts, deletes, relations)
    }

    fn begin_index_batch_with_manifest(
        &self,
        manifest: &CanonicalBatchManifest,
        _upserts: &[(StableId, Vec<u8>, String)],
        _deletes: &[StableId],
        _relations: &RelationManifests,
    ) -> PortResult<PendingIndexBatch> {
        let base = self.active_generation()?;
        let target = base
            .checked_add(1)
            .ok_or_else(|| PortError::Backend("generation overflow".into()))?;
        let target_sql = i64::try_from(target).map_err(backend)?;
        let base_sql = i64::try_from(base).map_err(backend)?;
        let op = operation_id()?;
        let upsert_json = serde_json::to_string(&manifest.upsert_ids).map_err(backend)?;
        let delete_json = serde_json::to_string(&manifest.delete_ids).map_err(backend)?;
        let conn = self.conn.borrow();
        conn.execute(
            "INSERT INTO index_batches(
                 operation_id, base_generation, target_generation, state,
                 operation_digest, upsert_ids_json, delete_ids_json,
                 relation_upserts_json, relation_deletes_json,
                 source_replacements_json, durable_point, created_at_ms, relocation_json
             ) VALUES(
                 ?1, ?2, ?3, 'building', ?4, ?5, ?6, ?7, ?8, ?9, 'intent', ?10, ?11
             )",
            rusqlite::params![
                op,
                base_sql,
                target_sql,
                manifest.operation_digest,
                upsert_json,
                delete_json,
                manifest.relation_upserts_json,
                manifest.relation_deletes_json,
                manifest.source_replacements_json,
                unix_ms()?,
                manifest.relocation_json,
            ],
        )
        .map_err(backend)?;
        Ok(PendingIndexBatch {
            operation_id: op,
            base_generation: base,
            target_generation: target,
            operation_digest: manifest.operation_digest.clone(),
        })
    }

    /// 阶段二（apply + activate）：在单个事务内应用 catalog+FTS 变更、推进活动
    /// generation，并把 outbox 行标记为 `activated`。
    ///
    /// FTS5 单存储下 catalog 与 FTS 同事务域提交，故 `catalog_committed` 与
    /// `search_built` 合为一个 durable point；generation 切换也在同一事务，
    /// 因此不存在"搜索已建但未激活"的中间崩溃窗口。
    ///
    /// CAS 前置：`active_generation == pending.base_generation`。不匹配则拒绝，
    /// 防止旧基线覆盖更新的同步结果。
    pub fn commit_index_batch(
        &self,
        pending: &PendingIndexBatch,
        upserts: &[(StableId, Vec<u8>, String)],
        deletes: &[StableId],
    ) -> PortResult<()> {
        Self::reject_source_owned_writes(&self.conn.borrow(), upserts, deletes)?;
        let relations = RelationManifests::default();
        let manifest = batch_manifest(upserts, deletes, &relations)?;
        self.commit_index_batch_with_relations(pending, upserts, deletes, &relations, &manifest)
    }

    /// 事务内校验 pending 句柄仍可安全激活：generation CAS + intent 行状态 + manifest 匹配。
    ///
    /// 从 [`commit_index_batch_with_relations`](Self::commit_index_batch_with_relations) 抽出，
    /// 使 rebuild 路径复用同一套“不覆盖更新基线 / 不与 durable intent 分歧”的前置检查。
    fn verify_pending_in_tx(
        tx: &rusqlite::Transaction<'_>,
        pending: &PendingIndexBatch,
        _upserts: &[(StableId, Vec<u8>, String)],
        _deletes: &[StableId],
        _relations: &RelationManifests,
        manifest: &CanonicalBatchManifest,
    ) -> PortResult<()> {
        // Public commit_index_batch constructs a fresh actual-input manifest;
        // the private source path passes the same immutable inputs used to seal
        // its intent. Reuse that manifest here (no second whole-batch hash).
        // CAS：活动 generation 必须仍等于 intent 记录的 base，否则中止本批次。
        let current: i64 = tx
            .query_row(
                "SELECT active_generation FROM store_metadata WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if current as u64 != pending.base_generation {
            return Err(PortError::Backend(format!(
                "generation CAS failed: active {current} != expected base {}",
                pending.base_generation
            )));
        }
        // intent 行及其完整 manifest 必须仍与 pending handle 匹配。
        let (
            declared_base,
            declared_target,
            state,
            declared_digest,
            declared_upserts,
            declared_deletes,
            declared_relation_upserts,
            declared_relation_deletes,
            declared_source_replacements,
        ): (
            i64,
            i64,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ) = tx
            .query_row(
                "SELECT base_generation, target_generation, state, operation_digest,
                        upsert_ids_json, delete_ids_json, relation_upserts_json,
                        relation_deletes_json, source_replacements_json
                 FROM index_batches WHERE operation_id = ?1",
                [&pending.operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                    ))
                },
            )
            .map_err(backend)?;
        if state != "building" {
            return Err(PortError::Backend(format!(
                "index batch {} is in state {state}, expected building",
                pending.operation_id
            )));
        }
        let declared_relocation: String = tx
            .query_row(
                "SELECT relocation_json FROM index_batches WHERE operation_id = ?1",
                [&pending.operation_id],
                |row| row.get(0),
            )
            .map_err(backend)?;
        let actual = manifest;
        let actual_upserts = serde_json::to_string(&actual.upsert_ids).map_err(backend)?;
        let actual_deletes = serde_json::to_string(&actual.delete_ids).map_err(backend)?;
        let handle_matches = declared_base == pending.base_generation as i64
            && declared_target == pending.target_generation as i64
            && declared_digest == pending.operation_digest;
        if !handle_matches
            || declared_digest != actual.operation_digest
            || declared_upserts != actual_upserts
            || declared_deletes != actual_deletes
            || declared_relation_upserts != actual.relation_upserts_json
            || declared_relation_deletes != actual.relation_deletes_json
            || declared_source_replacements != actual.source_replacements_json
            || declared_relocation != actual.relocation_json
        {
            return Err(PortError::Backend(format!(
                "index batch {} payload does not match durable intent",
                pending.operation_id
            )));
        }
        Ok(())
    }

    /// 收集本批被触碰的 placement id 归属的 Session 集合（分块 IN，无 N+1）。
    fn collect_placement_sessions(
        conn: &Connection,
        placement_ids: &[String],
        sessions: &mut BTreeSet<String>,
    ) -> PortResult<()> {
        for chunk in chunk_ids(placement_ids) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT DISTINCT session_id FROM message_placements
                     WHERE placement_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    row.get::<_, String>(0)
                })
                .map_err(backend)?;
            for row in rows {
                sessions.insert(row.map_err(backend)?);
            }
        }
        Ok(())
    }

    /// 收集本批被触碰的 message id 归属的 Session 集合（分块 IN，无 N+1）。
    fn collect_message_sessions(
        conn: &Connection,
        message_ids: &[String],
        sessions: &mut BTreeSet<String>,
    ) -> PortResult<()> {
        for chunk in chunk_ids(message_ids) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT DISTINCT session_id FROM message_placements
                     WHERE message_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    row.get::<_, String>(0)
                })
                .map_err(backend)?;
            for row in rows {
                sessions.insert(row.map_err(backend)?);
            }
        }
        Ok(())
    }

    /// 收集本批 source replacement 触碰的 Session 集合（分块 IN，无 N+1）。
    fn collect_resume_claim_sessions(
        conn: &Connection,
        source_paths: &[String],
        sessions: &mut BTreeSet<String>,
    ) -> PortResult<()> {
        for chunk in chunk_ids(source_paths) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT DISTINCT session_id FROM source_session_resume_claims
                     WHERE source_path IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    row.get::<_, String>(0)
                })
                .map_err(backend)?;
            for row in rows {
                sessions.insert(row.map_err(backend)?);
            }
        }
        Ok(())
    }

    /// 会话成员消息里第一条 role=user 且 text 非空的正文（placement 成员
    /// 顺序：timestamp → document → source_ordinal → placement_id），截断到
    /// `max_chars`（char 边界）。
    ///
    /// 注入噪声在 provider parse 层已被过滤（claude/codex user-noise filter，
    /// feat/noise-filter），不进 catalog——此处读到的第一条即"噪声过滤后"
    /// 的首条有效 user 消息。无 placement 或无有效 user 消息 → `None`。
    fn first_user_text_for_session(
        conn: &Connection,
        session_wire: &str,
        max_chars: usize,
    ) -> PortResult<Option<String>> {
        let mut stmt = conn
            .prepare(
                "SELECT catalog.payload
                 FROM message_placements
                 JOIN catalog ON catalog.id = message_placements.message_id
                 WHERE message_placements.session_id = ?1
                 ORDER BY asg_instant_sort_key(
                              CASE WHEN json_valid(catalog.payload)
                                   THEN json_extract(catalog.payload, '$.timestamp') END
                          ) IS NULL,
                          asg_instant_sort_key(
                              CASE WHEN json_valid(catalog.payload)
                                   THEN json_extract(catalog.payload, '$.timestamp') END
                          ),
                          message_placements.document_id,
                          message_placements.source_ordinal,
                          message_placements.placement_id",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([session_wire], |row| row.get::<_, Vec<u8>>(0))
            .map_err(backend)?;
        for row in rows {
            let payload = row.map_err(backend)?;
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&payload)
                && value.get("role").and_then(serde_json::Value::as_str) == Some("user")
                && let Some(text) = value.get("text").and_then(serde_json::Value::as_str)
                && !text.is_empty()
            {
                return Ok(Some(text.chars().take(max_chars).collect::<String>()));
            }
        }
        Ok(None)
    }

    /// 会话标题派生链（借鉴清单 #6；agent-sessions Session.swift `title` 的
    /// custom > lightweight > first-user 链 + cc-switch codex.rs 的
    /// thread-titles > first-user 链）：
    ///
    /// 1. custom-title：canonical session payload 的 `title` 字段（用户显式命名）；
    /// 2. ai-title：canonical session payload 的 `summary` 字段（AI/平台生成摘要）；
    /// 3. 首条有效 user 消息：噪声过滤后第一条非空 user 文本。
    ///
    /// claude-code/codex 的 Canonical payload 目前不携带 `title`/`summary`
    /// 字段（格式事实：两格式均无该概念），实际一律落到候选 3；候选 1/2 是
    /// 按格式事实预留的更高优先级来源，provider 未来填充即自动生效。全部
    /// 候选缺失（含会话不存在）→ `None`（不写行）。所有候选统一 trim +
    /// ≤[`SESSION_TITLE_MAX_CHARS`] char 边界截断。
    fn session_title(conn: &Connection, session_wire: &str) -> PortResult<Option<String>> {
        let payload: Option<Vec<u8>> = conn
            .query_row(
                "SELECT payload FROM catalog WHERE id = ?1",
                [session_wire],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        if let Some(bytes) = payload
            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
        {
            for key in ["title", "summary"] {
                if let Some(text) = value.get(key).and_then(serde_json::Value::as_str) {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        return Ok(Some(
                            trimmed.chars().take(SESSION_TITLE_MAX_CHARS).collect(),
                        ));
                    }
                }
            }
        }
        Self::first_user_text_for_session(conn, session_wire, SESSION_TITLE_MAX_CHARS)
    }

    /// Conflict-free resolved resume claim for one canonical Session。
    ///
    /// 声明冲突是隐私敏感信号：同一 Session 的 claims 必须逐字段一致，
    /// 否则 fail closed 返回 None（绝不取任一冲突值）。`session_fts`
    /// 搜索文本与 `session_repo_slugs`（schema v16）共用同一裁决。
    fn resolved_session_claim(
        conn: &Connection,
        session_wire: &str,
    ) -> PortResult<Option<StoredResumeClaim>> {
        let mut stmt = conn
            .prepare(
                "SELECT session_id, provider_id, provider_session_id,
                        provider_session_id_state, original_working_directory,
                        original_working_directory_state, pair_observed
                 FROM source_session_resume_claims
                 WHERE session_id = ?1",
            )
            .map_err(backend)?;
        let rows = stmt
            .query_map([session_wire], |row| {
                Ok(StoredResumeClaim {
                    session_id: row.get(0)?,
                    provider_id: row.get(1)?,
                    provider_session_id: row.get(2)?,
                    provider_session_id_state: row.get(3)?,
                    original_working_directory: row.get(4)?,
                    original_working_directory_state: row.get(5)?,
                    pair_observed: row.get(6)?,
                })
            })
            .map_err(backend)?;
        let mut claim: Option<StoredResumeClaim> = None;
        let mut conflicting = false;
        for row in rows {
            let next = row.map_err(backend)?;
            if claim.as_ref().is_some_and(|current| current != &next) {
                conflicting = true;
                break;
            }
            claim = Some(next);
        }
        if conflicting { Ok(None) } else { Ok(claim) }
    }

    /// Derive the repo slug for one resolved claim（schema v16）。
    ///
    /// 与 `session_fts` 的 cwd 披露同一门禁：provider_session_id 已
    /// resolved、pair-observed、cwd resolved 且非空；再交给注入的
    /// [`RepoSlugResolver`]。任一缺失或检测失败 → None（无行，不猜）。
    /// 纯函数（无 I/O 以外注入的 resolver），失败测试先行锚定。
    fn session_repo_slug(
        claim: &StoredResumeClaim,
        resolver: &dyn RepoSlugResolver,
    ) -> Option<String> {
        if claim.provider_session_id_state != "resolved" || !claim.pair_observed {
            return None;
        }
        if claim.original_working_directory_state != "resolved" {
            return None;
        }
        let cwd = claim.original_working_directory.as_deref()?;
        if cwd.is_empty() {
            return None;
        }
        resolver.resolve(cwd)
    }

    /// Build one Session's bounded search text from authoritative relational
    /// state. A representative placement is required so a metadata match can be
    /// returned as an existing Message `SearchHit` without fabricating an id.
    fn session_search_text(conn: &Connection, session_wire: &str) -> PortResult<Option<String>> {
        let session_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM catalog WHERE id = ?1)",
                [session_wire],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if !session_exists {
            return Ok(None);
        }

        let first_user_text =
            Self::first_user_text_for_session(conn, session_wire, SESSION_SEARCH_FIELD_CHARS)?;

        // Claims for one canonical Session must agree exactly. Conflict is
        // privacy-sensitive, so fail closed and index none of their values.
        let resolved_claim = Self::resolved_session_claim(conn, session_wire)?;

        let mut fields = Vec::new();
        if let Some(claim) = resolved_claim
            && claim.provider_session_id_state == "resolved"
            && let Some(provider_session_id) = claim.provider_session_id
            && !provider_session_id.is_empty()
        {
            fields.push(
                provider_session_id
                    .chars()
                    .take(SESSION_SEARCH_FIELD_CHARS)
                    .collect(),
            );
            if claim.pair_observed
                && claim.original_working_directory_state == "resolved"
                && let Some(directory) = claim.original_working_directory
                && !directory.is_empty()
            {
                fields.push(directory.chars().take(SESSION_SEARCH_FIELD_CHARS).collect());
            }
        }
        if let Some(text) = first_user_text {
            fields.push(text);
        }
        if fields.is_empty() {
            Ok(None)
        } else {
            Ok(Some(fields.join("\n")))
        }
    }

    fn rebuild_session_search_row_in_tx(
        tx: &rusqlite::Transaction<'_>,
        session_wire: &str,
        resolver: &dyn RepoSlugResolver,
        repo_identity: RepoIdentityRebuild,
    ) -> PortResult<()> {
        tx.execute(
            "DELETE FROM session_fts
             WHERE rowid = (
                 SELECT fts_rowid FROM session_fts_ids WHERE session_wire = ?1
             )",
            [session_wire],
        )
        .map_err(backend)?;
        tx.execute(
            "DELETE FROM session_fts_ids WHERE session_wire = ?1",
            [session_wire],
        )
        .map_err(backend)?;
        if let Some(text) = Self::session_search_text(tx, session_wire)? {
            tx.execute(
                "INSERT INTO session_fts(session_wire, text) VALUES(?1, ?2)",
                rusqlite::params![session_wire, fts_tokens_cjk(&text)],
            )
            .map_err(backend)?;
            tx.execute(
                "INSERT INTO session_fts_ids(session_wire, fts_rowid) VALUES(?1, ?2)",
                rusqlite::params![session_wire, tx.last_insert_rowid()],
            )
            .map_err(backend)?;
        }
        // 标题投影（schema v13）：与 session_fts 同一派生批次。先删旧行，
        // 再按派生链重投影（custom-title > ai-title > 首条有效 user）；
        // 派生链无候选 → 无行（读取侧恒得到 None，不写空标题）。
        tx.execute(
            "DELETE FROM session_titles WHERE session_wire = ?1",
            [session_wire],
        )
        .map_err(backend)?;
        if let Some(title) = Self::session_title(tx, session_wire)? {
            tx.execute(
                "INSERT INTO session_titles(session_wire, title) VALUES(?1, ?2)",
                rusqlite::params![session_wire, title],
            )
            .map_err(backend)?;
        }
        // repo identity 投影（schema v16）：与 session_fts/session_titles
        // 同一派生批次。先删旧行，派生成功才写新行；session 已退役（不
        // 在 catalog）、claim 缺失/冲突、门禁不满足或 git 检测失败 →
        // 无行（诚实降级）。绝对路径绝不落此表——只有三段 slug。
        if repo_identity == RepoIdentityRebuild::Preserve {
            return Ok(());
        }
        tx.execute(
            "DELETE FROM session_repo_slugs WHERE session_wire = ?1",
            [session_wire],
        )
        .map_err(backend)?;
        let session_exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM catalog WHERE id = ?1)",
                [session_wire],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if session_exists
            && let Some(claim) = Self::resolved_session_claim(tx, session_wire)?
            && let Some(slug) = Self::session_repo_slug(&claim, resolver)
        {
            tx.execute(
                "INSERT INTO session_repo_slugs(session_wire, repo_slug) VALUES(?1, ?2)",
                rusqlite::params![session_wire, slug],
            )
            .map_err(backend)?;
        }
        Ok(())
    }

    fn rebuild_all_session_search_in_tx(
        tx: &rusqlite::Transaction<'_>,
        resolver: &dyn RepoSlugResolver,
        repo_identity: RepoIdentityRebuild,
    ) -> PortResult<()> {
        tx.execute("DELETE FROM session_fts", []).map_err(backend)?;
        tx.execute("DELETE FROM session_fts_ids", [])
            .map_err(backend)?;
        tx.execute("DELETE FROM session_titles", [])
            .map_err(backend)?;
        if repo_identity == RepoIdentityRebuild::Rederive {
            tx.execute("DELETE FROM session_repo_slugs", [])
                .map_err(backend)?;
        } else {
            // 自愈不重派生 repo slug（它不可从 catalog 重建），但清理历史崩溃/
            // 旧 bug 遗留的 orphan 行，避免它们继续污染 repo facet。把清理放在
            // 全量重建入口而不是逐 session 行内，确保空 catalog 也能收敛且只执行一次。
            tx.execute(
                "DELETE FROM session_repo_slugs
                 WHERE NOT EXISTS (
                     SELECT 1 FROM catalog
                     WHERE catalog.id = session_repo_slugs.session_wire
                       AND catalog.id LIKE 'ses_v1_%'
                 )",
                [],
            )
            .map_err(backend)?;
        }
        let sessions = {
            let mut stmt = tx
                .prepare("SELECT id FROM catalog WHERE id LIKE 'ses_v1_%' ORDER BY id")
                .map_err(backend)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            let mut sessions = Vec::new();
            for row in rows {
                sessions.push(row.map_err(backend)?);
            }
            sessions
        };
        for session_wire in sessions {
            Self::rebuild_session_search_row_in_tx(tx, &session_wire, resolver, repo_identity)?;
        }
        Ok(())
    }

    /// Retire vectors only when the final embedding input changes. Source
    /// evidence updates and compatibility aliases alone do not invalidate it.
    fn invalidate_changed_vectors_in_tx<'a>(
        tx: &rusqlite::Transaction<'_>,
        upserts: impl IntoIterator<Item = (&'a StableId, &'a [u8])>,
        deletes: &[StableId],
    ) -> PortResult<()> {
        let mut stale: BTreeSet<String> = deletes.iter().map(|id| id.as_str().to_owned()).collect();
        let incoming: BTreeMap<_, _> = upserts
            .into_iter()
            .filter(|(id, _)| id.kind() == IdKind::Message)
            .map(|(id, payload)| (id.as_str(), payload))
            .collect();
        let ids: Vec<_> = incoming.keys().copied().collect();
        for chunk in chunk_ids(&ids) {
            let mut stmt = tx
                .prepare(&format!(
                    "SELECT v.wire_id, c.payload FROM message_vec v
                 LEFT JOIN catalog c ON c.id=v.wire_id
                 WHERE v.wire_id IN ({})",
                    in_placeholders(chunk.len())
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<Vec<u8>>>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (id, old) = row.map_err(backend)?;
                if old
                    .as_ref()
                    .is_none_or(|old| message_body(old) != message_body(incoming[id.as_str()]))
                {
                    stale.insert(id);
                }
            }
        }
        let ids: Vec<_> = stale.into_iter().collect();
        for chunk in chunk_ids(&ids) {
            tx.execute(
                &format!(
                    "DELETE FROM message_vec WHERE wire_id IN ({})",
                    in_placeholders(chunk.len())
                ),
                rusqlite::params_from_iter(chunk.iter()),
            )
            .map_err(backend)?;
        }
        Ok(())
    }

    fn commit_index_batch_with_relations(
        &self,
        pending: &PendingIndexBatch,
        upserts: &[(StableId, Vec<u8>, String)],
        deletes: &[StableId],
        relations: &RelationManifests,
        manifest: &CanonicalBatchManifest,
    ) -> PortResult<()> {
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(backend)?;
        if relations.source_replacements.is_empty() && relations.relocation.is_none() {
            Self::reject_source_owned_writes(&tx, upserts, deletes)?;
        }
        let mut trace_stages: Vec<(&'static str, std::time::Duration)> = Vec::new();
        let trace_started = trace::begin();
        Self::verify_pending_in_tx(&tx, pending, upserts, deletes, relations, manifest)?;
        if let Some(relocation) = &relations.relocation {
            Self::apply_relocation_in_tx(&tx, pending, relocation)?;
        }
        // Rebinding a historical empty placeholder must be revalidated inside
        // this write transaction, before the batch replaces the evidence. The
        // pre-parse reservation alone is never authorization.
        let authorized_placeholder_rebinds =
            SqliteStore::authorized_placeholder_rebinds(&tx, &relations.source_replacements)?;
        trace::add(&mut trace_stages, "verify_outbox", trace_started);

        // Session 元数据投影（schema v11）：收集本批触碰的 Session，提交末尾
        // 逐个重建其 `session_fts` 行（删除按 rowid 经边车定位，重插新投影）。
        // 覆盖 upsert/delete 实体、placement 变动（含移动归属的旧主）、以及
        // source replacement 的旧 placement 与 claim 行——与写入路径同事务。
        let trace_started = trace::begin();
        let mut affected_sessions = BTreeSet::new();
        for id in upserts.iter().map(|(id, _, _)| id).chain(deletes.iter()) {
            match id.kind() {
                IdKind::Session => {
                    affected_sessions.insert(id.as_str().to_string());
                }
                IdKind::Message => {
                    Self::collect_message_sessions(
                        &tx,
                        &[id.as_str().to_string()],
                        &mut affected_sessions,
                    )?;
                }
                IdKind::Document => {}
                IdKind::Source => {}
            }
        }
        let mut old_placement_ids = Vec::new();
        let mut source_paths = Vec::new();
        for delete in &relations.relation_deletes {
            if let RelationDeleteManifest::Placement(placement_id) = delete {
                old_placement_ids.push(placement_id.as_str().to_string());
            }
        }
        for upsert in &relations.relation_upserts {
            if let RelationUpsertManifest::Placement(placement) = upsert {
                // An upsert can move an existing placement to another Session;
                // read its old owner before the INSERT ... ON CONFLICT update.
                old_placement_ids.push(placement.id.as_str().to_string());
                affected_sessions.insert(placement.session_id.as_str().to_string());
                Self::collect_message_sessions(
                    &tx,
                    &[placement.message_id.as_str().to_string()],
                    &mut affected_sessions,
                )?;
            }
        }
        for source in &relations.source_replacements {
            source_paths.push(source.source_path.clone());
            for claim in &source.resume_claims {
                affected_sessions.insert(claim.session_id.clone());
            }
            let mut stmt = tx
                .prepare(
                    "SELECT placement_id FROM source_placement_membership
                     WHERE source_path = ?1",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([&source.source_path], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            for row in rows {
                old_placement_ids.push(row.map_err(backend)?);
            }
        }
        Self::collect_placement_sessions(&tx, &old_placement_ids, &mut affected_sessions)?;
        Self::collect_resume_claim_sessions(&tx, &source_paths, &mut affected_sessions)?;
        let alias_candidates = Self::alias_candidates(&tx, &source_paths)?;
        Self::invalidate_changed_vectors_in_tx(
            &tx,
            upserts
                .iter()
                .map(|(id, payload, _)| (id, payload.as_slice())),
            deletes,
        )?;
        trace::add(&mut trace_stages, "affected_sessions", trace_started);

        // 批量写入：同一事务内以多行 VALUES 语句替代逐行 prepared execute
        // （借鉴 hstry bulk_insert_messages_in_tx，MIT，
        // hstry/crates/hstry-core/src/db.rs:2990）。200K 实体 × 5 条语句的
        // 逐行 execute 支配了首次 ingest 的常数因子；批量后每 100 实体只发
        // 5 条语句。Scoped so the borrow ends before the relation/source
        // loops below.
        {
            let trace_catalog = trace::begin();
            const _: () = assert!(2 * BULK_INSERT_ROWS_PER_CHUNK <= 950);
            for chunk in upserts.chunks(BULK_INSERT_ROWS_PER_CHUNK) {
                let sql = format!(
                    "INSERT INTO catalog(id, payload) VALUES {}
                     ON CONFLICT(id) DO UPDATE SET payload = excluded.payload",
                    multi_row_values(chunk.len(), 2)
                );
                let rows: Vec<(String, &Vec<u8>)> = chunk
                    .iter()
                    .map(|(id, payload, _)| (id.as_str().to_string(), payload))
                    .collect();
                let params: Vec<&dyn rusqlite::ToSql> = rows
                    .iter()
                    .flat_map(|(wire, payload)| {
                        let c0: &dyn rusqlite::ToSql = wire;
                        let c1: &dyn rusqlite::ToSql = payload;
                        [c0, c1]
                    })
                    .collect();
                tx.execute(&sql, rusqlite::params_from_iter(params))
                    .map_err(backend)?;
            }
            // fts 行与 fts_ids 身份边车的批量维护（含按 rowid 的旧行删除）：
            // 与逐行路径同语义，rowid 显式分配（见 batch_upsert_fts_in_tx）。
            trace::add(&mut trace_stages, "catalog_upserts", trace_catalog);
            let trace_fts = trace::begin();
            Self::batch_upsert_fts_in_tx(&tx, upserts)?;
            trace::add(&mut trace_stages, "fts_projection", trace_fts);
            let trace_deletes = trace::begin();
            trace::add(&mut trace_stages, "catalog_deletes", trace_deletes);
            for chunk in deletes.chunks(BULK_INSERT_ROWS_PER_CHUNK) {
                let ids: Vec<&str> = chunk.iter().map(|id| id.as_str()).collect();
                let placeholders = in_placeholders(ids.len());
                tx.execute(
                    &format!("DELETE FROM catalog WHERE id IN ({placeholders})"),
                    rusqlite::params_from_iter(ids.iter().copied()),
                )
                .map_err(backend)?;
                tx.execute(
                    &format!(
                        "DELETE FROM fts
                         WHERE rowid IN (
                             SELECT fts_rowid FROM fts_ids WHERE wire_id IN ({placeholders})
                         )"
                    ),
                    rusqlite::params_from_iter(ids.iter().copied()),
                )
                .map_err(backend)?;
                tx.execute(
                    &format!("DELETE FROM fts_ids WHERE wire_id IN ({placeholders})"),
                    rusqlite::params_from_iter(ids.iter().copied()),
                )
                .map_err(backend)?;
            }
        }

        let trace_relations = trace::begin();
        for delete in &relations.relation_deletes {
            match delete {
                RelationDeleteManifest::Edge(placement_id) => {
                    tx.execute(
                        "DELETE FROM message_edges WHERE child_placement_id = ?1",
                        [placement_id.as_str()],
                    )
                    .map_err(backend)?;
                }
                RelationDeleteManifest::Placement(placement_id) => {
                    tx.execute(
                        "DELETE FROM message_placements WHERE placement_id = ?1",
                        [placement_id.as_str()],
                    )
                    .map_err(backend)?;
                }
                RelationDeleteManifest::Activity(activity_id) => {
                    tx.execute(
                        "DELETE FROM tool_activities WHERE activity_id = ?1",
                        [activity_id],
                    )
                    .map_err(backend)?;
                }
                RelationDeleteManifest::Usage(usage_id) => {
                    tx.execute("DELETE FROM usage_events WHERE usage_id = ?1", [usage_id])
                        .map_err(backend)?;
                }
            }
        }
        // 关系行 upsert：多行批量（借鉴 hstry bulk_insert_messages_in_tx，MIT，
        // hstry/crates/hstry-core/src/db.rs:2990）。message_placements 8 列 ×
        // 100 行 = 800 参数，message_edges 4 列 × 100 行 = 400 参数，均低于
        // SQLite 默认 999 变量上限（模块级编译期断言守住上限）。
        {
            let mut placement_rows: Vec<PlacementInsertRow<'_>> = Vec::new();
            for upsert in &relations.relation_upserts {
                if let RelationUpsertManifest::Placement(placement) = upsert {
                    let (byte_start, byte_end) = match &placement.span {
                        Some(span) => (
                            Some(i64::try_from(span.start).map_err(backend)?),
                            Some(i64::try_from(span.end).map_err(backend)?),
                        ),
                        None => (None, None),
                    };
                    placement_rows.push((
                        placement.id.as_str(),
                        placement.session_id.as_str(),
                        placement.source_document_id.as_str(),
                        placement.message_id.as_str(),
                        i64::from(placement.source_ordinal),
                        i64::from(placement.is_sidechain),
                        byte_start,
                        byte_end,
                    ));
                }
            }
            const _: () = assert!(8 * BULK_INSERT_ROWS_PER_CHUNK <= 950);
            for chunk in placement_rows.chunks(BULK_INSERT_ROWS_PER_CHUNK) {
                let sql = format!(
                    "INSERT INTO message_placements(
                         placement_id, session_id, document_id, message_id,
                         source_ordinal, is_sidechain, byte_start, byte_end
                     ) VALUES {}
                     ON CONFLICT(placement_id) DO UPDATE SET
                         session_id = excluded.session_id,
                         document_id = excluded.document_id,
                         message_id = excluded.message_id,
                         source_ordinal = excluded.source_ordinal,
                         is_sidechain = excluded.is_sidechain,
                         byte_start = excluded.byte_start,
                         byte_end = excluded.byte_end",
                    multi_row_values(chunk.len(), 8)
                );
                let params: Vec<&dyn rusqlite::ToSql> = chunk
                    .iter()
                    .flat_map(|row| {
                        let c0: &dyn rusqlite::ToSql = &row.0;
                        let c1: &dyn rusqlite::ToSql = &row.1;
                        let c2: &dyn rusqlite::ToSql = &row.2;
                        let c3: &dyn rusqlite::ToSql = &row.3;
                        let c4: &dyn rusqlite::ToSql = &row.4;
                        let c5: &dyn rusqlite::ToSql = &row.5;
                        let c6: &dyn rusqlite::ToSql = &row.6;
                        let c7: &dyn rusqlite::ToSql = &row.7;
                        [c0, c1, c2, c3, c4, c5, c6, c7]
                    })
                    .collect();
                tx.execute(&sql, rusqlite::params_from_iter(params))
                    .map_err(backend)?;
            }
            let mut edge_rows: Vec<(&str, &str, Option<&str>, &str)> = Vec::new();
            for upsert in &relations.relation_upserts {
                if let RelationUpsertManifest::Edge(edge) = upsert {
                    edge_rows.push((
                        edge.child_placement_id.as_str(),
                        edge.parent_message_id.as_str(),
                        edge.parent_native_id.as_deref(),
                        edge.relation.as_str(),
                    ));
                }
            }
            const _: () = assert!(4 * BULK_INSERT_ROWS_PER_CHUNK <= 950);
            for chunk in edge_rows.chunks(BULK_INSERT_ROWS_PER_CHUNK) {
                let sql = format!(
                    "INSERT INTO message_edges(
                         child_placement_id, parent_message_id,
                         parent_native_id, relation
                     ) VALUES {}
                     ON CONFLICT(child_placement_id) DO UPDATE SET
                         parent_message_id = excluded.parent_message_id,
                         parent_native_id = excluded.parent_native_id,
                         relation = excluded.relation",
                    multi_row_values(chunk.len(), 4)
                );
                let params: Vec<&dyn rusqlite::ToSql> = chunk
                    .iter()
                    .flat_map(|row| {
                        let c0: &dyn rusqlite::ToSql = &row.0;
                        let c1: &dyn rusqlite::ToSql = &row.1;
                        let c2: &dyn rusqlite::ToSql = &row.2;
                        let c3: &dyn rusqlite::ToSql = &row.3;
                        [c0, c1, c2, c3]
                    })
                    .collect();
                tx.execute(&sql, rusqlite::params_from_iter(params))
                    .map_err(backend)?;
            }
        }

        // 工具活动 upsert（v12）：逐行 upsert。活动行是内容寻址的（activity_id），
        // 同一事实跨源去重；行数由工具调用数决定，量级远小于 placements/edges。
        for upsert in &relations.relation_upserts {
            if let RelationUpsertManifest::Activity(activity) = upsert {
                tx.execute(
                    "INSERT INTO tool_activities(
                         activity_id, message_id, kind, actor, name, target, status
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(activity_id) DO UPDATE SET
                         message_id = excluded.message_id,
                         kind = excluded.kind,
                         actor = excluded.actor,
                         name = excluded.name,
                         target = excluded.target,
                         status = excluded.status",
                    rusqlite::params![
                        activity.activity_id,
                        activity.message_id,
                        activity.kind,
                        activity.actor,
                        activity.name,
                        activity.target,
                        activity.status,
                    ],
                )
                .map_err(backend)?;
            }
        }

        // token 用量事件 upsert（v15）：逐行 upsert。事件行内容寻址
        // （usage_id），同一事实跨源去重。
        for upsert in &relations.relation_upserts {
            if let RelationUpsertManifest::Usage(usage) = upsert {
                tx.execute(
                    "INSERT INTO usage_events(
                         usage_id, session_id, message_id, input_tokens, output_tokens,
                         cache_read_tokens, cache_write_tokens, reasoning_tokens, token_source
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT(usage_id) DO UPDATE SET
                         session_id = excluded.session_id,
                         message_id = excluded.message_id,
                         input_tokens = excluded.input_tokens,
                         output_tokens = excluded.output_tokens,
                         cache_read_tokens = excluded.cache_read_tokens,
                         cache_write_tokens = excluded.cache_write_tokens,
                         reasoning_tokens = excluded.reasoning_tokens,
                         token_source = excluded.token_source",
                    rusqlite::params![
                        usage.usage_id,
                        usage.session_id,
                        usage.message_id,
                        i64::try_from(usage.input_tokens).map_err(backend)?,
                        i64::try_from(usage.output_tokens).map_err(backend)?,
                        i64::try_from(usage.cache_read_tokens).map_err(backend)?,
                        i64::try_from(usage.cache_write_tokens).map_err(backend)?,
                        i64::try_from(usage.reasoning_tokens).map_err(backend)?,
                        usage.token_source,
                    ],
                )
                .map_err(backend)?;
            }
        }

        trace::add(&mut trace_stages, "relation_upserts", trace_relations);
        let trace_sources = trace::begin();
        for source in &relations.source_replacements {
            tx.execute(
                "DELETE FROM source_entity_projections WHERE source_path = ?1",
                [&source.source_path],
            )
            .map_err(backend)?;
            let rows: Vec<_> = source
                .projections
                .values()
                .map(|(id, payload, text)| {
                    Ok((
                        id.as_str(),
                        serde_json::to_string(id).map_err(backend)?,
                        payload,
                        text,
                    ))
                })
                .collect::<PortResult<Vec<_>>>()?;
            for chunk in rows.chunks(BULK_INSERT_ROWS_PER_CHUNK) {
                let sql = format!(
                    "INSERT INTO source_entity_projections(source_path, entity_id, id_json, payload, text) VALUES {}",
                    multi_row_values(chunk.len(), 5)
                );
                let params: Vec<&dyn rusqlite::ToSql> = chunk
                    .iter()
                    .flat_map(|row| {
                        [
                            &source.source_path as &dyn rusqlite::ToSql,
                            &row.0,
                            &row.1,
                            row.2,
                            row.3,
                        ]
                    })
                    .collect();
                tx.execute(&sql, rusqlite::params_from_iter(params))
                    .map_err(backend)?;
            }
            tx.execute(
                "DELETE FROM source_membership WHERE source_path = ?1",
                [&source.source_path],
            )
            .map_err(backend)?;
            // 每源成员行可能上千，多行批量插入（同 hstry bulk_insert 模式，
            // 3 列 × 100 行 = 300 参数）。
            const _: () = assert!(3 * BULK_INSERT_ROWS_PER_CHUNK <= 950);
            for chunk in source.entity_memberships.chunks(BULK_INSERT_ROWS_PER_CHUNK) {
                let sql = format!(
                    "INSERT INTO source_membership(source_path, message_id, document_id)
                     VALUES {}",
                    multi_row_values(chunk.len(), 3)
                );
                let params: Vec<&dyn rusqlite::ToSql> = chunk
                    .iter()
                    .flat_map(|membership| {
                        let c0: &dyn rusqlite::ToSql = &source.source_path;
                        let c1: &dyn rusqlite::ToSql = &membership.entity_id;
                        let c2: &dyn rusqlite::ToSql = &membership.document_id;
                        [c0, c1, c2]
                    })
                    .collect();
                tx.execute(&sql, rusqlite::params_from_iter(params))
                    .map_err(backend)?;
            }
            tx.execute(
                "DELETE FROM source_placement_membership WHERE source_path = ?1",
                [&source.source_path],
            )
            .map_err(backend)?;
            const _: () = assert!(2 * BULK_INSERT_ROWS_PER_CHUNK <= 950);
            for chunk in source.placement_ids.chunks(BULK_INSERT_ROWS_PER_CHUNK) {
                let sql = format!(
                    "INSERT INTO source_placement_membership(source_path, placement_id)
                     VALUES {}",
                    multi_row_values(chunk.len(), 2)
                );
                let rows: Vec<String> = chunk
                    .iter()
                    .map(|placement_id| placement_id.as_str().to_string())
                    .collect();
                let params: Vec<&dyn rusqlite::ToSql> = rows
                    .iter()
                    .flat_map(|placement_id| {
                        let c0: &dyn rusqlite::ToSql = &source.source_path;
                        let c1: &dyn rusqlite::ToSql = placement_id;
                        [c0, c1]
                    })
                    .collect();
                tx.execute(&sql, rusqlite::params_from_iter(params))
                    .map_err(backend)?;
            }
            // 工具活动成员（v12）：与 placement 同一生命周期——先清旧声明，
            // 本批带活动才写新行；无活动即清除（source 不再观察/移除）。
            tx.execute(
                "DELETE FROM tool_activity_membership WHERE source_path = ?1",
                [&source.source_path],
            )
            .map_err(backend)?;
            for activity_id in &source.activity_ids {
                tx.execute(
                    "INSERT INTO tool_activity_membership(source_path, activity_id)
                     VALUES(?1, ?2)",
                    rusqlite::params![&source.source_path, activity_id],
                )
                .map_err(backend)?;
            }
            // token 用量事件成员（v15）：与活动同一生命周期——先清旧声明，
            // 本批带用量才写新行；无用量的 source 即清除其旧声明。
            tx.execute(
                "DELETE FROM usage_event_membership WHERE source_path = ?1",
                [&source.source_path],
            )
            .map_err(backend)?;
            for usage_id in &source.usage_ids {
                tx.execute(
                    "INSERT INTO usage_event_membership(source_path, usage_id)
                     VALUES(?1, ?2)",
                    rusqlite::params![&source.source_path, usage_id],
                )
                .map_err(backend)?;
            }
            tx.execute(
                "INSERT INTO source_scans(
                     source_path, scanned_at_ms, len_bytes, fingerprint, provider_id,
                     parser_version
                 )
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(source_path) DO UPDATE SET
                     scanned_at_ms = excluded.scanned_at_ms,
                     len_bytes = excluded.len_bytes,
                     fingerprint = excluded.fingerprint,
                     provider_id = COALESCE(excluded.provider_id, source_scans.provider_id),
                     parser_version = excluded.parser_version",
                rusqlite::params![
                    &source.source_path,
                    unix_ms()?,
                    source.len_bytes,
                    source.fingerprint,
                    source.provider_id,
                    i64::from(PARSER_SEMANTIC_VERSION),
                ],
            )
            .map_err(backend)?;
            if source.relation_complete {
                tx.execute(
                    "INSERT INTO source_relation_scans(source_path, relation_schema_version)
                     VALUES(?1, ?2)
                     ON CONFLICT(source_path) DO UPDATE SET
                         relation_schema_version = excluded.relation_schema_version",
                    rusqlite::params![&source.source_path, RELATION_SCHEMA_VERSION],
                )
                .map_err(backend)?;
            } else {
                tx.execute(
                    "DELETE FROM source_relation_scans WHERE source_path = ?1",
                    [&source.source_path],
                )
                .map_err(backend)?;
            }
            if source.entity_memberships.is_empty()
                && source.fingerprint.is_none()
                && source.len_bytes.is_none()
            {
                tx.execute(
                    "DELETE FROM source_installations WHERE source_path=?1",
                    [&source.source_path],
                )
                .map_err(backend)?;
            } else if let Some(installation) = &source.installation {
                Self::persist_installation_in_tx(
                    &tx,
                    &source.source_path,
                    installation,
                    (self.relocation_clock)()?,
                    authorized_placeholder_rebinds.contains(&source.source_path),
                )?;
            }
            // Source-scoped Resume Metadata 声明（ADR-0009）：随 source replacement
            // 同事务原子替换——先清旧声明，本批带声明才写新行；无声明
            // （source 不再观察/移除）即清除，绝不残留旧声明。
            tx.execute(
                "DELETE FROM source_session_resume_claims WHERE source_path = ?1",
                [&source.source_path],
            )
            .map_err(backend)?;
            for claim in &source.resume_claims {
                tx.execute(
                    "INSERT INTO source_session_resume_claims(
                         source_path, session_id, provider_id, provider_session_id,
                         provider_session_id_state, original_working_directory,
                         original_working_directory_state, pair_observed
                     ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        &source.source_path,
                        &claim.session_id,
                        &claim.provider_id,
                        &claim.provider_session_id,
                        &claim.provider_session_id_state,
                        &claim.original_working_directory,
                        &claim.original_working_directory_state,
                        i64::from(claim.pair_observed),
                    ],
                )
                .map_err(backend)?;
            }
        }

        trace::add(&mut trace_stages, "source_replacements", trace_sources);
        let trace_sessions = trace::begin();
        let batch_sources: Vec<String> = relations
            .source_replacements
            .iter()
            .map(|replacement| replacement.source_path.clone())
            .collect();
        let in_memory_payloads: BTreeMap<&str, &[u8]> = upserts
            .iter()
            .map(|(id, payload, _)| (id.as_str(), payload.as_slice()))
            .collect();
        Self::regenerate_compatibility_aliases_in_tx(
            &tx,
            &batch_sources,
            &in_memory_payloads,
            alias_candidates,
        )?;
        let resolver: &dyn RepoSlugResolver = &**self.repo_slug_resolver.borrow();
        for session_wire in affected_sessions {
            Self::rebuild_session_search_row_in_tx(
                &tx,
                &session_wire,
                resolver,
                RepoIdentityRebuild::Rederive,
            )?;
        }

        // 本批触碰的关系行：只校验这些 id 的引用完整性。
        trace::add(&mut trace_stages, "session_projection", trace_sessions);
        let trace_integrity = trace::begin();
        let mut touched_placements: Vec<String> = Vec::new();
        let mut touched_edges: Vec<String> = Vec::new();
        let mut touched_claims: Vec<String> = Vec::new();
        for source in &relations.source_replacements {
            touched_claims.extend(
                source
                    .placement_ids
                    .iter()
                    .map(|id| id.as_str().to_string()),
            );
        }
        for upsert in &relations.relation_upserts {
            match upsert {
                RelationUpsertManifest::Placement(placement) => {
                    touched_placements.push(placement.id.as_str().to_string());
                }
                RelationUpsertManifest::Edge(edge) => {
                    touched_edges.push(edge.child_placement_id.as_str().to_string());
                }
                // 工具活动/用量事件不参与 placement/edge 引用完整性校验
                // （独立表）。
                RelationUpsertManifest::Activity(_) | RelationUpsertManifest::Usage(_) => {}
            }
        }
        for delete in &relations.relation_deletes {
            match delete {
                RelationDeleteManifest::Placement(id) => {
                    touched_placements.push(id.as_str().to_string());
                }
                RelationDeleteManifest::Edge(id) => {
                    touched_edges.push(id.as_str().to_string());
                }
                RelationDeleteManifest::Activity(_) | RelationDeleteManifest::Usage(_) => {}
            }
        }
        Self::verify_relational_integrity_in_tx(
            &tx,
            &touched_placements,
            &touched_edges,
            &touched_claims,
        )?;
        // B1 路径（裸 commit_batch/commit_index_batch）只删 catalog 实体、不维护
        // v7 关系行：被删实体若仍被 message_placements/message_edges 引用，会留下
        // 悬空引用，必须在此拒绝（B2 路径的删除按 claimer 推导，天然无悬空）。
        Self::verify_deleted_entities_unreferenced_in_tx(&tx, deletes)?;
        if let Some(relocation) = &relations.relocation {
            relocation.verify_snapshots()?;
        }

        trace::add(&mut trace_stages, "integrity_checks", trace_integrity);
        let trace_generation = trace::begin();
        tx.execute(
            "UPDATE store_metadata SET active_generation = ?1 WHERE singleton = 1",
            [pending.target_generation as i64],
        )
        .map_err(backend)?;
        tx.execute(
            "UPDATE index_batches
             SET state = 'activated', durable_point = 'activated', committed_at_ms = ?2
             WHERE operation_id = ?1",
            rusqlite::params![pending.operation_id, unix_ms()?],
        )
        .map_err(backend)?;

        trace::add(&mut trace_stages, "generation_activate", trace_generation);
        let trace_commit = trace::begin();
        tx.commit().map_err(backend)?;
        trace::add(&mut trace_stages, "tx_commit", trace_commit);
        trace::emit(
            "adapter:commit",
            &trace_stages,
            &format!(
                "upserts={} deletes={} relation_upserts={} relation_deletes={}",
                upserts.len(),
                deletes.len(),
                relations.relation_upserts.len(),
                relations.relation_deletes.len()
            ),
        );
        Ok(())
    }

    /// 校验本批触碰的关系行引用完整性。
    ///
    /// 只检查本批 upsert/delete 涉及的 placement/edge/claim ids：未触碰行的
    /// 完整性由归纳保持（每次提交维护自身行、删除只删本批 claims）。全表
    /// 扫描版本使每批提交成本 O(全库)，是首次 ingest O(n²) 的来源之一。
    fn verify_relational_integrity_in_tx(
        tx: &rusqlite::Transaction<'_>,
        touched_placement_ids: &[String],
        touched_edge_ids: &[String],
        touched_claim_ids: &[String],
    ) -> PortResult<()> {
        for chunk in chunk_ids(touched_placement_ids) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let missing_entity: Option<String> = tx
                .query_row(
                    &format!(
                        "SELECT placement_id
                         FROM message_placements
                         WHERE placement_id IN ({placeholders})
                           AND (NOT EXISTS(
                                    SELECT 1 FROM catalog WHERE id = message_placements.session_id
                                )
                             OR NOT EXISTS(
                                    SELECT 1 FROM catalog WHERE id = message_placements.document_id
                                )
                             OR NOT EXISTS(
                                    SELECT 1 FROM catalog WHERE id = message_placements.message_id
                                ))
                         LIMIT 1"
                    ),
                    rusqlite::params_from_iter(chunk.iter()),
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            if missing_entity.is_some() {
                return Err(PortError::Backend(
                    "message placement references a missing catalog entity".into(),
                ));
            }
        }

        for chunk in chunk_ids(touched_edge_ids) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let missing_placement: Option<String> = tx
                .query_row(
                    &format!(
                        "SELECT child_placement_id
                         FROM message_edges
                         WHERE child_placement_id IN ({placeholders})
                           AND NOT EXISTS(
                               SELECT 1 FROM message_placements
                               WHERE placement_id = message_edges.child_placement_id
                           )
                         LIMIT 1"
                    ),
                    rusqlite::params_from_iter(chunk.iter()),
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            if missing_placement.is_some() {
                return Err(PortError::Backend(
                    "message edge references a missing child placement".into(),
                ));
            }
        }

        for chunk in chunk_ids(touched_claim_ids) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let missing_claim: Option<String> = tx
                .query_row(
                    &format!(
                        "SELECT placement_id
                         FROM source_placement_membership
                         WHERE placement_id IN ({placeholders})
                           AND NOT EXISTS(
                               SELECT 1 FROM message_placements
                               WHERE placement_id = source_placement_membership.placement_id
                           )
                         LIMIT 1"
                    ),
                    rusqlite::params_from_iter(chunk.iter()),
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            if missing_claim.is_some() {
                return Err(PortError::Backend(
                    "source placement claim references a missing placement".into(),
                ));
            }
        }
        Ok(())
    }

    /// 校验 delete 列表中的 catalog 实体不被任何 v7 关系行引用。
    ///
    /// B1 路径（裸 commit_batch/commit_index_batch）不维护关系行：若被删实体仍被
    /// `message_placements`（session/document/message 任一身份）或 `message_edges`
    /// （parent_message_id）引用，提交会留下悬空引用且事后才被发现。按 chunk 检查
    /// 引用存在性，任一命中即拒绝整批。
    fn verify_deleted_entities_unreferenced_in_tx(
        tx: &rusqlite::Transaction<'_>,
        deletes: &[StableId],
    ) -> PortResult<()> {
        let ids: Vec<&str> = deletes.iter().map(|id| id.as_str()).collect();
        for chunk in chunk_ids(&ids) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let dangling_placement: Option<String> = tx
                .query_row(
                    &format!(
                        "SELECT placement_id
                         FROM message_placements
                         WHERE session_id IN ({placeholders})
                            OR document_id IN ({placeholders})
                            OR message_id IN ({placeholders})
                         LIMIT 1"
                    ),
                    rusqlite::params_from_iter(
                        chunk.iter().chain(chunk.iter()).chain(chunk.iter()),
                    ),
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            if dangling_placement.is_some() {
                return Err(PortError::Backend(
                    "cannot delete a catalog entity still referenced by message placements".into(),
                ));
            }
            let dangling_resume_claim: Option<String> = tx
                .query_row(
                    &format!(
                        "SELECT source_path
                         FROM source_session_resume_claims
                         WHERE session_id IN ({placeholders})
                         LIMIT 1"
                    ),
                    rusqlite::params_from_iter(chunk.iter()),
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            if dangling_resume_claim.is_some() {
                return Err(PortError::Backend(
                    "cannot delete a catalog session still referenced by resume metadata claims"
                        .into(),
                ));
            }
            let dangling_edge: Option<String> = tx
                .query_row(
                    &format!(
                        "SELECT child_placement_id
                         FROM message_edges
                         WHERE parent_message_id IN ({placeholders})
                         LIMIT 1"
                    ),
                    rusqlite::params_from_iter(chunk.iter()),
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)?;
            if dangling_edge.is_some() {
                return Err(PortError::Backend(
                    "cannot delete a catalog entity still referenced by message edges".into(),
                ));
            }
        }
        Ok(())
    }

    /// 在给定事务内批量维护一批实体的 fts 行与 fts_ids 身份边车。
    ///
    /// 与 [`Self::upsert_fts_row_in_tx`]（单条路径）同语义，但以多行
    /// `INSERT ... VALUES (...),(...),...` 批量执行（借鉴 hstry
    /// `bulk_insert_messages_in_tx`，MIT，hstry/crates/hstry-core/src/db.rs:2990）：
    /// 先按边车记录的 rowid 批量删除旧 fts 行（避免内容列整表扫描），再按
    /// kind 门控——只有 Message 实体进入 fts 全文表，session/document 是
    /// 容器实体，索引其正文会让搜索命中重复计数；非 Message 只保留身份边车
    /// （fts_rowid 为 NULL，按 NULL 定位删除即无操作）。
    ///
    /// fts5 的 rowid 由本函数显式分配（`INSERT INTO fts(rowid, ...)`）：
    /// 多行 INSERT 无法逐行取 `last_insert_rowid()`（只返回最后一行），而
    /// `RETURNING rowid` 在 fts5 上不可用（实测返回 -1）。分配从当前
    /// `MAX(rowid)+1` 起顺序递增；本批次每个 wire_id 至多出现一次且旧行
    /// 已先删除，故不会与存量行或同批其他行冲突。索引侧正文与查询侧配对
    /// 同一 CJK n-gram transform（ADR-0007，单字 + bigram）。
    fn batch_upsert_fts_in_tx(
        tx: &rusqlite::Transaction<'_>,
        upserts: &[(StableId, Vec<u8>, String)],
    ) -> PortResult<()> {
        if upserts.is_empty() {
            return Ok(());
        }
        let mut next_fts_rowid: Option<i64> = None;
        let mut trace_fts_time = std::time::Duration::ZERO;
        let mut trace_sidecar_time = std::time::Duration::ZERO;
        let mut allocate_rowid = || -> PortResult<i64> {
            match next_fts_rowid {
                Some(id) => {
                    next_fts_rowid = Some(id + 1);
                    Ok(id)
                }
                None => {
                    let max: i64 = tx
                        .query_row("SELECT COALESCE(MAX(rowid), 0) FROM fts", [], |row| {
                            row.get(0)
                        })
                        .map_err(backend)?;
                    next_fts_rowid = Some(max + 2);
                    Ok(max + 1)
                }
            }
        };
        for chunk in upserts.chunks(BULK_INSERT_ROWS_PER_CHUNK) {
            let trace_chunk = trace::begin();
            // StableId 无字符串反解构造器，故存其 serde JSON 以便查询时无损重建
            // （wire 串不含 stability，无法从 as_str() 还原完整身份）。
            let ids: Vec<&str> = chunk.iter().map(|(id, _, _)| id.as_str()).collect();
            let placeholders = in_placeholders(ids.len());
            tx.execute(
                &format!(
                    "DELETE FROM fts
                     WHERE rowid IN (
                         SELECT fts_rowid FROM fts_ids WHERE wire_id IN ({placeholders})
                     )"
                ),
                rusqlite::params_from_iter(ids.iter().copied()),
            )
            .map_err(backend)?;
            tx.execute(
                &format!("DELETE FROM fts_ids WHERE wire_id IN ({placeholders})"),
                rusqlite::params_from_iter(ids.iter().copied()),
            )
            .map_err(backend)?;

            // 显式 rowid 的顺序与消息实体顺序一一对应；非 Message 边车行 rowid 为 NULL。
            // 先收集自有行（id_json/wire 是逐实体新建的 String，不能跨语句借用），
            // 再在 execute 语句内取引用构造参数。
            let mut fts_rows: Vec<(i64, String, String)> = Vec::new();
            let mut fts_ids_rows: Vec<(String, String, Option<i64>)> = Vec::new();
            for (id, _payload, text) in chunk {
                let id_json = serde_json::to_string(id).map_err(backend)?;
                if id.kind() == IdKind::Message {
                    let fts_rowid = allocate_rowid()?;
                    // 索引侧强制有界（借鉴清单 #3）：入口已截断的 text 原样通过，
                    // 未截断的直接写入路径（MCP/单条 index）在此兜底。
                    fts_rows.push((
                        fts_rowid,
                        id_json.clone(),
                        fts_tokens_cjk(&bounded_index_text(text)),
                    ));
                    fts_ids_rows.push((id.as_str().to_string(), id_json, Some(fts_rowid)));
                } else {
                    fts_ids_rows.push((id.as_str().to_string(), id_json, None));
                }
            }
            if !fts_rows.is_empty() {
                const _: () = assert!(3 * BULK_INSERT_ROWS_PER_CHUNK <= 950);
                let sql = format!(
                    "INSERT INTO fts(rowid, id, text) VALUES {}",
                    multi_row_values(fts_rows.len(), 3)
                );
                let params: Vec<&dyn rusqlite::ToSql> = fts_rows
                    .iter()
                    .flat_map(|(rowid, id_json, text)| {
                        let c0: &dyn rusqlite::ToSql = rowid;
                        let c1: &dyn rusqlite::ToSql = id_json;
                        let c2: &dyn rusqlite::ToSql = text;
                        [c0, c1, c2]
                    })
                    .collect();
                tx.execute(&sql, rusqlite::params_from_iter(params))
                    .map_err(backend)?;
            }
            trace_fts_time += trace::elapsed(trace_chunk);
            let trace_sidecar = trace::begin();
            const _: () = assert!(3 * BULK_INSERT_ROWS_PER_CHUNK <= 950);
            let sql = format!(
                "INSERT INTO fts_ids(wire_id, id_json, fts_rowid) VALUES {}",
                multi_row_values(fts_ids_rows.len(), 3)
            );
            let params: Vec<&dyn rusqlite::ToSql> = fts_ids_rows
                .iter()
                .flat_map(|(wire, id_json, rowid)| {
                    let c0: &dyn rusqlite::ToSql = wire;
                    let c1: &dyn rusqlite::ToSql = id_json;
                    let c2: &dyn rusqlite::ToSql = rowid;
                    [c0, c1, c2]
                })
                .collect();
            tx.execute(&sql, rusqlite::params_from_iter(params))
                .map_err(backend)?;
            trace_sidecar_time += trace::elapsed(trace_sidecar);
        }
        trace::emit(
            "adapter:fts",
            &[
                ("fts_projection_rows", trace_fts_time),
                ("fts_ids_insert", trace_sidecar_time),
            ],
            &format!("entities={}", upserts.len()),
        );
        Ok(())
    }

    /// 在给定事务内维护单条实体的 fts 行与 fts_ids 身份边车。
    ///
    /// 与批量提交路径同语义：先按边车记录的 rowid 删除旧 fts 行（避免内容列整表
    /// 扫描），再按 kind 门控——只有 Message 实体进入 fts 全文表，session/document
    /// 是容器实体，索引其正文会让搜索命中重复计数；非 Message 只保留身份边车
    /// （fts_rowid 为 NULL，按 NULL 定位删除即无操作）。
    /// 单条 `SearchIndex::index` 与 `CatalogStore::put` 共用（后者投影自 payload）。
    fn upsert_fts_row_in_tx(
        tx: &rusqlite::Transaction<'_>,
        id: &StableId,
        text: &str,
    ) -> PortResult<()> {
        // StableId 无字符串反解构造器，故存其 serde JSON 以便查询时无损重建
        // （wire 串不含 stability，无法从 as_str() 还原完整身份）。
        let id_json = serde_json::to_string(id).map_err(backend)?;
        tx.execute(
            "DELETE FROM fts
             WHERE rowid = (SELECT fts_rowid FROM fts_ids WHERE wire_id = ?1)",
            [id.as_str()],
        )
        .map_err(backend)?;
        tx.execute("DELETE FROM fts_ids WHERE wire_id = ?1", [id.as_str()])
            .map_err(backend)?;
        let fts_rowid = if id.kind() == IdKind::Message {
            // 索引侧 CJK n-gram（ADR-0007，单字 + bigram）：`SearchIndex::index`
            // 与 `CatalogStore::put` 两条单条写入路径与批量提交共用同一 transform；
            // 先施加有界截断（借鉴清单 #3，与 searchable_text/批量路径同上限）。
            tx.execute(
                "INSERT INTO fts(id, text) VALUES(?1, ?2)",
                rusqlite::params![id_json, fts_tokens_cjk(&bounded_index_text(text))],
            )
            .map_err(backend)?;
            Some(tx.last_insert_rowid())
        } else {
            None
        };
        tx.execute(
            "INSERT INTO fts_ids(wire_id, id_json, fts_rowid) VALUES(?1, ?2, ?3)",
            rusqlite::params![id.as_str(), id_json, fts_rowid],
        )
        .map_err(backend)?;
        Ok(())
    }

    /// 在事务内把活动 generation 推进 1（单条 put/index 写入用）。
    ///
    /// 与批量路径（durable outbox 的 `base+1`）共用同一"内容变更即失效旧游标"
    /// 语义；单条路径无 CAS 前置（无并发写者场景），`active_generation + 1`
    /// 保证单调递增即可。
    fn advance_generation_in_tx(tx: &rusqlite::Transaction<'_>) -> PortResult<()> {
        tx.execute(
            "UPDATE store_metadata SET active_generation = active_generation + 1
             WHERE singleton = 1",
            [],
        )
        .map_err(backend)?;
        Ok(())
    }

    /// 从权威 catalog 全量重投影 FTS 索引，通过 durable outbox + generation 保证
    /// 重建期崩溃不污染当前活动 generation。
    ///
    /// catalog 是内容的权威事实源，`fts` 搜索索引是可重建的派生投影（ADR-0001）。本方法：
    /// 1. 以 catalog 为权威实体集，读取全部 `(id, payload)`，用 [`searchable_text`] 投影检索正文；
    ///    实体身份优先取 `fts_ids.id_json`（保真 kind+stability），缺失时回退 `from_wire`；
    /// 2. 先持久化一条 `building` intent，记录本次将索引的完整 id 集合与 digest；
    /// 3. 在单事务内**整表清空** `fts`/`fts_ids` 后按 catalog 集合重新写入，推进 generation，
    ///    并把 intent 标记 `activated`。
    ///
    /// 整表清空（而非逐条 upsert）是刻意的：rebuild 的场景正是“搜索索引已漂移/损坏”，
    /// 需清除任何不在 catalog 中的孤儿 `fts`/`fts_ids` 行。若事务中途失败，整个重建回滚，旧
    /// generation 及其索引原样保留，search 仍可用旧投影（“失败不污染旧 generation”）。
    ///
    /// 身份保真取 `fts_ids` 而非纯从 catalog wire 串还原：wire 串不含 stability，
    /// `from_wire` 只能得到 `Unstable`，会让 rebuild 后的搜索结果丢失原 Native/Reconstructed
    /// 身份（见 `SearchIndex::query` 存 id_json 的原因）。catalog 权威决定“有哪些实体、正文是什么”，
    /// `fts_ids` 保真“每个实体的完整身份”。
    ///
    /// rebuild 是显式维护命令，即使内容与现有投影一致也照常推进 generation——操作者
    /// 主动请求“干净重建”，不做 no-op 短路。返回重新索引的实体条数。
    ///
    /// 显式重建会**重派生** repo 身份投影（重新探测本机 git）；打开时的投影版本
    /// 自愈走同一实现但保留该投影，见 [`RepoIdentityRebuild`]。
    pub fn rebuild_index(&self) -> PortResult<usize> {
        self.reproject_from_catalog(RepoIdentityRebuild::Rederive)
    }

    /// [`rebuild_index`](Self::rebuild_index) 的实现体，`repo_identity` 决定
    /// 是否重派生 repo 身份投影（见 [`RepoIdentityRebuild`]）。
    fn reproject_from_catalog(&self, repo_identity: RepoIdentityRebuild) -> PortResult<usize> {
        // 1) 以 catalog 为权威实体集，投影检索正文；身份优先取 fts_ids 保真。
        // 单个 LEFT JOIN 取代逐行 fts_ids 查询(每行一次 prepare+execute 的 N+1)。
        let upserts: Vec<(StableId, Vec<u8>, String)> = {
            let conn = self.conn.borrow();
            let mut stmt = conn
                .prepare(
                    "SELECT catalog.id, catalog.payload, fts_ids.id_json
                     FROM catalog
                     LEFT JOIN fts_ids ON fts_ids.wire_id = catalog.id
                     ORDER BY catalog.id ASC",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([], |row| {
                    let wire: String = row.get(0)?;
                    let payload: Vec<u8> = row.get(1)?;
                    let id_json: Option<String> = row.get(2)?;
                    Ok((wire, payload, id_json))
                })
                .map_err(backend)?;
            let mut out = Vec::new();
            for row in rows {
                let (wire, payload, id_json) = row.map_err(backend)?;
                // 优先用 fts_ids 里保真的 id_json（含 kind+stability）；缺失才回退 from_wire。
                let id = match id_json {
                    Some(json) => serde_json::from_str(&json).map_err(backend)?,
                    None => StableId::from_wire(&wire).ok_or_else(|| {
                        PortError::Backend("catalog contains an invalid entity id".into())
                    })?,
                };
                let text = searchable_text(&payload);
                out.push((id, payload, text));
            }
            out
        };

        // 2) durable intent：记录本次重建将索引的完整集合（崩溃后 recover 会 abort 它）。
        let relations = RelationManifests::default();
        let manifest = batch_manifest(&upserts, &[], &relations)?;
        let pending = self.begin_index_batch_with_manifest(&manifest, &upserts, &[], &relations)?;

        // 3) 单事务：校验句柄 → 整表清空 FTS → 按 catalog 重投影 → 推进 generation → 标记 activated。
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(backend)?;
        Self::verify_pending_in_tx(&tx, &pending, &upserts, &[], &relations, &manifest)?;

        // Historical orphan vectors are repairable only on this writer-side
        // maintenance path. Readiness/query never mutate; live vectors survive.
        tx.execute(
            "DELETE FROM message_vec WHERE NOT EXISTS (
                 SELECT 1 FROM catalog WHERE catalog.id = message_vec.wire_id
             )",
            [],
        )
        .map_err(backend)?;
        tx.execute("DELETE FROM fts", []).map_err(backend)?;
        tx.execute("DELETE FROM fts_ids", []).map_err(backend)?;
        // Session 元数据投影（schema v11）：全量重建 `session_fts`——与消息
        // FTS 同一"catalog + claims 可重建投影"不变量，从声明与目录逐会话
        // 重投影，绝不从既有 session_fts 内容复制。repo identity（schema
        // v16）同批重建，解析器来自注入的 [`RepoSlugResolver`]。
        let resolver: &dyn RepoSlugResolver = &**self.repo_slug_resolver.borrow();
        Self::rebuild_all_session_search_in_tx(&tx, resolver, repo_identity)?;
        // 与提交路径一致：只有 Message 实体重投影进 fts，且把 fts5 行 rowid 回写
        // 进 fts_ids 边车，删除才能按 rowid 定位（见 ensure_fts_ids_rowid）。
        // 批量多行写入（与提交路径共用 batch_upsert_fts_in_tx；整表清空后
        // rowid 从 1 起显式分配，语义与逐行 last_insert_rowid 一致）。
        Self::batch_upsert_fts_in_tx(&tx, &upserts)?;

        tx.execute(
            "UPDATE store_metadata SET active_generation = ?1 WHERE singleton = 1",
            [pending.target_generation as i64],
        )
        .map_err(backend)?;
        // 投影版本戳与重投影同事务（schema v17）：只有"整库从 catalog 重投影"
        // 才能声明全库投影属于当前变换版本——增量提交路径只覆盖本批实体，
        // 因此刻意不在那里盖戳。回滚时戳与投影一起回滚，不会谎报已收敛。
        tx.execute(
            "UPDATE store_metadata SET index_projection_version = ?1 WHERE singleton = 1",
            [i64::from(INDEX_PROJECTION_VERSION)],
        )
        .map_err(backend)?;
        tx.execute(
            "UPDATE index_batches
             SET state = 'activated', durable_point = 'activated', committed_at_ms = ?2
             WHERE operation_id = ?1",
            rusqlite::params![pending.operation_id, unix_ms()?],
        )
        .map_err(backend)?;

        tx.commit().map_err(backend)?;
        Ok(upserts.len())
    }

    /// 崩溃恢复：把所有停在 `building` 的 outbox 行标记为 `aborted`。
    ///
    /// FTS5 单存储下 `building` 行必然无已提交副作用（apply 与 activate 同事务，
    /// 要么全成要么全滚），故恢复动作是幂等的纯 journal 清理，不触碰 catalog/FTS。
    /// 返回被 abort 的批次数，供诊断输出。写路径打开时自动调用。
    pub fn recover_interrupted(&self) -> PortResult<usize> {
        let conn = self.conn.borrow();
        let n = conn
            .execute(
                "UPDATE index_batches
                 SET state = 'aborted', error_code = 'interrupted_before_activation'
                 WHERE state = 'building'",
                [],
            )
            .map_err(backend)?;
        Ok(n)
    }

    /// 只读统计停在 `building` 的 outbox 行数——中断恢复的"待收敛"证据。
    ///
    /// 与 [`recover_interrupted`](Self::recover_interrupted) 不同，本方法不改状态：
    /// 供 doctor 等只读路径观测“有多少无副作用 intent 尚待下次写打开收敛”，
    /// 作为 durable outbox 中断恢复与 generation 一致性的证据。
    pub fn interrupted_batch_count(&self) -> PortResult<u64> {
        let conn = self.conn.borrow();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM index_batches WHERE state = 'building'",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        u64::try_from(n).map_err(backend)
    }

    /// 读回一条 outbox 行（供测试与诊断）。
    pub fn index_batch(&self, operation_id: &str) -> PortResult<Option<IndexBatch>> {
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare(
                "SELECT operation_id, base_generation, target_generation, state,
                        operation_digest, upsert_ids_json, delete_ids_json,
                        relation_upserts_json, relation_deletes_json,
                        source_replacements_json, durable_point, error_code
                 FROM index_batches WHERE operation_id = ?1",
            )
            .map_err(backend)?;
        let mut rows = stmt.query([operation_id]).map_err(backend)?;
        match rows.next().map_err(backend)? {
            None => Ok(None),
            Some(row) => {
                let upsert_json: String = row.get(5).map_err(backend)?;
                let delete_json: String = row.get(6).map_err(backend)?;
                let relation_upserts_json: String = row.get(7).map_err(backend)?;
                let relation_deletes_json: String = row.get(8).map_err(backend)?;
                let source_replacements_json: String = row.get(9).map_err(backend)?;
                Ok(Some(IndexBatch {
                    operation_id: row.get(0).map_err(backend)?,
                    base_generation: u64::try_from(row.get::<_, i64>(1).map_err(backend)?)
                        .map_err(backend)?,
                    target_generation: u64::try_from(row.get::<_, i64>(2).map_err(backend)?)
                        .map_err(backend)?,
                    state: row.get(3).map_err(backend)?,
                    operation_digest: row.get(4).map_err(backend)?,
                    upsert_ids: serde_json::from_str(&upsert_json).map_err(backend)?,
                    delete_ids: serde_json::from_str(&delete_json).map_err(backend)?,
                    relation_upserts: serde_json::from_str(&relation_upserts_json)
                        .map_err(backend)?,
                    relation_deletes: serde_json::from_str(&relation_deletes_json)
                        .map_err(backend)?,
                    source_replacements: serde_json::from_str(&source_replacements_json)
                        .map_err(backend)?,
                    durable_point: row.get(10).map_err(backend)?,
                    error_code: row.get(11).map_err(backend)?,
                }))
            }
        }
    }
}

/// Canonical relation projection version stored in `source_relation_scans`.
/// Resume-claim schema changes do not change relation completeness semantics.
const RELATION_SCHEMA_VERSION: i64 = 7;

/// 解析语义版本：任何改变已索引内容的解析语义升级都必须 +1（provider 解析层
/// 变化——如噪声过滤规则、字符串形态支持、投影字段语义——都属于；纯 schema
/// 结构变化不在此列，那走 [`SCHEMA_VERSION`] 迁移）。
///
/// 借鉴 Recall 的 parser_version 增量同步（usage/event parser_version 三层
/// 判断）：sync 的 unchanged 判定把 `source_scans.parser_version` 纳入比较，
/// 已存版本落后于该常量的源即使字节未变也走 targeted backfill（重跑 parse +
/// commit），并在重新 commit 时写回当前版本。单测（lib.rs
/// `stale_parser_version_forces_reparse_and_converges`）锁住该语义。
pub const PARSER_SEMANTIC_VERSION: u32 = 5;

/// 索引投影版本：任何改变 **FTS 词元流或派生投影文本** 的变化都必须 +1。
///
/// 与 [`PARSER_SEMANTIC_VERSION`] 正交——后者管"从 provider 源解析出的
/// canonical payload 语义"（变化 → 按源 reparse），本常量管"从权威 catalog
/// 投影出的检索索引形态"（变化 → 从 catalog 重投影，**不需要 reparse**）。
/// 两者都是"索引期与查询期必须一致"的契约，但失配后的修复动作不同。
///
/// 必须 +1 的变化（非穷举，但覆盖已知全部投影输入）：
/// - CJK 分词变换（[`agent_session_grep_application::fts_tokens_cjk`]）产出的
///   词元流——unigram/bigram 规则、汉字分类、运行分隔规则；
/// - 保留分级上限（[`agent_session_grep_application::MESSAGE_FTS_MAX_CHARS`]）
///   与 [`bounded_index_text`] 的截断规则——它改变被索引的正文；
/// - [`searchable_text`] 的 payload → 检索正文投影规则；
/// - `session_fts` 字段构成（[`SqliteStore::session_search_text`] 取哪些
///   字段、`SESSION_SEARCH_FIELD_CHARS` 上限）；
/// - `session_titles` 派生链与 [`SESSION_TITLE_MAX_CHARS`]——与 `session_fts`
///   同属 `rebuild_index` 一次重投影覆盖的派生投影，规则变化后旧行同样滞留。
///
/// **半覆盖**：`session_repo_slugs` 的 slug 派生规则同样属于"投影规则变了"，
/// 但该投影不可从 catalog 重建（slug 只能现场探测本机 git），所以只有显式
/// `index rebuild` 会重派生它；打开时的投影版本自愈刻意保留既有行
/// （见 [`RepoIdentityRebuild`]）。slug 规则变化的收敛动作因此是显式 rebuild，
/// 不要指望写路径打开时自动收敛。
///
/// **不**属于本轴：`message_vec` 语义向量（自带 model_id/dimension 归属，
/// 换模型即失效）、`tool_activities`/`usage_events`（claims 派生，随
/// `PARSER_SEMANTIC_VERSION` 的 reparse 收敛）、纯 schema 结构变化
/// （走 [`SCHEMA_VERSION`] 迁移）。
///
/// 存储镜像是 `store_metadata.index_projection_version`（schema v17，库级
/// singleton，不是 per-source）。失配处理：写路径打开时自动从 catalog 重投影
/// （[`SqliteStore::ensure_index_projection_current`]），读路径的 FTS 查询
/// fail-closed 报 [`PortError::SchemaIncompatible`]——**绝不静默返回一个
/// 用旧词元匹配新查询得到的错误命中集**。
pub const INDEX_PROJECTION_VERSION: u32 = 1;

/// 当前 catalog schema 版本。每次结构变更 +1 并在 [`SqliteStore::migrate`] 追加步骤。
///
/// v8：新增 `source_session_resume_claims`（ADR-0009）——source-scoped Resume
/// Metadata 声明的持久化表。
///
/// v9：`source_scans` 增加可空 `provider_id TEXT` 列——`sync --discover` 用它
/// 按 provider diff 已存源路径，找出已被删除的源并合成空批 tombstone。
/// 旧行保持 NULL（直到该源被再次扫描时回填）；列是 additive，v8 库前向迁移。
///
/// v10：`message_vec` 语义向量边车表（#3）。与 `fts` 同级的 catalog 投影，
/// 可从 catalog 全量重建；记录 model_id/dimension，换模型后旧向量可识别可清理。
///
/// v11：新增 `session_fts` 与 `session_fts_ids`，作为可重建、隐私安全的
/// Session metadata 搜索投影（resolved Provider-native Session ID、pair-observed
/// Original Working Directory、首个有效 user request 的 title-like 字段）；
/// 旧目录无需数据迁移，rebuild 或后续 source 提交填充。Provider custom
/// title/summary 仍未进入 Canonical 契约，继续显式 deferred。
///
/// v12：新增 `tool_activities` 与 `tool_activity_membership`——typed tool-call
/// 观察投影，content-addressed activity_id 跨 source 去重，生命周期镜像
/// `message_placements`（complete-scan replace、incomplete-scan union、
/// claims tombstone）；随 rebuild 或后续 source 提交填充。
///
/// v13：新增 `session_titles`——会话标题显示投影（借鉴清单 #6）。派生链
/// custom-title（session payload `title`）> ai-title（`summary`）> 首条有效
/// user 消息（provider parse 层噪声过滤后第一条非空 user 文本，≤80 字符
/// char 边界截断）；无候选 → 无行。与 `session_fts` 同一重建批次，随
/// affected-session 提交与 rebuild 同事务维护；旧库迁到 v13 后表为空。
///
/// v14：`source_scans` 增加 `parser_version INTEGER NOT NULL DEFAULT 0`
/// 列——解析语义版本（见 [`PARSER_SEMANTIC_VERSION`]，借鉴 Recall 的
/// parser_version 增量同步）。sync 的 unchanged 判定把该列纳入比较：
/// 解析语义升级后版本落后的源即使字节未变也会 targeted backfill（重跑
/// parse + commit）。旧行 DEFAULT 0 保证迁移后第一次 sync 自动 backfill；
/// 新库在 v5 建表 DDL 已带本列（v9 provider_id 同一模式）。
///
/// v15：新增 `usage_events` 与 `usage_event_membership`——token 用量只读
/// 投影，五桶非负、`token_source` 只允许 observed/derived（覆盖标记：
/// 行存在 = provider 报过用量，真 0 与未知可区分）。
///
/// v16：新增 `session_repo_slugs`——repo identity 投影（借鉴 Recall 的
/// repo_identity 与 sessiongrep 的 find_repo_root）。每行存会话的
/// pair-observed working directory 经本机 git 检测（rev-parse
/// --show-toplevel + remote get-url origin，由注入的
/// [`RepoSlugResolver`] 执行）派生的 `host/owner/name` 三段 slug；
/// **绝不落绝对路径**。行存在 = 检测成功；无行 = 未知/未派生（诚实
/// 降级，不猜）。生命周期与 `session_fts` 同一重建批次（affected-session
/// commit + rebuild 同事务），session 退役时同事务删除。旧库迁到 v16
/// 后表为空，由 rebuild 或后续 affected source 提交回填。
///
/// v17：`store_metadata` 增加 `index_projection_version INTEGER NOT NULL
/// DEFAULT 0` 列——索引投影版本（见 [`INDEX_PROJECTION_VERSION`]）。它是
/// **库级** singleton 事实（"现存 FTS 词元流与派生投影是哪个变换写的"），
/// 与 `active_generation` 同表；刻意不做成 per-source 列——一次重投影重写
/// 整库每一行，per-source 记录在部分迁移状态下没有自洽答案。迁移期按可观测
/// 事实标记：投影为空（`fts` 与 `session_fts` 都无行）→ 直接标记当前版本，
/// 新库不会在第一次打开就被判失配；投影非空的旧库保持 DEFAULT 0（0 永不
/// 等于当前版本 ≥1）→ 写路径打开时自动从 catalog 重投影，读路径的 FTS
/// 查询 fail-closed 报 `schema_incompatible`，绝不静默返回错误命中集。
///
/// v18：持久化 installation namespace/location/source binding 与迁移回执；
/// durable intent 增加 relocation manifest，保留所有既有 canonical ID。
pub const SCHEMA_VERSION: i64 = 19;

impl CatalogStore for SqliteStore {
    fn begin_read_snapshot(&self) -> PortResult<Box<dyn ReadSnapshot + '_>> {
        SqliteStore::begin_read_snapshot(self)
    }

    fn get(&self, id: &StableId) -> PortResult<Option<Vec<u8>>> {
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare("SELECT payload FROM catalog WHERE id = ?1")
            .map_err(backend)?;
        let mut rows = stmt.query([id.as_str()]).map_err(backend)?;
        match rows.next().map_err(backend)? {
            Some(row) => Ok(Some(row.get::<_, Vec<u8>>(0).map_err(backend)?)),
            None => Ok(None),
        }
    }

    fn get_many(&self, ids: &[StableId]) -> PortResult<Vec<(StableId, Option<Vec<u8>>)>> {
        let conn = self.conn.borrow();
        let wires: Vec<&str> = ids.iter().map(|id| id.as_str()).collect();
        // 批量读取，分块在 SQLite 变量上限之下（复用 integrity check 的 chunk 模式），
        // 绝不逐条查询（N+1）。
        let mut payloads: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for chunk in chunk_ids(&wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT id, payload FROM catalog WHERE id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (id, payload) = row.map_err(backend)?;
                payloads.insert(id, payload);
            }
        }
        // 保序：结果与 `ids` 同序；目录中不存在的 id → None。
        Ok(ids
            .iter()
            .map(|id| {
                let payload = payloads.get(id.as_str()).cloned();
                (id.clone(), payload)
            })
            .collect())
    }

    fn put(&self, id: &StableId, payload: &[u8]) -> PortResult<()> {
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(backend)?;
        Self::reject_source_owned_writes(&tx, &[], std::slice::from_ref(id))?;
        Self::invalidate_changed_vectors_in_tx(&tx, [(id, payload)], &[])?;
        tx.execute(
            "INSERT INTO catalog(id, payload) VALUES(?1, ?2)
             ON CONFLICT(id) DO UPDATE SET payload = excluded.payload",
            rusqlite::params![id.as_str(), payload],
        )
        .map_err(backend)?;
        // catalog 是内容的权威事实源：put 更新 payload 后必须同步维护 fts/边车，
        // 否则消息内容更新后旧文本仍可搜（与 rebuild_index 用同一 searchable_text
        // 投影函数，避免再次分叉）。
        Self::upsert_fts_row_in_tx(&tx, id, &searchable_text(payload))?;
        // 单条写入同样是 catalog 变更：推进 generation，使此前签发的 search/list
        // 游标（绑定旧 generation）在此变更后失效，维持"游标绑定 generation"的
        // CAS 契约（与 commit_batch/rebuild_index 的 generation 语义一致）。
        Self::advance_generation_in_tx(&tx)?;
        tx.commit().map_err(backend)?;
        Ok(())
    }

    fn list(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
        Self::list_filtered(&self.conn, None, limit)
    }

    fn list_sessions(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
        Self::list_filtered(&self.conn, Some(IdKind::Session), limit)
    }

    fn session_titles(&self, session_ids: &[StableId]) -> PortResult<Vec<Option<String>>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.borrow();
        let wires: Vec<&str> = session_ids.iter().map(|id| id.as_str()).collect();
        // 批量读取，分块在 SQLite 变量上限之下；绝不逐条查询（N+1）。
        let mut titles: BTreeMap<String, String> = BTreeMap::new();
        for chunk in chunk_ids(&wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT session_wire, title FROM session_titles
                     WHERE session_wire IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (wire, title) = row.map_err(backend)?;
                titles.insert(wire, title);
            }
        }
        // 保序：与 `session_ids` 同序；无投影行的 id → None。
        Ok(session_ids
            .iter()
            .map(|id| titles.get(id.as_str()).cloned())
            .collect())
    }

    fn session_repo_slugs(&self, session_ids: &[StableId]) -> PortResult<Vec<Option<String>>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.borrow();
        let wires: Vec<&str> = session_ids.iter().map(|id| id.as_str()).collect();
        // 批量读取 repo 投影（schema v16），分块在 SQLite 变量上限之下；
        // 与 `session_titles` 同一模式，绝不逐条查询（N+1）。
        let mut slugs: BTreeMap<String, String> = BTreeMap::new();
        for chunk in chunk_ids(&wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT session_wire, repo_slug FROM session_repo_slugs
                     WHERE session_wire IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (wire, slug) = row.map_err(backend)?;
                slugs.insert(wire, slug);
            }
        }
        // 保序：与 `session_ids` 同序；无 repo 身份的会话 → None（未知 ≠ 匹配）。
        Ok(session_ids
            .iter()
            .map(|id| slugs.get(id.as_str()).cloned())
            .collect())
    }

    fn count(&self) -> PortResult<u64> {
        let conn = self.conn.borrow();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM catalog", [], |row| row.get(0))
            .map_err(backend)?;
        u64::try_from(count).map_err(backend)
    }

    fn usage_totals(&self) -> PortResult<Option<UsageTotals>> {
        // 委托给 inherent 实现（同一方法体；覆盖默认 None）。
        SqliteStore::usage_totals(self)
    }

    fn repo_totals(&self) -> PortResult<Vec<RepoTotals>> {
        // 委托给 inherent 实现（同一方法体；覆盖默认空列表）。
        SqliteStore::repo_totals(self)
    }

    fn active_generation(&self) -> PortResult<u64> {
        SqliteStore::active_generation(self)
    }
}

impl ContextGraphStore for SqliteStore {
    fn load_session_graph(&self, session_id: &StableId) -> PortResult<SessionContextGraph> {
        if session_id.kind() != IdKind::Session {
            return Err(PortError::NotFound("session context not found".into()));
        }
        let conn = self.conn.borrow();
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM catalog WHERE id = ?1)",
                [session_id.as_str()],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if !exists {
            return Err(PortError::NotFound("session context not found".into()));
        }

        let sources = Self::relation_sources_for_session(&conn, session_id.as_str())?;
        Self::require_relation_complete_sources(&conn, &sources, true, "session")?;
        let stored_session_id = Self::stable_id_from_store(&conn, session_id.as_str())?;

        let raw_placements = {
            let mut stmt = conn
                .prepare(
                    "SELECT placement_id, document_id, message_id, source_ordinal,
                            is_sidechain, byte_start, byte_end
                     FROM message_placements
                     WHERE session_id = ?1
                     ORDER BY document_id, source_ordinal, placement_id",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([session_id.as_str()], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                    ))
                })
                .map_err(backend)?;
            let mut placements = Vec::new();
            for row in rows {
                placements.push(row.map_err(backend)?);
            }
            placements
        };

        let mut message_wires = BTreeSet::new();
        let mut document_wires = BTreeSet::new();

        // 先收集全部 wires，再批量加载 payload/identity（避免 N+1）。
        for (_, document_id, message_id, _, _, _, _) in &raw_placements {
            message_wires.insert(message_id.clone());
            document_wires.insert(document_id.clone());
        }
        {
            let mut stmt = conn
                .prepare(
                    "SELECT DISTINCT document_id
                     FROM source_membership
                     WHERE message_id = ?1 AND document_id IS NOT NULL",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([session_id.as_str()], |row| row.get::<_, String>(0))
                .map_err(backend)?;
            for row in rows {
                document_wires.insert(row.map_err(backend)?);
            }
        }

        // 边查询提前到批量加载之前:父消息 wire 必须并入 fts_ids 身份批集,否则
        // 每条边一次身份查询(N+1),且孤儿父(在本会话无出现的父)会退化为
        // Unstable 身份(降级)。
        let raw_edges = {
            let mut stmt = conn
                .prepare(
                    "SELECT edges.child_placement_id, edges.parent_message_id,
                            edges.parent_native_id, edges.relation
                     FROM message_edges AS edges
                     JOIN message_placements AS placements
                       ON placements.placement_id = edges.child_placement_id
                     WHERE placements.session_id = ?1
                     ORDER BY edges.child_placement_id",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([session_id.as_str()], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(backend)?;
            let mut edges = Vec::new();
            for row in rows {
                edges.push(row.map_err(backend)?);
            }
            edges
        };
        let mut parent_message_wires = BTreeSet::new();
        for (_, parent_message_id, _, _) in &raw_edges {
            parent_message_wires.insert(parent_message_id.clone());
        }

        // Batch-load message payloads and fts_ids identity for all wires at
        // once (was N+1 per message: one payload read + two identity reads).
        // Chunked under the SQLite variable limit for very large sessions.
        // 身份批集额外并入边的父消息 wire(含孤儿父),保真其 fts_ids 身份等级。
        let mut payload_by_id: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut id_json_by_wire: BTreeMap<String, String> = BTreeMap::new();
        let all_wires: Vec<&str> = message_wires
            .iter()
            .chain(document_wires.iter())
            .map(|wire| wire.as_str())
            .collect();
        let identity_wires: Vec<&str> = all_wires
            .iter()
            .copied()
            .chain(parent_message_wires.iter().map(|wire| wire.as_str()))
            .collect();
        for chunk in chunk_ids(&all_wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT id, payload FROM catalog WHERE id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (id, payload) = row.map_err(backend)?;
                payload_by_id.insert(id, payload);
            }
        }
        for chunk in chunk_ids(&identity_wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT wire_id, id_json FROM fts_ids WHERE wire_id IN ({placeholders})"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (wire_id, id_json) = row.map_err(backend)?;
                id_json_by_wire.insert(wire_id, id_json);
            }
        }

        let mut placements = Vec::with_capacity(raw_placements.len());
        for (placement_id, document_id, message_id, ordinal, sidechain, start, end) in
            raw_placements
        {
            let span = match (start, end) {
                (None, None) => None,
                (Some(start), Some(end)) => Some(EvidenceSpan {
                    start: u64::try_from(start).map_err(backend)?,
                    end: u64::try_from(end).map_err(backend)?,
                }),
                _ => {
                    return Err(PortError::Backend(
                        "stored placement has a partial span".into(),
                    ));
                }
            };
            placements.push(MessagePlacement {
                id: PlacementId::from_wire(&placement_id).ok_or_else(|| {
                    PortError::Backend("stored placement has an invalid id".into())
                })?,
                session_id: stored_session_id.clone(),
                source_document_id: Self::stable_id_from_wire(&document_id, &id_json_by_wire)?,
                message_id: Self::stable_id_from_wire(&message_id, &id_json_by_wire)?,
                source_ordinal: u32::try_from(ordinal).map_err(backend)?,
                is_sidechain: sidechain != 0,
                span,
            });
        }

        let mut messages = Vec::with_capacity(message_wires.len());
        for wire in &message_wires {
            let payload = payload_by_id.get(wire).cloned().ok_or_else(|| {
                PortError::Backend("session placement references a missing message".into())
            })?;
            let map = match serde_json::from_slice::<serde_json::Value>(&payload) {
                Ok(serde_json::Value::Object(map)) => map,
                _ => {
                    return Err(PortError::Backend(
                        "stored message payload is not an object".into(),
                    ));
                }
            };
            let role = map
                .get("role")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| PortError::Backend("stored message is missing role".into()))
                .and_then(stored_role)?;
            let text = map
                .get("text")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| PortError::Backend("stored message is missing text".into()))?
                .to_string();
            let timestamp = match map.get("timestamp") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(value)) => Some(value.clone()),
                Some(_) => {
                    return Err(PortError::Backend(
                        "stored message timestamp is not a string".into(),
                    ));
                }
            };
            messages.push(Message {
                id: Self::stable_id_from_wire(wire, &id_json_by_wire)?,
                role,
                text,
                timestamp,
            });
        }

        let mut source_documents = Vec::with_capacity(document_wires.len());
        for wire in &document_wires {
            let payload = payload_by_id.get(wire).cloned().ok_or_else(|| {
                PortError::Backend("session context references a missing document".into())
            })?;
            let map = match serde_json::from_slice::<serde_json::Value>(&payload) {
                Ok(serde_json::Value::Object(map)) => map,
                _ => {
                    return Err(PortError::Backend(
                        "stored document payload is not an object".into(),
                    ));
                }
            };
            source_documents.push(SourceDocument {
                id: Self::stable_id_from_wire(wire, &id_json_by_wire)?,
                provider_id: map
                    .get("provider")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        PortError::Backend("stored document is missing provider".into())
                    })?
                    .to_string(),
                variant_id: map
                    .get("variant")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| PortError::Backend("stored document is missing variant".into()))?
                    .to_string(),
                fingerprint: map
                    .get("fingerprint")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        PortError::Backend("stored document is missing fingerprint".into())
                    })?
                    .to_string(),
                len: map
                    .get("len")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        PortError::Backend("stored document is missing byte length".into())
                    })?,
            });
        }

        let mut edges = Vec::with_capacity(raw_edges.len());
        for (child_placement_id, parent_message_id, parent_native_id, relation) in raw_edges {
            edges.push(MessageEdge {
                child_placement_id: PlacementId::from_wire(&child_placement_id).ok_or_else(
                    || PortError::Backend("stored edge has an invalid child placement id".into()),
                )?,
                parent_message_id: Self::stable_id_from_wire(&parent_message_id, &id_json_by_wire)?,
                parent_native_id,
                relation: stored_relation(&relation)?,
            });
        }

        let graph = SessionContextGraph {
            session_id: stored_session_id,
            messages,
            source_documents,
            placements,
            edges,
        };
        graph
            .validate()
            .map_err(|error| PortError::Backend(error.to_string()))?;
        Ok(graph)
    }

    fn message_contexts(&self, message_id: &StableId) -> PortResult<Vec<MessageContextCandidate>> {
        if message_id.kind() != IdKind::Message {
            return Err(PortError::NotFound("message context not found".into()));
        }
        let conn = self.conn.borrow();
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM catalog WHERE id = ?1)",
                [message_id.as_str()],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if !exists {
            return Err(PortError::NotFound("message context not found".into()));
        }

        let message_sources = Self::relation_sources_for_message(&conn, message_id.as_str())?;
        Self::require_relation_complete_sources(&conn, &message_sources, false, "message")?;
        let mut grouped = BTreeMap::<String, Vec<PlacementId>>::new();
        {
            let mut stmt = conn
                .prepare(
                    "SELECT session_id, placement_id
                     FROM message_placements
                     WHERE message_id = ?1
                     ORDER BY session_id, placement_id",
                )
                .map_err(backend)?;
            let rows = stmt
                .query_map([message_id.as_str()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (session_id, placement_id) = row.map_err(backend)?;
                grouped.entry(session_id).or_default().push(
                    PlacementId::from_wire(&placement_id).ok_or_else(|| {
                        PortError::Backend("stored placement has an invalid id".into())
                    })?,
                );
            }
        }

        let mut candidates = Vec::with_capacity(grouped.len());
        for (session_wire, mut placement_ids) in grouped {
            let session_sources = Self::relation_sources_for_session(&conn, &session_wire)?;
            Self::require_relation_complete_sources(&conn, &session_sources, true, "session")?;
            placement_ids.sort();
            candidates.push(MessageContextCandidate {
                session_id: Self::stable_id_from_store(&conn, &session_wire)?,
                placement_ids,
            });
        }
        Ok(candidates)
    }

    fn session_of(
        &self,
        message_ids: &[StableId],
    ) -> PortResult<Vec<(StableId, Option<StableId>)>> {
        let conn = self.conn.borrow();
        let wires: Vec<&str> = message_ids.iter().map(|id| id.as_str()).collect();
        // 批量解析归属会话：每块一条 `MIN(session_id) GROUP BY message_id`，
        // 分块在 SQLite 变量上限之下（与 get_many 同一模式，无 N+1）。
        // MIN 取 wire id 字典序最小的会话（确定性，跨页稳定）。
        let mut owners: BTreeMap<String, String> = BTreeMap::new();
        for chunk in chunk_ids(&wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT message_id, MIN(session_id)
                     FROM message_placements
                     WHERE message_id IN ({placeholders})
                     GROUP BY message_id"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (message_id, session_id) = row.map_err(backend)?;
                owners.insert(message_id, session_id);
            }
        }
        // 保序：与 `message_ids` 同序；无任何 placement 的消息 → None。
        message_ids
            .iter()
            .map(|id| {
                let session = match owners.get(id.as_str()) {
                    Some(wire) => Some(StableId::from_wire(wire).ok_or_else(|| {
                        PortError::Backend("stored placement has an invalid session id".into())
                    })?),
                    None => None,
                };
                Ok((id.clone(), session))
            })
            .collect()
    }

    fn source_placements_of(
        &self,
        message_ids: &[StableId],
    ) -> PortResult<Vec<(StableId, Option<SourcePlacement>)>> {
        let conn = self.conn.borrow();
        let wires: Vec<&str> = message_ids.iter().map(|id| id.as_str()).collect();
        // 批量读取全部相关 placement，再在 Rust 侧取每个 message 的确定性单个
        // placement（source_document_id 字典序最小，其次 source_ordinal）——
        // 与 session_of 的 MIN 约定一致，跨调用稳定。无 N+1。
        let mut best: BTreeMap<String, (String, Option<i64>, Option<i64>)> = BTreeMap::new();
        for chunk in chunk_ids(&wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT message_id, document_id, byte_start, byte_end
                     FROM message_placements
                     WHERE message_id IN ({placeholders})
                     ORDER BY document_id, source_ordinal"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                })
                .map_err(backend)?;
            for row in rows {
                let (message_id, document_id, byte_start, byte_end) = row.map_err(backend)?;
                // 已按 document_id, source_ordinal 排序 → 每 message 首条即确定性最小。
                best.entry(message_id)
                    .or_insert_with(|| (document_id, byte_start, byte_end));
            }
        }
        message_ids
            .iter()
            .map(|id| {
                let placement = match best.get(id.as_str()) {
                    Some((document_id, byte_start, byte_end)) => Some(SourcePlacement {
                        source_document_id: StableId::from_wire(document_id).ok_or_else(|| {
                            PortError::Backend("stored placement has an invalid document id".into())
                        })?,
                        byte_start: byte_start.map(|v| v as u64),
                        byte_end: byte_end.map(|v| v as u64),
                    }),
                    None => None,
                };
                Ok((id.clone(), placement))
            })
            .collect()
    }

    fn tool_activities_for_messages(
        &self,
        message_ids: &[StableId],
    ) -> PortResult<Vec<serde_json::Value>> {
        // Delegate to the inherent method so CLI/MCP and the trait path share one
        // SQL implementation.
        SqliteStore::tool_activities_for_messages(self, message_ids)
    }

    fn context_stats(&self) -> PortResult<ContextStats> {
        let conn = self.conn.borrow();
        let placements: i64 = conn
            .query_row("SELECT COUNT(*) FROM message_placements", [], |row| {
                row.get(0)
            })
            .map_err(backend)?;
        let source_placement_claims: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_placement_membership",
                [],
                |row| row.get(0),
            )
            .map_err(backend)?;
        Ok(ContextStats {
            placements: u64::try_from(placements).map_err(backend)?,
            source_placement_claims: u64::try_from(source_placement_claims).map_err(backend)?,
        })
    }
}

impl SqliteStore {
    /// 把 Session 元数据命中（`session_fts MATCH`）并进既有消息命中列表。
    ///
    /// 查询侧先做与消息路径同一的 CJK n-gram 前置变换 + 字面量化；候选按
    /// `bm25(session_fts)` 排序后取前 `limit` 条。每条命中以"首个非系统
    /// 消息"作代表（保既有 SearchHit 形状，不臆造 id）；无非系统消息的
    /// Session（metadata-only）直接以 canonical Session 身份返回。已由匹配
    /// 非系统消息代表过的 Session 被排除（R3 去重），系统/developer 消息
    /// 单独命中不得压制 metadata-only Session。
    fn append_session_metadata_hits(
        conn: &Connection,
        safe_query: &str,
        filters: &agent_session_grep_ports::SearchFilters,
        message_wires: &[String],
        limit: usize,
        hits: &mut Vec<SearchHit>,
    ) -> PortResult<()> {
        if limit == 0 {
            return Ok(());
        }
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(safe_query.to_string())];
        let mut representative = String::from(
            "SELECT representative.message_id FROM message_placements representative
             WHERE representative.session_id = sfi.session_wire",
        );
        append_placement_predicates(
            &mut representative,
            &mut params,
            "representative",
            filters,
            &SearchFacets::default(),
        );
        // A metadata hit must project a Message satisfying the same predicates,
        // not a different (possibly out-of-window) Message from that Session.
        append_message_predicates(
            &mut representative,
            &mut params,
            "representative.message_id",
            filters,
            &SearchFacets::default(),
            false,
        );
        representative.push_str(
            " ORDER BY representative.document_id,
            representative.source_ordinal, representative.placement_id LIMIT 1",
        );
        let mut sql = format!(
            "SELECT COALESCE(
                 (SELECT fi.id_json FROM fts_ids fi WHERE fi.wire_id = ({representative})),
                 (SELECT fi.id_json FROM fts_ids fi WHERE fi.wire_id = sfi.session_wire)
             ), ({representative}), sfi.session_wire, bm25(session_fts)
             FROM session_fts JOIN session_fts_ids sfi ON sfi.session_wire = session_fts.session_wire
             WHERE session_fts MATCH ?1"
        );
        if !message_wires.is_empty() {
            params.push(Box::new(
                serde_json::to_string(message_wires).map_err(backend)?,
            ));
            sql.push_str(&format!(
                " AND NOT EXISTS (SELECT 1 FROM message_placements ep
                 JOIN catalog em ON em.id = ep.message_id
                 WHERE ep.session_id = sfi.session_wire
                 AND ep.message_id IN (SELECT value FROM json_each(?{}))
                 AND COALESCE(CASE WHEN json_valid(em.payload)
                     THEN json_extract(em.payload, '$.role') END, '') NOT IN ('system', 'developer'))",
                params.len()
            ));
        }
        if !filters.providers.is_empty() || filters.since.is_some() || filters.until.is_some() {
            sql.push_str(&format!(" AND ({representative}) IS NOT NULL"));
        }
        if let Some(repo) = &filters.repo {
            params.push(Box::new(repo.clone()));
            sql.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM session_repo_slugs rs
                WHERE rs.session_wire = sfi.session_wire AND rs.repo_slug = ?{})",
                params.len()
            ));
        }
        params.push(Box::new(i64::try_from(limit).unwrap_or(i64::MAX)));
        sql.push_str(&format!(
            " ORDER BY bm25(session_fts), sfi.session_wire LIMIT ?{}",
            params.len()
        ));

        let mut stmt = conn.prepare(&sql).map_err(backend)?;
        let params_ref: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(std::convert::AsRef::as_ref).collect();
        let rows = stmt
            .query_map(&*params_ref, |row| {
                let id_json: Option<String> = row.get(0)?;
                let message_wire: Option<String> = row.get(1)?;
                let session_wire: String = row.get(2)?;
                let bm25: f64 = row.get(3)?;
                Ok((id_json, message_wire, session_wire, bm25))
            })
            .map_err(backend)?;
        for row in rows {
            let (id_json, message_wire, session_wire, bm25) = row.map_err(backend)?;
            let id = match id_json {
                Some(json) => serde_json::from_str(&json).map_err(backend)?,
                None => message_wire
                    .as_deref()
                    .or(Some(session_wire.as_str()))
                    .and_then(StableId::from_wire)
                    .ok_or_else(|| {
                        PortError::Backend(
                            "session metadata representative has an invalid id".into(),
                        )
                    })?,
            };
            hits.push(SearchHit {
                id,
                score: -bm25 as f32,
                session_id: Some(session_wire),
                text: None,
                why_matched: Vec::new(),
                suggested_next_commands: Vec::new(),
                occurrences: 1,
                resume_available: false,
            });
        }
        Ok(())
    }
}

impl SearchIndex for SqliteStore {
    fn index(&self, id: &StableId, text: &str) -> PortResult<()> {
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(backend)?;
        Self::upsert_fts_row_in_tx(&tx, id, text)?;
        // 索引写入也是内容变更：推进 generation，保持游标 CAS 契约。
        Self::advance_generation_in_tx(&tx)?;
        tx.commit().map_err(backend)?;
        Ok(())
    }

    fn query_filtered(&self, query: SearchQuery<'_>, limit: usize) -> PortResult<Vec<SearchHit>> {
        self.query_with_policy(query, limit, &SearchFacets::default(), true)
    }

    fn query_faceted(
        &self,
        query: SearchQuery<'_>,
        limit: usize,
        facets: &SearchFacets,
    ) -> PortResult<Vec<SearchHit>> {
        self.query_with_policy(query, limit, facets, true)
    }

    fn query_with_policy(
        &self,
        query: SearchQuery<'_>,
        limit: usize,
        facets: &SearchFacets,
        include_system: bool,
    ) -> PortResult<Vec<SearchHit>> {
        let conn = self.conn.borrow();
        Self::assert_index_projection_current(&conn)?;
        let safe_query = safe_fts_query(&fts_tokens_cjk(query.text));
        if safe_query.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(safe_query.clone())];
        let owner = matching_session_sql(
            &mut params,
            "(SELECT wire_id FROM fts_ids WHERE id_json = f.id)",
            query.filters,
            facets,
        );
        let mut sql = format!("SELECT f.id, bm25(fts), ({owner}) FROM fts AS f WHERE fts MATCH ?1");
        append_message_predicates(
            &mut sql,
            &mut params,
            "(SELECT wire_id FROM fts_ids WHERE id_json = f.id)",
            query.filters,
            facets,
            include_system,
        );
        sql.push_str(
            " ORDER BY bm25(fts),
             (SELECT wire_id FROM fts_ids WHERE id_json = f.id) LIMIT ?",
        );
        params.push(Box::new(i64::try_from(limit).unwrap_or(i64::MAX)));
        let mut stmt = conn.prepare(&sql).map_err(backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(backend)?;
        let mut hits = collect_hits(rows)?;
        for (rank, hit) in hits.iter_mut().enumerate() {
            hit.score = 1.0 / (60.0 + (rank + 1) as f32);
        }
        if facets.is_default() {
            let message_wires: Vec<String> =
                hits.iter().map(|hit| hit.id.as_str().to_owned()).collect();
            let mut metadata_hits = Vec::new();
            Self::append_session_metadata_hits(
                &conn,
                &safe_query,
                query.filters,
                &message_wires,
                limit,
                &mut metadata_hits,
            )?;
            // Scores from separate FTS corpora are incomparable. Fuse their
            // ordinal ranks with k=60, preserving stable wire-id tie breaks.
            let mut merged: BTreeMap<String, SearchHit> = hits
                .into_iter()
                .map(|hit| (hit.id.as_str().to_owned(), hit))
                .collect();
            for (rank, mut hit) in metadata_hits.into_iter().enumerate() {
                hit.score = 1.0 / (60.0 + (rank + 1) as f32);
                merged
                    .entry(hit.id.as_str().to_owned())
                    .and_modify(|existing| existing.score += hit.score)
                    .or_insert(hit);
            }
            hits = merged.into_values().collect();
            hits.sort_by(|a, b| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| a.id.as_str().cmp(b.id.as_str()))
            });
            hits.truncate(limit);
        }
        Ok(hits)
    }
}

/// Select the canonical owner from placements satisfying all occurrence-local
/// predicates together; never combine a repo from one placement with another's
/// provider or sidechain flag. Wire ordering keeps shared-message ownership stable.
fn matching_session_sql(
    params: &mut Vec<Box<dyn rusqlite::ToSql>>,
    message: &str,
    filters: &agent_session_grep_ports::SearchFilters,
    facets: &SearchFacets,
) -> String {
    let mut sql = format!(
        "SELECT MIN(owner.session_id) FROM message_placements owner WHERE owner.message_id = {message}"
    );
    append_placement_predicates(&mut sql, params, "owner", filters, facets);
    sql
}

fn append_placement_predicates(
    sql: &mut String,
    params: &mut Vec<Box<dyn rusqlite::ToSql>>,
    placement: &str,
    filters: &agent_session_grep_ports::SearchFilters,
    facets: &SearchFacets,
) {
    if !filters.providers.is_empty() {
        sql.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM catalog doc WHERE doc.id = {placement}.document_id
             AND CASE WHEN json_valid(doc.payload) THEN json_extract(doc.payload, '$.provider') END IN ("
        ));
        for (index, provider) in filters.providers.iter().enumerate() {
            if index != 0 {
                sql.push(',');
            }
            params.push(Box::new(provider.as_str()));
            sql.push_str(&format!("?{}", params.len()));
        }
        sql.push_str("))");
    }
    if let Some(repo) = &filters.repo {
        params.push(Box::new(repo.clone()));
        sql.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM session_repo_slugs rs
             WHERE rs.session_wire = {placement}.session_id AND rs.repo_slug = ?{})",
            params.len()
        ));
    }
    if facets.sidechain != SidechainFacet::Include {
        let sidechain = i32::from(facets.sidechain == SidechainFacet::SubagentOnly);
        sql.push_str(&format!(" AND {placement}.is_sidechain = {sidechain}"));
    }
}

/// Shared lexical/semantic metadata predicates. The message expression is
/// generated only by this adapter; all user values remain bound parameters.
fn append_message_predicates(
    sql: &mut String,
    params: &mut Vec<Box<dyn rusqlite::ToSql>>,
    message: &str,
    filters: &agent_session_grep_ports::SearchFilters,
    facets: &SearchFacets,
    include_system: bool,
) {
    if !filters.providers.is_empty()
        || filters.repo.is_some()
        || facets.sidechain == SidechainFacet::SubagentOnly
    {
        let owner = matching_session_sql(params, message, filters, facets);
        sql.push_str(&format!(" AND ({owner}) IS NOT NULL"));
    }
    if filters.since.is_some() || filters.until.is_some() || !include_system {
        sql.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM catalog msg WHERE msg.id = {message}"
        ));
        for (instant, operator) in [(filters.since, ">="), (filters.until, "<")] {
            if let Some(instant) = instant {
                params.push(Box::new(instant.sort_key().to_vec()));
                sql.push_str(&format!(
                    " AND asg_instant_sort_key(CASE WHEN json_valid(msg.payload)
                      THEN json_extract(msg.payload, '$.timestamp') END) {operator} ?{}",
                    params.len()
                ));
            }
        }
        if !include_system {
            sql.push_str(
                " AND COALESCE(CASE WHEN json_valid(msg.payload)
                THEN json_extract(msg.payload, '$.role') END, '') NOT IN ('system', 'developer')",
            );
        }
        sql.push(')');
    }
    // Preserve MainOnly's existing message-wide exclusion of sidechain history.
    if facets.sidechain == SidechainFacet::MainOnly {
        sql.push_str(&format!(
            " AND NOT EXISTS (SELECT 1 FROM message_placements mp
            WHERE mp.message_id = {message} AND mp.is_sidechain = 1)"
        ));
    }
    for (value, column) in [(&facets.tool_kind, "kind"), (&facets.tool_name, "name")] {
        if let Some(value) = value {
            params.push(Box::new(value.clone()));
            sql.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM tool_activities ta
                WHERE ta.message_id = {message} AND ta.{column} = ?{})",
                params.len()
            ));
        }
    }
}

/// 语义向量边车（#3，schema v10）。
///
/// 向量以 little-endian f32 blob 存储；余弦相似度在 Rust 侧计算——SQLite 无
/// 向量扩展依赖（sqlite-vec 需额外二进制），语料规模下全表扫描 + Rust 点积
/// 已足够，且不引入新供应链。查询按 `model_id` 过滤：换模型后旧维度向量不会
/// 与新向量混算出垃圾相似度。
///
/// `is_ready` 表示"这张表里有当前模型的向量"，而不是"表存在"——空表意味着
/// 语义检索不可用，Application 必须显式降级到 lexical_fallback。
impl SemanticIndex for SqliteStore {
    fn index_embedding(&self, id: &StableId, embedding: &[f32]) -> PortResult<()> {
        if embedding.is_empty() || embedding.iter().any(|value| !value.is_finite()) {
            return Err(PortError::Backend(
                "embedding must contain finite values and not be empty".into(),
            ));
        }
        let model_id = self.semantic_model_id.borrow().clone().ok_or_else(|| {
            PortError::Backend("semantic model id not set; call set_semantic_model first".into())
        })?;
        let blob = f32_slice_to_bytes(embedding);
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(backend)?;
        tx.execute(
            "INSERT INTO message_vec(wire_id, model_id, dimension, embedding)
             VALUES(?1, ?2, ?3, ?4)
             ON CONFLICT(wire_id) DO UPDATE SET
                 model_id = excluded.model_id,
                 dimension = excluded.dimension,
                 embedding = excluded.embedding",
            rusqlite::params![id.as_str(), model_id, embedding.len() as i64, blob],
        )
        .map_err(backend)?;
        Self::advance_generation_in_tx(&tx)?;
        tx.commit().map_err(backend)?;
        Ok(())
    }

    fn query_semantic_filtered(
        &self,
        query_embedding: &[f32],
        limit: usize,
        filters: &agent_session_grep_ports::SearchFilters,
        facets: &SearchFacets,
        include_system: bool,
    ) -> PortResult<Vec<SearchHit>> {
        if query_embedding.iter().any(|value| !value.is_finite()) {
            return Err(PortError::Backend(
                "query embedding contains non-finite values".into(),
            ));
        }
        if query_embedding.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let Some(model_id) = self.semantic_model_id.borrow().clone() else {
            return Ok(Vec::new());
        };
        let conn = self.conn.borrow();
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![
            Box::new(model_id),
            Box::new(i64::try_from(query_embedding.len()).map_err(backend)?),
        ];
        let owner = matching_session_sql(&mut params, "mv.wire_id", filters, facets);
        let mut sql = format!(
            "SELECT mv.wire_id, fi.id_json, mv.embedding, ({owner})
             FROM message_vec mv
             JOIN catalog live_message ON live_message.id = mv.wire_id
             LEFT JOIN fts_ids fi ON fi.wire_id = mv.wire_id
             WHERE mv.model_id = ?1 AND mv.dimension = ?2"
        );
        append_message_predicates(
            &mut sql,
            &mut params,
            "mv.wire_id",
            filters,
            facets,
            include_system,
        );
        let mut stmt = conn.prepare(&sql).map_err(backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(backend)?;
        // Retain at most k identities, not every scored row in the corpus.
        let mut top = std::collections::BinaryHeap::new();
        for row in rows {
            let (wire, id_json, blob, session_id) = row.map_err(backend)?;
            if blob.len() != query_embedding.len() * 4 {
                return Err(PortError::Backend(
                    "stored embedding has an invalid dimension".into(),
                ));
            }
            let vector = bytes_to_f32_vec(&blob);
            if vector.iter().any(|value| !value.is_finite()) {
                return Err(PortError::Backend(
                    "stored embedding contains non-finite values".into(),
                ));
            }
            let score = cosine_similarity(query_embedding, &vector);
            if !score.is_finite() {
                return Err(PortError::Backend("semantic score is non-finite".into()));
            }
            let id = match id_json {
                Some(json) => serde_json::from_str(&json).map_err(backend)?,
                None => StableId::from_wire(&wire).ok_or_else(|| {
                    PortError::Backend("stored embedding has an invalid identity".into())
                })?,
            };
            top.push(SemanticCandidate {
                score,
                id,
                session_id,
            });
            if top.len() > limit {
                top.pop();
            }
        }
        Ok(top
            .into_sorted_vec()
            .into_iter()
            .map(|candidate| SearchHit {
                id: candidate.id,
                score: candidate.score,
                session_id: candidate.session_id,
                text: None,
                why_matched: Vec::new(),
                suggested_next_commands: Vec::new(),
                occurrences: 1,
                resume_available: false,
            })
            .collect())
    }

    fn is_ready(&self, query_dimension: usize) -> PortResult<bool> {
        let Some(model_id) = self.semantic_model_id.borrow().clone() else {
            return Ok(false);
        };
        let conn = self.conn.borrow();
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM message_vec mv
             JOIN catalog c ON c.id = mv.wire_id
             WHERE mv.model_id = ?1 AND mv.dimension = ?2 AND ?2 > 0)",
            rusqlite::params![model_id, i64::try_from(query_dimension).map_err(backend)?],
            |row| row.get::<_, bool>(0),
        )
        .map_err(backend)
    }

    fn semantic_model_id(&self) -> PortResult<Option<String>> {
        Ok(self.semantic_model_id.borrow().clone())
    }
}

/// Heap order puts the worst retained candidate at the root.
struct SemanticCandidate {
    score: f32,
    id: StableId,
    session_id: Option<String>,
}
impl PartialEq for SemanticCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for SemanticCandidate {}
impl PartialOrd for SemanticCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SemanticCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.id.as_str().cmp(other.id.as_str()))
    }
}

/// Serialize an f32 slice as little-endian bytes for BLOB storage.
fn f32_slice_to_bytes(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

/// Read a little-endian f32 BLOB back into a vector. A trailing partial float
/// is dropped rather than reconstructed from padding.
fn bytes_to_f32_vec(bytes: &[u8]) -> Vec<f32> {
    let (chunks, _) = bytes.as_chunks::<4>();
    chunks
        .iter()
        .map(|chunk| f32::from_le_bytes(*chunk))
        .collect()
}

/// Cosine similarity of two equal-length vectors. Zero-norm inputs score 0
/// (no direction to compare) rather than producing NaN.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;
    for (x, y) in a.iter().zip(b.iter()) {
        let (x, y) = (f64::from(*x), f64::from(*y));
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    (dot / (norm_a.sqrt() * norm_b.sqrt())) as f32
}

impl ResumeClaimsStore for SqliteStore {
    /// 批量解析 Session 的 Resume Metadata（ADR-0009）：分块 IN 一次查询
    /// 拿回全部命中 session 的声明行（无 N+1），仅在全部 source 声明完全
    /// 一致时解析；任一冲突都 fail closed。输出与 `session_ids` 同序。无声明/
    /// legacy → 全字段 None + `resume_available:false` + 明确的
    /// unavailable_reason——历史恒可检索，只是不可恢复。
    fn resume_of(&self, session_ids: &[StableId]) -> PortResult<Vec<SessionResumeMetadata>> {
        let conn = self.conn.borrow();
        let wires: Vec<&str> = session_ids.iter().map(|id| id.as_str()).collect();
        let mut claims: BTreeMap<String, Result<StoredResumeClaim, ()>> = BTreeMap::new();
        for chunk in chunk_ids(&wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT source_path, session_id, provider_id, provider_session_id,
                            provider_session_id_state, original_working_directory,
                            original_working_directory_state, pair_observed
                     FROM source_session_resume_claims
                     WHERE session_id IN ({placeholders})
                     ORDER BY source_path, session_id"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok(StoredResumeClaim {
                        session_id: row.get(1)?,
                        provider_id: row.get(2)?,
                        provider_session_id: row.get(3)?,
                        provider_session_id_state: row.get(4)?,
                        original_working_directory: row.get(5)?,
                        original_working_directory_state: row.get(6)?,
                        pair_observed: row.get(7)?,
                    })
                })
                .map_err(backend)?;
            for row in rows {
                let claim = row.map_err(backend)?;
                match claims.entry(claim.session_id.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(Ok(claim));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if entry.get().as_ref().is_ok_and(|current| current == &claim) {
                            continue;
                        }
                        let _ = entry.insert(Err(()));
                    }
                }
            }
        }
        Ok(session_ids
            .iter()
            .map(|id| match claims.get(id.as_str()) {
                Some(Ok(claim)) => resume_metadata_from_claim(id, claim),
                Some(Err(())) => SessionResumeMetadata {
                    session_id: id.clone(),
                    provider_id: None,
                    resume_available: false,
                    provider_session_id: None,
                    original_working_directory: None,
                    unavailable_reason: Some("conflicting resume metadata claims".into()),
                },
                None => SessionResumeMetadata {
                    session_id: id.clone(),
                    provider_id: None,
                    resume_available: false,
                    provider_session_id: None,
                    original_working_directory: None,
                    unavailable_reason: Some("no resume metadata claims".into()),
                },
            })
            .collect())
    }
}

impl SqliteStore {
    /// 批量取每个 canonical Session 的最近活动日期（`YYYY-MM-DD`）。
    ///
    /// 日期来源是会话内全部消息 payload 的 `timestamp`（provider-native ISO-8601）
    /// 的词法最大值的前 10 个字符。词法比较对 Claude Code / Codex 的
    /// `YYYY-MM-DDT...` 时间戳等价于时间排序；无 timestamp 的消息不参与。
    /// Human 表格展示专用，不进入 Robot/MCP 协议。无 N+1：按
    /// [`BATCH_IN_CHUNK`] 分块 IN 查询。
    pub fn latest_activity_ymd_for_sessions(
        &self,
        session_ids: &[StableId],
    ) -> PortResult<std::collections::HashMap<String, String>> {
        let conn = self.conn.borrow();
        let wires: Vec<&str> = session_ids.iter().map(|id| id.as_str()).collect();
        let mut out: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for chunk in chunk_ids(&wires) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT mp.session_id,
                            MAX(CASE WHEN json_valid(c.payload)
                                     THEN json_extract(c.payload, '$.timestamp') END) AS latest
                     FROM message_placements mp
                     JOIN catalog c ON c.id = mp.message_id
                     WHERE mp.session_id IN ({placeholders})
                       AND CASE WHEN json_valid(c.payload)
                                THEN json_extract(c.payload, '$.timestamp') END IS NOT NULL
                     GROUP BY mp.session_id"
                ))
                .map_err(backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(chunk.iter().copied()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })
                .map_err(backend)?;
            for row in rows {
                let (session_id, latest) = row.map_err(backend)?;
                if let Some(timestamp) = latest
                    && let Some(ymd) = timestamp.get(..10)
                {
                    out.insert(session_id, ymd.to_string());
                }
            }
        }
        Ok(out)
    }
}
fn collect_hits<F>(rows: rusqlite::MappedRows<'_, F>) -> PortResult<Vec<SearchHit>>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<(String, f64, Option<String>)>,
{
    let mut hits = Vec::new();
    for r in rows {
        let (id_json, bm25, session_id) = r.map_err(backend)?;
        let id: StableId = serde_json::from_str(&id_json).map_err(backend)?;
        // 取负使"分数越高越相关"，符合 SearchHit.score 的直觉（后端相对值）。
        hits.push(SearchHit {
            id,
            score: -bm25 as f32,
            // Preserve the matching placement's owner through application assembly.
            session_id,
            text: None,
            why_matched: Vec::new(),
            suggested_next_commands: Vec::new(),
            occurrences: 1,
            resume_available: false,
        });
    }
    Ok(hits)
}

/// 把用户搜索词转成 FTS5 安全查询：按空白分词，每个词用双引号包裹成短语查询，
/// 引号内的 FTS 保留字符（`: . - ( ) { } [ ] "`）按字面量匹配。
///
/// 尾随的 `*` 保留在引号外（`work*` → `"work"*`）：FTS5 只有引号外的 `*` 才是
/// 前缀操作符，包进引号（`"work*"`）会被分词器当字面分隔符丢弃，前缀查询静默
/// 退化为精确词匹配。
///
/// 这样 `search "codebuddy mcp.json"` 或搜索 Windows 路径片段不会触发
/// `fts5: syntax error near "."` 之类的底层错误，`hp-z8` 也不会被解析成
/// `hp NOT z8`（连字符被引号字面量化，不再是 NOT 操作符）。
/// 纯空白/纯标点输入返回空串。
///
/// 参考 hstry `sanitize_fts_query`（MIT，hstry/crates/hstry-core/src/db.rs:3177）
/// 的逐 token 引号化 + 引号外前缀 `*` 模式；本项目保留 CJK n-gram 前置变换
/// （ADR-0007）与全标点词跳过。
fn safe_fts_query(query: &str) -> String {
    let mut words: Vec<String> = Vec::new();
    for raw in query.split_whitespace() {
        let is_prefix = raw.ends_with('*');
        let stem = raw.trim_end_matches('*').replace('"', "\"\"");
        if stem.is_empty() {
            continue;
        }
        // 全标点无字母数字的词对 FTS 无意义，跳过以免生成空短语 `""`。
        if stem.chars().all(|c| !c.is_alphanumeric()) {
            continue;
        }
        if is_prefix {
            words.push(format!("\"{stem}\"*"));
        } else {
            words.push(format!("\"{stem}\""));
        }
    }
    words.join(" ")
}

#[cfg(test)]
mod filtered_query_tests {
    //! SQL-shape pin: the filtered path must keep predicates inside one
    //! prepared statement (pushdown before LIMIT), parameterize every filter
    //! value, and preserve the unfiltered SQL byte-for-byte for empty filters.
    use super::*;
    use crate::tests::{counted_statements, entity_entry, placement, sid, source_batch};
    use agent_session_grep_ports::{SearchFilters, SearchInstant, SearchProvider};

    fn instant(seconds: i64) -> SearchInstant {
        SearchInstant {
            unix_seconds: seconds,
            nanosecond: 0,
        }
    }

    fn search_filtered(store: &SqliteStore, text: &str, filters: &SearchFilters) -> Vec<String> {
        let hits = store
            .query_filtered(SearchQuery { text, filters }, 100)
            .unwrap();
        hits.into_iter()
            .map(|hit| hit.id.as_str().to_string())
            .collect()
    }

    #[test]
    fn asg_instant_sort_key_round_trips_and_orders() {
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.conn.borrow();
        let key: Vec<u8> = conn
            .query_row(
                "SELECT asg_instant_sort_key('2026-07-28T00:00:00Z')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(key, instant(1_785_196_800).sort_key().to_vec());
        // Offset forms normalize to the identical UTC key.
        let offset_key: Vec<u8> = conn
            .query_row(
                "SELECT asg_instant_sort_key('2026-07-28T02:00:00+02:00')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(offset_key, key);
        // NULL / non-string / unparseable inputs yield NULL (no match), never an error.
        for input in [
            "SELECT asg_instant_sort_key(NULL)",
            "SELECT asg_instant_sort_key(42)",
            "SELECT asg_instant_sort_key('not-a-timestamp')",
            "SELECT asg_instant_sort_key('2026-07-28T00:00:00')",
        ] {
            let value: Option<Vec<u8>> = conn.query_row(input, [], |row| row.get(0)).unwrap();
            assert!(value.is_none(), "{input}");
        }
        // Byte order is instant order across a mixed set.
        let ordered: Vec<String> = {
            let mut stmt = conn
                .prepare(
                    "SELECT value FROM (
                         SELECT '2026-07-28T00:00:01Z' AS value
                         UNION ALL SELECT '2026-07-27T23:59:59Z'
                         UNION ALL SELECT '2026-07-28T00:00:00.5Z'
                     )
                     ORDER BY asg_instant_sort_key(value)",
                )
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        assert_eq!(
            ordered,
            [
                "2026-07-27T23:59:59Z",
                "2026-07-28T00:00:00.5Z",
                "2026-07-28T00:00:01Z",
            ]
        );
    }

    /// Two providers × three timestamps sharing one FTS token.
    struct FilterFixture {
        store: SqliteStore,
        claude_early: StableId,
        claude_mid: StableId,
        codex_mid: StableId,
        codex_late: StableId,
        null_ts: StableId,
    }

    fn filter_fixture() -> FilterFixture {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"filter-session");
        let claude_doc = sid(IdKind::Document, b"filter-claude-doc");
        let codex_doc = sid(IdKind::Document, b"filter-codex-doc");
        let claude_early = sid(IdKind::Message, b"filter-claude-early");
        let claude_mid = sid(IdKind::Message, b"filter-claude-mid");
        let codex_mid = sid(IdKind::Message, b"filter-codex-mid");
        let codex_late = sid(IdKind::Message, b"filter-codex-late");
        let null_ts = sid(IdKind::Message, b"filter-null-ts");

        let message_entry = |id: &StableId, timestamp: Option<&str>| {
            let ts = match timestamp {
                Some(value) => serde_json::Value::String(value.to_string()),
                None => serde_json::Value::Null,
            };
            (
                id.clone(),
                serde_json::json!({
                    "role": "user",
                    "text": "shared-token body",
                    "timestamp": ts,
                    "parent": null,
                    "parent_native_id": null,
                    "is_sidechain": false,
                    "session": null,
                    "sessions": [],
                    "span": null,
                    "spans": [],
                })
                .to_string()
                .into_bytes(),
                "shared-token body".to_string(),
            )
        };
        let document_entry = |id: &StableId, provider: &str| {
            (
                id.clone(),
                serde_json::json!({
                    "provider": provider,
                    "variant": format!("{provider}/synthetic-v1"),
                    "page_ref": {
                        "source_fingerprint": null,
                        "document_ordinal": 0,
                        "first_line": 0,
                        "last_line": 0,
                        "byte_range": [0, 0],
                    },
                    "len": 128,
                })
                .to_string()
                .into_bytes(),
                String::new(),
            )
        };

        let early = "2026-07-01T00:00:00Z";
        let mid = "2026-07-28T00:00:00Z";
        let late = "2026-08-10T00:00:00Z";
        let entries = vec![
            entity_entry(&session),
            document_entry(&claude_doc, "claude-code"),
            document_entry(&codex_doc, "codex"),
            message_entry(&claude_early, Some(early)),
            message_entry(&claude_mid, Some(mid)),
            message_entry(&codex_mid, Some(mid)),
            message_entry(&codex_late, Some(late)),
            message_entry(&null_ts, None),
        ];
        let mut ordinal = 0_u32;
        let mut next = |document: &StableId, message: &StableId| {
            let placement = placement(&session, document, message, ordinal, false, Some((0, 4)));
            ordinal += 1;
            placement
        };
        let placements = vec![
            next(&claude_doc, &claude_early),
            next(&claude_doc, &claude_mid),
            next(&codex_doc, &codex_mid),
            next(&codex_doc, &codex_late),
            next(&codex_doc, &null_ts),
        ];
        let source = source_batch(
            "filter-fixture.jsonl",
            entries,
            placements,
            Vec::new(),
            true,
        );
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();
        FilterFixture {
            store,
            claude_early,
            claude_mid,
            codex_mid,
            codex_late,
            null_ts,
        }
    }

    #[test]
    fn semantic_and_hybrid_apply_provider_time_repo_and_facets_before_limit() {
        use agent_session_grep_application::{App, AppRequest, AppResponse, ResponseBudget};
        use agent_session_grep_ports::{NoResumeClaims, RetrievalMode};
        let fixture = filter_fixture();
        let store = &fixture.store;
        store.set_semantic_model("filter-model");
        let ids = [
            &fixture.claude_early,
            &fixture.claude_mid,
            &fixture.codex_late,
            &fixture.null_ts,
            &fixture.codex_mid,
        ];
        for (rank, id) in ids.iter().enumerate() {
            store.index_embedding(id, &[1.0, rank as f32]).unwrap();
        }
        let filters = SearchFilters {
            providers: vec![SearchProvider::Codex],
            since: Some(instant(1_785_196_800)),
            until: Some(instant(1_786_320_000)),
            repo: Some("example.test/team/project".into()),
        };
        {
            let conn = store.conn.borrow();
            conn.execute("INSERT INTO session_repo_slugs(session_wire,repo_slug) SELECT DISTINCT session_id,'example.test/team/project' FROM message_placements", []).unwrap();
            conn.execute(
                "UPDATE message_placements SET is_sidechain=1 WHERE message_id=?1",
                [fixture.codex_mid.as_str()],
            )
            .unwrap();
            conn.execute("INSERT INTO tool_activities(activity_id,message_id,kind,actor,name,target,status) VALUES('act_v1_filter',?1,'command','main','Shell',NULL,'success')", [fixture.codex_mid.as_str()]).unwrap();
        }
        let facets = SearchFacets {
            sidechain: SidechainFacet::SubagentOnly,
            tool_kind: Some("command".into()),
            tool_name: Some("Shell".into()),
        };
        let app = App::with_resume_semantic(store, store, NoResumeClaims, store);
        for mode in [
            RetrievalMode::Lexical,
            RetrievalMode::Semantic,
            RetrievalMode::Hybrid,
        ] {
            let AppResponse::Search { hits, .. } = app
                .handle(AppRequest::Search {
                    query: "shared-token".into(),
                    filters: filters.clone(),
                    facets: facets.clone(),
                    limit: 1,
                    cursor: None,
                    budget: ResponseBudget::default(),
                    include_system: false,
                    group_by_session: false,
                    mode,
                    query_embedding: Some(vec![1.0, 0.0]),
                })
                .unwrap()
            else {
                panic!("search")
            };
            assert_eq!(
                hits.iter().map(|hit| &hit.id).collect::<Vec<_>>(),
                vec![&fixture.codex_mid]
            );
        }
        for facets in [
            SearchFacets {
                tool_name: Some("NoSuchTool".into()),
                ..facets.clone()
            },
            SearchFacets {
                sidechain: SidechainFacet::MainOnly,
                ..facets.clone()
            },
        ] {
            assert!(
                store
                    .query_semantic_filtered(&[1.0, 0.0], 1, &filters, &facets, false)
                    .unwrap()
                    .is_empty()
            );
        }
        let filters = SearchFilters {
            repo: Some("example.test/other/project".into()),
            ..filters
        };
        assert!(
            store
                .query_semantic_filtered(&[1.0, 0.0], 1, &filters, &facets, false)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn filtered_shared_message_ownership_matches_repo_and_sidechain_in_all_modes() {
        use agent_session_grep_application::{App, AppRequest, AppResponse, ResponseBudget};
        use agent_session_grep_ports::{NoResumeClaims, RetrievalMode};
        for reverse in [false, true] {
            let store = SqliteStore::open_in_memory().unwrap();
            let message = sid(IdKind::Message, b"shared-ownership");
            let document = sid(IdKind::Document, b"shared-ownership-doc");
            let mut sessions = [
                sid(IdKind::Session, b"ownership-a"),
                sid(IdKind::Session, b"ownership-b"),
                sid(IdKind::Session, b"ownership-c"),
            ];
            sessions.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            let mut placements: Vec<_> = sessions
                .iter()
                .enumerate()
                .map(|(index, session)| {
                    placement(session, &document, &message, index as u32, index != 0, None)
                })
                .collect();
            if reverse {
                placements.reverse();
            }
            let mut entries = vec![
                (
                    message.clone(),
                    serde_json::json!({"role":"user", "text":"ownershipneedle"})
                        .to_string()
                        .into_bytes(),
                    "ownershipneedle".into(),
                ),
                (
                    document.clone(),
                    serde_json::json!({"provider":"codex"})
                        .to_string()
                        .into_bytes(),
                    String::new(),
                ),
            ];
            entries.extend(sessions.iter().map(entity_entry));
            store
                .commit_source_batches_if_changed(&[source_batch(
                    "ownership.jsonl",
                    entries,
                    placements,
                    Vec::new(),
                    true,
                )])
                .unwrap();
            for session in &sessions[1..] {
                store.conn.borrow().execute(
                    "INSERT INTO session_repo_slugs(session_wire, repo_slug) VALUES(?1, 'example.test/matched')",
                    [session.as_str()],
                ).unwrap();
            }
            store.set_semantic_model("ownership-model");
            store.index_embedding(&message, &[1.0, 0.0]).unwrap();
            let app = App::with_resume_semantic(&store, &store, NoResumeClaims, &store);
            for (repo, sidechain) in [
                (Some("example.test/matched"), SidechainFacet::Include),
                (None, SidechainFacet::SubagentOnly),
                (Some("example.test/matched"), SidechainFacet::SubagentOnly),
                (None, SidechainFacet::MainOnly),
                (Some("example.test/matched"), SidechainFacet::MainOnly),
            ] {
                for (mode, has_embedding) in [
                    (RetrievalMode::Lexical, true),
                    (RetrievalMode::Semantic, true),
                    (RetrievalMode::Hybrid, true),
                    (RetrievalMode::Semantic, false),
                    (RetrievalMode::Hybrid, false),
                ] {
                    for group_by_session in [false, true] {
                        let AppResponse::Search {
                            hits,
                            retrieval_mode,
                            fallback_warning,
                            ..
                        } = app
                            .handle(AppRequest::Search {
                                query: "ownershipneedle".into(),
                                filters: SearchFilters {
                                    repo: repo.map(str::to_owned),
                                    ..Default::default()
                                },
                                facets: SearchFacets {
                                    sidechain,
                                    ..Default::default()
                                },
                                limit: 1,
                                cursor: None,
                                budget: ResponseBudget::default(),
                                include_system: false,
                                group_by_session,
                                mode,
                                query_embedding: has_embedding.then(|| vec![1.0, 0.0]),
                            })
                            .unwrap()
                        else {
                            panic!("search")
                        };
                        assert_eq!(
                            retrieval_mode,
                            if has_embedding {
                                mode
                            } else {
                                RetrievalMode::LexicalFallback
                            }
                        );
                        assert_eq!(fallback_warning.is_some(), !has_embedding);
                        if sidechain == SidechainFacet::MainOnly {
                            assert!(
                                hits.is_empty(),
                                "mixed main/sidechain history must be excluded: {mode:?}, embedding={has_embedding}"
                            );
                            continue;
                        }
                        assert_eq!(hits.len(), 1);
                        assert_eq!(hits[0].id, message);
                        assert_eq!(
                            hits[0].session_id.as_deref(),
                            Some(sessions[1].as_str()),
                            "{mode:?}, {sidechain:?}, repo={repo:?}, reverse={reverse}, embedding={has_embedding}"
                        );
                    }
                }
            }
            // Legacy catalog messages without placements remain searchable under
            // MainOnly; absence of a placement is not evidence of sidechain use.
            let unplaced = sid(IdKind::Message, b"unplaced-ownership");
            store
                .put(
                    &unplaced,
                    &serde_json::to_vec(&serde_json::json!({
                        "role": "user", "text": "unplacedneedle"
                    }))
                    .unwrap(),
                )
                .unwrap();
            store.index_embedding(&unplaced, &[1.0, 0.0]).unwrap();
            let facets = SearchFacets {
                sidechain: SidechainFacet::MainOnly,
                ..Default::default()
            };
            let lexical = store
                .query_faceted(
                    SearchQuery {
                        text: "unplacedneedle",
                        filters: &SearchFilters::EMPTY,
                    },
                    10,
                    &facets,
                )
                .unwrap();
            let semantic = store
                .query_semantic_filtered(&[1.0, 0.0], 10, &SearchFilters::EMPTY, &facets, false)
                .unwrap();
            for hits in [lexical, semantic] {
                assert_eq!(hits.len(), 1);
                assert_eq!(hits[0].id, unplaced);
                assert_eq!(hits[0].session_id, None);
            }
        }
    }

    #[test]
    fn filtered_ownership_requires_repo_provider_and_sidechain_on_same_placement() {
        let fixture = filter_fixture();
        let store = &fixture.store;
        let other_session = sid(IdKind::Session, b"split-filter-session");
        let other_document = sid(IdKind::Document, b"split-filter-doc");
        store
            .put(
                &other_session,
                &serde_json::to_vec(&serde_json::json!({})).unwrap(),
            )
            .unwrap();
        store
            .put(
                &other_document,
                &serde_json::to_vec(&serde_json::json!({"provider":"codex"})).unwrap(),
            )
            .unwrap();
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO message_placements(placement_id,session_id,document_id,message_id,source_ordinal,is_sidechain)
                 VALUES(?1,?2,?3,?4,0,1)",
                rusqlite::params![
                    placement(&other_session, &other_document, &fixture.claude_mid, 0, true, None).id.as_str(),
                    other_session.as_str(), other_document.as_str(), fixture.claude_mid.as_str()
                ],
            ).unwrap();
            conn.execute("INSERT INTO session_repo_slugs(session_wire,repo_slug) SELECT DISTINCT session_id,'example.test/main' FROM message_placements WHERE session_id != ?1", [other_session.as_str()]).unwrap();
        }
        store.set_semantic_model("split-model");
        store
            .index_embedding(&fixture.claude_mid, &[1.0, 0.0])
            .unwrap();
        let filters = SearchFilters {
            repo: Some("example.test/main".into()),
            ..Default::default()
        };
        for (filters, facets) in [
            (
                filters.clone(),
                SearchFacets {
                    sidechain: SidechainFacet::SubagentOnly,
                    ..Default::default()
                },
            ),
            (
                SearchFilters {
                    providers: vec![SearchProvider::Codex],
                    ..filters
                },
                SearchFacets::default(),
            ),
        ] {
            let lexical = store
                .query_faceted(
                    SearchQuery {
                        text: "shared-token",
                        filters: &filters,
                    },
                    10,
                    &facets,
                )
                .unwrap();
            let semantic = store
                .query_semantic_filtered(&[1.0, 0.0], 10, &filters, &facets, false)
                .unwrap();
            assert!(!lexical.iter().any(|hit| hit.id == fixture.claude_mid));
            assert!(semantic.is_empty());
        }
    }

    #[test]
    fn filtered_metadata_owner_keeps_matching_session_and_representative() {
        let fixture = filter_fixture();
        let store = &fixture.store;
        let other_session = sid(IdKind::Session, b"metadata-other-session");
        let other_document = sid(IdKind::Document, b"metadata-other-doc");
        let mut source = source_batch(
            "metadata-other.jsonl",
            vec![
                entity_entry(&other_session),
                (
                    other_document.clone(),
                    serde_json::json!({"provider":"claude-code"})
                        .to_string()
                        .into_bytes(),
                    String::new(),
                ),
            ],
            vec![placement(
                &other_session,
                &other_document,
                &fixture.codex_mid,
                0,
                false,
                None,
            )],
            Vec::new(),
            true,
        );
        source.resume_claims.push(SourceResumeClaim {
            provider_id: "claude-code".into(),
            session_id: other_session.as_str().into(),
            provider_session_id: Some("uniquemetadataneedle".into()),
            provider_session_id_state: "resolved".into(),
            original_working_directory: None,
            original_working_directory_state: "missing".into(),
            pair_observed: false,
        });
        store.commit_source_batches_if_changed(&[source]).unwrap();
        let filters = SearchFilters {
            providers: vec![SearchProvider::Codex],
            ..Default::default()
        };
        // The shared message is Codex in another Session, not in the metadata hit's Session.
        assert!(
            store
                .query_filtered(
                    SearchQuery {
                        text: "uniquemetadataneedle",
                        filters: &filters
                    },
                    10
                )
                .unwrap()
                .is_empty()
        );
        let filters = SearchFilters {
            providers: vec![SearchProvider::Claude],
            ..Default::default()
        };
        let hits = store
            .query_filtered(
                SearchQuery {
                    text: "uniquemetadataneedle",
                    filters: &filters,
                },
                10,
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, fixture.codex_mid);
        assert_eq!(hits[0].session_id.as_deref(), Some(other_session.as_str()));
    }

    #[test]
    fn empty_filters_match_unfiltered_query_results() {
        let fixture = filter_fixture();
        let unfiltered: Vec<String> = fixture
            .store
            .query("shared-token", 100)
            .unwrap()
            .into_iter()
            .map(|hit| hit.id.as_str().to_string())
            .collect();
        let empty = search_filtered(&fixture.store, "shared-token", &SearchFilters::default());
        assert_eq!(empty, unfiltered);
        assert_eq!(empty.len(), 5);
    }

    #[test]
    fn provider_filter_matches_document_provider_or() {
        let fixture = filter_fixture();
        let claude_only = SearchFilters {
            providers: vec![SearchProvider::Claude],
            ..SearchFilters::default()
        };
        let mut hits = search_filtered(&fixture.store, "shared-token", &claude_only);
        hits.sort();
        let mut expected = vec![
            fixture.claude_early.as_str().to_string(),
            fixture.claude_mid.as_str().to_string(),
        ];
        expected.sort();
        assert_eq!(hits, expected);

        // Multi-provider OR: both providers, and the null-timestamp row is
        // still included when no time dimension constrains it.
        let both = SearchFilters {
            providers: vec![SearchProvider::Claude, SearchProvider::Codex],
            ..SearchFilters::default()
        };
        let hits = search_filtered(&fixture.store, "shared-token", &both);
        assert_eq!(hits.len(), 5);
        assert!(hits.contains(&fixture.null_ts.as_str().to_string()));
    }

    #[test]
    fn time_filter_is_half_open_since_inclusive_until_exclusive() {
        let fixture = filter_fixture();
        // [mid, late): the two mid rows are in, early and late are out; the
        // null timestamp never satisfies a time predicate.
        let window = SearchFilters {
            providers: Vec::new(),
            since: Some(instant(1_785_196_800)), // 2026-07-28T00:00:00Z
            until: Some(instant(1_786_320_000)), // 2026-08-10T00:00:00Z
            repo: None,
        };
        let mut hits = search_filtered(&fixture.store, "shared-token", &window);
        hits.sort();
        let mut expected = vec![
            fixture.claude_mid.as_str().to_string(),
            fixture.codex_mid.as_str().to_string(),
        ];
        expected.sort();
        assert_eq!(hits, expected, "since inclusive, until exclusive");

        // since-only: mid and late in, early out.
        let since_only = SearchFilters {
            providers: Vec::new(),
            since: Some(instant(1_785_196_800)),
            until: None,
            repo: None,
        };
        let hits = search_filtered(&fixture.store, "shared-token", &since_only);
        assert_eq!(hits.len(), 3);
        assert!(!hits.contains(&fixture.claude_early.as_str().to_string()));

        // until-only: early and mid in, late out.
        let until_only = SearchFilters {
            providers: Vec::new(),
            since: None,
            until: Some(instant(1_786_320_000)),
            repo: None,
        };
        let hits = search_filtered(&fixture.store, "shared-token", &until_only);
        assert_eq!(hits.len(), 3);
        assert!(!hits.contains(&fixture.codex_late.as_str().to_string()));
    }

    #[test]
    fn provider_and_time_dimensions_are_anded() {
        let fixture = filter_fixture();
        let filters = SearchFilters {
            providers: vec![SearchProvider::Codex],
            since: Some(instant(1_785_196_800)),
            until: Some(instant(1_786_320_000)),
            repo: None,
        };
        let hits = search_filtered(&fixture.store, "shared-token", &filters);
        assert_eq!(hits, vec![fixture.codex_mid.as_str().to_string()]);
    }

    #[test]
    fn zero_match_filters_return_clean_empty_page() {
        let fixture = filter_fixture();
        let no_provider_overlap = SearchFilters {
            providers: vec![SearchProvider::Claude],
            since: Some(instant(1_786_320_000)), // late window: codex only
            until: None,
            repo: None,
        };
        assert!(search_filtered(&fixture.store, "shared-token", &no_provider_overlap).is_empty());
        let empty_window = SearchFilters {
            providers: Vec::new(),
            since: Some(instant(1_800_000_000)),
            until: Some(instant(1_800_100_000)),
            repo: None,
        };
        assert!(search_filtered(&fixture.store, "shared-token", &empty_window).is_empty());
    }

    #[test]
    fn filtered_predicates_apply_before_limit_in_one_statement() {
        // Pushdown proof: with limit = 1 the filtered query must return the
        // codex row even though a claude row sorts earlier in bm25 order —
        // filtering happens inside the single statement, before LIMIT.
        let fixture = filter_fixture();
        let filters = SearchFilters {
            providers: vec![SearchProvider::Codex],
            ..SearchFilters::default()
        };
        let mut hits = Vec::new();
        let statements = counted_statements(&fixture.store, || {
            hits = fixture
                .store
                .query_filtered(
                    SearchQuery {
                        text: "shared-token",
                        filters: &filters,
                    },
                    1,
                )
                .unwrap();
        });
        assert_eq!(hits.len(), 1);
        let hit = hits[0].id.as_str().to_string();
        assert!(
            hit == fixture.codex_mid.as_str()
                || hit == fixture.codex_late.as_str()
                || hit == fixture.null_ts.as_str(),
            "limit must cut the already-filtered ordering, got {hit}"
        );
        assert!(
            !hits
                .iter()
                .any(|h| h.id.as_str() == fixture.claude_early.as_str()
                    || h.id.as_str() == fixture.claude_mid.as_str()),
            "claude rows must be excluded before LIMIT"
        );
        assert_eq!(
            statements, 3,
            "filtered message and session metadata candidates each use one prepared \
             statement, plus one constant-cost index-projection-version gate read \
             (singleton row; not per-row — the N+1 invariant this pins is unchanged)"
        );
    }

    #[test]
    fn filtered_query_preserves_score_order_and_scores() {
        let fixture = filter_fixture();
        let filters = SearchFilters {
            providers: vec![SearchProvider::Codex],
            ..SearchFilters::default()
        };
        let hits = fixture
            .store
            .query_filtered(
                SearchQuery {
                    text: "shared-token",
                    filters: &filters,
                },
                100,
            )
            .unwrap();
        assert_eq!(hits.len(), 3);
        for pair in hits.windows(2) {
            assert!(
                pair[0].score > pair[1].score
                    || (pair[0].score == pair[1].score
                        && pair[0].id.as_str() < pair[1].id.as_str()),
                "score desc + id asc pinned order broken"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_session_grep_domain::{
        EvidenceSpan, IdKind, MessageRelation, Stability, TokenSource, ToolActivity,
        ToolActivityActor, ToolActivityKind, ToolActivityStatus,
    };
    use agent_session_grep_ports::SearchFilters;

    struct SnapshotRaceIndex<'a> {
        store: &'a SqliteStore,
        change: RefCell<Option<Box<dyn FnOnce() + 'a>>>,
        before_query: bool,
    }

    impl SearchIndex for SnapshotRaceIndex<'_> {
        fn index(&self, id: &StableId, text: &str) -> PortResult<()> {
            self.store.index(id, text)
        }

        fn query_filtered(
            &self,
            query: SearchQuery<'_>,
            limit: usize,
        ) -> PortResult<Vec<SearchHit>> {
            let change = self.change.borrow_mut().take();
            if self.before_query {
                if let Some(change) = change {
                    change();
                }
                self.store.query_filtered(query, limit)
            } else {
                let mut hits = self.store.query_filtered(query, limit)?;
                if let Some(change) = change {
                    change();
                }
                // Exercise Application's batched ownership read as well as payload.
                for hit in &mut hits {
                    hit.session_id = None;
                }
                Ok(hits)
            }
        }
    }

    impl SemanticIndex for SnapshotRaceIndex<'_> {
        fn index_embedding(&self, id: &StableId, embedding: &[f32]) -> PortResult<()> {
            self.store.index_embedding(id, embedding)
        }
        fn is_ready(&self, dimension: usize) -> PortResult<bool> {
            let ready = self.store.is_ready(dimension)?;
            if self.before_query
                && let Some(change) = self.change.borrow_mut().take()
            {
                change();
            }
            Ok(ready)
        }
        fn semantic_model_id(&self) -> PortResult<Option<String>> {
            self.store.semantic_model_id()
        }
        fn query_semantic_filtered(
            &self,
            embedding: &[f32],
            limit: usize,
            filters: &SearchFilters,
            facets: &SearchFacets,
            include_system: bool,
        ) -> PortResult<Vec<SearchHit>> {
            let hits = self.store.query_semantic_filtered(
                embedding,
                limit,
                filters,
                facets,
                include_system,
            )?;
            if !self.before_query
                && let Some(change) = self.change.borrow_mut().take()
            {
                change();
            }
            Ok(hits)
        }
    }

    #[test]
    fn semantic_readiness_query_and_payload_share_one_wal_snapshot() {
        use agent_session_grep_application::{App, AppRequest, AppResponse, ResponseBudget};
        use agent_session_grep_ports::{NoResumeClaims, RetrievalMode};
        for before_query in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("catalog.sqlite");
            let writer = SqliteStore::open_for_write(path.to_str().unwrap()).unwrap();
            let message = sid(IdKind::Message, b"semantic-snapshot");
            writer
                .put(&message, br#"{"role":"user","text":"needle old"}"#)
                .unwrap();
            writer.set_semantic_model("snapshot-model");
            writer.index_embedding(&message, &[1.0, 0.0]).unwrap();
            let reader = SqliteStore::open(path.to_str().unwrap()).unwrap();
            reader.set_semantic_model("snapshot-model");
            let generation = reader.active_generation().unwrap();
            let semantic = SnapshotRaceIndex {
                store: &reader,
                before_query,
                change: RefCell::new(Some(Box::new(|| {
                    writer
                        .put(&message, br#"{"role":"user","text":"needle fixed"}"#)
                        .unwrap();
                }))),
            };
            let app = App::with_resume_semantic(&reader, &reader, NoResumeClaims, semantic);
            let request = || AppRequest::Search {
                query: "needle".into(),
                filters: SearchFilters::EMPTY,
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Semantic,
                query_embedding: Some(vec![1.0, 0.0]),
            };
            for (expected_generation, expected_mode, expected_text) in [
                (generation, RetrievalMode::Semantic, "needle old"),
                (
                    generation + 1,
                    RetrievalMode::LexicalFallback,
                    "needle fixed",
                ),
            ] {
                let AppResponse::Search {
                    hits,
                    generation,
                    retrieval_mode,
                    fallback_warning,
                    ..
                } = app.handle(request()).unwrap()
                else {
                    panic!("search response")
                };
                assert_eq!(
                    generation, expected_generation,
                    "before_query={before_query}"
                );
                assert_eq!(retrieval_mode, expected_mode);
                assert_eq!(
                    fallback_warning.is_some(),
                    expected_mode == RetrievalMode::LexicalFallback
                );
                assert_eq!(hits.len(), 1);
                assert_eq!(hits[0].id, message);
                assert_eq!(hits[0].text.as_deref(), Some(expected_text));
            }
        }
    }

    #[test]
    fn app_read_snapshot_survives_wal_writer_before_query() {
        assert_app_read_snapshot_with_writer(true);
    }

    #[test]
    fn app_read_snapshot_survives_wal_writer_before_payload_and_ownership() {
        assert_app_read_snapshot_with_writer(false);
    }

    fn assert_app_read_snapshot_with_writer(before_query: bool) {
        use agent_session_grep_application::{App, AppRequest, AppResponse, ResponseBudget};
        use agent_session_grep_ports::RetrievalMode;
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("catalog.db");
            let writer = SqliteStore::open_for_write(path.to_str().unwrap()).unwrap();
            let message = sid(IdKind::Message, b"snapshot-message");
            let document = sid(IdKind::Document, b"snapshot-document");
            let old_owner = sid(IdKind::Session, b"snapshot-old-owner");
            let new_owner = sid(IdKind::Session, b"snapshot-new-owner");
            let old_payload = br#"{"role":"user","text":"snapshotneedle old"}"#;
            let new_payload = br#"{"role":"user","text":"replacementneedle new"}"#;
            writer
                .commit_source_batches_if_changed(&[source_batch(
                    "synthetic-snapshot.jsonl",
                    vec![
                        (
                            message.clone(),
                            old_payload.to_vec(),
                            "snapshotneedle old".into(),
                        ),
                        entity_entry(&document),
                        entity_entry(&old_owner),
                        entity_entry(&new_owner),
                    ],
                    vec![placement(&old_owner, &document, &message, 0, false, None)],
                    Vec::new(),
                    true,
                )])
                .unwrap();
            let reader = SqliteStore::open(path.to_str().unwrap()).unwrap();
            let generation = reader.active_generation().unwrap();
            let index = SnapshotRaceIndex {
                store: &reader,
                before_query,
                change: RefCell::new(Some(Box::new(|| {
                    let mut conn = writer.conn.borrow_mut();
                    let tx = conn.transaction().unwrap();
                    tx.execute(
                        "UPDATE catalog SET payload=?1 WHERE id=?2",
                        rusqlite::params![new_payload.as_slice(), message.as_str()],
                    )
                    .unwrap();
                    tx.execute("UPDATE fts SET text='replacementneedle new' WHERE rowid=(SELECT fts_rowid FROM fts_ids WHERE wire_id=?1)", [message.as_str()]).unwrap();
                    tx.execute_batch("DELETE FROM session_fts; DELETE FROM session_fts_ids;")
                        .unwrap();
                    tx.execute(
                        "UPDATE message_placements SET session_id=?1 WHERE message_id=?2",
                        rusqlite::params![new_owner.as_str(), message.as_str()],
                    )
                    .unwrap();
                    tx.execute("UPDATE store_metadata SET active_generation=active_generation+1 WHERE singleton=1", []).unwrap();
                    tx.commit().unwrap();
                }))),
            };
            let app = App::new(&reader, index);
            let request = |text: &str| AppRequest::Search {
                query: text.into(),
                filters: SearchFilters::EMPTY,
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            };
            let AppResponse::Search {
                hits,
                generation: observed,
                ..
            } = app.handle(request("snapshotneedle")).unwrap()
            else {
                panic!("search")
            };
            assert_eq!(observed, generation);
            assert_eq!(hits.len(), 1, "query must use the generation's snapshot");
            assert_eq!(hits[0].text.as_deref(), Some("snapshotneedle old"));
            assert_eq!(hits[0].session_id.as_deref(), Some(old_owner.as_str()));
            let AppResponse::Search {
                hits,
                generation: observed,
                ..
            } = app.handle(request("replacementneedle")).unwrap()
            else {
                panic!("search")
            };
            assert_eq!(observed, generation + 1);
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].text.as_deref(), Some("replacementneedle new"));
            assert_eq!(hits[0].session_id.as_deref(), Some(new_owner.as_str()));
        }
    }

    #[test]
    fn read_snapshot_pins_immediately_and_nested_guards_release_last() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.db");
        let writer = SqliteStore::open_for_write(path.to_str().unwrap()).unwrap();
        let reader = SqliteStore::open(path.to_str().unwrap()).unwrap();
        let schema = reader.schema_version().unwrap();
        let outer = reader.begin_read_snapshot().unwrap();
        // No caller SELECT between begin and the writer: tests eager pinning.
        writer
            .conn
            .borrow()
            .execute("UPDATE store_metadata SET active_generation=1", [])
            .unwrap();
        let reader_ref = &reader;
        let inner = CatalogStore::begin_read_snapshot(&reader_ref).unwrap();
        assert_eq!(reader.active_generation().unwrap(), 0);
        drop(outer); // Non-LIFO release must not terminate the inner view.
        assert_eq!(reader.active_generation().unwrap(), 0);
        assert!(!reader.conn.borrow().is_autocommit());
        drop(inner);
        assert!(reader.conn.borrow().is_autocommit());
        let next = reader.begin_read_snapshot().unwrap();
        assert_eq!(reader.active_generation().unwrap(), 1);
        assert!(reader.conn.borrow().is_readonly("main").unwrap());
        assert_eq!(reader.schema_version().unwrap(), schema);
        assert!(
            reader
                .put(&sid(IdKind::Message, b"read-only"), b"{}")
                .is_err()
        );
        drop(next);
        assert_eq!(reader.conn.borrow().total_changes(), 0);
    }

    #[test]
    fn read_snapshot_releases_after_app_errors_and_unwind() {
        use agent_session_grep_application::{App, AppRequest};
        let store = SqliteStore::open_in_memory().unwrap();
        let app = App::new(&store, &store);
        // Validation error, after snapshot entry but before data assembly.
        assert!(
            app.handle(AppRequest::GetSessionResume {
                session_id: sid(IdKind::Message, b"wrong-kind"),
            })
            .is_err()
        );
        assert!(store.conn.borrow().is_autocommit());
        assert_eq!(store.read_snapshot_count.get(), 0);
        // Backend error in an App read releases its nested guard but not ours.
        let outer = store.begin_read_snapshot().unwrap();
        assert!(
            app.handle(AppRequest::Context {
                session_id: sid(IdKind::Session, b"absent"),
                policy: agent_session_grep_domain::ContextPolicy::Mainline,
                level: agent_session_grep_application::ContextLevel::Raw,
                budget: Default::default(),
            })
            .is_err()
        );
        assert_eq!(store.read_snapshot_count.get(), 1);
        drop(outer);
        assert!(store.conn.borrow().is_autocommit());
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _snapshot = store.begin_read_snapshot().unwrap();
            panic!("synthetic read failure");
        }));
        assert!(unwind.is_err());
        assert!(store.conn.borrow().is_autocommit());
        let _next = store.begin_read_snapshot().unwrap();
    }

    #[test]
    fn read_snapshot_refuses_unrelated_transaction_and_rolls_back_failed_pin() {
        let store = SqliteStore::open_in_memory().unwrap();
        store
            .conn
            .borrow()
            .execute_batch("BEGIN IMMEDIATE")
            .unwrap();
        assert!(matches!(
            store.begin_read_snapshot(),
            Err(PortError::InvalidRequest(_))
        ));
        assert!(!store.conn.borrow().is_autocommit());
        store
            .conn
            .borrow()
            .execute_batch("ROLLBACK; ALTER TABLE store_metadata RENAME TO hidden_metadata")
            .unwrap();
        assert!(matches!(
            store.begin_read_snapshot(),
            Err(PortError::Backend(_))
        ));
        assert!(store.conn.borrow().is_autocommit());
        assert_eq!(store.read_snapshot_count.get(), 0);
        store
            .conn
            .borrow()
            .execute_batch("ALTER TABLE hidden_metadata RENAME TO store_metadata")
            .unwrap();
        let _next = store.begin_read_snapshot().unwrap();
    }

    #[test]
    fn read_snapshot_cleanup_failure_refuses_later_requests() {
        let store = SqliteStore::open_in_memory().unwrap();
        let snapshot = store.begin_read_snapshot().unwrap();
        // Simulate external transaction misuse so cleanup cannot succeed.
        store.conn.borrow().execute_batch("ROLLBACK").unwrap();
        drop(snapshot);
        assert!(matches!(
            store.begin_read_snapshot(),
            Err(PortError::Backend(_))
        ));
        assert!(store.read_snapshot_failed.get());
    }

    #[test]
    fn read_snapshot_refuses_writes_and_restores_writer_after_last_drop() {
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"snapshot-write-refusal");
        let outer = store.begin_read_snapshot().unwrap();
        let inner = store.begin_read_snapshot().unwrap();
        assert!(store.put(&id, b"{}").is_err());
        assert!(store.index(&id, "forbidden").is_err());
        assert_eq!(store.get(&id).unwrap(), None);
        drop(outer);
        assert!(store.put(&id, b"{}").is_err());
        drop(inner);
        store.put(&id, b"{}").unwrap();
        assert_eq!(store.get(&id).unwrap(), Some(b"{}".to_vec()));
    }

    type PlacementSnapshotRow = (
        String,
        String,
        String,
        String,
        i64,
        i64,
        Option<i64>,
        Option<i64>,
    );

    /// Test migrations independently of write-open recovery/reprojection, while
    /// retaining the same exclusive lease required by production migrations.
    fn open_migration_fixture(path: &str) -> SqliteStore {
        let lease = WriterLease::try_acquire(Path::new(path).parent().unwrap()).unwrap();
        let conn = Connection::open(path).unwrap();
        SqliteStore::init(&conn).unwrap();
        SqliteStore {
            conn: RefCell::new(conn),
            read_snapshot_count: Cell::new(0),
            read_snapshot_failed: Cell::new(false),
            _lease: Some(lease),
            semantic_model_id: RefCell::new(None),
            repo_slug_resolver: RefCell::new(Box::new(NoopRepoSlugResolver)),
            pending_installations: RefCell::new(BTreeMap::new()),
            relocation_clock: unix_ms,
        }
    }

    pub(crate) fn sid(kind: IdKind, fact: &[u8]) -> StableId {
        StableId::derive(kind, Stability::Reconstructed, &[fact])
    }

    #[test]
    fn semantic_vectors_reject_nonfinite_and_corrupt_storage() {
        use agent_session_grep_application::{App, AppError, AppRequest, ResponseBudget};
        use agent_session_grep_ports::{NoResumeClaims, RetrievalMode};
        let store = SqliteStore::open_in_memory().unwrap();
        let search = || {
            App::with_resume_semantic(&store, &store, NoResumeClaims, &store).handle(
                AppRequest::Search {
                    query: "needle".into(),
                    filters: SearchFilters::EMPTY,
                    facets: SearchFacets::default(),
                    limit: 10,
                    cursor: None,
                    budget: ResponseBudget::default(),
                    include_system: false,
                    group_by_session: false,
                    mode: RetrievalMode::Semantic,
                    query_embedding: Some(vec![1.0, 0.0]),
                },
            )
        };
        store.set_semantic_model("finite-model");
        let id = sid(IdKind::Message, b"finite");
        store.put(&id, b"{}").unwrap();
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(store.index_embedding(&id, &[bad, 1.0]).is_err());
            assert!(store.query_semantic(&[bad, 1.0], 1).is_err());
        }
        assert!(!store.is_ready(2).unwrap());
        store.index_embedding(&id, &[f32::MAX, f32::MAX]).unwrap();
        let hits = store.query_semantic(&[f32::MAX, f32::MAX], 1).unwrap();
        assert!(
            (hits[0].score - 1.0).abs() < 1e-6,
            "finite extremes do not overflow f32 accumulators"
        );
        store
            .conn
            .borrow()
            .execute(
                "UPDATE message_vec SET embedding = ?1",
                [f32_slice_to_bytes(&[f32::NAN, 1.0])],
            )
            .unwrap();
        assert!(store.query_semantic(&[1.0, 0.0], 1).is_err());
        assert!(
            store.is_ready(2).unwrap(),
            "matching corrupt row must not masquerade as absent"
        );
        assert!(matches!(
            search(),
            Err(AppError::Port(PortError::Backend(_)))
        ));
        store
            .conn
            .borrow()
            .execute("UPDATE message_vec SET embedding = X'0000'", [])
            .unwrap();
        assert!(store.query_semantic(&[1.0, 0.0], 1).is_err());
        assert!(
            store.is_ready(2).unwrap(),
            "matching corrupt row must not masquerade as absent"
        );
        assert!(matches!(
            search(),
            Err(AppError::Port(PortError::Backend(_)))
        ));
        store
            .conn
            .borrow()
            .execute("DROP TABLE message_vec", [])
            .unwrap();
        assert!(matches!(store.is_ready(2), Err(PortError::Backend(_))));
        assert!(matches!(
            search(),
            Err(AppError::Port(PortError::Backend(_)))
        ));
    }

    #[test]
    fn semantic_noise_filter_preserves_later_pages_and_deleted_rows_are_hidden() {
        use agent_session_grep_application::{App, AppRequest, AppResponse, ResponseBudget};
        use agent_session_grep_ports::{NoResumeClaims, RetrievalMode};
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_semantic_model("page-model");
        let mut visible = Vec::new();
        for (index, role) in ["user", "system", "developer", "system", "user"]
            .iter()
            .enumerate()
        {
            let id = StableId::native(IdKind::Message, &format!("page-{index}"));
            store
                .commit_batch(&[(
                    id.clone(),
                    serde_json::json!({"text":"needle", "role":role})
                        .to_string()
                        .into_bytes(),
                    "needle".into(),
                )])
                .unwrap();
            store
                .index_embedding(&id, &[1.0, index as f32 / 5.0])
                .unwrap();
            if *role == "user" {
                visible.push(id);
            }
        }
        let app = App::with_resume_semantic(&store, &store, NoResumeClaims, &store);
        let mut token = None;
        let mut actual = Vec::new();
        for _ in 0..4 {
            let response = app
                .handle(AppRequest::Search {
                    query: "needle".into(),
                    filters: SearchFilters::EMPTY,
                    facets: SearchFacets::default(),
                    limit: 1,
                    cursor: token.take(),
                    budget: ResponseBudget::default(),
                    include_system: false,
                    group_by_session: false,
                    mode: RetrievalMode::Semantic,
                    query_embedding: Some(vec![1.0, 0.0]),
                })
                .unwrap();
            let AppResponse::Search {
                hits, next_cursor, ..
            } = response
            else {
                panic!("search")
            };
            actual.extend(hits.into_iter().map(|hit| hit.id));
            token = next_cursor;
            if token.is_none() {
                break;
            }
        }
        assert!(token.is_none());
        assert_eq!(actual, visible);
        store
            .conn
            .borrow()
            .execute("DELETE FROM catalog WHERE id = ?1", [visible[0].as_str()])
            .unwrap();
        assert!(
            store
                .query_semantic(&[1.0, 0.0], 10)
                .unwrap()
                .iter()
                .all(|hit| hit.id != visible[0])
        );
    }

    #[test]
    fn semantic_top_k_is_exact_over_a_representative_scan() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_semantic_model("scan-model");
        {
            let mut conn = store.conn.borrow_mut();
            let tx = conn.transaction().unwrap();
            for index in 0..2048 {
                let id = StableId::native(IdKind::Message, &format!("scan-{index:04}"));
                tx.execute(
                    "INSERT INTO catalog(id,payload) VALUES(?1,?2)",
                    rusqlite::params![id.as_str(), b"{}".as_slice()],
                )
                .unwrap();
                tx.execute("INSERT INTO message_vec(wire_id,model_id,dimension,embedding) VALUES(?1,'scan-model',2,?2)",
                    rusqlite::params![id.as_str(), f32_slice_to_bytes(&[1.0, index as f32 / 2048.0])]).unwrap();
            }
            tx.commit().unwrap();
        }
        let started = std::time::Instant::now();
        let hits = store.query_semantic(&[1.0, 1.0], 17).unwrap();
        assert_eq!(hits.len(), 17);
        // Independently sort all similarities to verify top-k and tie order.
        let mut expected: Vec<_> = (0..2048)
            .map(|index| {
                (
                    cosine_similarity(&[1.0, 1.0], &[1.0, index as f32 / 2048.0]),
                    format!("msg_v1_scan-{index:04}"),
                )
            })
            .collect();
        expected.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        assert_eq!(
            hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
            expected
                .iter()
                .take(17)
                .map(|item| item.1.as_str())
                .collect::<Vec<_>>()
        );
        eprintln!(
            "semantic exact scan: rows=2048 dimensions=2 retained=17 elapsed={:?}; candidate heap bounded to k+1, work remains O(N*d + N*log(k))",
            started.elapsed()
        );
    }

    thread_local! { static SEARCH_SQL: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) }; }

    #[test]
    fn lexical_rrf_ties_use_wire_id_before_limit_across_identity_tiers() {
        let store = SqliteStore::open_in_memory().unwrap();
        let mut ids = [
            StableId::native(IdKind::Message, "z-native"),
            StableId::derive(IdKind::Message, Stability::Reconstructed, &[b"tie"]),
            StableId::from_wire("msg_v1_a-unstable").unwrap(),
        ];
        let entries: Vec<_> = ids
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    br#"{"role":"user","text":"needle"}"#.to_vec(),
                    "needle".to_owned(),
                )
            })
            .collect();
        store.commit_batch(&entries).unwrap();
        ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        for limit in [1, ids.len()] {
            for facets in [
                SearchFacets::default(),
                SearchFacets {
                    sidechain: SidechainFacet::MainOnly,
                    ..Default::default()
                },
            ] {
                let hits = store
                    .query_with_policy(
                        SearchQuery {
                            text: "needle",
                            filters: &SearchFilters::EMPTY,
                        },
                        limit,
                        &facets,
                        false,
                    )
                    .unwrap();
                assert_eq!(
                    hits.iter().map(|hit| &hit.id).collect::<Vec<_>>(),
                    ids.iter().take(limit).collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn separate_fts_corpora_merge_by_rrf_rank_not_raw_bm25() {
        let store = SqliteStore::open_in_memory().unwrap();
        let first = StableId::native(IdKind::Message, "rrf-a");
        let second = StableId::native(IdKind::Message, "rrf-b");
        let session = StableId::native(IdKind::Session, "rrf-session");
        store
            .commit_batch(&[
                (
                    first.clone(),
                    br#"{"role":"user","text":"needle needle"}"#.to_vec(),
                    "needle needle".into(),
                ),
                (
                    second.clone(),
                    br#"{"role":"user","text":"needle other"}"#.to_vec(),
                    "needle other".into(),
                ),
                (session.clone(), b"{}".to_vec(), String::new()),
            ])
            .unwrap();
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO session_fts(session_wire,text) VALUES(?1,'needle')",
                [session.as_str()],
            )
            .unwrap();
            let rowid = conn.last_insert_rowid();
            conn.execute(
                "INSERT INTO session_fts_ids(session_wire,fts_rowid) VALUES(?1,?2)",
                rusqlite::params![session.as_str(), rowid],
            )
            .unwrap();
        }
        let hits = store.query("needle", 10).unwrap();
        assert_eq!(hits.len(), 3);
        let score = |id: &StableId| hits.iter().find(|hit| hit.id == *id).unwrap().score;
        assert_eq!(score(&first), 1.0 / 61.0);
        assert_eq!(score(&session), 1.0 / 61.0);
        assert_eq!(score(&second), 1.0 / 62.0);
    }

    #[test]
    fn metadata_sql_has_bounded_shape_and_indexed_session_probes() {
        let store = SqliteStore::open_in_memory().unwrap();
        let entries: Vec<_> = (0..1100)
            .map(|index| {
                (
                    StableId::native(IdKind::Message, &format!("bulk-{index}")),
                    br#"{"role":"user","text":"needle"}"#.to_vec(),
                    "needle".to_owned(),
                )
            })
            .collect();
        store.commit_batch(&entries).unwrap();
        SEARCH_SQL.with(|sql| sql.borrow_mut().clear());
        store.conn.borrow().trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
            Some(|event| {
                if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event
                    && sql.contains("FROM session_fts JOIN")
                {
                    SEARCH_SQL.with(|queries| queries.borrow_mut().push(sql.to_owned()));
                }
            }),
        );
        assert_eq!(store.query("needle", 1100).unwrap().len(), 1100);
        store
            .conn
            .borrow()
            .trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
        let sql = SEARCH_SQL.with(|queries| queries.borrow()[0].clone());
        assert_eq!(
            sql.matches("json_each(").count(),
            1,
            "one bound exclusion set, not one clause per message"
        );
        assert!(sql.len() < 3000);
        let conn = store.conn.borrow();
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let params = vec![rusqlite::types::Value::Null; stmt.parameter_count()];
        let plan: Vec<String> = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| row.get(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|line| line.contains("message_placements_session_order")),
            "{plan:?}"
        );
        eprintln!(
            "metadata SQL: message exclusions=1100 sql_bytes={} plan={plan:?}",
            sql.len()
        );
    }

    #[test]
    fn semantic_index_is_not_ready_without_model_or_vectors() {
        let store = SqliteStore::open_in_memory().unwrap();
        // 未设模型：未就绪，查询空，写入报错（不写无归属向量）。
        assert!(!store.is_ready(2).unwrap());
        assert!(store.query_semantic(&[0.1, 0.2], 5).unwrap().is_empty());
        assert!(
            store
                .index_embedding(&sid(IdKind::Message, b"m"), &[0.1])
                .is_err()
        );
        // 设了模型但表空：仍未就绪，Application 必须降级为 lexical_fallback。
        store.set_semantic_model("test-model");
        assert!(!store.is_ready(2).unwrap());
    }

    #[test]
    fn semantic_index_round_trips_and_ranks_by_cosine() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_semantic_model("test-model");
        let near = sid(IdKind::Message, b"near");
        let far = sid(IdKind::Message, b"far");
        store.put(&near, b"{}").unwrap();
        store.put(&far, b"{}").unwrap();
        // near 与查询同向；far 正交。
        store.index_embedding(&near, &[1.0, 0.0, 0.0]).unwrap();
        store.index_embedding(&far, &[0.0, 1.0, 0.0]).unwrap();
        assert!(store.is_ready(3).unwrap());

        let hits = store.query_semantic(&[1.0, 0.0, 0.0], 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id.as_str(), near.as_str());
        assert!(hits[0].score > hits[1].score);
        assert!((hits[0].score - 1.0).abs() < 1e-5);
        assert!(hits[1].score.abs() < 1e-5);
    }

    #[test]
    fn semantic_query_skips_other_models_and_dimensions() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_semantic_model("model-a");
        store.put(&sid(IdKind::Message, b"a"), b"{}").unwrap();
        store
            .index_embedding(&sid(IdKind::Message, b"a"), &[1.0, 0.0])
            .unwrap();
        // 换模型：旧向量因 model_id 不匹配被排除，不参与相似度。
        store.set_semantic_model("model-b");
        assert!(!store.is_ready(2).unwrap());
        assert!(store.query_semantic(&[1.0, 0.0], 10).unwrap().is_empty());
        // 同模型但维度不同的查询也不匹配（避免截断比较产出无意义分数）。
        store.set_semantic_model("model-a");
        assert!(
            store
                .query_semantic(&[1.0, 0.0, 0.0], 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn semantic_readiness_matches_query_dimension_model_and_live_catalog() {
        use agent_session_grep_application::{App, AppRequest, AppResponse, ResponseBudget};
        use agent_session_grep_ports::{NoResumeClaims, RetrievalMode};
        let store = SqliteStore::open_in_memory().unwrap();
        let live = sid(IdKind::Message, b"readiness-live");
        let foreign = sid(IdKind::Message, b"readiness-foreign");
        let orphan = sid(IdKind::Message, b"readiness-orphan");
        store
            .put(&live, br#"{"role":"user","text":"needle"}"#)
            .unwrap();
        store
            .put(&foreign, br#"{"role":"user","text":"foreign"}"#)
            .unwrap();
        store.set_semantic_model("other-model");
        store.index_embedding(&foreign, &[1.0, 0.0, 0.0]).unwrap();
        store.set_semantic_model("selected-model");
        store.index_embedding(&live, &[1.0, 0.0]).unwrap();
        // Historical orphan has the requested dimension, but no live payload.
        store.conn.borrow().execute(
            "INSERT INTO message_vec(wire_id,model_id,dimension,embedding) VALUES(?1,'selected-model',3,?2)",
            rusqlite::params![orphan.as_str(), f32_slice_to_bytes(&[1.0, 0.0, 0.0])],
        ).unwrap();
        let generation = store.active_generation().unwrap();
        let app = App::with_resume_semantic(&store, &store, NoResumeClaims, &store);
        for mode in [RetrievalMode::Semantic, RetrievalMode::Hybrid] {
            for (dimension, expected_mode) in [(3, RetrievalMode::LexicalFallback), (2, mode)] {
                let response = app
                    .handle(AppRequest::Search {
                        query: "needle".into(),
                        filters: SearchFilters::EMPTY,
                        facets: SearchFacets::default(),
                        limit: 10,
                        cursor: None,
                        budget: ResponseBudget::default(),
                        include_system: false,
                        group_by_session: false,
                        mode,
                        query_embedding: Some(vec![1.0; dimension]),
                    })
                    .unwrap();
                let AppResponse::Search {
                    hits,
                    retrieval_mode,
                    fallback_warning,
                    ..
                } = response
                else {
                    panic!("search response")
                };
                assert_eq!(
                    retrieval_mode, expected_mode,
                    "{mode:?}, dimension={dimension}"
                );
                assert_eq!(
                    fallback_warning.is_some(),
                    expected_mode == RetrievalMode::LexicalFallback
                );
                assert_eq!(hits.len(), 1);
                assert_eq!(hits[0].id, live);
            }
        }
        assert_eq!(store.active_generation().unwrap(), generation);
        assert_eq!(
            table_count(&store, "message_vec"),
            3,
            "readiness and query must not prune orphans"
        );
    }

    #[test]
    fn semantic_catalog_put_invalidates_only_changed_body_and_rolls_back_on_error() {
        let store = SqliteStore::open_in_memory().unwrap();
        let changed = sid(IdKind::Message, b"put-vector-changed");
        let retained = sid(IdKind::Message, b"put-vector-retained");
        let original = typed_message_entry(&changed, "obsolete body").1;
        store.put(&changed, &original).unwrap();
        store.put(&retained, br#"{"text":"unrelated"}"#).unwrap();
        store.set_semantic_model("original-model");
        store.index_embedding(&changed, &[1.0, 0.0]).unwrap();
        store.set_semantic_model("current-model");
        store.index_embedding(&retained, &[0.0, 1.0]).unwrap();
        let mut metadata_only: serde_json::Value = serde_json::from_slice(&original).unwrap();
        metadata_only["seq"] = serde_json::json!(2);
        let metadata_only = serde_json::to_vec(&metadata_only).unwrap();
        store.put(&changed, &metadata_only).unwrap();
        assert_eq!(
            table_count(&store, "message_vec"),
            2,
            "metadata alone retains vectors"
        );
        let generation = store.active_generation().unwrap();
        let corrected = typed_message_entry(&changed, "fixed").1;
        store.conn.borrow().execute_batch(
            "CREATE TRIGGER reject_put_vector_generation BEFORE UPDATE ON store_metadata BEGIN SELECT RAISE(ABORT, 'synthetic'); END;"
        ).unwrap();
        assert!(store.put(&changed, &corrected).is_err());
        assert_eq!(store.get(&changed).unwrap(), Some(metadata_only));
        assert_eq!(table_count(&store, "message_vec"), 2);
        assert_eq!(store.active_generation().unwrap(), generation);
        assert_eq!(store.query("obsolete", 10).unwrap().len(), 1);
        store
            .conn
            .borrow()
            .execute_batch("DROP TRIGGER reject_put_vector_generation;")
            .unwrap();
        store.put(&changed, &corrected).unwrap();
        assert_eq!(store.get(&changed).unwrap(), Some(corrected));
        assert_eq!(store.active_generation().unwrap(), generation + 1);
        assert_eq!(
            table_count(&store, "message_vec"),
            1,
            "put must retire the previous body vector even for an unselected model"
        );
        assert!(store.query("obsolete", 10).unwrap().is_empty());
        assert_eq!(store.query("fixed", 10).unwrap()[0].id, changed);
        assert_eq!(
            store.query_semantic(&[0.0, 1.0], 10).unwrap()[0].id,
            retained
        );
        store.set_semantic_model("original-model");
        assert!(store.query_semantic(&[1.0, 0.0], 10).unwrap().is_empty());
    }

    #[test]
    fn semantic_full_body_changes_beyond_fts_cap_invalidate_each_public_batch_path() {
        let prefix = "x".repeat(agent_session_grep_application::MESSAGE_FTS_MAX_CHARS);
        let old = format!("{prefix} old-tail");
        let corrected = format!("{prefix} new-tail");
        let mut stale_paths = Vec::new();
        for path in [
            "put",
            "commit_batch",
            "commit_batch_if_changed",
            "commit_index_batch",
            "source_batch",
        ] {
            let store = SqliteStore::open_in_memory().unwrap();
            let message = sid(IdKind::Message, path.as_bytes());
            let write = |body: &str| -> PortResult<()> {
                let entries = [typed_message_entry(&message, body)];
                match path {
                    "put" => store.put(&message, &entries[0].1),
                    "commit_batch" => store.commit_batch(&entries),
                    "commit_batch_if_changed" => {
                        store.commit_batch_if_changed(&entries).map(|_| ())
                    }
                    "commit_index_batch" => {
                        let pending = store.begin_index_batch(&entries, &[])?;
                        store.commit_index_batch(&pending, &entries, &[])
                    }
                    "source_batch" => store
                        .commit_source_batches_if_changed(&[projection_source(
                            "tail-source",
                            &message,
                            body,
                        )])
                        .map(|_| ()),
                    _ => unreachable!(),
                }
            };
            write(&old).unwrap();
            let before = store.get(&message).unwrap().unwrap();
            store.set_semantic_model("full-body-model");
            store.index_embedding(&message, &[1.0, 0.0]).unwrap();
            let generation = store.active_generation().unwrap();
            write(&corrected).unwrap();
            let after = store.get(&message).unwrap().unwrap();
            assert_eq!(
                searchable_text(&before),
                searchable_text(&after),
                "FTS prefix deliberately unchanged"
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&after).unwrap()["text"],
                corrected
            );
            assert_eq!(store.active_generation().unwrap(), generation + 1, "{path}");
            if table_count(&store, "message_vec") != 0 {
                stale_paths.push(path);
            }
        }
        assert!(
            stale_paths.is_empty(),
            "full body changed but stale vectors survived: {stale_paths:?}"
        );
    }

    fn vector_rows(store: &SqliteStore) -> Vec<(String, String, i64, Vec<u8>)> {
        let conn = store.conn.borrow();
        let mut stmt = conn
            .prepare(
                "SELECT wire_id,model_id,dimension,embedding FROM message_vec ORDER BY wire_id",
            )
            .unwrap();
        stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
    }

    #[test]
    fn semantic_orphans_are_pruned_only_by_explicit_rebuild_and_rollback_atomically() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("catalog.sqlite");
        let path = path.to_str().unwrap();
        let store = SqliteStore::open_for_write(path).unwrap();
        let live = sid(IdKind::Message, b"maintenance-live");
        let other = sid(IdKind::Message, b"maintenance-other");
        let orphan = sid(IdKind::Message, b"maintenance-orphan");
        for (id, model, embedding) in [
            (&live, "live-model", [1.0, 0.0]),
            (&other, "other-model", [0.0, 1.0]),
        ] {
            store
                .put(id, br#"{"role":"user","text":"needle"}"#)
                .unwrap();
            store.set_semantic_model(model);
            store.index_embedding(id, &embedding).unwrap();
        }
        store.conn.borrow().execute(
            "INSERT INTO message_vec(wire_id,model_id,dimension,embedding) VALUES(?1,'orphan-only-model',2,?2)",
            rusqlite::params![orphan.as_str(), f32_slice_to_bytes(&[1.0, 1.0])],
        ).unwrap();
        let before = vector_rows(&store);
        let generation = store.active_generation().unwrap();
        let payloads = store.get_many(&[live.clone(), other.clone()]).unwrap();
        drop(store);

        let reader = SqliteStore::open(path).unwrap();
        reader.set_semantic_model("orphan-only-model");
        assert!(!reader.is_ready(2).unwrap());
        assert!(reader.query_semantic(&[1.0, 1.0], 10).unwrap().is_empty());
        assert_eq!(reader.query("needle", 10).unwrap().len(), 2);
        assert_eq!(
            vector_rows(&reader),
            before,
            "read-side never repairs historical rows"
        );
        assert_eq!(reader.active_generation().unwrap(), generation);
        assert!(
            reader.rebuild_index().is_err(),
            "read-only open cannot perform maintenance"
        );
        assert_eq!(vector_rows(&reader), before);
        drop(reader);

        let store = SqliteStore::open_for_write(path).unwrap();
        assert_eq!(
            vector_rows(&store),
            before,
            "ordinary current-schema open does not prune"
        );
        store.conn.borrow().execute_batch(
            "CREATE TRIGGER reject_vector_rebuild_activation BEFORE UPDATE ON index_batches WHEN NEW.state='activated' BEGIN SELECT RAISE(ABORT, 'synthetic'); END;"
        ).unwrap();
        assert!(store.rebuild_index().is_err());
        assert_eq!(vector_rows(&store), before);
        assert_eq!(
            store.get_many(&[live.clone(), other.clone()]).unwrap(),
            payloads
        );
        assert_eq!(store.active_generation().unwrap(), generation);
        store
            .conn
            .borrow()
            .execute_batch("DROP TRIGGER reject_vector_rebuild_activation;")
            .unwrap();
        assert_eq!(store.rebuild_index().unwrap(), 2);
        let expected: Vec<_> = before
            .into_iter()
            .filter(|row| row.0 != orphan.as_str())
            .collect();
        assert_eq!(
            vector_rows(&store),
            expected,
            "maintenance removes only orphan vectors, across all models"
        );
        assert_eq!(store.get_many(&[live, other]).unwrap(), payloads);
        assert_eq!(store.active_generation().unwrap(), generation + 1);
        assert_eq!(store.query("needle", 10).unwrap().len(), 2);
    }

    #[test]
    fn semantic_index_upserts_and_clear_removes_only_that_model() {
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"m");
        store.put(&id, b"{}").unwrap();
        store.put(&sid(IdKind::Message, b"n"), b"{}").unwrap();
        store.set_semantic_model("model-a");
        store.index_embedding(&id, &[1.0, 0.0]).unwrap();
        // 同 id 重写是 upsert，不是第二行。
        store.index_embedding(&id, &[0.0, 1.0]).unwrap();
        let hits = store.query_semantic(&[0.0, 1.0], 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert!((hits[0].score - 1.0).abs() < 1e-5);

        store.set_semantic_model("model-b");
        store
            .index_embedding(&sid(IdKind::Message, b"n"), &[1.0, 0.0])
            .unwrap();
        let generation = store.active_generation().unwrap();
        assert_eq!(store.clear_embeddings("model-a").unwrap(), 1);
        assert_eq!(store.active_generation().unwrap(), generation + 1);
        assert_eq!(store.clear_embeddings("model-a").unwrap(), 0);
        assert_eq!(store.active_generation().unwrap(), generation + 1);
        // model-b 的向量不受影响。
        assert!(store.is_ready(2).unwrap());
    }

    #[test]
    fn embedding_rebuild_keyset_batches_visit_each_message_once() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_semantic_model("batch-model");
        for ordinal in 0..5 {
            store
                .put(
                    &StableId::native(IdKind::Message, &format!("batch-{ordinal}")),
                    b"{}",
                )
                .unwrap();
        }
        store
            .put(&sid(IdKind::Session, b"skip-session"), b"{}")
            .unwrap();
        store
            .index_embedding(&sid(IdKind::Message, b"orphan"), &[1.0, 0.0])
            .unwrap();
        let generation = store.active_generation().unwrap();
        let mut seen = Vec::new();
        let statements = counted_statements(&store, || {
            let counts = store
                .rebuild_embeddings_from_catalog("batch-model", 2, 2, |entry| {
                    assert_eq!(entry.id.kind(), IdKind::Message);
                    seen.push(entry.id.as_str().to_string());
                    Ok((!entry.id.as_str().ends_with('4')).then_some(vec![0.0, 1.0]))
                })
                .unwrap();
            assert_eq!(counts, (4, 2, 1));
        });
        // BEGIN/DELETE + four keyset reads (three batches and EOF) + four
        // inserts + generation UPDATE/COMMIT. Ignoring the batch cap fails.
        assert_eq!(statements, 12);
        assert_eq!(
            seen,
            (0..5)
                .map(|ordinal| format!("msg_v1_batch-{ordinal}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(store.query_semantic(&[0.0, 1.0], 10).unwrap().len(), 4);
        assert_eq!(store.active_generation().unwrap(), generation + 1);
        let plan: String = store.conn.borrow().query_row(
            "EXPLAIN QUERY PLAN SELECT id, payload FROM catalog WHERE id > ?1 ORDER BY id LIMIT ?2",
            rusqlite::params!["msg_v1_batch-1", 2],
            |row| row.get(3),
        ).unwrap();
        assert!(plan.contains("SEARCH") && plan.contains("id>?"), "{plan}");
    }

    #[test]
    fn embedding_rebuild_failure_rolls_back_vectors_and_generation() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_semantic_model("batch-model");
        for raw in ["a", "b"] {
            let id = StableId::native(IdKind::Message, raw);
            store.put(&id, b"{}").unwrap();
            store.index_embedding(&id, &[1.0, 0.0]).unwrap();
        }
        let generation = store.active_generation().unwrap();
        let mut calls = 0;
        let result = store.rebuild_embeddings_from_catalog("batch-model", 2, 1, |_| {
            calls += 1;
            if calls == 2 {
                Err(PortError::Backend("synthetic encoder failure".into()))
            } else {
                Ok(Some(vec![0.0, 1.0]))
            }
        });
        assert!(result.is_err());
        assert_eq!(calls, 2);
        let hits = store.query_semantic(&[1.0, 0.0], 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|hit| hit.score == 1.0));
        assert_eq!(store.active_generation().unwrap(), generation);
        for vector in [vec![f32::NAN, 0.0], vec![1.0], vec![]] {
            assert!(
                store
                    .rebuild_embeddings_from_catalog("batch-model", 2, 1, |_| Ok(Some(
                        vector.clone()
                    )))
                    .is_err()
            );
            assert_eq!(store.active_generation().unwrap(), generation);
        }
        assert!(
            store
                .rebuild_embeddings_from_catalog("batch-model", 2, 513, |_| Ok(None))
                .is_err()
        );
    }

    #[test]
    fn f32_blob_round_trips_and_drops_partial_tail() {
        let values = [1.5f32, -2.25, 0.0];
        let bytes = f32_slice_to_bytes(&values);
        assert_eq!(bytes.len(), 12);
        assert_eq!(bytes_to_f32_vec(&bytes), values);
        assert_eq!(bytes_to_f32_vec(&[0x00, 0x00, 0xC0, 0x3F]), [1.5]);
        assert!(bytes_to_f32_vec(&[]).is_empty());

        // A partial trailing float is ignored regardless of its byte length.
        for tail_len in 1..=3 {
            let mut with_partial_tail = bytes.clone();
            with_partial_tail.extend_from_slice(&[0xAA, 0xBB, 0xCC][..tail_len]);
            assert_eq!(bytes_to_f32_vec(&with_partial_tail), values);
        }

        // Truncating a complete float also drops its remaining bytes.
        assert_eq!(bytes_to_f32_vec(&bytes[..10]), values[..2]);
    }

    #[test]
    fn cosine_similarity_handles_zero_norm() {
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]), 0.0);
        assert_eq!(cosine_similarity(&[1.0, 1.0], &[0.0, 0.0]), 0.0);
        assert!((cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]) + 1.0).abs() < 1e-6);
    }

    #[test]
    fn schema_v10_creates_message_vec_table() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(SCHEMA_VERSION, 19);
        let conn = store.conn.borrow();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='message_vec'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn safe_fts_query_treats_operators_and_special_chars_as_literal_tokens() {
        // R4.1（ADR-0003）：FTS 操作符与保留字符（AND/OR/NOT/NEAR/引号/冒号/
        // 点号/连字符/星号）全部按字面量 token 包裹——输出永不含裸操作符，
        // 因此不可能触发 FTS5 语法错误或把查询解释为布尔表达式。纯标点词
        // （`*`、`-`）对 FTS 无意义，跳过。
        let cases = [
            // (输入查询, safe_fts_query 输出)
            ("AND OR NOT", "\"AND\" \"OR\" \"NOT\""),
            ("and or not", "\"and\" \"or\" \"not\""),
            ("NEAR", "\"NEAR\""),
            ("mcp.json", "\"mcp.json\""),
            ("a:b x-y", "\"a:b\" \"x-y\""),
            ("\"phrase\"", "\"\"\"phrase\"\"\""),
            // 尾随 `*` 保留在引号外才是 FTS5 前缀操作符（hstry sanitize 模式）；
            // 包进引号会被分词器当字面分隔符丢弃，前缀查询静默退化。
            ("prefix*", "\"prefix\"*"),
            ("work*", "\"work\"*"),
            ("a* b", "\"a\"* \"b\""),
            ("a**", "\"a\"*"),
            ("*a", "\"*a\""),
            ("column: value", "\"column:\" \"value\""),
            ("*", ""),
            ("* - :", ""),
            ("a - b", "\"a\" \"b\""),
            ("hello  world", "\"hello\" \"world\""),
            ("", ""),
            ("   ", ""),
        ];
        for (input, expected) in cases {
            assert_eq!(safe_fts_query(input), expected, "safe_fts_query({input:?})");
        }
        // 任何输出都不是裸操作符开头：逐词断言无 FTS 语法关键字裸露。
        for input in [
            "AND", "OR", "NOT", "NEAR", "a AND b", "NOT x", "x OR y", "a NEAR b",
        ] {
            let safe = safe_fts_query(input);
            for word in safe.split(' ') {
                assert!(
                    word.starts_with('"'),
                    "词必须被引号包裹: {input:?} -> {safe:?} (word {word:?})"
                );
            }
        }
    }

    #[test]
    fn hyphenated_query_stays_literal_not_not_operator() {
        // 回归（hstry sanitize_fts_query 修复的同一 misparse 类）：FTS5 裸查询
        // `hp-z8` 被解析成 `hp NOT z8` 并报 `no such column: z8`。字面量化后
        // 连字符不再是 NOT 操作符，含 `hp-z8` 的正文必须命中而不是语法错误。
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"hyphen-m1");
        store
            .index(&id, "fixed the hp-z8 backplane firmware")
            .unwrap();
        let hits = store.query("hp-z8", 10).unwrap();
        assert_eq!(hits.len(), 1, "hp-z8 must recall, not parse as NOT");
        assert_eq!(hits[0].id, id);
    }

    #[test]
    fn trailing_star_keeps_fts5_prefix_semantics() {
        // hstry 模式：`*` 保留在引号外才是 FTS5 前缀操作符（`"work"*` 命中
        // workstation）；包进引号（`"work*"`）会被分词器当字面分隔符丢弃，
        // 前缀查询静默退化为精确词。纯字母数字查询输出与之前逐字节一致。
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"prefix-m1");
        store.index(&id, "the workstation was rebooted").unwrap();
        let hits = store.query("work*", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, id);
        // 精确词 `work` 不命中 workstation——证明 `*` 是前缀操作符而非字面量。
        assert!(store.query("work", 10).unwrap().is_empty());
    }

    fn sqlite_failure(code: i32) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None)
    }

    fn create_v6_schema(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE catalog (id TEXT PRIMARY KEY, payload BLOB NOT NULL);
             CREATE VIRTUAL TABLE fts USING fts5(id UNINDEXED, text);
             CREATE TABLE store_metadata (
                 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                 active_generation INTEGER NOT NULL
             );
             INSERT INTO store_metadata(singleton, active_generation) VALUES(1, 1);
             CREATE TABLE index_batches (
                 operation_id TEXT PRIMARY KEY,
                 base_generation INTEGER NOT NULL,
                 target_generation INTEGER NOT NULL,
                 state TEXT NOT NULL,
                 operation_digest TEXT NOT NULL,
                 upsert_ids_json TEXT NOT NULL,
                 delete_ids_json TEXT NOT NULL,
                 durable_point TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 committed_at_ms INTEGER,
                 error_code TEXT
             );
             CREATE INDEX index_batches_state ON index_batches(state);
             CREATE TABLE fts_ids (
                 wire_id TEXT PRIMARY KEY,
                 id_json TEXT NOT NULL UNIQUE
             );
             CREATE TABLE source_membership (
                 source_path TEXT NOT NULL,
                 message_id TEXT NOT NULL,
                 document_id TEXT,
                 PRIMARY KEY(source_path, message_id)
             );
             CREATE INDEX source_membership_source
             ON source_membership(source_path);
             CREATE TABLE source_scans (
                 source_path TEXT PRIMARY KEY,
                 scanned_at_ms INTEGER NOT NULL
             );
             PRAGMA user_version = 6;",
        )
        .unwrap();
    }

    #[test]
    fn sqlite_busy_maps_to_retryable_writer_busy() {
        let error = backend(sqlite_failure(rusqlite::ffi::SQLITE_BUSY));
        assert!(matches!(
            error,
            PortError::WriterBusy(message)
                if message == "SQLite storage is busy or locked by another writer"
        ));
    }

    #[test]
    fn sqlite_locked_maps_to_retryable_writer_busy() {
        let error = backend(sqlite_failure(rusqlite::ffi::SQLITE_LOCKED));
        assert!(matches!(
            error,
            PortError::WriterBusy(message)
                if message == "SQLite storage is busy or locked by another writer"
        ));
    }

    #[test]
    fn non_contention_sqlite_failure_remains_backend() {
        let error = backend(sqlite_failure(rusqlite::ffi::SQLITE_CORRUPT));
        assert!(matches!(error, PortError::Backend(_)));
    }

    type SourceState = (u64, Vec<(String, Vec<u8>)>, Vec<(String, String)>, String);

    fn source_state(store: &SqliteStore) -> SourceState {
        let conn = store.conn.borrow();
        let catalog = {
            let mut stmt = conn
                .prepare("SELECT id, payload FROM catalog ORDER BY id")
                .unwrap();
            let rows = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap();
            rows.map(Result::unwrap).collect()
        };
        let membership = {
            let mut stmt = conn
                .prepare(
                    "SELECT source_path, message_id FROM source_membership
                     ORDER BY source_path, message_id",
                )
                .unwrap();
            let rows = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap();
            rows.map(Result::unwrap).collect()
        };
        let digest = conn
            .query_row(
                "SELECT operation_digest FROM index_batches
                 WHERE state = 'activated' ORDER BY target_generation DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        (
            store.active_generation().unwrap(),
            catalog,
            membership,
            digest,
        )
    }

    pub(crate) fn entity_entry(id: &StableId) -> (StableId, Vec<u8>, String) {
        (
            id.clone(),
            format!("payload:{}", id.as_str()).into_bytes(),
            if id.kind() == IdKind::Message {
                format!("text:{}", id.as_str())
            } else {
                String::new()
            },
        )
    }

    fn relational_message_payload(
        session_id: &StableId,
        document_id: &StableId,
        parent_id: &StableId,
        parent_native_id: &str,
        is_sidechain: bool,
        span: (u64, u64),
    ) -> Vec<u8> {
        serde_json::json!({
            "role": "user",
            "text": "shared stable body",
            "timestamp": "2026-07-28T00:00:00Z",
            "parent": parent_id.as_str(),
            "parent_native_id": parent_native_id,
            "is_sidechain": is_sidechain,
            "session": session_id.as_str(),
            "sessions": [session_id.as_str()],
            "span": { "start": span.0, "end": span.1 },
            "spans": [{
                "document": document_id.as_str(),
                "start": span.0,
                "end": span.1,
            }],
        })
        .to_string()
        .into_bytes()
    }

    fn typed_message_entry(id: &StableId, text: &str) -> (StableId, Vec<u8>, String) {
        (
            id.clone(),
            serde_json::json!({
                "role": "user",
                "text": text,
                "timestamp": "2026-07-28T00:00:00Z",
                "parent": null,
                "parent_native_id": null,
                "is_sidechain": false,
                "session": null,
                "sessions": [],
                "span": null,
                "spans": [],
            })
            .to_string()
            .into_bytes(),
            text.to_string(),
        )
    }

    fn typed_document_entry(id: &StableId) -> (StableId, Vec<u8>, String) {
        (
            id.clone(),
            serde_json::json!({
                "provider": "synthetic",
                "variant": "synthetic/jsonl-v1",
                "fingerprint": "0123456789abcdef",
                "len": 128,
            })
            .to_string()
            .into_bytes(),
            String::new(),
        )
    }

    pub(crate) fn placement(
        session_id: &StableId,
        document_id: &StableId,
        message_id: &StableId,
        source_ordinal: u32,
        is_sidechain: bool,
        span: Option<(u64, u64)>,
    ) -> MessagePlacement {
        MessagePlacement::new(
            session_id.clone(),
            document_id.clone(),
            message_id.clone(),
            source_ordinal,
            is_sidechain,
            span.map(|(start, end)| EvidenceSpan { start, end }),
        )
    }

    fn reply_edge(placement: &MessagePlacement, parent: &StableId) -> MessageEdge {
        MessageEdge {
            child_placement_id: placement.id.clone(),
            parent_message_id: parent.clone(),
            parent_native_id: Some("synthetic-parent".into()),
            relation: MessageRelation::Reply,
        }
    }

    pub(crate) fn source_batch(
        source_path: &str,
        entries: Vec<(StableId, Vec<u8>, String)>,
        placements: Vec<MessagePlacement>,
        edges: Vec<MessageEdge>,
        relation_complete: bool,
    ) -> SourceBatch {
        SourceBatch {
            source_path: source_path.into(),
            entries,
            placements,
            edges,
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
        }
    }

    fn table_count(store: &SqliteStore, table: &str) -> i64 {
        store
            .conn
            .borrow()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn projection_source(path: &str, message: &StableId, text: &str) -> SourceBatch {
        source_batch(
            path,
            vec![(
                message.clone(),
                message_payload("ses_v1_aaa", text),
                text.into(),
            )],
            vec![],
            vec![],
            true,
        )
    }

    fn cursor_timestamp_source(
        path: &str,
        message: &StableId,
        timestamp: serde_json::Value,
    ) -> SourceBatch {
        let mut source = projection_source(path, message, "timestamp upgrade body");
        let mut payload: serde_json::Value = serde_json::from_slice(&source.entries[0].1).unwrap();
        payload["timestamp"] = timestamp;
        source.entries[0].1 = serde_json::to_vec(&payload).unwrap();
        source.entries.insert(
            0,
            (
                sid(
                    IdKind::Document,
                    format!("cursor-timestamp-{path}").as_bytes(),
                ),
                serde_json::to_vec(&serde_json::json!({
                    "provider":"cursor", "variant":"cursor/vscdb-chat-v1",
                    "fingerprint":"synthetic", "len":1
                }))
                .unwrap(),
                String::new(),
            ),
        );
        source
    }

    #[test]
    fn cursor_timestamp_upgrade_requires_matching_itemtable_provenance_and_instant() {
        for (provider, variant, allowed) in [
            ("cursor", "cursor/vscdb-chat-v1", true),
            ("cursor", "cursor/disk-kv-v1", false),
            ("cline", "cline/api-history-json-v1", false),
        ] {
            for [first, second] in [["a", "b"], ["b", "a"]] {
                let store = SqliteStore::open_in_memory().unwrap();
                let message = sid(IdKind::Message, b"cursor-time-upgrade-message");
                let source = |path: &str, timestamp: &str| {
                    let mut source =
                        cursor_timestamp_source(path, &message, serde_json::json!(timestamp));
                    let mut document: serde_json::Value =
                        serde_json::from_slice(&source.entries[0].1).unwrap();
                    document["provider"] = serde_json::json!(provider);
                    document["variant"] = serde_json::json!(variant);
                    source.entries[0].1 = serde_json::to_vec(&document).unwrap();
                    source
                };
                store
                    .commit_source_batches_if_changed(&[
                        source("a", "1735689600123"),
                        source("b", "1735689600123"),
                    ])
                    .unwrap();
                store
                    .conn
                    .borrow()
                    .execute("UPDATE source_scans SET parser_version=4", [])
                    .unwrap();
                let before = store.active_generation().unwrap();
                let original = store.get(&message).unwrap();
                let old_observation =
                    SqliteStore::source_projections_for_path(&store.conn.borrow(), second).unwrap();
                for different in ["2025-01-01T00:00:00.124Z", "2025-01-01T00:00:00.123000001Z"] {
                    let error = store
                        .commit_source_batches_if_changed(&[source(first, different)])
                        .unwrap_err();
                    assert!(
                        matches!(error, PortError::Backend(ref detail) if detail.contains("timestamp"))
                    );
                    assert_eq!(store.active_generation().unwrap(), before);
                    assert_eq!(store.get(&message).unwrap(), original);
                }
                let canonical = "2025-01-01T00:00:00.123Z";
                let result = store.commit_source_batches_if_changed(&[source(first, canonical)]);
                if !allowed {
                    assert!(result.is_err(), "unproven units: {provider}/{variant}");
                    assert_eq!(store.active_generation().unwrap(), before);
                    assert_eq!(store.get(&message).unwrap(), original);
                    continue;
                }
                assert!(result.unwrap());
                let payload: serde_json::Value =
                    serde_json::from_slice(&store.get(&message).unwrap().unwrap()).unwrap();
                assert_eq!(payload["timestamp"], canonical);
                assert_eq!(
                    SqliteStore::source_projections_for_path(&store.conn.borrow(), second).unwrap(),
                    old_observation,
                    "never rewrite original source evidence"
                );
                assert!(
                    !store
                        .commit_source_batches_if_changed(&[source(first, canonical)])
                        .unwrap()
                );
                assert!(
                    store
                        .commit_source_batches_if_changed(&[source(second, canonical)])
                        .unwrap()
                );
                assert!(
                    !store
                        .commit_source_batches_if_changed(&[source(second, canonical)])
                        .unwrap()
                );
            }
        }
    }

    #[test]
    fn cursor_timestamp_upgrade_needs_each_claimants_associated_document() {
        for bad_source in ["a", "b"] {
            for missing_proof in [
                "missing_association",
                "wrong_document",
                "wrong_provider",
                "wrong_variant",
                "unrelated_document",
            ] {
                let store = SqliteStore::open_in_memory().unwrap();
                let message = sid(IdKind::Message, b"cursor-time-document-proof");
                let source = |path: &str, timestamp: &str| {
                    let mut source =
                        cursor_timestamp_source(path, &message, serde_json::json!(timestamp));
                    if path == bad_source {
                        let mut document: serde_json::Value =
                            serde_json::from_slice(&source.entries[0].1).unwrap();
                        match missing_proof {
                            "wrong_provider" => document["provider"] = serde_json::json!("cline"),
                            "wrong_variant" | "unrelated_document" => {
                                document["variant"] = serde_json::json!("cursor/disk-kv-v1")
                            }
                            _ => {}
                        }
                        if missing_proof == "unrelated_document" {
                            let mut unrelated = source.entries[0].clone();
                            unrelated.0 = sid(IdKind::Document, b"unrelated-cursor-document");
                            source.entries.push(unrelated);
                        }
                        source.entries[0].1 = serde_json::to_vec(&document).unwrap();
                    }
                    source
                };
                store
                    .commit_source_batches_if_changed(&[source("a", "1000"), source("b", "1000")])
                    .unwrap();
                if matches!(missing_proof, "missing_association" | "wrong_document") {
                    let wrong = sid(IdKind::Document, b"missing-cursor-document");
                    let document = (missing_proof == "wrong_document").then_some(wrong.as_str());
                    store.conn.borrow().execute(
                        "UPDATE source_membership SET document_id=?1 WHERE source_path=?2 AND message_id=?3",
                        rusqlite::params![document, bad_source, message.as_str()],
                    ).unwrap();
                }
                let good_source = if bad_source == "a" { "b" } else { "a" };
                let before = store.active_generation().unwrap();
                let original = store.get(&message).unwrap();
                let evidence =
                    SqliteStore::source_projections_for_path(&store.conn.borrow(), bad_source)
                        .unwrap();
                let error = store
                    .commit_source_batches_if_changed(&[source(
                        good_source,
                        "1970-01-01T00:00:01.000Z",
                    )])
                    .unwrap_err();
                assert!(
                    matches!(error, PortError::Backend(ref detail) if detail.contains("timestamp")),
                    "{missing_proof}"
                );
                assert_eq!(store.active_generation().unwrap(), before);
                assert_eq!(store.get(&message).unwrap(), original);
                assert_eq!(
                    SqliteStore::source_projections_for_path(&store.conn.borrow(), bad_source)
                        .unwrap(),
                    evidence
                );
            }
        }
    }

    #[test]
    fn cursor_timestamp_upgrade_checks_all_non_null_originals_before_folding() {
        for updated in ["a", "c"] {
            let store = SqliteStore::open_in_memory().unwrap();
            let message = sid(IdKind::Message, b"cursor-time-null-fold");
            let source = |path, timestamp| cursor_timestamp_source(path, &message, timestamp);
            store
                .commit_source_batches_if_changed(&[
                    source("a", serde_json::json!("1000")),
                    source("b", serde_json::Value::Null),
                    source("c", serde_json::json!("1000")),
                ])
                .unwrap();
            let before = store.active_generation().unwrap();
            let original = store.get(&message).unwrap();
            let old_path = if updated == "a" { "c" } else { "a" };
            let old_evidence =
                SqliteStore::source_projections_for_path(&store.conn.borrow(), old_path).unwrap();
            let null_evidence =
                SqliteStore::source_projections_for_path(&store.conn.borrow(), "b").unwrap();
            for different in ["1970-01-01T00:00:02.000Z", "1970-01-01T00:00:01.000000001Z"] {
                assert!(
                    store
                        .commit_source_batches_if_changed(&[source(
                            updated,
                            serde_json::json!(different)
                        )])
                        .is_err()
                );
                assert_eq!(store.active_generation().unwrap(), before);
                assert_eq!(store.get(&message).unwrap(), original);
            }
            // An equivalent offset spelling is the same instant; null still converges.
            let canonical = "1970-01-01T01:00:01.000000000+01:00";
            assert!(
                store
                    .commit_source_batches_if_changed(&[source(
                        updated,
                        serde_json::json!(canonical)
                    )])
                    .unwrap()
            );
            let payload: serde_json::Value =
                serde_json::from_slice(&store.get(&message).unwrap().unwrap()).unwrap();
            assert!(payload["timestamp"].is_null());
            assert_eq!(
                SqliteStore::source_projections_for_path(&store.conn.borrow(), old_path).unwrap(),
                old_evidence
            );
            assert_eq!(
                SqliteStore::source_projections_for_path(&store.conn.borrow(), "b").unwrap(),
                null_evidence
            );
            assert!(
                !store
                    .commit_source_batches_if_changed(&[source(
                        updated,
                        serde_json::json!(canonical)
                    )])
                    .unwrap()
            );
            assert!(
                store
                    .commit_source_batches_if_changed(&[source_batch(
                        "b",
                        vec![],
                        vec![],
                        vec![],
                        true
                    )])
                    .unwrap()
            );
            let payload: serde_json::Value =
                serde_json::from_slice(&store.get(&message).unwrap().unwrap()).unwrap();
            assert_eq!(payload["timestamp"], canonical);
        }
    }

    #[test]
    fn cursor_timestamp_upgrade_removal_restores_only_surviving_observations() {
        for updated in ["a", "c"] {
            let store = SqliteStore::open_in_memory().unwrap();
            let message = sid(IdKind::Message, b"cursor-time-removal");
            let source = |path, timestamp| {
                cursor_timestamp_source(path, &message, serde_json::json!(timestamp))
            };
            store
                .commit_source_batches_if_changed(&[
                    source("a", "-1"),
                    source("b", "-1"),
                    source("c", "-1"),
                ])
                .unwrap();
            let survivor = if updated == "a" { "c" } else { "a" };
            let old_evidence =
                SqliteStore::source_projections_for_path(&store.conn.borrow(), survivor).unwrap();
            let canonical = "1969-12-31T23:59:59.999Z";
            assert!(
                store
                    .commit_source_batches_if_changed(&[source(updated, canonical)])
                    .unwrap()
            );
            let timestamp = || {
                let payload: serde_json::Value =
                    serde_json::from_slice(&store.get(&message).unwrap().unwrap()).unwrap();
                payload["timestamp"].clone()
            };
            assert_eq!(timestamp(), canonical);
            // An incomplete scan is not a deletion of the RFC3339 observation.
            assert!(
                store
                    .commit_source_batches_if_changed(&[source_batch(
                        updated,
                        vec![],
                        vec![],
                        vec![],
                        false
                    )])
                    .unwrap()
            );
            assert_eq!(timestamp(), canonical);
            assert!(
                store
                    .commit_source_batches_if_changed(&[source_batch(
                        updated,
                        vec![],
                        vec![],
                        vec![],
                        true
                    )])
                    .unwrap()
            );
            assert_eq!(timestamp(), "-1");
            assert!(
                SqliteStore::source_projections_for_path(&store.conn.borrow(), updated)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                SqliteStore::source_projections_for_path(&store.conn.borrow(), survivor).unwrap(),
                old_evidence
            );
            assert!(
                !store
                    .commit_source_batches_if_changed(&[source_batch(
                        updated,
                        vec![],
                        vec![],
                        vec![],
                        true
                    )])
                    .unwrap()
            );
        }
    }

    #[test]
    fn cursor_timestamp_upgrade_does_not_relax_other_intrinsic_fields() {
        for field in ["role", "intrinsic_marker"] {
            let store = SqliteStore::open_in_memory().unwrap();
            let message = sid(IdKind::Message, b"cursor-time-intrinsic");
            let source = |path, timestamp| {
                cursor_timestamp_source(path, &message, serde_json::json!(timestamp))
            };
            store
                .commit_source_batches_if_changed(&[source("a", "1000"), source("b", "1000")])
                .unwrap();
            let before = store.active_generation().unwrap();
            let original = store.get(&message).unwrap();
            let mut changed = source("a", "1970-01-01T00:00:01.000Z");
            let payload = changed
                .entries
                .iter_mut()
                .find(|(id, _, _)| id == &message)
                .unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&payload.1).unwrap();
            value[field] = serde_json::json!("different");
            payload.1 = serde_json::to_vec(&value).unwrap();
            let error = store
                .commit_source_batches_if_changed(&[changed])
                .unwrap_err();
            assert!(matches!(error, PortError::Backend(ref detail) if detail.contains(field)));
            assert_eq!(store.active_generation().unwrap(), before);
            assert_eq!(store.get(&message).unwrap(), original);
        }
    }

    #[test]
    fn source_projection_latest_shorter_and_equal_length() {
        for replacement in ["new", "different-long-body"] {
            let store = SqliteStore::open_in_memory().unwrap();
            let message = sid(IdKind::Message, b"projection-latest");
            store
                .commit_source_batches_if_changed(&[projection_source(
                    "a",
                    &message,
                    "original-long-body!",
                )])
                .unwrap();
            store
                .commit_source_batches_if_changed(&[projection_source("a", &message, replacement)])
                .unwrap();
            let payload = store.get(&message).unwrap().unwrap();
            assert_eq!(searchable_text(&payload), replacement);
        }
    }

    #[test]
    fn source_projection_removal_reveals_surviving_observation() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"projection-remove");
        store
            .commit_source_batches_if_changed(&[
                projection_source("a", &message, "long winning original"),
                projection_source("b", &message, "short"),
            ])
            .unwrap();
        store
            .commit_source_batches_if_changed(&[source_batch("a", vec![], vec![], vec![], true)])
            .unwrap();
        assert_eq!(
            searchable_text(&store.get(&message).unwrap().unwrap()),
            "short"
        );
        assert!(store.query("winning", 10).unwrap().is_empty());
        store
            .commit_source_batches_if_changed(&[source_batch("b", vec![], vec![], vec![], true)])
            .unwrap();
        assert!(store.get(&message).unwrap().is_none());
    }

    #[test]
    fn source_projection_batch_order_and_incomplete_retention() {
        let message = sid(IdKind::Message, b"projection-order");
        let mut results = Vec::new();
        for separate in [false, true] {
            for reverse in [false, true] {
                let store = SqliteStore::open_in_memory().unwrap();
                let mut sources = vec![
                    projection_source("a", &message, "original-long"),
                    projection_source("b", &message, "short"),
                ];
                if reverse {
                    sources.reverse();
                }
                if separate {
                    for source in sources {
                        store.commit_source_batches_if_changed(&[source]).unwrap();
                    }
                } else {
                    store.commit_source_batches_if_changed(&sources).unwrap();
                }
                store
                    .commit_source_batches_if_changed(&[projection_source("a", &message, "new")])
                    .unwrap();
                store
                    .commit_source_batches_if_changed(&[source_batch(
                        "b",
                        vec![],
                        vec![],
                        vec![],
                        false,
                    )])
                    .unwrap();
                results.push(store.get(&message).unwrap().unwrap());
                assert_eq!(searchable_text(results.last().unwrap()), "short");
            }
        }
        assert!(results.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn source_projection_evidence_changes_even_when_aggregate_does_not() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"projection-evidence");
        store
            .commit_source_batches_if_changed(&[
                projection_source("a", &message, "winning-original"),
                projection_source("b", &message, "short"),
            ])
            .unwrap();
        let correction = projection_source("b", &message, "tiny");
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&correction))
                .unwrap()
        );
        assert!(
            !store
                .commit_source_batches_if_changed(&[correction])
                .unwrap()
        );
        store
            .commit_source_batches_if_changed(&[source_batch("a", vec![], vec![], vec![], true)])
            .unwrap();
        assert_eq!(
            searchable_text(&store.get(&message).unwrap().unwrap()),
            "tiny"
        );
    }

    #[test]
    fn source_projection_legacy_migration_reingest_and_incomplete_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.sqlite");
        let message = sid(IdKind::Message, b"legacy-projection");
        let first = projection_source("a", &message, "historical-long");
        let second = projection_source("b", &message, "short");
        let old_payload;
        {
            let store = SqliteStore::open_for_write(path.to_str().unwrap()).unwrap();
            store
                .commit_source_batches_if_changed(&[first.clone(), second.clone()])
                .unwrap();
            old_payload = store.get(&message).unwrap().unwrap();
            // Exact pre-v19 state: no original projection table existed.
            store
                .conn
                .borrow()
                .execute_batch("DROP TABLE source_entity_projections; PRAGMA user_version=18;")
                .unwrap();
        }
        assert!(matches!(
            SqliteStore::open(path.to_str().unwrap()),
            Err(PortError::SchemaIncompatible(_))
        ));
        {
            let store = SqliteStore::open_for_write(path.to_str().unwrap()).unwrap();
            assert_eq!(table_count(&store, "source_entity_projections"), 0);
            assert_eq!(store.get(&message).unwrap().unwrap(), old_payload);
            // Parser version is already current: evidence, not the version,
            // must invalidate BOTH no-op paths.
            assert!(!store.sources_are_current(&[&first]).unwrap());
            assert!(
                store
                    .commit_source_batches_if_changed(&[projection_source("a", &message, "new")])
                    .unwrap()
            );
            assert_eq!(store.get(&message).unwrap().unwrap(), old_payload);
            assert_eq!(table_count(&store, "source_entity_projections"), 1);
            assert!(
                store
                    .commit_source_batches_if_changed(&[source_batch(
                        "b",
                        vec![],
                        vec![],
                        vec![],
                        false
                    )])
                    .unwrap()
            );
            assert_eq!(table_count(&store, "source_entity_projections"), 1);
            assert_eq!(store.get(&message).unwrap().unwrap(), old_payload);
            assert!(
                store
                    .commit_source_batches_if_changed(std::slice::from_ref(&second))
                    .unwrap()
            );
            assert_eq!(
                searchable_text(&store.get(&message).unwrap().unwrap()),
                "short"
            );
            assert!(
                !store
                    .commit_source_batches_if_changed(std::slice::from_ref(&second))
                    .unwrap()
            );
        }
        let store = SqliteStore::open(path.to_str().unwrap()).unwrap();
        assert_eq!(table_count(&store, "source_entity_projections"), 2);
        assert_eq!(
            searchable_text(&store.get(&message).unwrap().unwrap()),
            "short"
        );
    }

    #[test]
    fn source_projection_migration_rolls_back_table_and_version_on_failure() {
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.conn.borrow();
        conn.execute_batch(
            "DROP TABLE source_entity_projections; PRAGMA user_version=18;
            CREATE TABLE source_entity_projections_entity (block INTEGER);",
        )
        .unwrap();
        assert!(SqliteStore::migrate(&conn).is_err());
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            18
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name='source_entity_projections'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn source_projection_phase_two_failure_retains_old_evidence_and_catalog() {
        for trigger in [
            "CREATE TRIGGER reject_projection BEFORE INSERT ON source_entity_projections BEGIN SELECT RAISE(ABORT, 'synthetic'); END;",
            "CREATE TRIGGER reject_activation BEFORE UPDATE ON index_batches WHEN NEW.state='activated' BEGIN SELECT RAISE(ABORT, 'synthetic'); END;",
        ] {
            let store = SqliteStore::open_in_memory().unwrap();
            let message = sid(IdKind::Message, b"projection-rollback");
            store
                .commit_source_batches_if_changed(&[projection_source("a", &message, "original")])
                .unwrap();
            let before = store.get(&message).unwrap();
            let evidence =
                SqliteStore::source_projections_for_path(&store.conn.borrow(), "a").unwrap();
            let generation = store.active_generation().unwrap();
            store.conn.borrow().execute_batch(trigger).unwrap();
            assert!(
                store
                    .commit_source_batches_if_changed(&[projection_source("a", &message, "new")])
                    .is_err()
            );
            assert_eq!(store.active_generation().unwrap(), generation);
            assert_eq!(store.get(&message).unwrap(), before);
            assert_eq!(
                SqliteStore::source_projections_for_path(&store.conn.borrow(), "a").unwrap(),
                evidence
            );
            assert_eq!(store.query("original", 10).unwrap().len(), 1);
            assert!(store.query("new", 10).unwrap().is_empty());
            assert_eq!(store.interrupted_batch_count().unwrap(), 1);
            assert_eq!(store.recover_interrupted().unwrap(), 1);
        }
    }

    #[test]
    fn source_projection_manifest_descriptor_size_is_independent_of_body_size() {
        let message = sid(IdKind::Message, b"descriptor-size");
        let mut sizes = Vec::new();
        for repeats in [1, 16_384] {
            let store = SqliteStore::open_in_memory().unwrap();
            let text = "synthetic-private-body".repeat(repeats);
            let source = projection_source("a", &message, &text);
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap();
            let batch = latest_index_batch(&store);
            let descriptor = &batch.source_replacements[0]["projections"][message.as_str()];
            let serialized = serde_json::to_string(descriptor).unwrap();
            assert!(
                serialized.len() < 512,
                "descriptor expanded to {} bytes",
                serialized.len()
            );
            assert!(!serialized.contains("synthetic-private-body"));
            assert_eq!(descriptor["id"], serde_json::to_value(&message).unwrap());
            assert_eq!(
                descriptor["payload_blake3"],
                blake3::hash(&source.entries[0].1).to_hex().to_string()
            );
            assert_eq!(
                descriptor["text_blake3"],
                blake3::hash(text.as_bytes()).to_hex().to_string()
            );
            sizes.push(serialized.len());
            let evidence =
                SqliteStore::source_projections_for_path(&store.conn.borrow(), "a").unwrap();
            assert_eq!(evidence[message.as_str()], source.entries[0]);
        }
        assert_eq!(sizes[0], sizes[1]);
    }

    #[test]
    fn source_projection_manifest_seals_original_payload_text_and_identity() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"projection-seal");
        let entry = projection_source("a", &message, "original")
            .entries
            .remove(0);
        let relations = RelationManifests {
            source_replacements: vec![SourceReplacementManifest {
                source_path: "a".into(),
                entity_memberships: vec![SourceEntityMembershipManifest {
                    entity_id: message.as_str().into(),
                    document_id: None,
                }],
                projections: BTreeMap::from([(message.as_str().into(), entry)]),
                placement_ids: vec![],
                activity_ids: vec![],
                usage_ids: vec![],
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: vec![],
                installation: None,
            }],
            ..RelationManifests::default()
        };
        let manifest = batch_manifest(&[], &[], &relations).unwrap();
        let pending = store
            .begin_index_batch_with_relations(&[], &[], &relations)
            .unwrap();
        for field in 0..3 {
            let mut tampered = relations.clone();
            let entry = tampered.source_replacements[0]
                .projections
                .values_mut()
                .next()
                .unwrap();
            match field {
                0 => entry.0 = StableId::from_wire(message.as_str()).unwrap(),
                1 => entry.1 = b"tampered".to_vec(),
                _ => entry.2 = "tampered".into(),
            }
            assert_ne!(
                manifest.operation_digest,
                batch_manifest(&[], &[], &tampered)
                    .unwrap()
                    .operation_digest,
                "changing source identity, payload, or text must change the sealed digest"
            );
            let tampered_json = tampered.canonical_json().unwrap().2;
            store
                .conn
                .borrow()
                .execute(
                    "UPDATE index_batches SET source_replacements_json=?1 WHERE operation_id=?2",
                    rusqlite::params![tampered_json, pending.operation_id],
                )
                .unwrap();
            assert!(
                store
                    .commit_index_batch_with_relations(&pending, &[], &[], &relations, &manifest)
                    .is_err()
            );
            assert_eq!(store.active_generation().unwrap(), 0);
            assert_eq!(table_count(&store, "source_entity_projections"), 0);
        }
    }

    #[test]
    fn source_projection_alias_removal_uses_before_candidates() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"projection-alias");
        let session = sid(IdKind::Session, b"projection-session");
        let docs = [
            sid(IdKind::Document, b"projection-doc-a"),
            sid(IdKind::Document, b"projection-doc-b"),
        ];
        let sources: Vec<_> = docs
            .iter()
            .enumerate()
            .map(|(n, doc)| {
                source_batch(
                    if n == 0 { "a" } else { "b" },
                    vec![
                        typed_message_entry(&message, "body"),
                        (
                            session.clone(),
                            session_payload(doc.as_str(), &[message.as_str()]),
                            String::new(),
                        ),
                        typed_document_entry(doc),
                    ],
                    vec![placement(
                        &session,
                        doc,
                        &message,
                        0,
                        false,
                        Some((n as u64, n as u64 + 1)),
                    )],
                    vec![],
                    true,
                )
            })
            .collect();
        store.commit_source_batches_if_changed(&sources).unwrap();
        store
            .commit_source_batches_if_changed(&[source_batch("a", vec![], vec![], vec![], true)])
            .unwrap();
        let payload: serde_json::Value =
            serde_json::from_slice(&store.get(&message).unwrap().unwrap()).unwrap();
        assert_eq!(payload["spans"].as_array().unwrap().len(), 1);
        assert_eq!(payload["spans"][0]["document"], docs[1].as_str());
        let session_payload: serde_json::Value =
            serde_json::from_slice(&store.get(&session).unwrap().unwrap()).unwrap();
        assert_eq!(
            session_payload["documents"],
            serde_json::json!([docs[1].as_str()])
        );
    }

    #[test]
    fn source_projection_final_text_change_invalidates_vectors_atomically() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"projection-vector");
        store.set_semantic_model("synthetic-model");
        store
            .commit_source_batches_if_changed(&[
                projection_source("a", &message, "winning-original"),
                projection_source("b", &message, "short"),
            ])
            .unwrap();
        store.index_embedding(&message, &[1.0, 0.0]).unwrap();
        store
            .commit_source_batches_if_changed(&[projection_source("b", &message, "tiny")])
            .unwrap();
        assert_eq!(
            table_count(&store, "message_vec"),
            1,
            "evidence-only change keeps valid embedding"
        );
        store.conn.borrow().execute_batch("CREATE TRIGGER reject_vector_activation BEFORE UPDATE ON index_batches WHEN NEW.state='activated' BEGIN SELECT RAISE(ABORT, 'synthetic'); END;").unwrap();
        let remove = source_batch("a", vec![], vec![], vec![], true);
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&remove))
                .is_err()
        );
        assert_eq!(table_count(&store, "message_vec"), 1);
        store
            .conn
            .borrow()
            .execute_batch("DROP TRIGGER reject_vector_activation;")
            .unwrap();
        store.commit_source_batches_if_changed(&[remove]).unwrap();
        assert_eq!(
            table_count(&store, "message_vec"),
            0,
            "changed final text must retire its embedding"
        );
        store.index_embedding(&message, &[1.0, 0.0]).unwrap();
        store
            .commit_source_batches_if_changed(&[source_batch("b", vec![], vec![], vec![], true)])
            .unwrap();
        assert_eq!(
            table_count(&store, "message_vec"),
            0,
            "last source deletion removes orphan embedding"
        );
    }

    #[test]
    fn source_projection_same_source_intrinsic_correction_and_missing_legacy_refusal() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"projection-intrinsic");
        let mut source = projection_source("a", &message, "body");
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();
        let mut payload: serde_json::Value = serde_json::from_slice(&source.entries[0].1).unwrap();
        payload["role"] = serde_json::json!("assistant");
        source.entries[0].1 = serde_json::to_vec(&payload).unwrap();
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap()
        );
        assert_eq!(store.get(&message).unwrap().unwrap(), source.entries[0].1);
        store
            .commit_source_batches_if_changed(&[projection_source("b", &message, "body")])
            .unwrap_err();
        // A legacy claimant without either evidence or an aggregate cannot be
        // guessed. The refusal must happen before creating a durable intent.
        store
            .conn
            .borrow()
            .execute_batch("DELETE FROM source_entity_projections; DELETE FROM catalog;")
            .unwrap();
        let generation = store.active_generation().unwrap();
        let error = store
            .commit_source_batches_if_changed(&[source_batch("a", vec![], vec![], vec![], false)])
            .unwrap_err();
        assert!(matches!(error, PortError::SchemaIncompatible(_)));
        assert_eq!(store.active_generation().unwrap(), generation);
        assert_eq!(store.interrupted_batch_count().unwrap(), 0);
    }

    #[test]
    fn review_incomplete_scan_preserves_generated_aliases_but_updates_intrinsics() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"review-incomplete");
        let session = sid(IdKind::Session, b"review-incomplete-session");
        let document = sid(IdKind::Document, b"review-incomplete-doc");
        let source = source_batch(
            "a",
            vec![
                typed_message_entry(&message, "original long body"),
                (
                    session.clone(),
                    session_payload(document.as_str(), &[message.as_str()]),
                    String::new(),
                ),
                typed_document_entry(&document),
            ],
            vec![placement(
                &session,
                &document,
                &message,
                0,
                false,
                Some((3, 8)),
            )],
            vec![],
            true,
        );
        store.commit_source_batches_if_changed(&[source]).unwrap();
        let before = store.get(&message).unwrap().unwrap();
        assert!(serde_json::from_slice::<serde_json::Value>(&before).unwrap()["spans"][0]["placement_id"].is_string());
        let empty = source_batch("a", vec![], vec![], vec![], false);
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&empty))
            .unwrap();
        assert_eq!(store.get(&message).unwrap().unwrap(), before);
        assert!(!store.commit_source_batches_if_changed(&[empty]).unwrap());
        let changed = source_batch(
            "a",
            vec![typed_message_entry(&message, "new")],
            vec![],
            vec![],
            false,
        );
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&changed))
            .unwrap();
        let mut expected: serde_json::Value = serde_json::from_slice(&before).unwrap();
        expected["text"] = serde_json::json!("new");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&store.get(&message).unwrap().unwrap())
                .unwrap(),
            expected
        );
        assert_eq!(store.query("new", 10).unwrap().len(), 1);
        assert!(store.query("original", 10).unwrap().is_empty());
        assert!(!store.commit_source_batches_if_changed(&[changed]).unwrap());
    }

    #[test]
    fn review_legacy_unknown_claimant_repeat_and_removal_converge() {
        for repeat in [false, true] {
            let store = SqliteStore::open_in_memory().unwrap();
            let message = sid(IdKind::Message, b"review-legacy");
            let mut source = projection_source("a", &message, "body");
            store
                .commit_source_batches_if_changed(&[
                    source.clone(),
                    projection_source("b", &message, "body"),
                ])
                .unwrap();
            store
                .conn
                .borrow()
                .execute("DELETE FROM source_entity_projections", [])
                .unwrap();
            let before = store.get(&message).unwrap();
            let mut payload: serde_json::Value =
                serde_json::from_slice(&source.entries[0].1).unwrap();
            payload["role"] = serde_json::json!("assistant");
            source.entries[0].1 = serde_json::to_vec(&payload).unwrap();
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap();
            assert_eq!(store.get(&message).unwrap(), before);
            if repeat {
                assert!(
                    !store
                        .commit_source_batches_if_changed(std::slice::from_ref(&source))
                        .unwrap()
                );
            }
            store
                .commit_source_batches_if_changed(&[source_batch(
                    "b",
                    vec![],
                    vec![],
                    vec![],
                    true,
                )])
                .unwrap();
            assert_eq!(store.get(&message).unwrap().unwrap(), source.entries[0].1);
            assert!(!store.commit_source_batches_if_changed(&[source]).unwrap());
        }
    }

    #[test]
    fn review_unscoped_mutations_reject_source_owned_entities_before_intent() {
        for path in [
            "put",
            "batch",
            "begin-upsert",
            "begin-delete",
            "commit-upsert",
            "commit-delete",
        ] {
            let store = SqliteStore::open_in_memory().unwrap();
            let message = sid(IdKind::Message, b"review-owned");
            let source = projection_source("a", &message, "original");
            let edit = projection_source("a", &message, "unscoped").entries;
            // Public phase-2 must recheck ownership acquired after intent creation.
            let pending = if path == "commit-upsert" {
                Some(store.begin_index_batch(&edit, &[]).unwrap())
            } else if path == "commit-delete" {
                Some(
                    store
                        .begin_index_batch(&[], std::slice::from_ref(&message))
                        .unwrap(),
                )
            } else {
                None
            };
            store.commit_source_batches_if_changed(&[source]).unwrap();
            store.set_semantic_model("source-owned-model");
            store.index_embedding(&message, &[1.0, 0.0]).unwrap();
            let vectors = vector_rows(&store);
            let before = store.get(&message).unwrap();
            let evidence =
                SqliteStore::source_projections_for_path(&store.conn.borrow(), "a").unwrap();
            let generation = store.active_generation().unwrap();
            let intents = table_count(&store, "index_batches");
            let result = match path {
                "put" => store.put(&message, &edit[0].1),
                "batch" => store.commit_batch_if_changed(&edit).map(|_| ()),
                "begin-upsert" => store.begin_index_batch(&edit, &[]).map(|_| ()),
                "begin-delete" => store
                    .begin_index_batch(&[], std::slice::from_ref(&message))
                    .map(|_| ()),
                "commit-upsert" => store.commit_index_batch(pending.as_ref().unwrap(), &edit, &[]),
                _ => store.commit_index_batch(
                    pending.as_ref().unwrap(),
                    &[],
                    std::slice::from_ref(&message),
                ),
            };
            assert!(
                matches!(result, Err(PortError::InvalidRequest(_))),
                "{path}: {result:?}"
            );
            assert_eq!(store.get(&message).unwrap(), before);
            assert_eq!(vector_rows(&store), vectors);
            assert_eq!(
                SqliteStore::source_projections_for_path(&store.conn.borrow(), "a").unwrap(),
                evidence
            );
            assert_eq!(store.query("original", 10).unwrap().len(), 1);
            assert!(store.query("unscoped", 10).unwrap().is_empty());
            assert_eq!(store.active_generation().unwrap(), generation);
            assert_eq!(table_count(&store, "index_batches"), intents);
        }
    }

    #[test]
    fn source_no_op_rejects_duplicate_facts_without_advancing_generation() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"noop-message");
        let session = sid(IdKind::Session, b"noop-session");
        let document = sid(IdKind::Document, b"noop-document");
        let p = placement(&session, &document, &message, 0, false, None);
        let mut batch = source_batch(
            "noop.jsonl",
            vec![
                typed_message_entry(&message, "no-op body"),
                (
                    session.clone(),
                    br#"{"documents":[],"messages":[]}"#.to_vec(),
                    String::new(),
                ),
                typed_document_entry(&document),
            ],
            vec![p],
            Vec::new(),
            true,
        );
        batch
            .resume_claims
            .push(SourceResumeClaim::from_observation(
                "claude-code",
                session.as_str(),
                &agent_session_grep_ports::ProviderSessionObservation::default(),
            ));
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch))
            .unwrap();
        assert!(store.sources_are_current(&[&batch]).unwrap());
        let generation = store.active_generation().unwrap();
        for duplicate_placement in [false, true] {
            let mut invalid = batch.clone();
            if duplicate_placement {
                invalid.placements.push(invalid.placements[0].clone());
            } else {
                invalid.entries.push(invalid.entries[0].clone());
            }
            assert!(store.commit_source_batches_if_changed(&[invalid]).is_err());
            assert_eq!(store.active_generation().unwrap(), generation);
        }
        let mut duplicate_claim = batch.clone();
        duplicate_claim
            .resume_claims
            .push(duplicate_claim.resume_claims[0].clone());
        assert!(store.sources_are_current(&[&duplicate_claim]).unwrap());
        assert!(
            store
                .commit_source_batches_if_changed(&[duplicate_claim])
                .is_err()
        );
        assert_eq!(store.active_generation().unwrap(), generation);
        for claim_session in [message.as_str(), "ses_v1_foreign-session"] {
            let mut invalid = batch.clone();
            invalid.resume_claims[0].session_id = claim_session.to_owned();
            assert!(store.commit_source_batches_if_changed(&[invalid]).is_err());
            assert_eq!(store.active_generation().unwrap(), generation);
        }
        assert!(!store.commit_source_batches_if_changed(&[batch]).unwrap());
    }

    fn source_placement_claims(store: &SqliteStore, source_path: &str) -> Vec<String> {
        let conn = store.conn.borrow();
        let mut stmt = conn
            .prepare(
                "SELECT placement_id FROM source_placement_membership
                 WHERE source_path = ?1 ORDER BY placement_id",
            )
            .unwrap();
        stmt.query_map([source_path], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn relation_complete_marker(store: &SqliteStore, source_path: &str) -> bool {
        store
            .conn
            .borrow()
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM source_relation_scans WHERE source_path = ?1
                 )",
                [source_path],
                |row| row.get(0),
            )
            .unwrap()
    }

    thread_local! {
        static TRACED_STATEMENTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// 在该 store 的连接上挂 `SQLITE_TRACE_STMT` 钩子执行 `run`,返回期间执行的
    /// SQL 语句总数(prepare 不计;同一 prepared statement 每次执行都计一条)。
    /// 钩子是每连接独立的,测试各跑各的线程,互不干扰。
    pub(crate) fn counted_statements<R>(store: &SqliteStore, run: impl FnOnce() -> R) -> usize {
        TRACED_STATEMENTS.with(|cell| cell.set(0));
        {
            let conn = store.conn.borrow();
            conn.trace_v2(
                rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
                Some(count_user_statement),
            );
        }
        let result = run();
        {
            let conn = store.conn.borrow();
            conn.trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
        }
        let _ = result;
        TRACED_STATEMENTS.with(|cell| cell.get())
    }

    /// `counted_statements` 的 trace 回调：只计用户层语句。SQLite 内部语句
    /// （FTS5 影子表查询、`PRAGMA data_version` 等）以 `--` 开头，不计入，
    /// 否则"pushdown 保持单语句"这类断言会被虚拟表内部执行数污染。
    fn count_user_statement(event: rusqlite::trace::TraceEvent<'_>) {
        match event {
            rusqlite::trace::TraceEvent::Stmt(_stmt, sql) if !sql.starts_with("--") => {
                TRACED_STATEMENTS.with(|cell| cell.set(cell.get() + 1));
            }
            _ => {}
        }
    }

    /// 大会话源批次:chain_len 条链式消息(除首条外每条带边指向前一条)+
    /// orphan_count 条孤儿父消息(Native 身份,入库但无出现,由带出现的子消息
    /// 指向),外加会话与文档条目。返回批次与孤儿父消息 id(供等级断言)。
    fn chain_source_batch(
        source_path: &str,
        session: &StableId,
        document: &StableId,
        chain_len: usize,
        orphan_count: usize,
    ) -> (SourceBatch, Vec<StableId>) {
        let mut entries = Vec::new();
        let mut placements = Vec::new();
        let mut edges = Vec::new();
        let chain_messages: Vec<StableId> = (0..chain_len)
            .map(|index| {
                sid(
                    IdKind::Message,
                    format!("{source_path}-chain-{index}").as_bytes(),
                )
            })
            .collect();
        for (index, message) in chain_messages.iter().enumerate() {
            entries.push(typed_message_entry(message, &format!("chain body {index}")));
            let placement = placement(
                session,
                document,
                message,
                index as u32,
                false,
                Some((0, 4)),
            );
            if index > 0 {
                edges.push(reply_edge(&placement, &chain_messages[index - 1]));
            }
            placements.push(placement);
        }
        let orphans: Vec<StableId> = (0..orphan_count)
            .map(|index| {
                StableId::native(IdKind::Message, &format!("{source_path}-orphan-{index}"))
            })
            .collect();
        for (index, orphan) in orphans.iter().enumerate() {
            entries.push(typed_message_entry(orphan, &format!("orphan body {index}")));
            let child = sid(
                IdKind::Message,
                format!("{source_path}-orphan-child-{index}").as_bytes(),
            );
            entries.push(typed_message_entry(
                &child,
                &format!("orphan child body {index}"),
            ));
            let child_placement = placement(
                session,
                document,
                &child,
                (chain_len + index) as u32,
                false,
                Some((0, 4)),
            );
            edges.push(reply_edge(&child_placement, orphan));
            placements.push(child_placement);
        }
        let placed_ids: Vec<&str> = placements
            .iter()
            .map(|placement| placement.message_id.as_str())
            .collect();
        entries.push((
            session.clone(),
            session_payload(document.as_str(), &placed_ids),
            String::new(),
        ));
        entries.push(typed_document_entry(document));
        (
            source_batch(source_path, entries, placements, edges, true),
            orphans,
        )
    }

    fn latest_index_batch(store: &SqliteStore) -> IndexBatch {
        let operation_id: String = store
            .conn
            .borrow()
            .query_row(
                "SELECT operation_id FROM index_batches
                 ORDER BY target_generation DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        store.index_batch(&operation_id).unwrap().unwrap()
    }

    #[test]
    fn catalog_put_get_roundtrip() {
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Session, b"s1");
        assert!(store.get(&id).unwrap().is_none());
        store.put(&id, b"hello payload").unwrap();
        assert_eq!(store.get(&id).unwrap().unwrap(), b"hello payload");
    }

    #[test]
    fn catalog_put_overwrites() {
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Session, b"s1");
        store.put(&id, b"first").unwrap();
        store.put(&id, b"second").unwrap();
        assert_eq!(store.get(&id).unwrap().unwrap(), b"second");
    }

    #[test]
    fn catalog_put_advances_generation() {
        // put 是内容变更：推进 generation，使此前签发的 search/list 游标在此变更
        // 后失效（游标绑定 generation 的 CAS 契约，与批量提交一致）。
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(store.active_generation().unwrap(), 0);
        let id = sid(IdKind::Session, b"s1");
        store.put(&id, b"payload").unwrap();
        assert_eq!(store.active_generation().unwrap(), 1);
        store.put(&id, b"updated").unwrap();
        assert_eq!(store.active_generation().unwrap(), 2);
        // SearchIndex::index 同样是索引写入，推进 generation。
        let message = sid(IdKind::Message, b"m1");
        store.index(&message, "needle").unwrap();
        assert_eq!(store.active_generation().unwrap(), 3);
    }

    #[test]
    fn catalog_get_many_preserves_order_and_misses() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"gma");
        let b = sid(IdKind::Message, b"gmb");
        let missing = sid(IdKind::Message, b"missing");
        store.put(&a, b"payload-a").unwrap();
        store.put(&b, b"payload-b").unwrap();
        // 乱序请求：结果必须与请求同序（保序契约），目录中不存在的 id → None。
        let got = store
            .get_many(&[b.clone(), missing.clone(), a.clone()])
            .unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0], (b, Some(b"payload-b".to_vec())));
        assert_eq!(got[1], (missing, None));
        assert_eq!(got[2], (a, Some(b"payload-a".to_vec())));
        // 空请求 → 空结果。
        assert!(store.get_many(&[]).unwrap().is_empty());
    }

    #[test]
    fn catalog_get_many_chunks_over_variable_limit() {
        let store = SqliteStore::open_in_memory().unwrap();
        // 501 个 id 跨过 BATCH_IN_CHUNK(500) 分块边界：两块 IN 都能正确取回。
        let mut ids: Vec<StableId> = Vec::new();
        for i in 0..501u32 {
            let id = sid(IdKind::Message, &i.to_le_bytes());
            store
                .put(&id, &format!("payload-{i}").into_bytes())
                .unwrap();
            ids.push(id);
        }
        let got = store.get_many(&ids).unwrap();
        assert_eq!(got.len(), 501);
        for (i, (id, payload)) in got.iter().enumerate() {
            assert_eq!(id, &ids[i]);
            assert_eq!(payload.as_deref(), Some(format!("payload-{i}").as_bytes()));
        }
    }

    /// 向 message_placements 直接插入一行（session_of 只读该表，无需 catalog/提交机制）。
    fn insert_placement(
        store: &SqliteStore,
        placement_id: &str,
        session: &StableId,
        document: &StableId,
        message: &StableId,
        source_ordinal: i64,
    ) {
        let conn = store.conn.borrow();
        conn.execute(
            "INSERT INTO message_placements(
                 placement_id, session_id, document_id, message_id,
                 source_ordinal, is_sidechain, byte_start, byte_end)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
            rusqlite::params![
                placement_id,
                session.as_str(),
                document.as_str(),
                message.as_str(),
                source_ordinal,
            ],
        )
        .unwrap();
    }

    #[test]
    fn session_of_resolves_owning_session_batched_and_order_preserving() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session_a = sid(IdKind::Session, b"owner-session-a");
        let session_b = sid(IdKind::Session, b"owner-session-b");
        let document = sid(IdKind::Document, b"owner-document");
        let msg_only_a = sid(IdKind::Message, b"owner-msg-only-a");
        let msg_both = sid(IdKind::Message, b"owner-msg-both");
        let msg_none = sid(IdKind::Message, b"owner-msg-none");
        insert_placement(
            &store,
            "plc_v1_owner_0",
            &session_a,
            &document,
            &msg_only_a,
            0,
        );
        insert_placement(
            &store,
            "plc_v1_owner_1",
            &session_a,
            &document,
            &msg_both,
            1,
        );
        insert_placement(
            &store,
            "plc_v1_owner_2",
            &session_b,
            &document,
            &msg_both,
            0,
        );

        // 乱序请求：结果与请求同序；msg_both 在两个会话都有 placement →
        // 确定性取 wire id 字典序最小的会话（跨页稳定）；msg_none 无 placement → None。
        let got = store
            .session_of(&[msg_both.clone(), msg_none.clone(), msg_only_a.clone()])
            .unwrap();
        let expected_both = [session_a.as_str(), session_b.as_str()]
            .iter()
            .min()
            .unwrap()
            .to_string();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0, msg_both);
        assert_eq!(
            got[0].1.as_ref().map(StableId::as_str),
            Some(expected_both.as_str())
        );
        assert_eq!(got[1].0, msg_none);
        assert_eq!(got[1].1, None);
        assert_eq!(got[2].0, msg_only_a);
        // 会话经 wire 往返重建（from_wire → Unstable tier），按 wire 串比较。
        assert_eq!(
            got[2].1.as_ref().map(StableId::as_str),
            Some(session_a.as_str())
        );
        // 空请求 → 空结果。
        assert!(store.session_of(&[]).unwrap().is_empty());
    }

    #[test]
    fn session_of_chunks_over_variable_limit() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"chunk-session");
        let document = sid(IdKind::Document, b"chunk-document");
        // 501 个消息跨过 BATCH_IN_CHUNK(500) 分块边界：两块 GROUP BY 查询都取回。
        let mut ids: Vec<StableId> = Vec::new();
        for i in 0..501u32 {
            let id = sid(IdKind::Message, &i.to_le_bytes());
            insert_placement(
                &store,
                &format!("plc_v1_chunk_{i}"),
                &session,
                &document,
                &id,
                i as i64,
            );
            ids.push(id);
        }
        let got = store.session_of(&ids).unwrap();
        assert_eq!(got.len(), 501);
        for (i, (id, owner)) in got.iter().enumerate() {
            assert_eq!(id, &ids[i]);
            assert_eq!(owner.as_ref().map(StableId::as_str), Some(session.as_str()));
        }
    }

    #[test]
    fn search_finds_indexed_and_rebuilds_id() {
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"m1");
        store.index(&id, "the quick brown fox").unwrap();
        let hits = store.query("brown", 10).unwrap();
        assert_eq!(hits.len(), 1);
        // 关键：从 FTS 取回的 id 与原 id 完全相等（含 kind/stability），
        // 证明 serde JSON 往返无损。
        assert_eq!(hits[0].id, id);
        assert_eq!(hits[0].id.kind(), IdKind::Message);
        assert_eq!(hits[0].id.stability(), Stability::Reconstructed);
    }

    #[test]
    fn reindex_is_idempotent() {
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"m1");
        store.index(&id, "alpha beta").unwrap();
        store.index(&id, "alpha gamma").unwrap();
        // 重索引后旧文本不再命中，新文本命中，且不产生重复行。
        assert!(store.query("beta", 10).unwrap().is_empty());
        assert_eq!(store.query("gamma", 10).unwrap().len(), 1);
        assert_eq!(store.query("alpha", 10).unwrap().len(), 1);
    }

    // ─── CJK bigram（ADR-0007）───

    #[test]
    fn cjk_bigram_recall_hits_two_char_queries_in_longer_sentences() {
        // R1.4：双字查询"配置"/"数据库"命中包含它们的长句。索引侧与查询侧
        // 同一 transform：整段汉字从 1 个 FTS 词元变成单字 + 相邻两字 bigram
        // 词元（"配置数据库迁移" → "配 置 数 据 库 迁 移 配置 置数 数据 据库 库迁 迁移"）。
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"cjk-m1");
        store
            .index(&id, "我们已经在生产环境配置了数据库迁移，备份策略也更新了")
            .unwrap();
        for query in ["配置", "数据库", "备份", "迁移", "策略"] {
            let hits = store.query(query, 10).unwrap();
            assert_eq!(hits.len(), 1, "query {query:?} must recall the message");
            assert_eq!(hits[0].id, id);
        }
        // 多字查询按单字 + bigram 并集 AND 匹配。
        assert_eq!(store.query("数据库迁移", 10).unwrap().len(), 1);
        // 不存在的双字组合不命中。
        assert!(store.query("翻墙", 10).unwrap().is_empty());
        // 单字 CJK 查询由同一索引流中的 unigram 词元覆盖，不再落空。
        assert_eq!(store.query("了", 10).unwrap().len(), 1);
    }

    #[test]
    fn cjk_unigram_tokens_recall_single_char_queries() {
        // 单字查询增强：FTS 索引流除 bigram 外还为每个汉字产出单字词元，
        // "了"/"配"这类单字查询不再 transform 成空串，命中含该字的句子。
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"cjk-uni");
        store
            .index(&id, "我们已经在生产环境配置了数据库迁移，备份策略也更新了")
            .unwrap();
        for query in ["我", "了", "配", "置", "迁", "移", "备", "新"] {
            let hits = store.query(query, 10).unwrap();
            assert_eq!(
                hits.len(),
                1,
                "single-char query {query:?} must recall the message"
            );
            assert_eq!(hits[0].id, id);
        }
        // 夹在 ASCII 之间的单字汉字运行（bigram 产不出词元）同样命中。
        let mixed = sid(IdKind::Message, b"cjk-uni-mixed");
        store.index(&mixed, "用cargo测试").unwrap();
        assert_eq!(store.query("用", 10).unwrap().len(), 1);
        assert_eq!(store.query("测", 10).unwrap().len(), 1);
        assert_eq!(store.query("cargo", 10).unwrap().len(), 1);
        // 未出现的单字不命中。
        assert!(store.query("丙", 10).unwrap().is_empty());
    }

    #[test]
    fn cjk_bigram_applies_on_source_batch_sync_path() {
        // 真实 ingest 路径（SourceBatch → commit_source_batches_if_changed）
        // 的 fts 写入与单条 SearchIndex::index 共用同一索引侧 transform。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"cjk-ses");
        let document = sid(IdKind::Document, b"cjk-doc");
        let message = sid(IdKind::Message, b"cjk-msg");
        let source = source_batch(
            "cjk-sync.jsonl",
            vec![
                entity_entry(&session),
                entity_entry(&document),
                typed_message_entry(&message, "启动服务时记得检查配置文件的路径"),
            ],
            vec![placement(
                &session,
                &document,
                &message,
                0,
                false,
                Some((0, 4)),
            )],
            Vec::new(),
            true,
        );
        let changed = store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();
        assert!(changed);
        assert_eq!(store.query("配置", 10).unwrap().len(), 1);
        assert_eq!(store.query("路径", 10).unwrap().len(), 1);
        assert_eq!(store.query("配置文件", 10).unwrap().len(), 1);

        // 内容级 no-op：再次同步同一源不推进 generation——current 判定对 fts
        // 存储的 transform 后正文与同一 transform 后的 batch text 比较（若只比原文，
        // 已同步的源每次重同步都会被误判为 not-current 而反复推进 generation）。
        let generation = store.active_generation().unwrap();
        let again = store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();
        assert!(!again, "unchanged re-sync must be a no-op");
        assert_eq!(store.active_generation().unwrap(), generation);
    }

    #[test]
    fn cjk_bigram_rebuild_reprojects_from_catalog_and_bumps_generation() {
        // R1.2：rebuild 从权威 catalog 重投影 FTS（searchable_text + 同一索引侧
        // transform），无 schema 变更、无 catalog 迁移；重建推进 generation
        // （旧 cursor 因此失效——正常契约行为）。
        // 词元增长：消息正文"今天把数据库备份到了新目录"（13 字）从 1 个整段
        // 词元变为 25 个单字 + bigram 词元——纯 CJK 文本约 4x 最坏增长
        // （ADR-0007 §后果）。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"rebuild-ses");
        let document = sid(IdKind::Document, b"rebuild-doc");
        let message = sid(IdKind::Message, b"rebuild-msg");
        let source = source_batch(
            "rebuild-cjk.jsonl",
            vec![
                entity_entry(&session),
                entity_entry(&document),
                typed_message_entry(&message, "今天把数据库备份到了新目录"),
            ],
            vec![placement(
                &session,
                &document,
                &message,
                0,
                false,
                Some((0, 4)),
            )],
            Vec::new(),
            true,
        );
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();
        assert_eq!(store.query("数据库", 10).unwrap().len(), 1);

        let generation_before = store.active_generation().unwrap();
        let n = store.rebuild_index().unwrap();
        assert_eq!(n, 3, "rebuild 应从 catalog 重投影全部 3 个实体");
        assert_eq!(
            store.active_generation().unwrap(),
            generation_before + 1,
            "rebuild 必须推进 generation"
        );
        // rebuild 后 CJK 依旧可搜（同一 searchable_text 投影 + 同一 transform）。
        assert_eq!(store.query("数据库", 10).unwrap().len(), 1);
        assert_eq!(store.query("备份", 10).unwrap().len(), 1);
        assert_eq!(store.query("新目录", 10).unwrap().len(), 1);
    }

    // ─── 索引投影版本（schema v17 / INDEX_PROJECTION_VERSION）───

    /// 把库改写成"旧投影"状态：FTS 正文用**上一版**纯 bigram 变换重写
    /// （`bigram_cjk`，即 `fts_tokens_cjk` 加入单字词元之前的形态），投影版本戳
    /// 回落为 0——精确复刻由旧二进制建立、迁到 v17 后的真实库。
    fn downgrade_projection_to_legacy_bigrams(store: &SqliteStore) {
        let conn = store.conn.borrow();
        // 消息 FTS：从权威 catalog payload 重投影后施加旧变换。
        let rows: Vec<(i64, Vec<u8>)> = conn
            .prepare(
                "SELECT f.rowid, c.payload FROM fts f
                 JOIN fts_ids fi ON fi.id_json = f.id
                 JOIN catalog c ON c.id = fi.wire_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(!rows.is_empty(), "夹具必须先有消息 FTS 行");
        for (rowid, payload) in rows {
            conn.execute(
                "UPDATE fts SET text = ?1 WHERE rowid = ?2",
                rusqlite::params![
                    agent_session_grep_application::bigram_cjk(&searchable_text(&payload)),
                    rowid
                ],
            )
            .unwrap();
        }
        // Session 元数据 FTS 同属本轴：同一旧变换重写。
        let sessions: Vec<(i64, String)> = conn
            .prepare("SELECT fts_rowid, session_wire FROM session_fts_ids")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for (rowid, wire) in sessions {
            let text = SqliteStore::session_search_text(&conn, &wire)
                .unwrap()
                .expect("fixture session must project search text");
            conn.execute(
                "UPDATE session_fts SET text = ?1 WHERE rowid = ?2",
                rusqlite::params![agent_session_grep_application::bigram_cjk(&text), rowid],
            )
            .unwrap();
        }
        conn.execute(
            "UPDATE store_metadata SET index_projection_version = 0 WHERE singleton = 1",
            [],
        )
        .unwrap();
    }

    /// 一条含中文的真实 ingest 批次（session + document + message）。
    fn cjk_projection_fixture(store: &SqliteStore, tag: &[u8], text: &str) -> StableId {
        let session = sid(IdKind::Session, &[tag, b"-ses"].concat());
        let document = sid(IdKind::Document, &[tag, b"-doc"].concat());
        let message = sid(IdKind::Message, &[tag, b"-msg"].concat());
        let source = source_batch(
            &format!("{}.jsonl", String::from_utf8_lossy(tag)),
            vec![
                entity_entry(&session),
                entity_entry(&document),
                typed_message_entry(&message, text),
            ],
            vec![placement(
                &session,
                &document,
                &message,
                0,
                false,
                Some((0, 4)),
            )],
            Vec::new(),
            true,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap()
        );
        message
    }

    #[test]
    fn stale_index_projection_refuses_instead_of_returning_wrong_cjk_hits() {
        // 实测缺陷复现（真实 170 468 实体库上通过 MCP search_sessions 观察到）：
        // 旧二进制写下的纯 bigram 词元流 + 新二进制的 unigram+bigram 查询词元
        // ⇒ 中文查询静默 0 命中，ASCII 查询照常命中。这是错误结果而非报错，
        // 最坏的失败类。修复后读路径必须 fail-closed。
        let store = SqliteStore::open_in_memory().unwrap();
        let message = cjk_projection_fixture(&store, b"stale-proj", "请帮我做配置备份 clippy");
        assert_eq!(store.query("配置备份", 10).unwrap().len(), 1);
        assert_eq!(store.query("备份", 10).unwrap().len(), 1);
        assert_eq!(store.query("clippy", 10).unwrap().len(), 1);
        assert!(store.index_projection_is_current().unwrap());

        downgrade_projection_to_legacy_bigrams(&store);
        assert_eq!(store.index_projection_version().unwrap(), 0);
        assert!(!store.index_projection_is_current().unwrap());

        // 修复前：下面三个查询分别返回 Ok([])、Ok([]) 与 Ok([hit])——中文静默
        // 落空、ASCII 照常命中，调用方无从察觉。修复后：三者一律 fail-closed。
        for query in ["配置备份", "备份", "clippy"] {
            let error = store.query(query, 10).unwrap_err();
            assert!(
                matches!(&error, PortError::SchemaIncompatible(message)
                    if message.contains("index rebuild")),
                "query {query:?} 必须 fail-closed 并给出修复命令，实际 {error:?}"
            );
        }
        // facet 路径同闸门。
        let error = store
            .query_faceted(
                SearchQuery {
                    text: "备份",
                    filters: &SearchFilters::EMPTY,
                },
                10,
                &SearchFacets {
                    sidechain: SidechainFacet::MainOnly,
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(matches!(error, PortError::SchemaIncompatible(_)));

        // catalog 是权威事实源，不受投影失配影响——get 仍返回原 payload。
        assert!(store.get(&message).unwrap().is_some());
    }

    #[test]
    fn write_open_reprojects_stale_projection_from_catalog_without_reparse() {
        // 方案 A（自愈）：写路径打开时检测失配 → 从权威 catalog 重投影。
        // 源文件**不存在于磁盘**，证明重投影无需回到 provider reparse。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stale.db");
        let p = path.to_string_lossy().into_owned();
        let generation_before;
        {
            let store = SqliteStore::open_for_write(&p).unwrap();
            cjk_projection_fixture(&store, b"heal-proj", "今天把配置备份到了新目录");
            assert_eq!(store.query("配置备份", 10).unwrap().len(), 1);
            downgrade_projection_to_legacy_bigrams(&store);
            generation_before = store.active_generation().unwrap();
        }
        // 只读打开：不写、不自愈，如实报告失配（doctor 走这条路）。
        {
            let store = SqliteStore::open(&p).unwrap();
            assert_eq!(store.index_projection_version().unwrap(), 0);
            assert!(store.query("配置备份", 10).is_err());
            assert_eq!(store.active_generation().unwrap(), generation_before);
        }
        // 写路径打开：自动重投影并收敛版本。
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert!(store.index_projection_is_current().unwrap());
        assert_eq!(
            store.active_generation().unwrap(),
            generation_before + 1,
            "重投影改变了投影内容，必须推进 generation 以失效旧 cursor"
        );
        assert_eq!(store.query("配置备份", 10).unwrap().len(), 1);
        assert_eq!(store.query("备份", 10).unwrap().len(), 1);
        assert_eq!(store.query("配", 10).unwrap().len(), 1);
        // 幂等：再次打开不再重投影、不再推进 generation。
        drop(store);
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.active_generation().unwrap(), generation_before + 1);
        assert!(!store.ensure_index_projection_current().unwrap());
    }

    #[test]
    fn empty_projection_is_stamped_current_without_rebuild_churn() {
        // 新库/空库：没有任何旧词元可纠正 → 只标记版本，不推进 generation、
        // 不写 outbox 行（否则每个新 data root 一打开就产生 churn）。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fresh.db");
        let p = path.to_string_lossy().into_owned();
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert!(store.index_projection_is_current().unwrap());
        assert_eq!(store.active_generation().unwrap(), 0);
        assert_eq!(store.interrupted_batch_count().unwrap(), 0);
        assert_eq!(table_count(&store, "index_batches"), 0);

        // 人为把戳打回 0（空投影）：收敛只盖戳，不重投影。
        {
            let conn = store.conn.borrow();
            conn.execute(
                "UPDATE store_metadata SET index_projection_version = 0 WHERE singleton = 1",
                [],
            )
            .unwrap();
        }
        assert!(!store.ensure_index_projection_current().unwrap());
        assert!(store.index_projection_is_current().unwrap());
        assert_eq!(store.active_generation().unwrap(), 0);
        assert_eq!(table_count(&store, "index_batches"), 0);
    }

    #[test]
    fn incremental_commit_does_not_claim_whole_store_projection_currency() {
        // 增量提交只覆盖本批实体，不能声明全库投影已收敛——否则一次 sync
        // 就会把"其余 17 万条仍是旧词元"的库标记为当前，缺陷原地复活。
        let store = SqliteStore::open_in_memory().unwrap();
        cjk_projection_fixture(&store, b"partial-a", "第一批配置备份");
        downgrade_projection_to_legacy_bigrams(&store);
        cjk_projection_fixture(&store, b"partial-b", "第二批配置备份");
        assert_eq!(
            store.index_projection_version().unwrap(),
            0,
            "增量提交不得盖投影版本戳"
        );
        // 只有整库重投影才盖戳。
        store.rebuild_index().unwrap();
        assert!(store.index_projection_is_current().unwrap());
        assert_eq!(store.query("配置备份", 10).unwrap().len(), 2);
    }

    #[test]
    fn cjk_bigram_keeps_ascii_path_and_punctuation_literals_unchanged() {
        // R1.3（ADR-0003）：纯 ASCII/路径/标点输入不含汉字，transform 原样
        // 返回——字面量化语义与之前逐字节一致，FTS 词元不变。
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"literal");
        store
            .index(&id, "check C:\\Users\\dev\\mcp.json config:backup")
            .unwrap();
        for query in ["mcp.json", "config:backup", "Users", "backup", "check"] {
            let hits = store.query(query, 10).unwrap();
            assert_eq!(hits.len(), 1, "query {query:?} must recall");
            assert_eq!(hits[0].id, id);
        }
        // 标点/操作符仍按字面量处理，不泄漏 FTS 语法错误。
        for query in ["a:b", "x-y", "prefix*", "AND OR NOT", "* - :"] {
            assert!(store.query(query, 10).unwrap().is_empty(), "{query:?}");
        }
        // 汉字与 ASCII 混合的内容两侧变换一致："配置v2.0" → "配置 v2.0" AND 匹配。
        let mixed = sid(IdKind::Message, b"cjk-mixed");
        store.index(&mixed, "使用配置v2.0备份").unwrap();
        assert_eq!(store.query("配置v2.0", 10).unwrap().len(), 1);
        assert_eq!(store.query("v2.0", 10).unwrap().len(), 1);
    }

    #[test]
    fn message_fts_body_is_capped_at_char_boundary_on_index() {
        // 借鉴清单 #3：单条消息正文超过 MESSAGE_FTS_MAX_CHARS 时，FTS 只索引前
        // MESSAGE_FTS_MAX_CHARS 字符——上限之后的词不可检索，上限内的词仍可检索。
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"index-long-body");
        let full = format!(
            "head-needle {}\n tail-needle",
            "f".repeat(agent_session_grep_application::MESSAGE_FTS_MAX_CHARS)
        );
        store.index(&id, &full).unwrap();
        assert_eq!(store.query("head-needle", 10).unwrap().len(), 1);
        assert!(
            store.query("tail-needle", 10).unwrap().is_empty(),
            "beyond-cap text must not be indexed"
        );
    }

    #[test]
    fn catalog_put_and_rebuild_project_capped_fts_body() {
        // put 与 rebuild 都按 searchable_text(payload) 重投影：catalog 保留全文
        // （THREAT-MODEL：Catalog 不在索引期改写原文），FTS 投影有界。
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"put-long-body");
        let full = format!(
            "early-needle {}\n late-needle",
            "f".repeat(agent_session_grep_application::MESSAGE_FTS_MAX_CHARS)
        );
        let payload = serde_json::json!({ "role": "user", "text": full })
            .to_string()
            .into_bytes();
        store.put(&id, &payload).unwrap();
        assert_eq!(
            store.get(&id).unwrap().unwrap(),
            payload,
            "catalog 保留原文全文"
        );
        assert_eq!(store.query("early-needle", 10).unwrap().len(), 1);
        assert!(store.query("late-needle", 10).unwrap().is_empty());

        store.rebuild_index().unwrap();
        assert_eq!(store.query("early-needle", 10).unwrap().len(), 1);
        assert!(store.query("late-needle", 10).unwrap().is_empty());
    }

    #[test]
    fn batch_commit_with_bounded_entry_text_stays_current() {
        // 生产 CLI 构造三元组时已把 text 截断到 MESSAGE_FTS_MAX_CHARS（与索引侧
        // 同一常量）：同样的有界 batch 重提交必须判 current，重同步幂等不被截断破坏。
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"bounded-current");
        let full = format!(
            "stable-head {}",
            "f".repeat(agent_session_grep_application::MESSAGE_FTS_MAX_CHARS)
        );
        let payload = serde_json::json!({ "role": "user", "text": full })
            .to_string()
            .into_bytes();
        let text = agent_session_grep_application::bounded_index_text(&full);
        let entries = [(id.clone(), payload, text)];
        assert!(store.commit_batch_if_changed(&entries).unwrap());
        let generation = store.active_generation().unwrap();
        assert!(!store.commit_batch_if_changed(&entries).unwrap());
        assert_eq!(store.active_generation().unwrap(), generation);
    }

    #[test]
    fn raw_han_sentence_is_one_token_without_bigram_transform() {
        // 对照基线（bigram 落地前的旧行为）：未经 transform 的原始正文经
        // unicode61 把整段汉字当一个词元，"配置"/"数据库"这类双字查询无法
        // 命中（ADR-0007 实证：中文召回 8-33%）。此 fixture 证明 recall 提升
        // 来自索引侧 transform 本身，而不是查询侧的特判；同时防止将来有人
        // 只回退写入侧、留下查询侧变换造成两侧错配。
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"raw-han");
        let id_json = serde_json::to_string(&id).unwrap();
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO fts(id, text) VALUES(?1, ?2)",
                rusqlite::params![
                    id_json,
                    "我们已经在生产环境配置了数据库迁移，备份策略也更新了"
                ],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO fts_ids(wire_id, id_json, fts_rowid) VALUES(?1, ?2, NULL)",
                rusqlite::params![id.as_str(), serde_json::to_string(&id).unwrap()],
            )
            .unwrap();
        }
        assert!(store.query("配置", 10).unwrap().is_empty());
        assert!(store.query("数据库", 10).unwrap().is_empty());
        assert!(store.query("迁移", 10).unwrap().is_empty());
    }

    #[test]
    fn query_respects_limit() {
        let store = SqliteStore::open_in_memory().unwrap();
        for i in 0..5u32 {
            let id = sid(IdKind::Message, &i.to_le_bytes());
            store.index(&id, "shared term").unwrap();
        }
        assert_eq!(store.query("shared", 3).unwrap().len(), 3);
    }

    #[test]
    fn fresh_db_reports_current_schema_version() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
    }

    #[test]
    fn read_open_never_creates_or_migrates_and_cannot_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("read.db");
        let p = path.to_str().unwrap();
        assert!(SqliteStore::open(p).is_err());
        assert!(!path.exists());
        let legacy = Connection::open(p).unwrap();
        legacy
            .execute_batch("PRAGMA user_version=1; CREATE TABLE marker(value);")
            .unwrap();
        drop(legacy);
        let before = std::fs::read(&path).unwrap();
        assert!(matches!(
            SqliteStore::open(p),
            Err(PortError::SchemaIncompatible(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let current = dir.path().join("current.db");
        let writer = SqliteStore::open_for_write(current.to_str().unwrap()).unwrap();
        let reader = SqliteStore::open(current.to_str().unwrap()).unwrap();
        assert!(
            reader
                .put(&sid(IdKind::Message, b"write"), b"denied")
                .is_err()
        );
        drop(writer);
    }

    #[test]
    fn reopen_preserves_data_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.db");
        let p = path.to_string_lossy().into_owned();
        // 用 Message id：非 Message 实体不进 fts 全文表（kind 门，见 index/put）。
        let id = sid(IdKind::Message, b"s1");
        {
            let store = SqliteStore::open_for_write(&p).unwrap();
            store.put(&id, b"persisted").unwrap();
            store.index(&id, "persisted body").unwrap();
        }
        // Reopen through the read-only path; data and schema remain unchanged.
        let store = SqliteStore::open(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(store.get(&id).unwrap().unwrap(), b"persisted");
        assert_eq!(store.query("persisted", 10).unwrap().len(), 1);
    }

    #[test]
    fn newer_schema_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future.db");
        let p = path.to_string_lossy().into_owned();
        // 先正常建库，再把 user_version 拨到未来版本，模拟更新的二进制写过的库。
        SqliteStore::open_for_write(&p).unwrap();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            conn.execute_batch(&format!("PRAGMA user_version = {};", SCHEMA_VERSION + 1))
                .unwrap();
        }
        // 旧二进制拒绝打开更新版本的库，而非按旧 schema 误读。
        // 用 match 而非 unwrap_err()——SqliteStore 内含 Connection，不实现 Debug。
        let err = match SqliteStore::open(&p) {
            Err(e) => e,
            Ok(_) => panic!("expected newer schema to be rejected"),
        };
        assert!(
            matches!(err, PortError::SchemaIncompatible(m) if m.contains("differs from supported"))
        );
    }

    #[test]
    fn v6_db_migrates_to_v7_without_fabricating_relations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v6.db");
        let p = path.to_string_lossy().into_owned();
        let payload = vec![0x00, 0xff, 0x7f, 0x01, 0x80];
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            conn.execute(
                "INSERT INTO catalog(id, payload) VALUES('msg_v1_legacy', ?1)",
                [&payload],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO source_membership(source_path, message_id, document_id)
                 VALUES('legacy.jsonl', 'msg_v1_legacy', NULL)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO source_scans(source_path, scanned_at_ms)
                 VALUES('legacy.jsonl', 1)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO index_batches(
                     operation_id, base_generation, target_generation, state,
                     operation_digest, upsert_ids_json, delete_ids_json,
                     durable_point, created_at_ms, committed_at_ms
                 ) VALUES(
                     'legacy-op', 0, 1, 'activated', 'legacy-digest', '[]', '[]',
                     'activated', 1, 2
                 )",
                [],
            )
            .unwrap();
        }

        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let id = StableId::from_wire("msg_v1_legacy").unwrap();
        assert_eq!(store.get(&id).unwrap().unwrap(), payload);
        let batch = store.index_batch("legacy-op").unwrap().unwrap();
        assert!(batch.relation_upserts.is_empty());
        assert!(batch.relation_deletes.is_empty());
        assert!(batch.source_replacements.is_empty());
        drop(store);

        let conn = rusqlite::Connection::open(&p).unwrap();
        let document_id: Option<String> = conn
            .query_row(
                "SELECT document_id FROM source_membership
                 WHERE source_path = 'legacy.jsonl' AND message_id = 'msg_v1_legacy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(document_id, None);

        for table in [
            "message_placements",
            "message_edges",
            "source_placement_membership",
            "source_relation_scans",
        ] {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{table} must start empty");
        }
        let relation_scan_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM source_relation_scans", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(relation_scan_count, 0);

        let manifests: (String, String, String) = conn
            .query_row(
                "SELECT relation_upserts_json, relation_deletes_json,
                        source_replacements_json
                 FROM index_batches WHERE operation_id = 'legacy-op'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            manifests,
            ("[]".to_string(), "[]".to_string(), "[]".to_string())
        );

        let mut stmt = conn
            .prepare(
                "SELECT name FROM sqlite_master
                 WHERE type = 'index' AND name IN (
                     'message_placements_session_order',
                     'message_placements_message',
                     'message_placements_document',
                     'source_placement_membership_placement'
                 )
                 ORDER BY name",
            )
            .unwrap();
        let indexes: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            indexes,
            vec![
                "message_placements_document",
                "message_placements_message",
                "message_placements_session_order",
                "source_placement_membership_placement",
            ]
        );
    }

    #[test]
    fn injected_v6_to_v7_failure_rolls_back_schema_and_version() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        create_v6_schema(&conn);

        let err = SqliteStore::migrate_v6_to_v7_inner(&conn, true).unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("injected v6-to-v7"))
        );
        assert!(conn.is_autocommit());
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 6);

        for table in [
            "message_placements",
            "message_edges",
            "source_placement_membership",
            "source_relation_scans",
        ] {
            let exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 0, "{table} must roll back");
        }
        let mut stmt = conn.prepare("PRAGMA table_info(index_batches)").unwrap();
        let columns: Vec<String> = stmt
            .query_map([], |row| row.get(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(!columns.iter().any(|name| {
            matches!(
                name.as_str(),
                "relation_upserts_json" | "relation_deletes_json" | "source_replacements_json"
            )
        }));
    }

    #[test]
    fn commit_batch_writes_all_entries() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"a");
        let b = sid(IdKind::Message, b"b");
        store
            .commit_batch(&[
                (a.clone(), b"role\talpha".to_vec(), "alpha text".into()),
                (b.clone(), b"role\tbeta".to_vec(), "beta text".into()),
            ])
            .unwrap();
        assert_eq!(store.get(&a).unwrap().unwrap(), b"role\talpha");
        assert_eq!(store.get(&b).unwrap().unwrap(), b"role\tbeta");
        assert_eq!(store.query("alpha", 10).unwrap().len(), 1);
        assert_eq!(store.query("beta", 10).unwrap().len(), 1);
    }

    #[test]
    fn commit_batch_is_idempotent() {
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"m");
        let entry = [(id.clone(), b"role\tv1".to_vec(), "version one".into())];
        store.commit_batch(&entry).unwrap();
        let entry2 = [(id.clone(), b"role\tv2".to_vec(), "version two".into())];
        store.commit_batch(&entry2).unwrap();
        assert_eq!(store.get(&id).unwrap().unwrap(), b"role\tv2");
        assert!(store.query("one", 10).unwrap().is_empty());
        assert_eq!(store.query("two", 10).unwrap().len(), 1);
    }

    #[test]
    fn batched_commit_rows_match_sequential_commit_rows() {
        // 批量 INSERT（多行 VALUES，100 行/块，见 BULK_INSERT_ROWS_PER_CHUNK）
        // 与逐行 INSERT 必须落出相同的 catalog/fts/fts_ids/placement/edge 行。
        // Store A 把整个 fixture 一次提交（256 实体 → 3 个 100 行块，走多行
        // 批量语句）；Store B 逐实体提交（每批 1 条 entry/placement/edge →
        // 逐行语句形状）。源级表（source_scans/source_membership 等）因源路径
        // 分布按设计不同而不比较；会话/文档容器 payload 的兼容别名按源拓扑
        // 重投影（document_id 归属不同），也不比较——消息 payload 必须逐字节相同。
        let session = sid(IdKind::Session, b"batch-equiv-session");
        let document = sid(IdKind::Document, b"batch-equiv-document");
        let (source, _) = chain_source_batch("batched.jsonl", &session, &document, 250, 2);
        assert!(
            source.entries.len() > 200 && source.placements.len() > 200,
            "fixture must span multiple 100-row chunks, got {} entries / {} placements",
            source.entries.len(),
            source.placements.len()
        );

        // Store A：单次大批次提交（多行批量路径）。
        let store_a = SqliteStore::open_in_memory().unwrap();
        store_a
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();

        // Store B：逐实体提交（每批 1 条 entry → 逐行语句形状）。
        let store_b = SqliteStore::open_in_memory().unwrap();
        let mut container_entries: Vec<(StableId, Vec<u8>, String)> = source
            .entries
            .iter()
            .filter(|(id, _, _)| id.kind() != IdKind::Message)
            .cloned()
            .collect();
        container_entries.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        // 会话与文档实体必须先提交：placement 完整性校验要求被引用的
        // session/document/message 实体已存在于 catalog。
        for (index, entry) in container_entries.iter().enumerate() {
            let batch = source_batch(
                &format!("seq-container-{index}.jsonl"),
                vec![entry.clone()],
                vec![],
                vec![],
                true,
            );
            store_b
                .commit_source_batches_if_changed(std::slice::from_ref(&batch))
                .unwrap();
        }
        let message_entries: Vec<(StableId, Vec<u8>, String)> = source
            .entries
            .iter()
            .filter(|(id, _, _)| id.kind() == IdKind::Message)
            .cloned()
            .collect();
        for (index, entry) in message_entries.iter().enumerate() {
            let placements: Vec<MessagePlacement> = source
                .placements
                .iter()
                .filter(|placement| placement.message_id == entry.0)
                .cloned()
                .collect();
            let edges: Vec<MessageEdge> = source
                .edges
                .iter()
                .filter(|edge| {
                    placements
                        .iter()
                        .any(|placement| placement.id == edge.child_placement_id)
                })
                .cloned()
                .collect();
            let batch = source_batch(
                &format!("seq-msg-{index:04}.jsonl"),
                vec![entry.clone()],
                placements,
                edges,
                true,
            );
            store_b
                .commit_source_batches_if_changed(std::slice::from_ref(&batch))
                .unwrap();
        }

        // 实体级表行数相等。
        for table in [
            "catalog",
            "fts",
            "fts_ids",
            "message_placements",
            "message_edges",
        ] {
            assert_eq!(
                table_count(&store_a, table),
                table_count(&store_b, table),
                "{table} row count must match between batched and sequential commits"
            );
        }
        // 源级表按设计不同：Store A 1 个源，Store B 逐实体一源。
        assert_eq!(table_count(&store_a, "source_scans"), 1);
        assert_eq!(
            table_count(&store_b, "source_scans"),
            (message_entries.len() + container_entries.len()) as i64
        );

        // 消息 payload 逐实体相等（容器实体的兼容别名按源拓扑重投影，跳过）。
        let catalog_payloads = |store: &SqliteStore| -> BTreeMap<String, Vec<u8>> {
            let conn = store.conn.borrow();
            let mut stmt = conn
                .prepare("SELECT id, payload FROM catalog ORDER BY id")
                .unwrap();
            stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .filter(|(wire, _)| !wire.starts_with("ses_v1_") && !wire.starts_with("doc_v1_"))
            .collect()
        };
        assert_eq!(catalog_payloads(&store_a), catalog_payloads(&store_b));

        // fts 正文逐实体相等（fts 存 id_json + transform 后正文）。
        let fts_rows = |store: &SqliteStore| -> BTreeMap<String, String> {
            let conn = store.conn.borrow();
            let mut stmt = conn
                .prepare("SELECT id, text FROM fts ORDER BY id")
                .unwrap();
            stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect()
        };
        assert_eq!(fts_rows(&store_a), fts_rows(&store_b));

        // fts_ids 边车逐实体相等（wire_id → id_json）。fts_rowid 是 FTS 内部
        // 物理 rowid，批量和顺序插入的分配顺序可以不同，不属于目录语义；下方
        // 另行断言每个 store 内部的 rowid 指向关系完整。
        let fts_ids_rows = |store: &SqliteStore| -> BTreeMap<String, String> {
            let conn = store.conn.borrow();
            let mut stmt = conn
                .prepare("SELECT wire_id, id_json FROM fts_ids ORDER BY wire_id")
                .unwrap();
            stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        assert_eq!(fts_ids_rows(&store_a), fts_ids_rows(&store_b));
        for store in [&store_a, &store_b] {
            let mismatched: i64 = store
                .conn
                .borrow()
                .query_row(
                    "SELECT COUNT(*) FROM fts f
                     JOIN fts_ids fi ON fi.id_json = f.id
                     WHERE fi.fts_rowid IS NULL OR fi.fts_rowid != f.rowid",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                mismatched, 0,
                "fts_ids.fts_rowid must match the fts row's actual rowid"
            );
        }

        // placements 全列相等。
        let placement_rows = |store: &SqliteStore| -> Vec<PlacementSnapshotRow> {
            let conn = store.conn.borrow();
            let mut stmt = conn
                .prepare(
                    "SELECT placement_id, session_id, document_id, message_id,
                                source_ordinal, is_sidechain, byte_start, byte_end
                         FROM message_placements ORDER BY placement_id",
                )
                .unwrap();
            stmt.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect()
        };
        assert_eq!(placement_rows(&store_a), placement_rows(&store_b));

        // edges 全列相等。
        let edge_rows = |store: &SqliteStore| -> Vec<(String, String, Option<String>, String)> {
            let conn = store.conn.borrow();
            let mut stmt = conn
                .prepare(
                    "SELECT child_placement_id, parent_message_id, parent_native_id, relation
                         FROM message_edges ORDER BY child_placement_id",
                )
                .unwrap();
            stmt.query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect()
        };
        assert_eq!(edge_rows(&store_a), edge_rows(&store_b));
    }

    #[test]
    fn relation_and_marker_only_changes_advance_once_then_noop() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"relation-generation-session");
        let document = sid(IdKind::Document, b"relation-generation-document");
        let parent = sid(IdKind::Message, b"relation-generation-parent");
        let child = sid(IdKind::Message, b"relation-generation-child");
        let child_placement = placement(&session, &document, &child, 1, false, Some((10, 20)));
        let entries = || {
            [&session, &document, &parent, &child]
                .into_iter()
                .map(entity_entry)
                .collect()
        };

        let initial = source_batch(
            "relation-generation.jsonl",
            entries(),
            vec![child_placement.clone()],
            Vec::new(),
            true,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&initial))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), 1);

        let edge = reply_edge(&child_placement, &parent);
        let relation_only = source_batch(
            "relation-generation.jsonl",
            entries(),
            vec![child_placement.clone()],
            vec![edge.clone()],
            true,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&relation_only))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), 2);
        let relation_batch = latest_index_batch(&store);
        assert_eq!(relation_batch.relation_upserts.len(), 2);
        assert!(relation_batch.relation_deletes.is_empty());
        assert_eq!(relation_batch.source_replacements.len(), 1);

        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&relation_only))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), 2);

        let marker_only = source_batch(
            "relation-generation.jsonl",
            entries(),
            vec![child_placement.clone()],
            vec![edge.clone()],
            false,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&marker_only))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), 3);
        assert!(!relation_complete_marker(
            &store,
            "relation-generation.jsonl"
        ));
        assert_eq!(
            latest_index_batch(&store).source_replacements[0]["relation_complete"],
            serde_json::Value::Bool(false)
        );

        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&marker_only))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), 3);

        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&relation_only))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), 4);
        assert!(relation_complete_marker(
            &store,
            "relation-generation.jsonl"
        ));
    }

    #[test]
    fn complete_relations_regenerate_divergent_context_aliases_without_stable_conflict() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session_a = sid(IdKind::Session, b"compat-session-a");
        let session_b = sid(IdKind::Session, b"compat-session-b");
        let document_a = sid(IdKind::Document, b"compat-document-a");
        let document_b = sid(IdKind::Document, b"compat-document-b");
        let parent_a = sid(IdKind::Message, b"compat-parent-a");
        let parent_b = sid(IdKind::Message, b"compat-parent-b");
        let child = sid(IdKind::Message, b"compat-child");
        let placement_a = placement(&session_a, &document_a, &child, 1, false, Some((10, 20)));
        let placement_b = placement(&session_b, &document_b, &child, 2, true, Some((30, 40)));
        let edge_a = MessageEdge {
            child_placement_id: placement_a.id.clone(),
            parent_message_id: parent_a.clone(),
            parent_native_id: Some("native-parent-a".into()),
            relation: MessageRelation::Reply,
        };
        let edge_b = MessageEdge {
            child_placement_id: placement_b.id.clone(),
            parent_message_id: parent_b.clone(),
            parent_native_id: Some("native-parent-b".into()),
            relation: MessageRelation::Reply,
        };
        let sources = [
            source_batch(
                "compat-a.jsonl",
                vec![
                    (
                        child.clone(),
                        relational_message_payload(
                            &session_a,
                            &document_a,
                            &parent_a,
                            "native-parent-a",
                            false,
                            (10, 20),
                        ),
                        "shared stable body".into(),
                    ),
                    (
                        session_a.clone(),
                        session_payload(document_a.as_str(), &[child.as_str()]),
                        String::new(),
                    ),
                    entity_entry(&document_a),
                    entity_entry(&parent_a),
                ],
                vec![placement_a.clone()],
                vec![edge_a],
                true,
            ),
            source_batch(
                "compat-b.jsonl",
                vec![
                    (
                        child.clone(),
                        relational_message_payload(
                            &session_b,
                            &document_b,
                            &parent_b,
                            "native-parent-b",
                            true,
                            (30, 40),
                        ),
                        "shared stable body".into(),
                    ),
                    (
                        session_b.clone(),
                        session_payload(document_b.as_str(), &[child.as_str()]),
                        String::new(),
                    ),
                    entity_entry(&document_b),
                    entity_entry(&parent_b),
                ],
                vec![placement_b.clone()],
                vec![edge_b],
                true,
            ),
        ];
        assert!(store.commit_source_batches_if_changed(&sources).unwrap());

        let stored: serde_json::Value =
            serde_json::from_slice(&store.get(&child).unwrap().unwrap()).unwrap();
        assert_eq!(stored["parent"], serde_json::Value::Null);
        assert_eq!(stored["parent_native_id"], serde_json::Value::Null);
        assert_eq!(stored["is_sidechain"], serde_json::Value::Null);
        assert_eq!(stored["sessions"].as_array().unwrap().len(), 2);
        let spans = stored["spans"].as_array().unwrap();
        assert_eq!(spans.len(), 2);
        assert!(
            spans
                .iter()
                .any(|span| span["placement_id"] == placement_a.id.as_str())
        );
        assert!(
            spans
                .iter()
                .any(|span| span["placement_id"] == placement_b.id.as_str())
        );

        let generation = store.active_generation().unwrap();
        assert!(!store.commit_source_batches_if_changed(&sources).unwrap());
        assert_eq!(store.active_generation().unwrap(), generation);
    }

    #[test]
    fn alias_regeneration_is_scoped_to_batch_sources_and_skips_unchanged_rows() {
        // P0-1: regenerating aliases for the whole catalog per batch made
        // first ingest O(n²). Only entities whose claimers intersect the
        // batch's sources may be rewritten, and a rewrite whose bytes are
        // unchanged must not hit the UPDATE (WAL stays flat).
        let store = SqliteStore::open_in_memory().unwrap();
        let session_a = sid(IdKind::Session, b"scope-session-a");
        let document_a = sid(IdKind::Document, b"scope-document-a");
        let parent_a = sid(IdKind::Message, b"scope-parent-a");
        let child_a = sid(IdKind::Message, b"scope-child-a");
        let placement_a = placement(&session_a, &document_a, &child_a, 1, false, Some((10, 20)));
        let edge_a = MessageEdge {
            child_placement_id: placement_a.id.clone(),
            parent_message_id: parent_a.clone(),
            parent_native_id: Some("native-parent-a".into()),
            relation: MessageRelation::Reply,
        };
        let batch_a = source_batch(
            "scope-a.jsonl",
            vec![
                (
                    child_a.clone(),
                    relational_message_payload(
                        &session_a,
                        &document_a,
                        &parent_a,
                        "native-parent-a",
                        false,
                        (10, 20),
                    ),
                    // entry text 必须与 payload 内嵌 text 一致（生产 CLI 恒等）：
                    // 合并路径按 searchable_text(payload) 重投影 FTS，不一致的
                    // fixture 会把重同步误判为需要"修复"fts 文本而推进 generation。
                    "shared stable body".into(),
                ),
                (
                    session_a.clone(),
                    session_payload(document_a.as_str(), &[child_a.as_str()]),
                    String::new(),
                ),
                entity_entry(&document_a),
                entity_entry(&parent_a),
            ],
            vec![placement_a.clone()],
            vec![edge_a],
            true,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&batch_a))
                .unwrap()
        );

        // Second batch touches only source B; entity A's stored payload must
        // remain byte-identical after commit B.
        let session_b = sid(IdKind::Session, b"scope-session-b");
        let document_b = sid(IdKind::Document, b"scope-document-b");
        let parent_b = sid(IdKind::Message, b"scope-parent-b");
        let child_b = sid(IdKind::Message, b"scope-child-b");
        let placement_b = placement(&session_b, &document_b, &child_b, 1, false, Some((30, 40)));
        let edge_b = MessageEdge {
            child_placement_id: placement_b.id.clone(),
            parent_message_id: parent_b.clone(),
            parent_native_id: Some("native-parent-b".into()),
            relation: MessageRelation::Reply,
        };
        let batch_b = source_batch(
            "scope-b.jsonl",
            vec![
                (
                    child_b.clone(),
                    relational_message_payload(
                        &session_b,
                        &document_b,
                        &parent_b,
                        "native-parent-b",
                        false,
                        (30, 40),
                    ),
                    "shared stable body".into(),
                ),
                (
                    session_b.clone(),
                    session_payload(document_b.as_str(), &[child_b.as_str()]),
                    String::new(),
                ),
                entity_entry(&document_b),
                entity_entry(&parent_b),
            ],
            vec![placement_b.clone()],
            vec![edge_b],
            true,
        );
        let stored_a_before = store.get(&child_a).unwrap().unwrap();
        assert!(store.commit_source_batches_if_changed(&[batch_b]).unwrap());
        let stored_a_after = store.get(&child_a).unwrap().unwrap();
        assert_eq!(
            stored_a_before, stored_a_after,
            "entity owned only by an untouched source must not be rewritten"
        );
        // And a full re-sync of A is a no-op that does not advance generation.
        let generation = store.active_generation().unwrap();
        assert!(!store.commit_source_batches_if_changed(&[batch_a]).unwrap());
        assert_eq!(store.active_generation().unwrap(), generation);
    }

    #[test]
    fn mixed_relation_completeness_preserves_alias_until_last_contributor_is_complete() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session_a = sid(IdKind::Session, b"mixed-session-a");
        let session_b = sid(IdKind::Session, b"mixed-session-b");
        let document_a = sid(IdKind::Document, b"mixed-document-a");
        let document_b = sid(IdKind::Document, b"mixed-document-b");
        let parent_a = sid(IdKind::Message, b"mixed-parent-a");
        let parent_b = sid(IdKind::Message, b"mixed-parent-b");
        let child = sid(IdKind::Message, b"mixed-child");
        let placement_a = placement(&session_a, &document_a, &child, 1, false, Some((10, 20)));
        let placement_b = placement(&session_b, &document_b, &child, 2, true, Some((30, 40)));
        let source_a = |complete| {
            source_batch(
                "mixed-a.jsonl",
                vec![
                    (
                        child.clone(),
                        relational_message_payload(
                            &session_a,
                            &document_a,
                            &parent_a,
                            "native-parent-a",
                            false,
                            (10, 20),
                        ),
                        "shared stable body".into(),
                    ),
                    (
                        session_a.clone(),
                        session_payload(document_a.as_str(), &[child.as_str()]),
                        String::new(),
                    ),
                    entity_entry(&document_a),
                    entity_entry(&parent_a),
                ],
                vec![placement_a.clone()],
                vec![MessageEdge {
                    child_placement_id: placement_a.id.clone(),
                    parent_message_id: parent_a.clone(),
                    parent_native_id: Some("native-parent-a".into()),
                    relation: MessageRelation::Reply,
                }],
                complete,
            )
        };
        let source_b = source_batch(
            "mixed-b.jsonl",
            vec![
                (
                    child.clone(),
                    relational_message_payload(
                        &session_b,
                        &document_b,
                        &parent_b,
                        "native-parent-b",
                        true,
                        (30, 40),
                    ),
                    "shared stable body".into(),
                ),
                (
                    session_b.clone(),
                    session_payload(document_b.as_str(), &[child.as_str()]),
                    String::new(),
                ),
                entity_entry(&document_b),
                entity_entry(&parent_b),
            ],
            vec![placement_b.clone()],
            vec![MessageEdge {
                child_placement_id: placement_b.id.clone(),
                parent_message_id: parent_b.clone(),
                parent_native_id: Some("native-parent-b".into()),
                relation: MessageRelation::Reply,
            }],
            true,
        );

        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source_a(false)))
            .unwrap();
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source_b))
            .unwrap();
        let mixed: serde_json::Value =
            serde_json::from_slice(&store.get(&child).unwrap().unwrap()).unwrap();
        assert_eq!(mixed["parent"], parent_a.as_str());
        assert_eq!(mixed["parent_native_id"], "native-parent-a");
        assert_eq!(mixed["is_sidechain"], false);

        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source_a(true)))
                .unwrap()
        );
        let complete: serde_json::Value =
            serde_json::from_slice(&store.get(&child).unwrap().unwrap()).unwrap();
        assert_eq!(complete["parent"], serde_json::Value::Null);
        assert_eq!(complete["parent_native_id"], serde_json::Value::Null);
        assert_eq!(complete["is_sidechain"], serde_json::Value::Null);
        assert_eq!(complete["spans"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn context_graph_store_loads_typed_graph_and_groups_message_candidates_by_session() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"typed-context-session");
        let document = sid(IdKind::Document, b"typed-context-document");
        let message = sid(IdKind::Message, b"typed-context-message");
        let first = placement(&session, &document, &message, 0, false, Some((0, 4)));
        let second = placement(&session, &document, &message, 1, true, Some((5, 9)));
        let source = source_batch(
            "typed-context.jsonl",
            vec![
                typed_message_entry(&message, "typed context body"),
                (
                    session.clone(),
                    session_payload(document.as_str(), &[message.as_str()]),
                    String::new(),
                ),
                typed_document_entry(&document),
            ],
            vec![first.clone(), second.clone()],
            Vec::new(),
            true,
        );
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();

        let graph = store.load_session_graph(&session).unwrap();
        assert_eq!(graph.session_id, session);
        assert_eq!(graph.messages.len(), 1);
        assert_eq!(graph.messages[0].id, message);
        assert_eq!(graph.source_documents.len(), 1);
        assert_eq!(graph.source_documents[0].id, document);
        assert_eq!(graph.placements.len(), 2);
        assert!(graph.edges.is_empty());
        graph.validate().unwrap();

        let candidates = store.message_contexts(&message).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].session_id, session);
        assert_eq!(
            candidates[0].placement_ids,
            vec![first.id.clone(), second.id.clone()]
        );
        assert_eq!(
            store.context_stats().unwrap(),
            ContextStats {
                placements: 2,
                source_placement_claims: 2,
            }
        );
    }

    #[test]
    fn context_graph_store_keeps_zero_message_session_document_attribution() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"zero-message-session");
        let document = sid(IdKind::Document, b"zero-message-document");
        let source = source_batch(
            "zero-message.jsonl",
            vec![
                (
                    session.clone(),
                    session_payload(document.as_str(), &[]),
                    String::new(),
                ),
                typed_document_entry(&document),
            ],
            Vec::new(),
            Vec::new(),
            true,
        );
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();

        let graph = store.load_session_graph(&session).unwrap();
        assert!(graph.messages.is_empty());
        assert!(graph.placements.is_empty());
        assert!(graph.edges.is_empty());
        assert_eq!(graph.source_documents.len(), 1);
        assert_eq!(graph.source_documents[0].id, document);
        graph.validate().unwrap();
    }

    #[test]
    fn every_contributing_source_must_be_relation_complete_before_context_reads() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"incomplete-context-session");
        let document_a = sid(IdKind::Document, b"complete-context-document");
        let document_b = sid(IdKind::Document, b"incomplete-context-document");
        let message_a = sid(IdKind::Message, b"complete-context-message");
        let message_b = sid(IdKind::Message, b"incomplete-context-message");
        let placement_a = placement(&session, &document_a, &message_a, 0, false, Some((0, 4)));
        let placement_b = placement(&session, &document_b, &message_b, 0, false, Some((0, 4)));
        let complete_source_path = "private-complete-source.jsonl";
        let incomplete_source_path = "private-incomplete-source.jsonl";
        let complete_source = source_batch(
            complete_source_path,
            vec![
                typed_message_entry(&message_a, "complete context body"),
                (
                    session.clone(),
                    session_payload(document_a.as_str(), &[message_a.as_str()]),
                    String::new(),
                ),
                typed_document_entry(&document_a),
            ],
            vec![placement_a],
            Vec::new(),
            true,
        );
        let incomplete_source = |complete| {
            source_batch(
                incomplete_source_path,
                vec![
                    typed_message_entry(&message_b, "incomplete context body"),
                    (
                        session.clone(),
                        session_payload(document_b.as_str(), &[message_b.as_str()]),
                        String::new(),
                    ),
                    typed_document_entry(&document_b),
                ],
                vec![placement_b.clone()],
                Vec::new(),
                complete,
            )
        };
        store
            .commit_source_batches_if_changed(&[complete_source, incomplete_source(false)])
            .unwrap();
        assert!(relation_complete_marker(&store, complete_source_path));
        assert!(!relation_complete_marker(&store, incomplete_source_path));

        let session_error = store.load_session_graph(&session).unwrap_err();
        assert!(
            matches!(&session_error, PortError::SchemaIncompatible(message) if message.contains("re-ingest required"))
        );
        assert!(!session_error.to_string().contains(complete_source_path));
        assert!(!session_error.to_string().contains(incomplete_source_path));

        let message_error = store.message_contexts(&message_b).unwrap_err();
        assert!(
            matches!(&message_error, PortError::SchemaIncompatible(message) if message.contains("re-ingest required"))
        );
        assert!(!message_error.to_string().contains(incomplete_source_path));

        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&incomplete_source(true)))
                .unwrap()
        );
        assert!(relation_complete_marker(&store, incomplete_source_path));

        let graph = store.load_session_graph(&session).unwrap();
        assert_eq!(graph.placements.len(), 2);
        assert!(
            graph
                .placements
                .iter()
                .any(|placement| placement.id == placement_b.id)
        );
        let contexts = store.message_contexts(&message_b).unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].session_id, session);
        assert_eq!(contexts[0].placement_ids, vec![placement_b.id.clone()]);
    }

    #[test]
    fn rebuild_preserves_relations_context_claims_and_completeness() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"rebuild-context-session");
        let document = sid(IdKind::Document, b"rebuild-context-document");
        let parent = sid(IdKind::Message, b"rebuild-context-parent");
        let child = sid(IdKind::Message, b"rebuild-context-child");
        let parent_placement = placement(&session, &document, &parent, 0, false, Some((0, 4)));
        let child_placement = placement(&session, &document, &child, 1, false, Some((5, 9)));
        let source_path = "rebuild-context.jsonl";
        let source = source_batch(
            source_path,
            vec![
                typed_message_entry(&parent, "parent body"),
                typed_message_entry(&child, "child body"),
                (
                    session.clone(),
                    session_payload(document.as_str(), &[parent.as_str(), child.as_str()]),
                    String::new(),
                ),
                typed_document_entry(&document),
            ],
            vec![parent_placement, child_placement.clone()],
            vec![reply_edge(&child_placement, &parent)],
            true,
        );
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();
        let graph_before = store.load_session_graph(&session).unwrap();
        let placements_before = store.stored_placements().unwrap();
        let edges_before = store.stored_edges().unwrap();
        let claims_before = source_placement_claims(&store, source_path);
        let stats_before = store.context_stats().unwrap();
        assert!(relation_complete_marker(&store, source_path));

        store.rebuild_index().unwrap();

        assert_eq!(store.load_session_graph(&session).unwrap(), graph_before);
        assert_eq!(store.stored_placements().unwrap(), placements_before);
        assert_eq!(store.stored_edges().unwrap(), edges_before);
        assert_eq!(source_placement_claims(&store, source_path), claims_before);
        assert_eq!(store.context_stats().unwrap(), stats_before);
        assert!(relation_complete_marker(&store, source_path));
    }

    #[test]
    fn load_session_graph_statement_count_is_bounded_regardless_of_edge_count() {
        // 上下文装配的 SQL 语句数只随 wire 批块增长,与边数无关(改前每条边
        // 一次父身份查询,N+1)。两个 1K+/2K+ 边的会话都应在常数界内,且
        // 语句数随边翻倍只增加批块差(≤12),不随边线性增长。
        let measure = |chain_len: usize, label: &str| -> (usize, usize) {
            let store = SqliteStore::open_in_memory().unwrap();
            let session = sid(IdKind::Session, format!("count-session-{label}").as_bytes());
            let document = sid(
                IdKind::Document,
                format!("count-document-{label}").as_bytes(),
            );
            let (source, _) =
                chain_source_batch(&format!("{label}.jsonl"), &session, &document, chain_len, 5);
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap();
            let edges = source.edges.len();
            let statements =
                counted_statements(&store, || store.load_session_graph(&session).unwrap());
            (statements, edges)
        };
        let (small_statements, small_edges) = measure(1200, "small");
        let (large_statements, large_edges) = measure(2400, "large");
        assert!(
            small_edges >= 1000,
            "fixture must exceed 1K edges, got {small_edges}"
        );
        assert!(
            small_statements <= 30,
            "small session ({small_edges} edges) issued {small_statements} statements; expected a constant bound independent of edge count"
        );
        assert!(
            large_statements <= 30,
            "large session ({large_edges} edges) issued {large_statements} statements; expected a constant bound independent of edge count"
        );
        assert!(
            large_statements <= small_statements + 12,
            "statement count must scale with wire chunks, not edges: {small_statements} -> {large_statements}"
        );
    }

    #[test]
    fn load_session_graph_keeps_orphan_parent_identity_grade() {
        // R1.2: 孤儿父(在本会话无出现、仅由边引用的消息)的身份必须经批量
        // fts_ids 加载保真,不能因批量路径缺 wire 而退化为 from_wire(Unstable)。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"grade-session");
        let document = sid(IdKind::Document, b"grade-document");
        let (source, orphans) = chain_source_batch("grade.jsonl", &session, &document, 20, 3);
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();

        let graph = store.load_session_graph(&session).unwrap();
        // 边按 child_placement_id 排序,与孤儿索引顺序无关,按排序后的集合比较。
        let mut orphan_parents: Vec<&str> = graph
            .edges
            .iter()
            .filter(|edge| edge.parent_message_id.stability() == Stability::Native)
            .map(|edge| edge.parent_message_id.as_str())
            .collect();
        let mut expected: Vec<&str> = orphans.iter().map(|id| id.as_str()).collect();
        orphan_parents.sort();
        expected.sort();
        assert_eq!(
            orphan_parents, expected,
            "orphan parent identities must keep their Native grade and value"
        );
    }

    #[test]
    fn rebuild_index_statement_count_is_bounded_per_catalog_row() {
        // 读相从逐行 fts_ids 查询改为单条 LEFT JOIN:语句数相对改前恰好减少
        // catalog 行数。界取 2.5×rows:改后 ≈2×rows(读 1 + 写相每实体固定),
        // 改前 ≈3×rows(读相每行 1 条),回归会超出此界。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"rebuild-count-session");
        let document = sid(IdKind::Document, b"rebuild-count-document");
        let (source, _) = chain_source_batch("rebuild-count.jsonl", &session, &document, 1200, 5);
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();
        let rows = table_count(&store, "catalog");
        assert!(
            rows > 1000,
            "fixture must exceed 1K catalog rows, got {rows}"
        );
        let statements = counted_statements(&store, || store.rebuild_index().unwrap());
        // 语句数含 FTS5 影子表维护,约 7×rows;改前逐行 fts_ids 身份读取还要再
        // 加 rows 条(≈8×rows),此界把逐行读回归挡在门外。
        assert!(
            statements as i64 <= rows * 7 + 600,
            "rebuild issued {statements} statements for {rows} catalog rows; expected <= {} (a per-row identity read would add ~{rows} more)",
            rows * 7 + 600
        );
    }

    #[test]
    fn incomplete_scan_updates_observed_relations_without_tombstoning_unseen_facts() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"incomplete-update-session");
        let document = sid(IdKind::Document, b"incomplete-update-document");
        let parent_a = sid(IdKind::Message, b"incomplete-update-parent-a");
        let parent_b = sid(IdKind::Message, b"incomplete-update-parent-b");
        let observed_message = sid(IdKind::Message, b"incomplete-update-observed");
        let unseen_message = sid(IdKind::Message, b"incomplete-update-unseen");
        let original = placement(
            &session,
            &document,
            &observed_message,
            1,
            false,
            Some((1, 5)),
        );
        let changed = placement(
            &session,
            &document,
            &observed_message,
            1,
            true,
            Some((2, 6)),
        );
        let unseen = placement(
            &session,
            &document,
            &unseen_message,
            2,
            false,
            Some((7, 11)),
        );
        let entries = || {
            [
                &session,
                &document,
                &parent_a,
                &parent_b,
                &observed_message,
                &unseen_message,
            ]
            .into_iter()
            .map(entity_entry)
            .collect()
        };
        let initial = source_batch(
            "incomplete-update.jsonl",
            entries(),
            vec![original.clone(), unseen.clone()],
            vec![
                reply_edge(&original, &parent_a),
                reply_edge(&unseen, &parent_a),
            ],
            true,
        );
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&initial))
            .unwrap();

        let changed_observation = source_batch(
            "incomplete-update.jsonl",
            entries(),
            vec![changed.clone()],
            vec![reply_edge(&changed, &parent_b)],
            false,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&changed_observation))
                .unwrap()
        );
        assert!(
            store
                .stored_placements()
                .unwrap()
                .get(changed.id.as_str())
                .unwrap()
                .matches(&changed)
        );
        assert!(
            store
                .stored_edges()
                .unwrap()
                .get(changed.id.as_str())
                .unwrap()
                .matches(&reply_edge(&changed, &parent_b))
        );
        assert!(
            store
                .stored_placements()
                .unwrap()
                .contains_key(unseen.id.as_str())
        );
        assert!(
            store
                .stored_edges()
                .unwrap()
                .contains_key(unseen.id.as_str())
        );
        assert!(!relation_complete_marker(&store, "incomplete-update.jsonl"));

        let observed_root = source_batch(
            "incomplete-update.jsonl",
            entries(),
            vec![changed.clone()],
            Vec::new(),
            false,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&observed_root))
                .unwrap()
        );
        assert!(
            !store
                .stored_edges()
                .unwrap()
                .contains_key(changed.id.as_str())
        );
        assert!(
            store
                .stored_edges()
                .unwrap()
                .contains_key(unseen.id.as_str())
        );
    }

    #[test]
    fn incomplete_scan_unions_claims_and_complete_scan_replaces_with_tombstones() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"replacement-session");
        let document = sid(IdKind::Document, b"replacement-document");
        let parent = sid(IdKind::Message, b"replacement-parent");
        let old_message = sid(IdKind::Message, b"replacement-old");
        let new_message = sid(IdKind::Message, b"replacement-new");
        let old_placement = placement(&session, &document, &old_message, 1, false, Some((1, 5)));
        let new_placement = placement(&session, &document, &new_message, 2, false, Some((6, 10)));

        let initial = source_batch(
            "replacement.jsonl",
            [&session, &document, &parent, &old_message]
                .into_iter()
                .map(entity_entry)
                .collect(),
            vec![old_placement.clone()],
            vec![reply_edge(&old_placement, &parent)],
            true,
        );
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&initial))
            .unwrap();

        let incomplete = source_batch(
            "replacement.jsonl",
            [&session, &document, &parent, &new_message]
                .into_iter()
                .map(entity_entry)
                .collect(),
            vec![new_placement.clone()],
            vec![reply_edge(&new_placement, &parent)],
            false,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&incomplete))
                .unwrap()
        );
        assert!(store.get(&old_message).unwrap().is_some());
        assert!(store.get(&new_message).unwrap().is_some());
        assert!(
            store
                .stored_placements()
                .unwrap()
                .contains_key(old_placement.id.as_str())
        );
        assert!(
            store
                .stored_edges()
                .unwrap()
                .contains_key(old_placement.id.as_str())
        );
        let expected_claims: Vec<_> = [
            old_placement.id.as_str().to_string(),
            new_placement.id.as_str().to_string(),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
        assert_eq!(
            source_placement_claims(&store, "replacement.jsonl"),
            expected_claims
        );
        assert!(!relation_complete_marker(&store, "replacement.jsonl"));

        let complete = source_batch(
            "replacement.jsonl",
            [&session, &document, &parent, &new_message]
                .into_iter()
                .map(entity_entry)
                .collect(),
            vec![new_placement.clone()],
            vec![reply_edge(&new_placement, &parent)],
            true,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&complete))
                .unwrap()
        );
        assert!(store.get(&old_message).unwrap().is_none());
        assert!(store.get(&new_message).unwrap().is_some());
        assert!(
            !store
                .stored_placements()
                .unwrap()
                .contains_key(old_placement.id.as_str())
        );
        assert!(
            !store
                .stored_edges()
                .unwrap()
                .contains_key(old_placement.id.as_str())
        );
        assert_eq!(
            source_placement_claims(&store, "replacement.jsonl"),
            vec![new_placement.id.as_str().to_string()]
        );
        assert!(relation_complete_marker(&store, "replacement.jsonl"));
    }

    #[test]
    fn complete_empty_replacement_preserves_shared_facts_then_tombstones_last_claim() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"shared-survival-session");
        let document = sid(IdKind::Document, b"shared-survival-document");
        let parent = sid(IdKind::Message, b"shared-survival-parent");
        let child = sid(IdKind::Message, b"shared-survival-child");
        let child_placement = placement(&session, &document, &child, 1, false, Some((2, 8)));
        let entries = || {
            [&session, &document, &parent, &child]
                .into_iter()
                .map(entity_entry)
                .collect()
        };
        let edge = reply_edge(&child_placement, &parent);
        let sources = [
            source_batch(
                "shared-a.jsonl",
                entries(),
                vec![child_placement.clone()],
                vec![edge.clone()],
                true,
            ),
            source_batch(
                "shared-b.jsonl",
                entries(),
                vec![child_placement.clone()],
                vec![edge],
                true,
            ),
        ];
        store.commit_source_batches_if_changed(&sources).unwrap();

        let empty_a = source_batch("shared-a.jsonl", Vec::new(), Vec::new(), Vec::new(), true);
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&empty_a))
                .unwrap()
        );
        assert!(store.get(&child).unwrap().is_some());
        assert!(
            store
                .stored_placements()
                .unwrap()
                .contains_key(child_placement.id.as_str())
        );
        assert!(
            store
                .stored_edges()
                .unwrap()
                .contains_key(child_placement.id.as_str())
        );
        assert!(source_placement_claims(&store, "shared-a.jsonl").is_empty());
        assert_eq!(
            source_placement_claims(&store, "shared-b.jsonl"),
            vec![child_placement.id.as_str().to_string()]
        );
        let empty_manifest = latest_index_batch(&store).source_replacements.remove(0);
        assert_eq!(empty_manifest["source_path"], "shared-a.jsonl");
        assert_eq!(empty_manifest["entity_memberships"], serde_json::json!([]));
        assert_eq!(empty_manifest["placement_ids"], serde_json::json!([]));
        assert_eq!(empty_manifest["relation_complete"], true);

        let generation = store.active_generation().unwrap();
        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&empty_a))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), generation);

        let empty_b = source_batch("shared-b.jsonl", Vec::new(), Vec::new(), Vec::new(), true);
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&empty_b))
                .unwrap()
        );
        assert!(store.get(&child).unwrap().is_none());
        assert!(
            !store
                .stored_placements()
                .unwrap()
                .contains_key(child_placement.id.as_str())
        );
        assert!(
            !store
                .stored_edges()
                .unwrap()
                .contains_key(child_placement.id.as_str())
        );
    }

    #[test]
    fn shared_relation_change_requires_all_claimants_in_one_complete_batch() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"shared-change-session");
        let document = sid(IdKind::Document, b"shared-change-document");
        let parent_a = sid(IdKind::Message, b"shared-change-parent-a");
        let parent_b = sid(IdKind::Message, b"shared-change-parent-b");
        let child = sid(IdKind::Message, b"shared-change-child");
        let original = placement(&session, &document, &child, 1, false, Some((10, 20)));
        let changed = placement(&session, &document, &child, 1, true, Some((11, 21)));
        assert_eq!(original.id, changed.id);
        let entries = || {
            [&session, &document, &parent_a, &parent_b, &child]
                .into_iter()
                .map(entity_entry)
                .collect()
        };
        let initial = [
            source_batch(
                "change-a.jsonl",
                entries(),
                vec![original.clone()],
                vec![reply_edge(&original, &parent_a)],
                true,
            ),
            source_batch(
                "change-b.jsonl",
                entries(),
                vec![original.clone()],
                vec![reply_edge(&original, &parent_a)],
                true,
            ),
        ];
        store.commit_source_batches_if_changed(&initial).unwrap();
        let generation = store.active_generation().unwrap();

        let only_a = source_batch(
            "change-a.jsonl",
            entries(),
            vec![changed.clone()],
            vec![reply_edge(&changed, &parent_a)],
            true,
        );
        let err = store
            .commit_source_batches_if_changed(std::slice::from_ref(&only_a))
            .unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("did not observe the same placement"))
        );
        assert_eq!(store.active_generation().unwrap(), generation);
        assert!(
            store
                .stored_placements()
                .unwrap()
                .get(original.id.as_str())
                .unwrap()
                .matches(&original)
        );

        let changed_both = [
            source_batch(
                "change-a.jsonl",
                entries(),
                vec![changed.clone()],
                vec![reply_edge(&changed, &parent_b)],
                true,
            ),
            source_batch(
                "change-b.jsonl",
                entries(),
                vec![changed.clone()],
                vec![reply_edge(&changed, &parent_b)],
                true,
            ),
        ];
        assert!(
            store
                .commit_source_batches_if_changed(&changed_both)
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), generation + 1);
        assert!(
            store
                .stored_placements()
                .unwrap()
                .get(changed.id.as_str())
                .unwrap()
                .matches(&changed)
        );
        assert!(
            store
                .stored_edges()
                .unwrap()
                .get(changed.id.as_str())
                .unwrap()
                .matches(&reply_edge(&changed, &parent_b))
        );
    }

    #[test]
    fn edge_only_change_requires_every_shared_claimant_to_observe_the_new_edge() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"shared-edge-session");
        let document = sid(IdKind::Document, b"shared-edge-document");
        let parent_a = sid(IdKind::Message, b"shared-edge-parent-a");
        let parent_b = sid(IdKind::Message, b"shared-edge-parent-b");
        let child = sid(IdKind::Message, b"shared-edge-child");
        let child_placement = placement(&session, &document, &child, 1, false, Some((3, 9)));
        let entries = || {
            [&session, &document, &parent_a, &parent_b, &child]
                .into_iter()
                .map(entity_entry)
                .collect()
        };
        let initial = [
            source_batch(
                "shared-edge-a.jsonl",
                entries(),
                vec![child_placement.clone()],
                vec![reply_edge(&child_placement, &parent_a)],
                true,
            ),
            source_batch(
                "shared-edge-b.jsonl",
                entries(),
                vec![child_placement.clone()],
                vec![reply_edge(&child_placement, &parent_a)],
                true,
            ),
        ];
        store.commit_source_batches_if_changed(&initial).unwrap();
        let generation = store.active_generation().unwrap();

        let only_a = source_batch(
            "shared-edge-a.jsonl",
            entries(),
            vec![child_placement.clone()],
            vec![reply_edge(&child_placement, &parent_b)],
            true,
        );
        let err = store
            .commit_source_batches_if_changed(std::slice::from_ref(&only_a))
            .unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("did not observe the same edge"))
        );
        assert_eq!(store.active_generation().unwrap(), generation);
        assert!(
            store
                .stored_edges()
                .unwrap()
                .get(child_placement.id.as_str())
                .unwrap()
                .matches(&reply_edge(&child_placement, &parent_a))
        );

        let changed_both = [
            source_batch(
                "shared-edge-a.jsonl",
                entries(),
                vec![child_placement.clone()],
                vec![reply_edge(&child_placement, &parent_b)],
                true,
            ),
            source_batch(
                "shared-edge-b.jsonl",
                entries(),
                vec![child_placement.clone()],
                vec![reply_edge(&child_placement, &parent_b)],
                true,
            ),
        ];
        assert!(
            store
                .commit_source_batches_if_changed(&changed_both)
                .unwrap()
        );
        assert!(
            store
                .stored_edges()
                .unwrap()
                .get(child_placement.id.as_str())
                .unwrap()
                .matches(&reply_edge(&child_placement, &parent_b))
        );
    }

    #[test]
    fn relation_apply_failure_rolls_back_everything_except_building_intent() {
        let store = SqliteStore::open_in_memory().unwrap();
        store
            .conn
            .borrow()
            .execute_batch(
                "CREATE TRIGGER fail_relation_insert
                 BEFORE INSERT ON message_placements
                 BEGIN
                     SELECT RAISE(ABORT, 'injected relation failure');
                 END;",
            )
            .unwrap();
        let session = sid(IdKind::Session, b"rollback-session");
        let document = sid(IdKind::Document, b"rollback-document");
        let message = sid(IdKind::Message, b"rollback-message");
        let message_placement = placement(&session, &document, &message, 0, false, Some((0, 4)));
        let source = source_batch(
            "rollback.jsonl",
            [&session, &document, &message]
                .into_iter()
                .map(entity_entry)
                .collect(),
            vec![message_placement],
            Vec::new(),
            true,
        );

        let err = store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap_err();
        assert!(matches!(err, PortError::Backend(_)));
        assert_eq!(store.active_generation().unwrap(), 0);
        for table in [
            "catalog",
            "fts",
            "fts_ids",
            "source_membership",
            "source_scans",
            "message_placements",
            "message_edges",
            "source_placement_membership",
            "source_relation_scans",
        ] {
            assert_eq!(table_count(&store, table), 0, "{table} must roll back");
        }
        let batch = latest_index_batch(&store);
        assert_eq!(batch.state, "building");
        assert_eq!(batch.durable_point, "intent");
    }

    #[test]
    fn deleting_by_wire_id_removes_search_row() {
        let store = SqliteStore::open_in_memory().unwrap();
        let original = sid(IdKind::Message, b"wire-delete");
        store.index(&original, "wire deletion text").unwrap();
        let wire_id = StableId::from_wire(original.as_str()).unwrap();
        let entries: [(StableId, Vec<u8>, String); 0] = [];
        let pending = store
            .begin_index_batch(&entries, std::slice::from_ref(&wire_id))
            .unwrap();
        store
            .commit_index_batch(&pending, &entries, &[wire_id])
            .unwrap();
        assert_eq!(store.count().unwrap(), 0);
        assert!(store.query("deletion", 10).unwrap().is_empty());
    }

    #[test]
    fn standalone_index_populates_wire_mapping() {
        let store = SqliteStore::open_in_memory().unwrap();
        let original = sid(IdKind::Message, b"standalone-map");
        store.index(&original, "standalone mapping text").unwrap();
        let wire_id = StableId::from_wire(original.as_str()).unwrap();
        let entries: [(StableId, Vec<u8>, String); 0] = [];
        let pending = store
            .begin_index_batch(&entries, std::slice::from_ref(&wire_id))
            .unwrap();
        store
            .commit_index_batch(&pending, &entries, &[wire_id])
            .unwrap();
        assert!(store.query("mapping", 10).unwrap().is_empty());
    }

    #[test]
    fn fts_rowids_track_rows_across_commit_rebuild_and_delete() {
        // 回归：fts5 的 id 列是内容列不是 rowid；fts_ids.fts_rowid 边车必须与
        // fts 行一一对应，且对批量提交、rebuild、按 wire 别名删除保持一致。
        let store = SqliteStore::open_in_memory().unwrap();
        let messages: Vec<StableId> = (0..50)
            .map(|i| sid(IdKind::Message, format!("rowid-{i}").as_bytes()))
            .collect();
        let ses = sid(IdKind::Session, b"rowid-container");
        let source = SourceBatch {
            source_path: "rowid.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: messages
                .iter()
                .enumerate()
                // payload 需与 text 在 rebuild 重投影下 round-trip：rebuild 从
                // catalog 经 searchable_text 重新提取正文（'\t' 前是 role），
                // 若 payload 不携带正文，rebuild 后 fts 行将不再可搜。
                .map(|(i, id)| {
                    (
                        id.clone(),
                        format!("user\trowid text {i}").into_bytes(),
                        format!("rowid text {i}"),
                    )
                })
                .chain(std::iter::once((ses.clone(), b"s".to_vec(), String::new())))
                .collect(),
        };
        assert!(store.commit_batch_if_changed(&source.entries).unwrap());
        let conn = store.conn.borrow();
        let mismatch: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts f
                 JOIN fts_ids fi ON fi.id_json = f.id
                 WHERE fi.fts_rowid IS NULL OR fi.fts_rowid != f.rowid",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            mismatch, 0,
            "every fts row must carry its rowid in the sidecar"
        );
        let fts_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM fts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fts_rows, 50);
        let session_rid: Option<i64> = conn
            .query_row(
                "SELECT fts_rowid FROM fts_ids WHERE wire_id = ?1",
                [ses.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(session_rid, None, "non-message sidecar rows stay NULL");
        drop(conn);

        // rebuild 整表清空重投影后映射仍然成立。
        store.rebuild_index().unwrap();
        let conn = store.conn.borrow();
        let mismatch: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts f
                 JOIN fts_ids fi ON fi.id_json = f.id
                 WHERE fi.fts_rowid IS NULL OR fi.fts_rowid != f.rowid",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(mismatch, 0, "rebuild must restore the rowid sidecar");
        drop(conn);

        // 按 wire 别名（from_wire 降级为 Unstable）删除仍能定位到 fts 行。
        let victim = messages[17].clone();
        let wire_id = StableId::from_wire(victim.as_str()).unwrap();
        let entries: [(StableId, Vec<u8>, String); 0] = [];
        let pending = store
            .begin_index_batch(&entries, std::slice::from_ref(&wire_id))
            .unwrap();
        store
            .commit_index_batch(&pending, &entries, &[wire_id])
            .unwrap();
        assert!(
            store.query("rowid text 17", 10).unwrap().is_empty(),
            "alias delete must remove the fts row"
        );
        assert_eq!(store.query("rowid text 16", 10).unwrap().len(), 1);
        assert_eq!(store.count().unwrap(), 50);
    }

    #[test]
    fn large_store_delete_is_rowid_scoped_not_content_scanned() {
        // 回归（性能护栏）：10K 消息库上删除单条。旧实现按内容列 id 比较，
        // 每次删除整表扫描 fts（10K 行约 6.3s）；新实现经 fts_ids.fts_rowid
        // 按 rowid 定位（µs 级）。3s 宽限只拦内容扫描回归，对 rowid 路径有
        // 数个数量级的余量，不依赖计时精度。
        let store = SqliteStore::open_in_memory().unwrap();
        let messages: Vec<StableId> = (0..10_000)
            .map(|i| sid(IdKind::Message, format!("bulk-{i}").as_bytes()))
            .collect();
        let source = SourceBatch {
            source_path: "bulk.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: messages
                .iter()
                .enumerate()
                .map(|(i, id)| (id.clone(), b"m".to_vec(), format!("bulk text {i}")))
                .collect(),
        };
        assert!(store.commit_batch_if_changed(&source.entries).unwrap());
        assert_eq!(store.query("bulk text 5000", 10).unwrap().len(), 1);
        let victim = messages[5000].clone();
        let entries: [(StableId, Vec<u8>, String); 0] = [];
        let pending = store
            .begin_index_batch(&entries, std::slice::from_ref(&victim))
            .unwrap();
        let started = std::time::Instant::now();
        store
            .commit_index_batch(&pending, &entries, &[victim])
            .unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(3),
            "delete must not scan the fts table: {elapsed:?}"
        );
        assert!(store.query("bulk text 5000", 10).unwrap().is_empty());
        assert_eq!(store.query("bulk text 4999", 10).unwrap().len(), 1);
    }

    #[test]
    fn v7_open_backfills_fts_rowid_for_legacy_rows() {
        // 旧 v7 库（fts_rowid 列加入前建成）首次打开必须回填边车：fts 行按
        // id_json 与 fts_ids 一一对应，回填后按 wire 别名删除才能按 rowid 定位。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-fts.db");
        let p = path.to_string_lossy().into_owned();
        let legacy = sid(IdKind::Message, b"legacy-fts-row");
        let legacy_json = serde_json::to_string(&legacy).unwrap();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            // 旧式 fts/fts_ids：fts 行由 fts5 自动分配 rowid，边车没有 rowid
            // 概念；随后 open 会走 v6→v7 + 本列一次性回填。
            conn.execute(
                "INSERT INTO catalog(id, payload) VALUES(?1, ?2)",
                rusqlite::params![legacy.as_str(), b"legacy".to_vec()],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO fts(id, text) VALUES(?1, ?2)",
                rusqlite::params![legacy_json, "legacy fts body"],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO fts_ids(wire_id, id_json) VALUES(?1, ?2)",
                rusqlite::params![legacy.as_str(), legacy_json],
            )
            .unwrap();
            // 容器实体：无 fts 行，回填后 fts_rowid 必须保持 NULL。
            conn.execute(
                "INSERT INTO fts_ids(wire_id, id_json) VALUES(?1, ?2)",
                rusqlite::params![
                    "legacy-session",
                    serde_json::to_string(&sid(IdKind::Session, b"legacy-session")).unwrap()
                ],
            )
            .unwrap();
        }
        let store = open_migration_fixture(&p);
        let conn = store.conn.borrow();
        let mismatch: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fts f
                 JOIN fts_ids fi ON fi.id_json = f.id
                 WHERE fi.fts_rowid IS NULL OR fi.fts_rowid != f.rowid",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(mismatch, 0, "legacy fts rows must be backfilled on open");
        let session_rid: Option<i64> = conn
            .query_row(
                "SELECT fts_rowid FROM fts_ids WHERE wire_id = 'legacy-session'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(session_rid, None);
        drop(conn);

        // 回填后按 wire 别名删除能定位到旧 fts 行（无 fts 残留）。
        // 断言直接查投影行数而非走 FTS MATCH：这个手工 v7 夹具的词元由"旧
        // 二进制"写下，迁到 v17 后投影版本戳为 0，查询路径按契约 fail-closed
        // （见 stale_index_projection_refuses_instead_of_returning_wrong_cjk_hits）。
        let wire_id = StableId::from_wire(legacy.as_str()).unwrap();
        let entries: [(StableId, Vec<u8>, String); 0] = [];
        let pending = store
            .begin_index_batch(&entries, std::slice::from_ref(&wire_id))
            .unwrap();
        store
            .commit_index_batch(&pending, &entries, &[wire_id])
            .unwrap();
        assert_eq!(table_count(&store, "fts"), 0, "旧 fts 行必须被删除");
        assert_eq!(store.count().unwrap(), 0);
    }

    #[test]
    fn source_rescan_tombstones_removed_messages() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"source-a");
        let b = sid(IdKind::Message, b"source-b");
        let first = SourceBatch {
            source_path: "fixture.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![
                (a.clone(), b"a".to_vec(), "keep alpha".into()),
                (b.clone(), b"b".to_vec(), "remove beta".into()),
            ],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&first))
                .unwrap()
        );
        assert_eq!(store.count().unwrap(), 2);
        assert_eq!(store.query("beta", 10).unwrap().len(), 1);

        let second = SourceBatch {
            source_path: "fixture.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(a.clone(), b"a".to_vec(), "keep alpha".into())],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&second))
                .unwrap()
        );
        assert_eq!(store.count().unwrap(), 1);
        assert!(store.get(&b).unwrap().is_none());
        assert!(store.query("beta", 10).unwrap().is_empty());
        assert_eq!(store.query("alpha", 10).unwrap().len(), 1);
    }

    #[test]
    fn unchanged_source_rescan_does_not_advance_generation() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"same-source");
        let source = SourceBatch {
            source_path: "same.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(a, b"payload".to_vec(), "same text".into())],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), 1);
        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), 1);
    }

    #[test]
    fn v3_db_migrates_source_membership_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v3.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            conn.execute_batch(
                "CREATE TABLE catalog (id TEXT PRIMARY KEY, payload BLOB NOT NULL);
                 CREATE VIRTUAL TABLE fts USING fts5(id UNINDEXED, text);
                 CREATE TABLE store_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), active_generation INTEGER NOT NULL);
                 INSERT INTO store_metadata(singleton, active_generation) VALUES(1, 0);
                 CREATE TABLE index_batches (
                     operation_id TEXT PRIMARY KEY, base_generation INTEGER NOT NULL,
                     target_generation INTEGER NOT NULL, state TEXT NOT NULL,
                     operation_digest TEXT NOT NULL, upsert_ids_json TEXT NOT NULL,
                     delete_ids_json TEXT NOT NULL, durable_point TEXT NOT NULL,
                     created_at_ms INTEGER NOT NULL, committed_at_ms INTEGER,
                     error_code TEXT
                 );
                 CREATE TABLE fts_ids (wire_id TEXT PRIMARY KEY, id_json TEXT NOT NULL UNIQUE);
                 PRAGMA user_version = 3;",
            )
            .unwrap();
        }
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let conn = rusqlite::Connection::open(&p).unwrap();
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'source_membership'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(exists, 1);
        drop(store);
    }

    #[test]
    fn v5_db_migrates_membership_document_id_column() {
        // 带数据的 v5 库升级到 v6：membership 旧行保留且 document_id 为 NULL。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v5.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            conn.execute_batch(
                "CREATE TABLE catalog (id TEXT PRIMARY KEY, payload BLOB NOT NULL);
                 CREATE VIRTUAL TABLE fts USING fts5(id UNINDEXED, text);
                 CREATE TABLE store_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), active_generation INTEGER NOT NULL);
                 INSERT INTO store_metadata(singleton, active_generation) VALUES(1, 1);
                 CREATE TABLE index_batches (
                     operation_id TEXT PRIMARY KEY, base_generation INTEGER NOT NULL,
                     target_generation INTEGER NOT NULL, state TEXT NOT NULL,
                     operation_digest TEXT NOT NULL, upsert_ids_json TEXT NOT NULL,
                     delete_ids_json TEXT NOT NULL, durable_point TEXT NOT NULL,
                     created_at_ms INTEGER NOT NULL, committed_at_ms INTEGER,
                     error_code TEXT
                 );
                 CREATE TABLE fts_ids (wire_id TEXT PRIMARY KEY, id_json TEXT NOT NULL UNIQUE);
                 CREATE TABLE source_membership (
                     source_path TEXT NOT NULL,
                     message_id  TEXT NOT NULL,
                     PRIMARY KEY(source_path, message_id)
                 );
                 CREATE TABLE source_scans (
                     source_path   TEXT PRIMARY KEY,
                     scanned_at_ms INTEGER NOT NULL
                 );
                 INSERT INTO catalog(id, payload) VALUES('msg_v1_legacy', X'01');
                 INSERT INTO source_membership(source_path, message_id)
                 VALUES('legacy.jsonl', 'msg_v1_legacy');
                 INSERT INTO source_scans(source_path, scanned_at_ms) VALUES('legacy.jsonl', 1);
                 PRAGMA user_version = 5;",
            )
            .unwrap();
        }
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        // 旧数据完整保留。
        let id = StableId::from_wire("msg_v1_legacy").unwrap();
        assert_eq!(store.get(&id).unwrap().unwrap(), vec![1u8]);
        drop(store);
        let conn = rusqlite::Connection::open(&p).unwrap();
        let doc: Option<String> = conn
            .query_row(
                "SELECT document_id FROM source_membership WHERE message_id = 'msg_v1_legacy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        // v6 前的 membership 行无文档归属信息——显式 NULL，不臆造。
        assert_eq!(doc, None);
    }

    #[test]
    fn non_message_entities_are_catalog_only() {
        // session/document 实体入 catalog、可 get/list，但绝不进入全文搜索。
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"cat-only-msg");
        let ses = sid(IdKind::Session, b"cat-only-ses");
        let doc = sid(IdKind::Document, b"cat-only-doc");
        let source = SourceBatch {
            source_path: "mixed.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![
                (msg.clone(), b"m".to_vec(), "unique searchable body".into()),
                (ses.clone(), b"s".to_vec(), "unique searchable body".into()),
                (doc.clone(), b"d".to_vec(), "unique searchable body".into()),
            ],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap()
        );
        // catalog 三个实体都在。
        assert_eq!(store.count().unwrap(), 3);
        assert!(store.get(&ses).unwrap().is_some());
        assert!(store.get(&doc).unwrap().is_some());
        // 搜索只命中消息——容器实体不参与全文命中，避免重复计数。
        let hits = store.query("searchable", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id.as_str(), msg.as_str());
        // fts_ids 身份边车对所有 kind 保留（rebuild 依赖它保真身份）。
        let conn = store.conn.borrow();
        let sidecar: i64 = conn
            .query_row("SELECT COUNT(*) FROM fts_ids", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sidecar, 3);
        // 重复提交同一批是内容级 no-op（非消息实体不因缺 fts 行而误判为变更）。
        drop(conn);
        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap()
        );
    }

    #[test]
    fn rebuild_keeps_non_message_entities_out_of_fts() {
        // 混合库 rebuild：身份保真、消息重投影、容器实体仍不进 fts。
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"rebuild-msg");
        let ses = sid(IdKind::Session, b"rebuild-ses");
        let source = SourceBatch {
            source_path: "rebuild.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![
                (
                    msg.clone(),
                    b"role\tbody words".to_vec(),
                    "body words".into(),
                ),
                (ses.clone(), b"s".to_vec(), String::new()),
            ],
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap();
        let rebuilt = store.rebuild_index().unwrap();
        assert_eq!(rebuilt, 2);
        // 消息可搜、身份保真（非 Unstable——来自 fts_ids 边车）。
        let hits = store.query("body", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id.stability(), Stability::Reconstructed);
        // 容器实体：无 fts 行、有 fts_ids 边车。
        let conn = store.conn.borrow();
        let fts_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM fts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(fts_rows, 1);
        let sidecar: i64 = conn
            .query_row("SELECT COUNT(*) FROM fts_ids", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sidecar, 2);
    }

    #[test]
    fn source_rescan_retires_session_and_document_rows() {
        // 源缩水成空 scan：其 session/document 目录行随消息一起 tombstone。
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"retire-msg");
        let ses = sid(IdKind::Session, b"retire-ses");
        let doc = sid(IdKind::Document, b"retire-doc");
        let full = SourceBatch {
            source_path: "retire.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![
                (msg.clone(), b"m".to_vec(), "text".into()),
                (ses.clone(), b"s".to_vec(), String::new()),
                (doc.clone(), b"d".to_vec(), String::new()),
            ],
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&full))
            .unwrap();
        // membership 记录了该源的文档归属。
        {
            let conn = store.conn.borrow();
            let recorded: Option<String> = conn
                .query_row(
                    "SELECT document_id FROM source_membership WHERE message_id = ?1",
                    [msg.as_str()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(recorded.as_deref(), Some(doc.as_str()));
        }
        let empty = SourceBatch {
            source_path: "retire.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![],
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&empty))
            .unwrap();
        assert_eq!(store.count().unwrap(), 0);
        assert!(store.get(&ses).unwrap().is_none());
        assert!(store.get(&doc).unwrap().is_none());
    }

    #[test]
    fn shared_entity_survives_other_source_rescan() {
        // 两个源共享同一实体：一个源消失不退役另一源仍引用的实体。
        let store = SqliteStore::open_in_memory().unwrap();
        let shared = sid(IdKind::Document, b"shared-doc");
        let m1 = sid(IdKind::Message, b"share-m1");
        let m2 = sid(IdKind::Message, b"share-m2");
        let sources = [
            SourceBatch {
                source_path: "one.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![
                    (m1.clone(), b"m1".to_vec(), "one text".into()),
                    (shared.clone(), b"d".to_vec(), String::new()),
                ],
            },
            SourceBatch {
                source_path: "two.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![
                    (m2.clone(), b"m2".to_vec(), "two text".into()),
                    (shared.clone(), b"d".to_vec(), String::new()),
                ],
            },
        ];
        store.commit_source_batches_if_changed(&sources).unwrap();
        assert_eq!(store.count().unwrap(), 3);
        // 源 one 变空：m1 退役；shared 仍被 two 引用，保留。
        let shrunk = SourceBatch {
            source_path: "one.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![],
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&shrunk))
            .unwrap();
        assert!(store.get(&m1).unwrap().is_none());
        assert!(store.get(&shared).unwrap().is_some());
        assert!(store.get(&m2).unwrap().is_some());
    }

    /// Canonical session payload for a source contributing `members`.
    fn session_payload(document: &str, members: &[&str]) -> Vec<u8> {
        serde_json::json!({
            "document": document,
            "documents": [document],
            "messages": members,
        })
        .to_string()
        .into_bytes()
    }

    fn session_members(store: &SqliteStore, id: &StableId) -> Vec<String> {
        let bytes = store.get(id).unwrap().expect("session must be present");
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["messages"]
            .as_array()
            .expect("messages array")
            .iter()
            .map(|entry| entry.as_str().expect("member is a string").to_string())
            .collect()
    }

    fn session_documents(store: &SqliteStore, id: &StableId) -> Vec<String> {
        let bytes = store.get(id).unwrap().expect("session must be present");
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["documents"]
            .as_array()
            .expect("documents array")
            .iter()
            .map(|entry| entry.as_str().expect("document is a string").to_string())
            .collect()
    }

    #[test]
    fn session_spanning_two_sources_in_one_batch_unions_its_members() {
        // 真实形态：一个逻辑会话被拆到多个 transcript 文件，每个源只声明自己那部分
        // 成员。旧行为把这判为冲突投影并拒绝整批（exit 6）；正确行为是取并集。
        let store = SqliteStore::open_in_memory().unwrap();
        let ses = sid(IdKind::Session, b"split-session");
        let doc_a = sid(IdKind::Document, b"split-doc-a");
        let doc_b = sid(IdKind::Document, b"split-doc-b");
        let m1 = sid(IdKind::Message, b"split-m1");
        let m2 = sid(IdKind::Message, b"split-m2");
        let sources = [
            SourceBatch {
                source_path: "part-a.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![
                    (m1.clone(), b"m1".to_vec(), "first half".into()),
                    (
                        ses.clone(),
                        session_payload(doc_a.as_str(), &[m1.as_str()]),
                        String::new(),
                    ),
                    (doc_a.clone(), b"da".to_vec(), String::new()),
                ],
            },
            SourceBatch {
                source_path: "part-b.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![
                    (m2.clone(), b"m2".to_vec(), "second half".into()),
                    (
                        ses.clone(),
                        session_payload(doc_b.as_str(), &[m2.as_str()]),
                        String::new(),
                    ),
                    (doc_b.clone(), b"db".to_vec(), String::new()),
                ],
            },
        ];
        assert!(store.commit_source_batches_if_changed(&sources).unwrap());
        assert_eq!(
            session_members(&store, &ses),
            vec![m1.as_str().to_string(), m2.as_str().to_string()],
        );
        // 两个贡献文档都保留；单值别名取升序首个，供旧读取方使用。
        let mut expected_docs = vec![doc_a.as_str().to_string(), doc_b.as_str().to_string()];
        expected_docs.sort();
        assert_eq!(session_documents(&store, &ses), expected_docs);
        let bytes = store.get(&ses).unwrap().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["document"], expected_docs[0].as_str());
    }

    #[test]
    fn session_synced_in_separate_batches_accumulates_members() {
        // 真实语料按批提交（命令行长度上限），所以合并必须以库中现值为起点：
        // 否则第二批的成员列表会覆盖第一批，只剩最后一批的成员。
        let store = SqliteStore::open_in_memory().unwrap();
        let ses = sid(IdKind::Session, b"batched-session");
        let doc_a = sid(IdKind::Document, b"batched-doc-a");
        let doc_b = sid(IdKind::Document, b"batched-doc-b");
        let m1 = sid(IdKind::Message, b"batched-m1");
        let m2 = sid(IdKind::Message, b"batched-m2");

        let first = SourceBatch {
            source_path: "batch-a.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![
                (m1.clone(), b"m1".to_vec(), "batch a".into()),
                (
                    ses.clone(),
                    session_payload(doc_a.as_str(), &[m1.as_str()]),
                    String::new(),
                ),
                (doc_a.clone(), b"da".to_vec(), String::new()),
            ],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&first))
                .unwrap()
        );

        let second = SourceBatch {
            source_path: "batch-b.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![
                (m2.clone(), b"m2".to_vec(), "batch b".into()),
                (
                    ses.clone(),
                    session_payload(doc_b.as_str(), &[m2.as_str()]),
                    String::new(),
                ),
                (doc_b.clone(), b"db".to_vec(), String::new()),
            ],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&second))
                .unwrap()
        );

        assert_eq!(
            session_members(&store, &ses),
            vec![m1.as_str().to_string(), m2.as_str().to_string()],
            "第二批不得覆盖第一批的成员",
        );
        assert_eq!(session_documents(&store, &ses).len(), 2);
    }

    #[test]
    fn resyncing_a_cross_source_session_is_a_content_level_noop() {
        // 合并结果必须稳定：同一语料重复 sync 不得推进 generation，否则每次运行都
        // 会作废所有分页 cursor。
        let store = SqliteStore::open_in_memory().unwrap();
        let ses = sid(IdKind::Session, b"noop-session");
        let doc_a = sid(IdKind::Document, b"noop-doc-a");
        let doc_b = sid(IdKind::Document, b"noop-doc-b");
        let m1 = sid(IdKind::Message, b"noop-m1");
        let m2 = sid(IdKind::Message, b"noop-m2");
        let sources = [
            SourceBatch {
                source_path: "noop-a.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![
                    (m1.clone(), b"m1".to_vec(), "noop a".into()),
                    (
                        ses.clone(),
                        session_payload(doc_a.as_str(), &[m1.as_str()]),
                        String::new(),
                    ),
                    (doc_a.clone(), b"da".to_vec(), String::new()),
                ],
            },
            SourceBatch {
                source_path: "noop-b.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![
                    (m2.clone(), b"m2".to_vec(), "noop b".into()),
                    (
                        ses.clone(),
                        session_payload(doc_b.as_str(), &[m2.as_str()]),
                        String::new(),
                    ),
                    (doc_b.clone(), b"db".to_vec(), String::new()),
                ],
            },
        ];
        assert!(store.commit_source_batches_if_changed(&sources).unwrap());
        let generation = store.active_generation().unwrap();
        assert!(
            !store.commit_source_batches_if_changed(&sources).unwrap(),
            "重复提交同一跨源语料应为内容级 no-op",
        );
        assert_eq!(store.active_generation().unwrap(), generation);
    }

    #[test]
    fn legacy_single_document_session_upgrades_without_losing_members() {
        // 升级前入库的会话行只有单值 `document`，且没有 `documents` 数组。
        // 新二进制再次 sync 时必须把旧成员并进来，而不是丢弃或报错。
        let store = SqliteStore::open_in_memory().unwrap();
        let ses = sid(IdKind::Session, b"legacy-session");
        let doc_a = sid(IdKind::Document, b"legacy-doc-a");
        let doc_b = sid(IdKind::Document, b"legacy-doc-b");
        let m1 = sid(IdKind::Message, b"legacy-m1");
        let m2 = sid(IdKind::Message, b"legacy-m2");

        let legacy_payload = serde_json::json!({
            "document": doc_a.as_str(),
            "messages": [m1.as_str()],
        })
        .to_string()
        .into_bytes();
        let legacy = SourceBatch {
            source_path: "legacy-a.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![
                (m1.clone(), b"m1".to_vec(), "legacy a".into()),
                (ses.clone(), legacy_payload, String::new()),
                (doc_a.clone(), b"da".to_vec(), String::new()),
            ],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&legacy))
                .unwrap()
        );

        let modern = SourceBatch {
            source_path: "legacy-b.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![
                (m2.clone(), b"m2".to_vec(), "legacy b".into()),
                (
                    ses.clone(),
                    session_payload(doc_b.as_str(), &[m2.as_str()]),
                    String::new(),
                ),
                (doc_b.clone(), b"db".to_vec(), String::new()),
            ],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&modern))
                .unwrap()
        );

        assert_eq!(
            session_members(&store, &ses),
            vec![m1.as_str().to_string(), m2.as_str().to_string()],
        );
        assert_eq!(session_documents(&store, &ses).len(), 2);
    }

    #[test]
    fn conflicting_message_projections_are_still_rejected() {
        // 合并只对容器实体开放。同一条消息在不同源上投影不同是真实的不一致
        // （同一 native id 却内容不同），必须继续拒绝，不能被容器合并顺带放行。
        let store = SqliteStore::open_in_memory().unwrap();
        let native_id = "private-provider-native-id";
        let msg = StableId::native(IdKind::Message, native_id);
        let sources = [
            SourceBatch {
                source_path: "conflict-a.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(msg.clone(), b"first projection".to_vec(), "one".into())],
            },
            SourceBatch {
                source_path: "conflict-b.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(msg.clone(), b"second projection".to_vec(), "two".into())],
            },
        ];
        let error = store
            .commit_source_batches_if_changed(&sources)
            .expect_err("conflicting message projections must be rejected");
        assert!(
            format!("{error}").contains("conflicting projections"),
            "{error}"
        );
        assert!(!format!("{error}").contains(native_id), "{error}");
        assert!(!format!("{error}").contains(msg.as_str()), "{error}");
    }

    /// Build the canonical message payload shape that ingest writes.
    fn message_payload(session: &str, text: &str) -> Vec<u8> {
        serde_json::json!({
            "role": "user",
            "text": text,
            "parent": null,
            "session": session,
            "span": { "start": 0, "end": 10 },
        })
        .to_string()
        .into_bytes()
    }

    /// Same message as it appears in one specific file: the copy sits at that
    /// file's own byte offsets and names the document it came from.
    fn message_payload_with_span(
        session: &str,
        text: &str,
        document: &str,
        start: u64,
        end: u64,
    ) -> Vec<u8> {
        serde_json::json!({
            "role": "user",
            "text": text,
            "parent": null,
            "session": session,
            "sessions": [session],
            "span": { "start": start, "end": end },
            "spans": [{ "document": document, "start": start, "end": end }],
        })
        .to_string()
        .into_bytes()
    }

    #[test]
    fn codex_timestamp_occurrence_projections_merge_without_conflict() {
        // Codex's old adapter stored the occurrence-local envelope timestamp
        // as the message timestamp; the current adapter emits no stable
        // timestamp. Re-ingesting an old catalog must therefore merge a
        // string timestamp with null instead of reporting a conflict.
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = StableId::native(IdKind::Message, "codex-msg-timestamp");
        let old = serde_json::json!({
            "role": "assistant",
            "text": "same body",
            "timestamp": "2026-07-19T23:40:01.000Z",
        })
        .to_string()
        .into_bytes();
        let new = serde_json::json!({
            "role": "assistant",
            "text": "same body",
            "timestamp": null,
        })
        .to_string()
        .into_bytes();
        let sources = [
            SourceBatch {
                source_path: "old-codex.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(msg.clone(), old, "one".into())],
            },
            SourceBatch {
                source_path: "new-codex.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(msg.clone(), new, "two".into())],
            },
        ];
        store
            .commit_source_batches_if_changed(&sources)
            .expect("occurrence timestamp string/null projections must merge");
        let stored = store.get(&msg).unwrap().unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&stored).unwrap();
        assert_eq!(
            payload.get("timestamp"),
            Some(&serde_json::Value::Null),
            "merged message must carry no stable timestamp"
        );
        assert_eq!(payload.get("text"), Some(&serde_json::json!("same body")));
    }

    #[test]
    fn codex_timestamp_merge_converges_to_null_regardless_of_side() {
        // The string/null convergence must not depend on which projection is
        // the left (merge) side.
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = StableId::native(IdKind::Message, "codex-msg-timestamp-side");
        let string_payload = serde_json::json!({
            "role": "assistant",
            "text": "same body",
            "timestamp": "2026-07-19T23:40:01.000Z",
        })
        .to_string()
        .into_bytes();
        let null_payload = serde_json::json!({
            "role": "assistant",
            "text": "same body",
            "timestamp": null,
        })
        .to_string()
        .into_bytes();
        let sources = [
            SourceBatch {
                source_path: "null-first.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(msg.clone(), null_payload, "one".into())],
            },
            SourceBatch {
                source_path: "string-second.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(msg.clone(), string_payload, "two".into())],
            },
        ];
        store
            .commit_source_batches_if_changed(&sources)
            .expect("null-first/string-second must merge");
        let stored = store.get(&msg).unwrap().unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&stored).unwrap();
        assert_eq!(
            payload.get("timestamp"),
            Some(&serde_json::Value::Null),
            "timestamp must converge to null in either order"
        );
    }

    #[test]
    fn message_copied_into_another_file_unions_its_per_document_spans() {
        // A resumed conversation's history is rewritten into the new transcript,
        // so the same message sits at a different byte offset in each file. Those
        // offsets are per-source facts: keep both, keyed by document, instead of
        // calling the difference a conflict.
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"respanned-msg");
        let sources = [
            SourceBatch {
                source_path: "original.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(
                    msg.clone(),
                    message_payload_with_span("ses_v1_aaa", "same body", "doc_v1_aaa", 0, 929),
                    "same body".into(),
                )],
            },
            SourceBatch {
                source_path: "resumed.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(
                    msg.clone(),
                    message_payload_with_span("ses_v1_bbb", "same body", "doc_v1_bbb", 512, 1322),
                    "same body".into(),
                )],
            },
        ];
        assert!(store.commit_source_batches_if_changed(&sources).unwrap());

        let stored: serde_json::Value =
            serde_json::from_slice(&store.get(&msg).unwrap().unwrap()).unwrap();
        let spans = stored["spans"].as_array().expect("spans array");
        assert_eq!(spans.len(), 2, "both locations must survive: {stored}");
        // Keyed by document and ordered by it, so the result does not depend on
        // which file happened to be scanned first.
        assert_eq!(spans[0]["document"], "doc_v1_aaa");
        assert_eq!(spans[0]["end"], 929);
        assert_eq!(spans[1]["document"], "doc_v1_bbb");
        assert_eq!(spans[1]["start"], 512);
        // The singular alias still names one real location, so evidence
        // assembly keeps reporting byte precision rather than degrading.
        assert_eq!(stored["span"]["start"], 0);
        assert_eq!(stored["span"]["end"], 929);

        // Re-syncing the same corpus changes nothing: spans are keyed by
        // document, so a second pass maps onto the same two entries.
        assert!(!store.commit_source_batches_if_changed(&sources).unwrap());
    }

    #[test]
    fn message_shared_by_resumed_sessions_unions_its_session_refs() {
        // Resuming or forking a session copies history into the new transcript,
        // so one message id legitimately appears under several session ids with
        // otherwise identical content. That must union, not conflict.
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"resumed-msg");
        let sources = [
            SourceBatch {
                source_path: "first.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(
                    msg.clone(),
                    message_payload("ses_v1_aaa", "shared body"),
                    "shared body".into(),
                )],
            },
            SourceBatch {
                source_path: "second.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(
                    msg.clone(),
                    message_payload("ses_v1_bbb", "shared body"),
                    "shared body".into(),
                )],
            },
        ];
        assert!(store.commit_source_batches_if_changed(&sources).unwrap());

        let stored: serde_json::Value =
            serde_json::from_slice(&store.get(&msg).unwrap().unwrap()).unwrap();
        assert_eq!(
            stored["sessions"],
            serde_json::json!(["ses_v1_aaa", "ses_v1_bbb"]),
            "both owning sessions must be recorded: {stored}"
        );
        // The single-value alias keeps pre-union readers working.
        assert_eq!(stored["session"], "ses_v1_aaa");
        // Everything else is untouched by the merge.
        assert_eq!(stored["text"], "shared body");
        assert_eq!(stored["span"]["end"], 10);
    }

    #[test]
    fn message_with_different_text_merges_to_longer_projection() {
        // text is a content projection, not a stable identity field: Claude
        // Code copies a conversation's history into a new transcript on resume
        // or fork, and a copy may carry a different number of content blocks
        // (e.g. a truncated tool_result). Diverging text under one id must not
        // conflict; the merged projection deterministically keeps the longer
        // body so no retrieved content is lost.
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"divergent-msg");
        let sources = [
            SourceBatch {
                source_path: "first.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(
                    msg.clone(),
                    message_payload("ses_v1_aaa", "original body"),
                    "original body".into(),
                )],
            },
            SourceBatch {
                source_path: "second.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(
                    msg.clone(),
                    message_payload("ses_v1_aaa", "a much longer rewritten body"),
                    "a much longer rewritten body".into(),
                )],
            },
        ];
        store
            .commit_source_batches_if_changed(&sources)
            .expect("diverging text is exempt from conflict and must merge");
        // The longer projection wins deterministically.
        let stored = store
            .get(&msg)
            .expect("message must be stored")
            .expect("message must be present");
        let stored_payload: serde_json::Value =
            serde_json::from_slice::<serde_json::Value>(&stored)
                .expect("stored payload must parse");
        assert_eq!(
            stored_payload["text"], "a much longer rewritten body",
            "merged text must keep the longer projection"
        );
    }

    #[test]
    fn merged_message_fts_projects_from_merged_payload() {
        // 回归（Major-1）：合并 payload 后，FTS 正文必须从合并后 payload 经
        // searchable_text 重投影（与 rebuild_index 同一投影函数），而不是取
        // “source_path 排序最后处理的源”的原始 text。长文本在排序靠前的源、
        // 短文本在排序靠后的源时，旧实现把长文本写进 catalog payload 却把短
        // 文本写进 fts——长文本搜不到（需 rebuild 才恢复），且按源分批重同步
        // 会交替改写 fts、每批推进 generation、内容级 no-op 失效。
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"merged-fts-msg");
        let sources = [
            SourceBatch {
                source_path: "aaa-long.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(
                    msg.clone(),
                    message_payload("ses_v1_aaa", "the long body that must stay searchable"),
                    "the long body that must stay searchable".into(),
                )],
            },
            SourceBatch {
                source_path: "zzz-short.jsonl".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(
                    msg.clone(),
                    message_payload("ses_v1_aaa", "short body"),
                    "short body".into(),
                )],
            },
        ];
        assert!(store.commit_source_batches_if_changed(&sources).unwrap());
        // 长文本必须可搜——它是合并 payload 的权威投影。
        assert_eq!(
            store.query("long body", 10).unwrap().len(),
            1,
            "merged payload text must be searchable"
        );
        assert!(
            store.query("short", 10).unwrap().is_empty(),
            "short projection must not shadow the merged text"
        );
        // fts 行文本 == searchable_text(合并后 payload)。
        let conn = store.conn.borrow();
        let fts_text: String = conn
            .query_row(
                "SELECT f.text FROM fts f
                 JOIN fts_ids fi ON fi.id_json = f.id
                 WHERE fi.wire_id = ?1",
                [msg.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        drop(conn);
        assert_eq!(
            fts_text, "the long body that must stay searchable",
            "fts must project from the merged payload"
        );
        // 内容级 no-op 恢复：同一语料重同步不推进 generation。
        let generation = store.active_generation().unwrap();
        assert!(!store.commit_source_batches_if_changed(&sources).unwrap());
        assert_eq!(store.active_generation().unwrap(), generation);
    }

    #[test]
    fn message_payload_only_change_is_not_dropped() {
        // A payload-only source correction must commit even when text is unchanged.
        // It replaces the old observation rather than accumulating historical aliases.
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"payload-only-msg");
        let first = SourceBatch {
            source_path: "payload-only.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(
                msg.clone(),
                message_payload("ses_v1_aaa", "stable body"),
                "stable body".into(),
            )],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&first))
                .unwrap()
        );

        // text 不变、payload 换 session（同一源路径，membership/relations 均不变）。
        let changed = SourceBatch {
            source_path: "payload-only.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(
                msg.clone(),
                message_payload("ses_v1_bbb", "stable body"),
                "stable body".into(),
            )],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&changed))
                .unwrap(),
            "payload-only change must not be silently dropped"
        );
        let stored: serde_json::Value =
            serde_json::from_slice(&store.get(&msg).unwrap().unwrap()).unwrap();
        assert_eq!(
            stored["session"],
            serde_json::json!("ses_v1_bbb"),
            "latest same-source payload must replace the old session ref"
        );
        // 已收敛：内容与存储一致后重同步是 no-op。
        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&changed))
                .unwrap()
        );
    }

    #[test]
    fn changed_source_fingerprint_forces_reparse_and_converges() {
        // 回归（Minor-3）：source_scans 的 len/fingerprint 指纹参与 current 判定
        // （cheap 与 B2 no-op 两条路径都查）。内容等长替换后，缓存指纹不匹配的源
        // 必须重解析并重写缓存；只查扫描行存在会让指纹缓存永不收敛，CLI 每次运行
        // 都重解析全部源。
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"fingerprint-msg");
        let source = |fingerprint: &str| SourceBatch {
            source_path: "fingerprint.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(10),
            fingerprint: Some(fingerprint.to_string()),
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(a.clone(), b"payload".to_vec(), "same text".into())],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source("aaa")))
                .unwrap()
        );
        // 等长异容替换：len 相同、fingerprint 不同 → 不得判为 current。
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source("bbb")))
                .unwrap(),
            "fingerprint change must force a re-scan commit"
        );
        // 指纹已重写为 bbb → 再次重同步是 no-op，缓存收敛。
        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&source("bbb")))
                .unwrap()
        );
        let fingerprints = store
            .source_fingerprints(&["fingerprint.jsonl".to_string()])
            .unwrap();
        assert_eq!(
            fingerprints.get("fingerprint.jsonl"),
            Some(&(
                Some(10),
                Some("bbb".to_string()),
                i64::from(PARSER_SEMANTIC_VERSION)
            ))
        );
    }

    #[test]
    fn stale_parser_version_forces_reparse_and_converges() {
        // 借鉴 Recall 的 parser_version 增量同步：解析语义升级
        // （PARSER_SEMANTIC_VERSION 递增）后，字节未变的源也必须 targeted
        // backfill（重跑 parse + commit），而不是滞留旧解析结果直到手动
        // `index rebuild` 或源文件变化。回归场景：旧库已存行版本落后、
        // len/fingerprint 完全相同——旧逻辑判 unchanged 永不复解析。
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"parser-version-msg");
        let source = SourceBatch {
            source_path: "parser-version.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(10),
            fingerprint: Some("aaa".to_string()),
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(a.clone(), b"payload".to_vec(), "same text".into())],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap()
        );
        // 模拟解析语义升级后的旧库：把已存行的 parser_version 拨回旧值（0），
        // 字节与内容均未变（len/fingerprint 相同）。
        {
            let conn = store.conn.borrow();
            conn.execute(
                "UPDATE source_scans SET parser_version = 0
                 WHERE source_path = 'parser-version.jsonl'",
                [],
            )
            .unwrap();
        }
        // 版本落后 + 字节未变：必须走提交路径重写 source_scans（targeted
        // backfill），否则重解析结果与库一致时 no-op 短路会让版本永不收敛。
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap(),
            "stale parser_version must force a re-scan commit even with unchanged bytes"
        );
        // 版本已写回当前值 → 再次重同步是 no-op，缓存收敛（零 churn）。
        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap()
        );
        let caches = store
            .source_fingerprints(&["parser-version.jsonl".to_string()])
            .unwrap();
        assert_eq!(
            caches.get("parser-version.jsonl"),
            Some(&(
                Some(10),
                Some("aaa".to_string()),
                i64::from(PARSER_SEMANTIC_VERSION)
            ))
        );
    }

    #[test]
    fn open_for_write_accepts_bare_relative_filename() {
        // 回归（Minor-4）：裸相对文件名（"catalog.db"）的 parent() 是空串 "",
        // create_dir_all("") 会报错；空 parent 应按当前工作目录处理。
        let dir = tempfile::tempdir().unwrap();
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();
        let result = SqliteStore::open_for_write("catalog.db");
        std::env::set_current_dir(original).unwrap();
        let store = result.unwrap();
        assert_eq!(store.count().unwrap(), 0);
        assert!(
            dir.path().join("writer.lock").exists(),
            "writer lease must be created next to the bare filename"
        );
        assert!(dir.path().join("catalog.db").exists());
    }

    #[test]
    fn standalone_index_keeps_non_message_out_of_fts() {
        // 回归（Minor-5）：单条 SearchIndex::index 与批量路径一样只让 Message
        // 实体进入 fts 全文表——session/document 是容器实体，索引其正文会让搜索
        // 命中重复计数。
        let store = SqliteStore::open_in_memory().unwrap();
        let ses = sid(IdKind::Session, b"index-container");
        store.index(&ses, "container body").unwrap();
        assert!(
            store.query("container", 10).unwrap().is_empty(),
            "non-message entities must not enter the fts table"
        );
        let conn = store.conn.borrow();
        let fts_rowid: Option<i64> = conn
            .query_row(
                "SELECT fts_rowid FROM fts_ids WHERE wire_id = ?1",
                [ses.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        drop(conn);
        assert_eq!(fts_rowid, None, "non-message sidecar row stays NULL");
        // Message 走同一路径仍正常进 fts。
        let msg = sid(IdKind::Message, b"index-message");
        store.index(&msg, "searchable body").unwrap();
        assert_eq!(store.query("searchable", 10).unwrap().len(), 1);
    }

    #[test]
    fn fresh_schema_carries_fts_rowid_without_open_time_ddl() {
        // 回归（Minor-6）：fts_rowid 列直接进 v3 建表 DDL，新库首次 open（含只读）
        // 不再执行 ALTER+回填事务；旧 v7 库仍走 ensure_fts_ids_rowid 的一次性回填。
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.conn.borrow();
        let has_fts_rowid: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('fts_ids') WHERE name = 'fts_rowid'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        drop(conn);
        assert_eq!(has_fts_rowid, 1);
    }

    #[test]
    fn batch_current_check_is_rowid_scoped_not_content_scanned() {
        // 回归（性能护栏，Minor-8）：batch_is_current 逐条读 fts text 时，旧实现
        // 按内容列 id 比较整表扫描（每条 O(全库)，B1 批量 no-op 判定 O(N²)）；改经
        // fts_ids.fts_rowid 按 rowid 定位后 O(N)。10K 库上内容扫描路径远超 3s 宽限
        // （单次整表扫描约 6.3s），rowid 路径有数量级余量。
        let store = SqliteStore::open_in_memory().unwrap();
        let entries: Vec<(StableId, Vec<u8>, String)> = (0..10_000)
            .map(|i| {
                let id = sid(IdKind::Message, format!("current-{i}").as_bytes());
                (
                    id,
                    format!("payload {i}").into_bytes(),
                    format!("current text {i}"),
                )
            })
            .collect();
        assert!(store.commit_batch_if_changed(&entries).unwrap());
        let started = std::time::Instant::now();
        assert!(!store.commit_batch_if_changed(&entries).unwrap());
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(3),
            "current check must not scan the fts table per message: {elapsed:?}"
        );
    }

    #[test]
    fn b1_delete_rejects_catalog_entity_still_referenced_by_relations() {
        // 回归（Minor-10）：B1 路径（裸 commit_index_batch）删 catalog 实体而 v7
        // 关系行仍引用它时，必须拒绝整批，而不是留下事后才发现的悬空引用。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"dangling-session");
        let document = sid(IdKind::Document, b"dangling-document");
        let message = sid(IdKind::Message, b"dangling-message");
        let p = placement(&session, &document, &message, 0, false, Some((1, 5)));
        let source = source_batch(
            "dangling.jsonl",
            vec![
                (message.clone(), b"m".to_vec(), "dangling text".into()),
                (session.clone(), b"s".to_vec(), String::new()),
                (document.clone(), b"d".to_vec(), String::new()),
            ],
            vec![p],
            Vec::new(),
            true,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&source))
                .unwrap()
        );

        // Reject the unscoped deletion before an intent is created.
        let intents = table_count(&store, "index_batches");
        let error = store
            .begin_index_batch(&[], std::slice::from_ref(&message))
            .expect_err("deleting a source-owned referenced entity must fail");
        assert!(matches!(error, PortError::InvalidRequest(_)));
        assert_eq!(table_count(&store, "index_batches"), intents);
        // 事务回滚：实体仍在，generation 未推进。
        assert!(
            store
                .get(&sid(IdKind::Message, b"dangling-message"))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn put_maintains_fts_projection() {
        // 回归（Minor-11）：put 更新 payload 后必须同步维护 fts/边车——旧文本不可
        // 再搜、新 payload 的 text 可搜（与 rebuild 同一 searchable_text 投影），
        // 且按 wire 删除仍能经边车定位新 fts 行。
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"put-fts-msg");
        let source = SourceBatch {
            source_path: "put.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(
                msg.clone(),
                message_payload("ses_v1_aaa", "old body"),
                "old body".into(),
            )],
        };
        assert!(store.commit_batch_if_changed(&source.entries).unwrap());
        assert_eq!(store.query("old", 10).unwrap().len(), 1);
        store
            .put(&msg, &message_payload("ses_v1_aaa", "brand new body"))
            .unwrap();
        assert!(
            store.query("old", 10).unwrap().is_empty(),
            "put must retire the old indexed text"
        );
        assert_eq!(store.query("brand", 10).unwrap().len(), 1);
        let entries: [(StableId, Vec<u8>, String); 0] = [];
        let pending = store
            .begin_index_batch(&entries, std::slice::from_ref(&msg))
            .unwrap();
        store
            .commit_index_batch(&pending, &entries, std::slice::from_ref(&msg))
            .unwrap();
        assert!(store.query("brand", 10).unwrap().is_empty());
    }

    #[test]
    fn timestamp_missing_key_is_symmetric_with_explicit_null() {
        // 回归（Minor-7）：timestamp 缺失键与显式 null 对称收敛——一侧带字符串
        // 时间戳、另一侧缺失该键时，合并结果必须收敛为 null（与 string/null 相同），
        // 而不是保留字符串；两侧都缺失时不引入 timestamp 键。
        let string_payload = serde_json::json!({
            "role": "assistant",
            "text": "same body",
            "timestamp": "2026-07-19T23:40:01.000Z",
        })
        .to_string()
        .into_bytes();
        let missing_payload = serde_json::json!({
            "role": "assistant",
            "text": "same body",
        })
        .to_string()
        .into_bytes();
        let merged = merge_message_payloads("wire", &string_payload, &missing_payload).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&merged).unwrap();
        assert_eq!(value.get("timestamp"), Some(&serde_json::Value::Null));
        // 方向对称。
        let merged = merge_message_payloads("wire", &missing_payload, &string_payload).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&merged).unwrap();
        assert_eq!(value.get("timestamp"), Some(&serde_json::Value::Null));
        // 两侧都缺失：不引入 timestamp 键。
        let merged = merge_message_payloads("wire", &missing_payload, &missing_payload).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&merged).unwrap();
        assert!(value.get("timestamp").is_none());
    }

    #[test]
    fn message_session_refs_accumulate_across_separate_batches() {
        // The same message arriving in a later batch must add its session
        // without dropping the ones already recorded.
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"batched-msg");
        let first = SourceBatch {
            source_path: "first.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(
                msg.clone(),
                message_payload("ses_v1_aaa", "body"),
                "body".into(),
            )],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&first))
                .unwrap()
        );
        let second = SourceBatch {
            source_path: "second.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(
                msg.clone(),
                message_payload("ses_v1_bbb", "body"),
                "body".into(),
            )],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&second))
                .unwrap()
        );

        let stored: serde_json::Value =
            serde_json::from_slice(&store.get(&msg).unwrap().unwrap()).unwrap();
        assert_eq!(
            stored["sessions"],
            serde_json::json!(["ses_v1_aaa", "ses_v1_bbb"]),
            "earlier batch's session must survive: {stored}"
        );
    }

    #[test]
    fn shared_message_survives_one_source_shrinking() {
        let store = SqliteStore::open_in_memory().unwrap();
        let shared = sid(IdKind::Message, b"shared");
        let first = [
            SourceBatch {
                source_path: "one".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(shared.clone(), b"p".to_vec(), "shared text".into())],
            },
            SourceBatch {
                source_path: "two".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(shared.clone(), b"p".to_vec(), "shared text".into())],
            },
        ];
        assert!(store.commit_source_batches_if_changed(&first).unwrap());
        let second = [
            SourceBatch {
                source_path: "one".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: Vec::new(),
            },
            SourceBatch {
                source_path: "two".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(shared.clone(), b"p".to_vec(), "shared text".into())],
            },
        ];
        assert!(store.commit_source_batches_if_changed(&second).unwrap());
        assert_eq!(store.count().unwrap(), 1);
        assert!(store.get(&shared).unwrap().is_some());
        assert_eq!(store.query("shared", 10).unwrap().len(), 1);
    }

    #[test]
    fn moving_message_to_new_source_is_atomic() {
        let store = SqliteStore::open_in_memory().unwrap();
        let moved = sid(IdKind::Message, b"move-to-new-source");
        let original = SourceBatch {
            source_path: "source-a".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(moved.clone(), b"p".to_vec(), "moved text".into())],
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&original))
            .unwrap();

        let moved_batches = [
            SourceBatch {
                source_path: "source-a".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: Vec::new(),
            },
            SourceBatch {
                source_path: "source-b".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(moved.clone(), b"p".to_vec(), "moved text".into())],
            },
        ];
        assert!(
            store
                .commit_source_batches_if_changed(&moved_batches)
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), 2);
        assert_eq!(store.get(&moved).unwrap().unwrap(), b"p");
        assert_eq!(store.query("moved", 10).unwrap().len(), 1);
        assert_eq!(
            store.source_message_ids("source-a").unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            store.source_message_ids("source-b").unwrap(),
            vec![moved.as_str().to_string()]
        );
    }

    #[test]
    fn source_batch_permutations_produce_identical_state() {
        fn run(order: [usize; 3]) -> SourceState {
            let store = SqliteStore::open_in_memory().unwrap();
            let moved = sid(IdKind::Message, b"permuted-move");
            let removed = sid(IdKind::Message, b"permuted-remove");
            let kept = sid(IdKind::Message, b"permuted-keep");
            let initial = [
                SourceBatch {
                    source_path: "source-a".into(),
                    placements: Vec::new(),
                    edges: Vec::new(),
                    activities: Vec::new(),
                    usage_events: Vec::new(),
                    relation_complete: true,
                    len_bytes: None,
                    fingerprint: None,
                    provider_id: None,
                    resume_claims: Vec::new(),
                    entries: vec![
                        (moved.clone(), b"m".to_vec(), "moved text".into()),
                        (removed.clone(), b"r".to_vec(), "removed text".into()),
                    ],
                },
                SourceBatch {
                    source_path: "source-c".into(),
                    placements: Vec::new(),
                    edges: Vec::new(),
                    activities: Vec::new(),
                    usage_events: Vec::new(),
                    relation_complete: true,
                    len_bytes: None,
                    fingerprint: None,
                    provider_id: None,
                    resume_claims: Vec::new(),
                    entries: vec![(kept.clone(), b"k".to_vec(), "kept text".into())],
                },
            ];
            store.commit_source_batches_if_changed(&initial).unwrap();

            let mut replacement = [
                Some(SourceBatch {
                    source_path: "source-a".into(),
                    placements: Vec::new(),
                    edges: Vec::new(),
                    activities: Vec::new(),
                    usage_events: Vec::new(),
                    relation_complete: true,
                    len_bytes: None,
                    fingerprint: None,
                    provider_id: None,
                    resume_claims: Vec::new(),
                    entries: Vec::new(),
                }),
                Some(SourceBatch {
                    source_path: "source-b".into(),
                    placements: Vec::new(),
                    edges: Vec::new(),
                    activities: Vec::new(),
                    usage_events: Vec::new(),
                    relation_complete: true,
                    len_bytes: None,
                    fingerprint: None,
                    provider_id: None,
                    resume_claims: Vec::new(),
                    entries: vec![(moved, b"m".to_vec(), "moved text".into())],
                }),
                Some(SourceBatch {
                    source_path: "source-c".into(),
                    placements: Vec::new(),
                    edges: Vec::new(),
                    activities: Vec::new(),
                    usage_events: Vec::new(),
                    relation_complete: true,
                    len_bytes: None,
                    fingerprint: None,
                    provider_id: None,
                    resume_claims: Vec::new(),
                    entries: vec![(kept, b"k".to_vec(), "kept text".into())],
                }),
            ];
            let ordered: Vec<SourceBatch> = order
                .into_iter()
                .map(|index| replacement[index].take().unwrap())
                .collect();
            store.commit_source_batches_if_changed(&ordered).unwrap();
            assert!(store.get(&removed).unwrap().is_none());
            source_state(&store)
        }

        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let expected = run(permutations[0]);
        for permutation in permutations.into_iter().skip(1) {
            assert_eq!(run(permutation), expected);
        }
    }

    #[test]
    fn empty_source_scan_tombstones_prior_membership() {
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"becomes-empty");
        let populated = SourceBatch {
            source_path: "empty-later".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(id.clone(), b"p".to_vec(), "will disappear".into())],
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&populated))
            .unwrap();
        let empty = SourceBatch {
            source_path: "empty-later".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: Vec::new(),
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&empty))
            .unwrap();
        assert_eq!(store.count().unwrap(), 0);
        assert!(store.query("disappear", 10).unwrap().is_empty());
    }

    #[test]
    fn duplicate_source_paths_are_rejected() {
        let store = SqliteStore::open_in_memory().unwrap();
        let sources = [
            SourceBatch {
                source_path: "same".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: Vec::new(),
            },
            SourceBatch {
                source_path: "same".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: Vec::new(),
            },
        ];
        let err = store
            .commit_source_batches_if_changed(&sources)
            .unwrap_err();
        assert!(matches!(err, PortError::Backend(m) if m.contains("duplicate source paths")));
    }

    #[test]
    fn duplicate_message_error_does_not_disclose_source_path() {
        let store = SqliteStore::open_in_memory().unwrap();
        let id = sid(IdKind::Message, b"duplicate-in-source");
        let private_path = "C:/private/provider/session.jsonl";
        let source = SourceBatch {
            source_path: private_path.into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![
                (id.clone(), b"p".to_vec(), "text".into()),
                (id, b"p".to_vec(), "text".into()),
            ],
        };
        let err = store
            .commit_source_batches_if_changed(std::slice::from_ref(&source))
            .unwrap_err();
        let PortError::Backend(message) = err else {
            panic!("expected backend error");
        };
        assert!(message.contains("duplicate entity ids"));
        assert!(!message.contains(private_path));
    }

    #[test]
    fn shared_wire_id_with_conflicting_identity_metadata_is_rejected() {
        let store = SqliteStore::open_in_memory().unwrap();
        let reconstructed = sid(IdKind::Message, b"shared-wire-identity");
        let unstable = StableId::from_wire(reconstructed.as_str()).unwrap();
        assert_ne!(reconstructed, unstable);
        assert_eq!(reconstructed.as_str(), unstable.as_str());

        let sources = [
            SourceBatch {
                source_path: "one".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(reconstructed, b"p".to_vec(), "same text".into())],
            },
            SourceBatch {
                source_path: "two".into(),
                placements: Vec::new(),
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
                entries: vec![(unstable, b"p".to_vec(), "same text".into())],
            },
        ];
        let err = store
            .commit_source_batches_if_changed(&sources)
            .unwrap_err();
        assert!(
            matches!(err, PortError::Backend(m) if m.contains("conflicting identity metadata"))
        );
    }

    #[test]
    fn separate_batches_reject_conflicting_identity_metadata_without_state_change() {
        let store = SqliteStore::open_in_memory().unwrap();
        let reconstructed = sid(IdKind::Message, b"stored-wire-identity");
        let wire = reconstructed.as_str().to_string();
        let unstable = StableId::from_wire(&wire).unwrap();
        let first = SourceBatch {
            source_path: "first-source".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(reconstructed, b"p".to_vec(), "same text".into())],
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&first))
                .unwrap()
        );
        let generation = store.active_generation().unwrap();

        let second = SourceBatch {
            source_path: "second-source".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
            entries: vec![(unstable, b"p".to_vec(), "same text".into())],
        };
        let err = store
            .commit_source_batches_if_changed(std::slice::from_ref(&second))
            .unwrap_err();
        let PortError::Backend(message) = err else {
            panic!("expected backend error");
        };
        assert!(message.contains("conflicting identity metadata"));
        assert!(!message.contains(&wire));
        assert_eq!(store.active_generation().unwrap(), generation);
        assert!(
            store
                .source_message_ids("second-source")
                .unwrap()
                .is_empty()
        );
        let hits = store.query("same", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id.stability(), Stability::Reconstructed);
    }

    #[test]
    fn catalog_list_is_sorted_and_limited() {
        let store = SqliteStore::open_in_memory().unwrap();
        let b = sid(IdKind::Message, b"b");
        let a = sid(IdKind::Message, b"a");
        store.put(&b, b"B").unwrap();
        store.put(&a, b"A").unwrap();
        assert_eq!(store.count().unwrap(), 2);
        let all = store.list(10).unwrap();
        assert_eq!(all.len(), 2);
        assert!(all[0].id.as_str() < all[1].id.as_str());
        let one = store.list(1).unwrap();
        assert_eq!(one.len(), 1);
    }

    #[test]
    fn fresh_store_starts_at_generation_zero() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(store.active_generation().unwrap(), 0);
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
    }

    #[test]
    fn index_batch_commits_and_advances_generation() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"a");
        let b = sid(IdKind::Message, b"b");
        let entries = [
            (a.clone(), b"role\talpha".to_vec(), "alpha text".into()),
            (b.clone(), b"role\tbeta".to_vec(), "beta text".into()),
        ];
        let pending = store.begin_index_batch(&entries, &[]).unwrap();
        assert_eq!(pending.base_generation, 0);
        assert_eq!(pending.target_generation, 1);
        // durable intent 已落盘；catalog 仍空。
        assert_eq!(store.count().unwrap(), 0);
        let batch = store.index_batch(&pending.operation_id).unwrap().unwrap();
        assert_eq!(batch.state, "building");
        assert_eq!(batch.durable_point, "intent");
        assert_eq!(batch.operation_digest, pending.operation_digest);

        store.commit_index_batch(&pending, &entries, &[]).unwrap();
        assert_eq!(store.active_generation().unwrap(), 1);
        assert_eq!(store.get(&a).unwrap().unwrap(), b"role\talpha");
        assert_eq!(store.query("alpha", 10).unwrap().len(), 1);
        let batch = store.index_batch(&pending.operation_id).unwrap().unwrap();
        assert_eq!(batch.state, "activated");
        assert_eq!(batch.durable_point, "activated");
    }

    #[test]
    fn commit_rejects_mismatched_payload() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"a");
        let b = sid(IdKind::Message, b"b");
        let intended = [(a.clone(), b"x".to_vec(), "x".into())];
        let pending = store.begin_index_batch(&intended, &[]).unwrap();
        // 实际 upsert 含未声明的 b —— 必须拒绝，journal 与数据不能分歧。
        let err = store
            .commit_index_batch(
                &pending,
                &[
                    (a, b"x".to_vec(), "x".into()),
                    (b, b"y".to_vec(), "y".into()),
                ],
                &[],
            )
            .unwrap_err();
        assert!(
            matches!(err, PortError::Backend(m) if m.contains("does not match durable intent"))
        );
        assert_eq!(store.active_generation().unwrap(), 0);
        assert_eq!(store.count().unwrap(), 0);
    }

    #[test]
    fn commit_rejects_tampered_durable_relation_manifest() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"manifest-session");
        let document = sid(IdKind::Document, b"manifest-document");
        let message = sid(IdKind::Message, b"manifest-message");
        let message_placement = placement(&session, &document, &message, 0, false, Some((0, 4)));
        let relations = RelationManifests {
            relation_upserts: vec![RelationUpsertManifest::Placement(message_placement)],
            ..RelationManifests::default()
        };
        let manifest = batch_manifest(&[], &[], &relations).unwrap();
        let pending = store
            .begin_index_batch_with_relations(&[], &[], &relations)
            .unwrap();
        store
            .conn
            .borrow()
            .execute(
                "UPDATE index_batches SET relation_upserts_json = '[]'
                 WHERE operation_id = ?1",
                [&pending.operation_id],
            )
            .unwrap();

        let err = store
            .commit_index_batch_with_relations(&pending, &[], &[], &relations, &manifest)
            .unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("does not match durable intent"))
        );
        assert_eq!(store.active_generation().unwrap(), 0);
        assert_eq!(table_count(&store, "message_placements"), 0);
        let batch = store.index_batch(&pending.operation_id).unwrap().unwrap();
        assert_eq!(batch.state, "building");
        assert_eq!(batch.durable_point, "intent");
    }

    #[test]
    fn commit_rejects_tampered_durable_source_replacement_manifest() {
        let store = SqliteStore::open_in_memory().unwrap();
        let relations = RelationManifests {
            source_replacements: vec![SourceReplacementManifest {
                projections: BTreeMap::new(),
                source_path: "manifest-source.jsonl".into(),
                installation: None,
                entity_memberships: Vec::new(),
                placement_ids: Vec::new(),
                activity_ids: Vec::new(),
                usage_ids: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: Vec::new(),
            }],
            ..RelationManifests::default()
        };
        let manifest = batch_manifest(&[], &[], &relations).unwrap();
        let pending = store
            .begin_index_batch_with_relations(&[], &[], &relations)
            .unwrap();
        store
            .conn
            .borrow()
            .execute(
                "UPDATE index_batches SET source_replacements_json = '[]'
                 WHERE operation_id = ?1",
                [&pending.operation_id],
            )
            .unwrap();

        let err = store
            .commit_index_batch_with_relations(&pending, &[], &[], &relations, &manifest)
            .unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("does not match durable intent"))
        );
        assert_eq!(store.active_generation().unwrap(), 0);
        assert_eq!(table_count(&store, "source_scans"), 0);
        assert_eq!(table_count(&store, "source_relation_scans"), 0);
        let batch = store.index_batch(&pending.operation_id).unwrap().unwrap();
        assert_eq!(batch.state, "building");
        assert_eq!(batch.durable_point, "intent");
    }

    #[test]
    fn commit_rejects_stale_base_generation() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"a");
        // 先成功推进到 gen 1。
        let first = [(a.clone(), b"v1".to_vec(), "v1".into())];
        let p1 = store.begin_index_batch(&first, &[]).unwrap();
        store.commit_index_batch(&p1, &first, &[]).unwrap();
        // 伪造一个 base=0 的 pending（模拟旧写者持过期 handle）。
        let stale = PendingIndexBatch {
            operation_id: p1.operation_id.clone(), // 已 activated，非 building
            base_generation: 0,
            target_generation: 1,
            operation_digest: p1.operation_digest.clone(),
        };
        let err = store
            .commit_index_batch(&stale, &[(a, b"v2".to_vec(), "v2".into())], &[])
            .unwrap_err();
        assert!(matches!(err, PortError::Backend(_)));
        assert_eq!(store.active_generation().unwrap(), 1);
    }

    #[test]
    fn recover_aborts_orphan_building_intents() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"a");
        let entries = [(a, b"x".to_vec(), "x".into())];
        let pending = store.begin_index_batch(&entries, &[]).unwrap();
        // 模拟崩溃：intent 已 durable，apply 未发生。
        assert_eq!(
            store
                .index_batch(&pending.operation_id)
                .unwrap()
                .unwrap()
                .state,
            "building"
        );
        let n = store.recover_interrupted().unwrap();
        assert_eq!(n, 1);
        let batch = store.index_batch(&pending.operation_id).unwrap().unwrap();
        assert_eq!(batch.state, "aborted");
        assert_eq!(
            batch.error_code.as_deref(),
            Some("interrupted_before_activation")
        );
        // 恢复后 generation 与 catalog 不受影响。
        assert_eq!(store.active_generation().unwrap(), 0);
        assert_eq!(store.count().unwrap(), 0);
        // 幂等：再 recover 0 行。
        assert_eq!(store.recover_interrupted().unwrap(), 0);
    }

    #[test]
    fn rebuild_reprojects_fts_from_catalog_and_advances_generation() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"a");
        let b = sid(IdKind::Message, b"b");
        let entries = [
            (
                a.clone(),
                b"user\talpha searchable".to_vec(),
                "alpha searchable".into(),
            ),
            (
                b.clone(),
                b"assistant\tbeta searchable".to_vec(),
                "beta searchable".into(),
            ),
        ];
        store.commit_batch(&entries).unwrap();
        assert_eq!(store.active_generation().unwrap(), 1);

        let n = store.rebuild_index().unwrap();
        assert_eq!(n, 2, "rebuild 应重新索引全部 catalog 实体");
        // rebuild 是显式维护动作：即便内容一致也推进 generation。
        assert_eq!(store.active_generation().unwrap(), 2);
        // 重建后搜索仍可命中，且 id 无损。
        assert_eq!(store.query("alpha", 10).unwrap().len(), 1);
        assert_eq!(store.query("beta", 10).unwrap().len(), 1);
        assert_eq!(store.query("alpha", 10).unwrap()[0].id, a);
    }

    #[test]
    fn rebuild_removes_orphan_fts_rows_not_in_catalog() {
        let store = SqliteStore::open_in_memory().unwrap();
        let real = sid(IdKind::Message, b"real");
        store
            .commit_batch(&[(
                real.clone(),
                b"user\treal body".to_vec(),
                "real body".into(),
            )])
            .unwrap();
        // 直接往 FTS 塞一条 catalog 里没有的孤儿行，模拟索引漂移。
        let orphan = sid(IdKind::Message, b"orphan");
        store.index(&orphan, "orphan drifted body").unwrap();
        assert_eq!(store.query("drifted", 10).unwrap().len(), 1);

        // rebuild 从 catalog 权威重投影：孤儿行应被清除，真实行保留。
        store.rebuild_index().unwrap();
        assert!(
            store.query("drifted", 10).unwrap().is_empty(),
            "rebuild 应清除不在 catalog 中的孤儿 FTS 行"
        );
        assert_eq!(store.query("real", 10).unwrap().len(), 1);
    }

    #[test]
    fn rebuild_indexes_json_payload_text_not_structural_tokens() {
        // H1 regression: ingest/sync writes full JSON payloads. rebuild must
        // index only the `text` field — indexing raw JSON would let
        // structural tokens (`user`, `null`, `sessions`) match every message.
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"json-message-a");
        let payload = serde_json::json!({
            "role": "user",
            "text": "the real searchable body",
            "parent": null,
            "session": "ses-1",
            "sessions": ["ses-1"],
            "timestamp": null,
        })
        .to_string()
        .into_bytes();
        store
            .commit_batch(&[(a.clone(), payload, "one".into())])
            .unwrap();
        // commit 实时路径用调用方传入的 text（此处为 "one"）。
        assert_eq!(store.query("one", 10).unwrap().len(), 1);
        // rebuild 从 catalog 重投影：只索引 JSON 的 text 字段，结构 token 不命中。
        store.rebuild_index().unwrap();
        assert_eq!(store.query("searchable", 10).unwrap().len(), 1);
        assert!(store.query("null", 10).unwrap().is_empty());
        assert!(store.query("sessions", 10).unwrap().is_empty());
        // `ses-1` 里的 `-` 会被 FTS5 当成 NOT 运算符（查询报错而非匹配），
        // 因此用单 token `ses` 断言会话值不被索引。
        assert!(store.query("ses", 10).unwrap().is_empty());
    }

    #[test]
    fn rebuild_restores_search_after_index_data_wiped() {
        // 验证 ADR-0001 的“Catalog 权威、Search 可删除重建”不变量：
        // 直接清空全文索引数据（模拟索引损坏/删除），rebuild 应仅凭权威 catalog 完全恢复搜索。
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"survivor-a");
        let b = sid(IdKind::Message, b"survivor-b");
        store
            .commit_batch(&[
                (
                    a.clone(),
                    b"user\tthe catalog is authoritative".to_vec(),
                    "the catalog is authoritative".into(),
                ),
                (
                    b.clone(),
                    b"assistant\tsearch is a derived projection".to_vec(),
                    "search is a derived projection".into(),
                ),
            ])
            .unwrap();
        assert_eq!(store.query("authoritative", 10).unwrap().len(), 1);

        // 删除全文引擎索引数据（catalog 保持不动，作为权威事实源）。
        {
            let conn = store.conn.borrow();
            conn.execute("DELETE FROM fts", []).unwrap();
            conn.execute("DELETE FROM fts_ids", []).unwrap();
        }
        assert!(
            store.query("authoritative", 10).unwrap().is_empty(),
            "清空后搜索应无结果"
        );
        // catalog 仍完好——rebuild 的权威来源未受影响。
        assert_eq!(store.count().unwrap(), 2);

        let n = store.rebuild_index().unwrap();
        assert_eq!(n, 2, "rebuild 应从 catalog 恢复全部 2 条");
        // 搜索完全恢复，两条都可命中。
        assert_eq!(store.query("authoritative", 10).unwrap().len(), 1);
        assert_eq!(store.query("projection", 10).unwrap().len(), 1);
        // 结果集等价性在 wire id 层面成立——catalog 权威保留的正是 wire 串（其主键）。
        // 注意 stability：fts_ids 边车一并被清空后，身份只能从 catalog wire 串还原，
        // 按域模型（StableId::from_wire）降级为 Unstable，与 catalog-only 的 `list` 读一致。
        // 这落在回滚 runbook“在声明的 identity stability 范围内等价”的语义内：全文索引
        // 数据（含 fts_ids 边车）被删除时，声明的 stability 范围即 Unstable。
        assert_eq!(
            store.query("authoritative", 10).unwrap()[0].id.as_str(),
            a.as_str()
        );
        assert_eq!(
            store.query("projection", 10).unwrap()[0].id.as_str(),
            b.as_str()
        );
        assert_eq!(
            store.query("authoritative", 10).unwrap()[0].id.stability(),
            Stability::Unstable,
            "全文索引数据被整体清空后，身份从 catalog wire 还原为 Unstable"
        );
    }

    #[test]
    fn rebuild_on_empty_catalog_yields_empty_index() {
        let store = SqliteStore::open_in_memory().unwrap();
        let n = store.rebuild_index().unwrap();
        assert_eq!(n, 0);
        assert_eq!(store.active_generation().unwrap(), 1);
        assert!(store.query("anything", 10).unwrap().is_empty());
    }

    #[test]
    fn rebuild_leaves_durable_activated_journal_row() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"journal");
        store
            .commit_batch(&[(a, b"user\tjournal body".to_vec(), "journal body".into())])
            .unwrap();
        let before = store.active_generation().unwrap();
        store.rebuild_index().unwrap();
        // 找到本次 rebuild 产生的 activated 批次：target = before + 1。
        let conn = store.conn.borrow();
        let (state, target): (String, i64) = conn
            .query_row(
                "SELECT state, target_generation FROM index_batches
                 ORDER BY target_generation DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, "activated");
        assert_eq!(target as u64, before + 1);
    }

    #[test]
    fn interrupted_batch_count_observes_without_mutating() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"a");
        let entries = [(a, b"x".to_vec(), "x".into())];
        // 干净：0 个待收敛。
        assert_eq!(store.interrupted_batch_count().unwrap(), 0);
        // 写 durable intent 但不 commit（模拟崩溃前）：1 个 building。
        let _pending = store.begin_index_batch(&entries, &[]).unwrap();
        assert_eq!(store.interrupted_batch_count().unwrap(), 1);
        // 只读观测不改状态：再查仍是 1，且 recover 仍能收敛它。
        assert_eq!(store.interrupted_batch_count().unwrap(), 1);
        assert_eq!(store.recover_interrupted().unwrap(), 1);
        assert_eq!(store.interrupted_batch_count().unwrap(), 0);
    }

    #[test]
    fn recover_does_not_touch_activated_batches() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = sid(IdKind::Message, b"a");
        let entries = [(a, b"x".to_vec(), "x".into())];
        let pending = store.begin_index_batch(&entries, &[]).unwrap();
        store.commit_index_batch(&pending, &entries, &[]).unwrap();
        assert_eq!(store.recover_interrupted().unwrap(), 0);
        assert_eq!(
            store
                .index_batch(&pending.operation_id)
                .unwrap()
                .unwrap()
                .state,
            "activated"
        );
    }

    #[test]
    fn v1_db_migrates_to_v2_with_generation_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let p = path.to_string_lossy().into_owned();
        // 手工造一个 v1 库（只有 catalog + fts，无 store_metadata）。
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            conn.execute_batch(
                "CREATE TABLE catalog (
                     id TEXT PRIMARY KEY,
                     payload BLOB NOT NULL
                 );
                 CREATE VIRTUAL TABLE fts USING fts5(id UNINDEXED, text);
                 PRAGMA user_version = 1;",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO catalog(id, payload) VALUES('msg_v1_legacy', x'01')",
                [],
            )
            .unwrap();
        }
        // 新二进制打开：自动迁到 v2，数据保留，generation 从 0 起步。
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(store.active_generation().unwrap(), 0);
        let id = StableId::from_wire("msg_v1_legacy").unwrap();
        assert_eq!(store.get(&id).unwrap().unwrap(), vec![1u8]);
    }

    #[test]
    fn v7_db_migrates_to_v8_creating_resume_claims_table() {
        // v7 库没有 resume claims 表；打开后升到 v8，表创建且为空——
        // legacy 数据全保留，claims 由 re-sync 回填（ADR-0009）。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v7.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            SqliteStore::migrate_v6_to_v7(&conn).unwrap();
            let version: i64 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, 7);
            let table_exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = 'source_session_resume_claims'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(table_exists, 0, "v7 尚无 resume claims 表");
        }
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        // 无声明/legacy：批量解析恒不可恢复（re-sync 前）。
        let legacy = sid(IdKind::Session, b"v7-legacy-ses");
        let metas = store.resume_of(std::slice::from_ref(&legacy)).unwrap();
        assert_eq!(metas.len(), 1);
        assert!(!metas[0].resume_available);
        assert_eq!(
            metas[0].unavailable_reason.as_deref(),
            Some("no resume metadata claims")
        );
        drop(store);
        let conn = rusqlite::Connection::open(&p).unwrap();
        let table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'source_session_resume_claims'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_exists, 1);
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_session_resume_claims",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "v8 迁移后 claims 表必须为空，等待 re-sync 回填");
    }

    #[test]
    fn injected_v7_to_v8_failure_rolls_back_schema_and_version() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        create_v6_schema(&conn);
        SqliteStore::migrate_v6_to_v7(&conn).unwrap();

        let err = SqliteStore::migrate_v7_to_v8_inner(&conn, true).unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("injected v7-to-v8"))
        );
        assert!(conn.is_autocommit());
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 7);
        let table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'source_session_resume_claims'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_exists, 0);
    }

    #[test]
    fn v8_to_v9_migration_adds_provider_id_column_non_destructively() {
        // 从真实 v6 schema 迁移到 v8，模拟尚未升级的旧库。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v8.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            SqliteStore::migrate_v6_to_v7(&conn).unwrap();
            SqliteStore::migrate_v7_to_v8(&conn).unwrap();
            let version: i64 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, 8);
            conn.execute(
                "INSERT INTO source_scans(source_path, scanned_at_ms, len_bytes, fingerprint)
                 VALUES('legacy.jsonl', 1, 12, 'legacy-fingerprint')",
                [],
            )
            .unwrap();
            // v9 列尚未存在。
            let cols: Vec<String> = conn
                .prepare("PRAGMA table_info(source_scans)")
                .unwrap()
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert!(!cols.iter().any(|c| c == "provider_id"));
        }
        // 重新打开：触发 v8→v9 迁移。
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let conn = rusqlite::Connection::open(&p).unwrap();
        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(source_scans)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(cols.iter().any(|c| c == "provider_id"));
        // 旧行的 provider_id 为 NULL（回填发生在下次 re-scan）。
        let legacy_provider: Option<String> = conn
            .query_row(
                "SELECT provider_id FROM source_scans WHERE source_path = 'legacy.jsonl'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(legacy_provider, None);
        let legacy_fingerprint: String = conn
            .query_row(
                "SELECT fingerprint FROM source_scans WHERE source_path = 'legacy.jsonl'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(legacy_fingerprint, "legacy-fingerprint");
        // 重新打开同样触发 v10→v11 迁移：Session 元数据搜索投影表必须存在。
        for table in ["session_fts", "session_fts_ids"] {
            let exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 1, "migrated database must contain {table}");
        }
    }

    #[test]
    fn fresh_db_creates_session_metadata_projection_tables() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let conn = store.conn.borrow();
        for table in [
            "session_fts",
            "session_fts_ids",
            "session_titles",
            "session_repo_slugs",
        ] {
            let exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 1, "{table} must exist in a fresh database");
        }
        let columns: Vec<String> = conn
            .prepare("PRAGMA table_info(session_fts_ids)")
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(columns, vec!["session_wire", "fts_rowid"]);
        // 标题投影（v13）：两列形状固定，读取侧按 (session_wire, title) 批量投影。
        let columns: Vec<String> = conn
            .prepare("PRAGMA table_info(session_titles)")
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(columns, vec!["session_wire", "title"]);
    }

    // ---- 会话标题投影（schema v13）：派生链/截断/降级/增量/迁移 ----

    /// 提交一个含单条 user 消息的会话（标题派生测试的种子）。
    fn titled_session_batch(
        tag: &[u8],
        session_extra: Option<(&str, &str)>,
        user_text: &str,
    ) -> (StableId, SourceBatch) {
        let session = sid(IdKind::Session, tag);
        let document = sid(IdKind::Document, tag);
        let message = sid(IdKind::Message, tag);
        let mut session_value = serde_json::json!({ "messages": [message.as_str()] });
        if let Some((key, value)) = session_extra {
            session_value[key] = serde_json::json!(value);
        }
        let batch = SourceBatch {
            source_path: format!("title-seed-{}.jsonl", String::from_utf8_lossy(tag)),
            entries: vec![
                (
                    session.clone(),
                    session_value.to_string().into_bytes(),
                    String::new(),
                ),
                typed_document_entry(&document),
                typed_message_entry(&message, user_text),
            ],
            placements: vec![placement(
                &session,
                &document,
                &message,
                0,
                false,
                Some((0, 5)),
            )],
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
        };
        (session, batch)
    }

    #[test]
    fn session_title_prefers_custom_then_ai_then_first_valid_user_message() {
        // 派生优先级链（借鉴清单 #6）：custom-title（session payload `title`）
        // > ai-title（`summary`）> 首条有效 user 消息。claude-code/codex 的
        // Canonical payload 不带 title/summary（格式事实），实际落到第 3 候选；
        // 字段由 provider 未来填充时自动优先。
        let store = SqliteStore::open_in_memory().unwrap();
        let (custom, custom_batch) = titled_session_batch(
            b"title-custom",
            Some(("title", "custom rename")),
            "first user fallback",
        );
        let (ai, ai_batch) = titled_session_batch(
            b"title-ai",
            Some(("summary", "ai summary")),
            "first user fallback",
        );
        let (plain, plain_batch) = titled_session_batch(b"title-plain", None, "plain first body");
        store
            .commit_source_batches_if_changed(&[custom_batch, ai_batch, plain_batch])
            .unwrap();
        let titles = store.session_titles(&[custom, ai, plain]).unwrap();
        assert_eq!(
            titles,
            vec![
                Some("custom rename".to_string()),
                Some("ai summary".to_string()),
                Some("plain first body".to_string()),
            ]
        );
    }

    #[test]
    fn session_title_truncates_to_80_chars_on_char_boundary() {
        // 上限（≤80 字符，char 边界）：100 个三字节汉字，截断恰好停在 80 字符
        // 处——按字节截会 panic 或产出非法 UTF-8。custom/ai 候选同样截断。
        let store = SqliteStore::open_in_memory().unwrap();
        let long_user = "界".repeat(100);
        let (session, batch) = titled_session_batch(b"title-trunc", None, &long_user);
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch))
            .unwrap();
        let title = store
            .session_titles(std::slice::from_ref(&session))
            .unwrap()
            .into_iter()
            .next()
            .flatten()
            .expect("derived title");
        assert_eq!(title.chars().count(), SESSION_TITLE_MAX_CHARS);
        assert_eq!(title, "界".repeat(SESSION_TITLE_MAX_CHARS));

        let long_custom = "🏷️".repeat(100);
        let (custom, custom_batch) = titled_session_batch(
            b"title-trunc-custom",
            Some(("title", &long_custom)),
            "fallback",
        );
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&custom_batch))
            .unwrap();
        let title = store
            .session_titles(std::slice::from_ref(&custom))
            .unwrap()
            .into_iter()
            .next()
            .flatten()
            .expect("derived title");
        assert_eq!(title.chars().count(), SESSION_TITLE_MAX_CHARS);
        assert!(
            long_custom.starts_with(&title),
            "prefix kept, tail cut: {title}"
        );
    }

    #[test]
    fn session_title_skips_non_user_roles_and_empty_text() {
        // 派生只认 role=user 且 text 非空的消息：assistant 与空文本用户被跳过。
        // 注入噪声（伪 user 封套）在 provider parse 层已过滤（feat/noise-filter
        // 的 claude/codex user-noise filter 测试守卫），catalog 中不存在——
        // 派生链读到的第一条 user 即"噪声过滤后"的首条有效请求。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"title-skip");
        let document = sid(IdKind::Document, b"title-skip");
        let assistant = sid(IdKind::Message, b"title-skip-a");
        let empty_user = sid(IdKind::Message, b"title-skip-e");
        let real_user = sid(IdKind::Message, b"title-skip-r");
        let role_payload = |role: &str, text: &str| {
            serde_json::json!({
                "role": role,
                "text": text,
                "timestamp": "2026-07-28T00:00:00Z",
            })
            .to_string()
            .into_bytes()
        };
        let session_value = serde_json::json!({
            "messages": [
                assistant.as_str(),
                empty_user.as_str(),
                real_user.as_str(),
            ],
        });
        let batch = SourceBatch {
            source_path: "title-skip-source.jsonl".into(),
            entries: vec![
                (
                    session.clone(),
                    session_value.to_string().into_bytes(),
                    String::new(),
                ),
                typed_document_entry(&document),
                (
                    assistant.clone(),
                    role_payload("assistant", "answer"),
                    String::new(),
                ),
                (empty_user.clone(), role_payload("user", ""), String::new()),
                (
                    real_user.clone(),
                    role_payload("user", "real prompt"),
                    String::new(),
                ),
            ],
            placements: vec![
                placement(&session, &document, &assistant, 0, false, Some((0, 3))),
                placement(&session, &document, &empty_user, 1, false, Some((3, 6))),
                placement(&session, &document, &real_user, 2, false, Some((6, 9))),
            ],
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch))
            .unwrap();
        let title = store
            .session_titles(std::slice::from_ref(&session))
            .unwrap()
            .into_iter()
            .next()
            .flatten();
        assert_eq!(title.as_deref(), Some("real prompt"));
    }

    #[test]
    fn session_title_is_none_without_valid_user_message() {
        // 派生链无候选（无 user 消息、无 placement、会话不存在）→ 无投影行，
        // 批量读取返回 None——绝不写空标题或臆造。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"title-none");
        let document = sid(IdKind::Document, b"title-none");
        let assistant = sid(IdKind::Message, b"title-none-a");
        let session_value = serde_json::json!({ "messages": [assistant.as_str()] });
        let batch = SourceBatch {
            source_path: "title-none-source.jsonl".into(),
            entries: vec![
                (
                    session.clone(),
                    session_value.to_string().into_bytes(),
                    String::new(),
                ),
                typed_document_entry(&document),
                (
                    assistant.clone(),
                    serde_json::json!({ "role": "assistant", "text": "answer" })
                        .to_string()
                        .into_bytes(),
                    String::new(),
                ),
            ],
            placements: vec![placement(
                &session,
                &document,
                &assistant,
                0,
                false,
                Some((0, 3)),
            )],
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch))
            .unwrap();
        let ghost = sid(IdKind::Session, b"title-none-ghost");
        let titles = store.session_titles(&[session, ghost]).unwrap();
        assert_eq!(titles, vec![None, None]);
    }

    #[test]
    fn session_title_projection_is_incremental_and_rebuildable() {
        // 投影与 session_fts 同一重建批次：affected 提交增量重投影（更早的
        // 新 user 消息成为新标题），rebuild_index 全量重投影得到同一派生链结果。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"title-incr");
        let document = sid(IdKind::Document, b"title-incr");
        let first = sid(IdKind::Message, b"title-incr-first");
        let zeroth = sid(IdKind::Message, b"title-incr-zeroth");
        let user_payload = |text: &str| {
            serde_json::json!({
                "role": "user",
                "text": text,
                "timestamp": "2026-07-28T00:00:00Z",
            })
            .to_string()
            .into_bytes()
        };
        let session_value = serde_json::json!({ "messages": [first.as_str(), zeroth.as_str()] });
        let batch = SourceBatch {
            source_path: "title-incr-source.jsonl".into(),
            entries: vec![
                (
                    session.clone(),
                    session_value.to_string().into_bytes(),
                    String::new(),
                ),
                typed_document_entry(&document),
                (first.clone(), user_payload("first request"), String::new()),
                (
                    zeroth.clone(),
                    user_payload("zeroth request"),
                    String::new(),
                ),
            ],
            placements: vec![
                placement(&session, &document, &first, 0, false, Some((0, 3))),
                placement(&session, &document, &zeroth, 1, false, Some((3, 6))),
            ],
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch))
            .unwrap();
        let title_of = |store: &SqliteStore| {
            store
                .session_titles(std::slice::from_ref(&session))
                .unwrap()
                .into_iter()
                .next()
                .flatten()
        };
        assert_eq!(title_of(&store).as_deref(), Some("first request"));
        // 同 source 重扫：zeroth 提到 ordinal 0——complete-scan replace 后
        // 派生链按新成员顺序取 "zeroth request"。
        let moved = SourceBatch {
            placements: vec![
                placement(&session, &document, &zeroth, 0, false, Some((0, 3))),
                placement(&session, &document, &first, 1, false, Some((3, 6))),
            ],
            ..batch
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&moved))
            .unwrap();
        assert_eq!(title_of(&store).as_deref(), Some("zeroth request"));
        // 全量 rebuild：清空后按 catalog + 关系重投影，标题不变。
        {
            let conn = store.conn.borrow();
            conn.execute("DELETE FROM session_titles", []).unwrap();
        }
        store.rebuild_index().unwrap();
        assert_eq!(title_of(&store).as_deref(), Some("zeroth request"));
    }

    #[test]
    fn v12_catalog_migrates_to_v13_adding_session_titles_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v12.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            SqliteStore::migrate_v6_to_v7(&conn).unwrap();
            SqliteStore::migrate_v7_to_v8(&conn).unwrap();
            SqliteStore::migrate_v8_to_v9(&conn).unwrap();
            SqliteStore::migrate_v9_to_v10(&conn).unwrap();
            SqliteStore::migrate_v10_to_v11(&conn).unwrap();
            SqliteStore::migrate_v11_to_v12(&conn).unwrap();
            let version: i64 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, 12);
            let table_exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = 'session_titles'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(table_exists, 0, "v12 尚无 session_titles 表");
        }
        // 重新打开：触发 v12→v13 迁移。
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let conn = store.conn.borrow();
        let table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'session_titles'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_exists, 1);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_titles", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0, "v13 迁移后标题表必须为空，等待 rebuild/提交回填");
    }

    #[test]
    fn injected_v12_to_v13_failure_rolls_back_schema_and_version() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        create_v6_schema(&conn);
        SqliteStore::migrate_v6_to_v7(&conn).unwrap();
        SqliteStore::migrate_v7_to_v8(&conn).unwrap();
        SqliteStore::migrate_v8_to_v9(&conn).unwrap();
        SqliteStore::migrate_v9_to_v10(&conn).unwrap();
        SqliteStore::migrate_v10_to_v11(&conn).unwrap();
        SqliteStore::migrate_v11_to_v12(&conn).unwrap();

        let err = SqliteStore::migrate_v12_to_v13_inner(&conn, true).unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("injected v12-to-v13"))
        );
        assert!(conn.is_autocommit());
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 12);
        let table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'session_titles'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_exists, 0);
    }

    #[test]
    fn v13_catalog_migrates_to_v14_adding_parser_version_column() {
        // 旧库迁到 v14 后 source_scans 带 parser_version 列，既有行 DEFAULT 0
        // ——0 永不等于当前 PARSER_SEMANTIC_VERSION（≥1），因此迁移后第一次
        // sync 自动 targeted backfill 全部已扫源，无需手动 rebuild。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v13.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            SqliteStore::migrate_v6_to_v7(&conn).unwrap();
            SqliteStore::migrate_v7_to_v8(&conn).unwrap();
            SqliteStore::migrate_v8_to_v9(&conn).unwrap();
            SqliteStore::migrate_v9_to_v10(&conn).unwrap();
            SqliteStore::migrate_v10_to_v11(&conn).unwrap();
            SqliteStore::migrate_v11_to_v12(&conn).unwrap();
            SqliteStore::migrate_v12_to_v13(&conn).unwrap();
            conn.execute(
                "INSERT INTO source_scans(source_path, scanned_at_ms, len_bytes, fingerprint)
                 VALUES('legacy.jsonl', 1, 100, 'ff')",
                [],
            )
            .unwrap();
            let version: i64 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, 13);
            let has_parser_version: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('source_scans')
                     WHERE name = 'parser_version'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(has_parser_version, 0, "v13 尚无 parser_version 列");
        }
        // 重新打开：触发 v13→v14 迁移。
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let conn = store.conn.borrow();
        let has_parser_version: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('source_scans')
                 WHERE name = 'parser_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(has_parser_version, 1);
        let stored_version: i64 = conn
            .query_row(
                "SELECT parser_version FROM source_scans WHERE source_path = 'legacy.jsonl'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            stored_version, 0,
            "迁移后旧行 parser_version 必须为 DEFAULT 0，驱动下次 sync 自动 backfill"
        );
    }

    #[test]
    fn injected_v13_to_v14_failure_rolls_back_schema_and_version() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        create_v6_schema(&conn);
        SqliteStore::migrate_v6_to_v7(&conn).unwrap();
        SqliteStore::migrate_v7_to_v8(&conn).unwrap();
        SqliteStore::migrate_v8_to_v9(&conn).unwrap();
        SqliteStore::migrate_v9_to_v10(&conn).unwrap();
        SqliteStore::migrate_v10_to_v11(&conn).unwrap();
        SqliteStore::migrate_v11_to_v12(&conn).unwrap();
        SqliteStore::migrate_v12_to_v13(&conn).unwrap();

        let err = SqliteStore::migrate_v13_to_v14_inner(&conn, true).unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("injected v13-to-v14"))
        );
        assert!(conn.is_autocommit());
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 13);
        let has_parser_version: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('source_scans')
                 WHERE name = 'parser_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(has_parser_version, 0);
    }

    #[test]
    fn v14_catalog_migrates_to_v15_adding_usage_projection() {
        // 旧库迁到 v15 后 usage_events / usage_event_membership 表存在且为空；
        // usage_totals 返回 Some（有投影）且 sessions == 0——覆盖标记：
        // "有投影但零事实" 与 "无投影（None）" 必须可区分。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v14.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            SqliteStore::migrate_v6_to_v7(&conn).unwrap();
            SqliteStore::migrate_v7_to_v8(&conn).unwrap();
            SqliteStore::migrate_v8_to_v9(&conn).unwrap();
            SqliteStore::migrate_v9_to_v10(&conn).unwrap();
            SqliteStore::migrate_v10_to_v11(&conn).unwrap();
            SqliteStore::migrate_v11_to_v12(&conn).unwrap();
            SqliteStore::migrate_v12_to_v13(&conn).unwrap();
            SqliteStore::migrate_v13_to_v14(&conn).unwrap();
            let table_exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = 'usage_events'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(table_exists, 0, "v14 尚无 usage_events 表");
        }
        // 重新打开：触发 v14→v15 迁移。
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let conn = store.conn.borrow();
        for table in ["usage_events", "usage_event_membership"] {
            let table_exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(table_exists, 1, "{table} 必须随 v15 迁移创建");
        }
        let totals = store.usage_totals().unwrap();
        assert_eq!(
            totals.map(|t| t.sessions),
            Some(0),
            "有投影但零事实必须是 Some(sessions=0)，不是 None"
        );
    }

    #[test]
    fn injected_v14_to_v15_failure_rolls_back_schema_and_version() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        create_v6_schema(&conn);
        SqliteStore::migrate_v6_to_v7(&conn).unwrap();
        SqliteStore::migrate_v7_to_v8(&conn).unwrap();
        SqliteStore::migrate_v8_to_v9(&conn).unwrap();
        SqliteStore::migrate_v9_to_v10(&conn).unwrap();
        SqliteStore::migrate_v10_to_v11(&conn).unwrap();
        SqliteStore::migrate_v11_to_v12(&conn).unwrap();
        SqliteStore::migrate_v12_to_v13(&conn).unwrap();
        SqliteStore::migrate_v13_to_v14(&conn).unwrap();

        let err = SqliteStore::migrate_v14_to_v15_inner(&conn, true).unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("injected v14-to-v15"))
        );
        assert!(conn.is_autocommit());
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 14);
        let table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'usage_events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_exists, 0);
    }

    #[test]
    fn v15_catalog_migrates_to_v16_with_empty_repo_projection() {
        // 旧库（停在 v15）重开：v15→v16 迁移建表；投影为空——repo_totals
        // 返回空列表（未知 ≠ 零），由 rebuild 或后续 affected source 提交回填。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v15.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            SqliteStore::migrate_v6_to_v7(&conn).unwrap();
            SqliteStore::migrate_v7_to_v8(&conn).unwrap();
            SqliteStore::migrate_v8_to_v9(&conn).unwrap();
            SqliteStore::migrate_v9_to_v10(&conn).unwrap();
            SqliteStore::migrate_v10_to_v11(&conn).unwrap();
            SqliteStore::migrate_v11_to_v12(&conn).unwrap();
            SqliteStore::migrate_v12_to_v13(&conn).unwrap();
            SqliteStore::migrate_v13_to_v14(&conn).unwrap();
            SqliteStore::migrate_v14_to_v15(&conn).unwrap();
            let table_exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = 'session_repo_slugs'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(table_exists, 0, "v15 尚无 session_repo_slugs 表");
        }
        // 重新打开：触发 v15→v16 迁移。
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let conn = store.conn.borrow();
        let table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'session_repo_slugs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_exists, 1, "session_repo_slugs 必须随 v16 迁移创建");
        drop(conn);
        assert!(
            store.repo_totals().unwrap().is_empty(),
            "迁移后投影为空：空列表（未知），不是伪造的零行聚合"
        );
    }

    #[test]
    fn injected_v15_to_v16_failure_rolls_back_schema_and_version() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        create_v6_schema(&conn);
        SqliteStore::migrate_v6_to_v7(&conn).unwrap();
        SqliteStore::migrate_v7_to_v8(&conn).unwrap();
        SqliteStore::migrate_v8_to_v9(&conn).unwrap();
        SqliteStore::migrate_v9_to_v10(&conn).unwrap();
        SqliteStore::migrate_v10_to_v11(&conn).unwrap();
        SqliteStore::migrate_v11_to_v12(&conn).unwrap();
        SqliteStore::migrate_v12_to_v13(&conn).unwrap();
        SqliteStore::migrate_v13_to_v14(&conn).unwrap();
        SqliteStore::migrate_v14_to_v15(&conn).unwrap();

        let err = SqliteStore::migrate_v15_to_v16_inner(&conn, true).unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("injected v15-to-v16"))
        );
        assert!(conn.is_autocommit());
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 15);
        let table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'session_repo_slugs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_exists, 0);
    }

    #[test]
    fn v16_catalog_migrates_to_v17_stamping_projection_version_honestly() {
        // 投影**非空**的旧 v16 库：迁到 v17 后戳留在 DEFAULT 0——0 永不等于当前
        // INDEX_PROJECTION_VERSION（≥1），因此读路径 fail-closed、写路径自动
        // 重投影。绝不因为"迁移刚跑过"就谎报投影已收敛。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v16.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            SqliteStore::migrate_v6_to_v7(&conn).unwrap();
            SqliteStore::migrate_v7_to_v8(&conn).unwrap();
            SqliteStore::migrate_v8_to_v9(&conn).unwrap();
            SqliteStore::migrate_v9_to_v10(&conn).unwrap();
            SqliteStore::migrate_v10_to_v11(&conn).unwrap();
            SqliteStore::migrate_v11_to_v12(&conn).unwrap();
            SqliteStore::migrate_v12_to_v13(&conn).unwrap();
            SqliteStore::migrate_v13_to_v14(&conn).unwrap();
            SqliteStore::migrate_v14_to_v15(&conn).unwrap();
            SqliteStore::migrate_v15_to_v16(&conn).unwrap();
            let version: i64 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, 16);
            let has_column: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('store_metadata')
                     WHERE name = 'index_projection_version'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(has_column, 0, "v16 尚无 index_projection_version 列");
            // 旧二进制写下的投影行（内容形态无关，只要非空）。
            conn.execute(
                "INSERT INTO fts(id, text) VALUES('\"legacy\"', '配置 置备 备份')",
                [],
            )
            .unwrap();
        }
        let store = open_migration_fixture(&p);
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(
            store.index_projection_version().unwrap(),
            0,
            "投影非空的旧库迁移后必须留 0，驱动重投影"
        );
        assert!(!store.index_projection_is_current().unwrap());
    }

    #[test]
    fn v16_catalog_with_empty_projection_migrates_to_v17_as_current() {
        // 投影为空的旧库（含全新库）：没有任何旧变换写下的词元 → 迁移期直接
        // 标记当前版本，第一次打开不会被判失配、不产生 rebuild churn。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v16-empty.db");
        let p = path.to_string_lossy().into_owned();
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            create_v6_schema(&conn);
            SqliteStore::migrate_v6_to_v7(&conn).unwrap();
            SqliteStore::migrate_v7_to_v8(&conn).unwrap();
            SqliteStore::migrate_v8_to_v9(&conn).unwrap();
            SqliteStore::migrate_v9_to_v10(&conn).unwrap();
            SqliteStore::migrate_v10_to_v11(&conn).unwrap();
            SqliteStore::migrate_v11_to_v12(&conn).unwrap();
            SqliteStore::migrate_v12_to_v13(&conn).unwrap();
            SqliteStore::migrate_v13_to_v14(&conn).unwrap();
            SqliteStore::migrate_v14_to_v15(&conn).unwrap();
            SqliteStore::migrate_v15_to_v16(&conn).unwrap();
        }
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert_eq!(
            store.index_projection_version().unwrap(),
            i64::from(INDEX_PROJECTION_VERSION)
        );
        assert!(store.index_projection_is_current().unwrap());
    }

    #[test]
    fn injected_v16_to_v17_failure_rolls_back_schema_and_version() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        create_v6_schema(&conn);
        SqliteStore::migrate_v6_to_v7(&conn).unwrap();
        SqliteStore::migrate_v7_to_v8(&conn).unwrap();
        SqliteStore::migrate_v8_to_v9(&conn).unwrap();
        SqliteStore::migrate_v9_to_v10(&conn).unwrap();
        SqliteStore::migrate_v10_to_v11(&conn).unwrap();
        SqliteStore::migrate_v11_to_v12(&conn).unwrap();
        SqliteStore::migrate_v12_to_v13(&conn).unwrap();
        SqliteStore::migrate_v13_to_v14(&conn).unwrap();
        SqliteStore::migrate_v14_to_v15(&conn).unwrap();
        SqliteStore::migrate_v15_to_v16(&conn).unwrap();

        let err = SqliteStore::migrate_v16_to_v17_inner(&conn, true).unwrap_err();
        assert!(
            matches!(err, PortError::Backend(message) if message.contains("injected v16-to-v17"))
        );
        assert!(conn.is_autocommit());
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 16);
        let has_column: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('store_metadata')
                 WHERE name = 'index_projection_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(has_column, 0, "回滚后加法列不得残留");
    }

    #[test]
    fn usage_token_source_check_constraint_rejects_unknown_kinds() {
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.conn.borrow();
        let err = conn
            .execute(
                "INSERT INTO usage_events(
                     usage_id, session_id, input_tokens, output_tokens,
                     cache_read_tokens, cache_write_tokens, reasoning_tokens, token_source
                 ) VALUES('use_x', 'ses_x', 1, 0, 0, 0, 0, 'estimated')",
                [],
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("CHECK"),
            "token_source 闭集外取值必须被 CHECK 拒绝: {err}"
        );
    }

    #[test]
    fn fresh_schema_creates_source_scans_with_parser_version_column() {
        // 新库 v5 建表 DDL 直接带 parser_version 列（v9 provider_id 同一模式），
        // v14 迁移对新库短路，不再补 ALTER。
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.conn.borrow();
        let has_parser_version: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('source_scans')
                 WHERE name = 'parser_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(has_parser_version, 1);
    }

    #[test]
    fn session_metadata_search_is_private_incremental_and_rebuildable() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"metadata-session");
        let document = sid(IdKind::Document, b"metadata-document");
        let message = sid(IdKind::Message, b"metadata-message");
        let claim = |native: &str, pair_observed: bool| SourceResumeClaim {
            provider_id: "synthetic".into(),
            session_id: session.as_str().into(),
            provider_session_id: Some(native.into()),
            provider_session_id_state: "resolved".into(),
            original_working_directory: Some("C:/private/worktree".into()),
            original_working_directory_state: "resolved".into(),
            pair_observed,
        };
        let batch = |claim: SourceResumeClaim| SourceBatch {
            source_path: "private-source.jsonl".into(),
            entries: vec![
                entity_entry(&session),
                typed_document_entry(&document),
                typed_message_entry(&message, "first user metadata body"),
            ],
            placements: vec![placement(
                &session,
                &document,
                &message,
                0,
                false,
                Some((0, 5)),
            )],
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: vec![claim],
        };

        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch(claim(
                "native-one",
                true,
            ))))
            .unwrap();
        let text: String = store
            .conn
            .borrow()
            .query_row(
                "SELECT text FROM session_fts WHERE session_wire = ?1",
                [session.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(text.contains("native-one"));
        assert!(text.contains("C:/private/worktree"));
        assert!(text.contains("first user metadata body"));
        assert!(!text.contains("private-source.jsonl"));

        let native_hits = store.query("native-one", 10).unwrap();
        assert_eq!(native_hits.len(), 1);
        assert_eq!(native_hits[0].id, message);
        assert_eq!(native_hits[0].session_id.as_deref(), Some(session.as_str()));
        assert!(store.query("private-source.jsonl", 10).unwrap().is_empty());

        {
            let conn = store.conn.borrow();
            conn.execute("DELETE FROM session_fts", []).unwrap();
            conn.execute("DELETE FROM session_fts_ids", []).unwrap();
        }
        assert!(store.query("native-one", 10).unwrap().is_empty());
        store.rebuild_index().unwrap();
        assert_eq!(store.query("native-one", 10).unwrap().len(), 1);

        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch(claim(
                "native-two",
                false,
            ))))
            .unwrap();
        assert!(store.query("native-one", 10).unwrap().is_empty());
        let updated = store.query("native-two", 10).unwrap();
        assert_eq!(updated.len(), 1);
        let updated_text: String = store
            .conn
            .borrow()
            .query_row(
                "SELECT text FROM session_fts WHERE session_wire = ?1",
                [session.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!updated_text.contains("C:/private/worktree"));

        // A system-only representative must not suppress the metadata hit: the
        // Application layer removes system messages by default, so the Session
        // candidate remains the user-visible result for a native-id query.
        {
            let conn = store.conn.borrow();
            conn.execute(
                "UPDATE catalog SET payload = ?1 WHERE id = ?2",
                rusqlite::params![
                    serde_json::json!({ "role": "system", "text": "native-two" })
                        .to_string()
                        .into_bytes(),
                    message.as_str(),
                ],
            )
            .unwrap();
        }
        store.rebuild_index().unwrap();
        let system_metadata_hits = store.query("native-two", 10).unwrap();
        assert!(
            system_metadata_hits
                .iter()
                .any(|hit| hit.id == session && hit.session_id.as_deref() == Some(session.as_str()))
        );
    }

    /// 测试用确定性解析器：按 cwd 逐字查表；未列出的 cwd → None（模拟
    /// "git 检测失败"）。解析调用次数可数（生命周期测试断言按需探测）。
    struct MapRepoSlugResolver {
        map: std::collections::HashMap<String, Option<String>>,
        calls: RefCell<usize>,
    }

    impl RepoSlugResolver for MapRepoSlugResolver {
        fn resolve(&self, cwd: &str) -> Option<String> {
            *self.calls.borrow_mut() += 1;
            self.map.get(cwd).and_then(|slug| slug.clone())
        }
    }

    fn repo_slug_rows(store: &SqliteStore, session_wire: &str) -> Vec<String> {
        let conn = store.conn.borrow();
        let mut stmt = conn
            .prepare("SELECT repo_slug FROM session_repo_slugs WHERE session_wire = ?1")
            .unwrap();
        stmt.query_map([session_wire], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn repo_claim(
        session: &StableId,
        native: &str,
        cwd: Option<&str>,
        pair_observed: bool,
    ) -> StoredResumeClaim {
        StoredResumeClaim {
            session_id: session.as_str().into(),
            provider_id: "synthetic".into(),
            provider_session_id: Some(native.into()),
            provider_session_id_state: "resolved".into(),
            original_working_directory: cwd.map(str::to_string),
            original_working_directory_state: "resolved".into(),
            pair_observed,
        }
    }

    /// [`StoredResumeClaim`] → [`SourceResumeClaim`]（字段同构的端口类型转换，
    /// 仅测试装配 SourceBatch 用）。
    fn source_repo_claim(claim: &StoredResumeClaim) -> SourceResumeClaim {
        SourceResumeClaim {
            provider_id: claim.provider_id.clone(),
            session_id: claim.session_id.clone(),
            provider_session_id: claim.provider_session_id.clone(),
            provider_session_id_state: claim.provider_session_id_state.clone(),
            original_working_directory: claim.original_working_directory.clone(),
            original_working_directory_state: claim.original_working_directory_state.clone(),
            pair_observed: claim.pair_observed,
        }
    }

    // ---- 每参数门禁：session_repo_slug 纯函数 ----

    #[test]
    fn session_repo_slug_gates_every_claim_parameter() {
        let resolver = MapRepoSlugResolver {
            map: std::collections::HashMap::from([(
                "Z:/projects/app".into(),
                Some("github.com/o/app".into()),
            )]),
            calls: RefCell::new(0),
        };
        let base = repo_claim(
            &sid(IdKind::Session, b"gate"),
            "native",
            Some("Z:/projects/app"),
            true,
        );
        assert_eq!(
            SqliteStore::session_repo_slug(&base, &resolver).as_deref(),
            Some("github.com/o/app")
        );
        // provider_session_id 未 resolved → 不派生。
        let mut claim = repo_claim(
            &sid(IdKind::Session, b"gate"),
            "native",
            Some("Z:/projects/app"),
            true,
        );
        claim.provider_session_id_state = "missing".into();
        assert_eq!(SqliteStore::session_repo_slug(&claim, &resolver), None);
        // 未 pair 观测 → 不派生（cwd 未披露即不派生身份）。
        let claim = repo_claim(
            &sid(IdKind::Session, b"gate"),
            "native",
            Some("Z:/projects/app"),
            false,
        );
        assert_eq!(SqliteStore::session_repo_slug(&claim, &resolver), None);
        // cwd 未 resolved → 不派生。
        let mut claim = repo_claim(
            &sid(IdKind::Session, b"gate"),
            "native",
            Some("Z:/projects/app"),
            true,
        );
        claim.original_working_directory_state = "missing".into();
        assert_eq!(SqliteStore::session_repo_slug(&claim, &resolver), None);
        // cwd 缺失/空 → 不派生。
        let claim = repo_claim(&sid(IdKind::Session, b"gate"), "native", None, true);
        assert_eq!(SqliteStore::session_repo_slug(&claim, &resolver), None);
        let claim = repo_claim(&sid(IdKind::Session, b"gate"), "native", Some(""), true);
        assert_eq!(SqliteStore::session_repo_slug(&claim, &resolver), None);
        // 解析器检测失败 → 诚实 None，不猜。
        let claim = repo_claim(
            &sid(IdKind::Session, b"gate"),
            "native",
            Some("Z:/deleted-project"),
            true,
        );
        assert_eq!(SqliteStore::session_repo_slug(&claim, &resolver), None);
    }

    // ---- 投影生命周期：随 session 重建批派生/更新/清除 ----

    #[test]
    fn repo_slug_projection_follows_session_rebuild_batch() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_repo_slug_resolver(Box::new(MapRepoSlugResolver {
            map: std::collections::HashMap::from([(
                "Z:/projects/app".into(),
                Some("github.com/o/app".into()),
            )]),
            calls: RefCell::new(0),
        }));
        let session = sid(IdKind::Session, b"repo-session");
        let document = sid(IdKind::Document, b"repo-document");
        let message = sid(IdKind::Message, b"repo-message");
        let batch = |claim: SourceResumeClaim| SourceBatch {
            source_path: "repo-source.jsonl".into(),
            entries: vec![
                entity_entry(&session),
                typed_document_entry(&document),
                typed_message_entry(&message, "repo filter body"),
            ],
            placements: vec![placement(
                &session,
                &document,
                &message,
                0,
                false,
                Some((0, 5)),
            )],
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: vec![claim],
        };

        // 无投影行（默认 no-op 解析器 + 空库）：repo filter 诚实返回空。
        let filters = SearchFilters {
            repo: Some("github.com/o/app".into()),
            ..SearchFilters::default()
        };
        assert!(
            store
                .query_filtered(
                    SearchQuery {
                        text: "repo",
                        filters: &filters,
                    },
                    10,
                )
                .unwrap()
                .is_empty()
        );

        // 首次提交：affected-session 重建派生 slug 行。
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch(source_repo_claim(
                &repo_claim(&session, "repo-native", Some("Z:/projects/app"), true),
            ))))
            .unwrap();
        assert_eq!(
            repo_slug_rows(&store, session.as_str()),
            vec!["github.com/o/app".to_string()]
        );
        let hits = store
            .query_filtered(
                SearchQuery {
                    text: "repo",
                    filters: &filters,
                },
                10,
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, message);
        // store 层消息命中不装配 session_id（Application 层批量装配）；
        // 会话元数据命中因该消息已代表会话被去重（R3），无重复返回。
        // 其它 slug 不命中。
        let other = SearchFilters {
            repo: Some("gitlab.com/team/other".into()),
            ..SearchFilters::default()
        };
        assert!(
            store
                .query_filtered(
                    SearchQuery {
                        text: "repo",
                        filters: &other,
                    },
                    10,
                )
                .unwrap()
                .is_empty()
        );

        // 全量 rebuild：投影可重建（先清后派生）。
        {
            let conn = store.conn.borrow();
            conn.execute("DELETE FROM session_repo_slugs", []).unwrap();
        }
        store.rebuild_index().unwrap();
        assert_eq!(
            repo_slug_rows(&store, session.as_str()),
            vec!["github.com/o/app".to_string()]
        );

        // claim 被替换（仓库检测失败）：重建后无行——诚实降级。
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch(source_repo_claim(
                &repo_claim(
                    &session,
                    "repo-native-two",
                    Some("Z:/deleted-project"),
                    true,
                ),
            ))))
            .unwrap();
        assert!(repo_slug_rows(&store, session.as_str()).is_empty());
    }

    #[test]
    fn conflicting_claims_fail_closed_without_slug_row() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_repo_slug_resolver(Box::new(MapRepoSlugResolver {
            map: std::collections::HashMap::from([
                ("Z:/projects/app".into(), Some("github.com/o/app".into())),
                (
                    "Z:/projects/other".into(),
                    Some("github.com/o/other".into()),
                ),
            ]),
            calls: RefCell::new(0),
        }));
        let session = sid(IdKind::Session, b"conflict-session");
        let document = sid(IdKind::Document, b"conflict-document");
        let message = sid(IdKind::Message, b"conflict-message");
        let claim_a = source_repo_claim(&repo_claim(
            &session,
            "conflict-native",
            Some("Z:/projects/app"),
            true,
        ));
        let claim_b = source_repo_claim(&repo_claim(
            &session,
            "conflict-native",
            Some("Z:/projects/other"),
            true,
        ));
        // 两个 source 对同一会话给出不同 cwd：冲突必须 fail closed。
        let batch = |source_path: &str, claim: SourceResumeClaim| SourceBatch {
            source_path: source_path.into(),
            entries: vec![
                entity_entry(&session),
                typed_document_entry(&document),
                typed_message_entry(&message, "conflict body"),
            ],
            placements: vec![placement(
                &session,
                &document,
                &message,
                0,
                false,
                Some((0, 5)),
            )],
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: vec![claim],
        };
        store
            .commit_source_batches_if_changed(&[
                batch("conflict-a.jsonl", claim_a),
                batch("conflict-b.jsonl", claim_b),
            ])
            .unwrap();
        assert!(repo_slug_rows(&store, session.as_str()).is_empty());
    }

    #[test]
    fn repo_totals_aggregates_by_slug_with_deterministic_order() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.set_repo_slug_resolver(Box::new(MapRepoSlugResolver {
            map: std::collections::HashMap::from([
                (
                    "Z:/projects/shared".into(),
                    Some("github.com/o/shared".into()),
                ),
                ("Z:/projects/solo".into(), Some("github.com/o/solo".into())),
            ]),
            calls: RefCell::new(0),
        }));
        // 两个会话同 slug + 一个会话另一 slug：会话数降序、slug 升序。
        let batches: Vec<SourceBatch> = ["a", "b", "c"]
            .iter()
            .map(|tag| {
                let session = sid(IdKind::Session, tag.as_bytes());
                let document = sid(IdKind::Document, tag.as_bytes());
                let message = sid(IdKind::Message, tag.as_bytes());
                let cwd = if *tag == "c" {
                    "Z:/projects/solo"
                } else {
                    "Z:/projects/shared"
                };
                SourceBatch {
                    source_path: format!("repo-{tag}.jsonl"),
                    entries: vec![
                        entity_entry(&session),
                        typed_document_entry(&document),
                        typed_message_entry(&message, "totals body"),
                    ],
                    placements: vec![placement(
                        &session,
                        &document,
                        &message,
                        0,
                        false,
                        Some((0, 5)),
                    )],
                    edges: Vec::new(),
                    activities: Vec::new(),
                    usage_events: Vec::new(),
                    relation_complete: true,
                    len_bytes: None,
                    fingerprint: None,
                    provider_id: None,
                    resume_claims: vec![source_repo_claim(&repo_claim(
                        &session,
                        "totals-native",
                        Some(cwd),
                        true,
                    ))],
                }
            })
            .collect();
        store.commit_source_batches_if_changed(&batches).unwrap();
        let totals = store.repo_totals().unwrap();
        assert_eq!(
            totals,
            vec![
                RepoTotals {
                    repo_slug: "github.com/o/shared".into(),
                    sessions: 2,
                },
                RepoTotals {
                    repo_slug: "github.com/o/solo".into(),
                    sessions: 1,
                },
            ]
        );
    }

    #[test]
    fn repo_totals_empty_when_no_projection_rows() {
        let store = SqliteStore::open_in_memory().unwrap();
        assert!(store.repo_totals().unwrap().is_empty());
    }

    #[test]
    fn session_repo_slugs_batch_read_preserves_order_and_reports_unknown_as_none() {
        // ranking 的当前仓库偏好读的是这条投影：与入参同序、无行即 None
        // （未知 ≠ 匹配），空入参不查库。
        let store = SqliteStore::open_in_memory().unwrap();
        assert!(store.session_repo_slugs(&[]).unwrap().is_empty());
        store.set_repo_slug_resolver(Box::new(MapRepoSlugResolver {
            map: std::collections::HashMap::from([
                (
                    "Z:/projects/shared".into(),
                    Some("github.com/o/shared".into()),
                ),
                ("Z:/projects/solo".into(), Some("github.com/o/solo".into())),
            ]),
            calls: RefCell::new(0),
        }));
        let batches: Vec<SourceBatch> = ["a", "b", "c"]
            .iter()
            .map(|tag| {
                let session = sid(IdKind::Session, tag.as_bytes());
                let document = sid(IdKind::Document, tag.as_bytes());
                let message = sid(IdKind::Message, tag.as_bytes());
                let cwd = if *tag == "c" {
                    "Z:/projects/solo"
                } else {
                    "Z:/projects/shared"
                };
                SourceBatch {
                    source_path: format!("slug-read-{tag}.jsonl"),
                    entries: vec![
                        entity_entry(&session),
                        typed_document_entry(&document),
                        typed_message_entry(&message, "slug read body"),
                    ],
                    placements: vec![placement(
                        &session,
                        &document,
                        &message,
                        0,
                        false,
                        Some((0, 5)),
                    )],
                    edges: Vec::new(),
                    activities: Vec::new(),
                    usage_events: Vec::new(),
                    relation_complete: true,
                    len_bytes: None,
                    fingerprint: None,
                    provider_id: None,
                    resume_claims: vec![source_repo_claim(&repo_claim(
                        &session,
                        "slug-read-native",
                        Some(cwd),
                        true,
                    ))],
                }
            })
            .collect();
        store.commit_source_batches_if_changed(&batches).unwrap();

        let shared = sid(IdKind::Session, b"a");
        let also_shared = sid(IdKind::Session, b"b");
        let solo = sid(IdKind::Session, b"c");
        let unknown = sid(IdKind::Session, b"missing");
        // 顺序、重复 id 与"无投影行"三件事一起钉住。
        let slugs = store
            .session_repo_slugs(&[
                solo.clone(),
                unknown.clone(),
                shared.clone(),
                also_shared,
                shared,
            ])
            .unwrap();
        assert_eq!(
            slugs,
            vec![
                Some("github.com/o/solo".to_string()),
                None,
                Some("github.com/o/shared".to_string()),
                Some("github.com/o/shared".to_string()),
                Some("github.com/o/shared".to_string()),
            ]
        );
        // 未派生 repo 身份的会话恒 None，绝不回落成任意 slug。
        assert_eq!(store.session_repo_slugs(&[unknown]).unwrap(), vec![None]);
    }

    #[test]
    fn metadata_search_indexes_claim_without_user_message_or_placement() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"metadata-only-session");
        let claim = SourceResumeClaim {
            provider_id: "synthetic".into(),
            session_id: session.as_str().into(),
            provider_session_id: Some("metadata-only-native".into()),
            provider_session_id_state: "resolved".into(),
            original_working_directory: Some("C:/metadata-only-worktree".into()),
            original_working_directory_state: "resolved".into(),
            pair_observed: true,
        };
        let batch = SourceBatch {
            source_path: "metadata-only-source.jsonl".into(),
            entries: vec![entity_entry(&session)],
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: vec![claim],
        };

        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch))
            .unwrap();

        let hits = store.query("metadata-only-native", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, session);
        assert_eq!(hits[0].session_id.as_deref(), Some(session.as_str()));
        assert!(store.query("C:/metadata-only-worktree", 10).unwrap().len() == 1);
    }

    #[test]
    fn session_metadata_search_conflicting_claims_fail_closed() {
        // 两个 Source 认领同一 canonical Session，但 provider_session_id 不同。
        // `session_search_text` 必须 fail closed：两个 native id 都不进入
        // session_fts 投影（首个 user 消息的 title-like 字段仍在——它不来自声明）。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"conflict-session");
        let document = sid(IdKind::Document, b"conflict-document");
        let message = sid(IdKind::Message, b"conflict-message");
        let claim = |native: &str| SourceResumeClaim {
            provider_id: "synthetic".into(),
            session_id: session.as_str().into(),
            provider_session_id: Some(native.into()),
            provider_session_id_state: "resolved".into(),
            original_working_directory: Some("C:/conflict-cwd".into()),
            original_working_directory_state: "resolved".into(),
            pair_observed: true,
        };
        let batch = |source_path: &str, native: &str| SourceBatch {
            source_path: source_path.into(),
            entries: vec![
                entity_entry(&session),
                typed_document_entry(&document),
                typed_message_entry(&message, "conflict user body"),
            ],
            placements: vec![placement(&session, &document, &message, 0, false, None)],
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: vec![claim(native)],
        };
        store
            .commit_source_batches_if_changed(&[
                batch("conflict-a.jsonl", "conflict-native-alpha"),
                batch("conflict-b.jsonl", "conflict-native-beta"),
            ])
            .unwrap();

        // 任一冲突 native id 都不可检索。
        assert!(
            store.query("conflict-native-alpha", 10).unwrap().is_empty(),
            "conflicting native id alpha must not be indexed"
        );
        assert!(
            store.query("conflict-native-beta", 10).unwrap().is_empty(),
            "conflicting native id beta must not be indexed"
        );
        // session_fts 行仍含首 user 消息投影（不来自冲突声明）。
        let text: String = store
            .conn
            .borrow()
            .query_row(
                "SELECT text FROM session_fts WHERE session_wire = ?1",
                [session.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(text.contains("conflict user body"));
        assert!(!text.contains("conflict-native-alpha"));
        assert!(!text.contains("conflict-native-beta"));
    }

    #[test]
    fn source_paths_for_provider_returns_only_that_providers_paths() {
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"provider-diff-msg");
        let ses = sid(IdKind::Session, b"provider-diff-ses");
        let claude_path = "claude-source.jsonl";
        let codex_path = "codex-source.jsonl";
        // discover 批次显式携带 provider_id（显式 sync 留 NULL，discover 不
        // tombstone 根外路径）。
        let claude_batch = SourceBatch {
            source_path: claude_path.into(),
            entries: vec![
                (msg.clone(), b"payload".to_vec(), "text".into()),
                (ses.clone(), b"session".to_vec(), String::new()),
            ],
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(7),
            fingerprint: Some("fp-claude".into()),
            provider_id: Some("claude-code".into()),
            resume_claims: vec![SourceResumeClaim {
                provider_id: "claude-code".into(),
                session_id: ses.as_str().to_string(),
                provider_session_id: Some("claude-native".into()),
                provider_session_id_state: "resolved".into(),
                original_working_directory: None,
                original_working_directory_state: "missing".into(),
                pair_observed: false,
            }],
        };
        let codex_batch = SourceBatch {
            source_path: codex_path.into(),
            entries: vec![
                (msg.clone(), b"payload".to_vec(), "text".into()),
                (ses.clone(), b"session".to_vec(), String::new()),
            ],
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(6),
            fingerprint: Some("fp-codex".into()),
            provider_id: Some("codex".into()),
            resume_claims: vec![SourceResumeClaim {
                provider_id: "codex".into(),
                session_id: ses.as_str().to_string(),
                provider_session_id: Some("codex-native".into()),
                provider_session_id_state: "resolved".into(),
                original_working_directory: None,
                original_working_directory_state: "missing".into(),
                pair_observed: false,
            }],
        };
        store
            .commit_source_batches_if_changed(&[claude_batch, codex_batch])
            .unwrap();
        // discover 批次的 provider_id 落入 source_scans，per-provider diff 可见。
        assert_eq!(
            store.source_paths_for_provider("claude-code").unwrap(),
            vec![claude_path.to_string()]
        );
        assert_eq!(
            store.source_paths_for_provider("codex").unwrap(),
            vec![codex_path.to_string()]
        );
        // 未走 discover 的源（provider_id NULL）对 discover diff 不可见。
        assert_eq!(
            store.source_paths_for_provider("unknown").unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn source_provider_id_backfill_is_detected_and_never_overwrites() {
        let store = SqliteStore::open_in_memory().unwrap();
        let path = "provider-backfill.jsonl";
        let msg = sid(IdKind::Message, b"provider-backfill-msg");
        let explicit_batch = SourceBatch {
            source_path: path.into(),
            entries: vec![(msg, b"payload".to_vec(), "backfill text".into())],
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(7),
            fingerprint: Some("backfill-fp".into()),
            provider_id: None,
            resume_claims: Vec::new(),
        };
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&explicit_batch))
                .unwrap()
        );
        assert!(
            store
                .source_paths_for_provider("claude-code")
                .unwrap()
                .is_empty()
        );

        let mut discovered_batch = explicit_batch.clone();
        discovered_batch.provider_id = Some("claude-code".into());
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&discovered_batch))
                .unwrap(),
            "provider-only change must not be treated as current"
        );
        assert_eq!(
            store.source_paths_for_provider("claude-code").unwrap(),
            vec![path.to_string()]
        );
        assert_eq!(
            store
                .backfill_source_provider_ids(&[(path.into(), "codex".into())])
                .unwrap(),
            0,
            "backfill must not overwrite an existing provider"
        );
        assert!(store.source_paths_for_provider("codex").unwrap().is_empty());
    }

    #[test]
    fn backfill_source_provider_ids_associates_explicit_sync_rows() {
        let store = SqliteStore::open_in_memory().unwrap();
        let path = "explicit-provider-backfill.jsonl";
        let batch = SourceBatch {
            source_path: path.into(),
            entries: vec![(
                sid(IdKind::Message, b"explicit-provider-backfill-msg"),
                b"payload".to_vec(),
                "backfill text".into(),
            )],
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(7),
            fingerprint: Some("backfill-fp".into()),
            provider_id: None,
            resume_claims: Vec::new(),
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch))
            .unwrap();

        assert_eq!(
            store
                .backfill_source_provider_ids(&[(path.into(), "claude-code".into())])
                .unwrap(),
            1
        );
        assert_eq!(
            store.source_paths_for_provider("claude-code").unwrap(),
            vec![path.to_string()]
        );
    }

    #[test]
    fn source_paths_requiring_relation_scan_excludes_complete_sources() {
        let store = SqliteStore::open_in_memory().unwrap();
        let path = "relation-recovery.jsonl".to_string();
        let msg = sid(IdKind::Message, b"relation-recovery-msg");
        let batch = SourceBatch {
            source_path: path.clone(),
            entries: vec![(msg, b"payload".to_vec(), "text".into())],
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(7),
            fingerprint: Some("recovery-fp".into()),
            provider_id: None,
            resume_claims: Vec::new(),
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch))
            .unwrap();
        assert!(
            store
                .source_paths_requiring_relation_scan(std::slice::from_ref(&path))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn source_paths_requiring_relation_scan_reports_incomplete_sources() {
        let store = SqliteStore::open_in_memory().unwrap();
        let path = "relation-recovery-incomplete.jsonl".to_string();
        let msg = sid(IdKind::Message, b"relation-recovery-incomplete-msg");
        let batch = SourceBatch {
            source_path: path.clone(),
            entries: vec![(msg, b"payload".to_vec(), "text".into())],
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: false,
            len_bytes: Some(7),
            fingerprint: Some("recovery-incomplete-fp".into()),
            provider_id: None,
            resume_claims: Vec::new(),
        };
        store
            .commit_source_batches_if_changed(std::slice::from_ref(&batch))
            .unwrap();
        assert_eq!(
            store
                .source_paths_requiring_relation_scan(std::slice::from_ref(&path))
                .unwrap(),
            BTreeSet::from([path])
        );
    }

    #[test]
    fn resume_claim_written_replaced_and_cleared_atomically_with_source() {
        let store = SqliteStore::open_in_memory().unwrap();
        let msg = sid(IdKind::Message, b"resume-claim-msg");
        let ses = sid(IdKind::Session, b"resume-claim-ses");
        let claim = |native: &str| SourceResumeClaim {
            provider_id: "codex".into(),
            session_id: ses.as_str().to_string(),
            provider_session_id: Some(native.into()),
            provider_session_id_state: "resolved".into(),
            original_working_directory: Some("C:/work".into()),
            original_working_directory_state: "resolved".into(),
            pair_observed: true,
        };
        type Entries = Vec<(StableId, Vec<u8>, String)>;
        let batch = |resume_claim: Option<SourceResumeClaim>, entries: Entries| SourceBatch {
            source_path: "resume.jsonl".into(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: resume_claim.into_iter().collect(),
            entries,
        };
        let populated = batch(
            Some(claim("native-1")),
            vec![
                (msg.clone(), b"payload".to_vec(), "same text".into()),
                (ses.clone(), b"session".to_vec(), String::new()),
            ],
        );

        // 写入：声明随 source 事务落表。
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&populated))
                .unwrap()
        );
        {
            let conn = store.conn.borrow();
            assert_eq!(
                stored_resume_claim(&conn, "resume.jsonl").unwrap(),
                BTreeMap::from([(
                    ses.as_str().to_string(),
                    StoredResumeClaim::from_claim(&claim("native-1"))
                )]),
            );
        }
        // 同一批次重提交（声明未变）：内容级 no-op，不推进 generation。
        let generation = store.active_generation().unwrap();
        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&populated))
                .unwrap()
        );
        assert_eq!(store.active_generation().unwrap(), generation);

        // 原子替换：只有声明变化也必须走提交路径，旧声明被替换而非残留。
        let replaced = batch(
            Some(claim("native-2")),
            vec![
                (msg.clone(), b"payload".to_vec(), "same text".into()),
                (ses.clone(), b"session".to_vec(), String::new()),
            ],
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&replaced))
                .unwrap()
        );
        {
            let conn = store.conn.borrow();
            assert_eq!(
                stored_resume_claim(&conn, "resume.jsonl").unwrap(),
                BTreeMap::from([(
                    ses.as_str().to_string(),
                    StoredResumeClaim::from_claim(&claim("native-2"))
                )]),
            );
        }

        // 声明移除：batch 带 None 声明时同事务清除旧行。
        let cleared = batch(
            None,
            vec![(msg.clone(), b"payload".to_vec(), "same text".into())],
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&cleared))
                .unwrap()
        );
        {
            let conn = store.conn.borrow();
            assert_eq!(
                stored_resume_claim(&conn, "resume.jsonl").unwrap(),
                BTreeMap::new()
            );
        }

        // source 移除（空 scan）：声明随 tombstone 同事务清除。
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&batch(
                    Some(claim("native-3")),
                    vec![
                        (msg.clone(), b"payload".to_vec(), "same text".into()),
                        (ses.clone(), b"session".to_vec(), String::new()),
                    ],
                )))
                .unwrap()
        );
        let retired = batch(None, Vec::new());
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&retired))
                .unwrap()
        );
        {
            let conn = store.conn.borrow();
            assert_eq!(
                stored_resume_claim(&conn, "resume.jsonl").unwrap(),
                BTreeMap::new()
            );
        }
        assert_eq!(store.count().unwrap(), 0);
    }

    #[test]
    fn resume_of_resolves_batched_claims_in_input_order_without_n_plus_one() {
        // 超过 BATCH_IN_CHUNK 的批量解析：分块 IN 一次调用返回全部，语句数与
        // session 数无关（无 N+1），输出与输入同序。
        let store = SqliteStore::open_in_memory().unwrap();
        let resolved: Vec<(StableId, usize)> = (0..BATCH_IN_CHUNK + 2)
            .map(|i| {
                (
                    sid(IdKind::Session, format!("bulk-resume-{i}").as_bytes()),
                    i,
                )
            })
            .collect();
        let ambiguous = sid(IdKind::Session, b"ambig-ses");
        let missing = sid(IdKind::Session, b"miss-ses");
        let unclaimed = sid(IdKind::Session, b"none-ses");
        {
            let conn = store.conn.borrow();
            for (id, i) in &resolved {
                conn.execute(
                    "INSERT INTO source_session_resume_claims(
                         source_path, session_id, provider_id, provider_session_id,
                         provider_session_id_state, original_working_directory,
                         original_working_directory_state, pair_observed
                     ) VALUES(?1, ?2, 'codex', ?3, 'resolved', ?4, 'resolved', 1)",
                    rusqlite::params![
                        format!("bulk-source-{i}.jsonl"),
                        id.as_str(),
                        format!("native-{i}"),
                        format!("C:/dir-{i}"),
                    ],
                )
                .unwrap();
            }
            conn.execute(
                "INSERT INTO source_session_resume_claims(
                     source_path, session_id, provider_id, provider_session_id,
                     provider_session_id_state, original_working_directory,
                     original_working_directory_state, pair_observed
                 ) VALUES('ambig.jsonl', ?1, 'codex', NULL, 'ambiguous', NULL, 'missing', 0)",
                [ambiguous.as_str()],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO source_session_resume_claims(
                     source_path, session_id, provider_id, provider_session_id,
                     provider_session_id_state, original_working_directory,
                     original_working_directory_state, pair_observed
                 ) VALUES('miss.jsonl', ?1, 'codex', NULL, 'missing', NULL, 'missing', 0)",
                [missing.as_str()],
            )
            .unwrap();
        }

        // 输入刻意乱序（unclaimed 打头 + resolved 倒序混排）。
        let mut input: Vec<StableId> = Vec::new();
        input.push(unclaimed.clone());
        input.extend(resolved.iter().rev().map(|(id, _)| id.clone()));
        input.push(ambiguous.clone());
        input.push(missing.clone());

        let statements = counted_statements(&store, || store.resume_of(&input).unwrap());
        assert_eq!(
            statements, 2,
            "resume_of 必须分块 IN（BATCH_IN_CHUNK 分块），不得逐 session 查询"
        );

        let metas = store.resume_of(&input).unwrap();
        assert_eq!(metas.len(), input.len());
        let expected: BTreeMap<String, usize> = resolved
            .iter()
            .map(|(id, i)| (id.as_str().to_string(), *i))
            .collect();
        for (id, meta) in input.iter().zip(&metas) {
            assert_eq!(&meta.session_id, id, "输出必须与输入同序");
            if id == &unclaimed {
                assert!(!meta.resume_available);
                assert_eq!(meta.provider_id, None);
                assert_eq!(meta.provider_session_id, None);
                assert_eq!(meta.original_working_directory, None);
                assert_eq!(
                    meta.unavailable_reason.as_deref(),
                    Some("no resume metadata claims")
                );
            } else if id == &ambiguous {
                assert!(!meta.resume_available);
                assert_eq!(meta.provider_id.as_deref(), Some("codex"));
                assert_eq!(meta.provider_session_id, None);
                assert_eq!(meta.original_working_directory, None);
                assert_eq!(
                    meta.unavailable_reason.as_deref(),
                    Some("ambiguous provider session id")
                );
            } else if id == &missing {
                assert!(!meta.resume_available);
                assert_eq!(meta.provider_id.as_deref(), Some("codex"));
                assert_eq!(meta.provider_session_id, None);
                assert_eq!(meta.original_working_directory, None);
                assert_eq!(
                    meta.unavailable_reason.as_deref(),
                    Some("provider session id not observed")
                );
            } else {
                let i = expected[id.as_str()];
                assert!(meta.resume_available, "resolved 声明必须可恢复");
                assert_eq!(meta.provider_id.as_deref(), Some("codex"));
                assert_eq!(meta.provider_session_id, Some(format!("native-{i}")));
                assert_eq!(meta.original_working_directory, Some(format!("C:/dir-{i}")));
                assert_eq!(meta.unavailable_reason, None);
            }
        }
    }

    #[test]
    fn resume_of_fails_closed_for_conflicting_source_claims() {
        // 同一 session 的 source-scoped 声明冲突时不得按路径或发现顺序挑选；
        // 固定返回不可恢复且不披露任一冲突值。
        let store = SqliteStore::open_in_memory().unwrap();
        let ses = sid(IdKind::Session, b"multi-source-ses");
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO source_session_resume_claims(
                     source_path, session_id, provider_id, provider_session_id,
                     provider_session_id_state, original_working_directory,
                     original_working_directory_state, pair_observed
                 ) VALUES('b-source.jsonl', ?1, 'codex', 'native-b', 'resolved', 'C:/b', 'resolved', 1)",
                [ses.as_str()],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO source_session_resume_claims(
                     source_path, session_id, provider_id, provider_session_id,
                     provider_session_id_state, original_working_directory,
                     original_working_directory_state, pair_observed
                 ) VALUES('a-source.jsonl', ?1, 'claude-code', 'native-a', 'resolved', 'C:/a', 'resolved', 1)",
                [ses.as_str()],
            )
            .unwrap();
        }
        let metas = store.resume_of(std::slice::from_ref(&ses)).unwrap();
        assert_eq!(metas.len(), 1);
        assert!(!metas[0].resume_available);
        assert_eq!(metas[0].provider_id, None);
        assert_eq!(metas[0].provider_session_id, None);
        assert_eq!(metas[0].original_working_directory, None);
        assert_eq!(
            metas[0].unavailable_reason.as_deref(),
            Some("conflicting resume metadata claims")
        );
    }

    #[test]
    fn resume_of_without_claims_reports_unavailable_not_resumable() {
        // legacy 无声明目录：恒可检索、不可恢复——全字段 None + 明确 reason。
        let store = SqliteStore::open_in_memory().unwrap();
        let ids: Vec<StableId> = (0..3)
            .map(|i| sid(IdKind::Session, format!("legacy-{i}").as_bytes()))
            .collect();
        let metas = store.resume_of(&ids).unwrap();
        for (id, meta) in ids.iter().zip(&metas) {
            assert_eq!(&meta.session_id, id);
            assert!(!meta.resume_available);
            assert_eq!(meta.provider_id, None);
            assert_eq!(meta.provider_session_id, None);
            assert_eq!(meta.original_working_directory, None);
            assert_eq!(
                meta.unavailable_reason.as_deref(),
                Some("no resume metadata claims")
            );
        }
    }

    #[test]
    fn resume_of_hides_cwd_when_pair_not_observed() {
        // Provider Session ID 已 resolved，但 cwd 与 session_id 未配对观测
        // (pair_observed=false)：cwd 必须返回 None，resume 仍可用。
        let store = SqliteStore::open_in_memory().unwrap();
        let ses = sid(IdKind::Session, b"pair-false-ses");
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO source_session_resume_claims(
                     source_path, session_id, provider_id, provider_session_id,
                     provider_session_id_state, original_working_directory,
                     original_working_directory_state, pair_observed
                 ) VALUES('pair.jsonl', ?1, 'codex', 'native-1', 'resolved', 'C:/unpaired', 'resolved', 0)",
                [ses.as_str()],
            )
            .unwrap();
        }
        let metas = store.resume_of(std::slice::from_ref(&ses)).unwrap();
        assert_eq!(metas.len(), 1);
        assert!(metas[0].resume_available);
        assert_eq!(metas[0].provider_id.as_deref(), Some("codex"));
        assert_eq!(metas[0].provider_session_id.as_deref(), Some("native-1"));
        assert_eq!(
            metas[0].original_working_directory, None,
            "未配对观测的 cwd 不得披露"
        );
        assert_eq!(metas[0].unavailable_reason, None);
    }

    #[test]
    fn resume_of_empty_input_returns_empty_without_query() {
        // 空输入短路返回空 Vec，不发起任何 SQL 查询。
        let store = SqliteStore::open_in_memory().unwrap();
        let statements = counted_statements(&store, || {
            let metas = store.resume_of(&[]).unwrap();
            assert!(metas.is_empty());
        });
        assert_eq!(statements, 0, "空输入不得触发数据库查询");
    }

    #[test]
    fn resume_claims_indexed_by_session_id_without_full_scan() {
        // resume_of 分块 IN 查询必须命中 session_id 索引而非全表扫描。
        let store = SqliteStore::open_in_memory().unwrap();
        let ses = sid(IdKind::Session, b"index-probe-ses");
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO source_session_resume_claims(
                     source_path, session_id, provider_id, provider_session_id,
                     provider_session_id_state, original_working_directory,
                     original_working_directory_state, pair_observed
                 ) VALUES('idx.jsonl', ?1, 'codex', 'native-1', 'resolved', 'C:/dir', 'resolved', 1)",
                [ses.as_str()],
            )
            .unwrap();
        }
        let conn = store.conn.borrow();
        let plan: String = conn
            .query_row(
                "EXPLAIN QUERY PLAN
                 SELECT source_path, provider_id, provider_session_id,
                        provider_session_id_state, original_working_directory,
                        original_working_directory_state, pair_observed
                 FROM source_session_resume_claims WHERE session_id = ?1",
                [ses.as_str()],
                |row| row.get::<_, String>(3),
            )
            .unwrap();
        assert!(
            !plan.to_lowercase().contains("scan"),
            "resume 查询必须使用索引而非全表扫描，实际 plan: {plan}"
        );
    }

    // ---- 工具活动（v12）：提交/去重/tombstone/边界/查询 ----

    fn tool_activity_batch(
        source_path: &str,
        message_id: &StableId,
        name: &str,
        kind: &str,
        target: Option<&str>,
        status: &str,
    ) -> SourceBatch {
        SourceBatch {
            source_path: source_path.into(),
            entries: vec![entity_entry(message_id)],
            placements: Vec::new(),
            edges: Vec::new(),
            activities: vec![SourceActivity {
                message_id: message_id.clone(),
                activity: ToolActivity {
                    kind: match kind {
                        "file" => ToolActivityKind::File,
                        "command" => ToolActivityKind::Command,
                        "web" => ToolActivityKind::Web,
                        "query" => ToolActivityKind::Query,
                        _ => ToolActivityKind::Unknown,
                    },
                    actor: ToolActivityActor::Main,
                    name: name.into(),
                    target: target.map(str::to_string),
                    status: match status {
                        "success" => ToolActivityStatus::Success,
                        "error" => ToolActivityStatus::Error,
                        _ => ToolActivityStatus::Unknown,
                    },
                },
            }],
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(1),
            fingerprint: Some(source_path.into()),
            provider_id: None,
            resume_claims: Vec::new(),
        }
    }

    fn activity_rows(
        store: &SqliteStore,
    ) -> Vec<(String, String, String, String, Option<String>, String)> {
        store
            .conn
            .borrow()
            .prepare("SELECT message_id, kind, actor, name, target, status FROM tool_activities ORDER BY activity_id")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn activity_commit_roundtrips_and_survives_unchanged_resync() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"activity-msg");
        let source = tool_activity_batch(
            "activity.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls -la"),
            "success",
        );
        assert!(store.commit_source_batches_if_changed(&[source]).unwrap());
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);

        let rows = activity_rows(&store);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, message.as_str());
        assert_eq!(rows[0].1, "command");
        assert_eq!(rows[0].2, "main");
        assert_eq!(rows[0].3, "Bash");
        assert_eq!(rows[0].4.as_deref(), Some("ls -la"));
        assert_eq!(rows[0].5, "success");

        // 内容未变 → 重同步是 no-op（sources_are_current 含活动行/claims 对比）。
        let source = tool_activity_batch(
            "activity.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls -la"),
            "success",
        );
        assert!(!store.commit_source_batches_if_changed(&[source]).unwrap());
        assert_eq!(activity_rows(&store).len(), 1);
    }

    #[test]
    fn activity_commit_is_idempotent_across_fingerprint_change() {
        // 指纹变化（重解析）但活动事实相同：幂等重写，不产生重复行。
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"activity-msg");
        let first = tool_activity_batch(
            "activity.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls -la"),
            "success",
        );
        assert!(store.commit_source_batches_if_changed(&[first]).unwrap());
        let mut second = tool_activity_batch(
            "activity.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls -la"),
            "success",
        );
        second.fingerprint = Some("changed-fingerprint".into());
        assert!(store.commit_source_batches_if_changed(&[second]).unwrap());
        assert_eq!(activity_rows(&store).len(), 1);
    }

    #[test]
    fn repeated_identical_activity_in_one_source_dedupes_instead_of_failing() {
        // 真实 transcript 回归：一条消息里两次完全相同的工具调用（同
        // kind/actor/name/target/status，例如连续读同一文件，或长 target 截断后
        // 相同）派生同一内容寻址 id。这是同一检索面事实的重复观察，必须按 id
        // 去重；此前批校验把它当作 duplicate activity ids 拒绝，导致整批 sync
        // 以 catalog_error（exit 6）失败。
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"activity-dup");
        let mut source = tool_activity_batch(
            "dup.jsonl",
            &message,
            "Read",
            "file",
            Some("src/lib.rs"),
            "success",
        );
        let repeated = source.activities[0].clone();
        source.activities.push(repeated);
        assert!(store.commit_source_batches_if_changed(&[source]).unwrap());
        let rows = activity_rows(&store);
        assert_eq!(rows.len(), 1, "重复观察折叠为一行");
        assert_eq!(rows[0].3, "Read");
        assert_eq!(rows[0].4.as_deref(), Some("src/lib.rs"));
        // claim 也只有一条：activity_ids 由去重后的集合派生。
        let claims: i64 = store
            .conn
            .borrow()
            .query_row(
                "SELECT COUNT(*) FROM tool_activity_membership WHERE source_path = 'dup.jsonl'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claims, 1);
    }

    #[test]
    fn activity_target_is_bounded_before_storage() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"activity-bound");
        let long_target = "x".repeat(TOOL_ACTIVITY_TARGET_MAX_CHARS + 100);
        let source = tool_activity_batch(
            "bound.jsonl",
            &message,
            "Bash",
            "command",
            Some(&long_target),
            "success",
        );
        store.commit_source_batches_if_changed(&[source]).unwrap();
        let rows = activity_rows(&store);
        assert_eq!(rows.len(), 1);
        let stored = rows[0].4.clone().unwrap();
        assert_eq!(stored.chars().count(), TOOL_ACTIVITY_TARGET_MAX_CHARS);
    }

    #[test]
    fn complete_rescan_removes_dropped_activities_and_empty_scan_tombstones_all() {
        let store = SqliteStore::open_in_memory().unwrap();
        let msg_a = sid(IdKind::Message, b"act-a");
        let msg_b = sid(IdKind::Message, b"act-b");
        let batch = |entries: Vec<(StableId, Vec<u8>, String)>, activities: Vec<SourceActivity>| {
            SourceBatch {
                source_path: "rescan.jsonl".into(),
                entries,
                placements: Vec::new(),
                edges: Vec::new(),
                activities,
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: Some(1),
                fingerprint: Some("rescan-fp".into()),
                provider_id: None,
                resume_claims: Vec::new(),
            }
        };
        let activity = |message_id: &StableId, name: &str| SourceActivity {
            message_id: message_id.clone(),
            activity: ToolActivity {
                kind: ToolActivityKind::Command,
                actor: ToolActivityActor::Main,
                name: name.into(),
                target: Some(format!("cmd-{name}")),
                status: ToolActivityStatus::Success,
            },
        };
        let first = batch(
            vec![entity_entry(&msg_a), entity_entry(&msg_b)],
            vec![activity(&msg_a, "Bash"), activity(&msg_b, "Read")],
        );
        assert!(store.commit_source_batches_if_changed(&[first]).unwrap());
        assert_eq!(activity_rows(&store).len(), 2);

        // 完整重扫只保留 Read：Bash 活动被 tombstone（claim 消失且无他人认领）。
        let second = batch(vec![entity_entry(&msg_b)], vec![activity(&msg_b, "Read")]);
        assert!(store.commit_source_batches_if_changed(&[second]).unwrap());
        let rows = activity_rows(&store);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].3, "Read");

        // 空源（整源清空，relation_complete=true）：全部活动 tombstone。
        let empty = batch(Vec::new(), Vec::new());
        assert!(store.commit_source_batches_if_changed(&[empty]).unwrap());
        assert!(activity_rows(&store).is_empty());
    }

    #[test]
    fn incomplete_scan_unions_activities_without_tombstoning() {
        let store = SqliteStore::open_in_memory().unwrap();
        let msg_a = sid(IdKind::Message, b"inc-a");
        let msg_b = sid(IdKind::Message, b"inc-b");
        let batch = |entries: Vec<(StableId, Vec<u8>, String)>,
                     activities: Vec<SourceActivity>,
                     complete: bool| {
            SourceBatch {
                source_path: "incomplete.jsonl".into(),
                entries,
                placements: Vec::new(),
                edges: Vec::new(),
                activities,
                usage_events: Vec::new(),
                relation_complete: complete,
                len_bytes: Some(1),
                fingerprint: Some("inc-fp".into()),
                provider_id: None,
                resume_claims: Vec::new(),
            }
        };
        let activity = |message_id: &StableId, name: &str| SourceActivity {
            message_id: message_id.clone(),
            activity: ToolActivity {
                kind: ToolActivityKind::File,
                actor: ToolActivityActor::Main,
                name: name.into(),
                target: None,
                status: ToolActivityStatus::Success,
            },
        };
        let first = batch(
            vec![entity_entry(&msg_a)],
            vec![activity(&msg_a, "Read")],
            true,
        );
        assert!(store.commit_source_batches_if_changed(&[first]).unwrap());

        // 不完整重扫（skipped>0 → relation_complete=false）：union，不删旧活动。
        let second = batch(
            vec![entity_entry(&msg_b)],
            vec![activity(&msg_b, "Grep")],
            false,
        );
        assert!(store.commit_source_batches_if_changed(&[second]).unwrap());
        let rows = activity_rows(&store);
        assert_eq!(rows.len(), 2, "不完整扫描必须保留未观察到的活动");
        let names: Vec<&str> = rows.iter().map(|row| row.3.as_str()).collect();
        assert!(names.contains(&"Read") && names.contains(&"Grep"));
    }

    #[test]
    fn identical_activity_from_two_sources_dedups_into_one_row() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"shared-act");
        let first = tool_activity_batch(
            "source-a.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls"),
            "success",
        );
        let second = tool_activity_batch(
            "source-b.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls"),
            "success",
        );
        assert!(
            store
                .commit_source_batches_if_changed(&[first, second])
                .unwrap()
        );
        assert_eq!(
            activity_rows(&store).len(),
            1,
            "同事实同锚点 → 同一行，两份 claim"
        );

        // 一个源消失（空批）不删除仍被另一源认领的活动。
        let empty = SourceBatch {
            source_path: "source-a.jsonl".into(),
            entries: Vec::new(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(0),
            fingerprint: Some("empty-a".into()),
            provider_id: None,
            resume_claims: Vec::new(),
        };
        assert!(store.commit_source_batches_if_changed(&[empty]).unwrap());
        assert_eq!(activity_rows(&store).len(), 1);

        // 两个源都消失 → 行删除。
        let empty_b = SourceBatch {
            source_path: "source-b.jsonl".into(),
            entries: Vec::new(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(0),
            fingerprint: Some("empty-b".into()),
            provider_id: None,
            resume_claims: Vec::new(),
        };
        assert!(store.commit_source_batches_if_changed(&[empty_b]).unwrap());
        assert!(activity_rows(&store).is_empty());
    }

    #[test]
    fn distinct_activity_facts_on_one_message_coexist_across_sources() {
        // activity_id 由事实内容寻址：同一消息上不同事实 → 不同行，两源各自认领。
        // （同消息同事实 → 同一行 + 双 claim，见 identical_activity_from_two_sources。）
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"conflict-act");
        let mut first = tool_activity_batch(
            "source-a.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls"),
            "success",
        );
        first.entries = vec![entity_entry(&message)];
        first.fingerprint = Some("fp-a".into());
        let mut second = tool_activity_batch(
            "source-b.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls -la"),
            "success",
        );
        second.entries = vec![entity_entry(&message)];
        second.fingerprint = Some("fp-b".into());
        assert!(
            store
                .commit_source_batches_if_changed(&[first, second])
                .unwrap()
        );
        let rows = activity_rows(&store);
        assert_eq!(rows.len(), 2, "不同事实 → 两行（锚点相同、事实不同）");
        let targets: Vec<Option<&str>> = rows.iter().map(|row| row.4.as_deref()).collect();
        assert!(targets.contains(&Some("ls")) && targets.contains(&Some("ls -la")));
    }

    #[test]
    fn activity_anchored_to_non_message_is_rejected() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"not-a-message");
        let source = tool_activity_batch(
            "bad-anchor.jsonl",
            &session,
            "Bash",
            "command",
            None,
            "success",
        );
        let error = store
            .commit_source_batches_if_changed(&[source])
            .unwrap_err();
        assert!(matches!(error, PortError::Backend(_)));
    }

    // ---- 工具活动保留策略（v12）：source 退役级联 + 孤儿扫描/修剪 ----

    #[test]
    fn source_removal_clears_activities_and_membership_in_same_transaction() {
        // 保留策略核心不变量（级联验证）：source 退役（空完整 scan）时，
        // 其活动的 tool_activities 行与 tool_activity_membership 行在同一
        // 提交内消失——投影行绝不比 catalog 事实活得更久，无需事后清理。
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"retire-activity");
        let full = tool_activity_batch(
            "retire-activity.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls"),
            "success",
        );
        assert!(store.commit_source_batches_if_changed(&[full]).unwrap());
        assert_eq!(activity_rows(&store).len(), 1);
        {
            let conn = store.conn.borrow();
            let claims: i64 = conn
                .query_row("SELECT COUNT(*) FROM tool_activity_membership", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(claims, 1, "活动行必须有对应 claim");
        }

        let empty = SourceBatch {
            source_path: "retire-activity.jsonl".into(),
            entries: Vec::new(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(0),
            fingerprint: Some("empty".into()),
            provider_id: None,
            resume_claims: Vec::new(),
        };
        assert!(store.commit_source_batches_if_changed(&[empty]).unwrap());
        assert!(store.get(&message).unwrap().is_none(), "消息随 source 退役");
        assert!(
            activity_rows(&store).is_empty(),
            "活动行必须与消息在同一提交内删除"
        );
        {
            let conn = store.conn.borrow();
            let claims: i64 = conn
                .query_row("SELECT COUNT(*) FROM tool_activity_membership", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(claims, 0, "成员行必须与 source 在同一提交内删除");
        }
        assert_eq!(
            store.orphaned_activity_counts().unwrap(),
            (0, 0),
            "级联后的库无孤儿行"
        );
    }

    #[test]
    fn orphaned_activity_scan_reports_activity_whose_message_was_removed() {
        // 漂移状态可通过公共 API 达到：完整重扫退役了消息但仍观察其活动
        // （不一致的 source 投影）→ 活动行与 claim 留存、锚点悬空。
        // 扫描必须报出，修剪必须可确定性清除。
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"orphan-msg");
        let first = tool_activity_batch("orphan.jsonl", &message, "Read", "file", None, "success");
        assert!(store.commit_source_batches_if_changed(&[first]).unwrap());

        let mut second =
            tool_activity_batch("orphan.jsonl", &message, "Read", "file", None, "success");
        second.entries = Vec::new();
        second.fingerprint = Some("rescan-fp".into());
        assert!(store.commit_source_batches_if_changed(&[second]).unwrap());
        assert!(store.get(&message).unwrap().is_none(), "消息已退役");
        assert_eq!(activity_rows(&store).len(), 1, "活动行仍在（锚点悬空）");
        let (activities, memberships) = store.orphaned_activity_counts().unwrap();
        assert_eq!(activities, 1, "悬空活动是孤儿");
        assert_eq!(memberships, 0, "活动行还在，其 claim 不算孤儿");
    }

    #[test]
    fn orphaned_membership_scan_reports_dangling_claim() {
        // 指向不存在活动的成员行（悬空 claim）：写路径同事务保证无法产生，
        // 只能由裸批/历史漂移造成；直接 SQL 构造并验证扫描报出。
        let store = SqliteStore::open_in_memory().unwrap();
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO tool_activity_membership(source_path, activity_id)
                 VALUES('ghost.jsonl', 'act_v1_deadbeefdeadbeef')",
                [],
            )
            .unwrap();
        }
        let (activities, memberships) = store.orphaned_activity_counts().unwrap();
        assert_eq!(activities, 0);
        assert_eq!(memberships, 1);
    }

    #[test]
    fn purge_orphaned_activities_removes_only_dangling_projection_rows() {
        // 修剪只删悬空行：合法活动、合法 claim、catalog 全部原样保留。
        let store = SqliteStore::open_in_memory().unwrap();
        let live_msg = sid(IdKind::Message, b"live-msg");
        let live = tool_activity_batch(
            "live.jsonl",
            &live_msg,
            "Bash",
            "command",
            Some("ls"),
            "success",
        );
        assert!(store.commit_source_batches_if_changed(&[live]).unwrap());

        // 孤儿活动：消息退役、活动留存（见 orphaned_activity_scan_*）。
        let orphan_msg = sid(IdKind::Message, b"purge-orphan-msg");
        let orphaned =
            tool_activity_batch("orphan.jsonl", &orphan_msg, "Read", "file", None, "success");
        assert!(store.commit_source_batches_if_changed(&[orphaned]).unwrap());
        let mut rescan =
            tool_activity_batch("orphan.jsonl", &orphan_msg, "Read", "file", None, "success");
        rescan.entries = Vec::new();
        rescan.fingerprint = Some("rescan-fp".into());
        assert!(store.commit_source_batches_if_changed(&[rescan]).unwrap());

        // 孤儿成员：指向不存在活动的 claim。
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO tool_activity_membership(source_path, activity_id)
                 VALUES('ghost.jsonl', 'act_v1_deadbeefdeadbeef')",
                [],
            )
            .unwrap();
        }
        assert_eq!(store.orphaned_activity_counts().unwrap(), (1, 1));

        let generation_before = store.active_generation().unwrap();
        let (removed_activities, removed_memberships) = store.purge_orphaned_activities().unwrap();
        // 成员行删除数含孤儿活动的 claim 级联：orphan.jsonl 对已删活动的
        // claim 随修剪一并清除，加预悬空的 ghost claim 共 2 行。
        assert_eq!((removed_activities, removed_memberships), (1, 2));
        assert_eq!(
            store.orphaned_activity_counts().unwrap(),
            (0, 0),
            "修剪后无孤儿行"
        );
        // 合法投影原样保留。
        let rows = activity_rows(&store);
        assert_eq!(rows.len(), 1, "合法活动行必须保留");
        assert_eq!(rows[0].0, live_msg.as_str());
        {
            let conn = store.conn.borrow();
            let live_claims: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM tool_activity_membership
                     WHERE source_path = 'live.jsonl'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(live_claims, 1, "合法 claim 必须保留");
        }
        assert_eq!(
            store.active_generation().unwrap(),
            generation_before + 1,
            "修剪走 rebuild 同款 generation 纪律：恰好推进一次"
        );

        // 幂等收敛：无孤儿时修剪是 no-op，不再推进 generation。
        let (again_activities, again_memberships) = store.purge_orphaned_activities().unwrap();
        assert_eq!((again_activities, again_memberships), (0, 0));
        assert_eq!(
            store.active_generation().unwrap(),
            generation_before + 1,
            "空跑修剪不得产生 generation churn"
        );
    }

    #[test]
    fn purge_leaves_catalog_fts_and_search_projections_unchanged() {
        // 修剪的不变量：除悬空投影行外，catalog、消息 FTS、session 元数据
        // 投影、合法 claim 逐行不变——修剪绝不能借机改写权威数据。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"invariant-session");
        let document = sid(IdKind::Document, b"invariant-document");
        let message = sid(IdKind::Message, b"invariant-message");
        let full = source_batch(
            "invariant.jsonl",
            vec![
                entity_entry(&session),
                typed_document_entry(&document),
                typed_message_entry(&message, "invariant searchable body"),
            ],
            vec![placement(&session, &document, &message, 0, false, None)],
            Vec::new(),
            true,
        );
        assert!(store.commit_source_batches_if_changed(&[full]).unwrap());
        assert!(store.query("invariant", 10).unwrap().len() == 1);

        // 孤儿活动（锚点悬空）+ 孤儿成员（claim 悬空）。
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO tool_activities(
                     activity_id, message_id, kind, actor, name, target, status
                 ) VALUES('act_v1_orphan', 'msg_v1_deadbeefdeadbeef',
                          'command', 'main', 'Ghost', NULL, 'success')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO tool_activity_membership(source_path, activity_id)
                 VALUES('ghost.jsonl', 'act_v1_ghostclaim')",
                [],
            )
            .unwrap();
        }
        let catalog_before = store.list(usize::MAX).unwrap();
        let session_fts_snapshot = |store: &SqliteStore| -> Vec<(String, String)> {
            let conn = store.conn.borrow();
            let mut stmt = conn
                .prepare("SELECT session_wire, text FROM session_fts ORDER BY session_wire")
                .unwrap();
            stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        };
        let session_fts_before = session_fts_snapshot(&store);
        assert!(!session_fts_before.is_empty(), "session 元数据投影必须有行");

        let (removed_activities, removed_memberships) = store.purge_orphaned_activities().unwrap();
        assert_eq!((removed_activities, removed_memberships), (1, 1));

        // catalog 逐行不变（权威数据）。
        assert_eq!(store.list(usize::MAX).unwrap(), catalog_before);
        // 消息 FTS 不变：既有查询仍命中且仅命中同一集合。
        assert_eq!(store.query("invariant", 10).unwrap().len(), 1);
        assert!(store.query("Ghost", 10).unwrap().is_empty());
        // session 元数据投影逐行不变。
        let session_fts_after = session_fts_snapshot(&store);
        assert_eq!(session_fts_after, session_fts_before);
        assert_eq!(
            store.orphaned_activity_counts().unwrap(),
            (0, 0),
            "修剪后无孤儿行"
        );
    }

    // ---- 活动 facet 查询（v12）----

    #[test]
    fn query_faceted_default_matches_plain_query() {
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"facet-msg");
        let source = tool_activity_batch(
            "facet.jsonl",
            &message,
            "Bash",
            "command",
            Some("ls"),
            "success",
        );
        store.commit_source_batches_if_changed(&[source]).unwrap();
        let plain = store.query("msg_v1", 10).unwrap();
        assert_eq!(
            plain.len(),
            1,
            "查询必须实际命中（FTS 正文是 wire id 文本）"
        );
        let faceted = store
            .query_faceted(
                SearchQuery {
                    text: "msg_v1",
                    filters: &SearchFilters::EMPTY,
                },
                10,
                &SearchFacets::default(),
            )
            .unwrap();
        assert_eq!(plain, faceted);
    }

    #[test]
    fn query_faceted_filters_by_tool_kind_and_name() {
        let store = SqliteStore::open_in_memory().unwrap();
        let msg_a = sid(IdKind::Message, b"facet-bash");
        let msg_b = sid(IdKind::Message, b"facet-read");
        let batch = |entries: Vec<(StableId, Vec<u8>, String)>, activities: Vec<SourceActivity>| {
            SourceBatch {
                source_path: "facet-two.jsonl".into(),
                entries,
                placements: Vec::new(),
                edges: Vec::new(),
                activities,
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: Some(1),
                fingerprint: Some("facet-two-fp".into()),
                provider_id: None,
                resume_claims: Vec::new(),
            }
        };
        let activity = |message_id: &StableId, kind: ToolActivityKind, name: &str| SourceActivity {
            message_id: message_id.clone(),
            activity: ToolActivity {
                kind,
                actor: ToolActivityActor::Main,
                name: name.into(),
                target: None,
                status: ToolActivityStatus::Success,
            },
        };
        let source = batch(
            vec![entity_entry(&msg_a), entity_entry(&msg_b)],
            vec![
                activity(&msg_a, ToolActivityKind::Command, "Bash"),
                activity(&msg_b, ToolActivityKind::File, "Read"),
            ],
        );
        store.commit_source_batches_if_changed(&[source]).unwrap();

        let by_kind = store
            .query_faceted(
                SearchQuery {
                    text: "msg_v1",
                    filters: &SearchFilters::EMPTY,
                },
                10,
                &SearchFacets {
                    tool_kind: Some("command".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(by_kind.len(), 1);
        assert_eq!(by_kind[0].id.as_str(), msg_a.as_str());

        let by_name = store
            .query_faceted(
                SearchQuery {
                    text: "msg_v1",
                    filters: &SearchFilters::EMPTY,
                },
                10,
                &SearchFacets {
                    tool_name: Some("Read".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(by_name.len(), 1);
        assert_eq!(by_name[0].id.as_str(), msg_b.as_str());

        let none = store
            .query_faceted(
                SearchQuery {
                    text: "msg_v1",
                    filters: &SearchFilters::EMPTY,
                },
                10,
                &SearchFacets {
                    tool_name: Some("Grep".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn query_faceted_filters_sidechains_on_indexed_columns() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"facet-session");
        let document = sid(IdKind::Document, b"facet-doc");
        let main_msg = sid(IdKind::Message, b"facet-main");
        let side_msg = sid(IdKind::Message, b"facet-side");
        let main_placement = MessagePlacement::new(
            session.clone(),
            document.clone(),
            main_msg.clone(),
            0,
            false,
            None,
        );
        let side_placement = MessagePlacement::new(
            session.clone(),
            document.clone(),
            side_msg.clone(),
            1,
            true,
            None,
        );
        let source = SourceBatch {
            source_path: "facet-sidechain.jsonl".into(),
            entries: vec![
                entity_entry(&main_msg),
                entity_entry(&side_msg),
                entity_entry(&session),
                entity_entry(&document),
            ],
            placements: vec![main_placement, side_placement],
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(1),
            fingerprint: Some("facet-sidechain-fp".into()),
            provider_id: None,
            resume_claims: Vec::new(),
        };
        store.commit_source_batches_if_changed(&[source]).unwrap();

        let main_only = store
            .query_faceted(
                SearchQuery {
                    text: "msg_v1",
                    filters: &SearchFilters::EMPTY,
                },
                10,
                &SearchFacets {
                    sidechain: SidechainFacet::MainOnly,
                    ..Default::default()
                },
            )
            .unwrap();
        let ids: Vec<&str> = main_only.iter().map(|hit| hit.id.as_str()).collect();
        assert!(ids.contains(&main_msg.as_str()));
        assert!(!ids.contains(&side_msg.as_str()));

        let subagent_only = store
            .query_faceted(
                SearchQuery {
                    text: "msg_v1",
                    filters: &SearchFilters::EMPTY,
                },
                10,
                &SearchFacets {
                    sidechain: SidechainFacet::SubagentOnly,
                    ..Default::default()
                },
            )
            .unwrap();
        let ids: Vec<&str> = subagent_only.iter().map(|hit| hit.id.as_str()).collect();
        assert_eq!(ids, vec![side_msg.as_str()]);
    }

    #[test]
    fn v7_catalog_migrates_to_v12_adding_activity_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v7.db");
        let p = path.to_string_lossy().into_owned();
        // 手工造一个 v7 库（最小关系 schema），打开后应迁到 v12 并补建活动表。
        {
            let conn = rusqlite::Connection::open(&p).unwrap();
            conn.execute_batch(
                "CREATE TABLE catalog (id TEXT PRIMARY KEY, payload BLOB NOT NULL);
                 CREATE VIRTUAL TABLE fts USING fts5(id UNINDEXED, text);
                 CREATE TABLE store_metadata (
                     singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                     active_generation INTEGER NOT NULL
                 );
                 INSERT INTO store_metadata(singleton, active_generation) VALUES(1, 3);
                 CREATE TABLE index_batches (
                     operation_id TEXT PRIMARY KEY,
                     base_generation INTEGER NOT NULL,
                     target_generation INTEGER NOT NULL,
                     state TEXT NOT NULL,
                     operation_digest TEXT NOT NULL,
                     upsert_ids_json TEXT NOT NULL,
                     delete_ids_json TEXT NOT NULL,
                     relation_upserts_json TEXT NOT NULL DEFAULT '[]',
                     relation_deletes_json TEXT NOT NULL DEFAULT '[]',
                     source_replacements_json TEXT NOT NULL DEFAULT '[]',
                     durable_point TEXT NOT NULL,
                     created_at_ms INTEGER NOT NULL,
                     committed_at_ms INTEGER,
                     error_code TEXT
                 );
                 CREATE TABLE fts_ids (wire_id TEXT PRIMARY KEY, id_json TEXT NOT NULL UNIQUE);
                 CREATE TABLE source_membership (
                     source_path TEXT NOT NULL,
                     message_id TEXT NOT NULL,
                     document_id TEXT,
                     PRIMARY KEY(source_path, message_id)
                 );
                 CREATE TABLE source_scans (
                     source_path TEXT PRIMARY KEY,
                     scanned_at_ms INTEGER NOT NULL,
                     len_bytes INTEGER,
                     fingerprint TEXT
                 );
                 PRAGMA user_version = 7;",
            )
            .unwrap();
        }
        let store = open_migration_fixture(&p);
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(store.active_generation().unwrap(), 3);
        // 活动表已建好且可写（直接 SQL 写入验证，不依赖完整 v7 关系提交路径）。
        let conn = store.conn.borrow();
        conn.execute(
            "INSERT INTO tool_activities(
                 activity_id, message_id, kind, actor, name, target, status
             ) VALUES('act_v1_test', 'msg_v1_migrated', 'command', 'main', 'Bash', 'ls', 'success')",
            [],
        )
        .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM tool_activities", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
        drop(conn);
    }

    // ---- token 用量事件（v15）：提交/去重/tombstone/聚合/孤儿 ----

    fn usage_obs(
        input: u64,
        output: u64,
        cache_read: u64,
        cache_write: u64,
        reasoning: u64,
        source: TokenSource,
    ) -> UsageObservation {
        UsageObservation {
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: cache_read,
            cache_write_tokens: cache_write,
            reasoning_tokens: reasoning,
            token_source: source,
        }
    }

    fn usage_batch(
        source_path: &str,
        session_id: &StableId,
        entries: Vec<(StableId, Vec<u8>, String)>,
        message_id: Option<&StableId>,
        usage: UsageObservation,
    ) -> SourceBatch {
        SourceBatch {
            source_path: source_path.into(),
            entries,
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: vec![SourceUsage {
                session_id: session_id.clone(),
                message_id: message_id.cloned(),
                usage,
            }],
            relation_complete: true,
            len_bytes: Some(1),
            fingerprint: Some(source_path.into()),
            provider_id: None,
            resume_claims: Vec::new(),
        }
    }

    struct UsageRow {
        session_id: String,
        message_id: Option<String>,
        input: i64,
        output: i64,
        cache_read: i64,
        cache_write: i64,
        reasoning: i64,
        token_source: String,
    }

    fn usage_rows(store: &SqliteStore) -> Vec<UsageRow> {
        store
            .conn
            .borrow()
            .prepare(
                "SELECT session_id, message_id, input_tokens, output_tokens,
                        cache_read_tokens, cache_write_tokens, reasoning_tokens, token_source
                 FROM usage_events ORDER BY usage_id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok(UsageRow {
                    session_id: row.get(0)?,
                    message_id: row.get(1)?,
                    input: row.get(2)?,
                    output: row.get(3)?,
                    cache_read: row.get(4)?,
                    cache_write: row.get(5)?,
                    reasoning: row.get(6)?,
                    token_source: row.get(7)?,
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn commit_usage_events_stores_anchored_and_session_scoped_rows() {
        // 消息锚定事件（Claude Code message.usage 形态）与 session 级事件
        // （Codex token_count 形态，message_id NULL）同表共存；五桶逐列。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"usage-session");
        let message = sid(IdKind::Message, b"usage-message");
        let entries = vec![entity_entry(&session), entity_entry(&message)];
        let anchored = usage_batch(
            "claude.jsonl",
            &session,
            entries.clone(),
            Some(&message),
            usage_obs(100, 50, 30, 20, 0, TokenSource::Observed),
        );
        let session_scoped = usage_batch(
            "codex.jsonl",
            &session,
            entries,
            None,
            usage_obs(8, 3, 2, 0, 1, TokenSource::Derived),
        );
        assert!(
            store
                .commit_source_batches_if_changed(&[anchored, session_scoped])
                .unwrap()
        );

        let rows = usage_rows(&store);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| {
            row.session_id == session.as_str()
                && row.message_id.as_deref() == Some(message.as_str())
                && row.input == 100
                && row.output == 50
                && row.cache_read == 30
                && row.cache_write == 20
                && row.reasoning == 0
                && row.token_source == "observed"
        }));
        assert!(rows.iter().any(|row| {
            row.session_id == session.as_str()
                && row.message_id.is_none()
                && row.input == 8
                && row.reasoning == 1
                && row.token_source == "derived"
        }));

        // 聚合：会话数 1，五桶求和，事件按来源分列。
        let totals = store.usage_totals().unwrap().expect("v15 投影必须存在");
        assert_eq!(totals.sessions, 1);
        assert_eq!(totals.input_tokens, 108);
        assert_eq!(totals.output_tokens, 53);
        assert_eq!(totals.cache_read_tokens, 32);
        assert_eq!(totals.cache_write_tokens, 20);
        assert_eq!(totals.reasoning_tokens, 1);
        assert_eq!(totals.observed_events, 1);
        assert_eq!(totals.derived_events, 1);
    }

    #[test]
    fn commit_usage_events_dedupes_across_sources() {
        // 同一事实跨两个源（副本文件）：内容寻址 id 去重为一行，claims 计数 2。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"usage-dedup-session");
        let entries = vec![entity_entry(&session)];
        let first = usage_batch(
            "copy-a.jsonl",
            &session,
            entries.clone(),
            None,
            usage_obs(8, 3, 2, 0, 1, TokenSource::Derived),
        );
        let second = usage_batch(
            "copy-b.jsonl",
            &session,
            entries,
            None,
            usage_obs(8, 3, 2, 0, 1, TokenSource::Derived),
        );
        assert!(
            store
                .commit_source_batches_if_changed(&[first, second])
                .unwrap()
        );
        assert_eq!(usage_rows(&store).len(), 1, "跨源副本必须去重为一行");
        let claims: i64 = store
            .conn
            .borrow()
            .query_row("SELECT COUNT(*) FROM usage_event_membership", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(claims, 2);
    }

    #[test]
    fn commit_usage_events_noop_resync_reports_unchanged() {
        // 同批重放：usage 投影参与 no-op 判定，返回 false 且不推进 generation。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"usage-noop-session");
        let entries = vec![entity_entry(&session)];
        let batch = usage_batch(
            "noop.jsonl",
            &session,
            entries,
            None,
            usage_obs(8, 3, 2, 0, 1, TokenSource::Derived),
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&batch))
                .unwrap()
        );
        let generation = store.active_generation().unwrap();
        assert!(
            !store.commit_source_batches_if_changed(&[batch]).unwrap(),
            "字节未变的源必须 no-op"
        );
        assert_eq!(store.active_generation().unwrap(), generation);
        assert_eq!(usage_rows(&store).len(), 1);
    }

    #[test]
    fn commit_usage_events_tombstones_when_source_rescan_drops_them() {
        // 完整重扫不再观察该用量事件 → claim 移除 → 无其它 claimer → 行删除。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"usage-tomb-session");
        let entries = vec![entity_entry(&session)];
        let with_usage = usage_batch(
            "tomb.jsonl",
            &session,
            entries.clone(),
            None,
            usage_obs(8, 3, 2, 0, 1, TokenSource::Derived),
        );
        assert!(
            store
                .commit_source_batches_if_changed(&[with_usage])
                .unwrap()
        );
        assert_eq!(usage_rows(&store).len(), 1);

        let without_usage = SourceBatch {
            source_path: "tomb.jsonl".into(),
            entries,
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: Some(2),
            fingerprint: Some("tomb.jsonl-v2".into()),
            provider_id: None,
            resume_claims: Vec::new(),
        };
        assert!(
            store
                .commit_source_batches_if_changed(&[without_usage])
                .unwrap()
        );
        assert!(usage_rows(&store).is_empty(), "无 claimer 的事件行必须退役");
        let totals = store.usage_totals().unwrap().expect("投影仍在");
        assert_eq!(totals.sessions, 0, "退役后回到零事实（未知 ≠ 零行）");
    }

    #[test]
    fn commit_usage_events_rejects_wrong_kind_anchors() {
        // 会话锚点必须是 Session 种类、消息锚点必须是 Message 种类——fail-closed。
        let store = SqliteStore::open_in_memory().unwrap();
        let message = sid(IdKind::Message, b"usage-bad-anchor");
        let batch = usage_batch(
            "bad-anchor.jsonl",
            &message, // 错误：不是 Session 种类
            vec![entity_entry(&message)],
            None,
            usage_obs(8, 3, 2, 0, 1, TokenSource::Derived),
        );
        let err = store
            .commit_source_batches_if_changed(&[batch])
            .unwrap_err();
        assert!(
            matches!(&err, PortError::Backend(message) if message.contains("wrong kind")),
            "got {err:?}"
        );

        let session = sid(IdKind::Session, b"usage-bad-message-anchor");
        let batch = usage_batch(
            "bad-anchor-2.jsonl",
            &session,
            vec![entity_entry(&session)],
            Some(&session), // 错误：消息锚点是 Session 种类
            usage_obs(8, 3, 2, 0, 1, TokenSource::Derived),
        );
        let err = store
            .commit_source_batches_if_changed(&[batch])
            .unwrap_err();
        assert!(
            matches!(&err, PortError::Backend(message) if message.contains("wrong kind")),
            "got {err:?}"
        );
    }

    #[test]
    fn orphaned_usage_counts_and_purge_remove_drift_rows() {
        // 会话退役后的漂移残余：孤儿事件行（无 catalog 会话）+ 悬空 claim；
        // `index purge-activities` 同事务清理，绝不触碰仍锚定的合法行。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"usage-orphan-session");
        let entries = vec![entity_entry(&session)];
        let batch = usage_batch(
            "orphan.jsonl",
            &session,
            entries,
            None,
            usage_obs(8, 3, 2, 0, 1, TokenSource::Derived),
        );
        assert!(store.commit_source_batches_if_changed(&[batch]).unwrap());
        {
            let conn = store.conn.borrow();
            conn.execute(
                "INSERT INTO usage_events(
                     usage_id, session_id, input_tokens, output_tokens,
                     cache_read_tokens, cache_write_tokens, reasoning_tokens, token_source
                 ) VALUES('use_v1_orphan', 'ses_v1_deadbeefdeadbeef', 1, 1, 0, 0, 0, 'derived')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO usage_event_membership(source_path, usage_id)
                 VALUES('ghost.jsonl', 'use_v1_ghostclaim')",
                [],
            )
            .unwrap();
        }
        assert_eq!(store.orphaned_usage_counts().unwrap(), (1, 1));

        let (removed_activities, removed_memberships) = store.purge_orphaned_activities().unwrap();
        assert_eq!((removed_activities, removed_memberships), (0, 0));
        assert_eq!(
            store.orphaned_usage_counts().unwrap(),
            (0, 0),
            "修剪后 usage 孤儿行清零"
        );
        // 合法行不受影响。
        assert_eq!(usage_rows(&store).len(), 1);
        let totals = store.usage_totals().unwrap().unwrap();
        assert_eq!(totals.sessions, 1);
        assert_eq!(totals.input_tokens, 8);
    }

    #[test]
    fn unchanged_batch_with_container_entities_takes_the_fast_current_path() {
        // 回归：`sources_are_current` 曾对**全部** entries 比较 fts 正文，而
        // session/document 容器实体按设计没有 fts 行（见 batch_upsert_fts_in_tx）
        // → 每个真实 ingest 批次（必含 session + document）都被判 not-current，
        // O(batch) 快路径永远不生效，未变源的重同步退化为加载全库
        // membership/placements/activities/usage。内容级 no-op 仍由
        // source_batches_are_current 兜住，缺陷不改变结果、只静默改变成本，
        // 因此必须直接锚定快路径本身。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"fast-ses");
        let document = sid(IdKind::Document, b"fast-doc");
        let message = sid(IdKind::Message, b"fast-msg");
        let placements = vec![placement(
            &session,
            &document,
            &message,
            0,
            false,
            Some((0, 4)),
        )];
        let batch = source_batch(
            "fast.jsonl",
            vec![
                entity_entry(&session),
                typed_document_entry(&document),
                typed_message_entry(&message, "fast path body"),
            ],
            placements.clone(),
            Vec::new(),
            true,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&batch))
                .unwrap()
        );

        // Reparse the original source bytes, not the derived catalog aliases.
        // Exact original evidence must also be a fast-path no-op.
        let rescan_entries: Vec<(StableId, Vec<u8>, String)> = batch.entries.to_vec();
        let rescan = source_batch(
            "fast.jsonl",
            rescan_entries,
            placements.clone(),
            Vec::new(),
            true,
        );
        assert!(
            store.sources_are_current(&[&rescan]).unwrap(),
            "未变化批次必须被快路径识别"
        );
        assert!(
            !store
                .commit_source_batches_if_changed(std::slice::from_ref(&rescan))
                .unwrap()
        );

        // 判定不是恒 true：消息正文变化必须离开快路径。
        let mut edited = rescan.clone();
        for entry in &mut edited.entries {
            if entry.0.kind() == IdKind::Message {
                entry.2 = "fast path body edited".into();
            }
        }
        assert!(!store.sources_are_current(&[&edited]).unwrap());
    }

    #[test]
    fn non_json_catalog_payload_does_not_error_json_projected_reads() {
        // 回归：catalog 里合法存在非 JSON payload——生产命令
        // `index <fact> <text>`（cli `index_one` → `commit_batch`）把裸文本
        // 直接写进 catalog，切片期的旧行同样如此。三处读路径当时缺
        // `json_valid` 门（message_facts_for 的 role、query_filtered /
        // query_faceted 的 provider+时间谓词、latest_activity_ymd 的
        // timestamp），SQLite 的 json_extract 对非 JSON 输入**报错**而非返回
        // NULL，于是"库里存在一条裸文本消息"就让每一次带时间过滤的搜索整体
        // 失败为 Backend("malformed JSON")。修复后非 JSON payload 一律折叠成
        // NULL（诚实排除），合法 JSON 行照常参与。
        let store = SqliteStore::open_in_memory().unwrap();
        let session = sid(IdKind::Session, b"nonjson-ses");
        let document = sid(IdKind::Document, b"nonjson-doc");
        let bare_member = sid(IdKind::Message, b"nonjson-msg");
        let json_member = sid(IdKind::Message, b"nonjson-json-msg");
        let batch = source_batch(
            "nonjson.jsonl",
            vec![
                entity_entry(&session),
                typed_document_entry(&document),
                (
                    bare_member.clone(),
                    b"bare legacy text".to_vec(),
                    "bareword".into(),
                ),
                typed_message_entry(&json_member, "jsonword"),
            ],
            vec![
                placement(&session, &document, &bare_member, 0, false, Some((0, 4))),
                placement(&session, &document, &json_member, 1, false, Some((0, 4))),
            ],
            Vec::new(),
            true,
        );
        assert!(
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&batch))
                .unwrap()
        );
        // `index_one` 形状：裸文本消息，无 placement，但仍进 fts。
        let lonely = sid(IdKind::Message, b"nonjson-bare");
        store
            .commit_batch(&[(
                lonely.clone(),
                b"lonely bare body".to_vec(),
                "lonelyword".into(),
            )])
            .unwrap();

        // 会话最近活动：只认可解析的 timestamp，裸文本行诚实缺席而不报错。
        let latest = store
            .latest_activity_ymd_for_sessions(std::slice::from_ref(&session))
            .unwrap();
        assert_eq!(
            latest.get(session.as_str()).map(String::as_str),
            Some("2026-07-28")
        );

        // role 事实：非 JSON payload → 'unknown'，不报错。
        let facts = store
            .message_facts_for(&[bare_member.clone(), json_member.clone()])
            .unwrap();
        let bare_role = facts
            .iter()
            .find(|(id, _, _)| id == bare_member.as_str())
            .map(|(_, role, _)| role.as_str());
        assert_eq!(bare_role, Some("unknown"));
        let json_role = facts
            .iter()
            .find(|(id, _, _)| id == json_member.as_str())
            .map(|(_, role, _)| role.as_str());
        assert_eq!(json_role, Some("user"));

        // 时间过滤：裸文本行既不报错也不被谎报为命中；合法 JSON 行照常命中。
        let filters = SearchFilters {
            since: Some(agent_session_grep_ports::SearchInstant::from_unix_millis(0)),
            ..SearchFilters::default()
        };
        for (text, expected) in [
            ("bareword", None),
            ("lonelyword", None),
            ("jsonword", Some(json_member.as_str().to_string())),
        ] {
            let hits = store
                .query_filtered(
                    SearchQuery {
                        text,
                        filters: &filters,
                    },
                    10,
                )
                .unwrap();
            let ids: Vec<String> = hits.iter().map(|hit| hit.id.as_str().to_string()).collect();
            match expected {
                Some(id) => assert_eq!(ids, vec![id], "query {text:?}"),
                None => assert!(ids.is_empty(), "query {text:?} -> {ids:?}"),
            }
        }
        // facet 路径同一门。
        let faceted = store
            .query_faceted(
                SearchQuery {
                    text: "bareword",
                    filters: &filters,
                },
                10,
                &SearchFacets {
                    sidechain: SidechainFacet::MainOnly,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(faceted.is_empty());
    }

    /// 生成本测试用的 repo-slug 解析器（`Z:/projects/app` → 三段 slug）。
    fn app_repo_resolver() -> Box<dyn RepoSlugResolver> {
        Box::new(MapRepoSlugResolver {
            map: std::collections::HashMap::from([(
                "Z:/projects/app".into(),
                Some("github.com/o/app".into()),
            )]),
            calls: RefCell::new(0),
        })
    }

    #[test]
    fn write_open_self_heal_keeps_the_repo_identity_projection() {
        // 回归：`open_for_write` 在返回之前就跑投影版本自愈
        // （ensure_index_projection_current → 整库重投影），而重投影当时无条件
        // `DELETE FROM session_repo_slugs` + 按注入的解析器重派生。解析器只能
        // 在 open 返回之后注入（CLI 正是那么做的），因此自愈时它仍是 Noop：
        // repo 投影被整表删空，同时 index_projection_version 被盖成当前——
        // `search --repo` 从此恒 0 命中、`status` 恒无仓库，且再没有任何信号
        // 会报告这次丢失。git 不在 PATH 时即使先注入真实解析器也一样删空。
        // 修复：repo 身份投影不可从 catalog 重建，自愈路径保留既有行
        // （RepoIdentityRebuild::Preserve）；重派生只属于显式 `index rebuild`。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repo-heal.db");
        let p = path.to_string_lossy().into_owned();
        let session = sid(IdKind::Session, b"heal-repo-ses");
        let document = sid(IdKind::Document, b"heal-repo-doc");
        let message = sid(IdKind::Message, b"heal-repo-msg");
        let orphan_session = sid(IdKind::Session, b"heal-repo-orphan");
        {
            let store = SqliteStore::open_for_write(&p).unwrap();
            store.set_repo_slug_resolver(app_repo_resolver());
            let batch = SourceBatch {
                source_path: "heal-repo.jsonl".into(),
                entries: vec![
                    entity_entry(&session),
                    typed_document_entry(&document),
                    typed_message_entry(&message, "今天把配置备份到了新目录"),
                ],
                placements: vec![placement(
                    &session,
                    &document,
                    &message,
                    0,
                    false,
                    Some((0, 4)),
                )],
                edges: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                relation_complete: true,
                len_bytes: None,
                fingerprint: None,
                provider_id: None,
                resume_claims: vec![source_repo_claim(&repo_claim(
                    &session,
                    "heal-native",
                    Some("Z:/projects/app"),
                    true,
                ))],
            };
            assert!(
                store
                    .commit_source_batches_if_changed(std::slice::from_ref(&batch))
                    .unwrap()
            );
            assert_eq!(
                repo_slug_rows(&store, session.as_str()),
                vec!["github.com/o/app".to_string()]
            );
            {
                let conn = store.conn.borrow();
                conn.execute(
                    "INSERT INTO session_repo_slugs(session_wire, repo_slug)
                     VALUES(?1, ?2)",
                    rusqlite::params![orphan_session.as_str(), "github.com/o/orphan"],
                )
                .unwrap();
            }
            downgrade_projection_to_legacy_bigrams(&store);
        }
        // 重开写路径（CLI 顺序：先 open，解析器随后注入）：自愈跑在注入之前，
        // repo 投影必须原样留存。
        let store = SqliteStore::open_for_write(&p).unwrap();
        assert!(store.index_projection_is_current().unwrap());
        assert_eq!(
            repo_slug_rows(&store, session.as_str()),
            vec!["github.com/o/app".to_string()],
            "自愈重投影后 repo 身份投影必须仍在"
        );
        assert!(
            repo_slug_rows(&store, orphan_session.as_str()).is_empty(),
            "自愈必须清理 catalog 已不存在的 repo slug 孤儿"
        );
        // 词元流也确实收敛了（自愈本身仍然生效，不是被整体跳过）。
        assert_eq!(store.query("配置备份", 10).unwrap().len(), 1);
        assert_eq!(store.query("配", 10).unwrap().len(), 1);
        // 显式 rebuild 仍然重派生（解析器缺席 ⇒ 诚实降级为无行），语义未被削弱。
        store.rebuild_index().unwrap();
        assert!(repo_slug_rows(&store, session.as_str()).is_empty());
        store.set_repo_slug_resolver(app_repo_resolver());
        store.rebuild_index().unwrap();
        assert_eq!(
            repo_slug_rows(&store, session.as_str()),
            vec!["github.com/o/app".to_string()]
        );
    }
}

//! Handoff Pack v1 generator (deterministic default, #4).
//!
//! Assembles a `HandoffPack` from search hits, authoritative source placements,
//! and catalog data. The default generator is fully deterministic — no LLM
//! calls, no wall-clock reads. Evidence (original text spans) and inference
//! (derived summaries) are strictly separated. Pack is preview-only: `asg`
//! emits the pack and suggested commands, never injects into another agent.
//!
//! Determinism contract (PRD Q50): the same catalog generation + query +
//! budget always yields a byte-identical pack. `created_at` is derived from the
//! pinned catalog generation (fixed base epoch + generation seconds), never
//! read from the clock.
//!
//! Redaction (ADR-0009): the pack is cross-boundary output, so evidence text is
//! redacted with the shared [`agent_session_grep_ports::redact`] engine by
//! default and `redaction.redacted_count`/`status` reflect what actually ran.
//!
//! Implementation reference: the pack structure is defined in
//! `schemas/handoff/v1/pack.schema.json` and the Rust contract types in
//! `agent_session_grep_ports::handoff`.

use agent_session_grep_domain::StableId;
use agent_session_grep_ports::handoff::{
    ConfidenceLevel, EvidenceEntry, GenerationMode, HandoffBudget, HandoffFilters, HandoffPack,
    HandoffQuery, HandoffTarget, MainlineEntry, MatchedSession, PackConfidence, RedactionMode,
    RedactionState, RedactionStatus, RetrievalMode, SessionConfidence, SourceLocator, TimeWindow,
    TruncationReason, TruncationStatus,
};
use agent_session_grep_ports::{ContextGraphStore, PortError, PortResult, SearchHit};

/// 一条命中对应的权威 source placement（由调用方经 [`ContextGraphStore`] 批量解析）。
///
/// 与 `hits` 保持同序（一一对应）。`source_document_id` 为权威文档身份
/// （`doc_v1_*`）；`byte_start`/`byte_end` 为 provider 报告的原文证据区间。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLocationInfo {
    pub message_id: String,
    pub source_document_id: Option<StableId>,
    pub byte_start: Option<u64>,
    pub byte_end: Option<u64>,
}

/// 批量解析每条命中的权威 source placement（无 N+1，走 [`ContextGraphStore`] 的
/// 批量方法）。无 placement 的命中对应 `source_document_id: None`——调用方/构建器
/// 不得臆造文档身份。
pub fn resolve_source_locations(
    graph: &dyn ContextGraphStore,
    hits: &[SearchHit],
) -> PortResult<Vec<SourceLocationInfo>> {
    let ids: Vec<StableId> = hits.iter().map(|hit| hit.id.clone()).collect();
    let resolved = graph.source_placements_of(&ids)?;
    Ok(resolved
        .into_iter()
        .map(|(id, placement)| SourceLocationInfo {
            message_id: id.as_str().to_string(),
            source_document_id: placement.as_ref().map(|p| p.source_document_id.clone()),
            byte_start: placement.as_ref().and_then(|p| p.byte_start),
            byte_end: placement.as_ref().and_then(|p| p.byte_end),
        })
        .collect())
}

/// 生成参数：搜索结果 + 权威 source placements + catalog generation + 预算。
pub struct HandoffInput<'a> {
    pub query_terms: &'a [String],
    pub retrieval_mode: RetrievalMode,
    pub filters: HandoffFilters,
    pub hits: &'a [SearchHit],
    /// 与 `hits` 同序的权威 source placements（见 [`resolve_source_locations`]）。
    pub source_locations: &'a [SourceLocationInfo],
    /// Pre-resolved tool activities for the hit messages (empty when none).
    /// Caller batches via the store; the pack never queries storage itself.
    pub tool_activities: &'a [serde_json::Value],
    /// Optional per-message role/sidechain facts, keyed by message wire id.
    /// Missing entries render as role=`unknown`, is_sidechain=`false`.
    pub message_facts: &'a [MessageFact],
    pub catalog_generation: u64,
    pub max_tokens: u64,
    pub max_bytes: u64,
    pub max_evidence: usize,
    pub target: Option<HandoffTarget>,
}

/// Authoritative per-message facts used by the mainline projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageFact {
    pub message_id: String,
    pub role: String,
    pub is_sidechain: bool,
}

/// 确定性 pack 装配时间基点：2026-08-16T00:00:00Z。`created_at =
/// PACK_TIME_BASE_UNIX + catalog_generation`，保证同 generation/query/budget 下
/// 字节可复现（PRD Q50），且随 generation 单调推进。
const PACK_TIME_BASE_UNIX: u64 = 1_786_838_400;

/// 生成一个 deterministic handoff pack。
///
/// - `created_at` 由 catalog generation 确定性派生（不读时钟）；
/// - `pack_id` 由 generation + query + filters + budget 派生（含预算，审计 P1）；
/// - 预算按 `max_evidence`/`max_tokens`/`max_bytes` 三层裁剪，截断如实记录
///   reason 与被丢弃的 locator；
/// - 证据文本默认经共享脱敏引擎（ADR-0009），`redaction` 如实反映；
/// - `source_document_id`/span 来自调用方传入的权威 placement，无 placement 的
///   命中不进证据（never fabricated）。
pub fn generate_deterministic(input: HandoffInput<'_>) -> PortResult<HandoffPack> {
    let created_at = created_at_from_generation(input.catalog_generation);
    let pack_id = derive_pack_id(&input);
    let matched_sessions = build_matched_sessions(input.hits);

    // 候选证据：redact 原文 → 附权威 source locator。无 source document 的命中
    // 直接排除（设计 D2：cursor-less entries excluded，绝不臆造文档身份）。
    let facts: std::collections::BTreeMap<&str, &MessageFact> = input
        .message_facts
        .iter()
        .map(|f| (f.message_id.as_str(), f))
        .collect();
    let mut candidates: Vec<Candidate> = Vec::new();
    for (hit, loc) in input.hits.iter().zip(input.source_locations.iter()) {
        let Some(source_document_id) = loc.source_document_id.clone() else {
            continue;
        };
        let text = hit.text.as_deref().unwrap_or("");
        if text.is_empty() {
            continue;
        }
        let (redacted_text, redacted_count) = agent_session_grep_ports::redact::redact_text(text);
        let tokens = estimate_tokens(&redacted_text);
        // 会话 wire 必须可解析为合法 Session 身份；证据不携带会话时用 None。
        let session_id = hit
            .session_id
            .as_ref()
            .and_then(|wire| StableId::from_wire(wire))
            .filter(|id| id.kind() == agent_session_grep_domain::IdKind::Session)
            .map(|_| hit.session_id.clone().unwrap_or_default());
        let fact = facts.get(hit.id.as_str());
        candidates.push(Candidate {
            message_id: hit.id.as_str().to_string(),
            source_document_id: source_document_id.as_str().to_string(),
            session_id,
            span_start: loc.byte_start,
            span_end: loc.byte_end,
            text: redacted_text,
            tokens,
            redacted: redacted_count > 0,
            role: fact
                .map(|f| f.role.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            is_sidechain: fact.map(|f| f.is_sidechain).unwrap_or(false),
        });
    }

    let mut truncation = TruncationStatus {
        truncated: false,
        reason: TruncationReason::None,
        dropped_count: 0,
        dropped_locators: Vec::new(),
    };

    // 第一层：max_evidence 条数上限。
    if candidates.len() > input.max_evidence {
        let extra: Vec<Candidate> = candidates.split_off(input.max_evidence);
        truncation.truncated = true;
        truncation.reason = TruncationReason::MaxEvidence;
        for candidate in &extra {
            truncation.dropped_locators.push(candidate.locator());
        }
    }

    // 第二层：max_tokens 预算（真实 token 估计，非 raw bytes）。一旦累计超过，
    // 该条及之后全部丢弃（保序前缀）。
    let mut kept: Vec<Candidate> = Vec::new();
    let mut used_tokens: u64 = 0;
    for (idx, candidate) in candidates.iter().enumerate() {
        if used_tokens.saturating_add(candidate.tokens) > input.max_tokens {
            truncation.truncated = true;
            truncation.reason = TruncationReason::BudgetExceeded;
            for rest in &candidates[idx..] {
                truncation.dropped_locators.push(rest.locator());
            }
            break;
        }
        used_tokens = used_tokens.saturating_add(candidate.tokens);
        kept.push(candidate.clone());
    }
    truncation.dropped_count = truncation.dropped_locators.len() as u64;

    // 证据与 mainline（mainline 是 evidence 的确定性投影，保持对齐）。
    let evidence: Vec<EvidenceEntry> = kept.iter().map(Candidate::evidence_entry).collect();
    let mainline: Vec<MainlineEntry> = kept
        .iter()
        .enumerate()
        .map(|(ord, candidate)| MainlineEntry {
            message_id: candidate.message_id.clone(),
            session_id: candidate.session_id.clone(),
            role: candidate.role.clone(),
            ordinal: ord as u64,
            text_preview: Some(candidate.text.chars().take(256).collect()),
            is_sidechain: candidate.is_sidechain,
        })
        .collect();

    let mut pack = HandoffPack {
        schema_version: HandoffPack::SCHEMA_VERSION.to_string(),
        pack_id,
        catalog_generation: input.catalog_generation,
        generation_mode: GenerationMode::Deterministic,
        query: HandoffQuery {
            terms: input.query_terms.to_vec(),
            retrieval_mode: input.retrieval_mode,
            filters: Some(input.filters.clone()),
        },
        created_at,
        matched_sessions,
        mainline,
        evidence,
        inference: Vec::new(), // deterministic default: no LLM inference
        target: input.target.clone(),
        time_window: time_window_from_filters(&input.filters),
        provenance: None, // 搜索型 pack 无单一会话/提供商，honest：不臆造
        budget: HandoffBudget {
            max_tokens: input.max_tokens,
            max_bytes: input.max_bytes,
            used_tokens,
            used_bytes: 0,
            max_evidence: Some(input.max_evidence as u64),
            context_lines: None,
        },
        truncation,
        redaction: RedactionStatus {
            mode: RedactionMode::Default,
            status: RedactionState::None,
            ruleset_version: agent_session_grep_ports::redact::RULESET_VERSION.to_string(),
            redacted_count: 0,
            audit_id: None,
        },
        confidence: PackConfidence {
            overall: ConfidenceLevel::Low,
            per_session: Vec::new(),
        },
        // Project caller-supplied activities (already redacted at source when
        // needed). Empty when the catalog has no tool_activities for these hits.
        tool_activity: input.tool_activities.to_vec(),
        source_locators: Vec::new(),
    };

    // 脱敏标记与保留证据对齐（byte gate 逐条 pop 时同步维护）。
    let mut redacted_flags: Vec<bool> = kept.iter().map(|c| c.redacted).collect();

    // 第三层：max_bytes 字节门（精确序列化收敛）。逐条丢弃队尾证据+mainline，
    // 直至内容序列化长度 ≤ max_bytes。内容测量排除截断/反向追踪诊断元数据
    // （dropped_locators/source_locators）——`max_bytes` 是 pack 内容预算，
    // 诊断元数据不占用户预算；`used_bytes` 报告内容字节（与 max_bytes 可比）。
    loop {
        // Measure the final metadata, not the pre-trim confidence/redaction.
        // Even a few bytes of status growth must not escape the hard bound.
        let overall = if pack.evidence.is_empty() {
            ConfidenceLevel::Low
        } else if pack.matched_sessions.len() > 1 {
            ConfidenceLevel::Medium
        } else {
            ConfidenceLevel::High
        };
        pack.confidence.overall = overall;
        pack.confidence.per_session = pack
            .matched_sessions
            .iter()
            .map(|session| SessionConfidence {
                session_id: session.session_id.clone(),
                confidence: if pack.evidence.iter().any(|evidence| {
                    evidence.session_id.as_deref() == Some(session.session_id.as_str())
                }) {
                    overall
                } else {
                    ConfidenceLevel::Low
                },
            })
            .collect();
        let redacted_count = redacted_flags.iter().filter(|&&redacted| redacted).count() as u64;
        pack.redaction.redacted_count = redacted_count;
        pack.redaction.status = if redacted_count > 0 {
            RedactionState::Applied
        } else {
            RedactionState::None
        };
        pack.budget.used_tokens = pack
            .evidence
            .iter()
            .map(|entry| estimate_tokens(&entry.text))
            .sum();
        let bytes = content_bytes(&pack);
        pack.budget.used_bytes = bytes;
        if bytes <= input.max_bytes {
            break;
        }
        if let Some(dropped) = pack.evidence.pop() {
            pack.truncation.dropped_locators.push(SourceLocator {
                source_document_id: dropped.source_document_id,
                cursor: Some(dropped.message_id),
            });
            pack.mainline.pop();
            redacted_flags.pop();
        } else if !pack.tool_activity.is_empty() {
            pack.tool_activity.pop();
        } else if !pack.matched_sessions.is_empty() {
            // No retained evidence can reference the removed session here.
            pack.matched_sessions.pop();
        } else {
            return Err(PortError::InvalidRequest(
                "handoff max_bytes is too small for mandatory content".into(),
            ));
        }
        pack.truncation.truncated = true;
        pack.truncation.reason = TruncationReason::MaxBytes;
        pack.truncation.dropped_count += 1;
    }

    // Source locators：保留证据的去重反向追踪引用（文档 + 精确消息 cursor）。
    let mut source_locators: Vec<SourceLocator> = Vec::new();
    for entry in &pack.evidence {
        let locator = SourceLocator {
            source_document_id: entry.source_document_id.clone(),
            cursor: Some(entry.message_id.clone()),
        };
        if !source_locators.contains(&locator) {
            source_locators.push(locator);
        }
    }
    pack.source_locators = source_locators;

    // 最终 used_bytes：与内容口径自洽（content_bytes 稳定，不自指）。
    pack.budget.used_bytes = content_bytes(&pack);

    Ok(pack)
}

/// 一条已通过脱敏与定位的候选证据。id 均为 wire 字符串（pack JSON 权威形态，
/// 与 schema 的 `^msg_v1_`/`^doc_v1_`/`^ses_v1_` pattern 对齐）。
#[derive(Debug, Clone)]
struct Candidate {
    message_id: String,
    source_document_id: String,
    session_id: Option<String>,
    span_start: Option<u64>,
    span_end: Option<u64>,
    text: String,
    tokens: u64,
    /// 该条原文在默认脱敏模式下是否真的发生了替换（ADR-0009）。
    redacted: bool,
    /// Canonical role from the hit payload when available; "unknown" otherwise.
    role: String,
    /// True when the hit message has at least one sidechain placement.
    is_sidechain: bool,
}

impl Candidate {
    fn locator(&self) -> SourceLocator {
        SourceLocator {
            source_document_id: self.source_document_id.clone(),
            cursor: Some(self.message_id.clone()),
        }
    }

    fn evidence_entry(&self) -> EvidenceEntry {
        EvidenceEntry {
            message_id: self.message_id.clone(),
            source_document_id: self.source_document_id.clone(),
            session_id: self.session_id.clone(),
            span_start: self.span_start,
            span_end: self.span_end,
            text: self.text.clone(),
        }
    }
}

/// matched sessions：按命中归属会话去重（ADR-0008 wire id），保留首次出现顺序。
/// 会话 id 必须可解析为 Session 实体，否则该命中不计入（never fabricated）。
fn build_matched_sessions(hits: &[SearchHit]) -> Vec<MatchedSession> {
    let mut seen: Vec<String> = Vec::new();
    let mut sessions: Vec<MatchedSession> = Vec::new();
    for hit in hits {
        let Some(session_wire) = hit.session_id.as_deref() else {
            continue;
        };
        // 校验 wire 可解析为合法 Session 身份；只用原始 wire 字符串（不臆造）。
        let Some(session) = StableId::from_wire(session_wire) else {
            continue;
        };
        if session.kind() != agent_session_grep_domain::IdKind::Session {
            continue;
        }
        match seen.iter().position(|s| s == session_wire) {
            Some(pos) => sessions[pos].occurrences += 1,
            None => {
                seen.push(session_wire.to_string());
                sessions.push(MatchedSession {
                    session_id: session_wire.to_string(),
                    provider_id: None,
                    title: None,
                    relevance_score: hit.score as f64,
                    occurrences: 1,
                });
            }
        }
    }
    sessions
}

/// time_window 投影：filters 的 since/until（半开区间 `[since, until)`）。
fn time_window_from_filters(filters: &HandoffFilters) -> Option<TimeWindow> {
    if filters.since.is_none() && filters.until.is_none() {
        None
    } else {
        Some(TimeWindow {
            since: filters.since.clone(),
            until: filters.until.clone(),
        })
    }
}

/// 确定性 `created_at`：固定基点 + catalog generation 秒。
fn created_at_from_generation(generation: u64) -> String {
    format_utc_iso8601(PACK_TIME_BASE_UNIX + generation)
}

/// 真实 token 估计：CJK 字 ≈ 1 token，其余字节 ≈ 1 token / 4 字节。确定性、
/// 单调，用于预算裁剪（与 raw bytes 区分）。
fn estimate_tokens(text: &str) -> u64 {
    let mut cjk = 0u64;
    let mut other_bytes = 0u64;
    for c in text.chars() {
        if crate::cjk::is_han(c) {
            cjk += 1;
        } else {
            other_bytes += c.len_utf8() as u64;
        }
    }
    cjk + other_bytes.div_ceil(4)
}

/// 内容字节测量：pack 的确定性内容序列化长度（排除截断/反向追踪诊断元数据，
/// 且 `used_bytes` 自身不计入——定义稳定、不自指）。
///
/// `max_bytes` 预算作用于内容（evidence + mainline + 核心字段）；诊断元数据
/// （dropped_locators/source_locators）与 `used_bytes` 自身的位数不计入，
/// 因此 `used_bytes == content_bytes(pack)` 恒成立（同一口径）。
fn content_bytes(pack: &HandoffPack) -> u64 {
    let mut probe = pack.clone();
    probe.budget.used_bytes = 0;
    probe.truncation.dropped_locators.clear();
    probe.source_locators.clear();
    serde_json::to_vec(&probe)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(u64::MAX)
}

/// Derive a deterministic pack_id from generation + query + filters + budget.
fn derive_pack_id(input: &HandoffInput<'_>) -> String {
    // Labeled JSON fields escape delimiters and distinguish absent/empty
    // values. Revision 2 invalidates old ambiguous cache identities without
    // changing the public pack schema or wire-prefix contract.
    let identity = serde_json::json!({
        "catalog_generation": input.catalog_generation,
        "retrieval_mode": input.retrieval_mode.as_str(),
        "query_terms": input.query_terms,
        "filters": input.filters,
        "max_tokens": input.max_tokens,
        "max_bytes": input.max_bytes,
        "max_evidence": input.max_evidence,
        "target": input.target,
    });
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"handoff-pack/identity-v2\0");
    hasher.update(identity.to_string().as_bytes());
    let hash = hasher.finalize();
    format!("pack_v1_{}", &hash.to_hex()[..16])
}

/// Format Unix seconds as `YYYY-MM-DDTHH:MM:SSZ` in UTC using the
/// civil-from-days algorithm (Howard Hinnant, public domain).
fn format_utc_iso8601(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hour, minute, second) = (rem / 3_600, (rem % 3_600) / 60, rem % 60);

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_session_grep_domain::{IdKind, Stability};
    use agent_session_grep_ports::handoff::{ConfidenceLevel, HandoffFilters};
    use agent_session_grep_ports::redact::RULESET_VERSION;

    #[test]
    fn utc_iso8601_known_epochs() {
        assert_eq!(format_utc_iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_utc_iso8601(86_400), "1970-01-02T00:00:00Z");
        // 2026-08-16T00:00:00Z = 1786838400 (leap years included)
        assert_eq!(format_utc_iso8601(1_786_838_400), "2026-08-16T00:00:00Z");
        // Leap day: 2024-02-29T12:34:56Z
        assert_eq!(format_utc_iso8601(1_709_210_096), "2024-02-29T12:34:56Z");
    }

    fn hit(id: &str, score: f32, text: &str) -> SearchHit {
        SearchHit {
            id: StableId::from_wire(id).unwrap(),
            score,
            session_id: Some("ses_v1_abc".to_string()),
            text: Some(text.to_string()),
            why_matched: Vec::new(),
            suggested_next_commands: Vec::new(),
            occurrences: 1,
            resume_available: false,
        }
    }

    /// 为每条命中生成与其实体 id 相同的权威 source placement。
    fn locations(hits: &[SearchHit]) -> Vec<SourceLocationInfo> {
        hits.iter()
            .map(|h| SourceLocationInfo {
                message_id: h.id.as_str().to_string(),
                source_document_id: Some(StableId::derive(
                    IdKind::Document,
                    Stability::Reconstructed,
                    &[h.id.as_str().as_bytes()],
                )),
                byte_start: Some(0),
                byte_end: Some(h.text.as_deref().unwrap_or("").len() as u64),
            })
            .collect()
    }

    fn default_input<'a>(
        hits: &'a [SearchHit],
        source_locations: &'a [SourceLocationInfo],
    ) -> HandoffInput<'a> {
        static EMPTY: Vec<String> = Vec::new();
        static EMPTY_ACT: Vec<serde_json::Value> = Vec::new();
        static EMPTY_FACTS: Vec<MessageFact> = Vec::new();
        HandoffInput {
            query_terms: &EMPTY,
            retrieval_mode: RetrievalMode::Lexical,
            filters: HandoffFilters::default(),
            hits,
            source_locations,
            tool_activities: &EMPTY_ACT,
            message_facts: &EMPTY_FACTS,
            catalog_generation: 1,
            max_tokens: 10000,
            max_bytes: 1_000_000,
            max_evidence: 100,
            target: None,
        }
    }

    /// 便捷包装：为命中生成默认 source placements 并构造默认输入。
    fn generate(hits: &[SearchHit]) -> HandoffPack {
        let locs = locations(hits);
        generate_deterministic(default_input(hits, &locs)).unwrap()
    }

    #[test]
    fn generates_pack_with_evidence() {
        let hits = vec![
            hit("msg_v1_aaa", 1.0, "hello world"),
            hit("msg_v1_bbb", 0.8, "hello there"),
        ];
        let pack = generate(&hits);
        assert_eq!(pack.schema_version, "1.0");
        assert!(!pack.pack_id.is_empty());
        assert_eq!(pack.evidence.len(), 2);
        assert!(pack.inference.is_empty()); // deterministic: no LLM
        assert!(!pack.truncation.truncated);
        assert_eq!(pack.confidence.overall, ConfidenceLevel::High);
        assert_eq!(pack.generation_mode, GenerationMode::Deterministic);
        // 证据带权威 source locator：doc 前缀 + 消息 cursor 反向追踪。
        for entry in &pack.evidence {
            assert!(
                entry.source_document_id.starts_with("doc_v1_"),
                "{}",
                entry.source_document_id
            );
        }
        assert_eq!(pack.source_locators.len(), 2);
        assert!(
            pack.source_locators[0]
                .cursor
                .as_deref()
                .is_some_and(|c| c.starts_with("msg_v1_"))
        );
    }

    #[test]
    fn audit_pack_identity_delimits_filter_fields_and_optional_values() {
        let mut since = default_input(&[], &[]);
        since.filters.since = Some("2026-01-01T00:00:00Z".into());
        let mut until = default_input(&[], &[]);
        until.filters.until = since.filters.since.clone();
        assert_ne!(derive_pack_id(&since), derive_pack_id(&until));
        let none = default_input(&[], &[]);
        let mut empty = default_input(&[], &[]);
        empty.filters.since = Some(String::new());
        assert_ne!(derive_pack_id(&none), derive_pack_id(&empty));
        let terms = vec!["same-value".to_string()];
        let mut query = default_input(&[], &[]);
        query.query_terms = &terms;
        let mut provider = default_input(&[], &[]);
        provider.filters.providers = terms.clone();
        assert_ne!(derive_pack_id(&query), derive_pack_id(&provider));
    }

    #[test]
    fn audit_handoff_never_returns_oversized_mandatory_content() {
        let terms = vec!["x".repeat(10_000)];
        let mut input = default_input(&[], &[]);
        input.query_terms = &terms;
        input.max_bytes = 4096;
        assert!(matches!(
            generate_deterministic(input),
            Err(PortError::InvalidRequest(_))
        ));
    }

    #[test]
    fn audit_handoff_trims_metadata_and_recomputes_empty_confidence() {
        let hits = vec![hit("msg_v1_audit", 1.0, &"body ".repeat(2000))];
        let locs = locations(&hits);
        let activities = vec![serde_json::json!({"result": "x".repeat(12_000)})];
        let mut input = default_input(&hits, &locs);
        input.tool_activities = &activities;
        input.max_bytes = 4096;
        let pack = generate_deterministic(input).unwrap();
        assert!(pack.budget.used_bytes <= pack.budget.max_bytes);
        assert!(pack.evidence.is_empty());
        assert_eq!(pack.confidence.overall, ConfidenceLevel::Low);
        assert!(pack.truncation.truncated);
    }

    #[test]
    fn empty_hits_produces_low_confidence() {
        let hits: Vec<SearchHit> = vec![];
        let pack = generate(&hits);
        assert_eq!(pack.evidence.len(), 0);
        assert_eq!(pack.confidence.overall, ConfidenceLevel::Low);
        assert_eq!(pack.matched_sessions.len(), 0);
    }

    #[test]
    fn created_at_is_deterministic_and_generation_derived() {
        let hits = vec![hit("msg_v1_aaa", 1.0, "hello world")];
        let p1 = generate(&hits);
        let p2 = generate(&hits);
        assert_eq!(p1.created_at, p2.created_at);
        assert_eq!(p1.created_at, "2026-08-16T00:00:01Z"); // base + generation 1
        // 同一 catalog 的两次运行 → 全包字节一致（determinism 契约）。
        assert_eq!(
            serde_json::to_string(&p1).unwrap(),
            serde_json::to_string(&p2).unwrap()
        );
    }

    #[test]
    fn pack_id_changes_with_budget() {
        let hits = vec![hit("msg_v1_aaa", 1.0, "hello world")];
        let locs = locations(&hits);
        let base = default_input(&hits, &locs);
        let mut other = default_input(&hits, &locs);
        other.max_tokens = 500;
        assert_ne!(
            generate_deterministic(base).unwrap().pack_id,
            generate_deterministic(other).unwrap().pack_id
        );
    }

    #[test]
    fn truncates_at_max_evidence() {
        let hits: Vec<SearchHit> = (0..10)
            .map(|i| {
                hit(
                    &format!("msg_v1_{i:03}"),
                    1.0 - i as f32 * 0.1,
                    &format!("text {i}"),
                )
            })
            .collect();
        let locs = locations(&hits);
        let mut input = default_input(&hits, &locs);
        input.max_evidence = 3;
        let pack = generate_deterministic(input).unwrap();
        assert!(pack.truncation.truncated);
        assert_eq!(pack.truncation.reason, TruncationReason::MaxEvidence);
        assert_eq!(pack.evidence.len(), 3);
        assert_eq!(pack.truncation.dropped_count, 7);
    }

    #[test]
    fn truncates_at_token_budget() {
        let hits = vec![
            hit("msg_v1_aaa", 1.0, "hello world this is a long text"),
            hit("msg_v1_bbb", 0.8, "another long text that exceeds budget"),
        ];
        let locs = locations(&hits);
        let mut input = default_input(&hits, &locs);
        input.max_tokens = 5; // 极小的 token 预算
        let pack = generate_deterministic(input).unwrap();
        assert!(pack.truncation.truncated);
        assert_eq!(pack.truncation.reason, TruncationReason::BudgetExceeded);
        assert!(pack.evidence.len() < 2);
    }

    #[test]
    fn truncates_at_byte_budget() {
        let hits: Vec<SearchHit> = (0..20)
            .map(|i| {
                hit(
                    &format!("msg_v1_{i:03}"),
                    1.0 - i as f32 * 0.01,
                    &format!("some reasonably long evidence text for message {i}"),
                )
            })
            .collect();
        let locs = locations(&hits);
        let mut input = default_input(&hits, &locs);
        input.max_bytes = 1500; // 只容得下几条证据
        let pack = generate_deterministic(input).unwrap();
        assert!(pack.truncation.truncated, "{:?}", pack.truncation.reason);
        assert_eq!(pack.truncation.reason, TruncationReason::MaxBytes);
        assert!(pack.budget.used_bytes <= 1500, "{}", pack.budget.used_bytes);
        // used_bytes 与内容口径自洽（诊断元数据不计入）。
        assert_eq!(pack.budget.used_bytes, content_bytes(&pack));
        assert!(pack.evidence.len() < 20, "evidence should be truncated");
    }

    #[test]
    fn evidence_without_source_document_is_excluded() {
        // 无 placement 的命中：不进证据，也不臆造文档 id。
        let hits = vec![hit("msg_v1_aaa", 1.0, "orphan text")];
        let no_locs = vec![SourceLocationInfo {
            message_id: "msg_v1_aaa".into(),
            source_document_id: None,
            byte_start: None,
            byte_end: None,
        }];
        let pack = generate_deterministic(default_input(&hits, &no_locs)).unwrap();
        assert_eq!(pack.evidence.len(), 0);
        assert_eq!(pack.confidence.overall, ConfidenceLevel::Low);
        assert_eq!(pack.matched_sessions.len(), 1); // 命中归属会话仍在 matched 列表
    }

    #[test]
    fn redaction_is_applied_and_reported() {
        let hits = vec![hit(
            "msg_v1_aaa",
            1.0,
            "deploy key is sk-ant-api03-1234567890abcdef now",
        )];
        let pack = generate(&hits);
        assert_eq!(pack.evidence.len(), 1);
        assert!(
            !pack.evidence[0].text.contains("sk-ant-"),
            "secret leaked: {}",
            pack.evidence[0].text
        );
        assert_eq!(pack.redaction.mode, RedactionMode::Default);
        assert_eq!(pack.redaction.status, RedactionState::Applied);
        assert_eq!(pack.redaction.redacted_count, 1);
        assert_eq!(pack.redaction.ruleset_version, RULESET_VERSION);
    }

    #[test]
    fn clean_text_reports_none_redaction() {
        let hits = vec![hit("msg_v1_aaa", 1.0, "plain ordinary message")];
        let pack = generate(&hits);
        assert_eq!(pack.redaction.status, RedactionState::None);
        assert_eq!(pack.redaction.redacted_count, 0);
    }

    #[test]
    fn matched_sessions_use_session_ids_not_message_ids() {
        let mut a = hit("msg_v1_aaa", 1.0, "text1");
        let mut b = hit("msg_v1_bbb", 0.8, "text2");
        a.session_id = Some("ses_v1_s1".to_string());
        b.session_id = Some("ses_v1_s1".to_string());
        let hits = vec![a, b];
        let pack = generate(&hits);
        assert_eq!(pack.matched_sessions.len(), 1);
        assert!(
            pack.matched_sessions[0].session_id.starts_with("ses_v1_"),
            "session_id must be a session, got {}",
            pack.matched_sessions[0].session_id
        );
        assert_eq!(pack.matched_sessions[0].occurrences, 2);
    }

    #[test]
    fn pack_id_is_deterministic() {
        let hits = vec![hit("msg_v1_aaa", 1.0, "hello")];
        let pack1 = generate(&hits);
        let pack2 = generate(&hits);
        assert_eq!(pack1.pack_id, pack2.pack_id);
    }

    #[test]
    fn different_queries_produce_different_pack_ids() {
        let hits = vec![hit("msg_v1_aaa", 1.0, "hello")];
        let terms1 = vec!["hello".to_string()];
        let terms2 = vec!["world".to_string()];
        let locs = locations(&hits);
        let mut input1 = default_input(&hits, &locs);
        input1.query_terms = &terms1;
        let mut input2 = default_input(&hits, &locs);
        input2.query_terms = &terms2;
        let pack1 = generate_deterministic(input1).unwrap();
        let pack2 = generate_deterministic(input2).unwrap();
        assert_ne!(pack1.pack_id, pack2.pack_id);
    }

    #[test]
    fn evidence_and_inference_are_separate() {
        let hits = vec![hit("msg_v1_aaa", 1.0, "real evidence text")];
        let pack = generate(&hits);
        assert_eq!(pack.evidence.len(), 1);
        assert_eq!(pack.evidence[0].text, "real evidence text");
        assert!(pack.inference.is_empty());
    }

    #[test]
    fn serialized_pack_has_no_path_keys_or_jsonl_paths() {
        // 隐私契约：pack 序列化不含任何 path 型 key 或 transcript 路径字符串。
        let hits = vec![hit("msg_v1_aaa", 1.0, "hello world")];
        let pack = generate(&hits);
        let json = serde_json::to_value(&pack).unwrap();
        assert_no_path_keys(&json);
        let text = serde_json::to_string(&pack).unwrap();
        assert!(!text.contains(".jsonl"), "transcript path leaked: {text}");
    }

    fn assert_no_path_keys(value: &serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    assert!(
                        !key.to_ascii_lowercase().contains("path"),
                        "path-bearing key: {key}"
                    );
                    assert_no_path_keys(val);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    assert_no_path_keys(item);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn schema_round_trip_preserves_pack() {
        let hits = vec![
            hit("msg_v1_aaa", 1.0, "first evidence"),
            hit("msg_v1_bbb", 0.8, "second evidence"),
        ];
        let pack = generate(&hits);
        let json = serde_json::to_string(&pack).unwrap();
        let back: HandoffPack = serde_json::from_str(&json).unwrap();
        assert_eq!(back, pack);
    }

    /// schema-drift 测试：发布 schema 的 required 字段与序列化 pack 的顶层键
    /// 保持一致，声明枚举与 Rust 实现一致（追加字段只允许 optional）。
    #[test]
    fn published_schema_stays_in_lockstep_with_implementation() {
        let schema: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../schemas/handoff/v1/pack.schema.json"
        )))
        .expect("published handoff schema must be valid JSON");

        let hits = vec![hit("msg_v1_aaa", 1.0, "hello world")];
        let pack = generate(&hits);
        let json = serde_json::to_value(&pack).unwrap();
        let required: Vec<&str> = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        // 每个 required 字段都出现在实现里。
        for field in &required {
            assert!(
                json.get(*field).is_some(),
                "schema requires {field} but implementation omits it"
            );
        }
        // 实现新增的顶层字段必须在 schema 的 properties 中声明（禁止漂移）。
        let properties = schema["properties"].as_object().unwrap();
        for (key, _) in json.as_object().unwrap() {
            assert!(
                properties.contains_key(key),
                "implementation field {key} is missing from the published schema"
            );
        }
        // generation_mode 常量与实现一致。
        assert_eq!(
            schema["properties"]["generation_mode"]["const"],
            "deterministic"
        );
        // retrieval_mode / truncation / redaction 枚举与实现一致。
        let schema_modes: Vec<&str> = schema["properties"]["query"]["properties"]["retrieval_mode"]
            ["enum"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        for mode in ["lexical", "semantic", "hybrid", "lexical_fallback"] {
            assert!(schema_modes.contains(&mode), "schema missing {mode}");
        }
        let reasons: Vec<&str> = schema["$defs"]["truncation"]["properties"]["reason"]["enum"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        for reason in [
            "budget_exceeded",
            "max_items",
            "max_evidence",
            "max_bytes",
            "none",
        ] {
            assert!(reasons.contains(&reason), "schema missing {reason}");
        }
        // id 形态一致：schema 声明 wire-string pattern，实现必须输出 wire 字符串
        // （不是 StableId 对象）——schema/impl 漂移防护。
        let evidence = json["evidence"].as_array().expect("evidence array");
        assert!(!evidence.is_empty());
        let source_document_id = evidence[0]["source_document_id"]
            .as_str()
            .unwrap_or_else(|| panic!("source_document_id must be a wire string: {json}"));
        assert!(
            source_document_id.starts_with("doc_v1_"),
            "source_document_id must be doc_v1_: {source_document_id}"
        );
        let message_id = evidence[0]["message_id"]
            .as_str()
            .unwrap_or_else(|| panic!("message_id must be a wire string: {json}"));
        assert!(
            message_id.starts_with("msg_v1_"),
            "message_id must be msg_v1_: {message_id}"
        );
        if let Some(matched) = json["matched_sessions"].as_array() {
            for session in matched {
                let id = session["session_id"]
                    .as_str()
                    .unwrap_or_else(|| panic!("session_id must be a wire string: {json}"));
                assert!(
                    id.starts_with("ses_v1_"),
                    "matched session id must be ses_v1_: {id}"
                );
            }
        }
    }

    /// `handoff` 能力列 ↔ 生成器真实行为 防漂移（与 `resume` 列的
    /// `capability_matrix_resume_level_matches_builder_support` 同一纪律）。
    ///
    /// 起因：capability.rs 曾对全部 14 个已实现 provider 声明
    /// `handoff: Unsupported`，而 `asg handoff` 是发布功能且对 codex（JSONL）、
    /// aider（markdown，native_id 恒空）、opencode（SQLite）三种结构迥异的真实
    /// golden 源实测都产出 `confidence: high` 的带证据 pack。少报的根因是
    /// 把 handoff 当成了 per-provider 能力：本生成器只消费 `SearchHit` +
    /// 权威 source placement，`provenance`/`matched_sessions[].provider_id`
    /// 一律为 `None`（"搜索型 pack 无单一会话/提供商，honest：不臆造"），
    /// 全流程不读 provider 身份，也没有任何 per-provider 分支。
    ///
    /// 因此该列的诚实口径是：能否装出带证据的 pack 只取决于消息是否落库并带
    /// source placement——这对所有已实现 provider 一致成立（parse 可用即成立）。
    /// 本测试把这条不变量钉住：用同一批合成命中，逐个 provider 身份走生成器，
    /// 断言产出逐字节相同，从而证明"provider 无关"，任何未来引入的
    /// per-provider 分支都会在此失败。
    #[test]
    fn handoff_pack_generation_is_provider_independent() {
        use agent_session_grep_ports::capability::{ProviderCapabilityMatrix, ProviderMaturity};

        let matrix = ProviderCapabilityMatrix::current();
        let implemented: Vec<String> = matrix
            .providers
            .iter()
            .filter(|p| p.maturity != ProviderMaturity::Unsupported)
            .map(|p| p.provider_id.clone())
            .collect();
        assert_eq!(
            implemented.len(),
            14,
            "已实现 provider 应为 14 个，实际 {}",
            implemented.len()
        );

        let hits = vec![
            hit("msg_v1_aaa", 1.0, "hello world"),
            hit("msg_v1_bbb", 0.8, "hello there"),
        ];
        let locs = locations(&hits);

        // 逐个 provider 身份进入 filters.providers：生成器若读 provider 身份做
        // 分支，pack 内容就会随之改变。这里只允许 `filters` 回显与 `pack_id`
        // （其派生输入含 filters）不同，装配出的证据/mainline/置信度必须一致。
        let mut baseline: Option<(Vec<EvidenceEntry>, Vec<MainlineEntry>, ConfidenceLevel)> = None;
        for provider_id in &implemented {
            let filters = HandoffFilters {
                providers: vec![provider_id.clone()],
                ..HandoffFilters::default()
            };
            let mut input = default_input(&hits, &locs);
            input.filters = filters;
            let pack = generate_deterministic(input).unwrap();

            assert!(
                !pack.evidence.is_empty(),
                "{provider_id}: handoff 生成器必须为已落库且带 source placement 的命中产出证据"
            );
            assert!(
                pack.provenance.is_none(),
                "{provider_id}: 搜索型 pack 不得臆造单一 provenance"
            );
            for session in &pack.matched_sessions {
                assert!(
                    session.provider_id.is_none(),
                    "{provider_id}: matched_sessions 不得携带臆造的 provider 身份"
                );
            }

            let shape = (
                pack.evidence.clone(),
                pack.mainline.clone(),
                pack.confidence.overall,
            );
            match &baseline {
                None => baseline = Some(shape),
                Some(expected) => assert_eq!(
                    &shape, expected,
                    "{provider_id}: handoff pack 装配随 provider 身份变化——\
                     生成器出现了 per-provider 分支，capability.rs 的 handoff \
                     列不能再按 provider 无关 统一声明"
                ),
            }
        }
    }
}

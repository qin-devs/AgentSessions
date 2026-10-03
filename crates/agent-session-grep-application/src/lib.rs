//! Application：跨前端（CLI / Robot-JSON / MCP / TUI）共享的用例与请求/结果 ADT。
//!
//! 本层不知道任何具体前端或后端——只依赖 domain 的类型与 ports 的抽象。
//! 所有前端把各自的输入归一为 [`AppRequest`]，把 [`AppResponse`] 渲染成各自的输出格式。
//! 分层依赖不变量：domain ← ports ← application ← adapters。

use agent_session_grep_domain::{
    ContextPolicy, DomainError, IdKind, Message, MessagePlacement, Role, SourceDocument, StableId,
    ToolActivity, UsageObservation, select_full, select_mainline,
};
use agent_session_grep_ports::{
    CanonicalEventSink, CatalogEntry, CatalogStore, Confidence, ContextGraphStore, MessageEvent,
    NoResumeClaims, NoSemanticIndex, ParseReport, PortError, PortResult, ProbeResult,
    ProviderAdapter, ProviderError, ReadOnlySource, RepoTotals, ResumeClaimsStore, RetrievalMode,
    SearchFacets, SearchFilters, SearchHit, SearchIndex, SearchInstant, SearchQuery, SemanticIndex,
    SessionResumeMetadata, ToolActivityEvent, UsageEvent, UsageTotals,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub mod activity;
pub mod budget;
#[cfg(feature = "semantic-candle")]
pub mod candle_embedding;
pub mod cjk;
pub mod cursor;
pub mod embedding;
pub mod evidence;
pub mod guidance;
pub mod handoff_pack;
pub mod hybrid;
pub mod peek;
pub mod ranking;
pub mod relocation;
pub mod resume;
pub mod retention;
pub mod snippet;

pub use budget::{ResponseBudget, Truncation};
pub use cjk::{bigram_cjk, fts_tokens_cjk};
pub use evidence::EvidenceSpanDto;
pub use peek::SessionPeek;
pub use retention::{MESSAGE_FTS_MAX_CHARS, bounded_index_text};

/// 排序方案标识：catalog 列表的钉住排序（wire id 升序，见 sqlite `ORDER BY id ASC`）。
pub const SORT_WIRE_ID_ASC: &str = "wire_id_asc";
/// 排序方案标识：检索结果的钉住排序（bm25 降序 + id tiebreak，全序确定）。
pub const SORT_SCORE_DESC: &str = "score_desc";

/// Cursor 结果集判别器（resume-protocol-prerequisites R2）：`list` 全部实体。
pub const RESULT_SET_ALL: &str = "all";
/// Cursor 结果集判别器：`list_sessions` 仅会话实体。
pub const RESULT_SET_SESSIONS_ONLY: &str = "sessions_only";

/// `resume_available` 恒序列化进 searchHit 的字节开销（`, "resume_available":false`）。
const RESUME_AVAILABLE_FIELD_BYTES: usize = 24;

/// clamp 前从 `max_response_bytes` 扣除的 envelope 预留（budget.rs 声明预留是调用方义务）。
/// 预算下限 4096 保证扣除后仍为正。
const ENVELOPE_RESERVE_BYTES: usize = 1024;

/// Maximum number of Session IDs exposed by a message-resolution ambiguity.
/// The total count remains available separately while the candidate list stays
/// bounded for protocol and privacy safety.
pub const MAX_MESSAGE_AMBIGUITY_CANDIDATES: usize = 8;

/// Fixed metadata for a derived context view. Duplicated message payloads are
/// charged per retained occurrence so clamping can still preserve a prefix.
const STRUCTURAL_METADATA_RESERVE_BYTES: usize = 320;

/// Upper bound on a single fetch window. Cursor offsets are tamper-evident
/// but not unforgeable; capping the window keeps a forged huge offset from
/// overflowing into a negative SQL LIMIT (SQLite treats -1 as "no limit").
const MAX_FETCH_WINDOW: u64 = 1 << 20;

/// 应用层重排路径（纯 lexical 的 rank signals、semantic/hybrid 未就绪时的
/// lexical_fallback，以及 hybrid 的 RRF 融合）的 **排序窗口**：与 cursor
/// offset 无关的固定取数上限。重排后的钉住排序是"同一窗口上的全序"——窗口
/// 若随 offset 增长，每一页都在不同的集合上重排，拼接结果会重复页尾命中、
/// 漏掉真正的高分命中（silent wrong result）。所有页取同一窗口、重排一次后
/// 按 offset 切片，分页才是同一个全序的不重不漏划分；offset 越过窗口后分页
/// 以 has_more=false 诚实终止（这是排序视界，不是缺陷）。512 ≈ 20 条/页 ×
/// 25 页，覆盖典型翻页深度；不受伪造 offset 影响（窗口不随 offset 变化）。
const RANK_SCAN_WINDOW: u64 = 512;

/// group_by_session（R3）模式下相对分页窗口的扫描放大倍数：归并需要把命中先
/// 汇到会话级再切页，扫描窗口取 `页窗口 × GROUP_SCAN_FACTOR`（仍被
/// MAX_FETCH_WINDOW 封顶），使 `occurrences` 覆盖更有意义的命中样本。
const GROUP_SCAN_FACTOR: u64 = 16;

/// JSON 语法开销：list 条目附加派生标题（#6，schema v13）时写入的
/// `,"title":` 前缀（`,` + `"title"` + `:`）。与 peek 的
/// [`peek::PEEK_ENTRY_OVERHEAD_BYTES`] 同一计费方式——标题是渲染产物，
/// 字节必须计入 `max_response_bytes` 字节闸。
const TITLE_ENTRY_OVERHEAD_BYTES: usize = 9;

/// 一条 JSON 字符串字面量的序列化长度（含两端引号与转义）。
fn json_string_len(value: &str) -> usize {
    2 + value
        .bytes()
        .map(|b| match b {
            b'"' | b'\\' => 2,
            0x00..=0x1F | 0x7F => 6, // \uXXXX
            _ => 1,
        })
        .sum::<usize>()
}

/// `payload` 经 `String::from_utf8_lossy` 转为字符串、再做 JSON 字符串转义后的
/// 序列化长度（含两端引号）。
///
/// 字节闸的估算必须按**序列化后**长度计，而不是原始字节数：`Vec<u8>` 渲染为
/// 字符串时，控制字节会膨胀为 `\uXXXX`（6 字符），无效 UTF-8 的每个"最大子部分"
/// 替换为一个 U+FFFD（3 字节）。
///
/// 实现直接复用渲染侧的同一个 `from_utf8_lossy`，而不是再实现一遍 UTF-8 校验：
/// 手写规则必然与 `core` 的替换语义分叉——overlong 编码、UTF-16 代理区、超出
/// U+10FFFF 的首字节都"看起来像"合法多字节序列，按长度计费即低估到真实值的
/// 三分之一，字节闸会放行超预算的页并报 `truncated: false`。合法 UTF-8 时
/// `from_utf8_lossy` 借用原缓冲、不分配。
fn lossy_payload_json_len(payload: &[u8]) -> usize {
    json_string_len(&String::from_utf8_lossy(payload))
}

/// Application 边界错误：保留 Domain、Port、Provider、Cursor 与 Budget 的原始分类，
/// 供各前端统一映射协议（cursor/budget 错误在 protocol 层有专属 canonical code）。
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Port(#[from] PortError),
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Cursor(#[from] cursor::CursorError),
    #[error(transparent)]
    Budget(#[from] budget::BudgetError),
    #[error(transparent)]
    MessageAmbiguous(#[from] MessageAmbiguity),
}

/// 应用层请求 ADT：所有前端的统一入口。
///
/// 每个变体是一个用例。前端负责解析各自语法后构造本枚举，
/// 从而保证 CLI / Robot / MCP / TUI 行为一致（见 CONTRACT-cli-robot-mcp-draft）。
#[derive(Debug, Clone, PartialEq)]
pub enum AppRequest {
    /// 全文检索：按查询串返回命中列表（分页 + 预算）。
    Search {
        query: String,
        /// Optional normalized metadata predicates. Providers are ORed; the
        /// provider and half-open UTC time dimensions are ANDed.
        filters: SearchFilters,
        /// 结构化 facet 过滤（sidechain / 工具 kind / 工具名）。默认值 = 无过滤，
        /// 行为与旧版完全一致；facet 变化会使已发 cursor 失效（绑定进 digest）。
        facets: SearchFacets,
        /// 最多返回条数；0 视为非法请求。与 `budget.max_items` 取较小者为页大小。
        limit: usize,
        /// 上一页发出的续读令牌；`None` 表示第一页。
        cursor: Option<String>,
        /// 响应预算（CONTRACT §3）；低于下限即校验错误。
        budget: ResponseBudget,
        /// 是否包含系统噪声（competitor-borrowings R2）：`false`（默认）排除
        /// role 为 system/developer 的命中，`true` 显式恢复。
        include_system: bool,
        /// 是否按会话归并（competitor-borrowings R3）：`false`（默认）保持
        /// 逐命中分页；`true` 时每会话只保留最高分命中并附带 `occurrences`。
        group_by_session: bool,
        /// 检索模式（#3）：`Lexical`（默认）走 FTS；`Semantic`/`Hybrid` 需要
        /// 已就绪的语义索引与 `query_embedding`，否则显式降级为
        /// `LexicalFallback` + warning。
        mode: RetrievalMode,
        /// 查询文本的 embedding（调用方经 EmbeddingModel 生成，Application
        /// 保持模型无关）。`Semantic`/`Hybrid` 模式必须提供；缺失即降级。
        query_embedding: Option<Vec<f32>>,
    },
    /// 按稳定 ID 取回单个实体的原始负载。
    Get { id: StableId },
    /// 按稳定 wire id 展开展示一个实体；当前 Beta 返回规范化 payload。
    Show { id: StableId },
    /// 按稳定 wire id 顺序列出实体（分页 + 预算）；0 视为非法。
    List {
        limit: usize,
        cursor: Option<String>,
        budget: ResponseBudget,
        /// true 时只列出 Session 实体（competitor-borrowings R1.3：`list_sessions`
        /// 不再被 doc/msg 实体淹没）。过滤在存储层做，offset/limit 分页语义
        /// 保持作用在过滤后的集合上。
        sessions_only: bool,
    },
    /// 会话上下文装配：按策略选取分支，返回消息链与证据区间（CONTRACT §1-2）。
    Context {
        session_id: StableId,
        policy: ContextPolicy,
        level: ContextLevel,
        budget: ResponseBudget,
    },
    /// Resolve one stable Message in an authoritative Session mainline and
    /// return a bounded placement window around it.
    Message {
        message_id: StableId,
        session_id: Option<StableId>,
        around: usize,
        budget: ResponseBudget,
    },
    /// Resolve every distinct Session that contains placements for one Message.
    MessageContexts { message_id: StableId },
    /// Resolve read-only Resume Metadata for one canonical Session (ADR-0009).
    GetSessionResume { session_id: StableId },
    /// 返回当前 Catalog 统计状态。
    Status,
}

/// Detail level for a context response. `Raw` is the compatibility default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContextLevel {
    Raw,
    Talks,
    Sessions,
}

impl ContextLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Talks => "talks",
            Self::Sessions => "sessions",
        }
    }
}

/// One placement-aware context response item.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ContextMessage {
    /// Compatibility alias retained for existing frontends.
    pub id: String,
    /// Authoritative occurrence identity.
    pub placement_id: String,
    /// Stable Message identity; always equal to `id`.
    pub message_id: String,
    /// Existing canonical Message payload, returned opaquely.
    pub payload: serde_json::Value,
}

/// A structural talk: one user message followed by assistant/tool messages.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ContextTalk {
    pub user_message: ContextMessage,
    pub following_messages: Vec<ContextMessage>,
}

/// A bounded structural overview of one selected context branch.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ContextSessionSummary {
    pub first_user_message: Option<ContextMessage>,
    pub message_count: usize,
    pub turn_count: usize,
    pub file_references: Vec<String>,
}

/// A deterministic next-call hint. Identifiers are copied from the context.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContextHint {
    pub command: String,
    pub session_id: String,
    pub level: ContextLevel,
}

/// One distinct Session candidate for a stable Message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MessageContextCandidate {
    pub session_id: String,
    pub placement_ids: Vec<String>,
}

/// Bounded explicit ambiguity when a shared Message belongs to several Sessions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAmbiguity {
    pub candidate_session_ids: Vec<String>,
    pub candidate_count: usize,
    pub hint: String,
}

impl std::fmt::Display for MessageAmbiguity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("message belongs to multiple sessions; provide session_id")
    }
}

impl std::error::Error for MessageAmbiguity {}

/// One message window selected from an authoritative Session mainline.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageWindow {
    pub message_id: String,
    pub session_id: String,
    pub anchor_placement_id: String,
    pub messages: Vec<ContextMessage>,
    pub truncation: Truncation,
    pub generation: u64,
}

/// 应用层结果 ADT：前端据此渲染，不再回到 domain/ports 类型。
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum AppResponse {
    /// 检索结果，按相关性降序（bm25 + id tiebreak 的钉住全序）。
    Search {
        hits: Vec<SearchHit>,
        /// 还有后续页时的续读令牌；`None` 表示已到末尾。
        next_cursor: Option<String>,
        /// 本页对应的活动 generation（cursor 绑定它）。
        generation: u64,
        /// 预算截断标记；`truncated` 时前端应报 partial。
        truncation: Truncation,
        /// 实际生效的检索模式（#3）：请求 semantic/hybrid 但索引未就绪时为
        /// `LexicalFallback`，envelope 必须如实标注。
        retrieval_mode: RetrievalMode,
        /// 降级说明（#3）：semantic/hybrid 降级到 lexical 时的 warning 文本。
        fallback_warning: Option<String>,
    },
    /// 单个实体的原始负载；`None` 表示未找到。
    Get { payload: Option<Vec<u8>> },
    /// 单个实体的规范化展示；`None` 表示未找到。
    Show { payload: Option<Vec<u8>> },
    /// 稳定排序后的 Catalog 条目（wire id 升序）。
    List {
        entries: Vec<CatalogEntry>,
        /// 与 `entries` 逐位对齐的 Peek 预览（#7）：`sessions_only` 列表的每个
        /// 会话条目一个 `Some`，普通 `list` 全为 `None`。预览字节计入
        /// `max_response_bytes` 字节闸，绝不免费越闸。
        peeks: Vec<Option<SessionPeek>>,
        /// 与 `entries` 逐位对齐的派生会话标题（#6，schema v13 标题投影）：
        /// `sessions_only` 列表的每个会话条目一个 `Option`（`None` = 无派生
        /// 标题），普通 `list` 全为 `None`。标题字节与 peek 一样计入
        /// `max_response_bytes` 字节闸。
        titles: Vec<Option<String>>,
        next_cursor: Option<String>,
        generation: u64,
        truncation: Truncation,
    },
    /// 会话上下文：选中分支的有序消息链 + 证据区间。
    Context {
        /// 会话 wire id。
        session_id: String,
        /// 会话 canonical payload（`{document, messages}`）。
        session: serde_json::Value,
        /// 选中分支的叶子消息 wire id；会话无消息时为 `None`。
        branch_leaf: Option<String>,
        /// Authoritative leaf occurrence; `branch_leaf` remains its Message-ID alias.
        branch_leaf_placement_id: Option<String>,
        /// root→leaf（mainline）或 deterministic placement order（full）的 occurrences。
        messages: Vec<ContextMessage>,
        /// 与 `messages` 对齐装配的证据区间（可能被 `max_evidence_spans` 截短）。
        evidence: Vec<EvidenceSpanDto>,
        /// Tool activities for messages in this context (schema v12 projection).
        /// Empty when none are stored; never fabricated.
        tool_activities: Vec<serde_json::Value>,
        /// Requested and effective structural response levels.
        requested_level: ContextLevel,
        effective_level: ContextLevel,
        /// Structural talk groups for `talks` responses.
        talks: Vec<ContextTalk>,
        /// Structural overview for `sessions` responses.
        summary: Option<ContextSessionSummary>,
        /// Deterministic next-call hint, when a real session identifier is available.
        hint: Option<ContextHint>,
        truncation: Truncation,
        generation: u64,
    },
    /// One placement-aware mainline window.
    Message { window: MessageWindow },
    /// Distinct-session candidates for a stable Message.
    MessageContexts {
        message_id: String,
        candidates: Vec<MessageContextCandidate>,
    },
    /// Read-only Resume Metadata projection (ADR-0009)：固定可空字段，恒在。
    SessionResume(SessionResumeMetadata),
    /// 当前 Catalog 实体总数、关系统计与活动 generation。
    Status {
        catalog_count: u64,
        active_generation: u64,
        placements: u64,
        source_placement_claims: u64,
        /// 全库 token 用量聚合；`None` = 存储无 usage 投影。
        usage: Option<UsageTotals>,
        /// 全库 repo 身份聚合（schema v16）；空列表 = 无 repo 事实（未知 ≠ 零）。
        repos: Vec<RepoTotals>,
    },
}

/// 一条经 staging 缓冲、待原子提交的规范化消息（RFC-0002 §5）。
///
/// 承载 parse 产出的语义与 provider-native 身份/threading 元数据，但不含 StableId——
/// id 派生策略（native uuid 优先，缺失时回退 path+seq）由调用方决定，
/// 使 stage 本身与存储无关、可独立单测。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedMessage {
    pub session: Option<agent_session_grep_ports::ProviderSessionIdentity>,
    pub seq: u32,
    /// provider-native 消息 id；空串表示 provider 未提供，调用方回退派生。
    pub native_id: String,
    /// 父消息 native id（threading 边）；`None` 表示根消息或未提供。
    pub parent_native_id: Option<String>,
    pub role: String,
    pub text: String,
    /// provider 原样时间串（ISO-8601 UTC）；`None` 表示缺失。
    pub timestamp: Option<String>,
    /// 是否为 sidechain（subagent/分支）消息。
    pub is_sidechain: bool,
    /// 源记录在已验证快照字节中的区间 `(start, end)`，end 排他；
    /// `None` 表示 provider 无法归因，绝不臆造。
    pub span: Option<(u64, u64)>,
}

/// 一条经 staging 缓冲、待原子提交的工具活动观察（RFC-0002 §5 扩展）。
///
/// 锚点以 provider-native 消息 id 承载；[`StagedBatch`] 的调用方负责把它解析
/// 为本批内稳定消息身份——解析失败（锚点消息未 emit）即丢弃，绝不臆造锚点。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedActivity {
    pub message_native_id: String,
    pub activity: ToolActivity,
}

/// 一次 staging 中缓冲的 token 用量观察（usage 维度）。
///
/// `message_native_id` 为空串表示 session 级观察（如 Codex `token_count`），
/// 由调用方挂到本批会话上；非空时调用方按消息锚点解析，失败即丢弃。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedUsage {
    pub message_native_id: String,
    pub usage: UsageObservation,
}

/// 一次 staging 的完整产物：缓冲消息 + provider 的完整解析报告。
///
/// Explicit per-message session identities take precedence; report-level
/// session metadata is the compatibility projection for single-session sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedBatch {
    pub messages: Vec<StagedMessage>,
    /// 工具活动观察（设计 R1-R6；provider 未声明工具活动时为空）。
    pub activities: Vec<StagedActivity>,
    /// token 用量观察（usage 维度；provider 未声明用量时为空）。
    pub usage_events: Vec<StagedUsage>,
    /// Authoritative provider parse accounting and diagnostics.
    pub report: ParseReport,
    /// Deprecated compatibility shim: the CLI composition root still
    /// constructs this struct literally, so the field cannot be removed yet.
    /// It mirrors `report.session_native_id` and must not be trusted
    /// independently. Multi-session sources carry membership on each message.
    pub session_native_id: Option<String>,
}

/// Ingest 编排（RFC-0002 §5 source-level staging）：
///
/// 1. `probe` 判定 variant；ambiguous / 探测失败 → 立即拒绝，不 parse；
/// 2. `parse` 把 Canonical 消息推入**内存缓冲**（不写库）；
/// 3. 仅当 parse 完整成功才返回全部缓冲消息；任何失败返回 Err 且**不产出部分结果**。
///
/// 提交（写 catalog + FTS）由调用方在拿到完整 [`StagedBatch`] 后，
/// 用单事务原子执行（见 `SqliteStore::commit_batch`）。
pub fn stage(adapter: &dyn ProviderAdapter, bytes: &[u8]) -> Result<StagedBatch, AppError> {
    let probe = adapter.probe(bytes)?;
    stage_probed(adapter, bytes, probe)
}

/// Probe 完成后继续 stage（内部步骤）。
///
/// ambiguous 置信度：默认拒绝解析，不做"尽量解析"（RFC-0002 §3）。错误消息只
/// 保留 variant 标识，不携带 `unmatched_evidence`（可能含路径/原文片段）。
fn stage_probed(
    adapter: &dyn ProviderAdapter,
    bytes: &[u8],
    probe: ProbeResult,
) -> Result<StagedBatch, AppError> {
    if matches!(probe.confidence, Confidence::Ambiguous) {
        return Err(
            DomainError::InvalidRequest(format!("ambiguous variant {}", probe.variant_id)).into(),
        );
    }
    let mut sink = StagingSink::default();
    let report = adapter.parse(bytes, &mut sink)?;
    Ok(staged_batch(sink, report))
}

/// Reader-aware variant of [`stage_probed`]: parses via a fresh bounded reader,
/// so no source-sized byte buffer crosses the application boundary.
fn stage_source_probed(
    adapter: &dyn ProviderAdapter,
    source: &dyn ReadOnlySource,
    probe: ProbeResult,
) -> Result<StagedBatch, AppError> {
    if matches!(probe.confidence, Confidence::Ambiguous) {
        return Err(
            DomainError::InvalidRequest(format!("ambiguous variant {}", probe.variant_id)).into(),
        );
    }
    let mut sink = StagingSink::default();
    let report = adapter.parse_source(source, &mut sink)?;
    Ok(staged_batch(sink, report))
}

fn staged_batch(sink: StagingSink, report: ParseReport) -> StagedBatch {
    // 镜像到兼容别名（见 StagedBatch::session_native_id 文档）：唯一权威来源
    // 是 report.session_native_id。
    let session_native_id = report.session_native_id.clone();
    StagedBatch {
        messages: sink.buffered,
        activities: sink.activities,
        usage_events: sink.usage_events,
        report,
        session_native_id,
    }
}

/// 从多个 provider adapter 中选出匹配的那个，再 stage（RFC-0002 §3 provider 选择）。
///
/// 对每个 adapter 调 `probe`：跳过报错或 ambiguous 的（不是候选）；在剩余候选里
/// 取置信度最高的（Confirmed > High > Low）。若并列最高有多个不同 variant，视为
/// 无法区分，拒绝（不做"猜一个"）。选中后用该 adapter stage。
///
/// 无任何候选 → `InvalidRequest`（没有 provider 认领此源）。若存在 probe
/// 报错的 adapter，错误消息追加最后一个 probe 错误的细节——provider 的拒绝
/// 诊断自带行号定位与修复方向（PRD R2.2），绝不裸报"没有 provider 认领"。
/// 组合根（CLI）持有具体 adapter 清单，本函数只负责与格式无关的选择编排。
pub fn select_and_stage(
    adapters: &[&dyn ProviderAdapter],
    bytes: &[u8],
) -> Result<StagedBatch, AppError> {
    // 置信度排序键：越大越可信；ambiguous 不参与。
    fn rank(c: Confidence) -> Option<u8> {
        match c {
            Confidence::Confirmed => Some(3),
            Confidence::High => Some(2),
            Confidence::Low => Some(1),
            Confidence::Ambiguous => None,
        }
    }

    let mut best: Option<(u8, usize, ProbeResult)> = None; // (rank, adapter index, probe)
    let mut tie = false;
    // 最后一个 probe 报错（PRD R2.2）：全部 adapter 拒绝时，把错误自带的行号
    // 定位与修复方向带给调用方——绝不裸报 "no provider recognized this source"。
    // probe 错误只由源字节内容派生（provider 看不到路径），消息即诊断本身。
    let mut last_probe_error: Option<ProviderError> = None;
    for (idx, adapter) in adapters.iter().enumerate() {
        // probe 报错的 adapter 不是候选——它明确表示"这不是我的格式"。
        let probe = match adapter.probe(bytes) {
            Ok(probe) => probe,
            Err(error) => {
                last_probe_error = Some(error);
                continue;
            }
        };
        let Some(r) = rank(probe.confidence) else {
            continue;
        };
        match &best {
            Some((best_rank, _, best_probe)) => {
                if r > *best_rank {
                    best = Some((r, idx, probe));
                    tie = false;
                } else if r == *best_rank && probe.variant_id != best_probe.variant_id {
                    // 同等置信度、不同 variant——无法区分，标记歧义。
                    tie = true;
                }
            }
            None => best = Some((r, idx, probe)),
        }
    }

    let (_, idx, probe) = best.ok_or_else(|| {
        let detail = match last_probe_error {
            Some(error) => format!("; last probe failure: {error}"),
            None => String::new(),
        };
        DomainError::InvalidRequest(format!("no provider recognized this source{detail}"))
    })?;
    if tie {
        return Err(DomainError::InvalidRequest(format!(
            "ambiguous provider selection: multiple variants matched with equal confidence \
             (one candidate was {})",
            probe.variant_id
        ))
        .into());
    }
    // 复用选中时的 probe 结果，不再对同一字节第二次 probe。
    stage_probed(adapters[idx], bytes, probe)
}

/// Select and stage a repeatable read-only source (RFC-0002 §7 bounded ingest).
///
/// Same selection rules as [`select_and_stage`], but every probe/parse opens a
/// fresh bounded reader, so no source-sized byte buffer crosses the application
/// boundary. Returns `(staged, selected variant_id)`.
pub fn select_and_stage_source(
    adapters: &[&dyn ProviderAdapter],
    source: &dyn ReadOnlySource,
) -> Result<(StagedBatch, String), AppError> {
    // 置信度排序键：越大越可信；ambiguous 不参与。
    fn rank(c: Confidence) -> Option<u8> {
        match c {
            Confidence::Confirmed => Some(3),
            Confidence::High => Some(2),
            Confidence::Low => Some(1),
            Confidence::Ambiguous => None,
        }
    }

    let mut best: Option<(u8, usize, ProbeResult)> = None; // (rank, adapter index, probe)
    let mut tie = false;
    let mut last_probe_error: Option<ProviderError> = None;
    for (idx, adapter) in adapters.iter().enumerate() {
        let probe = match adapter.probe_source(source) {
            Ok(probe) => probe,
            Err(error) => {
                last_probe_error = Some(error);
                continue;
            }
        };
        let Some(r) = rank(probe.confidence) else {
            continue;
        };
        match &best {
            Some((best_rank, _, best_probe)) => {
                if r > *best_rank {
                    best = Some((r, idx, probe));
                    tie = false;
                } else if r == *best_rank && probe.variant_id != best_probe.variant_id {
                    tie = true;
                }
            }
            None => best = Some((r, idx, probe)),
        }
    }

    let (_, idx, probe) = best.ok_or_else(|| {
        let detail = match last_probe_error {
            Some(error) => format!("; last probe failure: {error}"),
            None => String::new(),
        };
        DomainError::InvalidRequest(format!("no provider recognized this source{detail}"))
    })?;
    if tie {
        return Err(DomainError::InvalidRequest(format!(
            "ambiguous provider selection: multiple variants matched with equal confidence \
             (one candidate was {})",
            probe.variant_id
        ))
        .into());
    }
    let variant = probe.variant_id.clone();
    let staged = stage_source_probed(adapters[idx], source, probe)?;
    Ok((staged, variant))
}

/// 内存 staging sink：只缓冲，绝不触库。
#[derive(Default)]
struct StagingSink {
    buffered: Vec<StagedMessage>,
    activities: Vec<StagedActivity>,
    usage_events: Vec<StagedUsage>,
}

impl CanonicalEventSink for StagingSink {
    fn emit_message(&mut self, event: MessageEvent<'_>) -> PortResult<()> {
        self.buffered.push(StagedMessage {
            session: event.session.cloned(),
            seq: event.seq,
            native_id: event.native_id.to_string(),
            parent_native_id: event.parent_native_id.map(str::to_string),
            role: event.role.to_string(),
            text: event.text.to_string(),
            timestamp: event.timestamp.map(str::to_string),
            is_sidechain: event.is_sidechain,
            span: event.span,
        });
        Ok(())
    }

    fn emit_activity(&mut self, event: ToolActivityEvent<'_>) -> PortResult<()> {
        self.activities.push(StagedActivity {
            message_native_id: event.message_native_id.to_string(),
            activity: event.activity,
        });
        Ok(())
    }

    fn emit_usage(&mut self, event: UsageEvent<'_>) -> PortResult<()> {
        self.usage_events.push(StagedUsage {
            message_native_id: event.message_native_id.to_string(),
            usage: event.usage,
        });
        Ok(())
    }
}

/// Validate search input without accessing a backend. Entrypoints reuse this
/// before opening a catalog; Application remains authoritative for all callers.
pub fn validate_search_request(
    query: &str,
    filters: &SearchFilters,
    limit: usize,
    budget: &ResponseBudget,
) -> Result<(), AppError> {
    if limit == 0 {
        return Err(DomainError::InvalidRequest("limit must be > 0".into()).into());
    }
    if query.chars().any(char::is_control) {
        return Err(DomainError::InvalidRequest("query contains control characters".into()).into());
    }
    if query.trim().is_empty() {
        return Err(DomainError::InvalidRequest("query must not be empty".into()).into());
    }
    if let (Some(since), Some(until)) = (filters.since, filters.until)
        && since >= until
    {
        return Err(DomainError::InvalidRequest("since must be earlier than until".into()).into());
    }
    budget.validate().map_err(AppError::from)
}

/// Parse a timezone-qualified RFC3339/ISO-8601 timestamp into a normalized
/// UTC instant. Naive local times are rejected because the application cannot
/// infer a timezone without introducing host-dependent behavior.
pub fn parse_search_instant(value: &str) -> Option<SearchInstant> {
    let value = value.trim();
    let (date, time) = value.split_once(['T', ' '])?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() || !(1..=12).contains(&month) {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day == 0 || day > days_in_month {
        return None;
    }

    let (clock, offset_minutes) = if let Some(clock) = time.strip_suffix('Z') {
        (clock, 0_i32)
    } else {
        let sign_at = time.rfind(['+', '-'])?;
        if sign_at == 0 {
            return None;
        }
        let (clock, offset) = time.split_at(sign_at);
        let (sign, digits) = offset.split_at(1);
        let (hours, minutes) = digits
            .split_once(':')
            .unwrap_or_else(|| digits.split_at_checked(2).unwrap_or((digits, "")));
        if hours.len() != 2
            || minutes.len() != 2
            || !hours
                .bytes()
                .chain(minutes.bytes())
                .all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        let hours: i32 = hours.parse().ok()?;
        let minutes: i32 = minutes.parse().ok()?;
        if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
            return None;
        }
        let magnitude = hours * 60 + minutes;
        (clock, if sign == "+" { magnitude } else { -magnitude })
    };
    let (clock, mut fraction, has_fraction) = match clock.split_once('.') {
        Some((clock, fraction)) => (clock, fraction, true),
        None => (clock, "", false),
    };
    let (hour, minute, second) = match clock.split_once(':') {
        Some(_) => {
            if !clock
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte == b':')
            {
                return None;
            }
            let mut parts = clock.split(':');
            let hour: i64 = parts.next()?.parse().ok()?;
            let minute: i64 = parts.next()?.parse().ok()?;
            let second: i64 = parts.next()?.parse().ok()?;
            if parts.next().is_some() {
                return None;
            }
            (hour, minute, second)
        }
        None => {
            if clock.len() != 6 || !clock.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            (
                clock[0..2].parse().ok()?,
                clock[2..4].parse().ok()?,
                clock[4..6].parse().ok()?,
            )
        }
    };
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return None;
    }
    if has_fraction && (fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }
    if !has_fraction {
        fraction = "0";
    }
    let mut nanoseconds = 0_u32;
    for digit in fraction.bytes().take(9) {
        nanoseconds = nanoseconds * 10 + u32::from(digit - b'0');
    }
    if fraction.len() > 9 && fraction.bytes().skip(9).any(|digit| digit != b'0') {
        return None;
    }
    for _ in fraction.len().min(9)..9 {
        nanoseconds *= 10;
    }
    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(hour.checked_mul(3_600)?)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)?
        .checked_sub(i64::from(offset_minutes) * 60)?;
    Some(SearchInstant {
        unix_seconds: seconds,
        nanosecond: nanoseconds,
    })
}

fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let year = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = year.div_euclid(400);
    let era_years = era.checked_mul(400)?;
    let year_of_era = year.checked_sub(era_years)?;
    let shifted_month = (month + 9) % 12;
    let month_start = 153_u32
        .checked_mul(shifted_month)?
        .checked_add(2)?
        .checked_div(5)?;
    let day_of_year = i64::from(month_start.checked_add(day.checked_sub(1)?)?);
    let day_of_era = year_of_era
        .checked_mul(365)?
        .checked_add(year_of_era / 4)?
        .checked_sub(year_of_era / 100)?
        .checked_add(day_of_year)?;
    era.checked_mul(146_097)?
        .checked_add(day_of_era)?
        .checked_sub(719_468)
}

/// Resolve the CLI's compact duration syntax against the caller's injected
/// application clock. Absolute timestamps are handled by `parse_search_instant`.
pub fn parse_relative_search_instant(value: &str, now_ms: i64) -> Option<SearchInstant> {
    let value = value.trim();
    let (digits, unit) = value.split_at_checked(value.len().saturating_sub(1))?;
    let amount: i64 = digits.parse().ok()?;
    if amount <= 0 {
        return None;
    }
    let multiplier = match unit {
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return None,
    };
    let delta = amount.checked_mul(multiplier)?;
    Some(SearchInstant::from_unix_millis(now_ms.checked_sub(delta)?))
}

/// Cursor 绑定的查询摘要：任何会改变结果集或其顺序的输入都必须进这个摘要，
/// 否则换了输入的续页请求会静默按错误的 offset 切片。
///
/// 除 query/filters/facets/include_system/group_by_session 外，`current_repo`
/// （调用方当前工作目录派生的 repo slug，见 [`ranking::CURRENT_REPO_SCORE_BOOST`]）
/// 同样入摘要：它只改排序不改召回，但换了仓库就是另一个排序，跨仓库复用 cursor
/// 必须显式失败而不是静默错页。检索模式、模型、向量、排序版本同样绑定；
/// 旧版未绑定这些状态的 search cursor 显式失效。
struct RetrievalBinding<'a> {
    requested: RetrievalMode,
    effective: RetrievalMode,
    model: Option<String>,
    embedding: Option<&'a [f32]>,
}

fn search_query_digest(
    query: &str,
    filters: &SearchFilters,
    facets: &SearchFacets,
    include_system: bool,
    group_by_session: bool,
    current_repo: Option<&str>,
    retrieval: &RetrievalBinding<'_>,
) -> String {
    let embedding_digest = retrieval.embedding.map(|values| {
        let mut hasher = blake3::Hasher::new();
        for value in values {
            hasher.update(&value.to_le_bytes());
        }
        hasher.finalize().to_hex().to_string()
    });
    // Structured encoding prevents delimiter collisions in untrusted strings.
    // Reject cursors issued before ranking used the adapter-matched owner.
    cursor::digest_query(&serde_json::json!({
        "version": "search-v2-rrf60-signals-v3-matched-owner",
        "result_set": "search",
        "query": query,
        "providers": filters.providers.iter().map(|provider| provider.as_str()).collect::<Vec<_>>(),
        "since": filters.since.map(|i| (i.unix_seconds, i.nanosecond)),
        "until": filters.until.map(|i| (i.unix_seconds, i.nanosecond)),
        "repo": filters.repo,
        "facets": {
            "sidechain": facets.sidechain.as_str(),
            "tool_kind": facets.tool_kind,
            "tool_name": facets.tool_name,
        },
        "include_system": include_system,
        "group_by_session": group_by_session,
        "current_repo": current_repo,
        "requested_mode": retrieval.requested.as_str(),
        "effective_mode": retrieval.effective.as_str(),
        "model": retrieval.model,
        "dimension": retrieval.embedding.map(<[f32]>::len),
        "embedding": embedding_digest,
        "rank_window": RANK_SCAN_WINDOW,
        "group_factor": GROUP_SCAN_FACTOR,
    }).to_string())
}

/// R2 系统噪声判定：canonical message payload 的 `role` 字段为 system 或
/// developer（Codex 的 system/permission 层角色）即视为系统上下文。compaction
/// summary 在 parse 期已跳过（不产生消息）；AGENTS.md/skills/system prompt 以
/// 请求记录的 `system` 数组形式存在而非消息实体，故无需额外标记。payload 非
/// JSON 或无 role 字段（legacy）一律不判为噪声。
fn payload_role_is_system_noise(payload: Option<&[u8]>) -> bool {
    payload
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
        .and_then(|value| {
            value
                .get("role")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .is_some_and(|role| role == "system" || role == "developer")
}

/// 装配一条检索命中（R1/ADR-0008 + guidance）：从 payload 解析 `text` 摘要
/// （有字面证据取命中窗口，否则回退前缀，见 [`snippet::build`]）、填充归属
/// 会话、派生确定性 `why_matched` 与 `suggested_next_commands`。
/// payload 无 text（或非 JSON）→ text None；无 placement → session_id None。
fn assemble_search_hit(
    hit: &mut SearchHit,
    payload: Option<&[u8]>,
    session: Option<&StableId>,
    max_snippet_chars: usize,
    query_terms: &[String],
) {
    let full_text = payload.and_then(|bytes| {
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
            return None;
        };
        value
            .get("text")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    });
    // 证据装配期间全量 payload 仍可用：除规范 `text` 字段外，把整棵 JSON 值
    // 交给 guidance（string-leaves 源覆盖 Codex content blocks 等无顶层 text
    // 的 payload），显示摘要作最后一个兜底源（可能是截断窗口，会漏掉窗口之外
    // 的真实命中——guidance design §2）。
    let payload_value =
        payload.and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok());
    hit.text = snippet::build(full_text.as_deref(), query_terms, max_snippet_chars);
    // 保留 adapter 提供的 canonical session_id（Session 元数据命中自带归属
    // 会话）；否则才用 placement 解析的归属会话回填。metadata-only Session
    // 命中无 placement，session_of 返回 None，不得把既有值覆盖成 None。
    if hit.session_id.is_none() {
        hit.session_id = session.map(|s| s.as_str().to_string());
    }
    hit.why_matched = guidance::why_matched(
        query_terms,
        full_text.as_deref(),
        None,
        payload_value.as_ref(),
        hit.text.as_deref(),
    );
    hit.suggested_next_commands = guidance::suggested_next_commands(hit);
}

/// 检索命中在 `max_response_bytes` 闸内的序列化字节估算（与 CLI 渲染对齐）：
/// id + session_id + text + guidance；`occurrences` 仅当 >1（归并模式）时计入，
/// 与序列化器"occurrences == 1 时省略该键"的约定一致。
///
/// `session_id` 与 `text` 是**恒发**字段（schema 承诺键不消失，缺值渲染为
/// `null`），因此缺值也要计费——按 `null` 的 4 字节计。空集合的 guidance 字段
/// 才是真正省略整个键的追加字段，缺值记 0。
fn search_hit_charge(hit: &SearchHit) -> usize {
    /// `null` 字面量的序列化长度（恒发字段缺值时的实际字节）。
    const NULL_LEN: usize = 4;
    let why_matched_len = if hit.why_matched.is_empty() {
        0
    } else {
        hit.why_matched
            .iter()
            .map(|value| json_string_len(value))
            .sum::<usize>()
            + hit.why_matched.len()
            + 15
    };
    let suggested_len = if hit.suggested_next_commands.is_empty() {
        0
    } else {
        hit.suggested_next_commands
            .iter()
            .map(|value| json_string_len(value))
            .sum::<usize>()
            + hit.suggested_next_commands.len()
            + 27
    };
    let occurrences_len = if hit.occurrences > 1 {
        hit.occurrences.to_string().len() + 14
    } else {
        0
    };
    json_string_len(hit.id.as_str())
        + 14
        + hit
            .session_id
            .as_ref()
            .map_or(NULL_LEN, |s| json_string_len(s))
        + 8
        + hit.text.as_ref().map_or(NULL_LEN, |s| json_string_len(s))
        + why_matched_len
        + suggested_len
        + occurrences_len
        + RESUME_AVAILABLE_FIELD_BYTES
        + 32
}

/// 系统时钟（Unix 毫秒）。[`App::new`] 的默认时钟；测试经 [`App::with_clock`]
/// 注入固定值。CLI 组合根在 `ASG_CLOCK_MS` 未注入时回落本函数。
pub fn system_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Clone)]
struct ContextOccurrence {
    message: ContextMessage,
    evidence: EvidenceSpanDto,
    role: Role,
    source_document_id: String,
    estimated_bytes: usize,
}

struct MessageOccurrence {
    message: ContextMessage,
    estimated_bytes: usize,
    is_anchor: bool,
    payload_truncated: bool,
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::System => "system",
        Role::Developer => "developer",
        Role::Tool => "tool",
    }
}

fn json_escaped_char_len(value: char) -> usize {
    match value {
        '"' | '\\' | '\u{0008}' | '\t' | '\n' | '\u{000c}' | '\r' => 2,
        '\u{0000}'..='\u{001f}' => 6,
        _ => value.len_utf8(),
    }
}

fn project_anchor_payload(
    message: &mut ContextMessage,
    role: Role,
    full_text: &str,
    max_bytes: usize,
) -> Result<bool, budget::BudgetError> {
    if context_message_bytes(message) <= max_bytes {
        return Ok(false);
    }

    let preserves_other_fields = message
        .payload
        .as_object_mut()
        .and_then(|payload| payload.get_mut("text"))
        .is_some_and(|text| {
            if text.is_string() {
                *text = serde_json::Value::String(String::new());
                true
            } else {
                false
            }
        });
    if !preserves_other_fields || context_message_bytes(message) > max_bytes {
        message.payload = serde_json::json!({
            "role": role_name(role),
            "text": "",
        });
    }

    let base_bytes = context_message_bytes(message);
    if base_bytes > max_bytes {
        return Err(budget::BudgetError::TooSmall(
            "max_response_bytes cannot fit the message anchor identity".into(),
        ));
    }

    let mut text = String::new();
    let mut remaining = max_bytes - base_bytes;
    for value in full_text.chars() {
        let escaped_len = json_escaped_char_len(value);
        if escaped_len > remaining {
            break;
        }
        text.push(value);
        remaining -= escaped_len;
    }
    message
        .payload
        .as_object_mut()
        .expect("projected message payload is an object")
        .insert("text".into(), serde_json::Value::String(text));
    debug_assert!(context_message_bytes(message) <= max_bytes);
    Ok(true)
}

fn structural_metadata_reserve(session_id: &StableId, requested_level: ContextLevel) -> usize {
    match requested_level {
        ContextLevel::Raw => 0,
        ContextLevel::Talks | ContextLevel::Sessions => {
            STRUCTURAL_METADATA_RESERVE_BYTES.saturating_add(session_id.as_str().len())
        }
    }
}

fn occurrence_response_bytes(
    occurrence: &ContextOccurrence,
    requested_level: ContextLevel,
    has_user_anchor: &mut bool,
    references: &mut BTreeSet<String>,
) -> usize {
    let derived_copy = match requested_level {
        ContextLevel::Raw => false,
        ContextLevel::Talks => {
            if occurrence.role == Role::User {
                *has_user_anchor = true;
                true
            } else {
                *has_user_anchor && matches!(occurrence.role, Role::Assistant | Role::Tool)
            }
        }
        ContextLevel::Sessions => occurrence.role == Role::User && !*has_user_anchor,
    };
    if occurrence.role == Role::User {
        *has_user_anchor = true;
    }
    occurrence
        .estimated_bytes
        .saturating_add(if derived_copy {
            occurrence.estimated_bytes
        } else {
            0
        })
        .saturating_add(
            if requested_level == ContextLevel::Sessions
                && references.insert(occurrence.source_document_id.clone())
            {
                occurrence.source_document_id.len().saturating_add(4)
            } else {
                0
            },
        )
}

fn clamp_context_occurrences(
    occurrences: Vec<ContextOccurrence>,
    level: ContextLevel,
    max_messages: usize,
    max_bytes: usize,
) -> (Vec<ContextOccurrence>, Truncation, usize) {
    let mut has_user_anchor = false;
    let mut references = BTreeSet::new();
    let costed = occurrences
        .into_iter()
        .map(|occurrence| {
            let cost = occurrence_response_bytes(
                &occurrence,
                level,
                &mut has_user_anchor,
                &mut references,
            );
            (occurrence, cost)
        })
        .collect();
    let (kept, mut truncation, consumed) =
        budget::clamp_items(costed, max_messages, max_bytes, |(_, cost)| *cost);
    if truncation.reason.as_deref() == Some(budget::TRUNCATION_MAX_ITEMS) {
        truncation.reason = Some(budget::TRUNCATION_MAX_MESSAGES.to_string());
    }
    (
        kept.into_iter().map(|(occurrence, _)| occurrence).collect(),
        truncation,
        consumed,
    )
}

fn talks_of(occurrences: &[&ContextOccurrence]) -> Vec<ContextTalk> {
    let mut talks = Vec::new();
    for occurrence in occurrences {
        if occurrence.role == Role::User {
            talks.push(ContextTalk {
                user_message: occurrence.message.clone(),
                following_messages: Vec::new(),
            });
        } else if matches!(occurrence.role, Role::Assistant | Role::Tool)
            && let Some(talk) = talks.last_mut()
        {
            talk.following_messages.push(occurrence.message.clone());
        }
    }
    talks
}

fn summary_of(occurrences: &[&ContextOccurrence]) -> ContextSessionSummary {
    let first_user_message = occurrences
        .iter()
        .find(|occurrence| occurrence.role == Role::User)
        .map(|occurrence| occurrence.message.clone());
    let turn_count = occurrences
        .iter()
        .filter(|occurrence| occurrence.role == Role::User)
        .count();
    let mut file_references = Vec::new();
    for occurrence in occurrences {
        if !file_references.contains(&occurrence.source_document_id) {
            file_references.push(occurrence.source_document_id.clone());
        }
    }
    ContextSessionSummary {
        first_user_message,
        message_count: occurrences.len(),
        turn_count,
        file_references,
    }
}

fn available_level(
    requested_level: ContextLevel,
    occurrences: &[ContextOccurrence],
) -> ContextLevel {
    match requested_level {
        ContextLevel::Raw => ContextLevel::Raw,
        ContextLevel::Talks | ContextLevel::Sessions
            if occurrences
                .iter()
                .any(|occurrence| occurrence.role == Role::User) =>
        {
            requested_level
        }
        ContextLevel::Talks | ContextLevel::Sessions => ContextLevel::Raw,
    }
}

fn assemble_level(
    requested_level: ContextLevel,
    kept_occurrences: &[ContextOccurrence],
) -> (
    ContextLevel,
    Vec<ContextTalk>,
    Option<ContextSessionSummary>,
) {
    let occurrences: Vec<&ContextOccurrence> = kept_occurrences.iter().collect();
    match requested_level {
        ContextLevel::Raw => (ContextLevel::Raw, Vec::new(), None),
        ContextLevel::Talks => {
            let talks = talks_of(&occurrences);
            if talks.is_empty() {
                (ContextLevel::Raw, Vec::new(), None)
            } else {
                (ContextLevel::Talks, talks, None)
            }
        }
        ContextLevel::Sessions => {
            let talks = talks_of(&occurrences);
            if talks.is_empty() {
                (ContextLevel::Raw, Vec::new(), None)
            } else {
                (
                    ContextLevel::Sessions,
                    Vec::new(),
                    Some(summary_of(&occurrences)),
                )
            }
        }
    }
}

fn build_hint(
    session_id: &StableId,
    requested_level: ContextLevel,
    effective_level: ContextLevel,
) -> Option<ContextHint> {
    let next = match (requested_level, effective_level) {
        (ContextLevel::Sessions, ContextLevel::Sessions) => ContextLevel::Talks,
        (ContextLevel::Sessions, ContextLevel::Talks)
        | (ContextLevel::Talks, ContextLevel::Talks) => ContextLevel::Raw,
        _ => return None,
    };
    Some(ContextHint {
        command: "get_session_context".to_string(),
        session_id: session_id.as_str().to_string(),
        level: next,
    })
}

fn structural_fields_bytes(
    talks: &[ContextTalk],
    summary: &Option<ContextSessionSummary>,
    hint: &Option<ContextHint>,
) -> usize {
    let mut bytes = 128usize;
    if let Some(hint) = hint {
        bytes = bytes
            .saturating_add(hint.command.len())
            .saturating_add(hint.session_id.len())
            .saturating_add(hint.level.as_str().len())
            .saturating_add(48);
    }
    for talk in talks {
        bytes = bytes
            .saturating_add(context_message_bytes(&talk.user_message))
            .saturating_add(32);
        for message in &talk.following_messages {
            bytes = bytes
                .saturating_add(context_message_bytes(message))
                .saturating_add(8);
        }
    }
    if let Some(summary) = summary {
        bytes = bytes.saturating_add(96);
        if let Some(message) = &summary.first_user_message {
            bytes = bytes.saturating_add(context_message_bytes(message));
        }
        for reference in &summary.file_references {
            bytes = bytes.saturating_add(reference.len()).saturating_add(4);
        }
    }
    bytes
}

fn context_message_bytes(message: &ContextMessage) -> usize {
    message
        .id
        .len()
        .saturating_add(message.placement_id.len())
        .saturating_add(message.message_id.len())
        .saturating_add(message.payload.to_string().len())
        .saturating_add(48)
}

fn message_occurrence_bytes(message: &ContextMessage) -> usize {
    context_message_bytes(message).saturating_add(48)
}

fn clamp_message_window(
    occurrences: Vec<MessageOccurrence>,
    budget: &ResponseBudget,
    max_bytes: usize,
) -> (Vec<MessageOccurrence>, Truncation) {
    let total = occurrences.len();
    let anchor_index = occurrences
        .iter()
        .position(|occurrence| occurrence.is_anchor)
        .expect("message windows always contain their anchor");
    let anchor_cost = occurrences[anchor_index].estimated_bytes;
    let anchor_payload_truncated = occurrences[anchor_index].payload_truncated;
    let mut start = anchor_index;
    let mut end = anchor_index + 1;
    let mut consumed = anchor_cost;
    let mut byte_limited = anchor_payload_truncated || anchor_cost > max_bytes;
    let mut prefer_left = true;

    if !byte_limited {
        loop {
            let left = start.checked_sub(1);
            let right = (end < total).then_some(end);
            if left.is_none() && right.is_none() {
                break;
            }
            if end - start >= budget.max_items {
                break;
            }
            let next = match (left, right) {
                (Some(left), Some(right)) => {
                    let next = if prefer_left { left } else { right };
                    prefer_left = !prefer_left;
                    next
                }
                (Some(left), None) => left,
                (None, Some(right)) => right,
                (None, None) => unreachable!(),
            };
            let cost = occurrences[next].estimated_bytes;
            if consumed.saturating_add(cost) > max_bytes {
                byte_limited = true;
                break;
            }
            consumed += cost;
            if next < start {
                start = next;
            } else {
                end = next + 1;
            }
        }
    }

    let kept_count = end - start;
    let truncated = kept_count < total || anchor_payload_truncated || anchor_cost > max_bytes;
    let reason = if byte_limited {
        Some(budget::TRUNCATION_MAX_RESPONSE_BYTES.to_string())
    } else if kept_count < total {
        Some(budget::TRUNCATION_MAX_ITEMS.to_string())
    } else {
        None
    };
    let kept = occurrences
        .into_iter()
        .skip(start)
        .take(kept_count)
        .collect();
    (kept, Truncation { truncated, reason })
}

/// 用例执行器：绑定所需端口，串起领域校验与端口调用。
///
/// 泛型而非 trait object——前端在构造期决定后端实现，零动态分发开销。
/// 时钟以 fn 指针注入：cursor 的发行/校验都不读环境时间（可测确定性）。
pub struct App<
    C: CatalogStore + ContextGraphStore,
    S: SearchIndex,
    R: ResumeClaimsStore = NoResumeClaims,
    M: SemanticIndex = NoSemanticIndex,
> {
    catalog: C,
    index: S,
    resume: R,
    semantic: M,
    clock_ms: fn() -> i64,
    /// 调用方当前工作目录派生的 repo slug（`host/owner/name`），用于
    /// [`ranking::CURRENT_REPO_SCORE_BOOST`] 的当前仓库偏好。
    ///
    /// `None` = 该入口没有可派生的仓库身份（不在 git 工作树内、无 origin、
    /// 或该面本身没有 cwd 语义如 MCP/Web）——此时该信号恒不动分，且不产生
    /// 任何额外读取。组合根用 [`Self::with_current_repo`] 显式注入，既有
    /// 构造器一律为 `None`，调用点零改动。
    current_repo: Option<String>,
}

impl<C: CatalogStore + ContextGraphStore, S: SearchIndex, R: ResumeClaimsStore>
    App<C, S, R, NoSemanticIndex>
{
    /// 绑定 Resume 声明存储的构造器（ADR-0009）：生产路径（CLI/Robot/MCP）
    /// 用同一 SqliteStore 实例填充 catalog/index/resume 三个槽；semantic 为
    /// 占位 `NoSemanticIndex`（semantic 请求显式降级 lexical_fallback）。
    pub fn with_resume(catalog: C, index: S, resume: R) -> Self {
        Self {
            catalog,
            index,
            resume,
            semantic: NoSemanticIndex,
            clock_ms: system_now_ms,
            current_repo: None,
        }
    }

    /// 固定时钟 + Resume 声明存储（测试用）。
    pub fn with_resume_and_clock(catalog: C, index: S, resume: R, clock_ms: fn() -> i64) -> Self {
        Self {
            catalog,
            index,
            resume,
            semantic: NoSemanticIndex,
            clock_ms,
            current_repo: None,
        }
    }
}

/// 注入语义索引的构造器（#3）。
impl<C: CatalogStore + ContextGraphStore, S: SearchIndex, R: ResumeClaimsStore, M: SemanticIndex>
    App<C, S, R, M>
{
    pub fn with_resume_semantic(catalog: C, index: S, resume: R, semantic: M) -> Self {
        Self {
            catalog,
            index,
            resume,
            semantic,
            clock_ms: system_now_ms,
            current_repo: None,
        }
    }

    pub fn with_resume_semantic_and_clock(
        catalog: C,
        index: S,
        resume: R,
        semantic: M,
        clock_ms: fn() -> i64,
    ) -> Self {
        Self {
            catalog,
            index,
            resume,
            semantic,
            clock_ms,
            current_repo: None,
        }
    }
}

/// `NoResumeClaims` 固定槽的构造器：函数级泛型默认参数不生效（Rust 限制），
/// 把这些不携带 Resume 实现的构造器放进专属 impl，既有 `App::new` /
/// `App::with_clock` 调用点零改动继续编译。
impl<C: CatalogStore + ContextGraphStore, S: SearchIndex>
    App<C, S, NoResumeClaims, NoSemanticIndex>
{
    pub fn new(catalog: C, index: S) -> Self {
        Self {
            catalog,
            index,
            resume: NoResumeClaims,
            semantic: NoSemanticIndex,
            clock_ms: system_now_ms,
            current_repo: None,
        }
    }

    /// 注入固定时钟的构造器（测试用）；生产路径一律 [`App::new`]。
    pub fn with_clock(catalog: C, index: S, clock_ms: fn() -> i64) -> Self {
        Self {
            catalog,
            index,
            resume: NoResumeClaims,
            semantic: NoSemanticIndex,
            clock_ms,
            current_repo: None,
        }
    }
}

impl<C: CatalogStore + ContextGraphStore, S: SearchIndex, R: ResumeClaimsStore, M: SemanticIndex>
    App<C, S, R, M>
{
    /// Read the injected application clock. Relative search filters use this
    /// value so every frontend shares the same time source.
    pub fn now_ms(&self) -> i64 {
        (self.clock_ms)()
    }

    /// 注入调用方当前工作目录派生的 repo slug（当前仓库偏好信号，见
    /// [`ranking::CURRENT_REPO_SCORE_BOOST`]）。组合根在构造后链式调用；
    /// 传 `None` 或不调用即该信号关闭（默认），此时排序与注入前逐字节一致。
    ///
    /// 空白 slug 视为无身份（`None`）：宁可关掉信号，也不拿空串去比对。
    pub fn with_current_repo(mut self, slug: Option<String>) -> Self {
        self.current_repo = slug.filter(|value| !value.trim().is_empty());
        self
    }

    /// 解析续读偏移：无令牌即第一页（offset 0）；有令牌则完整校验
    /// （结构/摘要/过期/generation/查询与排序绑定），失败显式报错，绝不静默回第一页。
    fn resolve_offset(
        &self,
        token: Option<&str>,
        active_generation: u64,
        query_digest: &str,
        sort_digest: &str,
        result_set: Option<&str>,
    ) -> Result<u64, AppError> {
        match token {
            None => Ok(0),
            Some(t) => {
                let claims = cursor::verify(
                    t,
                    &cursor::CursorExpectations {
                        now_ms: (self.clock_ms)(),
                        active_generation,
                        query_digest: query_digest.to_string(),
                        sort_digest: sort_digest.to_string(),
                        result_set: result_set.map(str::to_string),
                    },
                )?;
                Ok(claims.offset)
            }
        }
    }

    /// 还有后续页时发行续读令牌；`offset_next` 是钉住排序内的下一读取位置。
    fn issue_cursor(
        &self,
        has_more: bool,
        generation: u64,
        query_digest: &str,
        sort_digest: &str,
        result_set: Option<&str>,
        offset_next: u64,
    ) -> Option<String> {
        if !has_more {
            return None;
        }
        let now = (self.clock_ms)();
        Some(
            cursor::issue(&cursor::CursorClaims {
                contract_major: cursor::SUPPORTED_CONTRACT_MAJOR,
                generation,
                issued_at_ms: now,
                expires_at_ms: now + cursor::DEFAULT_TTL_MS,
                query_digest: query_digest.to_string(),
                sort_digest: sort_digest.to_string(),
                result_set: result_set.map(str::to_string),
                offset: offset_next,
            })
            .into_string(),
        )
    }

    /// Resume 可用性批量装配（ADR-0009）：收集本页所有携带 `session_id` 的
    /// 命中，一次 `resume_of` 解析（分块 IN，无 N+1），按 wire id 映射回
    /// `hit.resume_available`。`session_id` 缺失或 wire 无效的命中保持 `false`
    /// ——可用性缺失绝不影响历史可检索。
    fn assemble_resume_availability(&self, hits: &mut [SearchHit]) -> PortResult<()> {
        if hits.is_empty() {
            return Ok(());
        }
        let ids: Vec<StableId> = hits
            .iter()
            .filter_map(|hit| {
                hit.session_id
                    .as_deref()
                    .and_then(StableId::from_wire)
                    .filter(|id| id.kind() == IdKind::Session)
            })
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        let metadata = self.resume.resume_of(&ids)?;
        let availability: HashMap<&str, bool> = metadata
            .iter()
            .map(|meta| (meta.session_id.as_str(), meta.resume_available))
            .collect();
        for hit in hits.iter_mut() {
            if let Some(wire) = hit.session_id.as_deref()
                && let Some(available) = availability.get(wire)
            {
                hit.resume_available = *available;
            }
        }
        Ok(())
    }

    /// 执行一个应用请求。校验错误保持 Domain 分类，端口错误保持 Port 分类，
    /// cursor/budget 错误保持各自分类（protocol 层有专属 canonical code）。
    pub fn handle(&self, req: AppRequest) -> Result<AppResponse, AppError> {
        // Every AppRequest is read-only. All data ports must share the catalog's
        // backend session; outer composition may keep a nested guard for its
        // additional evidence/display reads after this response is assembled.
        let _snapshot = self.catalog.begin_read_snapshot()?;
        match req {
            AppRequest::Search {
                query,
                mut filters,
                facets,
                limit,
                cursor: token,
                budget,
                include_system,
                group_by_session,
                mode,
                query_embedding,
            } => {
                validate_search_request(&query, &filters, limit, &budget)?;
                filters.providers.sort_unstable();
                filters.providers.dedup();
                let generation = self.catalog.active_generation()?;
                if mode != RetrievalMode::Lexical
                    && query_embedding.as_deref().is_some_and(|values| {
                        values.is_empty() || values.iter().any(|value| !value.is_finite())
                    })
                {
                    return Err(DomainError::InvalidRequest(
                        "query embedding must contain finite values and not be empty".into(),
                    )
                    .into());
                }
                let semantic_ready = match (mode, query_embedding.as_ref()) {
                    (RetrievalMode::Lexical, _) | (_, None) => false,
                    (_, Some(embedding)) => self.semantic.is_ready(embedding.len())?,
                };
                let response_mode = if mode == RetrievalMode::Lexical || semantic_ready {
                    mode
                } else {
                    RetrievalMode::LexicalFallback
                };
                let retrieval = RetrievalBinding {
                    requested: mode,
                    effective: response_mode,
                    model: if mode == RetrievalMode::Lexical {
                        None
                    } else {
                        self.semantic.semantic_model_id()?
                    },
                    // Fallback changes the execution path, not the identity
                    // of the requested query vector used by cursor validation.
                    embedding: if mode != RetrievalMode::Lexical {
                        query_embedding.as_deref()
                    } else {
                        None
                    },
                };
                let query_digest = search_query_digest(
                    &query,
                    &filters,
                    &facets,
                    include_system,
                    group_by_session,
                    self.current_repo.as_deref(),
                    &retrieval,
                );
                let now = self.now_ms();
                let search_claims = match token.as_deref() {
                    Some(token) => cursor::verify(
                        token,
                        &cursor::CursorExpectations {
                            now_ms: now,
                            active_generation: generation,
                            query_digest: query_digest.clone(),
                            sort_digest: SORT_SCORE_DESC.into(),
                            result_set: None,
                        },
                    )?,
                    None => cursor::CursorClaims {
                        contract_major: cursor::SUPPORTED_CONTRACT_MAJOR,
                        generation,
                        issued_at_ms: now,
                        expires_at_ms: now.saturating_add(cursor::DEFAULT_TTL_MS),
                        query_digest: query_digest.clone(),
                        sort_digest: SORT_SCORE_DESC.into(),
                        result_set: None,
                        offset: 0,
                    },
                };
                let offset = search_claims.offset;
                let ranking_time = search_claims.issued_at_ms;
                // Recency is part of the pinned order. Continuations retain
                // the initial query clock and expiry; only the offset changes.
                let issue_search_cursor = |has_more: bool, offset: u64| {
                    has_more.then(|| {
                        cursor::issue(&cursor::CursorClaims {
                            offset,
                            ..search_claims.clone()
                        })
                        .into_string()
                    })
                };

                // 分页模型：钉住排序（重排后的 final desc + id tiebreak 全序）
                // 内的 offset 续读。端口无 offset 参数，因此在应用层超取后切片。
                //
                // **应用层重排**路径的取数窗口必须与 offset 无关（见
                // RANK_SCAN_WINDOW）：重排后的钉住排序是"同一窗口上的全序"，
                // 窗口随 offset 增长会让每页在不同集合上重排，拼接结果重复页尾
                // 命中并漏掉真正的高分命中。重排有两种：lexical rank signals
                // （[`ranking::apply_lexical_signals`]）与 hybrid 的 RRF 融合
                // （[`hybrid::fuse`]）——后者尤其危险，两路都命中的文档拿到两份
                // `1/(k+rank)` 加分，可以一举超过任何单路命中，窗口一放大就整体
                // 顶到榜首。只有 semantic 单路不重排（顺序由检索侧给定且是稳定
                // 前缀），保持 offset+page+1 超取，+1 作 has_more 哨兵。
                //
                // grouped 模式把窗口放大 GROUP_SCAN_FACTOR 倍（仍封顶），让
                // occurrences 覆盖更有意义的同会话命中样本。
                let page = limit.min(budget.max_items);
                let will_rank = mode == RetrievalMode::Lexical || !semantic_ready;
                // 融合也是重排：窗口必须与 offset 无关，与 rank 路径同一口径。
                let reorders_window =
                    will_rank || mode == RetrievalMode::Hybrid || group_by_session;
                let fetch = if reorders_window {
                    RANK_SCAN_WINDOW
                } else {
                    offset
                        .saturating_add(page as u64)
                        .saturating_add(1)
                        .min(MAX_FETCH_WINDOW)
                };
                let scan = if group_by_session {
                    fetch
                        .saturating_mul(GROUP_SCAN_FACTOR)
                        .min(MAX_FETCH_WINDOW)
                } else {
                    fetch
                };
                // 检索模式（#3）：semantic/hybrid 需要已就绪的语义索引与调用方
                // 提供的查询向量；任一缺失时显式降级为 lexical_fallback + warning
                // （PRD Q54：禁止静默切换）。
                let (mut scanned, fallback_warning) = if mode == RetrievalMode::Lexical {
                    (
                        self.index.query_with_policy(
                            SearchQuery {
                                text: &query,
                                filters: &filters,
                            },
                            scan as usize,
                            &facets,
                            include_system,
                        )?,
                        None,
                    )
                } else if semantic_ready && let Some(query_embedding) = query_embedding.as_deref() {
                    let semantic_hits = self.semantic.query_semantic_filtered(
                        query_embedding,
                        scan as usize,
                        &filters,
                        &facets,
                        include_system,
                    )?;
                    if semantic_hits.iter().any(|hit| !hit.score.is_finite()) {
                        return Err(PortError::Backend(
                            "semantic index returned a non-finite score".into(),
                        )
                        .into());
                    }
                    if mode == RetrievalMode::Semantic {
                        (semantic_hits, None)
                    } else {
                        let lexical_hits = self.index.query_with_policy(
                            SearchQuery {
                                text: &query,
                                filters: &filters,
                            },
                            scan as usize,
                            &facets,
                            include_system,
                        )?;
                        (hybrid::fuse(&lexical_hits, &semantic_hits), None)
                    }
                } else {
                    let warning = Some(format!(
                        "semantic search unavailable (mode {}); fell back to lexical",
                        mode.as_str()
                    ));
                    (
                        self.index.query_with_policy(
                            SearchQuery {
                                text: &query,
                                filters: &filters,
                            },
                            scan as usize,
                            &facets,
                            include_system,
                        )?,
                        warning,
                    )
                };

                // 生效检索模式（#3）：请求 semantic/hybrid 而索引未就绪 → 降级
                // lexical_fallback，warning 已随 fallback_warning 返回。
                let response_mode = if fallback_warning.is_some() {
                    RetrievalMode::LexicalFallback
                } else {
                    mode
                };
                let response_warning = fallback_warning.clone();

                // 检索侧返回满窗 ⇒ 窗口之外可能还有命中。必须在 R2 噪声过滤
                // **之前**记录：窗口随 offset 增长的路径（semantic）以末尾 `+1`
                // 作 has_more 哨兵，而过滤发生在取数之后，窗口内一条
                // system/developer 命中就会把哨兵吃掉，让后续命中变成不可达
                // （has_more=false 却确有下一页）。固定窗口的重排路径不适用：
                // 那里的窗口就是排序视界，越界以 has_more=false 诚实终止。
                let window_truncated_by_fetch = !reorders_window && scanned.len() as u64 >= scan;

                // Rank signals（competitor-borrowings #1）：纯 lexical 命中（含
                // semantic/hybrid 未就绪时的 lexical_fallback）在分页钉住排序前
                // 重算最终分并重排；semantic 命中与 hybrid RRF 融合排序不动
                // （README 明示）。`will_rank` 在取数前已按同一谓词判定——
                // 它同时决定了扫描窗口形态（见上方分页模型注释），两处必须
                // 同源。时效与 sidechain 事实来自整窗 payload——
                // 与 R2 系统噪声过滤共用同一次批量 get_many，无额外 N+1。
                let rank_lexical = will_rank;
                let mut window_payloads = if rank_lexical || !include_system {
                    let scanned_ids: Vec<StableId> =
                        scanned.iter().map(|hit| hit.id.clone()).collect();
                    Some(self.catalog.get_many(&scanned_ids)?)
                } else {
                    None
                };

                // R2 系统噪声默认排除：role=system/developer 的命中不进入结果，
                // `include_system` 显式恢复。过滤先于 offset 切片，cursor 位置因此
                // 指向"非系统"序列。判定需整窗 payload（分块批量取，无 N+1）；扫描
                // 窗内系统噪声饱和时可能提前终止分页（边界行为，见 GROUP_SCAN_FACTOR）。
                // 过滤发生在任何重排之前，hit 与 payload 始终一一配对。
                if !include_system {
                    scanned = scanned
                        .into_iter()
                        .zip(window_payloads.take().expect("payloads fetched above"))
                        .filter(|(_, payload)| !payload_role_is_system_noise(payload.1.as_deref()))
                        .map(|(hit, _)| hit)
                        .collect();
                }

                // 重算最终分并重排为 (final desc, id asc)：payload 与仓库事实以
                // (hit, payload, in_current_repo) 三元组进入评分函数，排序发生在
                // 配对之后，结构上排除错位。payload 被噪声过滤消耗后按需补取一次
                // （同窗批量，无 N+1）。仓库事实只在调用方确有当前仓库身份时才
                // 读取（两次同窗批量：message→session、session→slug）；无身份时
                // 该项恒 false 且不产生任何额外读取。
                if rank_lexical {
                    let payloads = match window_payloads.take() {
                        Some(payloads) => payloads,
                        None => {
                            let scanned_ids: Vec<StableId> =
                                scanned.iter().map(|hit| hit.id.clone()).collect();
                            self.catalog.get_many(&scanned_ids)?
                        }
                    };
                    let in_current_repo: Vec<bool> = match self.current_repo.as_deref() {
                        Some(current) => {
                            let scanned_ids: Vec<StableId> =
                                scanned.iter().map(|hit| hit.id.clone()).collect();
                            let sessions = self.catalog.session_of(&scanned_ids)?;
                            let session_ids: Vec<StableId> = sessions
                                .iter()
                                .zip(&scanned)
                                .map(|((_message_id, session), hit)| {
                                    // Match display ownership: only legacy hits
                                    // without an owner use the unfiltered lookup.
                                    match hit.session_id.as_deref() {
                                        Some(owner) => StableId::from_wire(owner),
                                        None => session.clone(),
                                    }
                                })
                                .map(|session| {
                                    session.unwrap_or_else(|| {
                                        StableId::native(
                                            agent_session_grep_domain::IdKind::Session,
                                            "",
                                        )
                                    })
                                })
                                .collect();
                            let slugs = self.catalog.session_repo_slugs(&session_ids)?;
                            slugs
                                .into_iter()
                                .map(|slug| slug.as_deref() == Some(current))
                                .collect()
                        }
                        None => vec![false; scanned.len()],
                    };
                    scanned = ranking::apply_lexical_signals(
                        scanned
                            .into_iter()
                            .zip(payloads)
                            .zip(in_current_repo)
                            .map(|((hit, (_id, payload)), in_repo)| (hit, payload, in_repo))
                            .collect(),
                        ranking_time,
                    );
                }

                // R3 按会话归并：整窗装配后每会话只保留最高分命中（钉住顺序中的
                // 首个），occurrences 为该会话在扫描窗内的命中数；offset 语义为会话
                // 组偏移。非归并路径保持逐命中分页不变。
                if group_by_session {
                    let ids: Vec<StableId> = scanned.iter().map(|hit| hit.id.clone()).collect();
                    let payloads = self.catalog.get_many(&ids)?;
                    let sessions = self.catalog.session_of(&ids)?;
                    let max_snippet_chars = budget.max_snippet_chars;
                    let query_terms = guidance::literal_terms(&query);
                    let mut assembled = scanned;
                    for (hit, ((_id, payload), (_mid, session))) in
                        assembled.iter_mut().zip(payloads.into_iter().zip(sessions))
                    {
                        assemble_search_hit(
                            hit,
                            payload.as_deref(),
                            session.as_ref(),
                            max_snippet_chars,
                            &query_terms,
                        );
                    }
                    let mut grouped: Vec<SearchHit> = Vec::new();
                    let mut group_index: HashMap<String, usize> = HashMap::new();
                    for hit in assembled {
                        let key = hit
                            .session_id
                            .clone()
                            .unwrap_or_else(|| hit.id.as_str().to_string());
                        match group_index.get(&key).copied() {
                            Some(index) => grouped[index].occurrences += 1,
                            None => {
                                group_index.insert(key, grouped.len());
                                grouped.push(hit);
                            }
                        }
                    }
                    let grouped_len = grouped.len() as u64;
                    let slice: Vec<SearchHit> = grouped
                        .into_iter()
                        .skip(usize::try_from(offset).unwrap_or(usize::MAX))
                        .take(page)
                        .collect();
                    let net_bytes = budget
                        .max_response_bytes
                        .saturating_sub(ENVELOPE_RESERVE_BYTES);
                    let (mut hits, truncation, _) =
                        budget::clamp_items(slice, page, net_bytes, search_hit_charge);
                    // Resume 可用性（ADR-0009）：只对保留的命中批量解析一次（无 N+1）。
                    self.assemble_resume_availability(&mut hits)?;
                    let consumed = offset + hits.len() as u64;
                    // has_more 必须按**实际消耗**判定，与逐命中/List 分支同一口径
                    // （`总数 > consumed`）：字节 clamp 削短本页时被削掉的组仍在
                    // 后续页可达。若按"是否存在第 page+1 组"判定，组总数不超过页
                    // 大小时会报 has_more=false，被削掉的组从此不可达（静默漏结果）。
                    // `window_truncated_by_fetch` 补上归并对计数的压缩：整窗命中
                    // 挤在少数会话里时组数会低于消耗量，但更大的窗口仍有新组。
                    // `!hits.is_empty()` 守卫首组即超预算的情形：cursor 会停在原
                    // offset，此时必须终止分页而非死循环。
                    let has_more =
                        (grouped_len > consumed || window_truncated_by_fetch) && !hits.is_empty();
                    let next_cursor = issue_search_cursor(has_more, consumed);
                    return Ok(AppResponse::Search {
                        hits,
                        next_cursor,
                        generation,
                        truncation,
                        retrieval_mode: response_mode,
                        fallback_warning: response_warning,
                    });
                }

                let scanned_len = scanned.len() as u64;
                let slice: Vec<SearchHit> = scanned
                    .into_iter()
                    .skip(usize::try_from(offset).unwrap_or(usize::MAX))
                    .take(page)
                    .collect();

                // R1/ADR-0008 装配：对页内命中一次性批量取 payload（分块 IN，
                // 无 N+1），解析 `text` 字段按 `max_snippet_chars` 构建命中窗口
                // （无字面证据回退前缀，见 [`snippet::build`]）；再一次性批量
                // 解析归属会话（session_of，同序）。payload 无 text（或非 JSON）
                // → text None，不臆造正文；无 placement → session_id None。
                // 不做任何脱敏（ADR-0004 所有者决定，本地优先工具接受屏显）。
                let ids: Vec<StableId> = slice.iter().map(|hit| hit.id.clone()).collect();
                let payloads = self.catalog.get_many(&ids)?;
                let sessions = self.catalog.session_of(&ids)?;
                let max_snippet_chars = budget.max_snippet_chars;
                let query_terms = guidance::literal_terms(&query);
                let mut hits = slice;
                for (hit, ((_id, payload), (_mid, session))) in
                    hits.iter_mut().zip(payloads.into_iter().zip(sessions))
                {
                    assemble_search_hit(
                        hit,
                        payload.as_deref(),
                        session.as_ref(),
                        max_snippet_chars,
                        &query_terms,
                    );
                }
                let net_bytes = budget
                    .max_response_bytes
                    .saturating_sub(ENVELOPE_RESERVE_BYTES);
                let (mut hits, truncation, _) =
                    budget::clamp_items(hits, page, net_bytes, search_hit_charge);
                // Resume 可用性（ADR-0009）：只对保留的命中批量解析一次（无 N+1）。
                self.assemble_resume_availability(&mut hits)?;
                let consumed = offset + hits.len() as u64;
                // A truncated page with zero kept hits cannot advance the
                // cursor offset; terminate paging instead of looping forever.
                // `window_truncated_by_fetch` covers the offset-dependent
                // window whose `+1` sentinel the noise filter can consume.
                let has_more =
                    (scanned_len > consumed || window_truncated_by_fetch) && !hits.is_empty();
                let next_cursor = issue_search_cursor(has_more, consumed);
                Ok(AppResponse::Search {
                    hits,
                    next_cursor,
                    generation,
                    truncation,
                    retrieval_mode: response_mode,
                    fallback_warning: response_warning,
                })
            }
            AppRequest::Get { id } => {
                let payload = self.catalog.get(&id)?;
                Ok(AppResponse::Get { payload })
            }
            AppRequest::Show { id } => {
                let payload = self.catalog.get(&id)?;
                Ok(AppResponse::Show { payload })
            }
            AppRequest::List {
                limit,
                cursor: token,
                budget,
                sessions_only,
            } => {
                if limit == 0 {
                    return Err(DomainError::InvalidRequest("limit must be > 0".into()).into());
                }
                budget.validate().map_err(AppError::from)?;
                let generation = self.catalog.active_generation()?;
                // list 无查询串；令牌以空串摘要 + wire_id_asc 排序标识绑定用例；
                // result_set 判别器把 `list` 与 `list_sessions` 的续读序列隔开。
                let query_digest = cursor::digest_query("");
                let result_set = if sessions_only {
                    RESULT_SET_SESSIONS_ONLY
                } else {
                    RESULT_SET_ALL
                };
                let offset = self.resolve_offset(
                    token.as_deref(),
                    generation,
                    &query_digest,
                    SORT_WIRE_ID_ASC,
                    Some(result_set),
                )?;

                let page = limit.min(budget.max_items);
                // Cursors are tamper-evident but not unforgeable: a forged
                // huge offset must not overflow into a negative SQL LIMIT
                // (SQLite treats -1 as "no limit", which would load the whole
                // catalog into memory). Cap the fetch window instead.
                let fetch = offset
                    .saturating_add(page as u64)
                    .saturating_add(1)
                    .min(MAX_FETCH_WINDOW);
                let fetched = if sessions_only {
                    self.catalog.list_sessions(fetch as usize)?
                } else {
                    self.catalog.list(fetch as usize)?
                };
                let fetched_len = fetched.len() as u64;
                let slice: Vec<CatalogEntry> = fetched
                    .into_iter()
                    .skip(usize::try_from(offset).unwrap_or(usize::MAX))
                    .take(page)
                    .collect();
                // Peek 预览（#7）：sessions_only 列表在每条会话条目上附 1 KiB 级
                // 分诊预览。预览字节计入同一字节闸——`max_response_bytes` 是最终
                // 序列化硬门（CONTRACT §3），预览不是免费内容。
                let peeks: Vec<Option<SessionPeek>> = if sessions_only {
                    self.build_session_peeks(&slice)?
                } else {
                    slice.iter().map(|_| None).collect()
                };
                // 标题投影（#6，schema v13）：sessions_only 列表逐条附派生标题
                // （custom-title > ai-title > 首条有效 user，存储层批量读取）；
                // 普通 `list` 全 None。标题字节计入同一字节闸——与 peek 一样
                // 不是免费内容。
                let titles: Vec<Option<String>> = if sessions_only {
                    let session_ids: Vec<StableId> =
                        slice.iter().map(|entry| entry.id.clone()).collect();
                    self.catalog.session_titles(&session_ids)?
                } else {
                    slice.iter().map(|_| None).collect()
                };
                let net_bytes = budget
                    .max_response_bytes
                    .saturating_sub(ENVELOPE_RESERVE_BYTES);
                let paired: Vec<(CatalogEntry, Option<SessionPeek>, Option<String>)> = slice
                    .into_iter()
                    .zip(peeks)
                    .zip(titles)
                    .map(|((entry, peek), title)| (entry, peek, title))
                    .collect();
                let (paired, truncation, _) =
                    budget::clamp_items(paired, page, net_bytes, |(entry, peek, title)| {
                        // 最终 JSON 形态 `{"id":"<id>","payload":"<lossy utf-8>"}`
                        // （sessions_only 时附加 `,"peek":{...}` 与 `,"title":"..."`）：
                        // id 按转义计长，payload 按序列化后长度计（不是原始字节数），
                        // peek 按实际序列化长度计 + `,"peek":` 前缀 8 字节，
                        // title 按转义后长度计 + `,"title":` 前缀 9 字节。
                        json_string_len(entry.id.as_str())
                            + lossy_payload_json_len(&entry.payload)
                            + 18
                            + peek
                                .as_ref()
                                .map(|peek| peek::PEEK_ENTRY_OVERHEAD_BYTES + peek.json_len())
                                .unwrap_or(0)
                            + title
                                .as_ref()
                                .map(|title| TITLE_ENTRY_OVERHEAD_BYTES + json_string_len(title))
                                .unwrap_or(0)
                    });
                let mut entries = Vec::with_capacity(paired.len());
                let mut peeks = Vec::with_capacity(paired.len());
                let mut titles = Vec::with_capacity(paired.len());
                for (entry, peek, title) in paired {
                    entries.push(entry);
                    peeks.push(peek);
                    titles.push(title);
                }
                let consumed = offset + entries.len() as u64;
                // A truncated page with zero kept entries means the first
                // entity already exceeds the byte budget: the next cursor
                // would claim the same offset and loop forever. Terminate
                // paging instead — the page semantics stay honest via
                // `truncation`.
                let has_more = fetched_len > consumed && !entries.is_empty();
                let next_cursor = self.issue_cursor(
                    has_more,
                    generation,
                    &query_digest,
                    SORT_WIRE_ID_ASC,
                    Some(result_set),
                    consumed,
                );
                Ok(AppResponse::List {
                    entries,
                    peeks,
                    titles,
                    next_cursor,
                    generation,
                    truncation,
                })
            }
            AppRequest::Context {
                session_id,
                policy,
                level,
                budget,
            } => self.handle_context(session_id, policy, level, budget),
            AppRequest::Message {
                message_id,
                session_id,
                around,
                budget,
            } => self.handle_message(message_id, session_id, around, budget),
            AppRequest::MessageContexts { message_id } => self.handle_message_contexts(message_id),
            AppRequest::GetSessionResume { session_id } => {
                // 会话必须有 `ses_v1_*` 种类（protocol 层已校验，这里是纵深防御）。
                if session_id.kind() != IdKind::Session {
                    return Err(DomainError::InvalidRequest(
                        "session id must be a ses_v1_* id".into(),
                    )
                    .into());
                }
                let mut metadata = self
                    .resume
                    .resume_of(std::slice::from_ref(&session_id))?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        AppError::from(PortError::Backend(
                            "resume resolver returned no metadata".into(),
                        ))
                    })?;
                metadata.session_id = session_id;
                Ok(AppResponse::SessionResume(metadata))
            }
            AppRequest::Status => {
                let catalog_count = self.catalog.count()?;
                let active_generation = self.catalog.active_generation()?;
                let context_stats = self.catalog.context_stats()?;
                let usage = self.catalog.usage_totals()?;
                let repos = self.catalog.repo_totals()?;
                Ok(AppResponse::Status {
                    catalog_count,
                    active_generation,
                    placements: context_stats.placements,
                    source_placement_claims: context_stats.source_placement_claims,
                    usage,
                    repos,
                })
            }
        }
    }

    /// `list_sessions` 的 Peek 预览（#7）：按会话 payload 的 `messages` 数组
    /// 抽取成员 id，整页一次批量读（[`CatalogStore::get_many`]，绝不 N+1），
    /// 再交给 [`peek::build_session_peek`] 派生首/尾用户消息。
    ///
    /// 预览是派生数据：payload 不可解析、`messages` 缺失、成员 id 非法时
    /// 降级为全 null 字段，绝不拖垮列表本身。
    fn build_session_peeks(
        &self,
        slice: &[CatalogEntry],
    ) -> Result<Vec<Option<SessionPeek>>, AppError> {
        let mut member_lists: Vec<Vec<StableId>> = Vec::with_capacity(slice.len());
        for entry in slice {
            let mut members = Vec::new();
            let parsed = serde_json::from_slice::<serde_json::Value>(&entry.payload).ok();
            if let Some(ids) = parsed
                .as_ref()
                .and_then(|value| value.get("messages"))
                .and_then(serde_json::Value::as_array)
            {
                members = ids
                    .iter()
                    .filter_map(|id| id.as_str().and_then(StableId::from_wire))
                    .collect();
            }
            member_lists.push(members);
        }
        let all_ids: Vec<StableId> = member_lists.iter().flatten().cloned().collect();
        let fetched = self.catalog.get_many(&all_ids)?;
        let mut payload_by_id: HashMap<&str, Option<&[u8]>> = HashMap::with_capacity(fetched.len());
        for (id, payload) in &fetched {
            // 同一条消息可属多个会话：get_many 保序重复返回同一 payload，
            // 索引取首次出现即可。
            payload_by_id
                .entry(id.as_str())
                .or_insert(payload.as_deref());
        }
        let peeks = slice
            .iter()
            .zip(&member_lists)
            .map(|(_, members)| {
                Some(peek::build_session_peek(
                    members
                        .iter()
                        .map(|id| payload_by_id.get(id.as_str()).copied().flatten()),
                ))
            })
            .collect();
        Ok(peeks)
    }

    /// 会话上下文装配（CONTRACT §1-2）：
    ///
    /// 1. 通过 [`ContextGraphStore`] 加载一个 session-scoped typed graph；
    /// 2. 由 Domain selector 在 placement graph 上选择 mainline/full；
    /// 3. 从每个 placement 的 exact document/span 装配 occurrence evidence；
    /// 4. 预算：`max_messages` + 字节闸裁剪 placement occurrences，
    ///    `max_evidence_spans` 裁剪证据；
    ///    任何裁剪都在 [`Truncation`] 里如实报告对应旋钮名。
    fn handle_context(
        &self,
        session_id: StableId,
        policy: ContextPolicy,
        requested_level: ContextLevel,
        budget: ResponseBudget,
    ) -> Result<AppResponse, AppError> {
        budget.validate().map_err(AppError::from)?;
        let generation = self.catalog.active_generation()?;
        let graph = self.catalog.load_session_graph(&session_id)?;
        if graph.session_id.as_str() != session_id.as_str() {
            return Err(DomainError::InvariantViolation(
                "context store returned a different session than requested".into(),
            )
            .into());
        }

        let (selected, branch_leaf, branch_leaf_placement_id): (
            Vec<&MessagePlacement>,
            Option<String>,
            Option<String>,
        ) = match policy {
            ContextPolicy::Mainline => match select_mainline(&graph)? {
                Some(selection) => (
                    selection.placements,
                    Some(selection.leaf.message_id.as_str().to_string()),
                    Some(selection.leaf.id.as_str().to_string()),
                ),
                None => (Vec::new(), None, None),
            },
            ContextPolicy::Full => {
                let placements = select_full(&graph)?;
                let leaf = placements.last().copied();
                (
                    placements,
                    leaf.map(|placement| placement.message_id.as_str().to_string()),
                    leaf.map(|placement| placement.id.as_str().to_string()),
                )
            }
        };

        let session_bytes = self.catalog.get(&session_id)?.ok_or_else(|| {
            DomainError::InvariantViolation("context session is missing from the catalog".into())
        })?;
        let session: serde_json::Value = serde_json::from_slice(&session_bytes).map_err(|_| {
            DomainError::InvariantViolation("session payload is not canonical JSON".into())
        })?;

        let messages_by_id: BTreeMap<&str, &Message> = graph
            .messages
            .iter()
            .map(|message| (message.id.as_str(), message))
            .collect();
        let documents_by_id: BTreeMap<&str, &SourceDocument> = graph
            .source_documents
            .iter()
            .map(|document| (document.id.as_str(), document))
            .collect();
        let mut payloads = BTreeMap::<String, (serde_json::Value, usize)>::new();
        let mut occurrences = Vec::with_capacity(selected.len());
        for placement in selected {
            let message = messages_by_id
                .get(placement.message_id.as_str())
                .copied()
                .ok_or_else(|| {
                    DomainError::InvariantViolation(format!(
                        "placement {} references a missing message",
                        placement.id
                    ))
                })?;
            let document = documents_by_id
                .get(placement.source_document_id.as_str())
                .copied()
                .ok_or_else(|| {
                    DomainError::InvariantViolation(format!(
                        "placement {} references a missing source document",
                        placement.id
                    ))
                })?;

            let message_wire = message.id.as_str().to_string();
            let (payload, payload_len) = if let Some((payload, payload_len)) =
                payloads.get(&message_wire)
            {
                (payload.clone(), *payload_len)
            } else {
                let bytes = self.catalog.get(&message.id)?.ok_or_else(|| {
                    DomainError::InvariantViolation(
                        "context message is missing from the catalog".into(),
                    )
                })?;
                let payload: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
                    DomainError::InvariantViolation("message payload is not canonical JSON".into())
                })?;
                let payload_len = bytes.len();
                payloads.insert(message_wire.clone(), (payload.clone(), payload_len));
                (payload, payload_len)
            };

            let placement_wire = placement.id.as_str().to_string();
            let estimated_bytes = payload_len
                .saturating_add(message_wire.len().saturating_mul(2))
                .saturating_add(placement_wire.len())
                .saturating_add(96);
            occurrences.push(ContextOccurrence {
                message: ContextMessage {
                    id: message_wire.clone(),
                    placement_id: placement_wire,
                    message_id: message_wire,
                    payload,
                },
                evidence: evidence::assemble(message, placement, document, generation),
                role: message.role,
                source_document_id: document.id.as_str().to_string(),
                estimated_bytes,
            });
        }

        let pre_clamp_level = available_level(requested_level, &occurrences);
        let base_net_bytes = budget
            .max_response_bytes
            .saturating_sub(ENVELOPE_RESERVE_BYTES)
            // The session payload is embedded verbatim in the response; count
            // it against the hard byte gate up front, before clamping keeps.
            .saturating_sub(session_bytes.len());
        let mut assembly_level = requested_level;
        let mut net_bytes = base_net_bytes
            .saturating_sub(structural_metadata_reserve(&session_id, pre_clamp_level));
        let (mut kept_occurrences, mut truncation, _) = clamp_context_occurrences(
            occurrences.clone(),
            pre_clamp_level,
            budget.max_messages,
            net_bytes,
        );
        let mut fallback_reason = None;
        if pre_clamp_level != ContextLevel::Raw
            && available_level(pre_clamp_level, &kept_occurrences) == ContextLevel::Raw
        {
            fallback_reason = truncation.reason.clone();
            if requested_level == ContextLevel::Sessions {
                let talks_net_bytes = base_net_bytes.saturating_sub(structural_metadata_reserve(
                    &session_id,
                    ContextLevel::Talks,
                ));
                let (talks_occurrences, talks_truncation, _) = clamp_context_occurrences(
                    occurrences.clone(),
                    ContextLevel::Talks,
                    budget.max_messages,
                    talks_net_bytes,
                );
                if available_level(ContextLevel::Talks, &talks_occurrences) == ContextLevel::Talks {
                    assembly_level = ContextLevel::Talks;
                    net_bytes = talks_net_bytes;
                    kept_occurrences = talks_occurrences;
                    truncation = talks_truncation;
                }
            }
            if available_level(assembly_level, &kept_occurrences) == ContextLevel::Raw {
                assembly_level = ContextLevel::Raw;
                net_bytes = base_net_bytes;
                (kept_occurrences, truncation, _) = clamp_context_occurrences(
                    occurrences,
                    ContextLevel::Raw,
                    budget.max_messages,
                    net_bytes,
                );
            }
        }
        if let Some(reason) = fallback_reason
            && !truncation
                .reason
                .as_deref()
                .is_some_and(|current| current.split(',').any(|value| value == reason))
        {
            truncation.truncated = true;
            truncation.reason = Some(match truncation.reason.take() {
                None => reason,
                Some(current) => format!("{current},{reason}"),
            });
        }
        let mut evidence: Vec<EvidenceSpanDto> = kept_occurrences
            .iter()
            .map(|occurrence| occurrence.evidence.clone())
            .collect();
        if evidence.len() > budget.max_evidence_spans {
            evidence.truncate(budget.max_evidence_spans);
            truncation.truncated = true;
            // 两道闸都触发时两个旋钮名都要报——单升一个救不了另一个。
            truncation.reason = Some(match truncation.reason.take() {
                None => budget::TRUNCATION_MAX_EVIDENCE_SPANS.to_string(),
                Some(prior) => format!("{prior},{}", budget::TRUNCATION_MAX_EVIDENCE_SPANS),
            });
        }

        let messages: Vec<ContextMessage> = kept_occurrences
            .iter()
            .map(|occurrence| occurrence.message.clone())
            .collect();
        let (effective_level, talks, summary) = assemble_level(assembly_level, &kept_occurrences);
        let hint = build_hint(&session_id, requested_level, effective_level);
        if structural_fields_bytes(&talks, &summary, &hint) > net_bytes {
            truncation.truncated = true;
            truncation.reason = Some(match truncation.reason.take() {
                None => budget::TRUNCATION_MAX_RESPONSE_BYTES.to_string(),
                Some(prior)
                    if prior
                        .split(',')
                        .any(|reason| reason == budget::TRUNCATION_MAX_RESPONSE_BYTES) =>
                {
                    prior
                }
                Some(prior) => format!("{prior},{}", budget::TRUNCATION_MAX_RESPONSE_BYTES),
            });
        }

        Ok(AppResponse::Context {
            session_id: session_id.as_str().to_string(),
            session,
            branch_leaf,
            branch_leaf_placement_id,
            tool_activities: {
                let ids: Vec<StableId> = messages
                    .iter()
                    .filter_map(|m| StableId::from_wire(&m.message_id))
                    .collect();
                self.catalog.tool_activities_for_messages(&ids)?
            },
            messages,
            evidence,
            requested_level,
            effective_level,
            talks,
            summary,
            hint,
            truncation,
            generation,
        })
    }

    fn handle_message(
        &self,
        message_id: StableId,
        session_id: Option<StableId>,
        around: usize,
        budget: ResponseBudget,
    ) -> Result<AppResponse, AppError> {
        budget.validate().map_err(AppError::from)?;
        let candidates = self.catalog.message_contexts(&message_id)?;
        let selected_session = match session_id {
            Some(requested) => {
                if !candidates
                    .iter()
                    .any(|candidate| candidate.session_id.as_str() == requested.as_str())
                {
                    return Err(DomainError::NotFound(
                        "message has no placement in the requested session".into(),
                    )
                    .into());
                }
                requested
            }
            None => {
                let session_ids = candidates
                    .iter()
                    .map(|candidate| candidate.session_id.as_str().to_string())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                match session_ids.as_slice() {
                    [] => {
                        return Err(DomainError::NotFound(
                            "message has no session placement".into(),
                        )
                        .into());
                    }
                    [only] => StableId::from_wire(only).ok_or_else(|| {
                        AppError::Domain(DomainError::InvariantViolation(
                            "context store returned a session id outside the wire format".into(),
                        ))
                    })?,
                    _ => {
                        let mut session_ids = session_ids;
                        let candidate_count = session_ids.len();
                        session_ids.truncate(MAX_MESSAGE_AMBIGUITY_CANDIDATES);
                        return Err(MessageAmbiguity {
                            candidate_session_ids: session_ids,
                            candidate_count,
                            hint: "retry get_message with one candidate session_id".into(),
                        }
                        .into());
                    }
                }
            }
        };

        let generation = self.catalog.active_generation()?;
        let graph = self.catalog.load_session_graph(&selected_session)?;
        if graph.session_id.as_str() != selected_session.as_str() {
            return Err(DomainError::InvariantViolation(
                "context store returned a different session than requested".into(),
            )
            .into());
        }
        let mainline = select_mainline(&graph)?
            .map(|selection| selection.placements)
            .unwrap_or_default();
        let matching_anchor_indexes = mainline
            .iter()
            .enumerate()
            .filter_map(|(index, placement)| {
                (placement.message_id.as_str() == message_id.as_str()).then_some(index)
            })
            .collect::<Vec<_>>();
        let anchor_index = match matching_anchor_indexes.as_slice() {
            [index] => *index,
            [] => {
                return Err(DomainError::NotFound(
                    "message is not on the selected session mainline".into(),
                )
                .into());
            }
            _ => {
                return Err(DomainError::InvalidRequest(
                    "message has multiple placements on selected session mainline; use get_session_context to inspect placements".into(),
                )
                .into());
            }
        };
        let start = anchor_index.saturating_sub(around);
        let end = anchor_index
            .saturating_add(around)
            .saturating_add(1)
            .min(mainline.len());
        let selected = &mainline[start..end];
        let anchor_placement_id = mainline[anchor_index].id.as_str().to_string();
        let messages_by_id: BTreeMap<&str, &Message> = graph
            .messages
            .iter()
            .map(|message| (message.id.as_str(), message))
            .collect();
        let net_bytes = budget
            .max_response_bytes
            .saturating_sub(ENVELOPE_RESERVE_BYTES);
        let mut occurrences = Vec::with_capacity(selected.len());
        for placement in selected {
            let message = messages_by_id
                .get(placement.message_id.as_str())
                .copied()
                .ok_or_else(|| {
                    DomainError::InvariantViolation(
                        "message placement references a missing message".into(),
                    )
                })?;
            let bytes = self.catalog.get(&message.id)?.ok_or_else(|| {
                DomainError::InvariantViolation("message is missing from the catalog".into())
            })?;
            let payload = serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|_| {
                DomainError::InvariantViolation("message payload is not canonical JSON".into())
            })?;
            let full_text = payload
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| message.text.clone());
            let message_wire = message.id.as_str().to_string();
            let placement_wire = placement.id.as_str().to_string();
            let is_anchor = placement.id.as_str() == anchor_placement_id;
            let mut context_message = ContextMessage {
                id: message_wire.clone(),
                placement_id: placement_wire,
                message_id: message_wire,
                payload,
            };
            let payload_truncated = if is_anchor {
                project_anchor_payload(
                    &mut context_message,
                    message.role,
                    &full_text,
                    net_bytes.saturating_sub(48),
                )?
            } else {
                false
            };
            occurrences.push(MessageOccurrence {
                estimated_bytes: message_occurrence_bytes(&context_message),
                is_anchor,
                payload_truncated,
                message: context_message,
            });
        }

        let (kept, truncation) = clamp_message_window(occurrences, &budget, net_bytes);
        let messages = kept
            .into_iter()
            .map(|occurrence| occurrence.message)
            .collect();
        Ok(AppResponse::Message {
            window: MessageWindow {
                message_id: message_id.as_str().to_string(),
                session_id: selected_session.as_str().to_string(),
                anchor_placement_id,
                messages,
                truncation,
                generation,
            },
        })
    }

    fn handle_message_contexts(&self, message_id: StableId) -> Result<AppResponse, AppError> {
        let raw_candidates = self.catalog.message_contexts(&message_id)?;
        let mut grouped = BTreeMap::<String, BTreeSet<String>>::new();
        for candidate in raw_candidates {
            if candidate.placement_ids.is_empty() {
                return Err(DomainError::InvariantViolation(
                    "message context candidate has no placements".into(),
                )
                .into());
            }
            let placement_ids = grouped
                .entry(candidate.session_id.as_str().to_string())
                .or_default();
            placement_ids.extend(
                candidate
                    .placement_ids
                    .into_iter()
                    .map(|placement_id| placement_id.as_str().to_string()),
            );
        }
        let candidates = grouped
            .into_iter()
            .map(|(session_id, placement_ids)| MessageContextCandidate {
                session_id,
                placement_ids: placement_ids.into_iter().collect(),
            })
            .collect();
        Ok(AppResponse::MessageContexts {
            message_id: message_id.as_str().to_string(),
            candidates,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_session_grep_domain::{
        EvidenceSpan, IdKind, MessageEdge, MessagePlacement, MessageRelation, Role,
        SessionContextGraph, SourceDocument, Stability,
    };
    use agent_session_grep_ports::SourcePlacement;
    use agent_session_grep_ports::{
        ContextStats, MessageContextCandidate as PortMessageContextCandidate, PortResult,
        SidechainFacet, SourceSnapshot,
    };
    use agent_session_grep_testkit::FakeProvider;

    /// 内存态假后端，仅用于用例逻辑测试。
    struct FakeCatalog;
    impl FakeCatalog {
        fn list_mock(&self, kind: Option<IdKind>, limit: usize) -> PortResult<Vec<CatalogEntry>> {
            let id = match kind {
                Some(IdKind::Session) => StableId::derive(
                    IdKind::Session,
                    Stability::Reconstructed,
                    &[b"listed-session"],
                ),
                _ => StableId::derive(IdKind::Message, Stability::Reconstructed, &[b"listed"]),
            };
            Ok(vec![CatalogEntry {
                id,
                payload: b"payload".to_vec(),
            }]
            .into_iter()
            .take(limit)
            .collect())
        }
    }
    impl CatalogStore for FakeCatalog {
        fn get(&self, _id: &StableId) -> PortResult<Option<Vec<u8>>> {
            Ok(Some(b"payload".to_vec()))
        }
        fn get_many(&self, ids: &[StableId]) -> PortResult<Vec<(StableId, Option<Vec<u8>>)>> {
            Ok(ids
                .iter()
                .map(|id| (id.clone(), Some(b"payload".to_vec())))
                .collect())
        }
        fn put(&self, _id: &StableId, _payload: &[u8]) -> PortResult<()> {
            Ok(())
        }
        fn list(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
            self.list_mock(None, limit)
        }
        fn list_sessions(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
            self.list_mock(Some(IdKind::Session), limit)
        }
        fn count(&self) -> PortResult<u64> {
            Ok(1)
        }
        fn active_generation(&self) -> PortResult<u64> {
            Ok(7)
        }
    }
    impl ContextGraphStore for FakeCatalog {
        fn load_session_graph(&self, _session_id: &StableId) -> PortResult<SessionContextGraph> {
            Err(PortError::NotFound("session context not found".into()))
        }

        fn message_contexts(
            &self,
            _message_id: &StableId,
        ) -> PortResult<Vec<PortMessageContextCandidate>> {
            Ok(Vec::new())
        }

        fn session_of(&self, ids: &[StableId]) -> PortResult<Vec<(StableId, Option<StableId>)>> {
            Ok(ids.iter().map(|id| (id.clone(), None)).collect())
        }

        fn source_placements_of(
            &self,
            ids: &[StableId],
        ) -> PortResult<Vec<(StableId, Option<SourcePlacement>)>> {
            Ok(ids.iter().map(|id| (id.clone(), None)).collect())
        }

        fn context_stats(&self) -> PortResult<ContextStats> {
            Ok(ContextStats {
                placements: 2,
                source_placement_claims: 3,
            })
        }
    }

    struct FakeIndex;
    impl SearchIndex for FakeIndex {
        fn index(&self, _id: &StableId, _text: &str) -> PortResult<()> {
            Ok(())
        }
        fn query_filtered(
            &self,
            _query: SearchQuery<'_>,
            _limit: usize,
        ) -> PortResult<Vec<SearchHit>> {
            Ok(vec![SearchHit {
                id: StableId::derive(IdKind::Message, Stability::Reconstructed, &[b"h"]),
                score: 1.0,
                session_id: None,
                text: None,
                why_matched: Vec::new(),
                suggested_next_commands: Vec::new(),
                occurrences: 1,
                resume_available: false,
            }])
        }
    }

    fn app() -> App<FakeCatalog, FakeIndex> {
        App::new(FakeCatalog, FakeIndex)
    }

    // 引用 SourceSnapshot 以确认 ports DTO 可被应用层消费（编译期保证）。
    #[allow(dead_code)]
    fn _snapshot_is_usable(s: SourceSnapshot) -> u64 {
        s.len
    }

    #[test]
    fn search_returns_hits() {
        let r = app().handle(AppRequest::Search {
            query: "hello".into(),
            filters: SearchFilters::default(),
            facets: SearchFacets::default(),
            limit: 10,
            cursor: None,
            budget: ResponseBudget::default(),
            include_system: false,
            group_by_session: false,
            mode: RetrievalMode::Lexical,
            query_embedding: None,
        });
        assert!(matches!(r, Ok(AppResponse::Search { hits, .. }) if hits.len() == 1));
    }

    #[test]
    fn search_rejects_zero_limit() {
        let r = app().handle(AppRequest::Search {
            query: "x".into(),
            filters: SearchFilters::default(),
            facets: SearchFacets::default(),
            limit: 0,
            cursor: None,
            budget: ResponseBudget::default(),
            include_system: false,
            group_by_session: false,
            mode: RetrievalMode::Lexical,
            query_embedding: None,
        });
        assert!(matches!(
            r.unwrap_err(),
            AppError::Domain(DomainError::InvalidRequest(_))
        ));
    }

    #[test]
    fn search_rejects_empty_query() {
        let r = app().handle(AppRequest::Search {
            query: "   ".into(),
            filters: SearchFilters::default(),
            facets: SearchFacets::default(),
            limit: 5,
            cursor: None,
            budget: ResponseBudget::default(),
            include_system: false,
            group_by_session: false,
            mode: RetrievalMode::Lexical,
            query_embedding: None,
        });
        assert!(matches!(
            r.unwrap_err(),
            AppError::Domain(DomainError::InvalidRequest(_))
        ));
    }

    #[test]
    fn search_rejects_control_characters_before_index_query() {
        // R4.2（ADR-0003）：NUL/C0/C1 控制字符在 Application 边界拒绝为
        // invalid_request，绝不清除式净化（删除会拼接 token）；且必须发生在
        // 任何索引查询之前——命中索引即 panic。
        struct ExplodingIndex;
        impl SearchIndex for ExplodingIndex {
            fn index(&self, _id: &StableId, _text: &str) -> PortResult<()> {
                Ok(())
            }
            fn query_filtered(
                &self,
                _query: SearchQuery<'_>,
                _limit: usize,
            ) -> PortResult<Vec<SearchHit>> {
                panic!("control-character query must be rejected before any index query")
            }
        }
        let app = App::new(FakeCatalog, ExplodingIndex);
        for query in [
            "\u{0}", "a\u{1}b", "\u{7f}", "a\u{80}b", "\u{9f}", "a\tb", "a\nb",
        ] {
            let err = app
                .handle(search_req(query, 5, None))
                .expect_err("query {query:?} must be rejected");
            assert!(
                matches!(err, AppError::Domain(DomainError::InvalidRequest(_))),
                "{query:?}: {err}"
            );
        }
    }

    /// 与 [`PagedIndex`] 的派生规则一致（`hit{i:02}` 种子），使目录 payload
    /// 能对应到检索命中。
    fn hit_id(tag: &str) -> StableId {
        StableId::derive(IdKind::Message, Stability::Reconstructed, &[tag.as_bytes()])
    }

    #[test]
    fn search_hits_carry_text_summary_from_payloads() {
        // R1（ADR-0004）/ADR-0008：text 摘要（原 snippet）在 Application 检索
        // 装配时生成——批量取 payload、解析 `text` 字段、构建命中窗口（短正文
        // 整体输出）；不做任何脱敏。MapCatalog 无 placement → session_id 为 None。
        let mut cat = MapCatalog::new(7);
        for (tag, text) in [("hit00", "hello world"), ("hit01", "second hit")] {
            let id = hit_id(tag);
            cat.insert(
                &id,
                serde_json::json!({ "role": "user", "text": text })
                    .to_string()
                    .into_bytes(),
            );
        }
        let app = App::with_clock(&cat, PagedIndex { n: 2 }, clock_t0);
        let resp = app.handle(search_req("q", 10, None)).unwrap();
        let AppResponse::Search { hits, .. } = resp else {
            panic!("expected Search response");
        };
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, hit_id("hit00"));
        assert_eq!(hits[0].text.as_deref(), Some("hello world"));
        assert_eq!(hits[1].text.as_deref(), Some("second hit"));
        assert!(hits.iter().all(|hit| hit.session_id.is_none()));
    }

    #[test]
    fn search_text_falls_back_to_prefix_without_literal_evidence() {
        // R1.2：无字面证据（本用例查询词不在正文）时，单条 text 摘要按
        // `max_snippet_chars`（字符数）回退为前缀；命中窗口语义见 snippet 模块。
        let mut cat = MapCatalog::new(7);
        let id = hit_id("hit00");
        cat.insert(
            &id,
            serde_json::json!({ "text": "abcdefghij" })
                .to_string()
                .into_bytes(),
        );
        let app = App::with_clock(&cat, PagedIndex { n: 1 }, clock_t0);
        let resp = app
            .handle(AppRequest::Search {
                query: "q".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 5,
                cursor: None,
                budget: ResponseBudget {
                    max_snippet_chars: 4,
                    ..Default::default()
                },
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap();
        let AppResponse::Search { hits, .. } = resp else {
            panic!("expected Search response");
        };
        assert_eq!(hits[0].text.as_deref(), Some("abcd"));
    }

    #[test]
    fn search_text_none_when_payload_has_no_text() {
        // text 缺失 / 非字符串 / payload 非 JSON → text None（不臆造正文）。
        // 空字符串 text 视为有正文（与旧 CLI 行为一致）。
        let mut cat = MapCatalog::new(7);
        for (tag, payload) in [
            ("hit00", br#"{"role":"user"}"#.as_slice()),
            ("hit01", br#"{"text":42}"#.as_slice()),
            ("hit02", b"not json".as_slice()),
            ("hit03", br#"{"text":""}"#.as_slice()),
        ] {
            let id = hit_id(tag);
            cat.insert(&id, payload.to_vec());
        }
        let app = App::with_clock(&cat, PagedIndex { n: 4 }, clock_t0);
        let resp = app.handle(search_req("q", 10, None)).unwrap();
        let AppResponse::Search { hits, .. } = resp else {
            panic!("expected Search response");
        };
        assert_eq!(hits[3].text.as_deref(), Some(""));
        assert!(hits[..3].iter().all(|hit| hit.text.is_none()));
    }

    #[test]
    fn search_byte_gate_charges_text_bytes() {
        // R1.2/R4.2：text 摘要字节计入同一 `max_response_bytes` 闸。无 text 时
        // 4 条命中全部放得下（每条仅 ~70 B）；带 1000 字符 text 时每条
        // ~1080 B，净预算 3072 只容 2 条——证明摘要字节被计入闸门，
        // 且截断原因显式报 max_response_bytes。
        let mut cat = MapCatalog::new(7);
        for tag in ["hit00", "hit01", "hit02", "hit03"] {
            let id = hit_id(tag);
            cat.insert(
                &id,
                serde_json::json!({ "text": "x".repeat(1000) })
                    .to_string()
                    .into_bytes(),
            );
        }
        let app = App::with_clock(&cat, PagedIndex { n: 4 }, clock_t0);
        let resp = app
            .handle(AppRequest::Search {
                query: "q".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget {
                    max_response_bytes: budget::MIN_RESPONSE_BYTES,
                    ..Default::default()
                },
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap();
        let (ids, _, _, truncation) = hits_of(resp);
        assert!(!ids.is_empty() && ids.len() < 4, "kept {}", ids.len());
        assert!(truncation.truncated);
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_RESPONSE_BYTES)
        );
    }

    #[test]
    fn search_window_bytes_are_charged_not_full_payload() {
        // 命中窗口（而非完整正文）经既有 `search_hit_charge` 计入
        // `max_response_bytes`：4 条 10 万字符正文在 32 字符窗口下全部放得下
        // 且顺序不变；窗口放到 2000 字符时净预算 3072 只容 1 条，截断原因
        // 显式报 max_response_bytes（预算收紧时既有 truncation 语义可复现）。
        let mut cat = MapCatalog::new(7);
        for tag in ["hit00", "hit01", "hit02", "hit03"] {
            let id = hit_id(tag);
            let mut full = "x".repeat(100_000);
            full.push_str(" needle");
            cat.insert(
                &id,
                serde_json::json!({ "text": full }).to_string().into_bytes(),
            );
        }
        let app = App::with_clock(&cat, PagedIndex { n: 4 }, clock_t0);
        let request = |max_snippet_chars: usize| AppRequest::Search {
            query: "needle".into(),
            filters: SearchFilters::default(),
            facets: SearchFacets::default(),
            limit: 10,
            cursor: None,
            budget: ResponseBudget {
                max_response_bytes: budget::MIN_RESPONSE_BYTES,
                max_snippet_chars,
                ..Default::default()
            },
            include_system: false,
            group_by_session: false,
            mode: RetrievalMode::Lexical,
            query_embedding: None,
        };
        let AppResponse::Search {
            hits,
            next_cursor,
            truncation,
            ..
        } = app.handle(request(32)).unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(
            hits.iter().map(|hit| hit.id.clone()).collect::<Vec<_>>(),
            vec![
                hit_id("hit00"),
                hit_id("hit01"),
                hit_id("hit02"),
                hit_id("hit03")
            ],
            "窗口构建不得影响排序"
        );
        assert!(!truncation.truncated && next_cursor.is_none(), "{hits:?}");
        for hit in &hits {
            let window = hit.text.as_deref().expect("window text");
            assert_eq!(window.chars().count(), 32);
            assert!(window.contains("needle"), "{window:?}");
        }
        let AppResponse::Search {
            hits,
            next_cursor,
            truncation,
            ..
        } = app.handle(request(2000)).unwrap()
        else {
            panic!("expected Search response");
        };
        assert!(hits.len() < 4 && !hits.is_empty(), "kept {}", hits.len());
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_RESPONSE_BYTES)
        );
        assert!(next_cursor.is_some(), "被截断的页必须仍交出 cursor");
    }

    #[test]
    fn search_hits_carry_session_id_from_placements() {
        // ADR-0008：命中带归属会话 wire id（session_of 批量解析）+ text 摘要。
        // GraphCatalog 的 graph 里有 placement → session_id 有值；payload 的
        // `text` 字段 → text 有值。FixedHits 同分命中被 rank signals 重钉为
        // wire id 升序（与真实 store 的 bm25+id 全序一致）——输入按同序喂入，
        // 产出顺序即输入顺序。
        let fixture = ctx_fixture();
        let mut ids = vec![
            fixture.root.clone(),
            fixture.repeated.clone(),
            fixture.leaf.clone(),
        ];
        ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let app = App::with_clock(&fixture.store, FixedHits(ids.clone()), clock_t0);
        let resp = app.handle(search_req("q", 10, None)).unwrap();
        let AppResponse::Search { hits, .. } = resp else {
            panic!("expected Search response");
        };
        assert_eq!(hits.len(), 3);
        for (hit, expected) in hits.iter().zip(&ids) {
            assert_eq!(&hit.id, expected);
            assert_eq!(hit.session_id.as_deref(), Some(fixture.session.as_str()));
        }
        // wire id 升序：ctx-leaf < ctx-repeated < ctx-root。
        assert_eq!(hits[0].text.as_deref(), Some("leaf"));
        assert_eq!(hits[1].text.as_deref(), Some("repeated"));
        assert_eq!(hits[2].text.as_deref(), Some("root"));
    }

    #[test]
    fn get_returns_payload() {
        let id = StableId::derive(IdKind::Message, Stability::Reconstructed, &[b"g"]);
        let r = app().handle(AppRequest::Get { id });
        assert!(matches!(r, Ok(AppResponse::Get { payload: Some(_) })));
    }

    #[test]
    fn list_returns_stable_entries() {
        let r = app().handle(AppRequest::List {
            limit: 10,
            cursor: None,
            budget: ResponseBudget::default(),
            sessions_only: false,
        });
        assert!(matches!(r, Ok(AppResponse::List { entries, .. }) if entries.len() == 1));
    }

    #[test]
    fn list_sessions_only_filters_to_session_kind() {
        let r = app().handle(AppRequest::List {
            limit: 10,
            cursor: None,
            budget: ResponseBudget::default(),
            sessions_only: true,
        });
        assert!(
            matches!(r, Ok(AppResponse::List { entries, .. }) if entries.iter().all(|e| e.id.kind() == IdKind::Session))
        );
    }

    #[test]
    fn list_rejects_zero_limit() {
        let err = app()
            .handle(AppRequest::List {
                limit: 0,
                cursor: None,
                budget: ResponseBudget::default(),
                sessions_only: false,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            AppError::Domain(DomainError::InvalidRequest(_))
        ));
    }

    #[test]
    fn status_returns_catalog_count() {
        let r = app().handle(AppRequest::Status);
        assert!(matches!(
            r,
            Ok(AppResponse::Status {
                catalog_count: 1,
                active_generation: 7,
                placements: 2,
                source_placement_claims: 3,
                usage: None,
                repos,
            }) if repos.is_empty()
        ));
    }

    // ---- 分页 + cursor + budget + context（design §6 集成）----

    fn clock_t0() -> i64 {
        1_000_000
    }

    fn clock_after_ttl() -> i64 {
        1_000_000 + cursor::DEFAULT_TTL_MS
    }

    /// 可分页假索引：n 个确定性命中，按 limit 截取（模拟钉住排序上的超取）。
    /// 分数为正、严格降序（与真实 store 的 bm25 负分取负后的形态一致）——
    /// 产出顺序即 Application 重钉后的 (final desc, id asc) 序，重排是 no-op，
    /// 既有分页/顺序断言保持有效。
    struct PagedIndex {
        n: usize,
    }
    impl SearchIndex for PagedIndex {
        fn index(&self, _id: &StableId, _text: &str) -> PortResult<()> {
            Ok(())
        }
        fn query_filtered(
            &self,
            _query: SearchQuery<'_>,
            limit: usize,
        ) -> PortResult<Vec<SearchHit>> {
            Ok((0..self.n.min(limit))
                .map(|i| SearchHit {
                    id: StableId::derive(
                        IdKind::Message,
                        Stability::Reconstructed,
                        &[format!("hit{i:02}").as_bytes()],
                    ),
                    score: (self.n - i) as f32,
                    session_id: None,
                    text: None,
                    why_matched: Vec::new(),
                    suggested_next_commands: Vec::new(),
                    occurrences: 1,
                    resume_available: false,
                })
                .collect())
        }
    }

    struct FixedHits(Vec<StableId>);
    impl SearchIndex for FixedHits {
        fn index(&self, _id: &StableId, _text: &str) -> PortResult<()> {
            Ok(())
        }
        fn query_filtered(
            &self,
            _query: SearchQuery<'_>,
            limit: usize,
        ) -> PortResult<Vec<SearchHit>> {
            Ok(self
                .0
                .iter()
                .take(limit)
                .map(|id| SearchHit {
                    id: id.clone(),
                    score: 0.0,
                    session_id: None,
                    text: None,
                    why_matched: Vec::new(),
                    suggested_next_commands: Vec::new(),
                    occurrences: 1,
                    resume_available: false,
                })
                .collect())
        }
    }

    /// 固定分值假索引：按给定 (id, bm25) 序返回——rank signals 测试用非零
    /// 且可人为相等的 bm25（`FixedHits` 全 0 分无法区分衰减/惩罚）。
    struct ScoredHits(Vec<(StableId, f32)>);
    impl SearchIndex for ScoredHits {
        fn index(&self, _id: &StableId, _text: &str) -> PortResult<()> {
            Ok(())
        }
        fn query_filtered(
            &self,
            _query: SearchQuery<'_>,
            limit: usize,
        ) -> PortResult<Vec<SearchHit>> {
            Ok(self
                .0
                .iter()
                .take(limit)
                .map(|(id, score)| SearchHit {
                    id: id.clone(),
                    score: *score,
                    session_id: None,
                    text: None,
                    why_matched: Vec::new(),
                    suggested_next_commands: Vec::new(),
                    occurrences: 1,
                    resume_available: false,
                })
                .collect())
        }
    }

    /// 就绪的假语义索引：按给定 id 序返回"余弦相似度降序"的 top-k
    /// （`take(limit)`，与端口契约同形）。`is_ready` 恒 true，使
    /// semantic/hybrid 路径真正执行（而非降级 lexical_fallback）。
    struct FakeSemantic(Vec<StableId>);
    impl SemanticIndex for FakeSemantic {
        fn index_embedding(&self, _id: &StableId, _embedding: &[f32]) -> PortResult<()> {
            Ok(())
        }
        fn query_semantic_filtered(
            &self,
            _query_embedding: &[f32],
            limit: usize,
            _filters: &SearchFilters,
            _facets: &SearchFacets,
            _include_system: bool,
        ) -> PortResult<Vec<SearchHit>> {
            Ok(self
                .0
                .iter()
                .take(limit)
                .enumerate()
                .map(|(rank, id)| SearchHit {
                    id: id.clone(),
                    score: 1.0 - (rank as f32) / 100.0,
                    session_id: None,
                    text: None,
                    why_matched: Vec::new(),
                    suggested_next_commands: Vec::new(),
                    occurrences: 1,
                    resume_available: false,
                })
                .collect())
        }
        fn is_ready(&self, _query_dimension: usize) -> PortResult<bool> {
            Ok(true)
        }
        fn semantic_model_id(&self) -> PortResult<Option<String>> {
            Ok(Some("fake-model".into()))
        }
    }

    /// 内存 map 目录：BTreeMap 键序即 wire id 升序（与 sqlite list 的钉住排序一致）；
    /// generation 用 Cell 可变，测 cursor 的 generation 绑定。
    struct MapCatalog {
        map: std::collections::BTreeMap<String, Vec<u8>>,
        generation: std::cell::Cell<u64>,
        session_of: std::collections::BTreeMap<String, String>,
        titles: std::collections::BTreeMap<String, String>,
        repo_slugs: std::collections::BTreeMap<String, String>,
    }
    impl MapCatalog {
        fn new(generation: u64) -> Self {
            Self {
                map: Default::default(),
                generation: std::cell::Cell::new(generation),
                session_of: Default::default(),
                titles: Default::default(),
                repo_slugs: Default::default(),
            }
        }
        fn insert(&mut self, id: &StableId, payload: impl Into<Vec<u8>>) {
            self.map.insert(id.as_str().to_string(), payload.into());
        }
        fn set_session_of(&mut self, message_id: &StableId, session_id: &StableId) {
            self.session_of.insert(
                message_id.as_str().to_string(),
                session_id.as_str().to_string(),
            );
        }
        /// 注入标题投影（#6）：默认无标题；有则按 session wire id 返回。
        fn set_title(&mut self, session_id: &StableId, title: impl Into<String>) {
            self.titles
                .insert(session_id.as_str().to_string(), title.into());
        }
    }
    impl CatalogStore for MapCatalog {
        fn get(&self, id: &StableId) -> PortResult<Option<Vec<u8>>> {
            Ok(self.map.get(id.as_str()).cloned())
        }
        fn get_many(&self, ids: &[StableId]) -> PortResult<Vec<(StableId, Option<Vec<u8>>)>> {
            Ok(ids
                .iter()
                .map(|id| {
                    let payload = self.map.get(id.as_str()).cloned();
                    (id.clone(), payload)
                })
                .collect())
        }
        fn put(&self, _id: &StableId, _payload: &[u8]) -> PortResult<()> {
            Ok(())
        }
        fn list(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
            Ok(self
                .map
                .iter()
                .take(limit)
                .map(|(k, v)| CatalogEntry {
                    id: StableId::from_wire(k).expect("map keys are wire ids"),
                    payload: v.clone(),
                })
                .collect())
        }
        fn list_sessions(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
            Ok(self
                .map
                .iter()
                .filter(|(k, _)| k.starts_with("ses_v1_"))
                .take(limit)
                .map(|(k, v)| CatalogEntry {
                    id: StableId::from_wire(k).expect("map keys are wire ids"),
                    payload: v.clone(),
                })
                .collect())
        }
        fn session_titles(&self, session_ids: &[StableId]) -> PortResult<Vec<Option<String>>> {
            Ok(session_ids
                .iter()
                .map(|id| self.titles.get(id.as_str()).cloned())
                .collect())
        }
        fn session_repo_slugs(&self, session_ids: &[StableId]) -> PortResult<Vec<Option<String>>> {
            Ok(session_ids
                .iter()
                .map(|id| self.repo_slugs.get(id.as_str()).cloned())
                .collect())
        }
        fn count(&self) -> PortResult<u64> {
            Ok(self.map.len() as u64)
        }
        fn active_generation(&self) -> PortResult<u64> {
            Ok(self.generation.get())
        }
    }
    impl ContextGraphStore for MapCatalog {
        fn load_session_graph(&self, _session_id: &StableId) -> PortResult<SessionContextGraph> {
            Err(PortError::NotFound("session context not found".into()))
        }

        fn message_contexts(
            &self,
            _message_id: &StableId,
        ) -> PortResult<Vec<PortMessageContextCandidate>> {
            Ok(Vec::new())
        }

        fn session_of(&self, ids: &[StableId]) -> PortResult<Vec<(StableId, Option<StableId>)>> {
            // 纯 map 目录没有 placement 数据 → 全部 None；测试可用 set_session_of
            // 显式注入归属（R3 归并按会话坍缩需要真实归属）。
            Ok(ids
                .iter()
                .map(|id| {
                    let session = self
                        .session_of
                        .get(id.as_str())
                        .and_then(|wire| StableId::from_wire(wire));
                    (id.clone(), session)
                })
                .collect())
        }

        fn source_placements_of(
            &self,
            ids: &[StableId],
        ) -> PortResult<Vec<(StableId, Option<SourcePlacement>)>> {
            Ok(ids.iter().map(|id| (id.clone(), None)).collect())
        }

        fn context_stats(&self) -> PortResult<ContextStats> {
            Ok(ContextStats::default())
        }
    }

    fn search_req(query: &str, limit: usize, cursor: Option<String>) -> AppRequest {
        AppRequest::Search {
            query: query.into(),
            filters: SearchFilters::default(),
            facets: SearchFacets::default(),
            limit,
            cursor,
            budget: ResponseBudget::default(),
            include_system: false,
            group_by_session: false,
            mode: RetrievalMode::Lexical,
            query_embedding: None,
        }
    }

    /// 与 [`search_req`] 同构，但携带 facet 过滤。
    fn search_req_facets(
        query: &str,
        limit: usize,
        cursor: Option<String>,
        facets: SearchFacets,
    ) -> AppRequest {
        AppRequest::Search {
            query: query.into(),
            filters: SearchFilters::default(),
            facets,
            limit,
            cursor,
            budget: ResponseBudget::default(),
            include_system: false,
            group_by_session: false,
            mode: RetrievalMode::Lexical,
            query_embedding: None,
        }
    }

    #[test]
    fn search_attaches_cjk_and_ascii_why_matched_and_suggestions() {
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &hit_id("hit00"),
            serde_json::json!({ "text": "包含数据库迁移方案 guidance" })
                .to_string()
                .into_bytes(),
        );
        cat.insert(
            &hit_id("hit01"),
            serde_json::json!({ "text": "different guidance" })
                .to_string()
                .into_bytes(),
        );
        let app = App::with_clock(cat, PagedIndex { n: 2 }, clock_t0);
        let AppResponse::Search { hits, .. } =
            app.handle(search_req("数据库 guidance", 10, None)).unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(hits[0].why_matched, vec!["数据", "据库", "guidance"]);
        assert_eq!(hits[1].why_matched, vec!["guidance"]);
        assert!(
            hits.iter()
                .all(|hit| hit.suggested_next_commands.is_empty())
        );
    }

    #[test]
    fn semantic_mode_without_index_falls_back_explicitly() {
        // #3 Q54：semantic/hybrid 在语义索引未就绪（占位 NoSemanticIndex）时
        // 必须显式降级为 lexical_fallback + warning，禁止静默切换。
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &hit_id("hit00"),
            serde_json::json!({ "text": "needle in haystack" })
                .to_string()
                .into_bytes(),
        );
        let app = App::with_clock(cat, PagedIndex { n: 2 }, clock_t0);
        for mode in [RetrievalMode::Semantic, RetrievalMode::Hybrid] {
            let response = app
                .handle(AppRequest::Search {
                    query: "needle".into(),
                    filters: SearchFilters::default(),
                    facets: SearchFacets::default(),
                    limit: 10,
                    cursor: None,
                    budget: ResponseBudget::default(),
                    include_system: false,
                    group_by_session: false,
                    mode,
                    query_embedding: Some(vec![0.1f32; 384]),
                })
                .unwrap();
            let AppResponse::Search {
                hits,
                retrieval_mode,
                fallback_warning,
                ..
            } = response
            else {
                panic!("expected Search response");
            };
            // 词法命中仍可用（结果非空），但模式如实标注降级。
            assert!(!hits.is_empty());
            assert_eq!(retrieval_mode, RetrievalMode::LexicalFallback);
            let warning = fallback_warning.expect("fallback must be surfaced");
            assert!(warning.contains("fell back to lexical"));
        }
    }

    #[test]
    fn lexical_mode_is_never_marked_fallback() {
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &hit_id("hit00"),
            serde_json::json!({ "text": "needle" })
                .to_string()
                .into_bytes(),
        );
        let app = App::with_clock(cat, PagedIndex { n: 2 }, clock_t0);
        let AppResponse::Search {
            retrieval_mode,
            fallback_warning,
            ..
        } = app
            .handle(AppRequest::Search {
                query: "needle".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(retrieval_mode, RetrievalMode::Lexical);
        assert!(fallback_warning.is_none());
    }

    #[test]
    fn search_excludes_system_and_developer_roles_by_default() {
        // R2 系统噪声默认排除：role=system/developer 的命中不进结果；无 role
        // 字段或非 JSON payload（legacy）不判为噪声。过滤发生在 offset 切片前，
        // 保证 cursor 位置指向"非系统"序列。
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &hit_id("hit00"),
            serde_json::json!({ "role": "user", "text": "needle" })
                .to_string()
                .into_bytes(),
        );
        cat.insert(
            &hit_id("hit01"),
            serde_json::json!({ "role": "system", "text": "needle" })
                .to_string()
                .into_bytes(),
        );
        cat.insert(
            &hit_id("hit02"),
            serde_json::json!({ "role": "developer", "text": "needle" })
                .to_string()
                .into_bytes(),
        );
        cat.insert(&hit_id("hit03"), b"not json".to_vec());
        let index = FixedHits(vec![
            hit_id("hit00"),
            hit_id("hit01"),
            hit_id("hit02"),
            hit_id("hit03"),
        ]);
        let app = App::with_clock(cat, index, clock_t0);
        let AppResponse::Search { hits, .. } = app.handle(search_req("needle", 10, None)).unwrap()
        else {
            panic!("expected Search response");
        };
        let kept: Vec<StableId> = hits.iter().map(|hit| hit.id.clone()).collect();
        // rank signals 重钉后同分命中按 wire id 升序（与真实 store 的
        // bm25+id 全序同一约定）——期望值按同序排序再比较。
        let mut expected = vec![hit_id("hit00"), hit_id("hit03")];
        expected.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        assert_eq!(kept, expected);
    }

    #[test]
    fn search_include_system_restores_system_developer_hits() {
        // R2 opt-in：include_system=true 时 system/developer 命中恢复进结果。
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &hit_id("hit00"),
            serde_json::json!({ "role": "system", "text": "needle" })
                .to_string()
                .into_bytes(),
        );
        let index = FixedHits(vec![hit_id("hit00")]);
        let app = App::with_clock(cat, index, clock_t0);
        let AppResponse::Search { hits, .. } = app
            .handle(AppRequest::Search {
                query: "needle".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                include_system: true,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, hit_id("hit00"));
    }

    /// semantic 请求：`Semantic` 模式 + 就绪语义索引 + 查询向量。
    fn semantic_req(query: &str, limit: usize, cursor: Option<String>) -> AppRequest {
        AppRequest::Search {
            query: query.into(),
            filters: SearchFilters::default(),
            facets: SearchFacets::default(),
            limit,
            cursor,
            budget: ResponseBudget::default(),
            include_system: false,
            group_by_session: false,
            mode: RetrievalMode::Semantic,
            query_embedding: Some(vec![0.1f32; 8]),
        }
    }

    /// 回归：系统噪声过滤不得吃掉 `has_more` 哨兵。
    ///
    /// 缺陷形态：不重排的路径（semantic）按 `offset + page + 1` 超取，末尾那个
    /// `+1` 是"还有下一页"的唯一哨兵。R2 噪声过滤在**取数之后**执行，只要窗口
    /// 内出现一条 role=system/developer 的命中，过滤后的窗口长度就恰好等于
    /// 本页消耗量，`has_more` 判为 false、不发 cursor——窗口之外的命中从此
    /// 不可达。返回码正常、`truncated` 为 false，调用方以为这就是全部结果。
    ///
    /// fixture：4 条语义命中，第 1 条是 system 噪声；page=2 时首页窗口恰好 3 条
    /// （2 条可见 + 被吃掉的哨兵），第 4 条命中只有继续分页才能拿到。
    struct MutableSemantic {
        inner: FakeSemantic,
        ready: std::cell::Cell<bool>,
        model: std::cell::RefCell<String>,
        fail: std::cell::Cell<bool>,
        nonfinite: std::cell::Cell<bool>,
        readiness_dimensions: std::cell::RefCell<Vec<usize>>,
    }
    impl SemanticIndex for MutableSemantic {
        fn index_embedding(&self, _id: &StableId, _embedding: &[f32]) -> PortResult<()> {
            Ok(())
        }
        fn query_semantic_filtered(
            &self,
            embedding: &[f32],
            limit: usize,
            filters: &SearchFilters,
            facets: &SearchFacets,
            include_system: bool,
        ) -> PortResult<Vec<SearchHit>> {
            let mut hits = self.inner.query_semantic_filtered(
                embedding,
                limit,
                filters,
                facets,
                include_system,
            )?;
            if self.nonfinite.get() {
                hits[0].score = f32::NAN;
            }
            Ok(hits)
        }
        fn is_ready(&self, query_dimension: usize) -> PortResult<bool> {
            self.readiness_dimensions.borrow_mut().push(query_dimension);
            if self.fail.get() {
                Err(PortError::Backend("synthetic readiness failure".into()))
            } else {
                Ok(self.ready.get() && query_dimension == 8)
            }
        }
        fn semantic_model_id(&self) -> PortResult<Option<String>> {
            Ok(Some(self.model.borrow().clone()))
        }
    }

    fn mutable_semantic_app() -> App<MapCatalog, FixedHits, NoResumeClaims, MutableSemantic> {
        let ids: Vec<_> = ["sem-a", "sem-b", "sem-c"]
            .iter()
            .map(|tag| StableId::native(IdKind::Message, tag))
            .collect();
        let mut cat = MapCatalog::new(7);
        for id in &ids {
            cat.insert(id, br#"{"role":"user","text":"needle"}"#.to_vec());
        }
        App::with_resume_semantic_and_clock(
            cat,
            FixedHits(ids.clone()),
            NoResumeClaims,
            MutableSemantic {
                inner: FakeSemantic(ids),
                ready: std::cell::Cell::new(true),
                model: std::cell::RefCell::new("model-a".into()),
                fail: std::cell::Cell::new(false),
                nonfinite: std::cell::Cell::new(false),
                readiness_dimensions: std::cell::RefCell::new(Vec::new()),
            },
            clock_t0,
        )
    }

    #[test]
    fn semantic_missing_query_embedding_falls_back_without_readiness_probe() {
        let app = mutable_semantic_app();
        app.semantic.fail.set(true);
        for mode in [RetrievalMode::Semantic, RetrievalMode::Hybrid] {
            let mut request = semantic_req("needle", 10, None);
            if let AppRequest::Search {
                query_embedding,
                mode: requested_mode,
                ..
            } = &mut request
            {
                *query_embedding = None;
                *requested_mode = mode;
            }
            let AppResponse::Search {
                hits,
                retrieval_mode,
                fallback_warning,
                ..
            } = app.handle(request).unwrap()
            else {
                panic!("search response")
            };
            assert_eq!(retrieval_mode, RetrievalMode::LexicalFallback);
            assert!(fallback_warning.is_some());
            assert_eq!(hits.len(), 3);
        }
        assert!(app.semantic.readiness_dimensions.borrow().is_empty());
    }

    #[test]
    fn semantic_readiness_probes_the_query_dimension_once_per_request() {
        let app = mutable_semantic_app();
        for dimension in [8, 17] {
            let mut request = semantic_req("needle", 10, None);
            if let AppRequest::Search {
                query_embedding, ..
            } = &mut request
            {
                *query_embedding = Some(vec![1.0; dimension]);
            }
            let AppResponse::Search {
                retrieval_mode,
                fallback_warning,
                ..
            } = app.handle(request).unwrap()
            else {
                panic!("search response")
            };
            assert_eq!(
                retrieval_mode,
                if dimension == 8 {
                    RetrievalMode::Semantic
                } else {
                    RetrievalMode::LexicalFallback
                }
            );
            assert_eq!(fallback_warning.is_some(), dimension != 8);
        }
        assert_eq!(*app.semantic.readiness_dimensions.borrow(), [8, 17]);
    }

    #[test]
    fn semantic_readiness_errors_and_nonfinite_scores_do_not_fallback() {
        let app = mutable_semantic_app();
        app.semantic.fail.set(true);
        assert!(matches!(
            app.handle(semantic_req("needle", 1, None)),
            Err(AppError::Port(PortError::Backend(_)))
        ));
        // A lexical request never probes the semantic backend.
        assert!(app.handle(search_req("needle", 1, None)).is_ok());
        app.semantic.fail.set(false);
        app.semantic.nonfinite.set(true);
        assert!(matches!(
            app.handle(semantic_req("needle", 1, None)),
            Err(AppError::Port(PortError::Backend(_)))
        ));
    }

    #[test]
    fn search_cursor_facet_binding_has_no_delimiter_collisions() {
        let first = SearchFacets {
            tool_kind: Some("a|tool_name=b".into()),
            tool_name: Some("c".into()),
            ..Default::default()
        };
        let second = SearchFacets {
            tool_kind: Some("a".into()),
            tool_name: Some("b|tool_name=c".into()),
            ..Default::default()
        };
        let retrieval = RetrievalBinding {
            requested: RetrievalMode::Lexical,
            effective: RetrievalMode::Lexical,
            model: None,
            embedding: None,
        };
        let digest = |facets: &SearchFacets| {
            search_query_digest(
                "needle",
                &SearchFilters::EMPTY,
                facets,
                false,
                false,
                None,
                &retrieval,
            )
        };
        assert_ne!(digest(&first), digest(&second));
        assert_ne!(
            digest(&SearchFacets::default()),
            digest(&SearchFacets {
                tool_name: Some(String::new()),
                ..Default::default()
            })
        );
    }

    #[test]
    fn semantic_cursor_binds_mode_model_dimension_and_readiness() {
        let app = mutable_semantic_app();
        let (_, token, _, _) = hits_of(app.handle(semantic_req("needle", 1, None)).unwrap());
        let token = token.expect("first page");
        assert!(matches!(
            app.handle(search_req("needle", 1, Some(token.clone()))),
            Err(AppError::Cursor(_))
        ));
        *app.semantic.model.borrow_mut() = "model-b".into();
        assert!(matches!(
            app.handle(semantic_req("needle", 1, Some(token.clone()))),
            Err(AppError::Cursor(_))
        ));
        *app.semantic.model.borrow_mut() = "model-a".into();
        let mut request = semantic_req("needle", 1, Some(token.clone()));
        if let AppRequest::Search {
            query_embedding, ..
        } = &mut request
        {
            *query_embedding = Some(vec![1.0; 17]);
        }
        assert!(matches!(app.handle(request), Err(AppError::Cursor(_))));
        app.semantic.ready.set(false);
        assert!(matches!(
            app.handle(semantic_req("needle", 1, Some(token))),
            Err(AppError::Cursor(_))
        ));
    }

    #[test]
    fn semantic_fallback_cursor_binds_requested_vector_dimension_and_content() {
        for mode in [RetrievalMode::Semantic, RetrievalMode::Hybrid] {
            let app = mutable_semantic_app();
            let request = |embedding: Vec<f32>, token: Option<String>| {
                let mut request = semantic_req("needle", 1, token);
                if let AppRequest::Search {
                    query_embedding,
                    mode: requested_mode,
                    ..
                } = &mut request
                {
                    *query_embedding = Some(embedding);
                    *requested_mode = mode;
                }
                request
            };
            let (first, token, _, _) = hits_of(app.handle(request(vec![1.0; 17], None)).unwrap());
            let token = token.expect("fallback must retain paging");
            let same = app
                .handle(request(vec![1.0; 17], Some(token.clone())))
                .unwrap();
            assert!(matches!(
                &same,
                AppResponse::Search {
                    retrieval_mode: RetrievalMode::LexicalFallback,
                    ..
                }
            ));
            let (second, _, _, _) = hits_of(same);
            assert_eq!(first.len(), 1);
            assert_eq!(second.len(), 1);
            assert_ne!(
                first, second,
                "unchanged fallback input must continue the page"
            );
            for changed in [vec![1.0; 18], vec![0.5; 17]] {
                assert!(
                    matches!(
                        app.handle(request(changed, Some(token.clone()))),
                        Err(AppError::Cursor(cursor::CursorError::Invalid(_)))
                    ),
                    "fallback cursor must bind the requested dimension and vector"
                );
            }
        }
    }

    #[test]
    fn semantic_rejects_nonfinite_query_embedding_before_backend() {
        let app = mutable_semantic_app();
        app.semantic.fail.set(true);
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let mut request = semantic_req("needle", 1, None);
            if let AppRequest::Search {
                query_embedding, ..
            } = &mut request
            {
                *query_embedding = Some(vec![bad]);
            }
            assert!(matches!(
                app.handle(request),
                Err(AppError::Domain(DomainError::InvalidRequest(_)))
            ));
        }
    }

    #[test]
    fn search_semantic_noise_filter_does_not_eat_the_has_more_sentinel() {
        let mut cat = MapCatalog::new(7);
        let noise = StableId::native(IdKind::Message, "sem-noise");
        cat.insert(
            &noise,
            serde_json::json!({ "role": "system", "text": "semantic needle" })
                .to_string()
                .into_bytes(),
        );
        let visible: Vec<StableId> = ["sem-m1", "sem-m2", "sem-m3"]
            .iter()
            .map(|tag| StableId::native(IdKind::Message, tag))
            .collect();
        for id in &visible {
            cat.insert(
                id,
                serde_json::json!({ "role": "user", "text": "semantic needle" })
                    .to_string()
                    .into_bytes(),
            );
        }
        let mut ranked = vec![noise];
        ranked.extend(visible.iter().cloned());
        let app = App::with_resume_semantic_and_clock(
            cat,
            FixedHits(Vec::new()),
            NoResumeClaims,
            FakeSemantic(ranked),
            clock_t0,
        );

        let (unpaged, _, _, _) = hits_of(
            app.handle(semantic_req("semantic needle", 10, None))
                .unwrap(),
        );
        let expected: Vec<String> = visible
            .iter()
            .map(|id| id.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(unpaged, expected, "system noise stays excluded, order kept");

        let mut paged: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..4 {
            let (ids, next, _, _) = hits_of(
                app.handle(semantic_req("semantic needle", 2, token.take()))
                    .unwrap(),
            );
            paged.extend(ids);
            token = next;
            if token.is_none() {
                break;
            }
        }
        assert!(token.is_none(), "paging must terminate");
        assert_eq!(
            paged, expected,
            "a filtered-out hit inside the fetch window must not end paging early"
        );
    }

    #[test]
    fn search_group_by_session_collapses_with_occurrences() {
        // R3 归并：每会话保留最高分命中（钉住顺序中的首个），occurrences 为该
        // 会话在扫描窗内的命中数；无归属（None）命中自成单例组。同分命中被
        // rank signals 重钉为 wire id 升序——native id 使该序可静态断言。
        let mut cat = MapCatalog::new(7);
        let session_a = StableId::native(IdKind::Session, "sess-a");
        let session_b = StableId::native(IdKind::Session, "sess-b");
        for (tag, session) in [
            ("grp-a1", Some(&session_a)),
            ("grp-a2", Some(&session_a)),
            ("grp-b1", Some(&session_b)),
            ("grp-b2", Some(&session_b)),
            ("grp-solo", None),
        ] {
            let id = StableId::native(IdKind::Message, tag);
            cat.insert(
                &id,
                serde_json::json!({ "role": "user", "text": "needle" })
                    .to_string()
                    .into_bytes(),
            );
            if let Some(session) = session {
                cat.set_session_of(&id, session);
            }
        }
        // wire id 升序（与 rank signals 的重钉序一致）：a1 < a2 < b1 < b2 < solo。
        let index = FixedHits(vec![
            StableId::native(IdKind::Message, "grp-a1"),
            StableId::native(IdKind::Message, "grp-a2"),
            StableId::native(IdKind::Message, "grp-b1"),
            StableId::native(IdKind::Message, "grp-b2"),
            StableId::native(IdKind::Message, "grp-solo"),
        ]);
        let app = App::with_clock(cat, index, clock_t0);
        let AppResponse::Search { hits, .. } = app
            .handle(AppRequest::Search {
                query: "needle".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                include_system: false,
                group_by_session: true,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(hits.len(), 3, "one group per session + singleton");
        assert_eq!(hits[0].id, StableId::native(IdKind::Message, "grp-a1"));
        assert_eq!(hits[0].occurrences, 2);
        assert_eq!(hits[0].session_id.as_deref(), Some(session_a.as_str()));
        assert_eq!(hits[1].id, StableId::native(IdKind::Message, "grp-b1"));
        assert_eq!(hits[1].occurrences, 2);
        assert_eq!(hits[1].session_id.as_deref(), Some(session_b.as_str()));
        assert_eq!(hits[2].id, StableId::native(IdKind::Message, "grp-solo"));
        assert_eq!(hits[2].occurrences, 1);
        assert!(hits[2].session_id.is_none());
    }

    #[test]
    fn search_group_by_session_default_path_keeps_occurrences_one() {
        // R3 默认路径（group_by_session=false）保持不变：不归并、逐命中返回，
        // occurrences 恒为 1（序列化时省略该键，与既有输出字节兼容）。
        // 同分命中被 rank signals 重钉为 wire id 升序——输入按该序喂入。
        let mut cat = MapCatalog::new(7);
        let session_a = StableId::native(IdKind::Session, "sess-a");
        for tag in ["grp-a1", "grp-a2"] {
            let id = StableId::native(IdKind::Message, tag);
            cat.insert(
                &id,
                serde_json::json!({ "role": "user", "text": "needle" })
                    .to_string()
                    .into_bytes(),
            );
            cat.set_session_of(&id, &session_a);
        }
        let index = FixedHits(vec![
            StableId::native(IdKind::Message, "grp-a1"),
            StableId::native(IdKind::Message, "grp-a2"),
        ]);
        let app = App::with_clock(cat, index, clock_t0);
        let AppResponse::Search { hits, .. } = app.handle(search_req("needle", 10, None)).unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, StableId::native(IdKind::Message, "grp-a1"));
        assert_eq!(hits[0].occurrences, 1);
        assert_eq!(hits[1].occurrences, 1);
    }

    /// 回归：归并模式下被字节闸截断的页必须继续发 cursor。
    ///
    /// 缺陷形态：`has_more` 曾在字节 clamp **之前**按"是否存在第 page+1 组"
    /// 判定。当组总数不超过页大小、而字节预算只装得下前几组时，clamp 把本页
    /// 削短却仍报 `has_more = false` → 无 next_cursor，被削掉的组从此不可达。
    /// 逐命中路径（`has_more = scanned_len > consumed`）不存在该问题，两条
    /// 路径对同一输入给出不同答案，正是缺陷的判据。
    ///
    /// fixture：4 组、page=10（组数不足以触发 page+1 哨兵）、字节预算只容 1 组。
    #[test]
    fn search_group_by_session_byte_truncated_page_keeps_paging() {
        let mut cat = MapCatalog::new(7);
        let tags = ["cut-a", "cut-b", "cut-c", "cut-d"];
        for tag in tags {
            let id = StableId::native(IdKind::Message, tag);
            // 每组一条命中，正文足够长使字节闸每页只放过一组。
            cat.insert(
                &id,
                serde_json::json!({ "role": "user", "text": "x".repeat(2000) })
                    .to_string()
                    .into_bytes(),
            );
            cat.set_session_of(&id, &StableId::native(IdKind::Session, tag));
        }
        let index = FixedHits(
            tags.iter()
                .map(|tag| StableId::native(IdKind::Message, tag))
                .collect(),
        );
        let app = App::with_clock(cat, index, clock_t0);
        let grouped_req = |cursor: Option<String>| AppRequest::Search {
            query: "grouped needle".into(),
            filters: SearchFilters::default(),
            facets: SearchFacets::default(),
            limit: 10,
            cursor,
            budget: ResponseBudget {
                max_response_bytes: 4096,
                ..Default::default()
            },
            include_system: false,
            group_by_session: true,
            mode: RetrievalMode::Lexical,
            query_embedding: None,
        };

        let (first, token, _, truncation) = hits_of(app.handle(grouped_req(None)).unwrap());
        assert!(
            truncation.truncated && !first.is_empty() && first.len() < tags.len(),
            "byte gate must cut a non-empty prefix: kept {first:?}, {truncation:?}"
        );
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_RESPONSE_BYTES)
        );
        assert!(
            token.is_some(),
            "a byte-truncated grouped page must still hand out a cursor"
        );

        let mut paged = first;
        let mut token = token;
        while let Some(cursor) = token.take() {
            let (ids, next, _, _) = hits_of(app.handle(grouped_req(Some(cursor))).unwrap());
            assert!(!ids.is_empty(), "paging must advance, not spin");
            paged.extend(ids);
            token = next;
        }
        let expected: Vec<String> = tags
            .iter()
            .map(|tag| StableId::native(IdKind::Message, tag).as_str().to_string())
            .collect();
        assert_eq!(
            paged, expected,
            "every group must stay reachable across byte-truncated pages"
        );
    }

    #[test]
    fn search_text_window_keeps_late_literal_match_visible() {
        // 命中位于前缀之外：text 摘要不再是固定前缀，而是包含该命中的窗口；
        // why_matched 仍按完整正文派生（证据来源与显示窗口无关的回归断言）。
        let mut cat = MapCatalog::new(7);
        let mut text = "x".repeat(1500);
        text.push_str(" needle");
        cat.insert(
            &hit_id("hit00"),
            serde_json::json!({ "text": text }).to_string().into_bytes(),
        );
        let app = App::with_clock(cat, PagedIndex { n: 1 }, clock_t0);
        let AppResponse::Search { hits, .. } = app
            .handle(AppRequest::Search {
                query: "needle".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget {
                    max_snippet_chars: 8,
                    ..Default::default()
                },
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(hits[0].text.as_deref(), Some("x needle"));
        assert_eq!(hits[0].why_matched, vec!["needle"]);
    }

    #[test]
    fn search_json_escaped_guidance_counts_toward_byte_budget() {
        let mut cat = MapCatalog::new(7);
        for tag in ["hit00", "hit01", "hit02", "hit03"] {
            let text = format!("{} quoted \\\"needle\\\"", "x".repeat(1000));
            cat.insert(
                &hit_id(tag),
                serde_json::json!({ "text": text }).to_string().into_bytes(),
            );
        }
        let app = App::with_clock(cat, PagedIndex { n: 4 }, clock_t0);
        let response = app
            .handle(AppRequest::Search {
                query: "quoted needle".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget {
                    max_response_bytes: budget::MIN_RESPONSE_BYTES,
                    ..Default::default()
                },
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap();
        let (kept, next, _, truncation) = hits_of(response);
        assert!(!kept.is_empty() && kept.len() < 4);
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_RESPONSE_BYTES)
        );
        assert!(next.is_some());
    }

    #[test]
    fn search_guidance_deterministic_across_identical_queries() {
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &hit_id("hit00"),
            serde_json::json!({ "text": "数据库 guidance" })
                .to_string()
                .into_bytes(),
        );
        let app = App::with_clock(&cat, PagedIndex { n: 1 }, clock_t0);
        let first = app.handle(search_req("数据库 guidance", 10, None)).unwrap();
        let second = app.handle(search_req("数据库 guidance", 10, None)).unwrap();
        assert_eq!(first, second);
    }

    // ---- rank signals（competitor-borrowings #1）：lexical 时效衰减 + sidechain 惩罚 ----

    /// 固定 rank 时钟：2026-08-25T00:00:00Z（与 CLI e2e 的 `ASG_CLOCK_MS` 同值）。
    fn rank_clock() -> i64 {
        1_787_616_000_000
    }

    #[test]
    fn search_cursor_pins_recency_clock_and_original_expiry_across_pages() {
        thread_local! {
            static NOW: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
        }
        fn moving_clock() -> i64 {
            NOW.with(std::cell::Cell::get)
        }
        for grouped in [false, true] {
            NOW.with(|now| now.set(rank_clock()));
            let ids: Vec<_> = ["clock-a", "clock-b", "clock-c"]
                .iter()
                .map(|tag| StableId::native(IdKind::Message, tag))
                .collect();
            let mut catalog = MapCatalog::new(7);
            for id in &ids {
                catalog.insert(id, br#"{"text":"needle"}"#.to_vec());
            }
            // Rank 1 crosses rank 2's constant score five minutes after the
            // first page, well within the cursor's fifteen-minute lifetime.
            catalog.insert(
                &ids[0],
                br#"{"text":"needle","timestamp":"2026-08-24T07:11:34Z"}"#.to_vec(),
            );
            let app = App::with_clock(
                catalog,
                ScoredHits(
                    ids.iter()
                        .enumerate()
                        .map(|(rank, id)| (id.clone(), 1.0 / (61 + rank) as f32))
                        .collect(),
                ),
                moving_clock,
            );
            let request = |token| {
                let mut request = search_req("needle", 1, token);
                if let AppRequest::Search {
                    group_by_session, ..
                } = &mut request
                {
                    *group_by_session = grouped;
                }
                request
            };
            let (first, token, _, _) = hits_of(app.handle(request(None)).unwrap());
            assert_eq!(first, [ids[0].as_str()]);
            NOW.with(|now| now.set(rank_clock() + 10 * 60 * 1_000));
            let (fresh, _, _, _) = hits_of(app.handle(request(None)).unwrap());
            assert_eq!(
                fresh,
                [ids[1].as_str()],
                "fixture must change fresh ranking"
            );
            let (second, token, _, _) = hits_of(app.handle(request(token)).unwrap());
            assert_eq!(
                second,
                [ids[1].as_str()],
                "continuation must not repeat page one"
            );
            let token = token.expect("third page");
            NOW.with(|now| now.set(rank_clock() + 14 * 60 * 1_000));
            let (third, _, _, _) = hits_of(app.handle(request(Some(token.clone()))).unwrap());
            assert_eq!(third, [ids[2].as_str()]);
            NOW.with(|now| now.set(rank_clock() + cursor::DEFAULT_TTL_MS));
            assert!(matches!(
                app.handle(request(Some(token))),
                Err(AppError::Cursor(cursor::CursorError::Expired(_)))
            ));
        }
    }

    fn assert_current_repo_owner(matched_owner: bool) {
        struct OwnerIndex {
            hits: ScoredHits,
            owner: Option<String>,
        }
        impl SearchIndex for OwnerIndex {
            fn index(&self, _id: &StableId, _text: &str) -> PortResult<()> {
                Ok(())
            }
            fn query_filtered(
                &self,
                query: SearchQuery<'_>,
                limit: usize,
            ) -> PortResult<Vec<SearchHit>> {
                assert_eq!(query.filters.repo.as_deref(), Some("repo-b"));
                let mut hits = self.hits.query_filtered(query, limit)?;
                hits[0].session_id = self.owner.clone();
                Ok(hits)
            }
        }
        let shared = StableId::native(IdKind::Message, "shared-owner");
        let a = StableId::native(IdKind::Session, "owner-a");
        let b = StableId::native(IdKind::Session, "owner-b");
        let mut cat = MapCatalog::new(7);
        // A is the unfiltered owner; the adapter selects B for this repo filter.
        cat.set_session_of(&shared, &a);
        cat.repo_slugs.insert(a.as_str().into(), "repo-a".into());
        cat.repo_slugs.insert(b.as_str().into(), "repo-b".into());
        cat.insert(&shared, br#"{"text":"needle"}"#.to_vec());
        for current in ["repo-a", "repo-b"] {
            let app = App::with_clock(
                &cat,
                OwnerIndex {
                    hits: ScoredHits(vec![(shared.clone(), 1.0)]),
                    owner: matched_owner.then(|| b.as_str().to_string()),
                },
                rank_clock,
            )
            .with_current_repo(Some(current.into()));
            let mut request = search_req("needle", 10, None);
            let AppRequest::Search { filters, .. } = &mut request else {
                unreachable!()
            };
            filters.repo = Some("repo-b".into());
            let AppResponse::Search { hits, .. } = app.handle(request).unwrap() else {
                panic!("expected Search response");
            };
            assert_eq!(hits.len(), 1);
            let owner = if matched_owner { &b } else { &a };
            assert_eq!(hits[0].session_id.as_deref(), Some(owner.as_str()));
            let boosted = current == if matched_owner { "repo-b" } else { "repo-a" };
            assert_eq!(hits[0].score, ranking::final_score(1.0, 0, false, boosted));
        }
    }

    #[test]
    fn search_current_repo_uses_filtered_shared_message_owner() {
        assert_current_repo_owner(true);
    }

    #[test]
    fn search_current_repo_preserves_legacy_missing_owner_fallback() {
        assert_current_repo_owner(false);
    }

    #[test]
    fn search_current_repo_rejects_pre_owner_fix_cursor() {
        // Exact pre-fix binding: no data or request change can invalidate this
        // cursor unless the owner-selection ranking revision is also bound.
        let legacy_digest = cursor::digest_query(
            &serde_json::json!({
                "version": "search-v2-rrf60-signals-v2-clock",
                "result_set": "search", "query": "needle", "providers": [],
                "since": null, "until": null, "repo": null,
                "facets": { "sidechain": "include", "tool_kind": null, "tool_name": null },
                "include_system": false, "group_by_session": false,
                "current_repo": "repo-b", "requested_mode": "lexical",
                "effective_mode": "lexical", "model": null, "dimension": null,
                "embedding": null, "rank_window": RANK_SCAN_WINDOW,
                "group_factor": GROUP_SCAN_FACTOR,
            })
            .to_string(),
        );
        let token = cursor::issue(&cursor::CursorClaims {
            contract_major: cursor::SUPPORTED_CONTRACT_MAJOR,
            generation: 7,
            issued_at_ms: rank_clock(),
            expires_at_ms: rank_clock() + cursor::DEFAULT_TTL_MS,
            query_digest: legacy_digest,
            sort_digest: SORT_SCORE_DESC.into(),
            result_set: None,
            offset: 1,
        })
        .into_string();
        let app = App::with_clock(MapCatalog::new(7), ScoredHits(vec![]), rank_clock)
            .with_current_repo(Some("repo-b".into()));
        assert!(matches!(
            app.handle(search_req("needle", 10, Some(token))),
            Err(AppError::Cursor(cursor::CursorError::Invalid(_)))
        ));
    }

    #[test]
    fn search_lexical_ranking_prefers_newer_message_under_fixed_clock() {
        let old = StableId::native(IdKind::Message, "rank-old");
        let new = StableId::native(IdKind::Message, "rank-new");
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &old,
            serde_json::json!({ "text": "rank needle", "timestamp": "2026-01-01T00:00:00Z" })
                .to_string()
                .into_bytes(),
        );
        cat.insert(
            &new,
            serde_json::json!({ "text": "rank needle", "timestamp": "2026-08-24T00:00:00Z" })
                .to_string()
                .into_bytes(),
        );
        // 假索引按 [old, new] 序返回、bm25 全等——重排必须来自时效衰减。
        let app = App::with_clock(
            cat,
            ScoredHits(vec![(old.clone(), 1.0), (new.clone(), 1.0)]),
            rank_clock,
        );
        let (ids, _, _, _) = hits_of(app.handle(search_req("rank needle", 10, None)).unwrap());
        assert_eq!(
            ids,
            vec![new.as_str().to_string(), old.as_str().to_string()],
            "newer message must rank first"
        );
    }

    #[test]
    fn search_lexical_ranking_demotes_sidechain_under_equal_relevance() {
        let main = StableId::native(IdKind::Message, "rank-main");
        let side = StableId::native(IdKind::Message, "rank-side");
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &main,
            serde_json::json!({ "text": "side needle", "is_sidechain": false })
                .to_string()
                .into_bytes(),
        );
        cat.insert(
            &side,
            serde_json::json!({ "text": "side needle", "is_sidechain": true })
                .to_string()
                .into_bytes(),
        );
        let app = App::with_clock(
            cat,
            ScoredHits(vec![(side.clone(), 2.0), (main.clone(), 2.0)]),
            rank_clock,
        );
        let (ids, _, _, _) = hits_of(app.handle(search_req("side needle", 10, None)).unwrap());
        assert_eq!(
            ids,
            vec![main.as_str().to_string(), side.as_str().to_string()],
            "sidechain hit must rank after equal-relevance mainline hit"
        );
    }

    #[test]
    fn search_lexical_ranking_is_deterministic_across_identical_queries() {
        let old = StableId::native(IdKind::Message, "rank-old");
        let new = StableId::native(IdKind::Message, "rank-new");
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &old,
            serde_json::json!({ "text": "rank needle", "timestamp": "2026-01-01T00:00:00Z" })
                .to_string()
                .into_bytes(),
        );
        cat.insert(
            &new,
            serde_json::json!({ "text": "rank needle", "timestamp": "2026-08-24T00:00:00Z" })
                .to_string()
                .into_bytes(),
        );
        let app = App::with_clock(
            cat,
            ScoredHits(vec![(old.clone(), 1.0), (new.clone(), 1.0)]),
            rank_clock,
        );
        let first = app.handle(search_req("rank needle", 10, None)).unwrap();
        let second = app.handle(search_req("rank needle", 10, None)).unwrap();
        assert_eq!(first, second, "same clock + same query must be identical");
    }

    #[test]
    fn search_lexical_ranking_pages_partition_the_pinned_ordering() {
        // 时效重排后的钉住排序必须同样支持不重不漏分页（cursor 绑定 query+sort，
        // 同 clock 下 offset 续读稳定）。三条命中且每页 fetch 窗口 ≥ 3（offset 0
        // 时 fetch = page+1 = 3 恰好覆盖全集），窗口内的重排序与全集一致，
        // 三页拼接 == 不分页结果。
        let hits: Vec<(StableId, f32)> = (0..3)
            .map(|i| {
                (
                    StableId::native(IdKind::Message, &format!("rank-p{i}")),
                    1.0,
                )
            })
            .collect();
        let mut cat = MapCatalog::new(7);
        for (index, (id, _)) in hits.iter().enumerate() {
            cat.insert(
                id,
                serde_json::json!({ "text": "rank paged", "timestamp": format!(
                    "2026-08-0{}T00:00:00Z", index + 1
                ) })
                .to_string()
                .into_bytes(),
            );
        }
        let app = App::with_clock(cat, ScoredHits(hits), rank_clock);
        let (unpaged, _, _, _) = hits_of(app.handle(search_req("rank paged", 10, None)).unwrap());
        assert_eq!(
            unpaged,
            vec!["msg_v1_rank-p2", "msg_v1_rank-p1", "msg_v1_rank-p0"],
            "newest-first pinned order"
        );

        let mut paged: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..2 {
            let (ids, next, _, truncation) = hits_of(
                app.handle(search_req("rank paged", 2, token.take()))
                    .unwrap(),
            );
            assert!(!truncation.truncated);
            paged.extend(ids);
            token = next;
            if token.is_none() {
                break;
            }
        }
        assert_eq!(paged, unpaged);
        assert!(token.is_none());
    }

    /// 回归：重排后的分页必须不重不漏，且**扫描窗口不得随 offset 变化**。
    ///
    /// 缺陷形态：取数窗口曾是 `offset + page + 1`，于是第 1 页只看到全集的一个
    /// 前缀、第 2 页看到更大的前缀。当 bm25 序与重排序不一致时，两页各自在
    /// **不同的集合**上重排，拼接结果既重复又漏命中——返回码正常，结果静默错误。
    ///
    /// fixture 用等值 bm25 + 单调变新的时间戳，使时效成为唯一排序因素（重排序
    /// 恰为索引返回序的反序）。bm25 若有较大跨度会盖过衰减区间、把重排变成
    /// no-op，缺陷就观察不到——这正是既有分页测试用 3 条命中（fetch 窗口恰好
    /// 覆盖全集）时未能暴露它的原因。
    #[test]
    fn search_rank_pages_do_not_depend_on_the_cursor_offset() {
        let ids: Vec<StableId> = (0..4)
            .map(|i| StableId::native(IdKind::Message, &format!("rank-off{i}")))
            .collect();
        let mut cat = MapCatalog::new(7);
        // index 越大 → 时间越新 → 重排越靠前，与索引返回序完全相反。
        for (index, id) in ids.iter().enumerate() {
            cat.insert(
                id,
                serde_json::json!({
                    "text": "rank offset",
                    "timestamp": format!("2026-08-1{index}T00:00:00Z"),
                })
                .to_string()
                .into_bytes(),
            );
        }
        let scored: Vec<(StableId, f32)> = ids.iter().map(|id| (id.clone(), 1.0)).collect();
        let app = App::with_clock(cat, ScoredHits(scored), rank_clock);

        // 一次取全 4 条即钉住排序的真值：最新在前。
        let (unpaged, _, _, _) = hits_of(app.handle(search_req("rank offset", 10, None)).unwrap());
        assert_eq!(
            unpaged,
            vec![
                "msg_v1_rank-off3",
                "msg_v1_rank-off2",
                "msg_v1_rank-off1",
                "msg_v1_rank-off0",
            ],
            "unpaged pinned order must be newest-first"
        );

        let mut paged: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..4 {
            let (page_ids, next, _, truncation) = hits_of(
                app.handle(search_req("rank offset", 2, token.take()))
                    .unwrap(),
            );
            assert!(!truncation.truncated);
            paged.extend(page_ids);
            token = next;
            if token.is_none() {
                break;
            }
        }
        assert!(token.is_none(), "paging must terminate");
        assert_eq!(
            paged, unpaged,
            "paged concatenation must equal the unpaged pinned order: a window that \
             grows with the cursor offset re-ranks a different set on every page"
        );
    }

    /// hybrid 请求：`Hybrid` 模式 + 就绪语义索引 + 查询向量（三者齐备才真正
    /// 走 RRF 融合，缺一即降级 lexical_fallback）。
    fn hybrid_req(query: &str, limit: usize, cursor: Option<String>) -> AppRequest {
        AppRequest::Search {
            query: query.into(),
            filters: SearchFilters::default(),
            facets: SearchFacets::default(),
            limit,
            cursor,
            budget: ResponseBudget::default(),
            include_system: false,
            group_by_session: false,
            mode: RetrievalMode::Hybrid,
            query_embedding: Some(vec![0.1f32; 8]),
        }
    }

    /// 回归：hybrid 的 RRF 融合同样是"应用层重排"，扫描窗口必须与 offset 无关。
    ///
    /// 缺陷形态：hybrid 取数窗口曾是 `offset + page + 1`，两路召回各截同一前缀
    /// 后再融合。RRF 分数 `1/(k+rank)` 在**单路**内随窗口增长是稳定前缀，但
    /// **两路都命中**的文档拿到两份加分（`1/(k+r_lex) + 1/(k+r_sem)`），可以
    /// 一举超过任何单路命中——只要它的两个 rank 落在小窗口之外，它就在第 1 页
    /// 不可见、在第 2 页跃居榜首，把整个融合序整体下移。于是第 2 页原样重复
    /// 第 1 页的命中，而真正的最高分命中永远不出现。返回码正常，结果静默错误。
    ///
    /// fixture：两路各 5 条，前 3 条互不相交、后 2 条两路共有（rank 4/5）。
    /// page=2 时第 1 页窗口 3 只看到不相交部分，第 2 页窗口 5 才看到共有部分。
    #[test]
    fn search_hybrid_pages_do_not_depend_on_the_cursor_offset() {
        let tags = [
            "lex1", "lex2", "lex3", "sem1", "sem2", "sem3", "dup1", "dup2",
        ];
        let mut cat = MapCatalog::new(7);
        for tag in tags {
            cat.insert(
                &StableId::native(IdKind::Message, tag),
                serde_json::json!({ "role": "user", "text": "hybrid needle" })
                    .to_string()
                    .into_bytes(),
            );
        }
        let native = |tag: &str| StableId::native(IdKind::Message, tag);
        // 两路召回的后两位是同一对文档（dup1/dup2）——它们各拿两份 RRF 加分。
        let lexical = FixedHits(vec![
            native("lex1"),
            native("lex2"),
            native("lex3"),
            native("dup1"),
            native("dup2"),
        ]);
        let semantic = FakeSemantic(vec![
            native("sem1"),
            native("sem2"),
            native("sem3"),
            native("dup1"),
            native("dup2"),
        ]);
        let app =
            App::with_resume_semantic_and_clock(cat, lexical, NoResumeClaims, semantic, rank_clock);

        // 一次取全 8 条即融合后的钉住排序真值：两路共有的 dup1/dup2 居首。
        let (unpaged, _, _, _) =
            hits_of(app.handle(hybrid_req("hybrid needle", 10, None)).unwrap());
        assert_eq!(
            unpaged,
            vec![
                "msg_v1_dup1",
                "msg_v1_dup2",
                "msg_v1_lex1",
                "msg_v1_sem1",
                "msg_v1_lex2",
                "msg_v1_sem2",
                "msg_v1_lex3",
                "msg_v1_sem3",
            ],
            "documents hit by both retrievers must outrank single-retriever hits"
        );

        let mut paged: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..8 {
            let (page_ids, next, _, truncation) = hits_of(
                app.handle(hybrid_req("hybrid needle", 2, token.take()))
                    .unwrap(),
            );
            assert!(!truncation.truncated);
            paged.extend(page_ids);
            token = next;
            if token.is_none() {
                break;
            }
        }
        assert!(token.is_none(), "paging must terminate");
        assert_eq!(
            paged, unpaged,
            "paged concatenation must equal the unpaged fused order: a fusion window \
             that grows with the cursor offset re-fuses a different set on every page"
        );
    }

    #[test]
    fn search_guidance_uses_real_session_id_in_suggestions() {
        let mut fixture = ctx_fixture();
        fixture.store.catalog.insert(
            &fixture.leaf,
            serde_json::json!({ "text": "leaf guidance" })
                .to_string()
                .into_bytes(),
        );
        let app = App::with_clock(
            &fixture.store,
            FixedHits(vec![fixture.leaf.clone()]),
            clock_t0,
        );
        let AppResponse::Search { hits, .. } =
            app.handle(search_req("guidance", 10, None)).unwrap()
        else {
            panic!("expected Search response");
        };
        let hit = &hits[0];
        assert_eq!(hit.session_id.as_deref(), Some(fixture.session.as_str()));
        assert_eq!(hit.why_matched, vec!["guidance"]);
        assert!(hit.suggested_next_commands[0].contains(hit.id.as_str()));
        assert!(
            hit.suggested_next_commands
                .iter()
                .all(|command| command.contains(fixture.session.as_str()))
        );
    }

    #[test]
    fn search_guidance_omits_get_message_when_session_id_missing() {
        let mut cat = MapCatalog::new(7);
        cat.insert(
            &hit_id("hit00"),
            serde_json::json!({ "text": "needle" })
                .to_string()
                .into_bytes(),
        );
        let app = App::with_clock(cat, PagedIndex { n: 1 }, clock_t0);
        let AppResponse::Search { hits, .. } = app.handle(search_req("needle", 10, None)).unwrap()
        else {
            panic!("expected Search response");
        };
        assert!(hits[0].session_id.is_none());
        assert!(hits[0].suggested_next_commands.is_empty());
    }

    fn filtered_search_req(
        query: &str,
        limit: usize,
        cursor: Option<String>,
        filters: SearchFilters,
    ) -> AppRequest {
        AppRequest::Search {
            query: query.into(),
            filters,
            facets: SearchFacets::default(),
            limit,
            cursor,
            budget: ResponseBudget::default(),
            include_system: false,
            group_by_session: false,
            mode: RetrievalMode::Lexical,
            query_embedding: None,
        }
    }

    fn seconds_instant(unix_seconds: i64) -> SearchInstant {
        SearchInstant {
            unix_seconds,
            nanosecond: 0,
        }
    }

    #[test]
    fn search_rejects_since_not_before_until() {
        for (since, until) in [
            (seconds_instant(1_000), seconds_instant(1_000)),
            (seconds_instant(2_000), seconds_instant(1_000)),
        ] {
            let err = app()
                .handle(filtered_search_req(
                    "q",
                    5,
                    None,
                    SearchFilters {
                        providers: Vec::new(),
                        since: Some(since),
                        until: Some(until),
                        repo: None,
                    },
                ))
                .expect_err("since >= until must be rejected");
            assert!(matches!(
                err,
                AppError::Domain(DomainError::InvalidRequest(_))
            ));
        }
    }

    #[test]
    fn search_cursor_is_bound_to_normalized_filters() {
        use agent_session_grep_ports::SearchProvider;

        let app = App::with_clock(FakeCatalog, PagedIndex { n: 5 }, clock_t0);
        let issued_filters = SearchFilters {
            providers: vec![SearchProvider::Codex, SearchProvider::Claude],
            since: Some(seconds_instant(1_000)),
            until: None,
            repo: None,
        };
        let (_, next, _, _) = hits_of(
            app.handle(filtered_search_req("q", 2, None, issued_filters))
                .unwrap(),
        );
        let normalized_equivalent = SearchFilters {
            providers: vec![
                SearchProvider::Claude,
                SearchProvider::Codex,
                SearchProvider::Claude,
            ],
            since: Some(seconds_instant(1_000)),
            until: None,
            repo: None,
        };
        assert!(
            app.handle(filtered_search_req(
                "q",
                2,
                next.clone(),
                normalized_equivalent,
            ))
            .is_ok()
        );

        let mutated = SearchFilters {
            providers: Vec::new(),
            since: Some(seconds_instant(2_000)),
            until: None,
            repo: None,
        };
        let err = app
            .handle(filtered_search_req("q", 2, next.clone(), mutated))
            .unwrap_err();
        assert!(matches!(
            err,
            AppError::Cursor(cursor::CursorError::Invalid(_))
        ));

        // repo 维度同样绑定进 digest：同 query 同 provider/time、不同 repo
        // 的旧令牌必须失效（schema v16）。
        let mutated_repo = SearchFilters {
            providers: vec![SearchProvider::Codex, SearchProvider::Claude],
            since: Some(seconds_instant(1_000)),
            until: None,
            repo: Some("github.com/o/app".into()),
        };
        let err = app
            .handle(filtered_search_req("q", 2, next.clone(), mutated_repo))
            .unwrap_err();
        assert!(matches!(
            err,
            AppError::Cursor(cursor::CursorError::Invalid(_))
        ));

        let err = app.handle(search_req("q", 2, next)).unwrap_err();
        assert!(matches!(
            err,
            AppError::Cursor(cursor::CursorError::Invalid(_))
        ));

        let (_, plain_next, _, _) = hits_of(app.handle(search_req("q", 2, None)).unwrap());
        let err = app
            .handle(filtered_search_req(
                "q",
                2,
                plain_next,
                SearchFilters {
                    providers: vec![SearchProvider::Claude],
                    since: None,
                    until: None,
                    repo: None,
                },
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            AppError::Cursor(cursor::CursorError::Invalid(_))
        ));
    }

    #[test]
    fn parse_search_instant_normalizes_offsets_and_compact_time() {
        let z = parse_search_instant("2026-07-28T12:00:00Z").unwrap();
        for parsed in [
            parse_search_instant("2026-07-28T14:00:00+02:00").unwrap(),
            parse_search_instant("2026-07-28T140000+0200").unwrap(),
            parse_search_instant("2026-07-28 07:00:00-05:00").unwrap(),
        ] {
            assert_eq!(parsed, z);
        }
        assert_eq!(z.unix_seconds, 1_785_240_000);
        assert_eq!(z.nanosecond, 0);
    }

    #[test]
    fn parse_search_instant_validates_ranges_and_precision() {
        let instant = parse_search_instant("2026-07-28T00:00:00.123456789Z").unwrap();
        assert_eq!(instant.nanosecond, 123_456_789);
        for value in [
            "2026-07-28T12:00:00",
            "2026-02-29T00:00:00Z",
            "2026-07-28T24:00:00Z",
            "2026-07-28T00:00:00.1234567891Z",
            "2026-07-28T12:00Z",
            "2026-01-01T-1:00:00Z",
            "2026-01-01T00:-1:00Z",
            "2026-01-01T00:00:-1Z",
            "2026-01-01T-0:00:00Z",
            "2026-01-01T00:+0:00Z",
            "2026-01-01T00:00:00+-0:00",
            "2026-01-01T00:00:00+00:+0",
            "2026-01-01T00:00:00-25:00",
            "1h",
            "",
        ] {
            assert!(parse_search_instant(value).is_none(), "{value:?}");
        }
        assert!(parse_search_instant("2024-02-29T00:00:00Z").is_some());
        assert!(
            parse_search_instant(&format!("{}-12-31T00:00:00Z", i64::MAX)).is_none(),
            "extreme year arithmetic must fail closed instead of panicking or wrapping"
        );
    }

    #[test]
    fn parse_relative_search_instant_uses_injected_clock_and_rejects_negative_amounts() {
        let now_ms = 1_000_000_000;
        assert_eq!(
            parse_relative_search_instant("1h", now_ms).unwrap(),
            SearchInstant::from_unix_millis(now_ms - 3_600_000)
        );
        assert_eq!(
            parse_relative_search_instant(" 2d ", now_ms).unwrap(),
            SearchInstant::from_unix_millis(now_ms - 2 * 86_400_000)
        );
        for value in ["", "h", "1m", "-1h", "0h", "1.5h"] {
            assert!(parse_relative_search_instant(value, now_ms).is_none());
        }
    }

    fn hits_of(resp: AppResponse) -> (Vec<String>, Option<String>, u64, Truncation) {
        match resp {
            AppResponse::Search {
                hits,
                next_cursor,
                generation,
                truncation,
                retrieval_mode: _,
                fallback_warning: _,
            } => (
                hits.iter().map(|h| h.id.as_str().to_string()).collect(),
                next_cursor,
                generation,
                truncation,
            ),
            other => panic!("expected Search response, got {other:?}"),
        }
    }

    #[test]
    fn search_pages_partition_the_pinned_ordering() {
        let app = App::with_clock(FakeCatalog, PagedIndex { n: 5 }, clock_t0);
        let (unpaged, _, _, _) = hits_of(app.handle(search_req("q", 5, None)).unwrap());
        assert_eq!(unpaged.len(), 5);

        let mut paged: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..3 {
            let (ids, next, generation, truncation) =
                hits_of(app.handle(search_req("q", 2, token.take())).unwrap());
            assert_eq!(generation, 7);
            assert!(!truncation.truncated);
            paged.extend(ids);
            token = next;
            if token.is_none() {
                break;
            }
        }
        // 三页恰好不重不漏地划分同一钉住排序。
        assert_eq!(paged, unpaged);
        assert!(token.is_none());
    }

    #[test]
    fn search_cursor_is_bound_to_query() {
        let app = App::with_clock(FakeCatalog, PagedIndex { n: 5 }, clock_t0);
        let (_, next, _, _) = hits_of(app.handle(search_req("alpha", 2, None)).unwrap());
        let err = app
            .handle(search_req("beta", 2, Some(next.unwrap())))
            .unwrap_err();
        assert!(
            matches!(err, AppError::Cursor(cursor::CursorError::Invalid(_))),
            "{err}"
        );
    }

    #[test]
    fn search_cursor_rejects_generation_bump() {
        let cat = MapCatalog::new(7);
        let app = App::with_clock(&cat, PagedIndex { n: 5 }, clock_t0);
        let (_, next, _, _) = hits_of(app.handle(search_req("q", 2, None)).unwrap());
        cat.generation.set(8);
        let err = app.handle(search_req("q", 2, next)).unwrap_err();
        assert!(
            matches!(
                err,
                AppError::Cursor(cursor::CursorError::GenerationMismatch {
                    cursor: 7,
                    active: 8
                })
            ),
            "{err}"
        );
    }

    #[test]
    fn search_cursor_is_bound_to_facets() {
        // 同一查询串、不同 facet 的旧令牌必须失效（设计：facet 绑定进 digest）——
        // 否则换 facet 翻页会跨过滤条件续读。
        let app = App::with_clock(FakeCatalog, PagedIndex { n: 5 }, clock_t0);
        let (_, next, _, _) = hits_of(app.handle(search_req("q", 2, None)).unwrap());
        let err = app
            .handle(search_req_facets(
                "q",
                2,
                next,
                SearchFacets {
                    sidechain: SidechainFacet::MainOnly,
                    ..Default::default()
                },
            ))
            .unwrap_err();
        assert!(
            matches!(err, AppError::Cursor(cursor::CursorError::Invalid(_))),
            "{err}"
        );
    }

    #[test]
    fn search_cursor_expires_after_ttl() {
        let issued = App::with_clock(FakeCatalog, PagedIndex { n: 5 }, clock_t0);
        let (_, next, _, _) = hits_of(issued.handle(search_req("q", 2, None)).unwrap());
        let later = App::with_clock(FakeCatalog, PagedIndex { n: 5 }, clock_after_ttl);
        let err = later.handle(search_req("q", 2, next)).unwrap_err();
        assert!(
            matches!(err, AppError::Cursor(cursor::CursorError::Expired(_))),
            "{err}"
        );
    }

    #[test]
    fn search_byte_gate_truncates_and_cursor_resumes_at_kept_offset() {
        let app = App::with_clock(FakeCatalog, PagedIndex { n: 100 }, clock_t0);
        let resp = app
            .handle(AppRequest::Search {
                query: "q".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 60,
                cursor: None,
                budget: ResponseBudget {
                    max_response_bytes: budget::MIN_RESPONSE_BYTES,
                    ..Default::default()
                },
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap();
        let (ids, next, _, truncation) = hits_of(resp);
        assert!(!ids.is_empty() && ids.len() < 60, "kept {}", ids.len());
        assert!(truncation.truncated);
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_RESPONSE_BYTES)
        );
        // 续读令牌钉在字节闸切断处，而不是页边界。
        let claims = cursor::verify(
            &next.expect("byte-cut page must issue a cursor"),
            &cursor::CursorExpectations {
                now_ms: clock_t0(),
                active_generation: 7,
                query_digest: search_query_digest(
                    "q",
                    &SearchFilters::EMPTY,
                    &SearchFacets::default(),
                    false,
                    false,
                    None,
                    &RetrievalBinding {
                        requested: RetrievalMode::Lexical,
                        effective: RetrievalMode::Lexical,
                        model: None,
                        embedding: None,
                    },
                ),
                sort_digest: SORT_SCORE_DESC.into(),
                result_set: None,
            },
        )
        .unwrap();
        assert_eq!(claims.offset, ids.len() as u64);
    }

    #[test]
    fn search_rejects_budget_below_floor() {
        let err = app()
            .handle(AppRequest::Search {
                query: "q".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 5,
                cursor: None,
                budget: ResponseBudget {
                    max_items: 0,
                    ..Default::default()
                },
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap_err();
        assert!(
            matches!(err, AppError::Budget(budget::BudgetError::TooSmall(_))),
            "{err}"
        );
    }

    #[test]
    fn list_pages_partition_in_wire_id_order() {
        let mut cat = MapCatalog::new(7);
        for tag in ["la", "lb", "lc"] {
            let id = StableId::derive(IdKind::Message, Stability::Reconstructed, &[tag.as_bytes()]);
            cat.insert(&id, b"x".to_vec());
        }
        let app = App::with_clock(&cat, FakeIndex, clock_t0);
        let page1 = app
            .handle(AppRequest::List {
                limit: 2,
                cursor: None,
                budget: ResponseBudget::default(),
                sessions_only: false,
            })
            .unwrap();
        let AppResponse::List {
            entries,
            next_cursor,
            ..
        } = page1
        else {
            panic!("expected List response");
        };
        assert_eq!(entries.len(), 2);
        let page2 = app
            .handle(AppRequest::List {
                limit: 2,
                cursor: next_cursor,
                budget: ResponseBudget::default(),
                sessions_only: false,
            })
            .unwrap();
        let AppResponse::List {
            entries: entries2,
            next_cursor: next2,
            ..
        } = page2
        else {
            panic!("expected List response");
        };
        assert_eq!(entries2.len(), 1);
        assert!(next2.is_none());
        let mut all: Vec<String> = entries
            .iter()
            .chain(entries2.iter())
            .map(|e| e.id.as_str().to_string())
            .collect();
        let sorted = {
            let mut s = all.clone();
            s.sort();
            s
        };
        // 两页拼接即完整 wire id 升序（钉住排序稳定，无重无漏）。
        assert_eq!(all.len(), 3);
        assert_eq!(all, sorted);
        all.dedup();
        assert_eq!(all.len(), 3);
    }

    /// 回归：payload 字节估算必须与 CLI 的 `String::from_utf8_lossy` 渲染逐字节
    /// 一致。
    ///
    /// 缺陷形态：估算器手写了一遍 UTF-8 规则，只按首字节区间取长度、再检查续
    /// 字节的高两位。它因此把**永不合法**的序列当成合法多字节字符：overlong
    /// 编码（`C0 80`）、UTF-16 代理区（`ED A0 80`）、超出 U+10FFFF 的首字节
    /// （`F5..FF`、`F4 90..`）。这些序列 lossy 渲染时逐字节各产出一个 U+FFFD
    /// （3 字节），估算却只记 2–4 字节——最坏低估到真实值的三分之一，字节闸
    /// 因此放行超预算的页并报 `truncated: false`。
    ///
    /// 断言写成与渲染函数的恒等式：估算只能由同一个 lossy 转换派生，不能靠
    /// 再实现一遍 UTF-8 校验来"平行推导"。
    /// 回归：字节闸对**恒发**字段缺值时也必须计费。
    ///
    /// 缺陷形态：机器渲染器恒发 `session_id` 与 `text` 两个键（schema 1.1 承诺
    /// 键不消失，缺值渲染为 `null`），而估算把 `None` 记 0 字节。一条既无归属
    /// 会话（无 placement）又无 `text` 的命中因此少算 18 字节；同一页约 58 条
    /// 这样的命中就吃穿 1 KiB 的 envelope 预留，响应超出 `max_response_bytes`
    /// 却报 `truncated: false`（CONTRACT §3 违约）。
    ///
    /// 断言按权威线形态的实际长度写，而不是复述估算表达式。
    #[test]
    fn search_hit_charge_covers_always_emitted_null_fields() {
        let mut hit = SearchHit {
            id: StableId::from_wire("msg_v1_aaaa").expect("valid wire id"),
            score: 2.0,
            session_id: None,
            text: None,
            why_matched: Vec::new(),
            suggested_next_commands: Vec::new(),
            occurrences: 1,
            resume_available: false,
        };
        // 恒发字段缺值时的权威线形态（protocol schema 1.1）。
        let null_wire = concat!(
            r#"{"id":"msg_v1_aaaa","score":2.0,"session_id":null,"text":null,"#,
            r#""resume_available":false}"#
        );
        assert!(
            search_hit_charge(&hit) >= null_wire.len(),
            "charge {} must cover the {} rendered bytes of {null_wire}",
            search_hit_charge(&hit),
            null_wire.len()
        );
        // 带值时同样不得低估（既有行为，一并钉住）。
        hit.session_id = Some("ses_v1_aaaa".into());
        hit.text = Some("T".into());
        let full_wire = concat!(
            r#"{"id":"msg_v1_aaaa","score":2.0,"session_id":"ses_v1_aaaa","text":"T","#,
            r#""resume_available":false}"#
        );
        assert!(
            search_hit_charge(&hit) >= full_wire.len(),
            "charge {} must cover the {} rendered bytes of {full_wire}",
            search_hit_charge(&hit),
            full_wire.len()
        );
    }

    #[test]
    fn lossy_payload_estimate_matches_the_rendered_length() {
        let cases: Vec<Vec<u8>> = vec![
            // 合法输入：ASCII、转义字符、控制字节、多字节、非 BMP。
            b"plain ascii".to_vec(),
            br#"{"quoted":"va\\lue"}"#.to_vec(),
            vec![0x00, 0x01, 0x1F, 0x7F],
            "配置备份 café 🦀".as_bytes().to_vec(),
            Vec::new(),
            // 截断/孤立续字节（估算器原本已正确处理的形态）。
            vec![0xE7, 0x95], // 截断的 3 字节序列
            vec![0x80, 0xBF], // 孤立续字节
            vec![0xFF, 0xFE],
            // 永不合法的首字节/序列（缺陷所在）。
            vec![0xC0, 0x80],             // overlong NUL
            vec![0xC1, 0xBF],             // overlong
            vec![0xE0, 0x80, 0x80],       // overlong 3 字节
            vec![0xF0, 0x80, 0x80, 0x80], // overlong 4 字节
            vec![0xED, 0xA0, 0x80],       // UTF-16 代理 D800
            vec![0xED, 0xBF, 0xBF],       // UTF-16 代理 DFFF
            vec![0xF4, 0x90, 0x80, 0x80], // U+110000，超出上界
            vec![0xF5, 0x80, 0x80, 0x80], // 首字节超出上界
            vec![0xF7, 0xBF, 0xBF, 0xBF],
            // 混合：合法文本夹着非法序列。
            b"ok\xC0\x80ok\xED\xA0\x80ok".to_vec(),
        ];
        for payload in cases {
            let rendered = json_string_len(&String::from_utf8_lossy(&payload));
            assert_eq!(
                lossy_payload_json_len(&payload),
                rendered,
                "estimate must equal the rendered length for {payload:02x?}"
            );
        }
    }

    #[test]
    fn list_byte_gate_holds_for_never_valid_utf8_payloads() {
        // 端到端：`C0 80` 对每字节渲染为一个 U+FFFD（每对 6 字节而非 2 字节）。
        // 低估时三条全部放行且 `truncated: false`，实际渲染 3789 字节远超净预算
        // 3072——响应超出 `max_response_bytes` 却声称未截断（CONTRACT §3 违约）。
        let mut cat = MapCatalog::new(7);
        let payload: Vec<u8> = [0xC0u8, 0x80].repeat(200);
        for tag in ["ua", "ub", "uc"] {
            let id = StableId::derive(IdKind::Message, Stability::Reconstructed, &[tag.as_bytes()]);
            cat.insert(&id, payload.clone());
        }
        let app = App::with_clock(&cat, FakeIndex, clock_t0);
        let AppResponse::List {
            entries,
            truncation,
            ..
        } = app
            .handle(AppRequest::List {
                limit: 10,
                cursor: None,
                budget: ResponseBudget {
                    max_response_bytes: budget::MIN_RESPONSE_BYTES,
                    ..Default::default()
                },
                sessions_only: false,
            })
            .unwrap()
        else {
            panic!("expected List response");
        };
        let net_bytes = budget::MIN_RESPONSE_BYTES - ENVELOPE_RESERVE_BYTES;
        // 真实渲染字节（CLI 的 `{"id":...,"payload":<lossy>}` 形态）。
        let rendered: usize = entries
            .iter()
            .map(|entry| {
                json_string_len(entry.id.as_str())
                    + json_string_len(&String::from_utf8_lossy(&entry.payload))
                    + 18
            })
            .sum();
        assert!(
            rendered <= net_bytes,
            "kept page renders to {rendered} bytes, over the {net_bytes} net budget"
        );
        assert!(!entries.is_empty(), "a page that fits must not be empty");
        assert!(
            truncation.truncated && entries.len() < 3,
            "the over-budget tail must be reported as truncated: {truncation:?}"
        );
    }

    #[test]
    fn list_byte_gate_counts_serialized_payload_inflation() {
        // payload 含控制字节时，最终 JSON 里每个字节膨胀为 ``（6 字符）；
        // 估算按序列化后长度计（约 1.8 KB/条），净预算 3072 只容一条——若按
        // 原始字节数估算（约 360 B/条）会错误地放行全部四条。
        let mut cat = MapCatalog::new(7);
        let payload = vec![1u8; 300];
        for tag in ["pa", "pb", "pc", "pd"] {
            let id = StableId::derive(IdKind::Message, Stability::Reconstructed, &[tag.as_bytes()]);
            cat.insert(&id, payload.clone());
        }
        let app = App::with_clock(&cat, FakeIndex, clock_t0);
        let resp = app
            .handle(AppRequest::List {
                limit: 10,
                cursor: None,
                budget: ResponseBudget {
                    max_response_bytes: budget::MIN_RESPONSE_BYTES,
                    ..Default::default()
                },
                sessions_only: false,
            })
            .unwrap();
        let AppResponse::List {
            entries,
            truncation,
            ..
        } = resp
        else {
            panic!("expected List response");
        };
        assert_eq!(entries.len(), 1);
        assert!(truncation.truncated);
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_RESPONSE_BYTES)
        );
    }

    #[test]
    fn list_sessions_byte_gate_counts_peek_serialized_bytes() {
        // peek 是渲染产物，不是免费内容（#7）：字节闸必须按 peek 的实际序列化
        // 长度计费。每条会话条目 ≈ id(41) + 会话 payload(73) + 骨架(18) +
        // `,"peek":`(8) + peek(~1002，宽字符被字节闸压到 1 KiB 内) ≈ 1142；
        // 净预算 3072（4096 - envelope 预留 1024）只容 2 条，第三条必须被截。
        let mut cat = MapCatalog::new(7);
        let user_text = "🦀".repeat(1000);
        for tag in ["sa", "sb", "sc", "sd"] {
            let session =
                StableId::derive(IdKind::Session, Stability::Reconstructed, &[tag.as_bytes()]);
            let message =
                StableId::derive(IdKind::Message, Stability::Reconstructed, &[tag.as_bytes()]);
            cat.insert(
                &message,
                serde_json::json!({ "role": "user", "text": user_text })
                    .to_string()
                    .into_bytes(),
            );
            cat.insert(
                &session,
                serde_json::json!({ "documents": [], "messages": [message.as_str()] })
                    .to_string()
                    .into_bytes(),
            );
        }
        let app = App::with_clock(&cat, FakeIndex, clock_t0);
        let resp = app
            .handle(AppRequest::List {
                limit: 10,
                cursor: None,
                budget: ResponseBudget {
                    max_response_bytes: budget::MIN_RESPONSE_BYTES,
                    ..Default::default()
                },
                sessions_only: true,
            })
            .unwrap();
        let AppResponse::List {
            entries,
            peeks,
            truncation,
            ..
        } = resp
        else {
            panic!("expected List response");
        };
        assert_eq!(entries.len(), peeks.len(), "peeks must align with entries");
        assert_eq!(entries.len(), 2, "3072 net bytes fit two peeked sessions");
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_RESPONSE_BYTES)
        );
        for peek in peeks.iter().flatten() {
            assert!(peek.json_len() <= peek::PEEK_MAX_BYTES);
        }
        // 未截断的对照：宽松预算下四条全保留，且每条都带 peek。
        let resp = app
            .handle(AppRequest::List {
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                sessions_only: true,
            })
            .unwrap();
        let AppResponse::List {
            entries,
            peeks,
            truncation,
            ..
        } = resp
        else {
            panic!("expected List response");
        };
        assert_eq!(entries.len(), 4);
        assert!(peeks.iter().all(Option::is_some));
        assert!(!truncation.truncated);
    }

    #[test]
    fn list_sessions_attaches_titles_aligned_with_entries() {
        // 标题投影（#6，schema v13）：sessions_only 列表逐条附派生标题，
        // 与 entries/peeks 逐位对齐；无标题会话为 None；普通 list 全 None。
        let mut cat = MapCatalog::new(7);
        let titled = StableId::derive(IdKind::Session, Stability::Reconstructed, &[b"tsa"]);
        let untitled = StableId::derive(IdKind::Session, Stability::Reconstructed, &[b"tsb"]);
        for session in [&titled, &untitled] {
            cat.insert(
                session,
                serde_json::json!({ "documents": [], "messages": [] })
                    .to_string()
                    .into_bytes(),
            );
        }
        cat.set_title(&titled, "synthetic title");
        let app = App::with_clock(&cat, FakeIndex, clock_t0);
        let resp = app
            .handle(AppRequest::List {
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                sessions_only: true,
            })
            .unwrap();
        let AppResponse::List {
            entries,
            peeks,
            titles,
            ..
        } = resp
        else {
            panic!("expected List response");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries.len(), peeks.len(), "peeks must align with entries");
        assert_eq!(
            entries.len(),
            titles.len(),
            "titles must align with entries"
        );
        let titled_index = entries
            .iter()
            .position(|entry| entry.id.as_str() == titled.as_str())
            .expect("titled session listed");
        let untitled_index = entries
            .iter()
            .position(|entry| entry.id.as_str() == untitled.as_str())
            .expect("untitled session listed");
        assert_eq!(titles[titled_index].as_deref(), Some("synthetic title"));
        assert_eq!(titles[untitled_index], None);

        // 普通 list（非 sessions_only）不附标题。
        let resp = app
            .handle(AppRequest::List {
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                sessions_only: false,
            })
            .unwrap();
        let AppResponse::List { titles, .. } = resp else {
            panic!("expected List response");
        };
        assert!(titles.iter().all(Option::is_none), "{titles:?}");
    }

    #[test]
    fn list_sessions_byte_gate_counts_title_serialized_bytes() {
        // 标题是渲染产物，不是免费内容（#6）：字节闸必须按标题的序列化长度
        // 计费。每条带 500 个 emoji 标题（≈2 KiB 序列化）的会话条目超过净预算
        // 3072 的 1/2——只容 1 条；同一目录去掉标题后 4 条全保留。
        let mut cat = MapCatalog::new(7);
        for tag in ["ta", "tb", "tc", "td"] {
            let session =
                StableId::derive(IdKind::Session, Stability::Reconstructed, &[tag.as_bytes()]);
            cat.insert(
                &session,
                serde_json::json!({ "documents": [], "messages": [] })
                    .to_string()
                    .into_bytes(),
            );
            cat.set_title(&session, "🦀".repeat(500));
        }
        let app = App::with_clock(&cat, FakeIndex, clock_t0);
        let resp = app
            .handle(AppRequest::List {
                limit: 10,
                cursor: None,
                budget: ResponseBudget {
                    max_response_bytes: budget::MIN_RESPONSE_BYTES,
                    ..Default::default()
                },
                sessions_only: true,
            })
            .unwrap();
        let AppResponse::List {
            entries,
            titles,
            truncation,
            ..
        } = resp
        else {
            panic!("expected List response");
        };
        assert_eq!(
            entries.len(),
            titles.len(),
            "titles must align with entries even after clamping"
        );
        assert!(titles.iter().all(Option::is_some));
        assert!(
            entries.len() < 4,
            "title bytes must be charged: {entries:?}"
        );
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_RESPONSE_BYTES)
        );
    }

    // ---- context 装配 ----

    fn ctx_msg_payload(role: &str, text: &str, timestamp: &str) -> Vec<u8> {
        serde_json::json!({
            "role": role,
            "text": text,
            "timestamp": timestamp,
            // Deliberately misleading compatibility aliases. Context must use
            // the typed graph, never these values.
            "parent": null,
            "is_sidechain": false,
            "span": {"start": 90, "end": 99},
        })
        .to_string()
        .into_bytes()
    }

    fn context_message(id: StableId, role: Role, text: &str, timestamp: &str) -> Message {
        Message {
            id,
            role,
            text: text.into(),
            timestamp: Some(timestamp.into()),
        }
    }

    fn context_document(id: StableId, fingerprint: &str) -> SourceDocument {
        SourceDocument {
            id,
            provider_id: "test-provider".into(),
            variant_id: "test-provider/v1".into(),
            fingerprint: fingerprint.into(),
            len: 200,
        }
    }

    struct GraphCatalog {
        catalog: MapCatalog,
        graph: SessionContextGraph,
        context_message_id: StableId,
        candidates: Vec<PortMessageContextCandidate>,
        stats: ContextStats,
        activity_read: fn(&[StableId]) -> PortResult<Vec<serde_json::Value>>,
    }

    impl CatalogStore for GraphCatalog {
        fn get(&self, id: &StableId) -> PortResult<Option<Vec<u8>>> {
            self.catalog.get(id)
        }

        fn get_many(&self, ids: &[StableId]) -> PortResult<Vec<(StableId, Option<Vec<u8>>)>> {
            self.catalog.get_many(ids)
        }

        fn put(&self, id: &StableId, payload: &[u8]) -> PortResult<()> {
            self.catalog.put(id, payload)
        }

        fn list(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
            self.catalog.list(limit)
        }

        fn list_sessions(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
            self.catalog.list_sessions(limit)
        }

        fn count(&self) -> PortResult<u64> {
            self.catalog.count()
        }

        fn active_generation(&self) -> PortResult<u64> {
            self.catalog.active_generation()
        }
    }

    impl ContextGraphStore for GraphCatalog {
        fn load_session_graph(&self, session_id: &StableId) -> PortResult<SessionContextGraph> {
            if session_id.as_str() == self.graph.session_id.as_str() {
                Ok(self.graph.clone())
            } else {
                Err(PortError::NotFound("session context not found".into()))
            }
        }

        fn message_contexts(
            &self,
            message_id: &StableId,
        ) -> PortResult<Vec<PortMessageContextCandidate>> {
            if message_id.as_str() == self.context_message_id.as_str() {
                Ok(self.candidates.clone())
            } else {
                // Port contract: a message absent from the catalog is a
                // lookup miss, never an empty success.
                Err(PortError::NotFound("message not found".into()))
            }
        }

        fn session_of(&self, ids: &[StableId]) -> PortResult<Vec<(StableId, Option<StableId>)>> {
            // 与 SqliteStore 语义一致：取该消息所有 placement 中 wire id 字典序
            // 最小的会话；无 placement → None。
            let owners = self.graph.placements.iter().fold(
                BTreeMap::<String, String>::new(),
                |mut owners, placement| {
                    owners
                        .entry(placement.message_id.as_str().to_string())
                        .and_modify(|owner| {
                            if placement.session_id.as_str() < owner.as_str() {
                                *owner = placement.session_id.as_str().to_string();
                            }
                        })
                        .or_insert_with(|| placement.session_id.as_str().to_string());
                    owners
                },
            );
            Ok(ids
                .iter()
                .map(|id| {
                    let session = owners
                        .get(id.as_str())
                        .and_then(|wire| StableId::from_wire(wire));
                    (id.clone(), session)
                })
                .collect())
        }

        fn source_placements_of(
            &self,
            ids: &[StableId],
        ) -> PortResult<Vec<(StableId, Option<SourcePlacement>)>> {
            // 与 SqliteStore 语义一致：取该消息所有 placement 中 source_document_id
            // 字典序最小的 placement 作为权威来源（确定、稳定）。
            let mut best: BTreeMap<String, SourcePlacement> = BTreeMap::new();
            for placement in &self.graph.placements {
                let entry = best
                    .entry(placement.message_id.as_str().to_string())
                    .or_insert_with(|| SourcePlacement {
                        source_document_id: placement.source_document_id.clone(),
                        byte_start: placement.span.as_ref().map(|s| s.start),
                        byte_end: placement.span.as_ref().map(|s| s.end),
                    });
                if placement.source_document_id.as_str() < entry.source_document_id.as_str() {
                    entry.source_document_id = placement.source_document_id.clone();
                    entry.byte_start = placement.span.as_ref().map(|s| s.start);
                    entry.byte_end = placement.span.as_ref().map(|s| s.end);
                }
            }
            Ok(ids
                .iter()
                .map(|id| (id.clone(), best.get(id.as_str()).cloned()))
                .collect())
        }

        fn context_stats(&self) -> PortResult<ContextStats> {
            Ok(self.stats)
        }

        fn tool_activities_for_messages(
            &self,
            message_ids: &[StableId],
        ) -> PortResult<Vec<serde_json::Value>> {
            (self.activity_read)(message_ids)
        }
    }

    struct ContextFixture {
        store: GraphCatalog,
        session: StableId,
        document_a: StableId,
        document_b: StableId,
        root: StableId,
        repeated: StableId,
        sidechain: StableId,
        leaf: StableId,
        root_placement: MessagePlacement,
        repeated_a: MessagePlacement,
        repeated_b: MessagePlacement,
        leaf_placement: MessagePlacement,
    }

    /// Graph shape:
    ///
    /// - one stable Message has two placements in one Session;
    /// - the repeated placement in document B is the parent of the real leaf;
    /// - a sidechain placement exists but is excluded from mainline;
    /// - payload aliases disagree with graph facts and must be ignored.
    fn ctx_fixture() -> ContextFixture {
        let session = StableId::native(IdKind::Session, "sess-ctx");
        let document_a = StableId::native(IdKind::Document, "ctx-doc-a");
        let document_b = StableId::native(IdKind::Document, "ctx-doc-b");
        let root = StableId::native(IdKind::Message, "ctx-root");
        let repeated = StableId::native(IdKind::Message, "ctx-repeated");
        let sidechain = StableId::native(IdKind::Message, "ctx-side");
        let leaf = StableId::native(IdKind::Message, "ctx-leaf");

        let root_message =
            context_message(root.clone(), Role::User, "root", "2026-07-26T00:00:00Z");
        let repeated_message = context_message(
            repeated.clone(),
            Role::Assistant,
            "repeated",
            "2026-07-26T00:01:00Z",
        );
        let sidechain_message = context_message(
            sidechain.clone(),
            Role::Assistant,
            "side",
            "2026-07-26T00:01:30Z",
        );
        let leaf_message =
            context_message(leaf.clone(), Role::User, "leaf", "2026-07-26T00:02:00Z");
        let source_a = context_document(document_a.clone(), "b3-doc-a");
        let source_b = context_document(document_b.clone(), "b3-doc-b");

        let root_placement = MessagePlacement::new(
            session.clone(),
            document_a.clone(),
            root.clone(),
            0,
            false,
            Some(EvidenceSpan { start: 0, end: 10 }),
        );
        let repeated_a = MessagePlacement::new(
            session.clone(),
            document_a.clone(),
            repeated.clone(),
            1,
            false,
            Some(EvidenceSpan { start: 11, end: 25 }),
        );
        let sidechain_placement = MessagePlacement::new(
            session.clone(),
            document_a.clone(),
            sidechain.clone(),
            2,
            true,
            Some(EvidenceSpan { start: 26, end: 40 }),
        );
        let repeated_b = MessagePlacement::new(
            session.clone(),
            document_b.clone(),
            repeated.clone(),
            0,
            false,
            Some(EvidenceSpan { start: 5, end: 19 }),
        );
        let leaf_placement = MessagePlacement::new(
            session.clone(),
            document_b.clone(),
            leaf.clone(),
            1,
            false,
            Some(EvidenceSpan { start: 20, end: 40 }),
        );
        let edges = vec![
            MessageEdge {
                child_placement_id: repeated_a.id.clone(),
                parent_message_id: root.clone(),
                parent_native_id: Some("ctx-root".into()),
                relation: MessageRelation::Reply,
            },
            MessageEdge {
                child_placement_id: repeated_b.id.clone(),
                parent_message_id: root.clone(),
                parent_native_id: Some("ctx-root".into()),
                relation: MessageRelation::Reply,
            },
            MessageEdge {
                child_placement_id: sidechain_placement.id.clone(),
                parent_message_id: repeated.clone(),
                parent_native_id: Some("ctx-repeated".into()),
                relation: MessageRelation::Reply,
            },
            MessageEdge {
                child_placement_id: leaf_placement.id.clone(),
                parent_message_id: repeated.clone(),
                parent_native_id: Some("ctx-repeated".into()),
                relation: MessageRelation::Reply,
            },
        ];
        let graph = SessionContextGraph {
            session_id: session.clone(),
            messages: vec![
                root_message,
                repeated_message,
                sidechain_message,
                leaf_message,
            ],
            source_documents: vec![source_a, source_b],
            placements: vec![
                root_placement.clone(),
                repeated_a.clone(),
                sidechain_placement.clone(),
                repeated_b.clone(),
                leaf_placement.clone(),
            ],
            edges,
        };
        graph.validate().unwrap();

        let mut catalog = MapCatalog::new(7);
        catalog.insert(
            &root,
            ctx_msg_payload("user", "root", "2026-07-26T00:00:00Z"),
        );
        catalog.insert(
            &repeated,
            ctx_msg_payload("assistant", "repeated", "2026-07-26T00:01:00Z"),
        );
        catalog.insert(
            &sidechain,
            ctx_msg_payload("assistant", "side", "2026-07-26T00:01:30Z"),
        );
        catalog.insert(
            &leaf,
            ctx_msg_payload("user", "leaf", "2026-07-26T00:02:00Z"),
        );
        catalog.insert(
            &session,
            serde_json::json!({
                "document": "doc_v1_wrong-compatibility-alias",
                "messages": [],
            })
            .to_string()
            .into_bytes(),
        );

        let store = GraphCatalog {
            catalog,
            graph,
            activity_read: |ids| FakeCatalog.tool_activities_for_messages(ids),
            context_message_id: repeated.clone(),
            candidates: vec![
                PortMessageContextCandidate {
                    session_id: session.clone(),
                    placement_ids: vec![repeated_b.id.clone()],
                },
                PortMessageContextCandidate {
                    session_id: session.clone(),
                    placement_ids: vec![repeated_a.id.clone(), repeated_b.id.clone()],
                },
            ],
            stats: ContextStats {
                placements: 5,
                source_placement_claims: 6,
            },
        };
        ContextFixture {
            store,
            session,
            document_a,
            document_b,
            root,
            repeated,
            sidechain,
            leaf,
            root_placement,
            repeated_a,
            repeated_b,
            leaf_placement,
        }
    }

    fn ctx_req(ses: &StableId, policy: ContextPolicy, budget: ResponseBudget) -> AppRequest {
        AppRequest::Context {
            session_id: ses.clone(),
            policy,
            level: ContextLevel::Raw,
            budget,
        }
    }

    fn ctx_req_level(
        ses: &StableId,
        policy: ContextPolicy,
        level: ContextLevel,
        budget: ResponseBudget,
    ) -> AppRequest {
        AppRequest::Context {
            session_id: ses.clone(),
            policy,
            level,
            budget,
        }
    }

    #[test]
    fn context_activity_read_errors_propagate() {
        for empty_graph in [false, true] {
            let mut fixture = ctx_fixture();
            fixture.store.activity_read =
                |_| Err(PortError::Backend("activity read failed".into()));
            if empty_graph {
                fixture.store.graph.messages.clear();
                fixture.store.graph.placements.clear();
                fixture.store.graph.edges.clear();
            }
            for policy in [ContextPolicy::Mainline, ContextPolicy::Full] {
                for level in [
                    ContextLevel::Raw,
                    ContextLevel::Talks,
                    ContextLevel::Sessions,
                ] {
                    let err = app_ctx(&fixture.store)
                        .handle(ctx_req_level(
                            &fixture.session,
                            policy,
                            level,
                            ResponseBudget::default(),
                        ))
                        .unwrap_err();
                    assert!(
                        matches!(err, AppError::Port(PortError::Backend(ref message))
                            if message == "activity read failed"),
                        "{err:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn context_activity_missing_projection_remains_empty_success() {
        for empty_graph in [false, true] {
            let mut fixture = ctx_fixture();
            if empty_graph {
                fixture.store.graph.messages.clear();
                fixture.store.graph.placements.clear();
                fixture.store.graph.edges.clear();
            }
            for policy in [ContextPolicy::Mainline, ContextPolicy::Full] {
                let response = app_ctx(&fixture.store)
                    .handle(ctx_req(&fixture.session, policy, ResponseBudget::default()))
                    .unwrap();
                let AppResponse::Context {
                    tool_activities,
                    messages,
                    ..
                } = response
                else {
                    panic!("expected Context response");
                };
                assert!(tool_activities.is_empty());
                assert_eq!(messages.is_empty(), empty_graph);
            }
        }
    }

    #[test]
    fn context_activity_read_uses_retained_messages() {
        let mut fixture = ctx_fixture();
        fixture.store.activity_read = |ids| {
            assert_eq!(ids.len(), 1);
            Ok(vec![serde_json::json!({ "message_id": ids[0].as_str() })])
        };
        let response = app_ctx(&fixture.store)
            .handle(ctx_req(
                &fixture.session,
                ContextPolicy::Mainline,
                ResponseBudget {
                    max_messages: 1,
                    ..ResponseBudget::default()
                },
            ))
            .unwrap();
        let AppResponse::Context {
            tool_activities,
            messages,
            ..
        } = response
        else {
            panic!("expected Context response");
        };
        assert_eq!(messages.len(), 1);
        assert_eq!(
            tool_activities,
            vec![serde_json::json!({ "message_id": messages[0].message_id })]
        );
    }

    #[test]
    fn context_mainline_uses_typed_edges_and_exact_placement_evidence() {
        let fixture = ctx_fixture();
        let resp = app_ctx(&fixture.store)
            .handle(ctx_req(
                &fixture.session,
                ContextPolicy::Mainline,
                ResponseBudget::default(),
            ))
            .unwrap();
        let AppResponse::Context {
            session_id,
            branch_leaf,
            branch_leaf_placement_id,
            messages,
            evidence,
            truncation,
            generation,
            ..
        } = resp
        else {
            panic!("expected Context response");
        };
        assert_eq!(session_id, fixture.session.as_str());
        assert_eq!(generation, 7);
        assert!(!truncation.truncated);
        assert_eq!(branch_leaf.as_deref(), Some(fixture.leaf.as_str()));
        assert_eq!(
            branch_leaf_placement_id.as_deref(),
            Some(fixture.leaf_placement.id.as_str())
        );
        let message_ids: Vec<&str> = messages
            .iter()
            .map(|message| message.message_id.as_str())
            .collect();
        assert_eq!(
            message_ids,
            vec![
                fixture.root.as_str(),
                fixture.repeated.as_str(),
                fixture.leaf.as_str()
            ]
        );
        assert_eq!(
            messages[1].placement_id,
            fixture.repeated_b.id.as_str(),
            "same-document parent resolution must choose document B"
        );
        assert!(
            messages
                .iter()
                .all(|message| message.id == message.message_id)
        );

        assert_eq!(evidence.len(), 3);
        assert_eq!(evidence[0].precision, evidence::Precision::Byte);
        assert_eq!(evidence[0].byte_start, Some(0));
        assert_eq!(evidence[1].byte_start, Some(5));
        assert_eq!(evidence[2].byte_end, Some(40));
        assert_eq!(evidence[2].record_ordinal, Some(1));
        assert_eq!(evidence[0].source_fingerprint.as_deref(), Some("b3-doc-a"));
        assert_eq!(evidence[1].source_fingerprint.as_deref(), Some("b3-doc-b"));
        assert_eq!(
            evidence[0].source_document_id.as_deref(),
            Some(fixture.document_a.as_str())
        );
        assert_eq!(
            evidence[1].source_document_id.as_deref(),
            Some(fixture.document_b.as_str())
        );
        assert_eq!(
            evidence
                .iter()
                .map(|span| span.occurrence_id.as_str())
                .collect::<Vec<_>>(),
            messages
                .iter()
                .map(|message| message.placement_id.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn context_full_keeps_repeated_stable_messages_as_distinct_occurrences() {
        let fixture = ctx_fixture();
        let resp = app_ctx(&fixture.store)
            .handle(ctx_req(
                &fixture.session,
                ContextPolicy::Full,
                ResponseBudget::default(),
            ))
            .unwrap();
        let AppResponse::Context {
            messages,
            branch_leaf,
            branch_leaf_placement_id,
            evidence,
            ..
        } = resp
        else {
            panic!("expected Context response");
        };
        let wires: Vec<&str> = messages
            .iter()
            .map(|message| message.message_id.as_str())
            .collect();
        assert_eq!(
            wires,
            vec![
                fixture.root.as_str(),
                fixture.repeated.as_str(),
                fixture.repeated.as_str(),
                fixture.sidechain.as_str(),
                fixture.leaf.as_str(),
            ]
        );
        assert_ne!(messages[1].placement_id, messages[2].placement_id);
        assert_eq!(messages[1].placement_id, fixture.repeated_a.id.as_str());
        assert_eq!(messages[2].placement_id, fixture.repeated_b.id.as_str());
        assert_eq!(evidence.len(), 5);
        assert_eq!(evidence[1].message_id, evidence[2].message_id);
        assert_ne!(evidence[1].occurrence_id, evidence[2].occurrence_id);
        assert_eq!(branch_leaf.as_deref(), Some(fixture.leaf.as_str()));
        assert_eq!(
            branch_leaf_placement_id.as_deref(),
            Some(fixture.leaf_placement.id.as_str())
        );
    }

    #[test]
    fn context_budget_counts_placement_occurrences() {
        let fixture = ctx_fixture();
        let resp = app_ctx(&fixture.store)
            .handle(ctx_req(
                &fixture.session,
                ContextPolicy::Full,
                ResponseBudget {
                    max_messages: 3,
                    ..Default::default()
                },
            ))
            .unwrap();
        let AppResponse::Context {
            messages,
            evidence,
            truncation,
            ..
        } = resp
        else {
            panic!("expected Context response");
        };
        assert_eq!(messages.len(), 3);
        assert_eq!(evidence.len(), 3);
        assert_eq!(messages[1].message_id, messages[2].message_id);
        assert_ne!(messages[1].placement_id, messages[2].placement_id);
        assert!(truncation.truncated);
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_MESSAGES)
        );
    }

    #[test]
    fn context_evidence_span_budget_reports_reason() {
        let fixture = ctx_fixture();
        let resp = app_ctx(&fixture.store)
            .handle(ctx_req(
                &fixture.session,
                ContextPolicy::Mainline,
                ResponseBudget {
                    max_evidence_spans: 1,
                    ..Default::default()
                },
            ))
            .unwrap();
        let AppResponse::Context {
            messages,
            evidence,
            truncation,
            ..
        } = resp
        else {
            panic!("expected Context response");
        };
        assert_eq!(messages.len(), 3);
        assert_eq!(evidence.len(), 1);
        assert!(truncation.truncated);
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_EVIDENCE_SPANS)
        );
    }

    #[test]
    fn context_reports_both_knobs_when_messages_and_evidence_truncate() {
        // 两道闸同时触发时两个旋钮名都要报，不能只报 max_messages。
        let fixture = ctx_fixture();
        let resp = app_ctx(&fixture.store)
            .handle(ctx_req(
                &fixture.session,
                ContextPolicy::Full,
                ResponseBudget {
                    max_messages: 3,
                    max_evidence_spans: 1,
                    ..Default::default()
                },
            ))
            .unwrap();
        let AppResponse::Context {
            messages,
            evidence,
            truncation,
            ..
        } = resp
        else {
            panic!("expected Context response");
        };
        assert_eq!(messages.len(), 3);
        assert_eq!(evidence.len(), 1);
        assert!(truncation.truncated);
        assert_eq!(
            truncation.reason.as_deref(),
            Some("max_messages,max_evidence_spans")
        );
    }

    #[test]
    fn context_missing_session_is_not_found() {
        let fixture = ctx_fixture();
        let missing = StableId::native(IdKind::Session, "no-such-session");
        let err = app_ctx(&fixture.store)
            .handle(ctx_req(
                &missing,
                ContextPolicy::Mainline,
                ResponseBudget::default(),
            ))
            .unwrap_err();
        assert!(
            matches!(err, AppError::Port(PortError::NotFound(_))),
            "{err}"
        );
    }

    #[test]
    fn context_malformed_session_payload_is_invariant_violation() {
        let mut fixture = ctx_fixture();
        fixture
            .store
            .catalog
            .insert(&fixture.session, b"user\tnot canonical json".to_vec());
        let err = app_ctx(&fixture.store)
            .handle(ctx_req(
                &fixture.session,
                ContextPolicy::Mainline,
                ResponseBudget::default(),
            ))
            .unwrap_err();
        assert!(
            matches!(err, AppError::Domain(DomainError::InvariantViolation(_))),
            "{err}"
        );
    }

    #[test]
    fn message_context_candidates_are_grouped_by_distinct_session() {
        let mut fixture = ctx_fixture();
        let second_session = StableId::native(IdKind::Session, "sess-other");
        fixture.store.candidates.push(PortMessageContextCandidate {
            session_id: second_session.clone(),
            placement_ids: vec![fixture.root_placement.id.clone()],
        });

        let response = app_ctx(&fixture.store)
            .handle(AppRequest::MessageContexts {
                message_id: fixture.repeated.clone(),
            })
            .unwrap();
        let AppResponse::MessageContexts {
            message_id,
            candidates,
        } = response
        else {
            panic!("expected MessageContexts response");
        };
        assert_eq!(message_id, fixture.repeated.as_str());
        assert_eq!(candidates.len(), 2);
        let primary = candidates
            .iter()
            .find(|candidate| candidate.session_id == fixture.session.as_str())
            .unwrap();
        let expected: BTreeSet<String> = [
            fixture.repeated_a.id.as_str().to_string(),
            fixture.repeated_b.id.as_str().to_string(),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            primary
                .placement_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.session_id == second_session.as_str())
        );
    }

    #[test]
    fn message_contexts_missing_message_is_not_found() {
        // Port contract: a message absent from the catalog is a lookup miss
        // (NotFound), never an empty success.
        let fixture = ctx_fixture();
        let index_only = StableId::native(IdKind::Message, "index-only");
        let err = app_ctx(&fixture.store)
            .handle(AppRequest::MessageContexts {
                message_id: index_only.clone(),
            })
            .unwrap_err();
        assert!(
            matches!(err, AppError::Port(PortError::NotFound(_))),
            "missing message must map to NotFound, got {err:?}"
        );
    }

    fn app_ctx(cat: &GraphCatalog) -> App<&GraphCatalog, FakeIndex> {
        App::with_clock(cat, FakeIndex, clock_t0)
    }

    #[test]
    fn context_talks_group_user_message_with_following_messages() {
        let fixture = ctx_fixture();
        let AppResponse::Context {
            requested_level,
            effective_level,
            talks,
            summary,
            hint,
            messages,
            ..
        } = app_ctx(&fixture.store)
            .handle(ctx_req_level(
                &fixture.session,
                ContextPolicy::Mainline,
                ContextLevel::Talks,
                ResponseBudget::default(),
            ))
            .unwrap()
        else {
            panic!("expected Context response");
        };
        assert_eq!(requested_level, ContextLevel::Talks);
        assert_eq!(effective_level, ContextLevel::Talks);
        assert!(summary.is_none());
        assert_eq!(talks.len(), 2);
        assert_eq!(talks[0].user_message.message_id, fixture.root.as_str());
        assert_eq!(
            talks[0]
                .following_messages
                .iter()
                .map(|message| message.message_id.as_str())
                .collect::<Vec<_>>(),
            vec![fixture.repeated.as_str()]
        );
        assert_eq!(talks[1].user_message.message_id, fixture.leaf.as_str());
        assert!(talks[1].following_messages.is_empty());
        assert_eq!(messages.len(), 3);
        let hint = hint.expect("talks must hint toward raw");
        assert_eq!(hint.command, "get_session_context");
        assert_eq!(hint.session_id, fixture.session.as_str());
        assert_eq!(hint.level, ContextLevel::Raw);
    }

    #[test]
    fn context_talks_only_include_assistant_and_tool_followers() {
        for (role, payload_role, included) in [
            (Role::Assistant, "assistant", true),
            (Role::Tool, "tool", true),
            (Role::System, "system", false),
            (Role::Developer, "developer", false),
        ] {
            let mut fixture = ctx_fixture();
            fixture.store.catalog.insert(
                &fixture.repeated,
                ctx_msg_payload(payload_role, payload_role, "2026-07-26T00:01:00Z"),
            );
            fixture
                .store
                .graph
                .messages
                .iter_mut()
                .find(|message| message.id == fixture.repeated)
                .expect("repeated message")
                .role = role;
            let AppResponse::Context {
                talks, messages, ..
            } = app_ctx(&fixture.store)
                .handle(ctx_req_level(
                    &fixture.session,
                    ContextPolicy::Mainline,
                    ContextLevel::Talks,
                    ResponseBudget::default(),
                ))
                .unwrap()
            else {
                panic!("expected Context response");
            };
            assert_eq!(talks.len(), 2);
            assert_eq!(talks[0].following_messages.len(), included as usize);
            assert_eq!(messages.len(), 3);
        }
    }

    #[test]
    fn context_sessions_returns_structural_overview_and_talk_hint() {
        let fixture = ctx_fixture();
        let AppResponse::Context {
            requested_level,
            effective_level,
            talks,
            summary,
            hint,
            ..
        } = app_ctx(&fixture.store)
            .handle(ctx_req_level(
                &fixture.session,
                ContextPolicy::Mainline,
                ContextLevel::Sessions,
                ResponseBudget::default(),
            ))
            .unwrap()
        else {
            panic!("expected Context response");
        };
        assert_eq!(requested_level, ContextLevel::Sessions);
        assert_eq!(effective_level, ContextLevel::Sessions);
        let summary = summary.expect("sessions must produce a summary");
        assert_eq!(
            summary.first_user_message.unwrap().message_id,
            fixture.root.as_str()
        );
        assert_eq!(summary.message_count, 3);
        assert_eq!(summary.turn_count, 2);
        assert_eq!(
            summary.file_references,
            vec![
                fixture.document_a.as_str().to_string(),
                fixture.document_b.as_str().to_string(),
            ]
        );
        assert!(talks.is_empty());
        let hint = hint.expect("sessions must hint toward talks");
        assert_eq!(hint.command, "get_session_context");
        assert_eq!(hint.session_id, fixture.session.as_str());
        assert_eq!(hint.level, ContextLevel::Talks);
    }

    #[test]
    fn context_talks_and_sessions_fall_back_toward_raw_without_user() {
        let mut fixture = ctx_fixture();
        fixture
            .store
            .graph
            .messages
            .iter_mut()
            .for_each(|message| message.role = Role::Assistant);
        for requested in [ContextLevel::Talks, ContextLevel::Sessions] {
            let AppResponse::Context {
                requested_level,
                effective_level,
                talks,
                summary,
                hint,
                messages,
                ..
            } = app_ctx(&fixture.store)
                .handle(ctx_req_level(
                    &fixture.session,
                    ContextPolicy::Mainline,
                    requested,
                    ResponseBudget::default(),
                ))
                .unwrap()
            else {
                panic!("expected Context response");
            };
            assert_eq!(requested_level, requested);
            assert_eq!(effective_level, ContextLevel::Raw);
            assert!(talks.is_empty());
            assert!(summary.is_none());
            assert!(hint.is_none());
            assert_eq!(messages.len(), 3);
        }
    }

    #[test]
    fn context_sessions_budget_falls_back_through_talks_before_raw() {
        let mut fixture = ctx_fixture();
        fixture
            .store
            .graph
            .messages
            .iter_mut()
            .find(|message| message.id == fixture.root)
            .expect("root message")
            .role = Role::System;
        let mut response = None;
        for text_len in 1000..=2000 {
            fixture.store.catalog.insert(
                &fixture.root,
                ctx_msg_payload("system", &"x".repeat(text_len), "2026-07-26T00:00:00Z"),
            );
            let candidate = app_ctx(&fixture.store)
                .handle(ctx_req_level(
                    &fixture.session,
                    ContextPolicy::Mainline,
                    ContextLevel::Sessions,
                    ResponseBudget {
                        max_response_bytes: budget::MIN_RESPONSE_BYTES,
                        ..Default::default()
                    },
                ))
                .unwrap();
            if matches!(
                &candidate,
                AppResponse::Context {
                    effective_level: ContextLevel::Talks,
                    ..
                }
            ) {
                response = Some(candidate);
                break;
            }
        }
        let AppResponse::Context {
            requested_level,
            effective_level,
            talks,
            summary,
            hint,
            truncation,
            ..
        } = response.expect("a boundary must exist where sessions falls back to talks")
        else {
            panic!("expected Context response");
        };
        assert_eq!(requested_level, ContextLevel::Sessions);
        assert_eq!(effective_level, ContextLevel::Talks);
        assert_eq!(talks.len(), 1);
        assert!(summary.is_none());
        assert_eq!(
            hint.expect("talks fallback must hint raw").level,
            ContextLevel::Raw
        );
        assert!(truncation.truncated);
        assert!(
            truncation
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains(budget::TRUNCATION_MAX_RESPONSE_BYTES))
        );
    }

    #[test]
    fn context_clamp_reports_public_max_messages_reason() {
        let fixture = ctx_fixture();
        let AppResponse::Context { truncation, .. } = app_ctx(&fixture.store)
            .handle(ctx_req_level(
                &fixture.session,
                ContextPolicy::Mainline,
                ContextLevel::Talks,
                ResponseBudget {
                    max_messages: 1,
                    ..Default::default()
                },
            ))
            .unwrap()
        else {
            panic!("expected Context response");
        };
        assert!(truncation.truncated);
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_MESSAGES)
        );
        assert!(
            !truncation
                .reason
                .unwrap()
                .contains(budget::TRUNCATION_MAX_ITEMS)
        );
    }

    #[test]
    fn context_fallback_does_not_charge_unproduced_derived_bytes() {
        let mut fixture = ctx_fixture();
        fixture
            .store
            .graph
            .messages
            .iter_mut()
            .for_each(|message| message.role = Role::Assistant);
        let floor_budget = || ResponseBudget {
            max_response_bytes: budget::MIN_RESPONSE_BYTES,
            ..Default::default()
        };
        let AppResponse::Context {
            messages: raw_messages,
            truncation: raw_truncation,
            ..
        } = app_ctx(&fixture.store)
            .handle(ctx_req_level(
                &fixture.session,
                ContextPolicy::Mainline,
                ContextLevel::Raw,
                floor_budget(),
            ))
            .unwrap()
        else {
            panic!("expected Context response");
        };
        let AppResponse::Context {
            effective_level,
            messages: fallback_messages,
            truncation: fallback_truncation,
            ..
        } = app_ctx(&fixture.store)
            .handle(ctx_req_level(
                &fixture.session,
                ContextPolicy::Mainline,
                ContextLevel::Talks,
                floor_budget(),
            ))
            .unwrap()
        else {
            panic!("expected Context response");
        };
        assert_eq!(effective_level, ContextLevel::Raw);
        assert_eq!(fallback_messages, raw_messages);
        assert_eq!(fallback_truncation, raw_truncation);
    }

    #[test]
    fn context_raw_never_falls_back_and_carries_no_hint() {
        let fixture = ctx_fixture();
        let AppResponse::Context {
            requested_level,
            effective_level,
            talks,
            summary,
            hint,
            ..
        } = app_ctx(&fixture.store)
            .handle(ctx_req_level(
                &fixture.session,
                ContextPolicy::Mainline,
                ContextLevel::Raw,
                ResponseBudget::default(),
            ))
            .unwrap()
        else {
            panic!("expected Context response");
        };
        assert_eq!(requested_level, ContextLevel::Raw);
        assert_eq!(effective_level, ContextLevel::Raw);
        assert!(talks.is_empty());
        assert!(summary.is_none());
        assert!(hint.is_none());
    }

    #[test]
    fn context_derived_level_bytes_count_toward_budget() {
        let mut fixture = ctx_fixture();
        let big_text = "x".repeat(900);
        for (id, role, timestamp) in [
            (&fixture.root, "user", "2026-07-26T00:00:00Z"),
            (&fixture.repeated, "assistant", "2026-07-26T00:01:00Z"),
            (&fixture.leaf, "user", "2026-07-26T00:02:00Z"),
        ] {
            fixture
                .store
                .catalog
                .insert(id, ctx_msg_payload(role, &big_text, timestamp));
        }
        let floor_budget = || ResponseBudget {
            max_response_bytes: budget::MIN_RESPONSE_BYTES,
            ..Default::default()
        };
        let AppResponse::Context {
            messages: raw_messages,
            ..
        } = app_ctx(&fixture.store)
            .handle(ctx_req_level(
                &fixture.session,
                ContextPolicy::Mainline,
                ContextLevel::Raw,
                floor_budget(),
            ))
            .unwrap()
        else {
            panic!("expected Context response");
        };
        let AppResponse::Context {
            messages: derived_messages,
            effective_level,
            truncation,
            ..
        } = app_ctx(&fixture.store)
            .handle(ctx_req_level(
                &fixture.session,
                ContextPolicy::Mainline,
                ContextLevel::Talks,
                floor_budget(),
            ))
            .unwrap()
        else {
            panic!("expected Context response");
        };
        assert_eq!(effective_level, ContextLevel::Talks);
        assert!(truncation.truncated);
        assert!(
            truncation
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains(budget::TRUNCATION_MAX_RESPONSE_BYTES))
        );
        assert!(derived_messages.len() < raw_messages.len());
    }

    #[test]
    fn context_derived_views_honor_message_clamp() {
        let fixture = ctx_fixture();
        let AppResponse::Context {
            effective_level,
            talks,
            summary,
            truncation,
            ..
        } = app_ctx(&fixture.store)
            .handle(ctx_req_level(
                &fixture.session,
                ContextPolicy::Mainline,
                ContextLevel::Sessions,
                ResponseBudget {
                    max_messages: 2,
                    ..Default::default()
                },
            ))
            .unwrap()
        else {
            panic!("expected Context response");
        };
        assert!(truncation.truncated);
        assert_eq!(
            truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_MESSAGES)
        );
        assert_eq!(effective_level, ContextLevel::Sessions);
        assert!(talks.is_empty());
        let summary = summary.expect("clamped sessions still summarizes");
        assert_eq!(summary.message_count, 2);
        assert_eq!(summary.turn_count, 1);
    }

    fn message_req_with_id(
        message_id: StableId,
        session_id: Option<StableId>,
        around: usize,
        budget: ResponseBudget,
    ) -> AppRequest {
        AppRequest::Message {
            message_id,
            session_id,
            around,
            budget,
        }
    }

    fn message_req(
        fixture: &ContextFixture,
        session_id: Option<StableId>,
        around: usize,
        budget: ResponseBudget,
    ) -> AppRequest {
        message_req_with_id(fixture.repeated.clone(), session_id, around, budget)
    }

    #[test]
    fn message_around_zero_returns_anchor_only() {
        let fixture = ctx_fixture();
        let AppResponse::Message { window } = app_ctx(&fixture.store)
            .handle(message_req(
                &fixture,
                Some(fixture.session.clone()),
                0,
                ResponseBudget::default(),
            ))
            .unwrap()
        else {
            panic!("expected Message response");
        };
        assert_eq!(window.message_id, fixture.repeated.as_str());
        assert_eq!(window.session_id, fixture.session.as_str());
        assert_eq!(window.anchor_placement_id, fixture.repeated_b.id.as_str());
        assert_eq!(window.messages.len(), 1);
        assert_eq!(
            window.messages[0].placement_id,
            fixture.repeated_b.id.as_str()
        );
        assert!(!window.truncation.truncated);
    }

    #[test]
    fn message_window_clamps_at_mainline_ends_and_stays_chronological() {
        let fixture = ctx_fixture();
        let AppResponse::Message { window } = app_ctx(&fixture.store)
            .handle(message_req(
                &fixture,
                Some(fixture.session.clone()),
                9,
                ResponseBudget::default(),
            ))
            .unwrap()
        else {
            panic!("expected Message response");
        };
        assert_eq!(
            window
                .messages
                .iter()
                .map(|message| message.message_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                fixture.root.as_str(),
                fixture.repeated.as_str(),
                fixture.leaf.as_str(),
            ]
        );
        assert!(
            !window
                .messages
                .iter()
                .any(|message| message.message_id == fixture.sidechain.as_str())
        );
    }

    #[test]
    fn message_window_includes_one_neighbor_per_side() {
        let fixture = ctx_fixture();
        let AppResponse::Message { window } = app_ctx(&fixture.store)
            .handle(message_req(
                &fixture,
                Some(fixture.session.clone()),
                1,
                ResponseBudget::default(),
            ))
            .unwrap()
        else {
            panic!("expected Message response");
        };
        assert_eq!(window.messages.len(), 3);
        assert_eq!(window.messages[0].message_id, fixture.root.as_str());
        assert_eq!(window.messages[2].message_id, fixture.leaf.as_str());
    }

    #[test]
    fn message_multiple_mainline_placements_is_invalid_request() {
        let mut fixture = ctx_fixture();
        fixture.store.graph.placements[2].is_sidechain = false;
        fixture.store.graph.edges[1].parent_message_id = fixture.sidechain.clone();
        fixture.store.graph.validate().unwrap();

        let err = app_ctx(&fixture.store)
            .handle(message_req(
                &fixture,
                Some(fixture.session.clone()),
                0,
                ResponseBudget::default(),
            ))
            .unwrap_err();
        let AppError::Domain(error @ DomainError::InvalidRequest(_)) = err else {
            panic!("expected InvalidRequest, got {err:?}");
        };
        assert_eq!(error.code(), "invalid_request");
        let DomainError::InvalidRequest(message) = error else {
            unreachable!();
        };
        assert!(message.contains("multiple placements"), "{message}");
        assert!(message.contains("get_session_context"), "{message}");
    }

    #[test]
    fn message_without_session_auto_resolves_single_candidate() {
        let fixture = ctx_fixture();
        let AppResponse::Message { window } = app_ctx(&fixture.store)
            .handle(message_req(&fixture, None, 0, ResponseBudget::default()))
            .unwrap()
        else {
            panic!("expected Message response");
        };
        assert_eq!(window.session_id, fixture.session.as_str());
        assert_eq!(window.messages.len(), 1);
    }

    #[test]
    fn message_with_unknown_session_is_not_found() {
        let fixture = ctx_fixture();
        let wrong = StableId::native(IdKind::Session, "sess-not-a-candidate");
        let err = app_ctx(&fixture.store)
            .handle(message_req(
                &fixture,
                Some(wrong),
                0,
                ResponseBudget::default(),
            ))
            .unwrap_err();
        assert!(matches!(err, AppError::Domain(DomainError::NotFound(_))));
    }

    #[test]
    fn message_ambiguity_is_bounded_sorted_and_actionable() {
        let mut fixture = ctx_fixture();
        fixture.store.candidates = (0..10)
            .map(|index| PortMessageContextCandidate {
                session_id: StableId::native(IdKind::Session, &format!("sess-x{index}")),
                placement_ids: vec![fixture.repeated_a.id.clone()],
            })
            .collect();
        let AppError::MessageAmbiguous(ambiguity) = app_ctx(&fixture.store)
            .handle(message_req(&fixture, None, 0, ResponseBudget::default()))
            .unwrap_err()
        else {
            panic!("expected MessageAmbiguous error");
        };
        assert_eq!(ambiguity.candidate_count, 10);
        assert_eq!(
            ambiguity.candidate_session_ids.len(),
            MAX_MESSAGE_AMBIGUITY_CANDIDATES
        );
        let mut sorted = ambiguity.candidate_session_ids.clone();
        sorted.sort();
        assert_eq!(ambiguity.candidate_session_ids, sorted);
        assert!(!ambiguity.hint.is_empty());
    }

    #[test]
    fn message_missing_is_not_found() {
        let fixture = ctx_fixture();
        let err = app_ctx(&fixture.store)
            .handle(message_req_with_id(
                StableId::native(IdKind::Message, "index-only"),
                None,
                0,
                ResponseBudget::default(),
            ))
            .unwrap_err();
        assert!(matches!(err, AppError::Port(PortError::NotFound(_))));
    }

    #[test]
    fn message_off_mainline_is_not_found() {
        let mut fixture = ctx_fixture();
        fixture.store.context_message_id = fixture.sidechain.clone();
        fixture.store.candidates = vec![PortMessageContextCandidate {
            session_id: fixture.session.clone(),
            placement_ids: vec![fixture.store.graph.placements[2].id.clone()],
        }];
        let err = app_ctx(&fixture.store)
            .handle(message_req_with_id(
                fixture.sidechain.clone(),
                Some(fixture.session.clone()),
                0,
                ResponseBudget::default(),
            ))
            .unwrap_err();
        assert!(matches!(err, AppError::Domain(DomainError::NotFound(_))));
    }

    #[test]
    fn message_budget_rejects_below_floor() {
        let fixture = ctx_fixture();
        let err = app_ctx(&fixture.store)
            .handle(message_req(
                &fixture,
                Some(fixture.session.clone()),
                0,
                ResponseBudget {
                    max_response_bytes: budget::MIN_RESPONSE_BYTES - 1,
                    ..Default::default()
                },
            ))
            .unwrap_err();
        assert!(matches!(err, AppError::Budget(_)));
    }

    #[test]
    fn message_item_budget_keeps_anchor_and_reports_max_items() {
        let fixture = ctx_fixture();
        let AppResponse::Message { window } = app_ctx(&fixture.store)
            .handle(message_req(
                &fixture,
                Some(fixture.session.clone()),
                9,
                ResponseBudget {
                    max_items: 1,
                    ..Default::default()
                },
            ))
            .unwrap()
        else {
            panic!("expected Message response");
        };
        assert_eq!(window.messages.len(), 1);
        assert_eq!(
            window.messages[0].placement_id,
            fixture.repeated_b.id.as_str()
        );
        assert_eq!(
            window.truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_ITEMS)
        );
    }

    #[test]
    fn message_byte_budget_keeps_anchor_and_reports_max_response_bytes() {
        let mut fixture = ctx_fixture();
        fixture.store.context_message_id = fixture.root.clone();
        fixture.store.candidates = vec![PortMessageContextCandidate {
            session_id: fixture.session.clone(),
            placement_ids: vec![fixture.root_placement.id.clone()],
        }];
        let oversized_text = "\"\n".repeat(3072);
        fixture.store.catalog.insert(
            &fixture.root,
            serde_json::json!({
                "role": "user",
                "text": oversized_text.clone(),
                "metadata": "m".repeat(4096),
            })
            .to_string()
            .into_bytes(),
        );
        let requested_budget = budget::MIN_RESPONSE_BYTES;
        let AppResponse::Message { window } = app_ctx(&fixture.store)
            .handle(message_req_with_id(
                fixture.root.clone(),
                Some(fixture.session.clone()),
                9,
                ResponseBudget {
                    max_response_bytes: requested_budget,
                    ..Default::default()
                },
            ))
            .unwrap()
        else {
            panic!("expected Message response");
        };
        assert_eq!(window.message_id, fixture.root.as_str());
        assert_eq!(window.session_id, fixture.session.as_str());
        assert_eq!(
            window.anchor_placement_id,
            fixture.root_placement.id.as_str()
        );
        assert_eq!(window.messages.len(), 1);
        let anchor = &window.messages[0];
        assert_eq!(anchor.id, fixture.root.as_str());
        assert_eq!(anchor.message_id, fixture.root.as_str());
        assert_eq!(anchor.placement_id, fixture.root_placement.id.as_str());
        assert!(anchor.payload.get("metadata").is_none());
        assert_eq!(anchor.payload["role"], "user");
        assert!(
            anchor.payload["text"].as_str().unwrap().len() < oversized_text.len(),
            "oversized anchor text must be projected"
        );
        assert_eq!(
            window.truncation.reason.as_deref(),
            Some(budget::TRUNCATION_MAX_RESPONSE_BYTES)
        );
        let render_equivalent_bytes = ENVELOPE_RESERVE_BYTES
            + window
                .messages
                .iter()
                .map(message_occurrence_bytes)
                .sum::<usize>();
        assert!(
            render_equivalent_bytes <= requested_budget,
            "projected response estimate {render_equivalent_bytes} exceeds {requested_budget}"
        );
    }

    // ---- stage（RFC-0002 §5）----

    #[test]
    fn stage_returns_all_messages_on_success() {
        let provider = FakeProvider::good("demo", &[("user", "hi"), ("assistant", "yo")]);
        let staged = stage(&provider, b"anything").unwrap();
        assert_eq!(staged.messages.len(), 2);
        assert_eq!(staged.messages[0].seq, 0);
        assert_eq!(staged.messages[0].role, "user");
        assert_eq!(staged.messages[0].text, "hi");
        assert_eq!(staged.messages[1].seq, 1);
        assert_eq!(staged.messages[1].role, "assistant");
        assert_eq!(staged.messages[1].text, "yo");
        assert_eq!(staged.report.committed, 2);
        assert_eq!(staged.report.skipped, 0);
        assert!(staged.report.diagnostics.is_empty());
    }

    #[test]
    fn stage_preserves_spans_and_complete_parse_report() {
        // 内联 provider：emit 带 span 的消息并报告会话 native id，
        // 验证 staging 对两者的透传（不落在 FakeProvider 上，保持 testkit 最小）。
        struct SpanProvider;
        impl ProviderAdapter for SpanProvider {
            fn provider_id(&self) -> &str {
                "span-demo"
            }
            fn manifest(&self) -> agent_session_grep_ports::AdapterManifest {
                agent_session_grep_ports::manifest_for(self.provider_id(), None, &[])
            }
            fn probe(
                &self,
                _bytes: &[u8],
            ) -> Result<agent_session_grep_ports::ProbeResult, ProviderError> {
                Ok(agent_session_grep_ports::ProbeResult {
                    variant_id: "span-demo/fake-v1".into(),
                    confidence: Confidence::Confirmed,
                    matched_evidence: vec!["inline".into()],
                    unmatched_evidence: Vec::new(),
                })
            }
            fn parse(
                &self,
                _bytes: &[u8],
                sink: &mut dyn CanonicalEventSink,
            ) -> Result<agent_session_grep_ports::ParseReport, ProviderError> {
                sink.emit_message(MessageEvent {
                    session: None,
                    seq: 0,
                    native_id: "m-1",
                    parent_native_id: None,
                    role: "user",
                    text: "hello",
                    timestamp: None,
                    is_sidechain: false,
                    span: Some((0, 42)),
                })
                .map_err(|e| ProviderError::Io(e.to_string()))?;
                sink.emit_message(MessageEvent {
                    session: None,
                    seq: 1,
                    native_id: "m-2",
                    parent_native_id: None,
                    role: "assistant",
                    text: "world",
                    timestamp: None,
                    is_sidechain: false,
                    span: None,
                })
                .map_err(|e| ProviderError::Io(e.to_string()))?;
                Ok(agent_session_grep_ports::ParseReport {
                    committed: 2,
                    skipped: 3,
                    diagnostics: vec!["record 3 skipped".into(), "unknown field seen".into()],
                    session_native_id: Some("native-sess-1".into()),
                    session_observation: Default::default(),
                })
            }
        }
        let staged = stage(&SpanProvider, b"x").unwrap();
        // span 逐条透传；None 保持显式缺失。
        assert_eq!(staged.messages[0].span, Some((0, 42)));
        assert_eq!(staged.messages[1].span, None);
        assert_eq!(staged.report.committed, 2);
        assert_eq!(staged.report.skipped, 3);
        assert_eq!(
            staged.report.diagnostics,
            vec!["record 3 skipped", "unknown field seen"]
        );
        assert_eq!(
            staged.report.session_native_id.as_deref(),
            Some("native-sess-1")
        );
        // Compatibility alias remains synchronized with the authoritative report.
        assert_eq!(staged.session_native_id.as_deref(), Some("native-sess-1"));
    }

    #[test]
    fn stage_rejects_ambiguous_variant() {
        let provider = FakeProvider::ambiguous("demo");
        let err = stage(&provider, b"x").unwrap_err();
        assert!(matches!(
            err,
            AppError::Domain(DomainError::InvalidRequest(_))
        ));
    }

    #[test]
    fn stage_ambiguous_error_does_not_leak_evidence() {
        // ambiguous 错误消息只报 variant 标识，不携带 unmatched_evidence——
        // 证据可能含路径/原文片段。
        struct LeakyAmbiguous;
        impl ProviderAdapter for LeakyAmbiguous {
            fn provider_id(&self) -> &str {
                "leaky"
            }
            fn manifest(&self) -> agent_session_grep_ports::AdapterManifest {
                agent_session_grep_ports::manifest_for(self.provider_id(), None, &[])
            }
            fn probe(
                &self,
                _bytes: &[u8],
            ) -> Result<agent_session_grep_ports::ProbeResult, ProviderError> {
                Ok(agent_session_grep_ports::ProbeResult {
                    variant_id: "leaky/unknown".into(),
                    confidence: Confidence::Ambiguous,
                    matched_evidence: Vec::new(),
                    unmatched_evidence: vec!["C:\\Users\\secret\\transcript.jsonl".into()],
                })
            }
            fn parse(
                &self,
                _bytes: &[u8],
                _sink: &mut dyn CanonicalEventSink,
            ) -> Result<agent_session_grep_ports::ParseReport, ProviderError> {
                Ok(agent_session_grep_ports::ParseReport::default())
            }
        }
        let err = stage(&LeakyAmbiguous, b"x").unwrap_err();
        let message = err.to_string();
        assert!(
            !message.contains("secret"),
            "错误消息不得泄漏证据内容: {message}"
        );
        assert!(
            message.contains("leaky/unknown"),
            "仍应报告 variant 标识: {message}"
        );
    }

    #[test]
    fn select_and_stage_probes_each_adapter_exactly_once() {
        // 选中 adapter 后复用其 probe 结果——同一字节不得二次 probe。
        struct ProbeCounting<'a> {
            inner: &'a FakeProvider,
            probes: &'a std::sync::atomic::AtomicUsize,
        }
        impl ProviderAdapter for ProbeCounting<'_> {
            fn provider_id(&self) -> &str {
                self.inner.provider_id()
            }
            fn manifest(&self) -> agent_session_grep_ports::AdapterManifest {
                self.inner.manifest()
            }
            fn probe(
                &self,
                bytes: &[u8],
            ) -> Result<agent_session_grep_ports::ProbeResult, ProviderError> {
                self.probes
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.inner.probe(bytes)
            }
            fn parse(
                &self,
                bytes: &[u8],
                sink: &mut dyn CanonicalEventSink,
            ) -> Result<agent_session_grep_ports::ParseReport, ProviderError> {
                self.inner.parse(bytes, sink)
            }
        }

        let inner = FakeProvider::good("demo", &[("user", "hi"), ("assistant", "yo")]);
        let probes = std::sync::atomic::AtomicUsize::new(0);
        let wrapper = ProbeCounting {
            inner: &inner,
            probes: &probes,
        };
        let refs: Vec<&dyn ProviderAdapter> = vec![&wrapper];

        let staged = select_and_stage(&refs, b"x").unwrap();
        assert_eq!(
            probes.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "选中后不得对同一字节二次 probe"
        );
        assert_eq!(staged.messages.len(), 2);
    }

    #[test]
    fn select_and_stage_surfaces_probe_error_line_detail_when_all_rejected() {
        // PRD R2.2：全部 adapter 拒绝时，最后一个 probe 错误自带的行号定位与
        // 修复方向必须透出——绝不裸报 "no provider recognized this source"。
        // （probe 错误只由源字节内容派生，provider 看不到路径，消息即诊断。）
        struct LineDetailRejecter;
        impl ProviderAdapter for LineDetailRejecter {
            fn provider_id(&self) -> &str {
                "line-detail"
            }
            fn manifest(&self) -> agent_session_grep_ports::AdapterManifest {
                agent_session_grep_ports::manifest_for(self.provider_id(), None, &[])
            }
            fn probe(
                &self,
                _bytes: &[u8],
            ) -> Result<agent_session_grep_ports::ProbeResult, ProviderError> {
                Err(ProviderError::AmbiguousVariant(
                    "not line-delimited JSON: 0/3 sampled lines parsed。第 2 行不是有效 JSON。请修复或删除这些行后重试"
                        .into(),
                ))
            }
            fn parse(
                &self,
                _bytes: &[u8],
                _sink: &mut dyn CanonicalEventSink,
            ) -> Result<agent_session_grep_ports::ParseReport, ProviderError> {
                Ok(agent_session_grep_ports::ParseReport::default())
            }
        }
        struct SilentRejecter;
        impl ProviderAdapter for SilentRejecter {
            fn provider_id(&self) -> &str {
                "silent"
            }
            fn manifest(&self) -> agent_session_grep_ports::AdapterManifest {
                agent_session_grep_ports::manifest_for(self.provider_id(), None, &[])
            }
            fn probe(
                &self,
                _bytes: &[u8],
            ) -> Result<agent_session_grep_ports::ProbeResult, ProviderError> {
                Ok(agent_session_grep_ports::ProbeResult {
                    variant_id: "silent/unknown".into(),
                    confidence: Confidence::Ambiguous,
                    matched_evidence: Vec::new(),
                    unmatched_evidence: Vec::new(),
                })
            }
            fn parse(
                &self,
                _bytes: &[u8],
                _sink: &mut dyn CanonicalEventSink,
            ) -> Result<agent_session_grep_ports::ParseReport, ProviderError> {
                Ok(agent_session_grep_ports::ParseReport::default())
            }
        }
        let refs: Vec<&dyn ProviderAdapter> = vec![&SilentRejecter, &LineDetailRejecter];
        let err = select_and_stage(&refs, b"x").unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("no provider recognized this source"),
            "分类前缀必须保留: {message}"
        );
        assert!(message.contains("第 2 行"), "必须携带行号定位: {message}");
        assert!(
            message.contains("请修复或删除这些行后重试"),
            "必须携带修复方向: {message}"
        );
    }

    #[test]
    fn select_and_stage_all_ambiguous_keeps_bare_message() {
        // 全部 adapter 只是 ambiguous（非报错）时，保持原有裸消息，不出现
        // "last probe failure" 后缀（没有可复用的错误细节）。
        struct AmbiguousOnly;
        impl ProviderAdapter for AmbiguousOnly {
            fn provider_id(&self) -> &str {
                "ambiguous-only"
            }
            fn manifest(&self) -> agent_session_grep_ports::AdapterManifest {
                agent_session_grep_ports::manifest_for(self.provider_id(), None, &[])
            }
            fn probe(
                &self,
                _bytes: &[u8],
            ) -> Result<agent_session_grep_ports::ProbeResult, ProviderError> {
                Ok(agent_session_grep_ports::ProbeResult {
                    variant_id: "ambiguous-only/unknown".into(),
                    confidence: Confidence::Ambiguous,
                    matched_evidence: Vec::new(),
                    unmatched_evidence: Vec::new(),
                })
            }
            fn parse(
                &self,
                _bytes: &[u8],
                _sink: &mut dyn CanonicalEventSink,
            ) -> Result<agent_session_grep_ports::ParseReport, ProviderError> {
                Ok(agent_session_grep_ports::ParseReport::default())
            }
        }
        let refs: Vec<&dyn ProviderAdapter> = vec![&AmbiguousOnly];
        let err = select_and_stage(&refs, b"x").unwrap_err();
        assert!(matches!(
            err,
            AppError::Domain(DomainError::InvalidRequest(_))
        ));
        assert_eq!(
            err.to_string(),
            "invalid request: no provider recognized this source",
            "无 probe 错误时消息必须保持原样"
        );
    }

    // ---- source-scoped staging（RFC-0002 §7 bounded ingest）----

    /// 用 BoundedWholeSource provider id（cline）的 fake：默认 probe_source /
    /// parse_source 走「上限内整读 + 委托字节 parse」路径，无需覆盖新方法。
    struct SourceFake;
    impl ProviderAdapter for SourceFake {
        fn provider_id(&self) -> &str {
            "cline"
        }
        fn manifest(&self) -> agent_session_grep_ports::AdapterManifest {
            agent_session_grep_ports::manifest_for(self.provider_id(), None, &[])
        }
        fn probe(
            &self,
            _bytes: &[u8],
        ) -> Result<agent_session_grep_ports::ProbeResult, ProviderError> {
            Ok(agent_session_grep_ports::ProbeResult {
                variant_id: "cline/fake-v1".into(),
                confidence: Confidence::Confirmed,
                matched_evidence: vec!["fake probe".into()],
                unmatched_evidence: Vec::new(),
            })
        }
        fn parse(
            &self,
            _bytes: &[u8],
            sink: &mut dyn CanonicalEventSink,
        ) -> Result<agent_session_grep_ports::ParseReport, ProviderError> {
            sink.emit_message(MessageEvent {
                session: None,
                seq: 0,
                native_id: "m-1",
                parent_native_id: None,
                role: "user",
                text: "hi",
                timestamp: None,
                is_sidechain: false,
                span: Some((0, 2)),
            })
            .map_err(|e| ProviderError::Io(e.to_string()))?;
            Ok(agent_session_grep_ports::ParseReport {
                committed: 1,
                ..Default::default()
            })
        }
    }

    #[test]
    fn select_and_stage_source_streams_selected_variant() {
        // 生产路径：probe/parse 都从只读 source 重新打开，返回 (staged, variant)。
        let source = agent_session_grep_ports::SliceSource::new(b"{}");
        let refs: Vec<&dyn ProviderAdapter> = vec![&SourceFake];
        let (staged, variant) = select_and_stage_source(&refs, &source).unwrap();
        assert_eq!(variant, "cline/fake-v1");
        assert_eq!(staged.messages.len(), 1);
        assert_eq!(staged.messages[0].text, "hi");
        assert_eq!(staged.report.committed, 1);
    }

    #[test]
    fn select_and_stage_source_rejects_source_beyond_declared_cap() {
        // 大文件上限回归：超过 manifest max_source_size 的源必须诚实拒绝
        // （SourceTooLarge 诊断），而不是按文件大小分配内存或静默截断。
        let big = vec![b'x'; (agent_session_grep_ports::JSON_FAMILY_MAX_SOURCE_BYTES as usize) + 1];
        let source = agent_session_grep_ports::SliceSource::new(&big);
        let refs: Vec<&dyn ProviderAdapter> = vec![&SourceFake];
        let err = select_and_stage_source(&refs, &source).unwrap_err();
        // probe 失败按既有选择语义跳过该 adapter，但最后 probe 错误细节（含
        // 受测上限）必须透出，绝不 OOM、绝不静默截断。
        assert!(matches!(
            err,
            AppError::Domain(DomainError::InvalidRequest(_))
        ));
        assert!(
            err.to_string().contains("exceeds supported limit"),
            "必须携带受测上限诊断: {err}"
        );
    }

    #[test]
    fn stage_discards_partial_on_mid_parse_fatal() {
        // 关键：provider 先 emit 2 条再 StructuralFatal。
        // stage 必须返回 Err，且不产出任何部分结果（原子丢弃）。
        let provider =
            FakeProvider::failing_after("demo", &[("user", "a"), ("assistant", "b")], "boom");
        let err = stage(&provider, b"x").unwrap_err();
        assert!(matches!(
            &err,
            AppError::Provider(ProviderError::StructuralFatal(_))
        ));
        assert!(err.to_string().contains("structural fatal"));
    }

    // ---- ADR-0009 resume metadata / cursor result_set ----

    struct FakeResumeClaims;
    impl ResumeClaimsStore for FakeResumeClaims {
        fn resume_of(&self, session_ids: &[StableId]) -> PortResult<Vec<SessionResumeMetadata>> {
            Ok(session_ids
                .iter()
                .map(|id| SessionResumeMetadata {
                    session_id: id.clone(),
                    provider_id: Some("claude-code".into()),
                    resume_available: true,
                    provider_session_id: Some("prov-sess-1".into()),
                    original_working_directory: Some("C:/work".into()),
                    unavailable_reason: None,
                })
                .collect())
        }
    }

    #[test]
    fn get_session_resume_returns_fixed_nullable_metadata() {
        let session = StableId::native(IdKind::Session, "sess-resume");
        let app = App::with_resume_and_clock(FakeCatalog, FakeIndex, FakeResumeClaims, clock_t0);
        let AppResponse::SessionResume(metadata) = app
            .handle(AppRequest::GetSessionResume {
                session_id: session.clone(),
            })
            .unwrap()
        else {
            panic!("expected SessionResume response");
        };
        assert_eq!(metadata.session_id, session);
        assert_eq!(metadata.provider_id.as_deref(), Some("claude-code"));
        assert!(metadata.resume_available);
        assert_eq!(metadata.provider_session_id.as_deref(), Some("prov-sess-1"));
        assert_eq!(
            metadata.original_working_directory.as_deref(),
            Some("C:/work")
        );
        assert!(metadata.unavailable_reason.is_none());
    }

    #[test]
    fn get_session_resume_rejects_non_session_kind() {
        let message = StableId::native(IdKind::Message, "msg-not-session");
        let err = app()
            .handle(AppRequest::GetSessionResume {
                session_id: message,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            AppError::Domain(DomainError::InvalidRequest(_))
        ));
    }

    #[test]
    fn get_session_resume_without_claims_reports_unavailable() {
        // NoResumeClaims 兜底：resume_available=false + 明确 unavailable_reason。
        let session = StableId::native(IdKind::Session, "sess-no-claims");
        let AppResponse::SessionResume(metadata) = app()
            .handle(AppRequest::GetSessionResume {
                session_id: session.clone(),
            })
            .unwrap()
        else {
            panic!("expected SessionResume response");
        };
        assert_eq!(metadata.session_id, session);
        assert!(!metadata.resume_available);
        assert!(metadata.unavailable_reason.is_some());
    }

    #[test]
    fn search_hits_carry_resume_availability_from_claims() {
        // 一次批量 resume_of 装配页内命中：有声明 → true；无声明/无归属 → false。
        let mut cat = MapCatalog::new(7);
        let session_a = StableId::native(IdKind::Session, "sess-a");
        let session_b = StableId::native(IdKind::Session, "sess-b");
        for (tag, session) in [
            ("hit00", Some(&session_a)),
            ("hit01", Some(&session_b)),
            ("hit02", None),
        ] {
            let id = hit_id(tag);
            cat.insert(
                &id,
                serde_json::json!({ "role": "user", "text": "needle" })
                    .to_string()
                    .into_bytes(),
            );
            if let Some(session) = session {
                cat.set_session_of(&id, session);
            }
        }
        struct OnlySessionA;
        impl ResumeClaimsStore for OnlySessionA {
            fn resume_of(
                &self,
                session_ids: &[StableId],
            ) -> PortResult<Vec<SessionResumeMetadata>> {
                Ok(session_ids
                    .iter()
                    .map(|id| SessionResumeMetadata {
                        session_id: id.clone(),
                        provider_id: None,
                        resume_available: id.as_str().ends_with("sess-a"),
                        provider_session_id: None,
                        original_working_directory: None,
                        unavailable_reason: (!id.as_str().ends_with("sess-a"))
                            .then(|| "no claims".into()),
                    })
                    .collect())
            }
        }
        let index = FixedHits(vec![hit_id("hit00"), hit_id("hit01"), hit_id("hit02")]);
        let app = App::with_resume_and_clock(cat, index, OnlySessionA, clock_t0);
        let AppResponse::Search { hits, .. } = app.handle(search_req("needle", 10, None)).unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(hits.len(), 3);
        // rank signals 把同分命中重钉为 wire id 升序，位置断言改为按 id 查找，
        // 保持"有声明 → true；无声明/无归属 → false"的语义不变。
        let resume_of = |tag: &str| {
            hits.iter()
                .find(|hit| hit.id == hit_id(tag))
                .unwrap_or_else(|| panic!("missing hit {tag}"))
                .resume_available
        };
        assert!(resume_of("hit00"), "claimed session must be true");
        assert!(!resume_of("hit01"), "unclaimed session must be false");
        assert!(!resume_of("hit02"), "no session_id must be false");
    }

    #[test]
    fn search_group_by_session_carries_resume_availability() {
        // 归并路径同样在 clamp 后装配：组代表命中携带其会话的声明。
        let mut cat = MapCatalog::new(7);
        let session_a = StableId::native(IdKind::Session, "sess-a");
        for tag in ["hit00", "hit01"] {
            let id = hit_id(tag);
            cat.insert(
                &id,
                serde_json::json!({ "role": "user", "text": "needle" })
                    .to_string()
                    .into_bytes(),
            );
            cat.set_session_of(&id, &session_a);
        }
        let index = FixedHits(vec![hit_id("hit00"), hit_id("hit01")]);
        let app = App::with_resume_and_clock(cat, index, FakeResumeClaims, clock_t0);
        let AppResponse::Search { hits, .. } = app
            .handle(AppRequest::Search {
                query: "needle".into(),
                filters: SearchFilters::default(),
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                include_system: false,
                group_by_session: true,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })
            .unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].occurrences, 2);
        assert!(hits[0].resume_available);
    }

    #[test]
    fn search_without_resume_claims_reports_false() {
        // NoResumeClaims 默认：resume_available 恒 false。
        let mut cat = MapCatalog::new(7);
        let session_a = StableId::native(IdKind::Session, "sess-a");
        let id = hit_id("hit00");
        cat.insert(
            &id,
            serde_json::json!({ "role": "user", "text": "needle" })
                .to_string()
                .into_bytes(),
        );
        cat.set_session_of(&id, &session_a);
        let app = App::with_clock(cat, FixedHits(vec![id]), clock_t0);
        let AppResponse::Search { hits, .. } = app.handle(search_req("needle", 10, None)).unwrap()
        else {
            panic!("expected Search response");
        };
        assert_eq!(hits.len(), 1);
        assert!(!hits[0].resume_available);
    }

    #[test]
    fn list_cursor_rejects_result_set_mismatch() {
        // result_set 判别器：sessions_only 与全实体列表的续读令牌不得互换。
        let mut cat = MapCatalog::new(7);
        for tag in ["la", "lb", "lc"] {
            let id = StableId::derive(IdKind::Message, Stability::Reconstructed, &[tag.as_bytes()]);
            cat.insert(&id, b"x".to_vec());
        }
        for tag in ["ls-a", "ls-b", "ls-c"] {
            let id = StableId::native(IdKind::Session, tag);
            cat.insert(&id, b"x".to_vec());
        }
        let app = App::with_clock(&cat, FakeIndex, clock_t0);
        let list_req = |cursor: Option<String>, sessions_only: bool| AppRequest::List {
            limit: 2,
            cursor,
            budget: ResponseBudget::default(),
            sessions_only,
        };
        let AppResponse::List { next_cursor, .. } = app.handle(list_req(None, false)).unwrap()
        else {
            panic!("expected List response");
        };
        let all_token = next_cursor.expect("list must page");

        // 全实体令牌 + sessions_only=true → 拒绝。
        let err = app
            .handle(list_req(Some(all_token.clone()), true))
            .unwrap_err();
        assert!(
            matches!(err, AppError::Cursor(cursor::CursorError::Invalid(_))),
            "{err}"
        );

        // 反向：sessions_only 令牌 + 全实体 → 拒绝。
        let AppResponse::List { next_cursor, .. } = app.handle(list_req(None, true)).unwrap()
        else {
            panic!("expected List response");
        };
        let sessions_token = next_cursor.expect("sessions list must page");
        let err = app
            .handle(list_req(Some(sessions_token), false))
            .unwrap_err();
        assert!(
            matches!(err, AppError::Cursor(cursor::CursorError::Invalid(_))),
            "{err}"
        );

        // 同判别器续读正常。
        assert!(app.handle(list_req(Some(all_token), false)).is_ok());
    }
}

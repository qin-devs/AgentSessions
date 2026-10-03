//! Ports：Application 与外部世界之间的契约边界（Hexagonal 架构的"端口"）。
//!
//! 这里只定义 trait 和相关 DTO，不含任何具体实现——实现属于 adapter crate。
//! Application 只依赖本 crate 的抽象，从而与 SQLite / 文件系统 / 具体 provider 解耦。
//!
//! 分层依赖不变量：domain ← ports ← application ← adapters。

pub mod redact;
pub mod relocation;

use agent_session_grep_domain::{
    DomainError, DomainResult, PlacementId, SessionContextGraph, StableId, ToolActivity,
    ToolActivityKind, UsageObservation,
};
use std::io::{BufRead, Read};

/// 端口层错误：包裹底层 IO/存储故障，向上层暴露稳定分类。
///
/// 与 `DomainError` 区分：DomainError 是"业务语义错误"，PortError 是"基础设施错误"。
/// Application 层负责把 PortError 归一为对外协议错误。
#[derive(Debug, thiserror::Error)]
pub enum PortError {
    /// Invalid caller input or a relocation plan that cannot authorize this request.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// The catalog changed after the caller obtained its plan.
    #[error("catalog generation mismatch: {0}")]
    GenerationMismatch(String),
    /// 底层存储/IO 故障。
    #[error("backend failure: {0}")]
    Backend(String),

    /// 源文件读取或快照元数据 I/O 故障。
    #[error("source I/O failure: {0}")]
    SourceIo(String),

    /// 当前二进制不支持该 catalog schema 版本。
    #[error("schema incompatible: {0}")]
    SchemaIncompatible(String),

    /// 请求的资源在后端不存在。
    #[error("not found: {0}")]
    NotFound(String),

    /// 源快照校验失败（长度/mtime/指纹不一致，见 ReadOnlySourceSnapshot 契约）。
    #[error("source snapshot changed: {0}")]
    SnapshotChanged(String),

    /// data-root writer lease 已被占用，未能取得（对应 error catalog `writer_busy`）。
    #[error("writer lease held: {0}")]
    WriterBusy(String),
}

pub type PortResult<T> = Result<T, PortError>;

/// 只读源快照：记录发现时刻源文件的验证元数据。
///
/// 落实 ReadOnlySourceSnapshot 契约——原始会话文件严格只读，
/// 通过 len + mtime + fingerprint 三元组检测读取期间是否被并发改写。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSnapshot {
    /// 源在存储中的规范路径（相对 data-root 或绝对，由 adapter 定义）。
    pub path: String,
    /// Captured byte length (logical backup length for SQLite sources).
    pub len: u64,
    /// 修改时间（Unix 毫秒）。
    pub mtime_ms: i64,
    /// Opaque content fingerprint. Files use BLAKE3 hex; SQLite logical
    /// snapshots use `sqlite:` followed by the backup's BLAKE3 hex.
    pub fingerprint: String,
}

/// 源发现端口：枚举某个 provider 根目录下的候选原始会话文件。
///
/// 严格只读——实现绝不修改被发现的文件。
pub trait SourceDiscovery {
    /// 列出当前可见的源快照。返回顺序不做保证。
    fn discover(&self) -> PortResult<Vec<SourceSnapshot>>;

    /// 读取指定源的完整字节，并校验其未在发现后被改写。
    ///
    /// 若 len/mtime/fingerprint 与传入快照不符，返回 [`PortError::SnapshotChanged`]。
    fn read_verified(&self, snapshot: &SourceSnapshot) -> PortResult<Vec<u8>>;
}

/// Catalog 列表项：稳定 ID + 已规范化 payload。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub id: StableId,
    pub payload: Vec<u8>,
}

/// A request-scoped, already pinned read view. Dropping the last nested guard
/// releases the view, including on early return or unwinding. Implementations
/// must not retain an interior-mutability borrow across other port calls.
/// Keep this guard out of writes, prompts, model loading and network waits.
pub trait ReadSnapshot {}

impl ReadSnapshot for () {}

/// 目录存储端口：规范化实体的持久化目录（对应 SQLite catalog）。
///
/// 只暴露按 StableId 存取及稳定排序列表；全文查询能力由 SearchIndex 承担。
pub trait CatalogStore {
    /// Pin the shared catalog/search/context/resume read view before any read.
    /// All mutable data ports in one request must use this same backend session.
    /// Nested guards share the view until the last guard drops (not just LIFO).
    /// The default is only suitable for immutable/in-memory request fixtures;
    /// mutable persistent adapters must override it, as must forwarding wrappers.
    fn begin_read_snapshot(&self) -> PortResult<Box<dyn ReadSnapshot + '_>> {
        Ok(Box::new(()))
    }

    /// 按 StableId 取回已规范化实体的原始 JSON 负载。
    fn get(&self, id: &StableId) -> PortResult<Option<Vec<u8>>>;

    /// 按多个 StableId 批量取回已规范化实体的原始 JSON 负载。
    ///
    /// 返回顺序与 `ids` 完全一致（保序）；目录中不存在的 id 对应 `None`。
    /// 实现必须批量读取（分块 IN 等），不得逐条查询（N+1）。
    fn get_many(&self, ids: &[StableId]) -> PortResult<Vec<(StableId, Option<Vec<u8>>)>>;

    /// 写入（或覆盖）一个规范化实体。
    fn put(&self, id: &StableId, payload: &[u8]) -> PortResult<()>;

    /// 按 wire id 升序列出最多 `limit` 个实体；稳定排序便于后续接 cursor。
    fn list(&self, limit: usize) -> PortResult<Vec<CatalogEntry>>;

    /// 只列出 Session 实体（`ses_v1_` 前缀），仍按 wire id 升序。
    ///
    /// 过滤必须在存储层完成（而不是 application 取全量后再筛）：调用方按
    /// 过滤后集合的 offset 续页，若先 `list(limit)` 再筛会得到少于 `limit`
    /// 的结果并让 cursor 错位（competitor-borrowings R1.3）。
    fn list_sessions(&self, limit: usize) -> PortResult<Vec<CatalogEntry>>;

    /// 批量读取会话标题投影（schema v13 `session_titles`）：与 `session_ids`
    /// 同序的 `Option<String>`，`None` 表示该会话没有可派生标题（无候选或
    /// 存储无此投影）。实现必须批量读取（分块 IN），不得逐条查询（N+1）。
    ///
    /// 默认空实现：无标题投影的存储对每个 id 返回 `None`，保持与条目数
    /// 对齐的契约不变。
    fn session_titles(&self, session_ids: &[StableId]) -> PortResult<Vec<Option<String>>> {
        Ok(session_ids.iter().map(|_| None).collect())
    }

    /// 批量读取会话 repo 身份投影（schema v16 `session_repo_slugs`）：与
    /// `session_ids` 同序的 `Option<String>`，值为三段 slug `host/owner/name`。
    /// `None` 表示该会话没有 repo 身份（检测失败/非 git 目录/存储无此投影）——
    /// "无行 = 未知"，未知绝不等于匹配。实现必须批量读取（分块 IN），不得逐条
    /// 查询（N+1）。
    ///
    /// 默认空实现：无 repo 投影的存储对每个 id 返回 `None`，保持与条目数对齐。
    fn session_repo_slugs(&self, session_ids: &[StableId]) -> PortResult<Vec<Option<String>>> {
        Ok(session_ids.iter().map(|_| None).collect())
    }

    /// Catalog 当前实体总数（status/doctor 使用）。
    fn count(&self) -> PortResult<u64>;

    /// 全库 token 用量聚合（usage 维度只读投影）。`None` = 存储无 usage
    /// 投影（legacy schema 未迁移或后端未实现）；`Some(totals)` 且
    /// `totals.sessions == 0` = 有投影但没有任何 usage 事实（未知 ≠ 零）。
    /// 默认空实现：无 usage 投影的存储返回 `None`。
    fn usage_totals(&self) -> PortResult<Option<UsageTotals>> {
        Ok(None)
    }

    /// 当前对外可见的不可变 generation。`0` 表示尚未激活任何写批次。
    fn active_generation(&self) -> PortResult<u64>;

    /// 全库 repo 身份聚合（schema v16 只读投影；status 展示用）。
    ///
    /// 返回 `(repo_slug, sessions)` 列表；无 repo 投影的存储返回空列表
    /// （未知 ≠ 零）。默认空实现。
    fn repo_totals(&self) -> PortResult<Vec<RepoTotals>> {
        Ok(Vec::new())
    }
}

/// One distinct Session that contains placements for a stable Message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageContextCandidate {
    pub session_id: StableId,
    pub placement_ids: Vec<PlacementId>,
}

/// The authoritative source placement of one Message: the owning document and
/// the provider-reported evidence byte range within it.
///
/// Byte ranges are half-open `[byte_start, byte_end)` offsets into the exact
/// verified source document named by `source_document_id`. Absolute paths never
/// cross this boundary. `byte_start`/`byte_end` are `None` when the provider
/// could not attribute one contiguous range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePlacement {
    pub source_document_id: StableId,
    pub byte_start: Option<u64>,
    pub byte_end: Option<u64>,
}

/// Aggregate contextual-relation counts exposed without backend details.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContextStats {
    pub placements: u64,
    pub source_placement_claims: u64,
}

/// 全库 token 用量聚合（status 展示用；只读投影）。
///
/// 覆盖标记原则（agentsview has_*_tokens 同义）：`sessions == 0` 表示库中
/// **没有任何** usage 事实（未知），而不是"用量为零"——真 0 与未知必须可区分。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageTotals {
    /// 至少挂有一条 usage 事件的会话数。
    pub sessions: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub reasoning_tokens: u64,
    /// 事件数按来源分列（observed = provider 逐事件给出，derived = 累计量派生）。
    pub observed_events: u64,
    pub derived_events: u64,
}

/// 单仓库会话聚合（schema v16 `session_repo_slugs` 只读投影；status 展示用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoTotals {
    /// 三段 repo slug（`host/owner/name`）——投影只存 slug，绝不落绝对路径。
    pub repo_slug: String,
    /// 派生到该 slug 的会话数。
    pub sessions: u64,
}

/// Backend-independent read capability for contextual Message relations.
///
/// Implementations return Domain graph values and stable typed candidates;
/// SQLite rows, table names, and compatibility JSON never cross this boundary.
pub trait ContextGraphStore {
    fn load_session_graph(&self, session_id: &StableId) -> PortResult<SessionContextGraph>;

    /// Return candidates grouped by distinct Session, not by raw placement.
    ///
    /// A message that is not present in the catalog is a lookup miss and must
    /// fail with [`PortError::NotFound`], matching `load_session_graph`; it is
    /// never an empty success. Implementations must not invent candidates for
    /// messages they cannot see.
    fn message_contexts(&self, message_id: &StableId) -> PortResult<Vec<MessageContextCandidate>>;

    /// 批量解析消息的归属会话（ADR-0008）：返回与 `message_ids` 同序的
    /// `(消息 id, 会话 id)`。消息没有任何 placement → `None`。
    ///
    /// 消息可同时属于多个会话（多会话文件、被复制的历史）；此处取确定的单个
    /// 会话——所有 placement 中 wire id 字典序最小的会话（与列表类用例的
    /// wire-id-asc 惯例一致），保证分页游标下的结果稳定。实现必须批量读取
    /// （分块 IN），不得逐条查询（N+1）。
    fn session_of(&self, message_ids: &[StableId])
    -> PortResult<Vec<(StableId, Option<StableId>)>>;

    /// Batch-resolve each Message's authoritative source placement (owning
    /// document + evidence byte range), in `message_ids` order.
    ///
    /// A message with no placement resolves to `None`. For a message with
    /// multiple placements the deterministic single placement is chosen —
    /// the lexicographically smallest `source_document_id` wire id (matching
    /// the [`Self::session_of`] convention), so results are stable across
    /// calls. Implementations must batch read (chunked IN), never query per id.
    fn source_placements_of(
        &self,
        message_ids: &[StableId],
    ) -> PortResult<Vec<(StableId, Option<SourcePlacement>)>>;

    /// Batch-load tool activities for the given message wire ids.
    ///
    /// Default empty: stores without a tool_activities projection contribute
    /// nothing. SQLite implements the real batch read.
    fn tool_activities_for_messages(
        &self,
        _message_ids: &[StableId],
    ) -> PortResult<Vec<serde_json::Value>> {
        Ok(Vec::new())
    }

    fn context_stats(&self) -> PortResult<ContextStats>;
}

/// A provider whose authoritative source-document metadata may constrain search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SearchProvider(&'static str);

impl SearchProvider {
    // Preserve the original public constructor spellings while making the
    // capability matrix authoritative for every additional provider.
    #[allow(non_upper_case_globals)]
    pub const Claude: Self = Self("claude-code");
    #[allow(non_upper_case_globals)]
    pub const Codex: Self = Self("codex");

    pub fn parse(value: &str) -> Option<Self> {
        capability::canonical_search_provider_id(value).map(Self)
    }

    /// Canonical provider id stored in SourceDocument payloads.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// A normalized instant used by backend-independent search filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SearchInstant {
    pub unix_seconds: i64,
    pub nanosecond: u32,
}

impl SearchInstant {
    pub fn from_unix_millis(milliseconds: i64) -> Self {
        Self {
            unix_seconds: milliseconds.div_euclid(1_000),
            nanosecond: (milliseconds.rem_euclid(1_000) as u32) * 1_000_000,
        }
    }

    /// Big-endian key whose byte order is the instant order. SQLite uses this
    /// as a BLOB so comparisons retain nanosecond precision.
    pub fn sort_key(self) -> [u8; 12] {
        let mut key = [0; 12];
        key[..8].copy_from_slice(&((self.unix_seconds as u64) ^ (1_u64 << 63)).to_be_bytes());
        key[8..].copy_from_slice(&self.nanosecond.to_be_bytes());
        key
    }
}

/// Backend-independent, normalized metadata predicates for a search query.
///
/// `providers` is a canonical sorted set at the Application boundary. Provider
/// entries are ORed; provider and time dimensions are ANDed. Time is a
/// half-open UTC interval `[since, until)`. `repo` (schema v16) is an exact
/// match on the privacy-safe `host/owner/name` repo slug derived from the
/// session's pair-observed working directory; `None` = no repo restriction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchFilters {
    pub providers: Vec<SearchProvider>,
    pub since: Option<SearchInstant>,
    pub until: Option<SearchInstant>,
    pub repo: Option<String>,
}

impl SearchFilters {
    pub const EMPTY: Self = Self {
        providers: Vec::new(),
        since: None,
        until: None,
        repo: None,
    };

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
            && self.since.is_none()
            && self.until.is_none()
            && self.repo.is_none()
    }
}

/// One normalized full-text query and its metadata predicates.
#[derive(Debug, Clone, Copy)]
pub struct SearchQuery<'a> {
    pub text: &'a str,
    pub filters: &'a SearchFilters,
}

/// 检索命中：一条搜索结果。
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    /// 命中实体的稳定 ID。
    pub id: StableId,
    /// 相关性打分（后端相对值，跨后端不可比）。
    pub score: f32,
    /// 命中实体的归属会话 wire id（ADR-0008），由 Application 检索装配时经
    /// [`ContextGraphStore::session_of`] 批量填充。`None` 表示该消息没有
    /// 任何 placement（无归属会话）。
    pub session_id: Option<String>,
    /// 命中实体正文的摘要，由 Application 检索装配时填充（保序批量取 payload
    /// 的规范 `text` 字段后构建显示窗口）。`None` 表示该实体没有可展示正文。
    ///
    /// 摘要字符数 ≤ `max_snippet_chars`，且始终是原文的连续切片：存在可证明的
    /// 字面命中（与 `why_matched` 同源词元）时，以最早命中为中心按 2 右 : 1 左
    /// 交替扩展；锚点自身超过上限时取锚点起始的 `max_snippet_chars` 个字符；
    /// 无字面证据（含语义-only 命中）时回退为正文前缀。不插入省略号、高亮或
    /// 任何合成字符；大小写不敏感匹配把逐字符小写展开回映到原字符边界（如
    /// `İ`），只保证 Unicode 标量边界、不保证字素簇完整。摘要字节经既有
    /// 命中级字节估算计入 `max_response_bytes`（不分入口另行计费）。
    ///
    /// robot/json/jsonl 序列化器输出为命中对象的 `text` 字段；人类渲染器打印
    /// 同一摘要的片段行（human 预览会再按 `why_matched` 词元居中，见 CLI
    /// `render_search`）。
    pub text: Option<String>,
    /// 确定性字面量命中证据（search-match-guidance）：由 Application 用与索引侧
    /// 同一 CJK/plain-text 词元分析对用户**字面查询**派生，逐词断言在命中完整正文
    /// 中存在。只含字面词元/字段名——绝不携带 FTS 引号化查询串或语法表达式。
    /// 空列表表示无证据可附；序列化时空列表省略（追加字段，字节兼容）。
    pub why_matched: Vec<String>,
    /// 建议的下一步调用（search-match-guidance）：只由命中实际携带的 id 派生
    /// （get_message 需 message_id + session_id，缺任一则省略该条建议），
    /// 条数固定有界，绝不臆造标识符。
    pub suggested_next_commands: Vec<String>,
    /// 归并计数（competitor-borrowings R3）：`group_by_session` 模式下同一会话
    /// 的命中数；非归并模式下恒为 1（序列化时省略，保持既有输出字节兼容）。
    pub occurrences: usize,
    /// Resume Metadata 可用性（ADR-0009）：Application 经
    /// [`ResumeClaimsStore::resume_of`] 批量装配。`false` 仅表示该会话
    /// 没有可恢复的 Provider 元数据，绝不表示历史不可检索。
    pub resume_available: bool,
}

/// 检索模式：标识本次搜索结果使用哪种匹配策略（wire 字符串见 [`RetrievalMode::as_str`]）。
///
/// 与 `schemas/handoff/v1/pack.schema.json` 中 `retrieval_mode` 枚举一致。
/// 当前所有检索均为 [`RetrievalMode::Lexical`]（语义检索尚未实现）；
/// `LexicalFallback` 预留给语义检索失败后回退到词法检索的场景。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalMode {
    #[default]
    Lexical,
    Semantic,
    Hybrid,
    LexicalFallback,
}

impl RetrievalMode {
    /// Wire 字符串（envelope `retrieval_mode` 字段值），与 schema 枚举一致。
    pub fn as_str(self) -> &'static str {
        match self {
            RetrievalMode::Lexical => "lexical",
            RetrievalMode::Semantic => "semantic",
            RetrievalMode::Hybrid => "hybrid",
            RetrievalMode::LexicalFallback => "lexical_fallback",
        }
    }
}

/// 脱敏状态：envelope `redaction` 字段的投影。
///
/// 与 `schemas/handoff/v1/pack.schema.json` 中 `redaction` 定义一致。
/// 当前所有响应固定为 `mode=default, status=none`（脱敏实现尚未落地，
/// 由后续任务完成）。`redacted_count` 为已脱敏条数；`audit_id` 关联审计记录。
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct RedactionStatus {
    pub mode: RedactionMode,
    pub status: RedactionState,
    pub ruleset_version: String,
    pub redacted_count: u64,
    pub audit_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedactionMode {
    #[default]
    Default,
    Revealed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedactionState {
    Applied,
    #[default]
    None,
    Partial,
}

impl RedactionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            RedactionMode::Default => "default",
            RedactionMode::Revealed => "revealed",
        }
    }
}

impl RedactionState {
    pub fn as_str(self) -> &'static str {
        match self {
            RedactionState::Applied => "applied",
            RedactionState::None => "none",
            RedactionState::Partial => "partial",
        }
    }
}

/// 检索的 sidechain facet：命中消息的 sidechain 状态过滤。
///
/// 语义（确定性）：`MainOnly` 只保留**没有任何** sidechain placement 的消息；
/// `SubagentOnly` 只保留**至少一个** sidechain placement 的消息；
/// `Include`（默认）不过滤。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SidechainFacet {
    #[default]
    Include,
    MainOnly,
    SubagentOnly,
}

impl SidechainFacet {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Include => "include",
            Self::MainOnly => "main_only",
            Self::SubagentOnly => "subagent_only",
        }
    }
}

/// 检索的 facet 过滤器（additive；默认值 = 无过滤，行为与 `query` 完全一致）。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchFacets {
    pub sidechain: SidechainFacet,
    /// 只保留附着至少一条 `kind` 工具活动的消息（闭集：file/command/web/query/unknown）。
    pub tool_kind: Option<String>,
    /// 只保留附着该工具名（逐字相等）活动的消息。
    pub tool_name: Option<String>,
}

impl SearchFacets {
    /// 是否与默认值（无过滤）相同。
    pub fn is_default(&self) -> bool {
        self.sidechain == SidechainFacet::Include
            && self.tool_kind.is_none()
            && self.tool_name.is_none()
    }

    /// 规范化绑定串：cursor 的 query digest 用它把 facet 绑定进续读令牌
    /// （同串在发行与校验两侧计算，facet 变化即旧令牌失效）。
    pub fn canonical_binding(&self) -> String {
        format!(
            "sidechain={}|tool_kind={}|tool_name={}",
            self.sidechain.as_str(),
            self.tool_kind.as_deref().unwrap_or(""),
            self.tool_name.as_deref().unwrap_or(""),
        )
    }
}

/// 工具活动 target 的存储/投影上限（字符数）：显式截断；真实 transcript 的
/// 路径与命令可能很长，但活动只承载检索面事实，不需要全文。
///
/// 单一来源：SQLite 落库前按本上限截断（`StoredActivity` 派生 activity id 前的
/// 归一化点），provider-claude 把 `tool_use` 摘要并入可检索正文时用同一上限——
/// 两侧同值，正文里看到的 target 与库里存的 target 才逐字一致。
pub const TOOL_ACTIVITY_TARGET_MAX_CHARS: usize = 512;

/// 工具名 → [`ToolActivityKind`] 的闭集（设计 R2）。名字**逐字**匹配 provider
/// 记录的工具名，大小写敏感；不在表内 → `Unknown`（fail-closed，绝不猜）。
///
/// 闭集里每个名字都有真实语料证据（普查口径：本机真实 transcript 的结构统计，
/// 只取「记录类型 / 工具名 / 参数键名」，不取任何内容字节）：
///
/// - Claude Code JSONL，`message.content[]` 的 `tool_use` block `name` 字段：
///   `PowerShell`（Windows 形态下最高频）、`Read`、`Edit`、`Grep`、`Agent`
///   （subagent 派发工具的现名，`Task` 为旧名）、`Bash`、`Write`、`Glob`、
///   `WebFetch`、`WebSearch`。
/// - Codex rollout，`response_item/function_call` 的 `name`（参数在 JSON 字符串
///   `arguments` 里）：`shell_command`（最高频）、`exec_command`、`web_fetch`、
///   `web_search`、`view_image`、`spawn_agent`。
/// - Codex rollout，`response_item/custom_tool_call` 的 `name`（参数在字符串
///   `input` 里）：`exec`、`apply_patch`。
///
/// 刻意**不**收入闭集：`mcp__*`（语义由 MCP server 定义）、`update_plan` /
/// `get_goal` / `wait` / `wait_agent` / `close_agent` / `send_message` /
/// `write_stdin` / `js` 等编排控制面工具（不属 file/command/web/query 任一类），
/// 以及用户自定义插件工具（`CronList` 等）。它们如实记 `Unknown`。
const COMMAND_TOOL_NAMES: [&str; 6] = [
    "Bash",
    "PowerShell",
    "shell",
    "shell_command",
    "exec",
    "exec_command",
];
/// 见 [`COMMAND_TOOL_NAMES`] 的证据说明。
const FILE_TOOL_NAMES: [&str; 8] = [
    "Read",
    "Write",
    "Edit",
    "MultiEdit",
    "NotebookEdit",
    "ApplyPatch",
    "apply_patch",
    "view_image",
];
/// 见 [`COMMAND_TOOL_NAMES`] 的证据说明。
const QUERY_TOOL_NAMES: [&str; 5] = ["Glob", "Grep", "Task", "Agent", "spawn_agent"];
/// 见 [`COMMAND_TOOL_NAMES`] 的证据说明。
const WEB_TOOL_NAMES: [&str; 4] = ["WebFetch", "WebSearch", "web_fetch", "web_search"];

/// 工具活动 kind 推断规则（设计 R2）：按 provider 记录的**工具名**判定，
/// 顺序匹配、首个命中生效；不在已知闭集内 → [`ToolActivityKind::Unknown`]
/// （fail-closed，绝不猜）。
pub fn infer_tool_activity_kind(name: &str) -> ToolActivityKind {
    if COMMAND_TOOL_NAMES.contains(&name) {
        return ToolActivityKind::Command;
    }
    if FILE_TOOL_NAMES.contains(&name) {
        return ToolActivityKind::File;
    }
    if QUERY_TOOL_NAMES.contains(&name) {
        return ToolActivityKind::Query;
    }
    if WEB_TOOL_NAMES.contains(&name) {
        return ToolActivityKind::Web;
    }
    ToolActivityKind::Unknown
}

/// 把 provider 记录的调用事实构造成规范活动（设计 R1/R2/R4 的单一落点）。
///
/// fail-closed 双保险：未知名 → `kind = Unknown` **且** `target = None`
/// （即使 input 带了形似 command 的字段也不猜）；已知名但 input 无可用字段 →
/// kind 保留、`target = None`。
pub fn build_tool_activity(
    name: &str,
    actor: agent_session_grep_domain::ToolActivityActor,
    input: &serde_json::Value,
    status: agent_session_grep_domain::ToolActivityStatus,
) -> ToolActivity {
    let kind = infer_tool_activity_kind(name);
    let target = if kind == ToolActivityKind::Unknown {
        None
    } else {
        extract_tool_activity_target(input)
    };
    ToolActivity {
        kind,
        actor,
        name: name.to_string(),
        target,
        status,
    }
}

/// 工具活动 target 提取优先级链（设计 R1）：在 provider 记录的 input 对象里按
/// 固定键序取第一个非空字符串；全部缺失/为空 → `None`（绝不猜）。
///
/// 键集与大体顺序沿用 Recall `src/adapters/events.rs::target_from_value`（MIT，
/// 已注明出处）：`path/file_path/filePath/target/command/cmd/…/pattern/glob/
/// glob_pattern/regex`；两处刻意调整并在设计文档记录：`url` 提到 `query` 之前
/// （本项目 Web 工具的 input 只有 url），末尾追加 `description`（Task 类工具）。
/// 与 Recall 的差异：只接受字符串值（数组形态的 `bash -c …` 参数在 Claude/Codex
/// 记录中不出现，fail-closed 不猜），键匹配区分大小写（provider 记录的确切字段名）。
///
/// **字符串形态的 input**（不是对象）同样受理：Codex rollout 的
/// `response_item/custom_tool_call` 把整段参数放在字符串 `input` 里（真实语料
/// 1503/1503 条如此），此时整条字符串就是 provider 记录的 target。其中
/// `apply_patch` 的 input 是补丁封套（真实语料 549/549 条首行恰为
/// `*** Begin Patch`），按封套语法取首个 `*** {Add,Update,Delete} File:` 的路径
/// ——比拿整段补丁当 target 有用得多；封套内没有 File 头时回退整段原文，绝不
/// 编造路径。
pub fn extract_tool_activity_target(input: &serde_json::Value) -> Option<String> {
    const PRIORITY: [&str; 13] = [
        "path",
        "file_path",
        "filePath",
        "target",
        "command",
        "cmd",
        "url",
        "query",
        "pattern",
        "glob",
        "glob_pattern",
        "regex",
        "description",
    ];
    if let Some(raw) = input.as_str() {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        return Some(apply_patch_target(trimmed).unwrap_or(trimmed).to_string());
    }
    let object = input.as_object()?;
    for key in PRIORITY {
        if let Some(value) = object.get(key).and_then(serde_json::Value::as_str) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

/// Codex `apply_patch` 封套里的首个受影响文件路径。
///
/// 只在**首行恰为** `*** Begin Patch` 时解析（fail-closed：真实语料里有把补丁
/// 文本嵌进 shell 命令的 `exec` 调用，那类命令必须整条保留，不能被当补丁解析）。
/// 返回 `None` 表示「不是补丁封套 / 封套内无 File 头」，由调用方回退原文。
fn apply_patch_target(input: &str) -> Option<&str> {
    const ENVELOPE_HEADER: &str = "*** Begin Patch";
    const FILE_HEADERS: [&str; 3] = ["*** Update File: ", "*** Add File: ", "*** Delete File: "];
    let mut lines = input.lines();
    if lines.next()?.trim() != ENVELOPE_HEADER {
        return None;
    }
    for line in lines {
        for header in FILE_HEADERS {
            if let Some(path) = line.trim_start().strip_prefix(header) {
                let path = path.trim();
                if !path.is_empty() {
                    return Some(path);
                }
            }
        }
    }
    None
}

/// 全文检索端口：对应 FTS5 主存（见 ADR-0001）。
pub trait SearchIndex {
    /// 将实体文本纳入索引。`text` 为已抽取的可检索正文。
    fn index(&self, id: &StableId, text: &str) -> PortResult<()>;

    /// Keep the omitted-filter call shape for existing clients. Application
    /// uses `query_filtered` when metadata predicates are present.
    fn query(&self, query: &str, limit: usize) -> PortResult<Vec<SearchHit>> {
        self.query_filtered(
            SearchQuery {
                text: query,
                filters: &SearchFilters::EMPTY,
            },
            limit,
        )
    }

    /// Execute a query with normalized metadata predicates, returning at most
    /// `limit` hits in the backend's pinned relevance order.
    fn query_filtered(&self, query: SearchQuery<'_>, limit: usize) -> PortResult<Vec<SearchHit>>;

    /// 带 facet 过滤的查询（additive）：`facets` 为默认值时语义与
    /// [`Self::query_filtered`] 完全一致（实现可短路）。命中仍按钉住排序
    /// （rank fusion + id tiebreak）。
    fn query_faceted(
        &self,
        query: SearchQuery<'_>,
        limit: usize,
        facets: &SearchFacets,
    ) -> PortResult<Vec<SearchHit>> {
        let _ = facets;
        self.query_filtered(query, limit)
    }

    /// Apply visibility before the backend limit. Legacy implementations may
    /// leave visibility to Application; storage adapters should push it down.
    fn query_with_policy(
        &self,
        query: SearchQuery<'_>,
        limit: usize,
        facets: &SearchFacets,
        include_system: bool,
    ) -> PortResult<Vec<SearchHit>> {
        let _ = include_system;
        self.query_faceted(query, limit, facets)
    }
}

/// 语义检索端口：本地 embedding 向量检索（ADR pending / #3 任务）。
///
/// 这是 semantic search 的端口契约。具体实现（sqlite-vec + ONNX Runtime
/// 或 candle）落在 adapter crate；Application 只依赖本抽象。
/// Lexical 永远可用；semantic 不可用时 Application 显式降级到
/// `RetrievalMode::LexicalFallback` + warning，禁止静默切换。
pub trait SemanticIndex {
    /// 将一条消息的 embedding 纳入向量索引。
    ///
    /// `embedding` 是归一化后的 float 向量（维度由模型 manifest 声明）。
    fn index_embedding(&self, id: &StableId, embedding: &[f32]) -> PortResult<()>;

    /// 执行语义查询：返回与 query embedding 最相似的 top-k 消息。
    ///
    /// 结果按余弦相似度降序。`limit` 是最大返回数。
    fn query_semantic(&self, query_embedding: &[f32], limit: usize) -> PortResult<Vec<SearchHit>> {
        self.query_semantic_filtered(
            query_embedding,
            limit,
            &SearchFilters::EMPTY,
            &SearchFacets::default(),
            true,
        )
    }

    /// Apply the same metadata/facet/visibility predicates as lexical search
    /// before selecting top-k. Non-finite vectors or scores are errors.
    fn query_semantic_filtered(
        &self,
        query_embedding: &[f32],
        limit: usize,
        filters: &SearchFilters,
        facets: &SearchFacets,
        include_system: bool,
    ) -> PortResult<Vec<SearchHit>>;

    /// Whether the selected model has live catalog vectors of the query dimension.
    /// Zero is never ready.
    /// Only `Ok(false)` permits lexical fallback; backend errors propagate.
    fn is_ready(&self, query_dimension: usize) -> PortResult<bool>;

    /// Current model identity for cursor binding; errors must not be hidden.
    fn semantic_model_id(&self) -> PortResult<Option<String>>;
}

/// 未提供语义索引实现时的占位（对应 `App<.., NoSemanticIndex>`）：语义/
/// 混合检索请求显式降级为 `RetrievalMode::LexicalFallback` + warning，
/// 与 `NoResumeClaims` 同一模式——既有构造签名零改动。
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSemanticIndex;

impl SemanticIndex for NoSemanticIndex {
    fn index_embedding(&self, _id: &StableId, _embedding: &[f32]) -> PortResult<()> {
        Ok(())
    }

    fn query_semantic_filtered(
        &self,
        _query_embedding: &[f32],
        _limit: usize,
        _filters: &SearchFilters,
        _facets: &SearchFacets,
        _include_system: bool,
    ) -> PortResult<Vec<SearchHit>> {
        Ok(Vec::new())
    }

    fn is_ready(&self, _query_dimension: usize) -> PortResult<bool> {
        Ok(false)
    }

    fn semantic_model_id(&self) -> PortResult<Option<String>> {
        Ok(None)
    }
}

/// Embedding 模型端口：把文本转成归一化向量。
///
/// 实现可能是 ONNX Runtime 动态加载、candle 本地推理、或外部 API（opt-in）。
/// 模型 manifest 记录 id/hash/dimension/license（#3 Req 1）。
pub trait EmbeddingModel {
    /// 把单条文本转成归一化 embedding 向量。
    ///
    /// `is_query` 为 true 时使用 query 前缀（如 `query: `），
    /// false 时使用 passage 前缀（如 `passage: `）。
    fn embed(&self, text: &str, is_query: bool) -> PortResult<Vec<f32>>;

    /// 模型维度（embedding 向量长度）。
    fn dimension(&self) -> usize;

    /// 模型 manifest 信息（id/hash/license）。
    fn manifest(&self) -> &EmbeddingManifest;
}

/// Embedding 模型 manifest：锁定模型身份与完整性（#3 Req 1）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EmbeddingManifest {
    /// 模型 id（如 `multilingual-e5-small`）。
    pub model_id: String,
    /// 模型文件 SHA-256（首次下载后校验）。
    pub file_hash: String,
    /// 向量维度。
    pub dimension: usize,
    /// 模型 license。
    pub license: String,
}

// 对 `&T` 的 blanket impl：端口方法均取 `&self`，故一个具体 store 可以
// 用共享引用同时填充 App<C,S> 的两个泛型槽（catalog 与 index 是同一实例）。
// 组合根据此复用单一 SqliteStore，无需两份连接或内部 Arc。
impl<T: CatalogStore + ?Sized> CatalogStore for &T {
    fn begin_read_snapshot(&self) -> PortResult<Box<dyn ReadSnapshot + '_>> {
        (**self).begin_read_snapshot()
    }

    fn get(&self, id: &StableId) -> PortResult<Option<Vec<u8>>> {
        (**self).get(id)
    }
    fn get_many(&self, ids: &[StableId]) -> PortResult<Vec<(StableId, Option<Vec<u8>>)>> {
        (**self).get_many(ids)
    }
    fn put(&self, id: &StableId, payload: &[u8]) -> PortResult<()> {
        (**self).put(id, payload)
    }
    fn list(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
        (**self).list(limit)
    }
    fn list_sessions(&self, limit: usize) -> PortResult<Vec<CatalogEntry>> {
        (**self).list_sessions(limit)
    }
    fn session_titles(&self, session_ids: &[StableId]) -> PortResult<Vec<Option<String>>> {
        (**self).session_titles(session_ids)
    }
    fn session_repo_slugs(&self, session_ids: &[StableId]) -> PortResult<Vec<Option<String>>> {
        (**self).session_repo_slugs(session_ids)
    }
    fn count(&self) -> PortResult<u64> {
        (**self).count()
    }
    fn usage_totals(&self) -> PortResult<Option<UsageTotals>> {
        (**self).usage_totals()
    }
    fn repo_totals(&self) -> PortResult<Vec<RepoTotals>> {
        (**self).repo_totals()
    }
    fn active_generation(&self) -> PortResult<u64> {
        (**self).active_generation()
    }
}

impl<T: ContextGraphStore + ?Sized> ContextGraphStore for &T {
    fn load_session_graph(&self, session_id: &StableId) -> PortResult<SessionContextGraph> {
        (**self).load_session_graph(session_id)
    }

    fn message_contexts(&self, message_id: &StableId) -> PortResult<Vec<MessageContextCandidate>> {
        (**self).message_contexts(message_id)
    }

    fn session_of(
        &self,
        message_ids: &[StableId],
    ) -> PortResult<Vec<(StableId, Option<StableId>)>> {
        (**self).session_of(message_ids)
    }

    fn source_placements_of(
        &self,
        message_ids: &[StableId],
    ) -> PortResult<Vec<(StableId, Option<SourcePlacement>)>> {
        (**self).source_placements_of(message_ids)
    }

    fn tool_activities_for_messages(
        &self,
        message_ids: &[StableId],
    ) -> PortResult<Vec<serde_json::Value>> {
        (**self).tool_activities_for_messages(message_ids)
    }

    fn context_stats(&self) -> PortResult<ContextStats> {
        (**self).context_stats()
    }
}

impl<T: SearchIndex + ?Sized> SearchIndex for &T {
    fn index(&self, id: &StableId, text: &str) -> PortResult<()> {
        (**self).index(id, text)
    }
    fn query_filtered(&self, query: SearchQuery<'_>, limit: usize) -> PortResult<Vec<SearchHit>> {
        (**self).query_filtered(query, limit)
    }
    fn query_faceted(
        &self,
        query: SearchQuery<'_>,
        limit: usize,
        facets: &SearchFacets,
    ) -> PortResult<Vec<SearchHit>> {
        (**self).query_faceted(query, limit, facets)
    }

    fn query_with_policy(
        &self,
        query: SearchQuery<'_>,
        limit: usize,
        facets: &SearchFacets,
        include_system: bool,
    ) -> PortResult<Vec<SearchHit>> {
        (**self).query_with_policy(query, limit, facets, include_system)
    }
}

/// 读取并校验一个源快照，把端口错误归一为领域语义，并返回校验过的字节——
/// 调用方直接复用返回值，不必再读一次（单次 I/O）。
///
/// - 快照漂移（[`PortError::SnapshotChanged`]） → [`DomainError::InvalidRequest`]
///   （可重试的"请求已过期"信号，由 application 决定如何降级）；
/// - 源缺失（[`PortError::NotFound`]） → [`DomainError::NotFound`]（保留分类，
///   协议层映射 not_found，而不是 bug 信号）；
/// - 其余端口故障 → [`DomainError::InvariantViolation`]。
///
/// 错误消息一律不带后端细节（可能含路径）：只保留稳定分类，细节由端口层日志承担。
pub fn ensure_readable(
    discovery: &dyn SourceDiscovery,
    snapshot: &SourceSnapshot,
) -> DomainResult<Vec<u8>> {
    match discovery.read_verified(snapshot) {
        Ok(bytes) => Ok(bytes),
        Err(PortError::SnapshotChanged(_)) => {
            Err(DomainError::InvalidRequest("source snapshot stale".into()))
        }
        Err(PortError::NotFound(_)) => {
            Err(DomainError::NotFound("source snapshot not found".into()))
        }
        Err(other) => Err(DomainError::InvariantViolation(format!(
            "unexpected backend error: {}",
            port_error_kind(&other)
        ))),
    }
}

/// 端口错误的稳定分类名（不含载荷——载荷可能携带后端路径等内部细节）。
fn port_error_kind(error: &PortError) -> &'static str {
    match error {
        PortError::Backend(_) => "backend",
        PortError::SourceIo(_) => "source_io",
        PortError::SchemaIncompatible(_) => "schema_incompatible",
        PortError::NotFound(_) => "not_found",
        PortError::SnapshotChanged(_) => "snapshot_changed",
        PortError::WriterBusy(_) => "writer_busy",
        PortError::InvalidRequest(_) => "invalid_request",
        PortError::GenerationMismatch(_) => "generation_mismatch",
    }
}

// ---------------------------------------------------------------------------
// Provider adapter 契约（落实 RFC-0002）。
//
// 每个 Provider 的格式差异被隔离在一个 adapter 内；adapter 之外只面对统一的
// Canonical 事件流。四阶段职责：discover / probe / fingerprint / parse。
// ---------------------------------------------------------------------------

/// Provider adapter 的错误分级（对应 RFC-0002 §5 错误矩阵）。
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ProviderError {
    /// variant 无法确定或未知——默认拒绝解析，不做"尽量解析"。
    #[error("ambiguous or unknown variant: {0}")]
    AmbiguousVariant(String),

    /// 结构性致命错误——source 整体回滚，不污染旧数据。
    #[error("structural fatal: {0}")]
    StructuralFatal(String),

    /// 读取期间源被改写（见 ReadOnlySourceSnapshot 契约）——丢弃 staging。
    #[error("source changed during read: {0}")]
    SourceChangedDuringRead(String),

    /// 源超过了该 adapter manifest 声明的受测上限——诚实拒绝而非 OOM。
    #[error("source is too large for this adapter: {actual} bytes exceeds supported limit {max}")]
    SourceTooLarge { actual: u64, max: u64 },

    /// 单条记录超过了该 adapter manifest 声明的受测上限。
    #[error(
        "source record is too large for this adapter: {actual} bytes exceeds supported limit {max}"
    )]
    RecordTooLarge { actual: u64, max: u64 },

    /// 底层 IO 故障。
    #[error("io failure: {0}")]
    Io(String),
}

/// variant 探测置信度（RFC-0002 §3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    /// 证据充分、唯一匹配已知 variant。
    Confirmed,
    /// 高度可能，但存在少量未匹配证据。
    High,
    /// 弱匹配——单独不足以承诺解析。
    Low,
    /// 多个 variant 都可能或无法区分——默认拒绝解析。
    Ambiguous,
}

/// variant 探测结果（RFC-0002 §3 的最小落地子集）。
///
/// 首个 provider 切片只承载判定所需的核心字段；`compatibility_range`、
/// `required_capabilities` 等留待 provider 数量增长后补齐，避免过早抽象。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeResult {
    /// 判定出的 variant 标识（如 `claude-code/jsonl-v1`）。
    pub variant_id: String,
    /// 探测置信度。
    pub confidence: Confidence,
    /// 支持判定的证据（人类可读，供诊断）。
    pub matched_evidence: Vec<String>,
    /// 与判定相悖或缺失的证据。
    pub unmatched_evidence: Vec<String>,
}

/// 解析报告（RFC-0002 §5）：区分 committed / skipped / failed，
/// 不把部分成功伪装成成功。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParseReport {
    /// 成功规范化并可提交的消息数。
    pub committed: usize,
    /// 可恢复地跳过的记录数（source 标记 incomplete，仍可提交）。
    pub skipped: usize,
    /// 诊断信息（跳过原因、未知字段计数等）。
    pub diagnostics: Vec<String>,
    /// provider 报告的 durable 会话 native id（如 Claude Code 的 `sessionId`、
    /// Codex `session_meta` 的 `session_id`）。`None` 表示 provider 未提供，
    /// 由上层回退 Reconstructed 派生——绝不臆造。
    pub session_native_id: Option<String>,
    /// Provider-native Resume Metadata 观察（ADR-0009）：native id 之外的
    /// `provider_session_id` 与 `original_working_directory` 同源关联、
    /// 显式可空、歧义 fail closed。旧 adapter 未填充时保持全 Missing。
    pub session_observation: ProviderSessionObservation,
}

/// 单个 Provider-native Resume Metadata 值的解析状态（ADR-0009）：
/// 缺失/解析成功/歧义三态，绝不臆造、绝不把不同来源的值拼成假 pair。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum MetadataResolution<T> {
    /// provider 未提供（字段缺失/空白）。
    #[default]
    Missing,
    /// provider 权威提供且唯一。
    Resolved(T),
    /// 同一 Source 观察到多个不同值（多会话/合并文件）——fail closed。
    Ambiguous,
}

/// 一次权威的 Provider-native Session Resume Metadata 观察（ADR-0009）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderSessionObservation {
    pub provider_session_id: MetadataResolution<String>,
    pub original_working_directory: MetadataResolution<String>,
    /// 两值来自同一条权威 provider 记录（保关联，绝不跨记录拼 pair）。
    pub pair_observed: bool,
    /// Source 携带多个不同 native Session ID（现有诊断契约的 typed 映射）。
    pub multi_session: bool,
}

/// 只读、固定形状的会话 Resume Metadata 投影（ADR-0009）：字段恒在、
/// 未知/歧义为 `None`；从不省略、从不臆造、从不暴露 Source path。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionResumeMetadata {
    /// Canonical Session 身份（关联/分组键，不是 provider 恢复标识）。
    pub session_id: StableId,
    pub provider_id: Option<String>,
    pub resume_available: bool,
    pub provider_session_id: Option<String>,
    pub original_working_directory: Option<String>,
    pub unavailable_reason: Option<String>,
}

/// Source-scoped Resume Metadata 声明的批量只读解析端口（ADR-0009）。
/// 与 Catalog/FTS 分离；历史缺失/歧义恒可检索，只是不可恢复。
pub trait ResumeClaimsStore {
    /// 批量解析 Session 的 Resume Metadata；与 `session_ids` 同序。
    /// 实现必须分块 IN 批量读取（无 N+1）；无声明/legacy → 全字段
    /// `None` + `resume_available:false` + 明确的 unavailable_reason。
    fn resume_of(&self, session_ids: &[StableId]) -> PortResult<Vec<SessionResumeMetadata>>;
}

/// Source-scoped Resume Metadata 声明（ADR-0009）：组合根把
/// [`ParseReport::session_observation`] 归一为本形状，随 source 事务
/// 原子写入；`None` 表示该 source 无可声明值（缺失/歧义已折叠进 state）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceResumeClaim {
    /// 该 source 认领的 provider id（如 `claude-code` / `codex`）。
    pub provider_id: String,
    /// 组合根派生的 canonical Session wire id（`ses_v1_*`），与本声明
    /// 同步写入同一 source 事务。
    pub session_id: String,
    pub provider_session_id: Option<String>,
    pub provider_session_id_state: String,
    pub original_working_directory: Option<String>,
    pub original_working_directory_state: String,
    pub pair_observed: bool,
}

impl SourceResumeClaim {
    /// 把 provider 观察转为 source 声明；state 取值与
    /// [`MetadataResolution`] 一一对应（`missing`/`resolved`/`ambiguous`）。
    pub fn from_observation(
        provider_id: &str,
        session_id: &str,
        observation: &ProviderSessionObservation,
    ) -> Self {
        // fail closed：同一 Source 出现多个不同 native Session ID 时，
        // 任何单个 ID 都不能被宣称权威（ADR-0009）——折叠为 ambiguous。
        let multi = observation.multi_session;
        let (id_value, id_state) = match &observation.provider_session_id {
            MetadataResolution::Missing => (None, "missing"),
            MetadataResolution::Resolved(v) if !multi => (Some(v.clone()), "resolved"),
            _ => (None, "ambiguous"),
        };
        let (dir_value, dir_state) = match &observation.original_working_directory {
            MetadataResolution::Missing => (None, "missing"),
            MetadataResolution::Resolved(v) if !multi => (Some(v.clone()), "resolved"),
            _ => (None, "ambiguous"),
        };
        Self {
            provider_id: provider_id.to_string(),
            session_id: session_id.to_string(),
            provider_session_id: id_value,
            provider_session_id_state: id_state.to_string(),
            original_working_directory: dir_value,
            original_working_directory_state: dir_state.to_string(),
            pair_observed: observation.pair_observed && !multi,
        }
    }
}

/// 默认空实现：任何 Session 均不可恢复（legacy/无声明）。App 在
/// 调用方未提供实现时用它兜底，保证既有构造签名不变。
#[derive(Debug, Clone, Copy, Default)]
pub struct NoResumeClaims;

impl ResumeClaimsStore for NoResumeClaims {
    fn resume_of(&self, session_ids: &[StableId]) -> PortResult<Vec<SessionResumeMetadata>> {
        Ok(session_ids
            .iter()
            .map(|id| SessionResumeMetadata {
                session_id: id.clone(),
                provider_id: None,
                resume_available: false,
                provider_session_id: None,
                original_working_directory: None,
                unavailable_reason: Some("no resume metadata claims".into()),
            })
            .collect())
    }
}

impl<T: ResumeClaimsStore + ?Sized> ResumeClaimsStore for &T {
    fn resume_of(&self, session_ids: &[StableId]) -> PortResult<Vec<SessionResumeMetadata>> {
        (**self).resume_of(session_ids)
    }
}

impl<T: SemanticIndex + ?Sized> SemanticIndex for &T {
    fn index_embedding(&self, id: &StableId, embedding: &[f32]) -> PortResult<()> {
        (**self).index_embedding(id, embedding)
    }

    fn query_semantic(&self, query_embedding: &[f32], limit: usize) -> PortResult<Vec<SearchHit>> {
        (**self).query_semantic(query_embedding, limit)
    }

    fn query_semantic_filtered(
        &self,
        query_embedding: &[f32],
        limit: usize,
        filters: &SearchFilters,
        facets: &SearchFacets,
        include_system: bool,
    ) -> PortResult<Vec<SearchHit>> {
        (**self).query_semantic_filtered(query_embedding, limit, filters, facets, include_system)
    }

    fn is_ready(&self, query_dimension: usize) -> PortResult<bool> {
        (**self).is_ready(query_dimension)
    }

    fn semantic_model_id(&self) -> PortResult<Option<String>> {
        (**self).semantic_model_id()
    }
}

/// 一条规范化消息的事件载荷（RFC-0002 §2）：parse 流式产出的最小单元。
///
/// 用结构体而非长参数列表，使后续增删字段（如工具调用元数据）不必改动
/// 所有 sink 实现的方法签名。字段刻意用 provider-native 的字符串/原串承载，
/// 由 sink 侧决定如何映射到 domain 类型（未知角色、id 稳定性策略归 sink）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageEvent<'a> {
    /// Explicit session membership for a multi-session source. None retains
    /// the report-level single-session contract.
    pub session: Option<&'a ProviderSessionIdentity>,
    /// Source-local ordinal, starting at zero and increasing for emitted messages.
    pub seq: u32,
    /// provider-native 消息 id（如 Claude Code 的 `uuid`）。空串表示 provider 未提供，
    /// 此时 sink 必须自行派生，且不得声称 `Stability::Native`。
    ///
    /// 空串是显式契约而非疏漏：provider 绝不编造形如 `<provider>-msg-{seq}` 的
    /// 假 id——那会把纯序号伪装成持久身份。composition root 的回退用
    /// document-scoped path-free facts（provider、variant、document id、seq）派生
    /// `Stability::Unstable`，因为 seq 会随 provider 记录过滤规则变化而漂移，
    /// 不能承诺跨运行稳定。
    pub native_id: &'a str,
    /// 父消息的 native id（threading 边）。`None` 表示根消息或 provider 未提供。
    pub parent_native_id: Option<&'a str>,
    /// 角色标签（`user`/`assistant`/`system`/`tool` 等原串）。
    pub role: &'a str,
    /// 归一化可检索正文。
    pub text: &'a str,
    /// provider 原样时间串（Claude Code 为 ISO-8601 UTC）。`None` 表示缺失。
    pub timestamp: Option<&'a str>,
    /// 是否为 sidechain（subagent/分支）消息。
    pub is_sidechain: bool,
    /// 本消息源记录在**已验证快照字节**中的字节区间 `(start, end)`，end 排他。
    ///
    /// 坐标系是"快照字节"而非"文件"：对文件级来源即快照全文，对未来的行级
    /// 来源即提取出的行负载——同一契约无需改动即可覆盖两者（R4）。
    /// `None` 表示 provider 无法归因一段连续区间，绝不臆造。
    pub span: Option<(u64, u64)>,
}

/// A provider's source-local session boundary, with independently observed
/// resume metadata. `source_key` is not a native ID and must never be exposed
/// as one; it distinguishes sessions whose provider omitted a durable ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSessionIdentity {
    pub source_key: String,
    pub observation: ProviderSessionObservation,
}

/// 一条规范化的工具活动观察（RFC-0002 §2 扩展）：provider 把 `tool_use` /
/// `tool_result`（或 Codex `custom_tool_call` / `function_call_output`）配对后
/// 连同锚定消息一起推入 sink。
///
/// 事实提取遵循任务 design 的显式有序规则集（target 优先级链、kind 推断、
/// actor、status）；fail-closed——未知一律 `kind = Unknown`、`target = None`，
/// 绝不臆造。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolActivityEvent<'a> {
    /// 锚定消息的 provider-native id（设计 R5 的 attachment 规则决定哪条消息）。
    pub message_native_id: &'a str,
    /// 提取出的完整活动事实（owned：provider 必须分配 target 字符串）。
    pub activity: ToolActivity,
}

/// 一条规范化的 token 用量观察（RFC-0002 §2 扩展，usage 维度）。
///
/// 只承载 provider 格式**明确给出**的数字（Observed），或由累计量经单调校验
/// 确定性派生的增量（Derived）——绝不按文本长度等代理估算。锚定规则：
/// `message_native_id` 非空时挂在该消息上（如 Claude Code 的 `message.usage`
/// 锚在 assistant 记录）；空串表示 **session 级观察**（如 Codex 的
/// `token_count` 累计事件没有消息关联），由组合根挂到该源的会话上。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageEvent<'a> {
    /// 锚定消息的 provider-native id；空串 = session 级观察，绝不臆造锚点。
    pub message_native_id: &'a str,
    /// 提取出的完整用量事实。
    pub usage: UsageObservation,
}

/// Canonical 事件接收端（RFC-0002 §2）：parse 流式产出，绝不整体加载。
///
/// adapter 把每条规范化消息推入 sink；sink 的具体实现（staging / 直接入库）
/// 由上层决定，adapter 不关心。
pub trait CanonicalEventSink {
    /// 接收一条规范化消息事件。
    ///
    /// 载荷用 [`MessageEvent`] 承载 native 身份、父指针、时间与 sidechain 标记；
    /// 角色/ id 均以字符串传递，避免 ports 依赖 domain 的 `Role`/`StableId` 构造细节，
    /// 由 sink 侧负责映射到 Canonical 类型（未知角色、id 稳定性策略归 sink）。
    fn emit_message(&mut self, event: MessageEvent<'_>) -> PortResult<()>;

    /// 接收一条工具活动观察（additive：默认实现为 no-op，既有 sink 不受影响）。
    ///
    /// 活动附着在 `event.message_native_id` 指向的消息上；sink 负责把 native id
    /// 解析为稳定消息身份，解析失败的活动必须丢弃（绝不臆造锚点）。
    fn emit_activity(&mut self, _event: ToolActivityEvent<'_>) -> PortResult<()> {
        Ok(())
    }

    /// 接收一条 token 用量观察（additive：默认实现为 no-op，既有 sink 不受影响）。
    ///
    /// `event.message_native_id` 非空时用量挂在该消息上；空串是 session 级观察。
    /// sink 负责解析锚点；解析失败必须丢弃（绝不臆造锚点）。
    fn emit_usage(&mut self, _event: UsageEvent<'_>) -> PortResult<()> {
        Ok(())
    }
}

/// A fresh, read-only view of one captured source (RFC-0002 §7 bounded ingest).
///
/// Implementations must return a newly positioned reader for every `open`
/// call. This lets capture, probe/parse, and final verification stream
/// independently without ever retaining the complete source in memory.
pub trait ReadOnlySource: Send + Sync {
    /// Captured byte length (the source's stable identity, not the live file).
    fn len(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Open a fresh reader positioned at the start of the captured range.
    ///
    /// Whole-source adapters bound total memory via their manifest
    /// `max_source_size`; record-stream adapters bound per-record memory.
    fn open(&self) -> PortResult<Box<dyn BufRead + Send + '_>>;
}

/// Bounded compatibility source over an in-memory slice.
///
/// Used by adapter unit tests and legacy byte callers (`parse(&[u8])`); it
/// produces the same line/offset semantics as a file-backed source.
pub struct SliceSource<'a> {
    bytes: &'a [u8],
}

impl<'a> SliceSource<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl ReadOnlySource for SliceSource<'_> {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn open(&self) -> PortResult<Box<dyn BufRead + Send + '_>> {
        Ok(Box::new(std::io::BufReader::with_capacity(
            64 * 1024,
            std::io::Cursor::new(self.bytes),
        )))
    }
}

/// 端口错误 → provider 错误（携带稳定分类，细节由端口层日志承担）。
fn provider_io(error: PortError) -> ProviderError {
    ProviderError::Io(error.to_string())
}

/// Read a whole source into a byte buffer bounded by `max_source_size`.
///
/// For `BoundedWholeSource` formats (JSON array / Markdown / SQLite) that need
/// the complete payload. The allocation is `try_reserve_exact`-guarded and the
/// total is re-checked while reading, so an adversarial growth never overflows
/// the declared limit.
pub fn read_bounded_source(
    source: &dyn ReadOnlySource,
    max_source_size: u64,
) -> Result<Vec<u8>, ProviderError> {
    if source.len() > max_source_size {
        return Err(ProviderError::SourceTooLarge {
            actual: source.len(),
            max: max_source_size,
        });
    }
    let capacity = usize::try_from(source.len()).map_err(|_| ProviderError::SourceTooLarge {
        actual: source.len(),
        max: max_source_size,
    })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| ProviderError::Io("bounded source buffer allocation failed".into()))?;
    let mut reader = source.open().map_err(provider_io)?;
    let mut chunk = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = reader
            .read(&mut chunk)
            .map_err(|error| ProviderError::Io(error.to_string()))?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if total > max_source_size {
            return Err(ProviderError::SourceTooLarge {
                actual: total,
                max: max_source_size,
            });
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    Ok(bytes)
}

/// Read one line (through `\n`) into `line`, bounded by `max_record_size`.
///
/// Returns the number of bytes read; 0 means EOF. The line buffer is cleared
/// and reused on every call, so streaming adapters never allocate per record.
fn read_one_bounded_line(
    reader: &mut dyn BufRead,
    line: &mut Vec<u8>,
    max_record_size: u64,
) -> Result<usize, ProviderError> {
    line.clear();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|error| ProviderError::Io(error.to_string()))?;
        if available.is_empty() {
            return Ok(line.len());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        let actual = line.len() as u64 + take as u64;
        if actual > max_record_size {
            return Err(ProviderError::RecordTooLarge {
                actual,
                max: max_record_size,
            });
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(line.len());
        }
    }
}

/// One bounded text record with original snapshot byte coordinates.
///
/// `bytes` is the record payload with CR/LF and a first-record UTF-8 BOM
/// removed; `start`/`end` are the record's byte range in the captured source
/// (end is exclusive and excludes the trailing newline, matching the
/// `MessageEvent::span` coordinate system). CRLF/BOM/truncated-tail handling
/// is byte-identical to the legacy whole-slice line split.
pub struct BoundedSourceLine<'a> {
    pub number: usize,
    pub start: u64,
    pub end: u64,
    pub bytes: &'a [u8],
}

/// A reusable bounded line reader over a [`ReadOnlySource`].
///
/// Production JSONL adapters iterate this with `while let Some(line) = ...`
/// so their per-line logic (including `continue`) stays in a plain loop. The
/// raw record buffer is reused across iterations, so memory is bounded by
/// `max_record_size`, never by file size.
pub struct BoundedLineReader<'a> {
    reader: Box<dyn BufRead + Send + 'a>,
    raw: Vec<u8>,
    offset: u64,
    number: usize,
    max_record_size: u64,
}

impl<'a> BoundedLineReader<'a> {
    pub fn new(source: &'a dyn ReadOnlySource, max_record_size: u64) -> PortResult<Self> {
        Ok(Self {
            reader: source.open()?,
            raw: Vec::new(),
            offset: 0,
            number: 1,
            max_record_size,
        })
    }

    /// Read the next stripped record; `Ok(None)` at EOF.
    ///
    /// The returned record borrows the reader's internal buffer and is only
    /// valid until the next call — iterate with `while let` and process each
    /// record before advancing.
    pub fn next_record(&mut self) -> Result<Option<BoundedSourceLine<'_>>, ProviderError> {
        let read =
            read_one_bounded_line(self.reader.as_mut(), &mut self.raw, self.max_record_size)?;
        if read == 0 {
            return Ok(None);
        }
        let start = self.offset;
        self.offset = self.offset.saturating_add(read as u64);
        let payload = self.raw.strip_suffix(b"\n").unwrap_or(&self.raw);
        let payload = payload.strip_suffix(b"\r").unwrap_or(payload);
        // end includes a first-record BOM (span is byte-exact vs. whole-slice
        // parse, where `end = start + line.len()` also includes the BOM).
        let end = start.saturating_add(payload.len() as u64);
        let bytes = if self.number == 1 {
            payload.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(payload)
        } else {
            payload
        };
        let number = self.number;
        self.number += 1;
        Ok(Some(BoundedSourceLine {
            number,
            start,
            end,
            bytes,
        }))
    }
}

/// Visit a line-delimited source without retaining more than one bounded
/// record. `visit` is called synchronously per record; the borrowed `bytes`
/// slice is only valid for the duration of the call.
pub fn for_each_bounded_source_line(
    source: &dyn ReadOnlySource,
    max_record_size: u64,
    mut visit: impl FnMut(BoundedSourceLine<'_>) -> Result<(), ProviderError>,
) -> Result<(), ProviderError> {
    let mut reader = BoundedLineReader::new(source, max_record_size).map_err(provider_io)?;
    while let Some(line) = reader.next_record()? {
        visit(line)?;
    }
    Ok(())
}

/// Read a bounded probe sample from a record-stream source.
///
/// Collects up to `MAX_NON_BLANK_LINES` non-blank lines (and the blank lines
/// between them) without exceeding `max_record_size` total. Adapters probe on
/// this prefix, mirroring their existing sample-window logic.
pub fn read_probe_sample(
    source: &dyn ReadOnlySource,
    max_record_size: u64,
) -> Result<Vec<u8>, ProviderError> {
    const MAX_NON_BLANK_LINES: usize = 64;
    let mut reader = source.open().map_err(provider_io)?;
    let mut raw = Vec::new();
    let mut sample = Vec::new();
    let mut non_blank = 0_usize;
    loop {
        let read = read_one_bounded_line(reader.as_mut(), &mut raw, max_record_size)?;
        if read == 0 {
            break;
        }
        if sample.len().saturating_add(raw.len()) > max_record_size as usize {
            break;
        }
        sample.extend_from_slice(&raw);
        if !raw.iter().all(u8::is_ascii_whitespace) {
            non_blank += 1;
            if non_blank == MAX_NON_BLANK_LINES {
                break;
            }
        }
    }
    Ok(sample)
}

/// Provider adapter 最低合同（RFC-0002 §2）。
///
/// 四阶段中，discover/fingerprint 在首个切片从简（由 SourceDiscovery 端口承担
/// 发现与快照），本 trait 聚焦 probe + parse 这对最能体现格式隔离的职责。
pub trait ProviderAdapter: Send + Sync {
    /// 稳定的 provider 标识（如 `claude-code`）。
    fn provider_id(&self) -> &str;

    /// Stable, structured metadata for this adapter (RFC-0002 §6).
    fn manifest(&self) -> AdapterManifest;

    /// 判定字节流属于哪个 variant 及置信度。
    ///
    /// `ambiguous`/未知 variant 必须返回 [`Confidence::Ambiguous`] 或
    /// [`ProviderError::AmbiguousVariant`]，绝不静默降级为已知语义。
    fn probe(&self, bytes: &[u8]) -> Result<ProbeResult, ProviderError>;

    /// 在一致快照的字节上流式解析，把 Canonical 消息推入 `sink`。
    ///
    /// 只有整体成功才返回 `Ok(ParseReport)`；结构性错误返回
    /// [`ProviderError::StructuralFatal`]，由上层回滚 staging。
    fn parse(
        &self,
        bytes: &[u8],
        sink: &mut dyn CanonicalEventSink,
    ) -> Result<ParseReport, ProviderError>;

    /// Probe a fresh read-only source (RFC-0002 §7 bounded ingest).
    ///
    /// Default: record-stream adapters receive a bounded sample (memory bounded
    /// by `max_record_size`), whole-source adapters receive a `max_source_size`
    ///-capped complete read. The probe judgement itself never changes — it
    /// runs on the same sample window the byte `probe` already uses.
    fn probe_source(&self, source: &dyn ReadOnlySource) -> Result<ProbeResult, ProviderError> {
        let manifest = self.manifest();
        let bytes = match manifest.streaming_support {
            StreamingSupport::RecordStream => read_probe_sample(
                source,
                manifest.max_record_size.unwrap_or(STREAM_RECORD_MAX_BYTES),
            )?,
            StreamingSupport::BoundedWholeSource => {
                let max = manifest.max_source_size.ok_or_else(|| {
                    ProviderError::StructuralFatal(
                        "bounded adapter manifest is missing max_source_size".into(),
                    )
                })?;
                read_bounded_source(source, max)?
            }
        };
        self.probe(&bytes)
    }

    /// Parse a fresh read-only source (RFC-0002 §7 bounded ingest).
    ///
    /// Default: whole-source formats read a `max_source_size`-capped buffer and
    /// delegate to the byte `parse`. Record-stream adapters **must override**
    /// this to iterate `for_each_bounded_source_line`; the default refuses
    /// rather than silently buffering an unbounded transcript.
    fn parse_source(
        &self,
        source: &dyn ReadOnlySource,
        sink: &mut dyn CanonicalEventSink,
    ) -> Result<ParseReport, ProviderError> {
        let manifest = self.manifest();
        let max = manifest.max_source_size.ok_or_else(|| {
            ProviderError::StructuralFatal(
                "record-stream adapter must implement parse_source".into(),
            )
        })?;
        let bytes = read_bounded_source(source, max)?;
        self.parse(&bytes, sink)
    }
}

pub mod capability;
pub mod handoff;
pub mod manifest;

pub use manifest::{
    AdapterManifest, JSON_FAMILY_MAX_SOURCE_BYTES, SQLITE_MAX_SOURCE_BYTES,
    STREAM_RECORD_MAX_BYTES, StreamingSupport, manifest_for,
};

#[cfg(test)]
mod tests {
    use super::*;
    use agent_session_grep_domain::{IdKind, SessionContextGraph, Stability};

    // ---- 工具活动规则集（设计 R1/R2）----

    #[test]
    fn tool_activity_kind_inference_follows_the_ordered_table() {
        use agent_session_grep_domain::ToolActivityKind as K;
        let cases = [
            ("Bash", K::Command),
            ("shell", K::Command),
            ("exec", K::Command),
            ("Read", K::File),
            ("Write", K::File),
            ("Edit", K::File),
            ("MultiEdit", K::File),
            ("NotebookEdit", K::File),
            ("ApplyPatch", K::File),
            ("Glob", K::Query),
            ("Grep", K::Query),
            ("WebFetch", K::Web),
            ("WebSearch", K::Web),
            ("Task", K::Query),
            // 未知名字（含大小写变体）→ Unknown，绝不猜。
            ("BASH", K::Unknown),
            ("read", K::Unknown),
            ("CustomThing", K::Unknown),
            ("", K::Unknown),
        ];
        for (name, expected) in cases {
            assert_eq!(infer_tool_activity_kind(name), expected, "name: {name:?}");
        }
    }

    #[test]
    fn tool_activity_kind_covers_the_names_real_transcripts_record() {
        use agent_session_grep_domain::ToolActivityKind as K;
        // 名字取自真实语料普查（见 `TOOL_NAME_EVIDENCE`）。此前这些名字全部落
        // Unknown，且 Unknown 连 target 也一起丢弃（fail-closed 双保险），于是
        // 真实语料里最高频的工具活动反而信息最少——这正是 "richer extraction"
        // 要补的洞。
        let cases = [
            // Claude Code（Windows 形态的 shell 工具）。
            ("PowerShell", K::Command),
            // Claude Code：subagent 派发工具的现名（`Task` 是旧名，两者同工具）。
            ("Agent", K::Query),
            // Codex `function_call`：命令执行两代工具名。
            ("shell_command", K::Command),
            ("exec_command", K::Command),
            // Codex `custom_tool_call`：补丁工具的真实名（小写下划线）。
            ("apply_patch", K::File),
            // Codex `function_call`：网页检索/抓取。
            ("web_search", K::Web),
            ("web_fetch", K::Web),
            // Codex `function_call`：读图（按路径取文件内容）。
            ("view_image", K::File),
            // Codex `function_call`：子 agent 派发，与 Claude `Agent`/`Task` 同类。
            ("spawn_agent", K::Query),
        ];
        for (name, expected) in cases {
            assert_eq!(infer_tool_activity_kind(name), expected, "name: {name:?}");
        }

        // 反向：用户自定义 / MCP / 编排控制面工具不进闭集（语义由 server 或
        // 会话编排定义，按名字猜 kind 就是编造）。
        for name in [
            "mcp__some-server__web_search",
            "update_plan",
            "get_goal",
            "wait",
            "wait_agent",
            "close_agent",
            "send_message",
            "write_stdin",
            "js",
            "TodoWrite",
            "CronList",
        ] {
            assert_eq!(
                infer_tool_activity_kind(name),
                K::Unknown,
                "name: {name:?} 必须留在 Unknown（不按名字猜语义）"
            );
        }
    }

    #[test]
    fn tool_activity_target_accepts_string_shaped_tool_input() {
        use serde_json::json;
        // 证据：Codex rollout 的 `response_item/custom_tool_call` 把参数放在
        // **字符串** `input` 里（不是 `arguments` 对象），真实语料 1503/1503 条
        // 皆如此。此前只接受对象 → 这些调用 target 恒为 None。
        assert_eq!(
            extract_tool_activity_target(&json!("cargo test --workspace")).as_deref(),
            Some("cargo test --workspace")
        );
        // trim 后为空 → None（不把空白当 target）。
        assert_eq!(extract_tool_activity_target(&json!("   ")), None);
        assert_eq!(extract_tool_activity_target(&json!("")), None);
        // 前后空白 trim，与对象形态同规。
        assert_eq!(
            extract_tool_activity_target(&json!("  git status  ")).as_deref(),
            Some("git status")
        );
    }

    #[test]
    fn tool_activity_target_reads_the_apply_patch_envelope_file_header() {
        use serde_json::json;
        // 证据：Codex `custom_tool_call` name=`apply_patch` 的 `input` 是补丁
        // 封套，真实语料 549/549 条首行恰为 `*** Begin Patch`，其后按
        // `*** Update File: ` / `*** Add File: ` / `*** Delete File: ` 声明路径。
        // 取首个 File 头的路径，比拿整段补丁文本当 target 有用得多。
        let patch =
            "*** Begin Patch\n*** Update File: crates/a/src/lib.rs\n@@\n-old\n+new\n*** End Patch";
        assert_eq!(
            extract_tool_activity_target(&json!(patch)).as_deref(),
            Some("crates/a/src/lib.rs")
        );
        let added = "*** Begin Patch\n*** Add File: docs/new.md\n+hello\n*** End Patch";
        assert_eq!(
            extract_tool_activity_target(&json!(added)).as_deref(),
            Some("docs/new.md")
        );
        let deleted = "*** Begin Patch\n*** Delete File: tmp/gone.txt\n*** End Patch";
        assert_eq!(
            extract_tool_activity_target(&json!(deleted)).as_deref(),
            Some("tmp/gone.txt")
        );
        // 多个 File 头 → 取首个（确定性；活动只承载检索面事实）。
        let multi =
            "*** Begin Patch\n*** Update File: first.rs\n*** Add File: second.rs\n*** End Patch";
        assert_eq!(
            extract_tool_activity_target(&json!(multi)).as_deref(),
            Some("first.rs")
        );
        // 封套但无 File 头 → 回退整段（trim 后的原文），绝不编路径。
        let headerless = "*** Begin Patch\n*** End Patch";
        assert_eq!(
            extract_tool_activity_target(&json!(headerless)).as_deref(),
            Some(headerless)
        );
        // 首行不是 `*** Begin Patch` 的字符串（真实语料里有 3 条 exec 命令内嵌
        // 补丁文本）→ 不当补丁解析，保持整条命令。
        let heredoc = "bash -lc 'apply_patch <<EOF\n*** Update File: a.rs\nEOF'";
        assert_eq!(
            extract_tool_activity_target(&json!(heredoc)).as_deref(),
            Some(heredoc)
        );
        // File 头存在但路径为空白 → 继续找下一个头；都没有则回退整段。
        let blank = "*** Begin Patch\n*** Update File:   \n*** Add File: real.rs\n*** End Patch";
        assert_eq!(
            extract_tool_activity_target(&json!(blank)).as_deref(),
            Some("real.rs")
        );
    }

    #[test]
    fn tool_activity_target_priority_chain_first_present_wins() {
        use serde_json::json;
        // 优先级（借用 Recall events.rs::target_from_value 的键序）：
        // path/file_path/filePath/target > command/cmd > url > query >
        // pattern/glob/glob_pattern/regex > description。
        assert_eq!(
            extract_tool_activity_target(&json!({"command": "ls", "file_path": "a.rs"})).as_deref(),
            Some("a.rs")
        );
        assert_eq!(
            extract_tool_activity_target(&json!({"url": "https://x", "command": "ls"})).as_deref(),
            Some("ls")
        );
        assert_eq!(
            extract_tool_activity_target(&json!({"query": "fn main", "url": "https://x"}))
                .as_deref(),
            Some("https://x")
        );
        assert_eq!(
            extract_tool_activity_target(&json!({"pattern": "*.rs", "query": "q"})).as_deref(),
            Some("q")
        );
        assert_eq!(
            extract_tool_activity_target(&json!({"description": "do it", "pattern": "p"}))
                .as_deref(),
            Some("p")
        );
        // 别名覆盖：path 优先于 file_path；filePath/target/cmd/glob 生效。
        assert_eq!(
            extract_tool_activity_target(&json!({"file_path": "b.rs", "path": "a.rs"})).as_deref(),
            Some("a.rs")
        );
        assert_eq!(
            extract_tool_activity_target(&json!({"filePath": "c.rs"})).as_deref(),
            Some("c.rs")
        );
        assert_eq!(
            extract_tool_activity_target(&json!({"target": "d.rs"})).as_deref(),
            Some("d.rs")
        );
        assert_eq!(
            extract_tool_activity_target(&json!({"cmd": "ls -la"})).as_deref(),
            Some("ls -la")
        );
        assert_eq!(
            extract_tool_activity_target(&json!({"glob": "**/*.rs"})).as_deref(),
            Some("**/*.rs")
        );
        // 空串 / 纯空白视为缺失，继续向下取。
        assert_eq!(
            extract_tool_activity_target(&json!({"file_path": "  ", "command": "ok"})).as_deref(),
            Some("ok")
        );
        // 全部缺失 / 非字符串 / 非对象 → None（fail-closed，不猜）。
        assert_eq!(extract_tool_activity_target(&json!({})), None);
        assert_eq!(
            extract_tool_activity_target(&json!({"file_path": 42})),
            None
        );
        assert_eq!(extract_tool_activity_target(&json!(null)), None);
        // 字符串形态的 input 不再是"非对象 → None"：Codex custom_tool_call 把
        // 整段参数记在字符串 `input` 里，那就是 provider 记录的 target。
        // 见 `tool_activity_target_accepts_string_shaped_tool_input`。
        assert_eq!(
            extract_tool_activity_target(&json!("not an object")).as_deref(),
            Some("not an object")
        );
        // 数组值（Recall 的 bash -c 形态）在本项目 fail-closed：不猜。
        assert_eq!(
            extract_tool_activity_target(&json!({"command": ["bash", "-c", "ls"]})),
            None
        );
        // 返回值已 trim。
        assert_eq!(
            extract_tool_activity_target(&json!({"command": "  git status  "})).as_deref(),
            Some("git status")
        );
    }

    #[test]
    fn build_tool_activity_keeps_the_fail_closed_double_guarantee() {
        use agent_session_grep_domain::{
            ToolActivityActor, ToolActivityKind as K, ToolActivityStatus,
        };
        use serde_json::json;
        // 闭集扩张后这条不变量更重要：未知名 → kind=Unknown **且** target=None，
        // 即使 input 带着形似命令/路径的字段（也包括字符串形态的 input）。
        let unknown_object = build_tool_activity(
            "mcp__some-server__run",
            ToolActivityActor::Main,
            &json!({"command": "rm -rf /"}),
            ToolActivityStatus::Success,
        );
        assert_eq!(unknown_object.kind, K::Unknown);
        assert_eq!(unknown_object.target, None);
        let unknown_string = build_tool_activity(
            "update_plan",
            ToolActivityActor::Main,
            &json!("cargo test"),
            ToolActivityStatus::Unknown,
        );
        assert_eq!(unknown_string.kind, K::Unknown);
        assert_eq!(unknown_string.target, None);
        // 已知名 + 无可用字段 → kind 保留、target=None。
        let known_without_target = build_tool_activity(
            "Read",
            ToolActivityActor::Subagent,
            &json!({"limit": 20}),
            ToolActivityStatus::Success,
        );
        assert_eq!(known_without_target.kind, K::File);
        assert_eq!(known_without_target.target, None);
        assert_eq!(known_without_target.actor, ToolActivityActor::Subagent);
        // 已知名 + 字符串 input（Codex custom_tool_call 形态）→ target 取整条。
        let known_string = build_tool_activity(
            "exec",
            ToolActivityActor::Main,
            &json!("cargo fmt --all"),
            ToolActivityStatus::Success,
        );
        assert_eq!(known_string.kind, K::Command);
        assert_eq!(known_string.target.as_deref(), Some("cargo fmt --all"));
    }

    #[test]
    fn search_facets_default_and_canonical_binding() {
        let default = SearchFacets::default();
        assert!(default.is_default());
        assert_eq!(default.sidechain, SidechainFacet::Include);
        assert_eq!(
            default.canonical_binding(),
            "sidechain=include|tool_kind=|tool_name="
        );

        let faceted = SearchFacets {
            sidechain: SidechainFacet::MainOnly,
            tool_kind: Some("command".into()),
            tool_name: Some("Bash".into()),
        };
        assert!(!faceted.is_default());
        assert_eq!(
            faceted.canonical_binding(),
            "sidechain=main_only|tool_kind=command|tool_name=Bash"
        );
        // 绑定串确定性：同 facets 同串。
        assert_eq!(faceted.canonical_binding(), faceted.canonical_binding());
    }

    #[test]
    fn search_provider_maps_to_canonical_ids() {
        assert_eq!(SearchProvider::Claude.as_str(), "claude-code");
        assert_eq!(SearchProvider::Codex.as_str(), "codex");
    }

    #[test]
    fn search_instant_orders_by_seconds_then_nanoseconds() {
        let base = SearchInstant {
            unix_seconds: 1_000,
            nanosecond: 0,
        };
        let same_second = SearchInstant {
            unix_seconds: 1_000,
            nanosecond: 1,
        };
        let next_second = SearchInstant {
            unix_seconds: 1_001,
            nanosecond: 0,
        };
        assert!(base < same_second);
        assert!(same_second < next_second);
    }

    #[test]
    fn search_instant_sort_key_byte_order_is_instant_order() {
        let negative = SearchInstant {
            unix_seconds: -1,
            nanosecond: 999_999_999,
        };
        let epoch = SearchInstant {
            unix_seconds: 0,
            nanosecond: 0,
        };
        let positive = SearchInstant {
            unix_seconds: 1,
            nanosecond: 0,
        };
        let positive_nanos = SearchInstant {
            unix_seconds: 1,
            nanosecond: 1,
        };
        let mut keys = [
            positive_nanos.sort_key(),
            epoch.sort_key(),
            positive.sort_key(),
            negative.sort_key(),
        ];
        keys.sort();
        assert_eq!(
            keys,
            [
                negative.sort_key(),
                epoch.sort_key(),
                positive.sort_key(),
                positive_nanos.sort_key(),
            ]
        );
    }

    #[test]
    fn search_instant_from_unix_millis_uses_euclid_for_negative_values() {
        let instant = SearchInstant::from_unix_millis(-1);
        assert_eq!(instant.unix_seconds, -1);
        assert_eq!(instant.nanosecond, 999_000_000);
        let epoch = SearchInstant::from_unix_millis(0);
        assert_eq!(epoch.unix_seconds, 0);
        assert_eq!(epoch.nanosecond, 0);
        let positive = SearchInstant::from_unix_millis(1_234);
        assert_eq!(positive.unix_seconds, 1);
        assert_eq!(positive.nanosecond, 234_000_000);
    }

    #[test]
    fn search_filters_is_empty_reflects_all_dimensions() {
        assert!(SearchFilters::EMPTY.is_empty());
        assert!(SearchFilters::default().is_empty());
        let provider_only = SearchFilters {
            providers: vec![SearchProvider::Claude],
            ..SearchFilters::default()
        };
        assert!(!provider_only.is_empty());
        let since_only = SearchFilters {
            since: Some(SearchInstant {
                unix_seconds: 0,
                nanosecond: 0,
            }),
            ..SearchFilters::default()
        };
        assert!(!since_only.is_empty());
        let until_only = SearchFilters {
            until: Some(SearchInstant {
                unix_seconds: 0,
                nanosecond: 0,
            }),
            ..SearchFilters::default()
        };
        assert!(!until_only.is_empty());
        let repo_only = SearchFilters {
            repo: Some("github.com/owner/name".into()),
            ..SearchFilters::default()
        };
        assert!(!repo_only.is_empty());
    }

    #[test]
    fn semantic_index_reference_forwards_dimension_and_errors() {
        struct DimensionIndex;
        impl SemanticIndex for DimensionIndex {
            fn index_embedding(&self, _id: &StableId, _embedding: &[f32]) -> PortResult<()> {
                Ok(())
            }
            fn query_semantic_filtered(
                &self,
                _embedding: &[f32],
                _limit: usize,
                _filters: &SearchFilters,
                _facets: &SearchFacets,
                _include_system: bool,
            ) -> PortResult<Vec<SearchHit>> {
                Ok(Vec::new())
            }
            fn is_ready(&self, dimension: usize) -> PortResult<bool> {
                if dimension == 4 {
                    Err(PortError::WriterBusy(
                        "synthetic readiness contention".into(),
                    ))
                } else {
                    Ok(dimension == 3)
                }
            }
            fn semantic_model_id(&self) -> PortResult<Option<String>> {
                Ok(Some("dimension-model".into()))
            }
        }
        let index = &DimensionIndex;
        assert!(SemanticIndex::is_ready(&index, 3).unwrap());
        assert!(!SemanticIndex::is_ready(&index, 2).unwrap());
        assert!(matches!(
            SemanticIndex::is_ready(&index, 4),
            Err(PortError::WriterBusy(_))
        ));
        assert_eq!(
            SemanticIndex::semantic_model_id(&index).unwrap().as_deref(),
            Some("dimension-model")
        );
        for dimension in [0, 2, 3] {
            assert!(!SemanticIndex::is_ready(&NoSemanticIndex, dimension).unwrap());
        }
    }

    struct FakeContextStore {
        graph: SessionContextGraph,
    }

    impl ContextGraphStore for FakeContextStore {
        fn load_session_graph(&self, session_id: &StableId) -> PortResult<SessionContextGraph> {
            if session_id.as_str() == self.graph.session_id.as_str() {
                Ok(self.graph.clone())
            } else {
                Err(PortError::NotFound("session graph".into()))
            }
        }

        fn message_contexts(
            &self,
            _message_id: &StableId,
        ) -> PortResult<Vec<MessageContextCandidate>> {
            Ok(Vec::new())
        }

        fn session_of(
            &self,
            message_ids: &[StableId],
        ) -> PortResult<Vec<(StableId, Option<StableId>)>> {
            Ok(message_ids.iter().map(|id| (id.clone(), None)).collect())
        }

        fn source_placements_of(
            &self,
            message_ids: &[StableId],
        ) -> PortResult<Vec<(StableId, Option<SourcePlacement>)>> {
            Ok(message_ids.iter().map(|id| (id.clone(), None)).collect())
        }

        fn context_stats(&self) -> PortResult<ContextStats> {
            Ok(ContextStats {
                placements: 2,
                source_placement_claims: 3,
            })
        }
    }

    #[test]
    fn context_graph_store_reference_blanket_impl_forwards() {
        let session_id = StableId::derive(IdKind::Session, Stability::Reconstructed, &[b"session"]);
        let store = FakeContextStore {
            graph: SessionContextGraph {
                session_id: session_id.clone(),
                messages: Vec::new(),
                source_documents: Vec::new(),
                placements: Vec::new(),
                edges: Vec::new(),
            },
        };
        let store_ref = &store;

        assert_eq!(
            ContextGraphStore::load_session_graph(&store_ref, &session_id)
                .unwrap()
                .session_id,
            session_id
        );
        assert_eq!(
            ContextGraphStore::context_stats(&store_ref).unwrap(),
            ContextStats {
                placements: 2,
                source_placement_claims: 3,
            }
        );
    }

    /// 只响应一次 `read_verified` 的假发现端口（后续读取报 Backend，用于
    /// 断言调用方不会重复读取）。
    struct ReadOnceDiscovery {
        reads: std::cell::Cell<usize>,
        result: std::cell::RefCell<Option<Result<Vec<u8>, PortError>>>,
    }

    impl ReadOnceDiscovery {
        fn new(result: Result<Vec<u8>, PortError>) -> Self {
            Self {
                reads: std::cell::Cell::new(0),
                result: std::cell::RefCell::new(Some(result)),
            }
        }
    }

    impl SourceDiscovery for ReadOnceDiscovery {
        fn discover(&self) -> PortResult<Vec<SourceSnapshot>> {
            Ok(Vec::new())
        }

        fn read_verified(&self, _snapshot: &SourceSnapshot) -> PortResult<Vec<u8>> {
            self.reads.set(self.reads.get() + 1);
            self.result
                .borrow_mut()
                .take()
                .unwrap_or_else(|| Err(PortError::Backend("double read".into())))
        }
    }

    fn snapshot() -> SourceSnapshot {
        SourceSnapshot {
            path: "C:\\Users\\secret\\transcript.jsonl".into(),
            len: 42,
            mtime_ms: 1,
            fingerprint: "fp".into(),
        }
    }

    #[test]
    fn ensure_readable_returns_verified_bytes_with_single_read() {
        let discovery = ReadOnceDiscovery::new(Ok(b"verified bytes".to_vec()));
        let bytes = ensure_readable(&discovery, &snapshot()).unwrap();
        assert_eq!(bytes, b"verified bytes");
        assert_eq!(
            discovery.reads.get(),
            1,
            "校验用的字节必须直接复用，不得二次读取"
        );
    }

    #[test]
    fn ensure_readable_maps_snapshot_changed_without_backend_detail() {
        let discovery = ReadOnceDiscovery::new(Err(PortError::SnapshotChanged(
            "C:\\Users\\secret\\transcript.jsonl changed".into(),
        )));
        let err = ensure_readable(&discovery, &snapshot()).unwrap_err();
        assert_eq!(err.code(), "invalid_request");
        assert!(
            !err.to_string().contains("secret"),
            "错误消息不得泄漏后端路径: {err}"
        );
    }

    #[test]
    fn ensure_readable_preserves_not_found_classification() {
        let discovery = ReadOnceDiscovery::new(Err(PortError::NotFound(
            "C:\\Users\\secret\\missing.jsonl".into(),
        )));
        let err = ensure_readable(&discovery, &snapshot()).unwrap_err();
        assert_eq!(err.code(), "not_found");
        assert!(
            !err.to_string().contains("secret"),
            "错误消息不得泄漏后端路径: {err}"
        );
    }

    #[test]
    fn ensure_readable_maps_other_backend_errors_without_payload() {
        let discovery = ReadOnceDiscovery::new(Err(PortError::Backend(
            "sqlite error at C:\\Users\\secret\\catalog.db".into(),
        )));
        let err = ensure_readable(&discovery, &snapshot()).unwrap_err();
        assert_eq!(err.code(), "invariant_violation");
        let message = err.to_string();
        assert!(
            !message.contains("secret") && !message.contains("sqlite"),
            "错误消息只保留分类，不带后端载荷: {message}"
        );
    }

    // ---- bounded source streaming（RFC-0002 §7）----

    fn collect_lines(
        source: &dyn ReadOnlySource,
        max_record_size: u64,
    ) -> Result<Vec<(usize, u64, u64, String)>, ProviderError> {
        let mut out = Vec::new();
        for_each_bounded_source_line(source, max_record_size, |line| {
            out.push((
                line.number,
                line.start,
                line.end,
                String::from_utf8_lossy(line.bytes).into_owned(),
            ));
            Ok(())
        })?;
        Ok(out)
    }

    #[test]
    fn bounded_line_iteration_preserves_byte_offsets_and_crlf_bom() {
        // BOM + CRLF + blank line + truncated tail (no trailing newline).
        // Byte layout: BOM(0-2) {"a":1}(3-9) \r(10) \n(11) \r(12) \n(13) {"b":2}(14-20)
        let bytes = b"\xEF\xBB\xBF{\"a\":1}\r\n\r\n{\"b\":2}";
        let source = SliceSource::new(bytes);
        let lines = collect_lines(&source, 1024).unwrap();
        assert_eq!(
            lines,
            vec![
                (1, 0, 10, "{\"a\":1}".to_string()), // BOM stripped, end covers BOM+\r\n-exclusive
                (2, 12, 12, String::new()),          // blank CRLF line at [12,12)
                (3, 14, 21, "{\"b\":2}".to_string()), // truncated tail
            ]
        );
    }

    #[test]
    fn bounded_line_iteration_matches_legacy_split_semantics() {
        // Byte-identical to the legacy `split_inclusive('\n')` header that
        // JSONL adapters used: end excludes \n/\r but includes a first-line BOM.
        let bytes = b"abc\r\ndef\nghi";
        let source = SliceSource::new(bytes);
        let mut offset = 0_u64;
        let mut expected = Vec::new();
        for (idx, raw) in bytes.split(|b| *b == b'\n').enumerate() {
            let start = offset;
            offset += raw.len() as u64 + 1;
            let line = raw.strip_suffix(b"\r").unwrap_or(raw);
            let parse_line = if idx == 0 {
                line.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(line)
            } else {
                line
            };
            expected.push((
                idx + 1,
                start,
                start + line.len() as u64,
                String::from_utf8_lossy(parse_line).into_owned(),
            ));
        }
        let lines = collect_lines(&source, 1024).unwrap();
        assert_eq!(lines, expected);
    }

    #[test]
    fn bounded_line_iteration_rejects_oversized_record() {
        let source = SliceSource::new(b"{\"small\":true}\n{\"this line is too long\":1}");
        let err = collect_lines(&source, 16).unwrap_err();
        assert!(
            matches!(err, ProviderError::RecordTooLarge { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn read_bounded_source_enforces_cap_before_and_during_read() {
        let source = SliceSource::new(b"0123456789");
        let err = read_bounded_source(&source, 5).unwrap_err();
        assert!(matches!(err, ProviderError::SourceTooLarge { max: 5, .. }));

        let ok = read_bounded_source(&source, 10).unwrap();
        assert_eq!(ok, b"0123456789");
    }

    /// RFC-0002 原文：provider adapter 契约的权威规范。
    const RFC_0002: &str =
        include_str!("../../../docs/architecture/RFC-0002-provider-adapter-contract.md");

    /// RFC-0002 §5 错误矩阵里的标签 → 对应的 `ProviderError` 变体名。
    ///
    /// 契约文档用 snake_case 标签描述错误类，代码用 Pascal 变体实现。两侧此前
    /// 没有任何联系：重命名一个变体、或在文档里写一个不存在的标签，都不会有测试
    /// 失败，而 `PROVIDER-ADAPTER-CONTRIBUTOR-GUIDE.md` 让贡献者按这些标签实现
    /// adapter——标签失效等于把规范变成传说。
    ///
    /// 只登记"确实是错误类型"的标签：`record_recoverable` 与 `incomplete_tail`
    /// 是处理策略（分别落在 `ParseReport.skipped` 与"不提交半截"的调用方行为上），
    /// 不是 `ProviderError` 变体，故不在此表。
    const RFC_0002_ERROR_LABELS: &[(&str, &str)] = &[
        ("source_changed_during_read", "SourceChangedDuringRead"),
        ("structural_fatal", "StructuralFatal"),
        ("ambiguous_variant", "AmbiguousVariant"),
    ];

    #[test]
    fn rfc_0002_error_vocabulary_maps_to_real_provider_error_variants() {
        let source = include_str!("lib.rs");
        // 只在生产区找变体定义，避免测试里的字符串自证。
        let production = source
            .split_once("#[cfg(test)]")
            .map(|(before, _)| before)
            .unwrap_or(source);

        for (label, variant) in RFC_0002_ERROR_LABELS {
            assert!(
                RFC_0002.contains(&format!("`{label}`")),
                "RFC-0002 不再提及错误标签 `{label}`——本表必须同步删除该行，\
                 否则守护会声称文档有一条它其实没有的规范"
            );
            assert!(
                production.contains(&format!("{variant}(")),
                "RFC-0002 §5 的错误标签 `{label}` 对应的 `ProviderError::{variant}` \
                 在生产代码里找不到——契约词汇与实现已脱节，必须同时修文档与代码"
            );
        }

        // §6 的字段能力词汇必须与 `CapabilityLevel` 的对外字符串一致：文档写
        // `native | derived | partial | unsupported | unknown`，代码经 serde
        // (`rename_all = "snake_case"`) 把同一组值输出给 CLI/MCP/Web。任一侧
        // 改名而不同步，对外文档即失真。取值走 serde 而不是手抄常量。
        for level in [
            crate::capability::CapabilityLevel::Native,
            crate::capability::CapabilityLevel::Derived,
            crate::capability::CapabilityLevel::Partial,
            crate::capability::CapabilityLevel::Unsupported,
            crate::capability::CapabilityLevel::Unknown,
        ] {
            let wire = serde_json::to_string(&level).expect("CapabilityLevel 必须可序列化");
            let name = wire.trim_matches('"');
            assert!(
                RFC_0002.contains(&format!("`{name}")) || RFC_0002.contains(&format!("{name} |")),
                "RFC-0002 §6 的字段能力词汇缺少 `{name}`——\
                 CapabilityLevel 有该档位，契约文档不得漏档"
            );
        }
    }
}

//! Robot 协议层：统一 JSON envelope、错误目录（Error Catalog）与 Outcome。
//!
//! 遵循 `docs/contracts/CONTRACT-cli-robot-mcp-draft.md`、
//! `schemas/robot/v1.1/envelope.schema.json` 与 `schemas/robot/v1/error-catalog.json`：所有入口
//! 先把结果/错误归一到同一组版本化 DTO，再按同一映射投影到 exit code / JSON，
//! 不允许各命令各自决定语义。本模块目前是唯一消费者（CLI）；MCP 落地时再抽 crate。
//!
//! 约束：
//! - stdout 只输出协议数据；进程级诊断只走 stderr。
//! - 错误 envelope 携带稳定 `code` + 安全 `message` + `retryable` + 有界 `details`。
//! - `schema_version` 走 major.minor；未知 major 由调用方拒绝。

use agent_session_grep_application::AppError;
use agent_session_grep_application::cursor::CursorError;
use agent_session_grep_domain::DomainError;
use agent_session_grep_ports::{PortError, ProviderError, RedactionStatus, RetrievalMode};
use serde_json::{Value, json};
use std::io::{ErrorKind, Write};

/// 当前协议 schema 版本（major.minor）。未知 major 必须拒绝，兼容 minor 按合同处理。
pub const SCHEMA_VERSION: &str = "1.1";

/// 业务结果层级。与进程错误分离：partial 绝不伪装成 success。
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Partial,
}

/// 输出模式。模式分支（human 渲染 vs envelope/帧）由 main.rs 持有；本层只提供帧构造。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// 人类可读（默认）。
    Human,
    /// 单个 JSON envelope。
    Json,
    /// 每行一个完整协议 frame。
    Jsonl,
}

/// `schemas/robot/v1/error-catalog.json` 中错误目录的实现子集。
///
/// 每个 canonical code 固定映射到 exit code / retryable / redaction，
/// 新增错误必须先在此登记，任何入口才能返回。
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalCode {
    /// 参数或请求校验失败 → exit 2。
    InvalidRequest,
    /// 请求对象不存在 → exit 4。
    NotFound,
    /// Source/File I/O 错误 → exit 5。
    SourceIo,
    /// 源在读取期间被改写（`source_changed`）→ exit 5。
    SourceChanged,
    /// 快照校验阶段无法完成（与已确认 source_changed 区分）。
    #[allow(dead_code)]
    SnapshotFailed,
    /// Catalog 或 Search Index 错误 → exit 6。
    CatalogError,
    /// Provider 格式或 Adapter 错误 → exit 7。
    ProviderError,
    /// 请求的能力当前不可用（provider 未声明，或 `--offline` 拒绝联网）→ exit 7。
    /// 不静默降级——调用方必须收到明确信号（unified-release-contract D2）。
    CapabilityNotSupported,
    /// writer lease 未取得（`writer_busy`）→ exit 6，可重试。
    WriterBusy,
    /// JSON/协议版本不兼容 → exit 9。
    SchemaIncompatible,
    /// Cursor 令牌无法解析/校验失败（`cursor_invalid`）→ exit 2。
    CursorInvalid,
    /// Cursor 超出 TTL（`cursor_expired`）→ exit 2。
    CursorExpired,
    /// Cursor 携带的 generation 与活动 generation 不一致 → exit 9。
    GenerationMismatch,
    /// 未分类内部错误（不变量违反等 bug 信号）→ exit 70。
    Internal,
}

impl CanonicalCode {
    /// 稳定 wire 字符串。对外契约的一部分，不可随意更名。
    pub fn as_str(self) -> &'static str {
        match self {
            CanonicalCode::InvalidRequest => "invalid_request",
            CanonicalCode::NotFound => "not_found",
            CanonicalCode::SourceIo => "source_io",
            CanonicalCode::SourceChanged => "source_changed",
            CanonicalCode::SnapshotFailed => "snapshot_failed",
            CanonicalCode::CatalogError => "catalog_error",
            CanonicalCode::ProviderError => "provider_error",
            CanonicalCode::CapabilityNotSupported => "capability_not_supported",
            CanonicalCode::WriterBusy => "writer_busy",
            CanonicalCode::SchemaIncompatible => "schema_incompatible",
            CanonicalCode::CursorInvalid => "cursor_invalid",
            CanonicalCode::CursorExpired => "cursor_expired",
            CanonicalCode::GenerationMismatch => "generation_mismatch",
            CanonicalCode::Internal => "internal",
        }
    }

    /// CLI exit code，必须与 Robot v1 error catalog 保持一致。
    pub fn exit_code(self) -> i32 {
        match self {
            CanonicalCode::InvalidRequest
            | CanonicalCode::CursorInvalid
            | CanonicalCode::CursorExpired => 2,
            CanonicalCode::NotFound => 4,
            CanonicalCode::SourceIo
            | CanonicalCode::SourceChanged
            | CanonicalCode::SnapshotFailed => 5,
            CanonicalCode::CatalogError | CanonicalCode::WriterBusy => 6,
            CanonicalCode::ProviderError | CanonicalCode::CapabilityNotSupported => 7,
            CanonicalCode::SchemaIncompatible | CanonicalCode::GenerationMismatch => 9,
            CanonicalCode::Internal => 70,
        }
    }

    /// 是否值得重试（writer_busy 等瞬态错误为 true）。
    pub fn retryable(self) -> bool {
        matches!(
            self,
            CanonicalCode::WriterBusy | CanonicalCode::SourceChanged
        )
    }

    /// 人类可读的"下一步怎么办"指引。与 `schemas/robot/v1/error-catalog.json` 的
    /// `operator_action` 对齐（逐 code 独立映射，禁止合并分组——错误语义互不相同），
    /// 但写成对不读源码的新手也能执行的步骤；human 模式渲染错误时追加到报错行之后
    /// （robot/json 模式保持稳定 code，指引留给调用方）。
    pub fn operator_action(self) -> &'static str {
        match self {
            CanonicalCode::InvalidRequest => "检查命令与参数写法，运行 --help 查看完整用法",
            CanonicalCode::NotFound => "确认实体 ID 是否正确（运行 list 可浏览可用实体）",
            CanonicalCode::SourceIo => "确认源文件路径存在且可读",
            CanonicalCode::SourceChanged => {
                "源文件正在被写入（例如 Claude Code 正在记录当前会话），稍等后重试"
            }
            CanonicalCode::SnapshotFailed => "快照校验失败：检查源文件元数据与文件系统健康状态",
            CanonicalCode::CatalogError => {
                "先运行 doctor --db <path> 自检。若 doctor 正常，说明这不是打开失败而是\
                 一次被拒绝的写入（例如同一条消息在不同源上投影冲突）：用 \
                 ASG_DEBUG_ERRORS=1 重跑同一命令查看被掩码的原因，并按 \
                 docs/operations/rebuild-and-migration-runbook.md 处理"
            }
            CanonicalCode::ProviderError => "该文件不是可识别的 transcript 格式，或文件已被破坏",
            CanonicalCode::CapabilityNotSupported => {
                "该能力当前不可用：--offline 下拒绝需要联网的操作，或该 provider 未声明此能力"
            }
            CanonicalCode::WriterBusy => {
                "另一个进程正在写入数据库，等待其结束（或结束残留的 agent-session-grep 进程）后重试"
            }
            CanonicalCode::SchemaIncompatible => {
                "数据库版本与当前程序不兼容：升级程序，或对旧库重新执行完整同步"
            }
            CanonicalCode::CursorInvalid => "游标无效：丢弃该游标，从第一页重新执行查询",
            CanonicalCode::CursorExpired => "游标已过期：重新执行查询以获得新的游标",
            CanonicalCode::GenerationMismatch => "索引已推进：请从第一页重新执行查询",
            CanonicalCode::Internal => "内部错误：请记录完整输出并反馈",
        }
    }
}

/// 归一后的协议错误：稳定 code + 安全 message + 有界结构化 details。
#[derive(Debug, Clone)]
pub struct ProtocolError {
    pub code: CanonicalCode,
    pub message: String,
    /// envelope `error.details`：默认 `{}`；只允许目录允诺的有界字段（schema 上限 32 属性）。
    pub details: Value,
}

impl ProtocolError {
    pub fn new(code: CanonicalCode, message: impl Into<String>) -> Self {
        ProtocolError {
            code,
            message: message.into(),
            details: json!({}),
        }
    }

    /// 附加结构化 details；构造方保证对象有界（≤32 属性）且不含敏感内容。
    pub fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
}

impl From<DomainError> for ProtocolError {
    fn from(e: DomainError) -> Self {
        let code = match &e {
            DomainError::NotFound(_) => CanonicalCode::NotFound,
            DomainError::InvalidRequest(_) => CanonicalCode::InvalidRequest,
            DomainError::InvariantViolation(_) => CanonicalCode::Internal,
            DomainError::UnstableIdentity(_) => CanonicalCode::InvalidRequest,
            DomainError::AmbiguousGraph(_) => CanonicalCode::Internal,
        };
        ProtocolError::new(code, e.to_string())
    }
}

impl From<AppError> for ProtocolError {
    fn from(error: AppError) -> Self {
        match error {
            AppError::Domain(error) => error.into(),
            AppError::Port(error) => error.into(),
            AppError::Provider(error) => error.into(),
            // cursor 错误族有专属 canonical code；contract major 不符归 schema_incompatible。
            // mismatch 双方数值投影成有界 details，供 Robot 端无需解析 message 即可自恢复。
            AppError::Cursor(error) => {
                let (code, details) = match &error {
                    CursorError::Invalid(_) => (CanonicalCode::CursorInvalid, json!({})),
                    CursorError::Expired(_) => (CanonicalCode::CursorExpired, json!({})),
                    CursorError::GenerationMismatch { cursor, active } => (
                        CanonicalCode::GenerationMismatch,
                        json!({ "cursor_generation": cursor, "active_generation": active }),
                    ),
                    CursorError::ContractMismatch { cursor, supported } => (
                        CanonicalCode::SchemaIncompatible,
                        json!({ "cursor_contract_major": cursor, "supported": supported }),
                    ),
                };
                ProtocolError::new(code, error.to_string()).with_details(details)
            }
            AppError::Budget(error) => {
                ProtocolError::new(CanonicalCode::InvalidRequest, error.to_string())
            }
            AppError::MessageAmbiguous(ambiguity) => {
                ProtocolError::new(CanonicalCode::InvalidRequest, ambiguity.to_string())
                    .with_details(json!({
                        "candidate_count": ambiguity.candidate_count,
                        "candidate_session_ids": ambiguity.candidate_session_ids,
                        "hint": ambiguity.hint
                    }))
            }
        }
    }
}

impl From<ProviderError> for ProtocolError {
    fn from(error: ProviderError) -> Self {
        // R4.3 同款纪律：`Io` 变体携带 OS 层原始错误文本（Windows 上可能包含
        // 真实绝对 transcript 路径），绝不进入用户可见 message。其余变体是
        // adapter 的静态文案或有界数值，保持原样。原始细节仅在
        // ASG_DEBUG_ERRORS=1 时写 stderr（永不进 stdout envelope）。
        let message = match &error {
            ProviderError::Io(_) => "提供方源文件读取失败".to_string(),
            _ => error.to_string(),
        };
        if std::env::var_os("ASG_DEBUG_ERRORS").is_some() {
            eprintln!("debug [{}]: {error}", CanonicalCode::ProviderError.as_str());
        }
        ProtocolError::new(CanonicalCode::ProviderError, message)
    }
}

/// One canonical classification for both ordinary and strictly private ports.
fn port_error_code(error: &PortError) -> CanonicalCode {
    match error {
        PortError::Backend(_) => CanonicalCode::CatalogError,
        PortError::SourceIo(_) => CanonicalCode::SourceIo,
        PortError::SchemaIncompatible(_) => CanonicalCode::SchemaIncompatible,
        PortError::NotFound(_) => CanonicalCode::NotFound,
        PortError::SnapshotChanged(_) => CanonicalCode::SourceChanged,
        PortError::WriterBusy(_) => CanonicalCode::WriterBusy,
        PortError::InvalidRequest(_) => CanonicalCode::InvalidRequest,
        PortError::GenerationMismatch(_) => CanonicalCode::GenerationMismatch,
    }
}

impl ProtocolError {
    /// Relocation inputs include private roots, backup destinations and plan
    /// tokens. Their port errors must stay opaque even under ASG_DEBUG_ERRORS.
    pub fn from_private_port_error(error: PortError) -> Self {
        let code = port_error_code(&error);
        let message = match code {
            CanonicalCode::CatalogError => "catalog operation failed",
            CanonicalCode::SourceIo => "source or backup could not be accessed",
            CanonicalCode::SchemaIncompatible => {
                "catalog schema is incompatible; run index rebuild explicitly before relocation"
            }
            CanonicalCode::NotFound => "registered installation was not found",
            CanonicalCode::SourceChanged => "source changed; create a fresh relocation preview",
            CanonicalCode::WriterBusy => "another writer holds the catalog lease",
            CanonicalCode::InvalidRequest => {
                "relocation request is invalid; check the mapping and obtain a fresh preview"
            }
            CanonicalCode::GenerationMismatch => {
                "catalog generation changed; create a fresh relocation preview"
            }
            _ => unreachable!("port_error_code only returns port error categories"),
        };
        Self::new(code, message)
    }
}

impl From<PortError> for ProtocolError {
    fn from(e: PortError) -> Self {
        let code = port_error_code(&e);
        // R4.3：Backend、SourceIo 携带后端/文件系统原始细节，绝不进入用户可见
        // message（其中 SourceIo 可能包含绝对 transcript 路径）。其余变体已验证
        // 不携带路径/ID（静态文案或数值），保持原样。
        let message = match &e {
            PortError::Backend(_) => "数据库内部错误".to_string(),
            PortError::SourceIo(_) => "源文件无法读取".to_string(),
            _ => e.to_string(),
        };
        // 掩码后原始细节即丢失，`catalog_error` 在现场无从诊断。ASG_DEBUG_ERRORS=1
        // 时把细节写 stderr（永不进 stdout envelope，不影响协议契约）。
        if std::env::var_os("ASG_DEBUG_ERRORS").is_some() {
            eprintln!("debug [{}]: {e}", code.as_str());
        }
        ProtocolError::new(code, message)
    }
}

/// 从参数中解析输出模式：`--robot` 等价稳定 JSON；`--output human|json|jsonl`。
///
/// `--robot` 优先级最高（等价 `--output json` + 无色 + 无进度）。缺省为 Human。
/// 未知或缺少 `--output` 值属于请求错误，不能静默降级为另一种协议；
/// `--output` 与 `--robot` 互相冲突、同类 flag 重复出现同样是请求错误，
/// 不允许静默 first-wins（R8.2）。
///
/// flag 只在前缀位置识别：扫描在第一个裸参数（命令名）处停止，命令名之后的
/// token 一律不当 flag——否则 `search --robot` 这类"查询文本恰等于 flag 名"
/// 的合法检索会被误判输出模式。
pub fn parse_output_mode(args: &[String]) -> Result<OutputMode, String> {
    let mut it = args.iter();
    // 已选定的模式；再次遇到 --robot/--output（重复或冲突）即报错。
    let mut chosen: Option<OutputMode> = None;
    while let Some(a) = it.next() {
        if !a.starts_with('-') {
            break; // 第一个位置参数（命令名）之后的 token 不当 flag 解析
        }
        match a.as_str() {
            "--robot" => {
                if chosen.is_some() {
                    return Err(
                        "conflicting output flags: --robot cannot be combined with --output".into(),
                    );
                }
                chosen = Some(OutputMode::Json);
            }
            "--output" => {
                let mode = match it.next().map(|s| s.as_str()) {
                    Some("human") => OutputMode::Human,
                    Some("json") => OutputMode::Json,
                    Some("jsonl") => OutputMode::Jsonl,
                    Some(value) => return Err(format!("unsupported output mode: {value}")),
                    None => return Err("--output requires human|json|jsonl".into()),
                };
                if chosen.is_some() {
                    return Err(
                        "duplicate output flags: --output cannot be repeated or combined with --robot"
                            .into(),
                    );
                }
                chosen = Some(mode);
            }
            // 其它带值 flag 及其取值不在本层消费，跳过取值避免误判。
            // 列表必须与 main.rs 各前缀扫描器（extract_request_id/command_name/
            // intercept_help_or_version/extract_db_flag_impl）保持一致，漏掉一个
            // 会让它的取值把后面的 --robot/--output 挡在扫描之外。
            "--db" | "--request-id" | "--cursor" | "--max-items" | "--max-bytes"
            | "--max-messages" | "--max-evidence" | "--max-tokens" | "--policy" | "--level"
            | "--provider" | "--since" | "--until" | "--session" | "--around" | "--tool-kind"
            | "--tool-name" | "--from" | "--to" | "--alias-ttl-days" | "--plan" | "--backup" => {
                it.next();
            }
            _ => {}
        }
    }
    Ok(chosen.unwrap_or(OutputMode::Human))
}

/// request_id 语义：调用方提供（`--request-id`）则逐字回显；缺省生成 `cli-<pid>-<millis>`。
fn resolve_request_id(request_id: Option<&str>) -> String {
    request_id.map_or_else(generated_request_id, str::to_string)
}

fn generated_request_id() -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("cli-{}-{millis}", std::process::id())
}

/// envelope 约束 `^[A-Za-z0-9._:-]+$` 且 1..=128 字符。
/// 允许集为纯 ASCII，任何多字节字符都过不了逐字节校验，故字节长度即字符长度。
pub fn valid_request_id(s: &str) -> bool {
    (1..=128).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

/// 分页元数据（envelope `page` 字段）：续读令牌 + 是否还有后续页。
#[derive(Debug, Clone, Default)]
pub struct Page {
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// 脱敏状态块（ADR-0009）：Robot envelope 与 MCP 工具结果共用同一形状，
/// 两个跨边界出口不得各写一份（否则一侧漏报脱敏，调用方无法区分
/// "服务端涂红"与"原文就是 `[redacted:...]`"）。
pub fn redaction_block(redaction: &RedactionStatus) -> Value {
    json!({
        "mode": redaction.mode.as_str(),
        "status": redaction.status.as_str(),
        "ruleset_version": redaction.ruleset_version,
        "redacted_count": redaction.redacted_count,
        "audit_id": redaction.audit_id,
    })
}

/// 成功 envelope。`data` 是已验证的 JSON Value，不接受未校验字符串片段。
/// `warnings` 原样序列化进 envelope 数组；模式分支（human vs envelope）由 main.rs 决定。
///
/// `retrieval_mode` 标识本次检索使用的匹配策略（lexical/semantic/hybrid/lexical_fallback），
/// 当前固定为 [`RetrievalMode::Lexical`]（语义检索尚未实现）。
/// `redaction` 投影脱敏状态，当前固定为 `mode=default, status=none`（脱敏实现尚未落地）。
#[allow(clippy::too_many_arguments)]
pub fn success_envelope(
    command: &str,
    outcome: Outcome,
    data: Value,
    duration_ms: u64,
    page: &Page,
    warnings: &[String],
    request_id: Option<&str>,
    retrieval_mode: RetrievalMode,
    redaction: &RedactionStatus,
) -> String {
    let outcome_str = match outcome {
        Outcome::Success => "success",
        Outcome::Partial => "partial",
    };
    let generation = data.get("generation").cloned().unwrap_or(Value::Null);
    json!({
        "schema_version": SCHEMA_VERSION,
        "frame_type": "response",
        "command": command,
        "request_id": resolve_request_id(request_id),
        "ok": true,
        "outcome": outcome_str,
        "data": data,
        "retrieval_mode": retrieval_mode.as_str(),
        "redaction": redaction_block(redaction),
        "warnings": warnings,
        "page": {
            "next_cursor": page.next_cursor.as_deref().map_or(Value::Null, |c| json!(c)),
            "has_more": page.has_more,
        },
        "meta": {
            "duration_ms": duration_ms,
            "generation": generation,
        },
    })
    .to_string()
}

/// 错误 envelope。stdout 只有这一个对象；细节安全、有界（`err.details` 由构造方约束）。
pub fn error_envelope(command: &str, err: &ProtocolError, request_id: Option<&str>) -> String {
    // 与成功 envelope 同一脱敏纪律（ADR-0009）：错误路径不得绕过跨边界脱敏。
    // message/details 可能回显调用方输入（密钥形状的 flag 值、用户参数），
    // 出帧前统一过共享脱敏引擎；路径类细节已在各 From 转换处掩码
    // （PortError::SourceIo / ProviderError::Io，R4.3）。
    let (message, _) = crate::redaction::redact_text(&err.message);
    let (details, _) = crate::redaction::redact_value(err.details.clone());
    json!({
        "schema_version": SCHEMA_VERSION,
        "frame_type": "error",
        "command": command,
        "request_id": resolve_request_id(request_id),
        "ok": false,
        "outcome": "failure",
        "error": {
            "code": err.code.as_str(),
            "message": message,
            "retryable": err.code.retryable(),
            "details": details,
        },
        "warnings": [],
        "page": {
            "next_cursor": Value::Null,
            "has_more": false,
        },
        "meta": {
            "duration_ms": 0,
            "generation": Value::Null,
        },
    })
    .to_string()
}

/// progress frame：`{schema_version, frame_type:"progress", command, request_id, message}`。
/// 只允许在 `--output jsonl` 下发射（`--robot`/Json/Human 禁止）；该约束由调用方执行。
pub fn progress_frame(command: &str, message: &str, request_id: Option<&str>) -> String {
    stream_frame("progress", command, message, request_id)
}

/// diagnostic frame：为契约完备性定义（design §0.4），v1 没有任何发射点。
#[allow(dead_code)]
pub fn diagnostic_frame(command: &str, message: &str, request_id: Option<&str>) -> String {
    stream_frame("diagnostic", command, message, request_id)
}

fn stream_frame(
    frame_type: &str,
    command: &str,
    message: &str,
    request_id: Option<&str>,
) -> String {
    let (message, _) = crate::redaction::redact_text(message);
    json!({
        "schema_version": SCHEMA_VERSION,
        "frame_type": frame_type,
        "command": command,
        "request_id": resolve_request_id(request_id),
        "message": message,
    })
    .to_string()
}

/// 协议 stdout 的唯一出口（println 替身）：整行写入 + 换行 + flush。
///
/// CONTRACT §6：stdout 必须协议干净、退出码受控。下游提前关管道（head/pager）
/// 触发 EPIPE 属正常消费行为 → 静默 exit 0，不得 panic（exit 101）或污染 stderr；
/// 其余写失败归 source_io 类 → stderr 一行诊断 + exit 5。
/// 逐行 flush 是必须的：块缓冲下 EPIPE 只在冲刷时暴露，且 jsonl 进度帧要求实时可见。
pub fn write_stdout_line(line: &str) {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let result = handle
        .write_all(line.as_bytes())
        .and_then(|()| handle.write_all(b"\n"))
        .and_then(|()| handle.flush());
    if let Err(error) = result {
        if error.kind() == ErrorKind::BrokenPipe {
            std::process::exit(0);
        }
        eprintln!("error [source_io]: cannot write protocol output: {error}");
        std::process::exit(5);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_follow_contract() {
        assert_eq!(CanonicalCode::InvalidRequest.exit_code(), 2);
        assert_eq!(CanonicalCode::NotFound.exit_code(), 4);
        assert_eq!(CanonicalCode::SourceIo.exit_code(), 5);
        assert_eq!(CanonicalCode::SourceChanged.exit_code(), 5);
        assert_eq!(CanonicalCode::CatalogError.exit_code(), 6);
        assert_eq!(CanonicalCode::WriterBusy.exit_code(), 6);
        assert_eq!(CanonicalCode::ProviderError.exit_code(), 7);
        assert_eq!(CanonicalCode::CapabilityNotSupported.exit_code(), 7);
        assert_eq!(CanonicalCode::SchemaIncompatible.exit_code(), 9);
        assert_eq!(CanonicalCode::Internal.exit_code(), 70);
    }

    #[test]
    fn operator_action_exists_for_every_code() {
        for code in [
            CanonicalCode::InvalidRequest,
            CanonicalCode::NotFound,
            CanonicalCode::SourceIo,
            CanonicalCode::SourceChanged,
            CanonicalCode::SnapshotFailed,
            CanonicalCode::CatalogError,
            CanonicalCode::ProviderError,
            CanonicalCode::CapabilityNotSupported,
            CanonicalCode::WriterBusy,
            CanonicalCode::SchemaIncompatible,
            CanonicalCode::CursorInvalid,
            CanonicalCode::CursorExpired,
            CanonicalCode::GenerationMismatch,
            CanonicalCode::Internal,
        ] {
            assert!(!code.operator_action().is_empty(), "{}", code.as_str());
        }
    }

    #[test]
    fn cursor_snapshot_generation_actions_match_catalog_semantics() {
        // catalog cursor_invalid: "Discard the cursor and rerun the query from the start."
        let invalid = CanonicalCode::CursorInvalid.operator_action();
        assert!(
            invalid.contains("丢弃") && invalid.contains("重新"),
            "{invalid}"
        );
        assert!(
            !invalid.contains("--help"),
            "cursor_invalid 不得把用户引向 --help: {invalid}"
        );

        // catalog cursor_expired: "Rerun the query to obtain a fresh cursor."
        let expired = CanonicalCode::CursorExpired.operator_action();
        assert!(expired.contains("重新执行查询"), "{expired}");

        // catalog snapshot_failed: "Inspect source metadata and filesystem health."（非重试）
        let snapshot = CanonicalCode::SnapshotFailed.operator_action();
        assert!(
            snapshot.contains("文件系统") || snapshot.contains("源文件"),
            "{snapshot}"
        );
        assert!(
            !snapshot.contains("重试"),
            "snapshot_failed 不可重试: {snapshot}"
        );

        // catalog generation_mismatch: "The index advanced; rerun the query against the
        // new generation."（不是 schema 不兼容）
        let generation = CanonicalCode::GenerationMismatch.operator_action();
        assert!(generation.contains("重新执行查询"), "{generation}");
        assert!(
            !generation.contains("升级") && !generation.contains("不兼容"),
            "generation_mismatch 不是 schema 不兼容: {generation}"
        );
    }

    #[test]
    fn writer_busy_maps_from_port_error() {
        let e: ProtocolError = PortError::WriterBusy("held".into()).into();
        assert_eq!(e.code, CanonicalCode::WriterBusy);
        assert!(e.code.retryable());
    }

    #[test]
    fn port_error_categories_map_to_catalog() {
        let e: ProtocolError = PortError::SnapshotChanged("mtime".into()).into();
        assert_eq!(e.code, CanonicalCode::SourceChanged);

        let e: ProtocolError =
            PortError::SourceIo("cannot open C:/Users/secret/transcript.jsonl".into()).into();
        assert_eq!(e.code, CanonicalCode::SourceIo);
        assert_eq!(e.message, "源文件无法读取");
        assert!(!e.message.contains("secret"));

        let e: ProtocolError = PortError::SchemaIncompatible("newer schema".into()).into();
        assert_eq!(e.code, CanonicalCode::SchemaIncompatible);
    }

    #[test]
    fn source_io_error_masks_source_path_in_message() {
        let e: ProtocolError =
            PortError::SourceIo("cannot open C:/Users/secret/transcript.jsonl".into()).into();
        assert_eq!(e.code, CanonicalCode::SourceIo);
        assert_eq!(e.message, "源文件无法读取");
        assert!(!e.message.contains("secret"));
        assert!(!e.message.contains("transcript.jsonl"));
    }

    #[test]
    fn provider_io_error_masks_source_path_in_message() {
        // ProviderError::Io 携带 OS 层原始文本：Windows 上可能包含真实绝对
        // transcript 路径。与 PortError::SourceIo 同一掩码纪律（R4.3）。
        let e: ProtocolError =
            ProviderError::Io("cannot open C:/Users/secret/transcript.jsonl (os error 2)".into())
                .into();
        assert_eq!(e.code, CanonicalCode::ProviderError);
        assert_eq!(e.message, "提供方源文件读取失败");
        assert!(!e.message.contains("secret"));
        assert!(!e.message.contains("transcript.jsonl"));
    }

    #[test]
    fn boundary_stream_frames_redact_message_not_request_id() {
        let secret = "sk_live_abcdef1234567890xyz";
        for text in [
            progress_frame("ingest", secret, Some(secret)),
            diagnostic_frame("ingest", secret, Some(secret)),
        ] {
            let frame: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(frame["request_id"], secret);
            assert_eq!(frame["command"], "ingest");
            assert_eq!(frame["message"], "[redacted:stripe_key]");
        }
    }

    #[test]
    fn boundary_envelopes_preserve_correlation_cursor_and_stable_ids() {
        let correlation = "sk_live_abcdef1234567890xyz";
        let id = "ses_v1_native-123";
        let page = Page {
            next_cursor: Some(correlation.into()),
            has_more: true,
        };
        let text = success_envelope(
            "list",
            Outcome::Success,
            json!({"session_id": id}),
            0,
            &page,
            &[],
            Some(correlation),
            RetrievalMode::default(),
            &RedactionStatus::default(),
        );
        let frame: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(frame["request_id"], correlation);
        assert_eq!(frame["page"]["next_cursor"], correlation);
        assert_eq!(frame["data"]["session_id"], id);
        let error = ProtocolError::new(CanonicalCode::InvalidRequest, correlation);
        let frame: Value =
            serde_json::from_str(&error_envelope("search", &error, Some(correlation))).unwrap();
        assert_eq!(frame["request_id"], correlation);
        assert_eq!(frame["error"]["message"], "[redacted:stripe_key]");
    }

    #[test]
    fn error_envelope_redacts_message_and_details() {
        // 错误 envelope 与成功 envelope 同一脱敏纪律（ADR-0009）：message 里
        // 回显的用户输入与 details 中的密钥形状值都必须在跨边界输出前脱敏。
        let err = ProtocolError::new(
            CanonicalCode::InvalidRequest,
            "--mode must be lexical|semantic|hybrid, got \"sk-ant-api03-1234567890abcdef\"",
        )
        .with_details(json!({ "echo": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" }));
        let s = error_envelope("search", &err, None);
        assert!(!s.contains("sk-ant-api03-1234567890abcdef"), "{s}");
        assert!(s.contains("[redacted:api_key]"), "{s}");
        assert!(!s.contains("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"), "{s}");
        assert!(s.contains("[redacted:bearer_token]"), "{s}");
    }

    #[test]
    fn backend_port_error_is_masked_in_message() {
        // R4.3：Backend 原始细节（rusqlite/FTS parser 等）不得进入用户可见 message。
        let e: ProtocolError = PortError::Backend("unterminated string".into()).into();
        assert_eq!(e.code, CanonicalCode::CatalogError);
        assert_eq!(e.message, "数据库内部错误");
        assert!(!e.message.contains("unterminated"));
        assert!(!e.message.contains("backend failure"));
    }

    #[test]
    fn invariant_violation_is_internal() {
        let e: ProtocolError = DomainError::InvariantViolation("bug".into()).into();
        assert_eq!(e.code, CanonicalCode::Internal);
        assert_eq!(e.code.exit_code(), 70);

        let e: ProtocolError = DomainError::AmbiguousGraph("ambiguous parent".into()).into();
        assert_eq!(e.code, CanonicalCode::Internal);
        assert_eq!(e.code.exit_code(), 70);
    }

    #[test]
    fn cursor_and_budget_errors_map_to_dedicated_codes() {
        let e: ProtocolError = AppError::Cursor(CursorError::Invalid("x".into())).into();
        assert_eq!(e.code, CanonicalCode::CursorInvalid);
        assert_eq!(e.code.exit_code(), 2);

        let e: ProtocolError = AppError::Cursor(CursorError::Expired("x".into())).into();
        assert_eq!(e.code, CanonicalCode::CursorExpired);
        assert_eq!(e.code.exit_code(), 2);

        let e: ProtocolError = AppError::Cursor(CursorError::GenerationMismatch {
            cursor: 1,
            active: 2,
        })
        .into();
        assert_eq!(e.code, CanonicalCode::GenerationMismatch);
        assert_eq!(e.code.exit_code(), 9);

        let e: ProtocolError = AppError::Cursor(CursorError::ContractMismatch {
            cursor: 2,
            supported: 1,
        })
        .into();
        assert_eq!(e.code, CanonicalCode::SchemaIncompatible);

        let e: ProtocolError = AppError::Budget(
            agent_session_grep_application::budget::BudgetError::TooSmall("x".into()),
        )
        .into();
        assert_eq!(e.code, CanonicalCode::InvalidRequest);
    }

    #[test]
    fn success_envelope_is_well_formed() {
        let s = success_envelope(
            "status",
            Outcome::Success,
            json!({ "catalog_count": 3, "generation": 5 }),
            42,
            &Page::default(),
            &[],
            None,
            RetrievalMode::default(),
            &RedactionStatus::default(),
        );
        assert!(s.contains("\"schema_version\":\"1.1\""));
        assert!(s.contains("\"frame_type\":\"response\""));
        assert!(s.contains("\"command\":\"status\""));
        assert!(s.contains("\"ok\":true"));
        assert!(s.contains("\"outcome\":\"success\""));
        assert!(s.contains("\"data\":{\"catalog_count\":3"));
        assert!(s.contains("\"duration_ms\":42"));
        assert!(s.contains("\"generation\":5"));
        assert!(s.contains("\"next_cursor\":null"));
        assert!(s.contains("\"has_more\":false"));
        assert!(s.contains("\"warnings\":[]"));
        assert!(s.contains("\"retrieval_mode\":\"lexical\""));
        assert!(s.contains("\"mode\":\"default\""));
        assert!(s.contains("\"status\":\"none\""));
        assert!(s.contains("\"redacted_count\":0"));
    }

    #[test]
    fn success_envelope_carries_page_cursor() {
        let s = success_envelope(
            "search",
            Outcome::Partial,
            json!({ "hits": [] }),
            1,
            &Page {
                next_cursor: Some("tok.abc".into()),
                has_more: true,
            },
            &[],
            None,
            RetrievalMode::default(),
            &RedactionStatus::default(),
        );
        assert!(s.contains("\"outcome\":\"partial\""));
        assert!(s.contains("\"next_cursor\":\"tok.abc\""));
        assert!(s.contains("\"has_more\":true"));
    }

    #[test]
    fn success_envelope_carries_warnings_array() {
        let warnings = vec!["w1".to_string(), "w2".to_string()];
        let s = success_envelope(
            "context",
            Outcome::Success,
            json!({}),
            0,
            &Page::default(),
            &warnings,
            None,
            RetrievalMode::default(),
            &RedactionStatus::default(),
        );
        let v: Value = serde_json::from_str(&s).expect("envelope must be valid JSON");
        assert_eq!(v["warnings"], json!(["w1", "w2"]));
    }

    #[test]
    fn success_envelope_carries_retrieval_mode_and_redaction() {
        let s = success_envelope(
            "search",
            Outcome::Success,
            json!({ "hits": [] }),
            0,
            &Page::default(),
            &[],
            None,
            RetrievalMode::default(),
            &RedactionStatus::default(),
        );
        let v: Value = serde_json::from_str(&s).expect("envelope must be valid JSON");
        assert_eq!(v["retrieval_mode"], "lexical");
        assert_eq!(v["redaction"]["mode"], "default");
        assert_eq!(v["redaction"]["status"], "none");
        assert_eq!(v["redaction"]["redacted_count"], 0);
        assert_eq!(v["redaction"]["audit_id"], Value::Null);
    }

    #[test]
    fn valid_request_id_accepts_envelope_pattern() {
        assert!(valid_request_id("a"));
        assert!(valid_request_id(&"x".repeat(128)));
        assert!(valid_request_id("Az09._:-"));
    }

    #[test]
    fn valid_request_id_rejects_out_of_contract_input() {
        assert!(!valid_request_id(""));
        assert!(!valid_request_id(&"x".repeat(129)));
        assert!(!valid_request_id("has space"));
        assert!(!valid_request_id("请求-1"));
    }

    #[test]
    fn frames_echo_caller_request_id_verbatim() {
        let s = success_envelope(
            "status",
            Outcome::Success,
            json!({}),
            0,
            &Page::default(),
            &[],
            Some("abc.123"),
            RetrievalMode::default(),
            &RedactionStatus::default(),
        );
        assert!(s.contains("\"request_id\":\"abc.123\""));

        let err = ProtocolError::new(CanonicalCode::NotFound, "missing");
        let s = error_envelope("get", &err, Some("abc.123"));
        assert!(s.contains("\"request_id\":\"abc.123\""));

        let s = progress_frame("sync", "staged", Some("abc.123"));
        assert!(s.contains("\"request_id\":\"abc.123\""));
    }

    #[test]
    fn frames_generate_request_id_when_absent() {
        let s = success_envelope(
            "status",
            Outcome::Success,
            json!({}),
            0,
            &Page::default(),
            &[],
            None,
            RetrievalMode::default(),
            &RedactionStatus::default(),
        );
        assert!(s.contains("\"request_id\":\"cli-"));
        let s = progress_frame("sync", "staged", None);
        assert!(s.contains("\"request_id\":\"cli-"));
    }

    #[test]
    fn progress_frame_has_exact_contract_shape() {
        let s = progress_frame("sync", "staged source 1/2", Some("req-1"));
        let v: Value = serde_json::from_str(&s).expect("frame must be valid JSON");
        let object = v.as_object().expect("frame must be an object");
        assert_eq!(object.len(), 5);
        assert_eq!(v["schema_version"], SCHEMA_VERSION);
        assert_eq!(v["frame_type"], "progress");
        assert_eq!(v["command"], "sync");
        assert_eq!(v["request_id"], "req-1");
        assert_eq!(v["message"], "staged source 1/2");
    }

    #[test]
    fn diagnostic_frame_has_exact_contract_shape() {
        let s = diagnostic_frame("sync", "note", None);
        let v: Value = serde_json::from_str(&s).expect("frame must be valid JSON");
        assert_eq!(v.as_object().expect("frame must be an object").len(), 5);
        assert_eq!(v["frame_type"], "diagnostic");
    }

    #[test]
    fn cursor_mismatch_errors_populate_bounded_details() {
        let e: ProtocolError = AppError::Cursor(CursorError::GenerationMismatch {
            cursor: 1,
            active: 2,
        })
        .into();
        assert_eq!(
            e.details,
            json!({ "cursor_generation": 1, "active_generation": 2 })
        );

        let e: ProtocolError = AppError::Cursor(CursorError::ContractMismatch {
            cursor: 2,
            supported: 1,
        })
        .into();
        assert_eq!(
            e.details,
            json!({ "cursor_contract_major": 2, "supported": 1 })
        );
    }

    #[test]
    fn message_ambiguity_maps_to_bounded_invalid_request_details() {
        let error: ProtocolError =
            AppError::MessageAmbiguous(agent_session_grep_application::MessageAmbiguity {
                candidate_session_ids: vec!["ses_v1_aaaa".into(), "ses_v1_bbbb".into()],
                candidate_count: 2,
                hint: "retry get_message with one candidate session_id".into(),
            })
            .into();
        assert_eq!(error.code, CanonicalCode::InvalidRequest);
        assert_eq!(error.code.exit_code(), 2);
        assert_eq!(
            error.details,
            json!({
                "candidate_count": 2,
                "candidate_session_ids": ["ses_v1_aaaa", "ses_v1_bbbb"],
                "hint": "retry get_message with one candidate session_id"
            })
        );
    }

    #[test]
    fn non_mismatch_errors_keep_empty_details() {
        let e: ProtocolError = AppError::Cursor(CursorError::Invalid("x".into())).into();
        assert_eq!(e.details, json!({}));
        let e: ProtocolError = PortError::WriterBusy("held".into()).into();
        assert_eq!(e.details, json!({}));
        let e: ProtocolError = DomainError::NotFound("x".into()).into();
        assert_eq!(e.details, json!({}));
    }

    #[test]
    fn error_envelope_emits_details_field() {
        let err: ProtocolError = AppError::Cursor(CursorError::GenerationMismatch {
            cursor: 1,
            active: 2,
        })
        .into();
        let s = error_envelope("search", &err, None);
        let v: Value = serde_json::from_str(&s).expect("envelope must be valid JSON");
        assert_eq!(
            v["error"]["details"],
            json!({ "cursor_generation": 1, "active_generation": 2 })
        );
    }

    #[test]
    fn published_error_catalog_matches_runtime_mapping() {
        let catalog: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../schemas/robot/v1/error-catalog.json"
        )))
        .expect("published error catalog must be valid JSON");
        let published = catalog["errors"]
            .as_array()
            .expect("error catalog must contain errors");
        let runtime = [
            CanonicalCode::InvalidRequest,
            CanonicalCode::NotFound,
            CanonicalCode::SourceIo,
            CanonicalCode::SourceChanged,
            CanonicalCode::SnapshotFailed,
            CanonicalCode::CatalogError,
            CanonicalCode::ProviderError,
            CanonicalCode::CapabilityNotSupported,
            CanonicalCode::WriterBusy,
            CanonicalCode::SchemaIncompatible,
            CanonicalCode::CursorInvalid,
            CanonicalCode::CursorExpired,
            CanonicalCode::GenerationMismatch,
            CanonicalCode::Internal,
        ];
        assert_eq!(published.len(), runtime.len());
        for code in runtime {
            let entry = published
                .iter()
                .find(|entry| entry["code"] == code.as_str())
                .unwrap_or_else(|| panic!("missing published error {}", code.as_str()));
            assert_eq!(entry["cli_exit_code"], code.exit_code());
            assert_eq!(entry["retryable"], code.retryable());
            assert_eq!(entry["robot_ok"], false);
        }
    }

    #[test]
    fn published_envelope_schema_contains_runtime_contract() {
        let schema: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../schemas/robot/v1.1/envelope.schema.json"
        )))
        .expect("published envelope schema must be valid JSON");
        assert_eq!(
            schema["$defs"]["success"]["properties"]["schema_version"]["const"],
            SCHEMA_VERSION
        );
        assert_eq!(
            schema["$defs"]["error"]["properties"]["schema_version"]["const"],
            SCHEMA_VERSION
        );
        let codes = schema["$defs"]["errorBody"]["properties"]["code"]["enum"]
            .as_array()
            .expect("schema must enumerate canonical error codes");
        assert_eq!(codes.len(), 14);
        let search_condition = &schema["$defs"]["success"]["allOf"][0];
        assert_eq!(
            search_condition["if"]["properties"]["command"]["const"],
            "search"
        );
        assert_eq!(
            search_condition["then"]["properties"]["data"]["$ref"],
            "#/$defs/searchData"
        );
        assert_eq!(
            schema["$defs"]["searchData"]["properties"]["hits"]["items"]["$ref"],
            "#/$defs/searchHit"
        );
        // facets: optional structured facet echo, matching SearchFacets enum sets.
        let facets_def = &schema["$defs"]["searchData"]["properties"]["facets"];
        assert_eq!(facets_def["$ref"], "#/$defs/searchFacets");
        let facets_schema = &schema["$defs"]["searchFacets"];
        assert_eq!(facets_schema["additionalProperties"], false);
        assert_eq!(
            facets_schema["properties"]["sidechain"]["enum"],
            json!(["include", "main_only", "subagent_only"])
        );
        assert_eq!(
            facets_schema["properties"]["tool_kind"]["enum"],
            json!(["file", "command", "web", "query", "unknown", null])
        );
        assert_eq!(
            facets_schema["properties"]["tool_name"]["type"],
            json!(["string", "null"])
        );
        assert_eq!(
            schema["$defs"]["searchHit"]["properties"]["why_matched"]["maxItems"],
            8
        );
        assert_eq!(
            schema["$defs"]["searchHit"]["properties"]["suggested_next_commands"]["maxItems"],
            2
        );
        for field in [
            "schema_version",
            "frame_type",
            "command",
            "request_id",
            "ok",
            "outcome",
            "retrieval_mode",
            "redaction",
            "warnings",
            "page",
            "meta",
        ] {
            let required = schema["$defs"]["success"]["required"]
                .as_array()
                .expect("success required must be an array");
            assert!(
                required.iter().any(|value| value == field),
                "missing {field}"
            );
        }
        // v1.1 追加字段：occurrences（省略即 1）与 resume_available（boolean）。
        assert_eq!(
            schema["$defs"]["searchHit"]["properties"]["occurrences"]["type"],
            "integer"
        );
        assert_eq!(
            schema["$defs"]["searchHit"]["properties"]["occurrences"]["minimum"],
            1
        );
        assert_eq!(
            schema["$defs"]["searchHit"]["properties"]["resume_available"]["type"],
            "boolean"
        );
        // frame 词汇表：response/error/progress/diagnostic 四种，全部挂在顶层 oneOf。
        // 冻结断言：1.0 保持发布时原样。测试内不能跑 git，改为显式结构断言——
        // 1.0 的 searchHit 不得包含 v1.1 才引入的 occurrences/resume_available。
        let frozen: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../schemas/robot/v1/envelope.schema.json"
        )))
        .expect("frozen 1.0 envelope schema must be valid JSON");
        assert_eq!(
            frozen["$defs"]["success"]["properties"]["schema_version"]["const"],
            "1.0"
        );
        let frozen_hit = &frozen["$defs"]["searchHit"]["properties"];
        assert!(
            frozen_hit.get("occurrences").is_none(),
            "1.0 must stay frozen: no occurrences"
        );
        assert!(
            frozen_hit.get("resume_available").is_none(),
            "1.0 must stay frozen: no resume_available"
        );
        let one_of = schema["oneOf"]
            .as_array()
            .expect("schema oneOf must be an array");
        assert_eq!(one_of.len(), 4);
        for kind in ["progress", "diagnostic"] {
            let def = &schema["$defs"][kind];
            assert_eq!(def["properties"]["frame_type"]["const"], kind);
            assert_eq!(def["properties"]["schema_version"]["const"], SCHEMA_VERSION);
            assert_eq!(def["additionalProperties"], false);
            let required = def["required"]
                .as_array()
                .unwrap_or_else(|| panic!("{kind} required must be an array"));
            for field in [
                "schema_version",
                "frame_type",
                "command",
                "request_id",
                "message",
            ] {
                assert!(
                    required.iter().any(|value| value == field),
                    "missing {field} in {kind}"
                );
            }
        }
    }

    #[test]
    fn output_mode_rejects_unknown_or_missing_value() {
        assert!(parse_output_mode(&["--output".into(), "yaml".into()]).is_err());
        assert!(parse_output_mode(&["--output".into()]).is_err());
        // --robot 先到、--output 后带非法值：同样是用法错误，不静默接受。
        assert!(parse_output_mode(&["--robot".into(), "--output".into(), "yaml".into()]).is_err());
    }

    #[test]
    fn output_mode_rejects_conflicting_or_duplicate_flags() {
        // --robot 与 --output 冲突（无论取值）：用法错误，不静默 first-wins。
        assert!(parse_output_mode(&["--output".into(), "human".into(), "--robot".into()]).is_err());
        assert!(parse_output_mode(&["--robot".into(), "--output".into(), "json".into()]).is_err());
        assert!(parse_output_mode(&["--robot".into(), "--robot".into()]).is_err());
        // --output 重复（即使取值不同）：用法错误，不静默 first-wins。
        assert!(
            parse_output_mode(&[
                "--output".into(),
                "json".into(),
                "--output".into(),
                "yaml".into()
            ])
            .is_err()
        );
        assert!(
            parse_output_mode(&[
                "--output".into(),
                "human".into(),
                "--output".into(),
                "json".into()
            ])
            .is_err()
        );
        // 命令名之前的其它 flag 仍正常跳过取值，不影响模式解析。
        assert_eq!(
            parse_output_mode(&[
                "--db".into(),
                "store.db".into(),
                "--output".into(),
                "json".into()
            ]),
            Ok(OutputMode::Json)
        );
    }

    #[test]
    fn output_mode_only_reads_flags_in_prefix_position() {
        // 命令名之后的 token 一律不当 flag：query 恰等于 flag 名时按查询走。
        assert_eq!(
            parse_output_mode(&["search".into(), "--robot".into()]),
            Ok(OutputMode::Human)
        );
        assert_eq!(
            parse_output_mode(&["search".into(), "--output".into()]),
            Ok(OutputMode::Human)
        );
        assert_eq!(
            parse_output_mode(&["search".into(), "--robot".into(), "--output".into()]),
            Ok(OutputMode::Human)
        );
        // 前缀位置（命令名之前）的 flag 仍然生效，且带值 flag 跳过其取值。
        assert_eq!(
            parse_output_mode(&[
                "--db".into(),
                "store.db".into(),
                "--robot".into(),
                "search".into(),
                "--output".into(),
            ]),
            Ok(OutputMode::Json)
        );
        assert_eq!(
            parse_output_mode(&[
                "--level".into(),
                "talks".into(),
                "--robot".into(),
                "context".into(),
            ]),
            Ok(OutputMode::Json)
        );
        assert_eq!(
            parse_output_mode(&[
                "--request-id".into(),
                "req-1".into(),
                "--output".into(),
                "jsonl".into(),
                "sync".into(),
                "a.jsonl".into(),
            ]),
            Ok(OutputMode::Jsonl)
        );
    }

    #[test]
    fn error_envelope_carries_code_and_retryable() {
        let err = ProtocolError::new(CanonicalCode::WriterBusy, "another writer holds the lease");
        let s = error_envelope("sync", &err, None);
        assert!(s.contains("\"frame_type\":\"error\""));
        assert!(s.contains("\"ok\":false"));
        assert!(s.contains("\"outcome\":\"failure\""));
        assert!(s.contains("\"code\":\"writer_busy\""));
        assert!(s.contains("\"retryable\":true"));
        assert!(s.contains("\"duration_ms\":0"));
        assert!(s.contains("\"details\":{}"));
    }

    #[test]
    fn value_flag_skip_lists_cover_tool_facets_across_all_scanners() {
        // parse_output_mode 上方的注释要求带值 flag 列表与 main.rs 各前缀
        // 扫描器保持一致。此测试把 main.rs 源 include 进来，逐扫描器断言
        // --tool-kind/--tool-name 都在跳过列表里——上次 drift 正是漏掉它们。
        let main_src = include_str!("lib.rs");
        let scanners = [
            "fn extract_request_id",
            "fn command_name",
            "fn intercept_help_or_version",
            "fn extract_db_flag_impl",
            "fn bare_positionals",
        ];
        for scanner in scanners {
            let start = main_src
                .find(scanner)
                .unwrap_or_else(|| panic!("main.rs missing {scanner}"));
            let end = main_src[start..]
                .find("\n}\n")
                .map(|offset| start + offset)
                .unwrap_or(main_src.len());
            let body = &main_src[start..end];
            assert!(
                body.contains("\"--tool-kind\"") && body.contains("\"--tool-name\""),
                "{scanner} value-skip list missing --tool-kind/--tool-name"
            );
        }
    }

    #[test]
    fn relocation_errors_keep_canonical_categories_and_private_values_out() {
        let secret = "private-root/private-backup/private-plan/private-native";
        for (error, code) in [
            (
                PortError::InvalidRequest(secret.into()),
                CanonicalCode::InvalidRequest,
            ),
            (
                PortError::GenerationMismatch(secret.into()),
                CanonicalCode::GenerationMismatch,
            ),
            (
                PortError::Backend(secret.into()),
                CanonicalCode::CatalogError,
            ),
            (PortError::SourceIo(secret.into()), CanonicalCode::SourceIo),
            (
                PortError::SnapshotChanged(secret.into()),
                CanonicalCode::SourceChanged,
            ),
            (
                PortError::WriterBusy(secret.into()),
                CanonicalCode::WriterBusy,
            ),
            (
                PortError::SchemaIncompatible(secret.into()),
                CanonicalCode::SchemaIncompatible,
            ),
            (PortError::NotFound(secret.into()), CanonicalCode::NotFound),
        ] {
            let projected = ProtocolError::from_private_port_error(error);
            assert_eq!(projected.code, code);
            assert!(!projected.message.contains(secret));
            assert!(projected.details.as_object().unwrap().is_empty());
            assert!(projected.message.len() < 256);
        }
        assert_eq!(
            ProtocolError::from(PortError::InvalidRequest("invalid plan".into())).code,
            CanonicalCode::InvalidRequest
        );
        assert_eq!(
            ProtocolError::from(PortError::GenerationMismatch("stale plan".into())).code,
            CanonicalCode::GenerationMismatch
        );
    }

    #[test]
    fn relocation_scalar_flags_are_skipped_by_every_prefix_scanner() {
        let source = include_str!("lib.rs");
        for flag in ["--from", "--to", "--plan", "--backup", "--alias-ttl-days"] {
            assert_eq!(
                parse_output_mode(&[
                    flag.into(),
                    "private-value".into(),
                    "--robot".into(),
                    "relocate".into()
                ]),
                Ok(OutputMode::Json),
                "a value must not hide --robot",
            );
            for scanner in [
                "fn extract_offline_flag",
                "fn extract_request_id",
                "fn command_name",
                "fn intercept_help_or_version",
                "fn extract_db_flag_impl",
                "fn bare_positionals",
            ] {
                let start = source.find(scanner).expect("scanner exists");
                let end = source[start..]
                    .find("\n}\n")
                    .map_or(source.len(), |offset| start + offset);
                assert!(
                    source[start..end].contains(&format!("\"{flag}\"")),
                    "{scanner} must skip {flag}'s value"
                );
            }
        }
    }
}

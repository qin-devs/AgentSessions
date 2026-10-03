//! agent-session-grep CLI：组合根（composition root）。
//!
//! 本 crate 是唯一把抽象端口与具体 adapter 绑定的地方——它 `new` 出 [`SqliteStore`]
//! 并注入 [`App`]，其余各层对具体后端一无所知；分层依赖保持
//! domain ← ports ← application ← adapters。
//! 首个垂直切片只暴露两个子命令，用于端到端打通 discovery→catalog→search 骨架：
//!
//! ```text
//! agent-session-grep --db <path> index <id-fact> <text>   # 写入 catalog 并索引
//! agent-session-grep --db <path> search <query>           # 全文检索
//! agent-session-grep --db <path> get <wire-id>            # 按 id 取回 payload
//! ```
//!
//! 参数解析刻意手写、不引第三方 CLI 框架——切片阶段只需最小可用面。
//! 输出走 Robot-JSON 雏形（每行一个 JSON 对象），为后续 CONTRACT 对齐留口。

mod hooks;
mod human;
mod mcp;
mod protocol;
mod redaction;
mod repo_identity;
mod serve;
mod trace;
mod tui;

use agent_session_grep_adapters_sqlite::{
    INDEX_PROJECTION_VERSION, PARSER_SEMANTIC_VERSION, SourceActivity, SourceBatch, SourceUsage,
    SqliteStore, capture, open_snapshot_source, verify_snapshot,
};
use agent_session_grep_application::{
    App, AppError, AppRequest, AppResponse, ContextLevel, ResponseBudget, StagedBatch, Truncation,
    bounded_index_text, evidence::Precision, handoff_pack::HandoffInput,
    parse_relative_search_instant, parse_search_instant, select_and_stage_source,
};
use agent_session_grep_domain::{
    ContextPolicy, DomainError, EvidenceSpan, IdKind, MessageEdge, MessagePlacement,
    MessageRelation, SessionIdentityNamespace, Stability, StableId,
};
use agent_session_grep_ports::{
    ParseReport, PortError, ProviderAdapter, ProviderSessionObservation, ReadOnlySource,
    RedactionStatus, ResumeClaimsStore, RetrievalMode, SearchFacets, SearchFilters, SearchInstant,
    SearchProvider, SidechainFacet, SourceResumeClaim, SourceSnapshot,
    capability::{ProviderCapability, ProviderCapabilityMatrix, ProviderMaturity},
    relocation::{
        DEFAULT_ALIAS_TTL_DAYS, MAX_ALIAS_TTL_DAYS, MIN_ALIAS_TTL_DAYS, PLAN_TTL_MS,
        RelocationResult, validate_alias_ttl_days,
    },
};
use agent_session_grep_provider_aider::AiderAdapter;
use agent_session_grep_provider_antigravity::AntigravityAdapter;
use agent_session_grep_provider_claude::ClaudeCodeAdapter;
use agent_session_grep_provider_cline::ClineAdapter;
use agent_session_grep_provider_codebuddy::CodeBuddyAdapter;
use agent_session_grep_provider_codex::CodexAdapter;
use agent_session_grep_provider_cursor::CursorAdapter;
use agent_session_grep_provider_grok::GrokBuildAdapter;
use agent_session_grep_provider_hermes::OpenHermesAdapter;
use agent_session_grep_provider_kimi::KimiCodeAdapter;
use agent_session_grep_provider_openclaw::OpenClawAdapter;
use agent_session_grep_provider_opencode::OpenCodeAdapter;
use agent_session_grep_provider_pi::PiAdapter;
use agent_session_grep_provider_qoder::QoderAdapter;
use protocol::{CanonicalCode, ProtocolError};
use std::collections::{BTreeMap, BTreeSet};

/// Provider parse diagnostics exposed through the existing success-envelope
/// `warnings` channel are bounded at the CLI boundary. This keeps a badly
/// damaged source from producing an unbounded response while preserving the
/// actionable line/session detail required by sync diagnostics.
const DIAGNOSTIC_WARNING_LIMIT: usize = 16;
const DIAGNOSTIC_WARNING_CHARS: usize = 512;
const PARTIAL_SOURCE_WARNING: &str = "partial source scan: previously indexed history is retained \
    where present; old and new document-scoped copies may temporarily coexist until a complete rescan";

/// CLI 顶层错误：所有失败都归一到 [`ProtocolError`]，exit code 由 Error Catalog 决定。
///
/// `Usage` 保留为薄封装，仅表示参数校验失败（映射 `invalid_request` → exit 2），
/// 使用法错误与业务错误遵循 CLI/Robot/MCP contract 的同一 envelope/退出码映射。
#[derive(Debug)]
struct CliError(ProtocolError);

impl CliError {
    /// 参数或请求校验失败（exit 2）。
    fn usage(msg: impl Into<String>) -> Self {
        CliError(ProtocolError::new(CanonicalCode::InvalidRequest, msg))
    }
}

impl From<DomainError> for CliError {
    fn from(e: DomainError) -> Self {
        CliError(e.into())
    }
}

impl From<AppError> for CliError {
    fn from(e: AppError) -> Self {
        CliError(e.into())
    }
}

impl From<ProtocolError> for CliError {
    fn from(e: ProtocolError) -> Self {
        CliError(e)
    }
}

pub fn run_cli() {
    // command 名先解析出来供错误 envelope 使用；失败时也要标注是哪个命令。
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = command_name(&args);
    let mode = match protocol::parse_output_mode(&args) {
        Ok(mode) => mode,
        Err(message) => {
            let err = ProtocolError::new(CanonicalCode::InvalidRequest, message);
            protocol::write_stdout_line(&protocol::error_envelope(&command, &err, None));
            std::process::exit(err.code.exit_code());
        }
    };
    // --request-id：robot 调用方的关联 id。非法值是用法错误（exit 2），
    // 不静默替换为生成 id——那会让调用方以为关联成功。
    let request_id = match extract_request_id(&args) {
        Ok(id) => id,
        Err(message) => {
            let err = ProtocolError::new(CanonicalCode::InvalidRequest, message);
            match mode {
                protocol::OutputMode::Human => {
                    render_human_error(&err);
                }
                protocol::OutputMode::Json | protocol::OutputMode::Jsonl => {
                    protocol::write_stdout_line(&protocol::error_envelope(&command, &err, None));
                }
            }
            std::process::exit(err.code.exit_code());
        }
    };
    match run(
        &args,
        mode,
        request_id.as_deref(),
        extract_offline_flag(&args),
    ) {
        Ok(protocol::Outcome::Success) => {}
        // 部分成功（预算截断）按 contract §5 exit 10——结果可用但不完整，不伪装 success。
        Ok(protocol::Outcome::Partial) => std::process::exit(10),
        Err(CliError(err)) => {
            // hook 的退出码同它的 stdout 一样属于 Claude Code 契约，不属于本 CLI 的
            // 错误目录：在那份契约里 exit 2 是"阻塞这次提交"。于是一个含控制字符的
            // 普通 prompt（App 会拒 `query contains control characters`）或 settings
            // 里的事件名拼写错误，都会让用户的提问被直接丢弃。注入失败必须是非阻塞
            // 的：stdout 一个字节不写（否则会被原样注入上下文），诊断走 stderr，
            // 退出码取契约里的"非阻塞错误" 1。
            if command == "hook" {
                eprintln!(
                    "hook: not injecting: [{}] {}",
                    err.code.as_str(),
                    redaction::redact_text(&err.message).0
                );
                std::process::exit(1);
            }
            // 错误 envelope 只写 stdout 一个对象；进程级诊断（人类模式）走 stderr。
            match mode {
                protocol::OutputMode::Human => {
                    render_human_error(&err);
                }
                protocol::OutputMode::Json | protocol::OutputMode::Jsonl => {
                    protocol::write_stdout_line(&protocol::error_envelope(
                        &command,
                        &err,
                        request_id.as_deref(),
                    ));
                }
            }
            std::process::exit(err.code.exit_code());
        }
    }
}

/// 人类模式的错误渲染：报错行（保留稳定 code 前缀）+ 一行白话"下一步"指引
/// （error catalog 的 `operator_action` 面向新手落地）。robot/json 模式保持
/// 稳定 envelope，不受影响。
fn render_human_error(err: &ProtocolError) {
    eprintln!("error [{}]: {}", err.code.as_str(), err.message);
    eprintln!("下一步：{}", err.code.operator_action());
}

/// Limit the number of provider diagnostics, retaining raw text until rendering.
/// The output boundary must redact machine diagnostics before character clamping
/// and retain the redaction count; Human diagnostics are clamped without redaction.
fn diagnostic_warnings<'a>(
    diagnostics: impl IntoIterator<Item = &'a str>,
    total: usize,
) -> Vec<String> {
    let detail_limit = if total > DIAGNOSTIC_WARNING_LIMIT {
        DIAGNOSTIC_WARNING_LIMIT.saturating_sub(1)
    } else {
        DIAGNOSTIC_WARNING_LIMIT
    };
    let mut warnings: Vec<String> = diagnostics
        .into_iter()
        .take(detail_limit)
        .map(str::to_owned)
        .collect();
    if total > detail_limit {
        warnings.push(format!(
            "{} additional provider diagnostics omitted",
            total - detail_limit
        ));
    }
    warnings
}

/// Apply output policy before bounding provider diagnostic text. Count changed
/// warning entries before truncation, not by comparing the final bounded strings.
fn render_warnings(
    command: &str,
    mode: protocol::OutputMode,
    warnings: &[String],
) -> (Vec<String>, u64) {
    let mut redacted_count = 0;
    let warnings = warnings
        .iter()
        .map(|warning| {
            let mut text = if mode == protocol::OutputMode::Human {
                warning.clone()
            } else {
                let (text, status) = redaction::redact_text(warning);
                redacted_count += u64::from(status.redacted_count > 0);
                text
            };
            if matches!(command, "ingest" | "sync")
                && text.chars().count() > DIAGNOSTIC_WARNING_CHARS
            {
                text = text.chars().take(DIAGNOSTIC_WARNING_CHARS).collect();
                text.push('…');
            }
            text
        })
        .collect();
    (warnings, redacted_count)
}

/// 从参数抽出 `--offline`（裸 flag，无取值）：只在全局 flag 前缀位置识别。
/// 缺省 false；命令名之后的同名 token 是位置参数，不当 flag 解析。重复出现
/// 与其它裸 flag（`--discover`/`--include-system`）一致，按在场一次处理。
fn extract_offline_flag(args: &[String]) -> bool {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if !a.starts_with('-') {
            return false; // 已到命令名：之后的 token 不当 flag 解析
        }
        if a == "--offline" {
            return true;
        }
        // 其它带值 flag 跳过其取值，避免把取值误当命令名。
        match a.as_str() {
            "--db" | "--output" | "--request-id" | "--cursor" | "--max-items" | "--max-bytes"
            | "--max-messages" | "--policy" | "--level" | "--provider" | "--since" | "--until"
            | "--repo" | "--session" | "--around" | "--max-evidence" | "--max-tokens"
            | "--tool-kind" | "--tool-name" | "--from" | "--to" | "--alias-ttl-days" | "--plan"
            | "--backup" => {
                it.next();
            }
            _ => {}
        }
    }
    false
}

/// 从参数抽出 `--request-id`：缺 flag → None；有 flag 则值必须满足 envelope
/// 约束（`^[A-Za-z0-9._:-]+$`，1..=128），否则是用法错误。
///
/// 与 [`protocol::parse_output_mode`] 同一规则：flag 只在前缀位置（第一个位置
/// 参数之前）识别——命令名/查询文本恰等于 `--request-id` 时按查询走，不得误判。
/// 取值为已知 flag 名（`--request-id --robot` 会把 flag 当取值，且 `--robot`
/// 恰好能通过 id 字符集校验）与重复 `--request-id`（不再静默 first-wins）
/// 都是用法错误（R8.1/R8.2）。
fn extract_request_id(args: &[String]) -> Result<Option<String>, String> {
    let mut it = args.iter();
    let mut seen: Option<String> = None;
    while let Some(a) = it.next() {
        if !a.starts_with('-') {
            return Ok(seen); // 已到命令名：之后的 token 不当 flag 解析
        }
        if a == "--request-id" {
            let value = it.next().ok_or("--request-id requires a value")?;
            if is_known_flag_name(value) {
                return Err(format!(
                    "--request-id requires a value, got {value:?} (a flag name)"
                ));
            }
            if !protocol::valid_request_id(value) {
                return Err(format!(
                    "--request-id must match ^[A-Za-z0-9._:-]+$ (1..=128 chars), got {value:?}"
                ));
            }
            if seen.is_some() {
                return Err("duplicate --request-id".into());
            }
            seen = Some(value.clone());
        }
        // 其它带值 flag 跳过其取值，避免把取值误当位置参数提前终止扫描。
        match a.as_str() {
            "--db" | "--output" | "--cursor" | "--max-items" | "--max-bytes" | "--max-messages"
            | "--max-evidence" | "--max-tokens" | "--policy" | "--level" | "--provider"
            | "--since" | "--until" | "--repo" | "--session" | "--around" | "--tool-kind"
            | "--tool-name" | "--from" | "--to" | "--alias-ttl-days" | "--plan" | "--backup" => {
                it.next();
            }
            _ => {}
        }
    }
    Ok(seen)
}

/// 从参数里解出子命令名（用于错误 envelope 的 `command` 字段）。
///
/// 跳过已知 flag（带值的连同其取值）；第一个既不是已知 flag、也不是已知 flag
/// 取值的 token 即命令名。未知的 `-` 开头 token 不是 flag——它是命令名笔误
/// （如 `--bogus`），错误 envelope 的 `command` 必须指向它而不是后面的真命令
/// （R8.4：`--bogus --robot status` 的失败者是 `--bogus`，不是 `status`）。
fn command_name(args: &[String]) -> String {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--db" | "--output" | "--cursor" | "--max-items" | "--max-bytes" | "--max-messages"
            | "--max-evidence" | "--max-tokens" | "--policy" | "--level" | "--request-id"
            | "--provider" | "--since" | "--until" | "--repo" | "--session" | "--around"
            | "--tool-kind" | "--tool-name" | "--from" | "--to" | "--alias-ttl-days" | "--plan"
            | "--backup" => {
                it.next(); // 消费其取值
            }
            "--robot" | "--no-color" | "--help" | "-h" | "--version" | "-V" | "--discover"
            | "--offline" => {}
            s => return if known_subcommand(s) { s } else { "unknown" }.to_string(),
        }
    }
    "unknown".into()
}

/// help/version 拦截结果（ADR-0006）：在 `--db` 解析与存储打开之前识别。
#[derive(Debug, Clone, PartialEq, Eq)]
enum HelpRequest {
    /// 顶层 `--help` / `-h`（无子命令）。
    TopLevelHelp,
    /// 顶层 `--version` / `-V`。
    TopLevelVersion,
    /// `<cmd> --help` / `-h`（含 `index rebuild --help`）。
    SubcommandHelp(String),
}

/// 提前拦截 help/version（ADR-0006）：只在全局 flag 前缀位置之后识别。
///
/// - 全为前缀 flag（无命令名）时，`--help`/`-h` → 顶层帮助，`--version`/`-V`
///   → 版本；两者同时出现时帮助优先（与旧行为一致）。
/// - 命令名紧跟 `--help`/`-h` → 该子命令帮助（`index rebuild --help` 也覆盖）；
///   `search foo --help` 里更靠后的 `--help` 不在此位——不得拦截，交给命令层
///   按位置参数处理（search 只接受一个查询词，多余 token 是 usage error）。
/// - 未知命令（不在 [`known_subcommand`]）后的 `--help` 不拦截——交给 dispatch
///   报 unknown subcommand，而不是给出误导性帮助。
fn intercept_help_or_version(args: &[String]) -> Option<HelpRequest> {
    let mut it = args.iter();
    let mut prefix_help = false;
    let mut prefix_version = false;
    while let Some(token) = it.next() {
        if !token.starts_with('-') {
            // 第一个裸 token 即命令名：紧随其后的 --help/-h 是子命令帮助。
            let cmd = token.as_str();
            match it.next().map(String::as_str) {
                Some("--help") | Some("-h") if known_subcommand(cmd) => {
                    return Some(HelpRequest::SubcommandHelp(cmd.to_string()));
                }
                // index rebuild|embeddings --help：rebuild/embeddings 是 index 的
                // 子词，帮助旗标跟在它们后面。
                Some("rebuild") | Some("embeddings") if cmd == "index" => {
                    if matches!(it.next().map(String::as_str), Some("--help") | Some("-h")) {
                        return Some(HelpRequest::SubcommandHelp("index".into()));
                    }
                }
                // model import|status --help：import/status 是 model 的子词，
                // 帮助旗标跟在它们后面（与 index rebuild --help 同规则）。
                Some("import") | Some("status") if cmd == "model" => {
                    if matches!(it.next().map(String::as_str), Some("--help") | Some("-h")) {
                        return Some(HelpRequest::SubcommandHelp("model".into()));
                    }
                }
                _ => {}
            }
            // 命令已出现且帮助旗标不在紧跟位：不拦截，按正常命令/查询走。
            return None;
        }
        match token.as_str() {
            "--help" | "-h" => prefix_help = true,
            "--version" | "-V" => prefix_version = true,
            // 裸 flag（无取值）不改变拦截判定：--robot/--no-color/--offline 等同理。
            "--robot" | "--no-color" | "--discover" | "--offline" => {}
            // 带值 flag 跳过其取值，避免把取值误当命令名。
            "--db" | "--output" | "--request-id" | "--cursor" | "--max-items" | "--max-bytes"
            | "--max-messages" | "--max-evidence" | "--max-tokens" | "--policy" | "--level"
            | "--provider" | "--since" | "--until" | "--repo" | "--session" | "--around"
            | "--tool-kind" | "--tool-name" | "--from" | "--to" | "--alias-ttl-days" | "--plan"
            | "--backup" => {
                it.next();
            }
            _ => {}
        }
    }
    if prefix_help {
        Some(HelpRequest::TopLevelHelp)
    } else if prefix_version {
        Some(HelpRequest::TopLevelVersion)
    } else {
        None
    }
}

fn run(
    args: &[String],
    mode: protocol::OutputMode,
    request_id: Option<&str>,
    offline: bool,
) -> Result<protocol::Outcome, CliError> {
    let started = std::time::Instant::now();
    // help/version 提前拦截（ADR-0006）：在解析 --db、打开存储、special-command
    // 分发（doctor/config/mcp/tui）之前处理——`<cmd> --help` 不要求 --db，任何
    // help 路径都不创建数据库、不抢 writer lease、无文件副作用。
    // 语义：--help/-h/--version 紧跟命令名（对 index 可跟在 rebuild 后）才是
    // 子命令帮助；命令名之后更靠后的同名 token 不得拦截——由命令层按位置参数
    // 处理（search 只接受一个查询词，`search foo --help` 因此是多余参数
    // usage error，`search --help` 则始终是帮助拦截）。查询文本恰等于 flag 名
    // 的检索（`search --robot` / `search --output`）只发生在 help 旗标不在
    // 命令名紧跟位时。
    if let Some(intercept) = intercept_help_or_version(args) {
        match intercept {
            HelpRequest::TopLevelHelp => emit_help("help", &help_text(), mode, request_id),
            HelpRequest::TopLevelVersion => emit_version(
                "version",
                &format!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
                mode,
                request_id,
            ),
            HelpRequest::SubcommandHelp(cmd) => {
                emit_help(&cmd, subcommand_help_text(&cmd), mode, request_id);
            }
        }
        return Ok(protocol::Outcome::Success);
    }
    if command_name(args) == "doctor" {
        return doctor(args, mode, request_id, offline);
    }
    if command_name(args) == "config" {
        let positionals = bare_positionals(args);
        if positionals == ["config".to_string(), "paths".to_string()] {
            return config_paths(mode, request_id);
        }
        if positionals.first().map(String::as_str) == Some("config") && positionals.len() > 2 {
            return Err(CliError::usage(
                "config paths takes no additional arguments",
            ));
        }
        return Err(CliError::usage(
            "config paths is the only supported config command",
        ));
    }

    if command_name(args) == "providers" {
        if bare_positionals(args).len() > 1 {
            return Err(CliError::usage("providers takes no positional arguments"));
        }
        return providers(mode, request_id);
    }

    // model import/status: offline-only model cache management. Does not open
    // the catalog DB. Import requires a semantic-candle build and refuses with
    // capability_not_supported otherwise (honest, never stages silently);
    // status works in every build and reports the default bundle state.
    if command_name(args) == "model" {
        return model_command(args, mode, request_id, offline);
    }

    let (db, rest) = parse_db_flag(args)?;
    // Disabled hooks and payloads without a query have no catalog dependency.
    if rest.first().map(String::as_str) == Some("hook") {
        let data = hook_data(&db, &rest, offline)?;
        if let Some(line) = hooks::hook_stdout_line(&data) {
            protocol::write_stdout_line(&line);
        }
        eprintln!("{}", hooks::hook_diagnostics(&data));
        return Ok(protocol::Outcome::Success);
    }
    // Relocation validates all flags before acquiring a writer. Its apply path
    // also requires an existing, current-schema catalog: opening for write must
    // never bootstrap a missing catalog or upgrade one before plan validation.
    let relocation = rest.first().is_some_and(|command| command == "relocate");
    let relocation_apply = if relocation {
        let request = parse_relocation_args(&rest)?;
        if request.apply.is_some() {
            drop(SqliteStore::open(&db).map_err(ProtocolError::from_private_port_error)?);
        }
        request.apply.is_some()
    } else {
        false
    };
    // 写入子命令抢 data-root writer lease；读路径不抢，允许多读者并发。
    let writes = relocation_apply
        || rest
            .first()
            .is_some_and(|command| matches!(command.as_str(), "index" | "ingest" | "sync"));
    let store = if relocation_apply {
        SqliteStore::open_for_relocation(&db)
    } else if writes {
        SqliteStore::open_for_write(&db)
    } else {
        SqliteStore::open(&db)
    }
    .map_err(|error| {
        if relocation {
            ProtocolError::from_private_port_error(error)
        } else {
            ProtocolError::from(error)
        }
    })?
    .with_relocation_clock(|| Ok(app_clock_ms()));
    if writes {
        // repo identity（schema v16）：写路径注入真实 git 解析器（Recall
        // 同款 rev-parse + remote get-url）。检测失败一律 None（诚实降级），
        // 绝不阻塞 sync/index——git 不可用只是少一维投影。
        store.set_repo_slug_resolver(Box::new(repo_identity::GitRepoSlugResolver::default()));
    }
    // mcp：stdio JSON-RPC 服务接管整个 stdout（MCP framing 即协议），不走
    // dispatch/emit_result；--output/--robot/--request-id 对其无意义（design §0.6）。
    if rest.first().map(String::as_str) == Some("mcp") {
        if rest.len() > 1 {
            return Err(CliError::usage("mcp takes no positional arguments"));
        }
        return mcp::serve(&store, offline);
    }
    // tui：交互式只读浏览（Preview）。同 mcp 一样接管终端，不走 dispatch/
    // emit_result；输出模式 flag 对其无意义（task design §0.6）。
    // `tui --snapshot-json <query>` 是无终端的 headless 结构投影，供 release
    // 一致性 harness 复用同一 Application 搜索路径做跨入口比对。
    if rest.first().map(String::as_str) == Some("tui") {
        let mut tui_args = rest[1..].to_vec();
        let snapshot_query = extract_flag(&mut tui_args, "--snapshot-json")?;
        if let Some(query) = snapshot_query {
            if !tui_args.is_empty() {
                return Err(CliError::usage(
                    "tui --snapshot-json <query> takes no additional arguments",
                ));
            }
            let snapshot = tui::snapshot_search(&store, query)?;
            protocol::write_stdout_line(&snapshot.to_string());
            return Ok(protocol::Outcome::Success);
        }
        if !tui_args.is_empty() {
            return Err(CliError::usage("tui takes no positional arguments"));
        }
        return tui::run(&store);
    }
    // serve：loopback HTTP + 嵌入式 Web UI。同样接管（长期运行），不走
    // dispatch/emit_result；--output/--robot 无意义。`--port <n>` 可选（默认 0 = 随机端口）。
    if rest.first().map(String::as_str) == Some("serve") {
        let mut args = rest[1..].to_vec();
        let port_value = extract_flag(&mut args, "--port")?;
        let lan_requested = take_bool_flag(&mut args, "--lan");
        if lan_requested {
            return Err(CliError::usage(
                "serve --lan: capability_not_supported; this release is loopback-only",
            ));
        }
        if !args.is_empty() {
            return Err(CliError::usage("serve takes no positional arguments"));
        }
        let port = port_value
            .as_deref()
            .map(|v| {
                v.parse::<u16>()
                    .map_err(|_| CliError::usage("--port 需要 0-65535 的整数"))
            })
            .transpose()?
            .unwrap_or(0);
        let session = serve::ServeSession::bind_loopback(port)
            .map_err(|e| CliError::usage(format!("serve: bind failed: {e}")))?;
        return serve::run(&session, &db, offline, &store);
    }
    // catalog 与 index 是同一个 SqliteStore；App 泛型接受同一实例的两次移动，
    // 故这里克隆一个连接语义上的第二把手不可行——改为让 App 持有单一 store。
    let (command, outcome, data, page, warnings) =
        dispatch(&store, &db, &rest, mode, request_id, offline)?;
    let duration_ms = started.elapsed().as_millis() as u64;
    // 生效检索模式：search 的 data 已含 `retrieval_mode` 字段（render 投影）；
    // 其他命令恒为 lexical。
    let retrieval_mode = data
        .get("retrieval_mode")
        .and_then(serde_json::Value::as_str)
        .map(|s| match s {
            "semantic" => RetrievalMode::Semantic,
            "hybrid" => RetrievalMode::Hybrid,
            "lexical_fallback" => RetrievalMode::LexicalFallback,
            _ => RetrievalMode::Lexical,
        })
        .unwrap_or(RetrievalMode::Lexical);
    emit_result(
        command,
        mode,
        outcome,
        data,
        duration_ms,
        &page,
        &warnings,
        request_id,
        retrieval_mode,
    );
    Ok(outcome)
}

/// Parse hook input before opening a read-only catalog. Only an enabled hook
/// with an actual query needs storage; no-op hooks remain usable on a fresh install.
fn hook_data(db: &str, rest: &[String], offline: bool) -> Result<serde_json::Value, CliError> {
    // Claude Code Hook（#8）：默认关闭，用户显式 --enable 才注入历史上下文。
    // payload 从 stdin 读，输出走 Claude Code 的 hookSpecificOutput 契约。
    // 未启用时输出空 context——不报错、不注入任何历史。
    let mut args = rest.to_vec();
    let max_tokens_flag = extract_flag(&mut args, "--max-tokens")?;
    let enabled = take_bool_flag(&mut args, "--enable");
    // #8 hook provider/time filter：--provider 可重复（OR），--decay-days
    // 限定时间窗（0 = 不过滤）。两者都是 opt-in，缺省空/0 = 全部历史。
    // --repo（schema v16）同为 opt-in：只注入该仓库的历史（与 search
    // `--repo` / MCP `repo` 同一维度与同一语义）。
    let providers = extract_repeated_flag(&mut args, "--provider")?;
    let decay_days = extract_flag(&mut args, "--decay-days")?;
    let repo = extract_flag(&mut args, "--repo")?;
    no_extra_args(&args, 1, "hook <session-start|user-prompt-submit>")?;
    let raw_event = arg(&args, 1, "hook <session-start|user-prompt-submit>")?;
    let event = hooks::HookEvent::parse(raw_event).ok_or_else(|| {
        CliError::usage("hook <event>: event must be session-start|user-prompt-submit")
    })?;
    let max_tokens = max_tokens_flag
        .as_deref()
        .map(|v| {
            v.parse::<u64>()
                .map_err(|_| CliError::usage("--max-tokens 需要非负整数"))
        })
        .transpose()?
        .unwrap_or(2000);
    let config = hooks::HookConfig {
        enabled,
        max_tokens,
        providers,
        decay_days: decay_days
            .as_deref()
            .map(|v| {
                v.parse::<u32>()
                    .map_err(|_| CliError::usage("--decay-days 需要非负整数"))
            })
            .transpose()?
            .unwrap_or(0),
        repo,
        ..Default::default()
    };
    // stdin payload 允许为空（手工调用/探测）：空即无 query，注入空 context。
    let mut raw_payload = String::new();
    use std::io::Read as _;
    std::io::stdin()
        .read_to_string(&mut raw_payload)
        .map_err(|_| CliError::usage("hook: failed to read payload from stdin"))?;
    let payload: serde_json::Value = if raw_payload.trim().is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_str(&raw_payload)
            .map_err(|_| CliError::usage("hook: payload is not valid JSON"))?
    };
    let query = hooks::query_from_payload(event, &payload);
    let (text, hits_count) = match (config.should_run(), query) {
        (true, Some(query)) => {
            let store = SqliteStore::open(db).map_err(ProtocolError::from)?;
            let app = resume_app(&store);
            let response = app.handle(AppRequest::Search {
                query: query.clone(),
                filters: hook_search_filters(&config, app.now_ms())?,
                facets: SearchFacets::default(),
                limit: 10,
                cursor: None,
                budget: ResponseBudget::default(),
                include_system: false,
                group_by_session: true,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })?;
            let AppResponse::Search { hits, .. } = response else {
                return Err(CliError::usage("hook: unexpected search response"));
            };
            let mut text = hooks::format_context_header(&query, hits.len());
            for hit in &hits {
                // Hook 注入是跨边界输出：文本经脱敏后才进入其他 agent 上下文。
                let (body, _) = redaction::redact_text(hit.text.as_deref().unwrap_or(""));
                text.push_str(&format!("- [{}] {}\n", hit.id.as_str(), body));
            }
            let count = hits.len();
            (text, count)
        }
        _ => (String::new(), 0usize),
    };
    let output = hooks::build_hook_output(event, &text, config.max_tokens);
    let mut data = serde_json::to_value(&output)
        .map_err(|e| CliError::usage(format!("hook: serialization error: {e}")))?;
    if let Some(object) = data.as_object_mut() {
        object.insert("event".into(), serde_json::json!(event.as_str()));
        object.insert("enabled".into(), serde_json::json!(config.should_run()));
        object.insert("offline".into(), serde_json::json!(offline));
        object.insert("hits".into(), serde_json::json!(hits_count));
    }
    Ok(data)
}

/// 成功结果的统一出口（contract §6 truth table）：
/// Human → 渲染器文本行走 stdout、warnings 走 stderr（无 envelope）；
/// Json/Jsonl → 单个 success envelope。所有 stdout 写入都经 pipe-safe 通道。
#[allow(clippy::too_many_arguments)]
fn emit_result(
    command: &str,
    mode: protocol::OutputMode,
    outcome: protocol::Outcome,
    data: serde_json::Value,
    duration_ms: u64,
    page: &protocol::Page,
    warnings: &[String],
    request_id: Option<&str>,
    retrieval_mode: RetrievalMode,
) {
    let (warnings, warning_redactions) = render_warnings(command, mode, warnings);
    match mode {
        protocol::OutputMode::Human => {
            for warning in warnings {
                eprintln!("warning: {warning}");
            }
            for line in human::render_success(command, outcome, &data, page) {
                protocol::write_stdout_line(&line);
            }
        }
        protocol::OutputMode::Json | protocol::OutputMode::Jsonl => {
            // ADR-0009: machine/cross-boundary output is redacted by default.
            // Human CLI output stays unredacted per ADR-0004 (handled above).
            let (redacted_data, mut redaction) = redaction::redact_value(data);
            if warning_redactions > 0 {
                redaction.redacted_count += warning_redactions;
                redaction.status = agent_session_grep_ports::RedactionState::Applied;
            }
            protocol::write_stdout_line(&protocol::success_envelope(
                command,
                outcome,
                redacted_data,
                duration_ms,
                page,
                &warnings,
                request_id,
                retrieval_mode,
                &redaction,
            ));
        }
    }
}

#[derive(serde::Serialize)]
struct ProviderCapabilityView<'a> {
    #[serde(flatten)]
    capability: &'a ProviderCapability,
    maturity_target: Option<ProviderMaturity>,
}

/// Read-only public projection of the provider capability matrix. The current
/// matrix supplies every fact; only `maturity_target` is computed, through the
/// matrix-owned roadmap function rather than an entry-point-local table.
fn provider_matrix_data() -> serde_json::Value {
    let matrix = ProviderCapabilityMatrix::current();
    let providers = matrix
        .providers
        .iter()
        .map(|capability| ProviderCapabilityView {
            capability,
            maturity_target: ProviderMaturity::target_for(&capability.provider_id),
        })
        .collect::<Vec<_>>();
    // `semantic` 是对 frozen v1.1 envelope 的加法键（Robot 消费方一次命令即可
    // 同时发现能力矩阵与语义检索事实）；`data` 对 providers 无 schema 约束。
    serde_json::json!({
        "providers": providers,
        "semantic": semantic_capability_data(),
        "relocation": {
            "interfaces": ["cli"],
            "command": "relocate",
            "default_action": "preview",
            "apply_requires": ["plan", "new_verified_backup"],
            "plan_ttl_seconds": PLAN_TTL_MS / 1000,
            "alias_ttl_days": {
                "default": DEFAULT_ALIAS_TTL_DAYS,
                "minimum": MIN_ALIAS_TTL_DAYS,
                "maximum": MAX_ALIAS_TTL_DAYS,
            },
            "moves_source_files": false,
        },
    })
}

/// 语义检索构建事实（`providers` 的 `semantic` 键，`model status` 同一诚实模式）。
///
/// - `feature`：编译进 semantic-candle 时为 "semantic-candle"，默认构建为 null。
/// - `default_model`：恒为诚实默认向量化器 id（bigram-hash，非语义模型）。
/// - `runtime`：编译进 semantic-candle 时为本地 Candle E5 运行时的稳定标识
///   "candle-e5-local"，默认构建为 null。这是构建事实，不是安装事实：E5
///   bundle 是否已通过校验缓存在本地，由 `model status` 报告。
fn semantic_capability_data() -> serde_json::Value {
    use agent_session_grep_application::embedding::BIGRAM_HASH_MODEL_ID;
    #[cfg(feature = "semantic-candle")]
    {
        serde_json::json!({
            "feature": "semantic-candle",
            "default_model": BIGRAM_HASH_MODEL_ID,
            "runtime": "candle-e5-local",
        })
    }
    #[cfg(not(feature = "semantic-candle"))]
    {
        serde_json::json!({
            "feature": null,
            "default_model": BIGRAM_HASH_MODEL_ID,
            "runtime": null,
        })
    }
}

/// `doctor` 的构建事实：semantic-candle 编译进二进制时报告 true，默认构建
/// 报告 null（与 `model status` 的 `feature: null` 诚实模式一致）。
fn semantic_feature_flag() -> serde_json::Value {
    #[cfg(feature = "semantic-candle")]
    {
        serde_json::json!(true)
    }
    #[cfg(not(feature = "semantic-candle"))]
    {
        serde_json::json!(null)
    }
}

fn providers(
    mode: protocol::OutputMode,
    request_id: Option<&str>,
) -> Result<protocol::Outcome, CliError> {
    emit_result(
        "providers",
        mode,
        protocol::Outcome::Success,
        provider_matrix_data(),
        0,
        &protocol::Page::default(),
        &[],
        request_id,
        RetrievalMode::Lexical,
    );
    Ok(protocol::Outcome::Success)
}

fn config_paths(
    mode: protocol::OutputMode,
    request_id: Option<&str>,
) -> Result<protocol::Outcome, CliError> {
    let paths = platform_paths()?;
    emit_result(
        "config.paths",
        mode,
        protocol::Outcome::Success,
        paths,
        0,
        &protocol::Page::default(),
        &[],
        request_id,
        RetrievalMode::Lexical,
    );
    Ok(protocol::Outcome::Success)
}

/// `model import --dir <bundle>` / `model status`: offline model-cache management.
///
/// Never opens a network connection. Import verifies SHA-256 of every declared
/// file, then atomically publishes under `{cache}/models/{model_id}/`. Status
/// reports whether the default E5 bundle is present and verified.
fn model_command(
    args: &[String],
    mode: protocol::OutputMode,
    request_id: Option<&str>,
    offline: bool,
) -> Result<protocol::Outcome, CliError> {
    // model never needs the network; offline is reported honestly but never rejects.
    let _ = offline;
    let positionals = bare_positionals(args);
    let sub = positionals.get(1).map(String::as_str).unwrap_or("");
    match sub {
        "import" => {
            let mut rest = args.to_vec();
            // Strip the command name tokens so extract_flag sees only flags.
            // Flags may appear before or after `model import`.
            let dir = extract_flag(&mut rest, "--dir")?
                .ok_or_else(|| CliError::usage("model import requires --dir <bundle-directory>"))?;
            let paths = platform_paths()?;
            let cache = paths
                .get("cache")
                .and_then(|v| v.as_str())
                .ok_or_else(|| CliError::usage("cannot resolve platform cache path"))?;
            #[cfg(feature = "semantic-candle")]
            {
                let published = agent_session_grep_application::candle_embedding::import_bundle(
                    std::path::Path::new(&dir),
                    std::path::Path::new(cache),
                )
                .map_err(|e| CliError(ProtocolError::from(e)))?;
                let manifest =
                    agent_session_grep_application::candle_embedding::read_and_verify_bundle(
                        &published,
                    )
                    .map_err(|e| CliError(ProtocolError::from(e)))?;
                emit_result(
                    "model.import",
                    mode,
                    protocol::Outcome::Success,
                    serde_json::json!({
                        "imported": true,
                        "path": published.to_string_lossy(),
                        "model_id": manifest.model_id,
                        "dimension": manifest.dimension,
                        "license": manifest.license,
                        "files": manifest.files.len(),
                    }),
                    0,
                    &protocol::Page::default(),
                    &[],
                    request_id,
                    RetrievalMode::Lexical,
                );
                Ok(protocol::Outcome::Success)
            }
            #[cfg(not(feature = "semantic-candle"))]
            {
                // Default build: import is refused honestly (capability_not_supported)
                // rather than staged silently — operators rebuild with the feature.
                let _ = dir;
                let _ = cache;
                Err(CliError(ProtocolError::new(
                    CanonicalCode::CapabilityNotSupported,
                    "model import requires a binary built with --features semantic-candle \
                     (default build stays lexical-only; rebuild with the feature to import)",
                )))
            }
        }
        "status" => {
            let paths = platform_paths()?;
            let cache = paths.get("cache").and_then(|v| v.as_str()).unwrap_or("");
            #[cfg(feature = "semantic-candle")]
            {
                let dir = agent_session_grep_application::candle_embedding::default_model_dir(
                    std::path::Path::new(cache),
                );
                let (present, verified, detail) =
                    match agent_session_grep_application::candle_embedding::read_and_verify_bundle(
                        &dir,
                    ) {
                        Ok(m) => (
                            true,
                            true,
                            serde_json::json!({
                                "model_id": m.model_id,
                                "dimension": m.dimension,
                                "license": m.license,
                                "files": m.files.len(),
                                "path": dir.to_string_lossy(),
                            }),
                        ),
                        Err(e) => (
                            dir.exists(),
                            false,
                            serde_json::json!({
                                "path": dir.to_string_lossy(),
                                "error": e.to_string(),
                            }),
                        ),
                    };
                emit_result(
                    "model.status",
                    mode,
                    protocol::Outcome::Success,
                    serde_json::json!({
                        "feature": "semantic-candle",
                        "present": present,
                        "verified": verified,
                        "detail": detail,
                    }),
                    0,
                    &protocol::Page::default(),
                    &[],
                    request_id,
                    RetrievalMode::Lexical,
                );
                Ok(protocol::Outcome::Success)
            }
            #[cfg(not(feature = "semantic-candle"))]
            {
                let _ = cache;
                emit_result(
                    "model.status",
                    mode,
                    protocol::Outcome::Success,
                    serde_json::json!({
                        "feature": null,
                        "present": false,
                        "verified": false,
                        "detail": {
                            "note": "default build has no semantic-candle feature; \
                                     lexical/bigram-hash remains the only vector backend"
                        },
                    }),
                    0,
                    &protocol::Page::default(),
                    &[],
                    request_id,
                    RetrievalMode::Lexical,
                );
                Ok(protocol::Outcome::Success)
            }
        }
        "" => Err(CliError::usage(
            "model requires a subcommand: import | status",
        )),
        other => Err(CliError::usage(format!(
            "unknown model subcommand `{other}` (expected import|status)"
        ))),
    }
}

fn platform_paths() -> Result<serde_json::Value, CliError> {
    platform_paths_impl()
}

#[cfg(windows)]
// 数据兼容性：目录名 `AgentSessions` 刻意保留旧名——data-root 布局（config/data/
// cache/logs）与既有安装共享，改名会破坏已存在 data root 的路径查找。
fn platform_paths_impl() -> Result<serde_json::Value, CliError> {
    let roaming = std::env::var_os("APPDATA")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| CliError::usage("APPDATA environment variable is not set"))?;
    let local = std::env::var_os("LOCALAPPDATA")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| CliError::usage("LOCALAPPDATA environment variable is not set"))?;
    Ok(serde_json::json!({
        "config": roaming.join("AgentSessions").join("config.toml").to_string_lossy(),
        "data":   local.join("AgentSessions").join("data").to_string_lossy(),
        "cache":  local.join("AgentSessions").join("cache").to_string_lossy(),
        "logs":   local.join("AgentSessions").join("logs").to_string_lossy(),
    }))
}

#[cfg(target_os = "macos")]
// 数据兼容性：目录名 `AgentSessions` 刻意保留旧名（见 windows 分支注释）。
fn platform_paths_impl() -> Result<serde_json::Value, CliError> {
    let home = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| CliError::usage("HOME environment variable is not set"))?;
    let lib = home.join("Library");
    let support = lib.join("Application Support").join("AgentSessions");
    Ok(serde_json::json!({
        "config": support.join("config.toml").to_string_lossy(),
        "data":   support.join("data").to_string_lossy(),
        "cache":  lib.join("Caches").join("AgentSessions").to_string_lossy(),
        "logs":   lib.join("Logs").join("AgentSessions").to_string_lossy(),
    }))
}

#[cfg(all(unix, not(target_os = "macos")))]
// 数据兼容性：目录名 `agentsessions`（`.config/agentsessions`、
// `.local/share/agentsessions` 等）刻意保留旧名——data-root 布局与既有安装共享。
fn platform_paths_impl() -> Result<serde_json::Value, CliError> {
    let home = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| CliError::usage("HOME environment variable is not set"))?;
    let config_base = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| home.join(".config"));
    let data_base = std::env::var_os("XDG_DATA_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| home.join(".local").join("share"));
    let cache_base = std::env::var_os("XDG_CACHE_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| home.join(".cache"));
    Ok(serde_json::json!({
        "config": config_base.join("agentsessions").join("config.toml").to_string_lossy(),
        "data":   data_base.join("agentsessions").to_string_lossy(),
        "cache":  cache_base.join("agentsessions").to_string_lossy(),
        "logs":   data_base.join("agentsessions").join("logs").to_string_lossy(),
    }))
}

#[cfg(not(any(windows, unix)))]
// 数据兼容性：目录名 `.agentsessions` 刻意保留旧名（见 windows 分支注释）。
fn platform_paths_impl() -> Result<serde_json::Value, CliError> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .ok_or_else(|| CliError::usage("cannot resolve home directory"))?;
    let base = home.join(".agentsessions");
    Ok(serde_json::json!({
        "config": base.join("config.toml").to_string_lossy(),
        "data":   base.join("data").to_string_lossy(),
        "cache":  base.join("cache").to_string_lossy(),
        "logs":   base.join("logs").to_string_lossy(),
    }))
}

/// 顶层帮助文本。与 --version 一样走协议出口：裸 println! 会在下游提前关管道时
/// panic（exit 101 + stderr 污染），违反 CONTRACT §6 的 EPIPE 静默 exit 0。
fn help_text() -> String {
    format!(
        "{name} {version}
AI coding-agent history search engine（本地 AI 编程会话历史搜索）。

快速上手（新手从这里开始）:
    agent-session-grep config paths                 查看数据默认放哪里
    agent-session-grep providers                    查看 Provider 成熟度与能力
    agent-session-grep --db <库路径> search 关键词    搜索历史会话
    agent-session-grep --db <库路径> show <命中ID>   看一条命中的正文
    agent-session-grep --db <库路径> context <会话ID> 展开一个会话的上下文
数据流：search 返回命中消息 → show <msg_id> 看正文 → context <ses_id> 看整个会话。

USAGE:
    agent-session-grep --db <path> <COMMAND> [ARGS]
    agent-session-grep doctor [--db <path>]
    agent-session-grep --help | --version

COMMANDS:
    ingest <file>          解析单个 transcript 文件并入库（只读源）
    sync <file>...          原子扫描多个 transcript 文件；无变化时不生成新 generation
    sync --discover          自动发现各 provider 数据根下的源并同步（jsonl/json/db；只读源）
    relocate --provider <id> --from <old-root> --to <new-root>
                             预览一个显式安装迁移（默认只读；--apply 才提交）
    index <id-fact> <text> 直接写入一条 catalog + 索引（切片期写入入口）
    index rebuild          从权威 catalog 全量重投影 FTS 索引（维护命令）
    index embeddings       从权威 catalog 构建语义向量索引（semantic/hybrid 检索前置）
    index purge-activities 修剪孤儿工具活动行（无 catalog 消息的活动/悬空 claim；维护命令）
    search <query>         全文检索，按相关性降序返回命中（支持分页/预算/过滤 flag）
    handoff <query>        检索并为查询生成 handoff pack（原文证据 + 建议命令；dry-run）
    get-message <msg-id>   返回命中消息及其同会话主线邻居（--session/--around）
    get-session-resume <ses-id> 返回只读 Resume Metadata（Provider Session ID / Original Working Directory）
    resume <ses-id>        预览恢复命令（默认 dry-run；--yes 才实际执行）
    hook <event>           Claude Code Hook 集成（默认关闭；--enable 才注入历史）
    get <wire-id>          按实体 id 取回原始 payload
    show <wire-id>         按实体 id 取回并归一化展示（role/text 结构）
    list [limit]           稳定排序列出 catalog 实体（默认 20；支持分页/预算 flag）
    context <ses-id>       装配会话上下文：分支消息链 + 证据区间
    status                 报告 catalog 实体总数
    mcp                    启动 stdio MCP 服务（JSON-RPC 2.0；stdout 只输出 MCP frame）
    tui                    交互式只读浏览（Preview；需要交互式终端）
    serve                  启动 loopback HTTP 服务 + 嵌入式 Web UI（--port <n>；仅 loopback）
    doctor                 环境自检（可选 --db 校验存储可打开）
    providers              报告 Provider 成熟度、路线目标与逐字段能力
    config paths           报告当前平台的 config/data/cache/logs 路径
    model import|status    本地 embedding 模型缓存（永不联网；import 需 semantic-candle 构建）

PAGINATION / BUDGET (search, list):
    --cursor <token>       上一页 envelope `page.next_cursor` 的续读令牌
    --max-items <n>        页大小上限（同时作为响应条目预算）
    --max-bytes <n>        响应字节预算（最低 4096）

RELOCATION:
    relocate --provider <id> --from <old-root> --to <new-root>
                             只读生成迁移计划；不创建或修改 catalog、源文件或备份
    relocate ... --apply --plan <opaque-plan> --backup <new-file>
                             校验新鲜计划、备份 catalog 后原子提交位置映射
    --alias-ttl-days <n>     退休位置兼容期（默认 90，范围 1..365）

FILTER (search):
    --provider claude|claude-code|codex  限定 provider（可重复，多个取值按 OR 合并）
    --since <time>         起始时间（含）；RFC3339/ISO-8601 绝对值或 1h/1d/1w 相对量
    --until <time>         结束时间（不含）；语法同 --since
    --repo <slug>          限定会话仓库（host/owner/name 三段 slug，逐字相等；
                           与 status 的 repos 清单一致；无仓库身份的会话被排除）
    --include-system       默认排除 system/developer 角色消息；加此旗标恢复
    --group-by-session     按会话归并：每会话保留最高分命中并附 occurrences 计数

FACETS (search，结构化过滤；默认不过滤，输出与旧版一致):
    --main-only            只看主线消息（排除 sidechain）
    --subagent-only        只看 subagent（sidechain）消息；与 --main-only 互斥
    --include-sidechain    显式包含 sidechain（默认值；不与上述两者并用）
    --tool-kind <kind>     只保留做过 file|command|web|query|unknown 工具调用的消息
    --tool-name <name>     只保留用过该工具（逐字相等）的消息

GET MESSAGE:
    --session <ses-id>     共享消息的所属会话；有歧义时必须指定
    --around <n>           主线两侧各返回 n 条邻居（默认 0，仅锚点）
    --max-items <n>        返回消息条数预算
    --max-bytes <n>        响应字节预算（最低 4096）

CONTEXT:
    --policy mainline|full 分支策略（默认 mainline：排除 sidechain 沿 parent 链）
    --level raw|talks|sessions 结构层级（默认 raw：纯消息链；talks 按用户消息分组；sessions 结构概览）
    --max-messages <n>     消息条数预算
    --max-bytes <n>        响应字节预算

GLOBAL（全局 flag 放在命令名之前；子命令 flag 如 --max-items 放在命令名之后）:
    --db <path>            SQLite 数据存储路径（doctor/help/version/config paths 除外必需）
    --output human|json|jsonl  输出模式（默认 human：人类可读文本；json/jsonl 为协议 envelope）
    --robot                等价 --output json，无颜色/进度（stdout 只输出协议）
    --no-color             接受但无效果：human 输出本就不着色（供 NO_COLOR 习惯的调用方）
    --request-id <id>      robot 调用方关联 id，原样回显于每个 frame（A-Za-z0-9._:- 计 1-128 字符）
    --offline              拒绝任何需要联网的显式操作（fail-closed；当前所有命令本地执行，本 flag 是稳定显式模式，doctor/hook 会如实上报）
    -h, --help             打印本帮助
    -V, --version          打印版本

术语速记:
    generation       第 N 次入库（数据每更新一次 +1）
    cursor           翻页令牌（结果多于一页时用来取下一页）
    wire-id          实体 ID（msg_v1_ 消息 / ses_v1_ 会话 / doc_v1_ 文档）
    score            相关度分数（越高越相关，按分数降序排列）

EXIT CODES:
    0 成功；10 部分成功（预算截断，结果可用但不完整）；其余见 error catalog",
        name = env!("CARGO_PKG_NAME"),
        version = env!("CARGO_PKG_VERSION"),
    )
}

/// 帮助/版本统一出口（ADR-0006）：human 逐行打印文本到 stdout；json/jsonl/robot
/// 输出单个 success envelope（jsonl 即单帧），`--request-id` 原样回显。所有
/// help 路径都发生在 --db 解析与存储打开之前，无任何文件副作用。
fn emit_help(command: &str, text: &str, mode: protocol::OutputMode, request_id: Option<&str>) {
    emit_help_payload(
        command,
        serde_json::json!({ "help_text": text }),
        text,
        mode,
        request_id,
    );
}

/// 版本统一出口：与 [`emit_help`] 同构，版本串放进 `data.version`。
fn emit_version(command: &str, text: &str, mode: protocol::OutputMode, request_id: Option<&str>) {
    emit_help_payload(
        command,
        serde_json::json!({ "version": text }),
        text,
        mode,
        request_id,
    );
}

/// [`emit_help`] / [`emit_version`] 的公共实现。stdout 写入统一走
/// [`protocol::write_stdout_line`]（EPIPE 静默 exit 0，CONTRACT §6）。
fn emit_help_payload(
    command: &str,
    data: serde_json::Value,
    text: &str,
    mode: protocol::OutputMode,
    request_id: Option<&str>,
) {
    match mode {
        protocol::OutputMode::Human => {
            for line in text.lines() {
                protocol::write_stdout_line(line);
            }
        }
        protocol::OutputMode::Json | protocol::OutputMode::Jsonl => {
            protocol::write_stdout_line(&help_envelope(command, data, request_id));
        }
    }
}

/// 机器模式下 help/version 的 envelope（json/jsonl 同形：单个 success envelope，
/// jsonl 即单帧；`data` 携带帮助文本/版本串；`--request-id` 原样回显）。
fn help_envelope(command: &str, data: serde_json::Value, request_id: Option<&str>) -> String {
    protocol::success_envelope(
        command,
        protocol::Outcome::Success,
        data,
        0,
        &protocol::Page::default(),
        &[],
        request_id,
        RetrievalMode::default(),
        &RedactionStatus::default(),
    )
}

/// 全部子命令名，与 `--help` 的 COMMANDS 段同序。
///
/// 单一事实来源：[`known_subcommand`] 的识别与 dispatch 的 `unknown subcommand`
/// 提示都读这一份清单。两处各自手写会漂移——新增命令只改了一处时，帮助里
/// 存在的命令会被"可用命令"提示否认（新手照提示改写反而更错）。
const KNOWN_SUBCOMMANDS: &[&str] = &[
    "ingest",
    "sync",
    "relocate",
    "index",
    "search",
    "handoff",
    "get-message",
    "get-session-resume",
    "resume",
    "hook",
    "get",
    "show",
    "list",
    "context",
    "status",
    "mcp",
    "tui",
    "serve",
    "doctor",
    "providers",
    "config",
    "model",
];

/// 是否为已知子命令（用于子命令 `--help` 拦截与 unknown-subcommand 报错提示）。
fn known_subcommand(cmd: &str) -> bool {
    KNOWN_SUBCOMMANDS.contains(&cmd)
}

/// 子命令级帮助文本：渲染该命令的签名、flag 与一个真实示例。由 help/version
/// 提前拦截阶段（ADR-0006）在 `<cmd> --help|-h`（含 `index rebuild --help`）时
/// 触发——顶层 --help 只给全局概览，子命令帮助给单命令的用法。
fn subcommand_help_text(cmd: &str) -> &'static str {
    match cmd {
        "search" => {
            "search <query>：全文检索历史会话，按相关性降序返回命中。\n\
                     示例：agent-session-grep --db <path> search 配置备份\n\
                     flag（放子命令后）：--max-items <n> 页大小、--cursor <token> 翻页、--max-bytes <n> 预算；\n\
                     过滤：--provider claude|claude-code|codex（可重复，OR）、--since/--until <RFC3339 或 1h|1d|1w>（半开区间 [since, until)）、\n\
                     --repo <host/owner/name>（会话仓库 slug，逐字相等；与 status 的 repos 清单一致）；\n\
                     检索模式：--mode lexical|semantic|hybrid（默认 lexical）。semantic/hybrid 需先跑 `index embeddings`；\n\
                     向量索引未就绪时结果标注 retrieval_mode=lexical_fallback 并给出 warning，绝不静默降级；\n\
                     --include-system（默认排除 system/developer 角色消息）、--group-by-session（按会话归并并附 occurrences）；\n\
                     结构化过滤：--main-only 只看主线（排除 sidechain）、--subagent-only 只看 subagent 消息、\n\
                     --tool-kind file|command|web|query|unknown 只保留做过该种工具调用的消息、\n\
                     --tool-name <名字> 只保留用过该工具（逐字相等）的消息（--main-only 与 --subagent-only 互斥）"
        }
        "get-message" => {
            "get-message <msg-id>：返回一个消息及其同会话主线邻居。\n\
                          示例：agent-session-grep --db <path> get-message msg_v1_... --session ses_v1_... --around 2\n\
                          flag：--session <ses-id>、--around <n>、--max-items <n>、--max-bytes <n>"
        }
        "handoff" => {
            "handoff <query>：为查询生成 handoff pack（handoff-pack/v1）。\n\
                     示例：agent-session-grep --db <path> handoff 配置备份\n\
                     检索命中后组装：原文证据（evidence）与推断（inference）严格分栏；\n\
                     deterministic 默认（无 LLM 调用）；dry-run——只输出 pack 与建议命令，不注入任何 agent。\n\
                     flag：--max-evidence <n> 证据条数上限（默认 20）、--max-tokens <n> token 预算（默认 8000）、\n\
                     --max-bytes <n> 序列化字节预算（默认 2000000）；截断时 exit 10"
        }
        "get" => {
            "get <wire-id>：按实体 ID 取回原始 payload。\n\
                  示例：agent-session-grep --db <path> get msg_v1_..."
        }
        "get-session-resume" => {
            "get-session-resume <ses-id>：返回一个会话的只读 Resume Metadata。\n\
                  示例：agent-session-grep --db <path> get-session-resume ses_v1_...\n\
                  固定字段（缺失为 null）：provider_session_id、original_working_directory；\n\
                  不会生成/执行任何恢复命令，也不暴露 transcript 路径。"
        }
        "resume" => {
            "resume <ses-id>：预览（默认）或执行会话的原地恢复。\n\
                  示例：agent-session-grep --db <path> resume ses_v1_...\n\
                  默认 dry-run——打印将执行的完整命令（provider/cwd/session id）并退出；\n\
                  确认无误后加 --yes 才在原工作目录实际启动 provider 进程。\n\
                  首次使用 resume 强制只预览一次（持久标记），--yes 从第二次起才生效；\n\
                  未核验恢复命令的 provider 报 available:false，绝不编造命令。"
        }
        "hook" => {
            "hook <session-start|user-prompt-submit>：Claude Code Hook 集成（默认关闭）。\n\
                  示例：echo '{\"prompt\":\"数据库迁移\"}' | agent-session-grep --db <path> hook user-prompt-submit --enable\n\
                  从 stdin 读 hook payload，检索历史后把 hookSpecificOutput 契约裸写到 stdout；\n\
                  Claude Code 直接读 stdout 当协议，所以本命令不套 envelope（--robot/--output 无效），\n\
                  不加 --enable 时 stdout 一个字节都不写（不注入任何历史）；运行事实走 stderr；\n\
                  flag：--enable 启用注入、--max-tokens <n> 预算（默认 2000）；\n\
                  --provider claude|claude-code|codex（可重复，OR 限定 provider）、--decay-days <n>（只注入最近 N 天）、\n\
                  --repo <host/owner/name>（只注入该仓库的历史；与 search --repo 同语义）；\n\
                  注入文本经跨边界脱敏（ADR-0009）；全局 --offline 时如实上报 offline 字段。"
        }
        "show" => {
            "show <wire-id>：按实体 ID 取回并展示（role/text/时间戳）。\n\
                   示例：agent-session-grep --db <path> show msg_v1_...\n\
                   从 search 命中或 show 输出里的 session 字段，可继续用 context 展开会话。"
        }
        "list" => {
            "list [limit]：按稳定序列出实体（默认 20）。\n\
                   示例：agent-session-grep --db <path> list 50\n\
                   flag（放子命令后）：--cursor <token> 翻页"
        }
        "context" => {
            "context <ses-id>：装配一个会话的完整上下文（消息链 + 证据区间）。\n\
                      示例：agent-session-grep --db <path> context ses_v1_...\n\
                      flag：--policy mainline|full、--level raw|talks|sessions、--max-messages <n>、--max-bytes <n>"
        }
        "status" => {
            "status：报告当前库的实体总数与 generation。\n\
                     示例：agent-session-grep --db <path> status"
        }
        "sync" => {
            "sync <file>...：原子扫描一个或多个 transcript 文件入库；无变化不写库。\n\
                   sync --discover：扫描已登记活动位置及 12 个默认 provider 数据根（~/.claude/projects、~/.codex/sessions 等，逐根登记扩展名 jsonl/json/db）下的源并同步。\n\
                   示例：agent-session-grep --db <path> --robot sync 会话.jsonl\n\
                   示例：agent-session-grep --db <path> sync --discover\n\
                   约束：单个 transcript 文件应只包含一个会话；检测到多个 sessionId 时仍归属首个会话，并在 warnings 报告。\n\
                   提示：接受任何能被 provider 注册表识别的 transcript 文件（.jsonl / .json / .md / SQLite .db），不接受目录；--discover 会递归扫描 provider 数据根。"
        }
        "relocate" => {
            "relocate --provider <id> --from <old-root> --to <new-root>：\n\
                       默认只读生成显式安装迁移计划，不创建或修改 catalog、源文件或备份。\n\
                       应用计划：追加 --apply --plan <opaque-plan> --backup <new-file>；\n\
                       先由用户移动源目录；本命令只重连索引位置，不移动或改写源文件。\n\
                       旧根按已存位置匹配（可以不存在）；新根必须是本机绝对路径。\n\
                       --provider 接受 providers 中的规范 id 与 claude 别名。\n\
                       计划有效期为 15 分钟；应用时必须沿用预览的映射和兼容期，备份文件必须尚不存在。\n\
                       --alias-ttl-days <n> 可用于预览和应用（1..365，默认 90）；兼容期到期不影响原会话 ID 查询。\n\
                       旧版 catalog 须先显式执行 index rebuild 升级；同一映射重复执行无变化。\n\
                       仅 CLI 支持迁移；MCP/Web 保持只读。输出只含 opaque plan、计数与 generation。"
        }
        "ingest" => {
            "ingest <file>：解析单个 transcript 文件入库（.jsonl / .json / .md / SQLite .db）。\n\
                     示例：agent-session-grep --db <path> ingest 会话.jsonl\n\
                     约束：单个 transcript 文件应只包含一个会话；检测到多个 sessionId 时仍归属首个会话，并输出诊断 warning。"
        }
        "index" => {
            "index <id-fact> <text>：写入一条 catalog + 索引。\n\
                    index rebuild：从权威 catalog 重建全文（FTS）索引；\n\
                    index embeddings：从权威 catalog 构建语义向量索引（semantic/hybrid 检索前置，需 semantic-candle 构建的二进制）。\n\
                   示例：agent-session-grep --db <path> --robot index rebuild"
        }
        "doctor" => {
            "doctor [--db <path>]：环境自检；带 --db 时校验存储可打开、报 schema。\n\
                     示例：agent-session-grep --db <path> doctor"
        }
        "mcp" => {
            "mcp：启动 stdio MCP 服务（供 Claude Code 等 AI 宿主调用）。\n\
                  示例：agent-session-grep --db <path> mcp"
        }
        "tui" => {
            "tui：交互式只读浏览（Preview）。需要交互式终端。\n\
                   tui --snapshot-json <query>：headless 结构投影（供 release 一致性 harness 跨入口比对）。\n\
                   示例：agent-session-grep --db <path> tui"
        }
        "serve" => {
            "serve：启动 loopback HTTP 服务 + 嵌入式 Web UI（仅 127.0.0.1）。\n\
                    每次启动生成随机 bearer token；浏览器打开终端打印的 URL（含 token）即可访问。\n\
                    本 release 仅 loopback：--lan 为 capability_not_supported（绝不暴露局域网）。\n\
                    flag：--port <n>（可选，默认 0 = 随机端口）。\n\
                    示例：agent-session-grep --db <path> serve"
        }
        "providers" => {
            "providers：报告当前 Provider 能力矩阵（成熟度事实、路线目标与逐字段能力）。\n\
                         示例：agent-session-grep --robot providers\n\
                         数据来自唯一能力矩阵；deferred provider 的 maturity_target 为 null。"
        }
        "config" => {
            "config paths：报告当前平台的 config/data/cache/logs 路径。\n\
                     示例：agent-session-grep config paths"
        }
        "model" => {
            "model import|status：本地 embedding 模型缓存管理（永不联网）。\n\
                     model import --dir <bundle>  校验 SHA-256 后原子发布到 cache/models/...\n\
                     model status                 报告默认 E5 bundle 是否已导入且校验通过\n\
                     需要 `--features semantic-candle` 构建的二进制才能 import；默认构建仅 status。\n\
                     示例：agent-session-grep model status"
        }
        _ => "运行 agent-session-grep --help 查看完整命令列表。",
    }
}

/// doctor：最小环境自检。报告版本；若给了 --db，尝试打开存储并报告 schema。
/// `offline` 作为诊断字段原样上报（design D5）：`--offline` 是稳定显式模式，
/// 当前没有任何命令需要联网，doctor 如实反映调用方声明的 offline 意图。
fn doctor(
    args: &[String],
    mode: protocol::OutputMode,
    request_id: Option<&str>,
    offline: bool,
) -> Result<protocol::Outcome, CliError> {
    let started = std::time::Instant::now();
    // 多余位置参数是用法错误，不静默忽略（与其它子命令一致）。
    if bare_positionals(args).len() > 1 {
        return Err(CliError::usage("doctor takes no positional arguments"));
    }
    // 与 parse_db_flag 共用同一 --db 取值守卫：`doctor --db --robot` 不得把
    // --robot 当路径（会造出同名文件），重复 --db 是用法错误（R8.1/R8.2）。
    // doctor 的 --db 允许在命令名之后（`doctor [--db <path>]`），故整串扫描。
    let db_opt = extract_db_flag_anywhere(args)?;
    let data = match db_opt {
        None => serde_json::json!({
            "tool": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "db": "not-checked",
            "schema": null,
            "offline": offline,
            // 构建事实：semantic-candle 是否编译进本二进制（默认构建为 null）。
            "semantic_feature": semantic_feature_flag(),
            // schema v12 事实：tool_activities/tool_activity_membership 表随
            // 本二进制管理的每个 catalog 落库，未指定 --db 也成立。
            "tool_activity_storage": true,
            // schema v15 事实：usage_events/usage_event_membership 表同上述
            // 落库（未指定 --db 也成立）。
            "usage_storage": true,
            // 构建事实：本二进制期望的索引投影版本（v17）。未指定 --db 时
            // 无库可比，version/stale 显式为 null——不猜。
            "index_projection_expected": INDEX_PROJECTION_VERSION,
            "index_projection_version": null,
            "index_projection_stale": null,
            // 新手会误以为 db: not-checked 是自检失败（10 角色体验测试缺陷）。
            // 加一行白话提示，说明如何真正校验。
            "hint": "未指定数据库：以上仅检查了环境。运行 doctor --db <path> 可校验数据库与 schema。",
        }),
        Some(path) => {
            let store = SqliteStore::open(&path).map_err(ProtocolError::from)?;
            doctor_store_data(&store, offline)?
        }
    };
    let duration_ms = started.elapsed().as_millis() as u64;
    emit_result(
        "doctor",
        mode,
        protocol::Outcome::Success,
        data,
        duration_ms,
        &protocol::Page::default(),
        &[],
        request_id,
        RetrievalMode::Lexical,
    );
    Ok(protocol::Outcome::Success)
}

/// `doctor` 的库内事实投影：CLI `doctor --db` 与 MCP `doctor` 工具的单一来源。
///
/// 两侧曾各写一份 `json!`，MCP 那份漏掉 `offline` / `semantic_feature` /
/// `tool_activity_storage` / `usage_storage` / `orphaned_usage_*` 六个字段——
/// 同一个诊断问题经 MCP 问会得到严格更弱的答案，而没有任何测试比对两侧。
/// 新增字段只能改这一处。
pub(crate) fn doctor_store_data(
    store: &SqliteStore,
    offline: bool,
) -> Result<serde_json::Value, ProtocolError> {
    let _snapshot = store.begin_read_snapshot()?;
    let schema = store.schema_version()?;
    // generation 与待收敛 intent 数是 durable outbox 中断恢复与一致性的只读证据。
    let generation = store.active_generation()?;
    let interrupted = store.interrupted_batch_count()?;
    // 工具活动保留策略证据（v12）：孤儿投影行计数。>0 时用
    // `index purge-activities` 确定性修剪（catalog/FTS 不受影响）。
    let (orphaned_tool_activities, orphaned_activity_memberships) =
        store.orphaned_activity_counts()?;
    // usage 投影保留策略证据（v15）：孤儿投影行计数；同一修剪命令
    // `index purge-activities` 同事务清理。
    let (orphaned_usage_events, orphaned_usage_memberships) = store.orphaned_usage_counts()?;
    // 索引投影版本事实（v17）：现存 FTS 词元流由哪个投影变换写成。
    // stale ⇒ 该库的词元与本二进制的查询词元不可比，搜索按契约
    // fail-closed；`index rebuild` 或任意写路径 sync 会重投影收敛。
    let index_projection_version = store.index_projection_version()?;
    let index_projection_stale = index_projection_version != i64::from(INDEX_PROJECTION_VERSION);
    Ok(serde_json::json!({
        "tool": env!("CARGO_PKG_NAME"),
        "version": env!("CARGO_PKG_VERSION"),
        "db": "ok",
        "schema": schema,
        "offline": offline,
        "semantic_feature": semantic_feature_flag(),
        // 打开的库已被迁移到本二进制的 schema v12，工具活动存储存在。
        "tool_activity_storage": true,
        // schema v15：usage 投影存在。
        "usage_storage": true,
        "generation": generation,
        "interrupted_batches": interrupted,
        "orphaned_tool_activities": orphaned_tool_activities,
        "orphaned_activity_memberships": orphaned_activity_memberships,
        "orphaned_usage_events": orphaned_usage_events,
        "orphaned_usage_memberships": orphaned_usage_memberships,
        "index_projection_version": index_projection_version,
        "index_projection_expected": INDEX_PROJECTION_VERSION,
        "index_projection_stale": index_projection_stale,
    }))
}

/// 已知 flag 名全集（前缀位置可出现的旗标）。取值守卫用它拒绝 `--db --robot`
/// 这类把 flag 当取值的写法——否则 `parse_db_flag` 会造出名为 `--robot` 的文件。
fn is_known_flag_name(token: &str) -> bool {
    matches!(
        token,
        "--db"
            | "--output"
            | "--request-id"
            | "--robot"
            | "--no-color"
            | "--help"
            | "-h"
            | "--version"
            | "-V"
            | "--cursor"
            | "--max-items"
            | "--max-bytes"
            | "--max-messages"
            | "--max-evidence"
            | "--max-tokens"
            | "--policy"
            | "--level"
            | "--provider"
            | "--since"
            | "--until"
            | "--repo"
            | "--session"
            | "--around"
            | "--snapshot-json"
            | "--discover"
            | "--offline"
            | "--main-only"
            | "--subagent-only"
            | "--include-sidechain"
            | "--tool-kind"
            | "--tool-name"
            | "--from"
            | "--to"
            | "--alias-ttl-days"
            | "--plan"
            | "--backup"
            | "--apply"
    )
}

/// 从前缀位置抽出 `--db <path>`；缺省返回 None。带值 flag 的取值跳过。
///
/// 取值缺失、取值是已知 flag 名（`--db --robot` 会造出名为 `--robot` 的文件）、
/// 重复出现（`--db a --db b` 不再静默 last-wins）都是用法错误（R8.1/R8.2）。
/// doctor 与 [`parse_db_flag`] 共用同一守卫，避免 doctor 的 --db 绕过校验。
fn extract_db_flag(args: &[String]) -> Result<Option<String>, CliError> {
    extract_db_flag_impl(args, true)
}

/// doctor 专用变体：`--db` 允许跟在命令名之后（`doctor [--db <path>]`），
/// 扫描全部 token 而非只扫前缀；取值守卫与 [`extract_db_flag`] 一致。
fn extract_db_flag_anywhere(args: &[String]) -> Result<Option<String>, CliError> {
    extract_db_flag_impl(args, false)
}

fn extract_db_flag_impl(args: &[String], prefix_only: bool) -> Result<Option<String>, CliError> {
    let mut db = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if !a.starts_with('-') {
            if prefix_only {
                break; // 已到命令名：之后的 token 不当全局 flag 解析
            }
            continue; // doctor 的扫描：非 flag token 直接跳过
        }
        if a == "--db" {
            let value = it
                .next()
                .ok_or_else(|| CliError::usage("--db requires a path"))?;
            if is_known_flag_name(value) {
                return Err(CliError::usage(format!(
                    "--db requires a path (got flag {value}); --db <path> must precede other flags"
                )));
            }
            if value.is_empty() {
                return Err(CliError::usage(
                    "--db requires a non-empty path (an empty path silently uses SQLite's private temporary database)",
                ));
            }
            if db.is_some() {
                return Err(CliError::usage("duplicate --db flag"));
            }
            db = Some(value.clone());
        }
        match a.as_str() {
            "--output" | "--request-id" | "--cursor" | "--max-items" | "--max-bytes"
            | "--max-messages" | "--max-evidence" | "--max-tokens" | "--policy" | "--level"
            | "--provider" | "--since" | "--until" | "--session" | "--around" | "--tool-kind"
            | "--tool-name" | "--from" | "--to" | "--alias-ttl-days" | "--plan" | "--backup" => {
                it.next();
            }
            _ => {}
        }
    }
    Ok(db)
}

/// 从 `--db <path>` 抽出数据库路径，返回其余参数。
///
/// flag 只在前缀位置（第一个裸参数即命令名之前）识别；命令名之后的 token
/// 原样进 `rest`，由 dispatch 的 `extract_flag` 挑出命令级 flag——这样查询文本
/// 恰等于 `--help`/`--robot`/`--output` 等 flag 名时不会被吞掉。
///
/// `--db` 的取值守卫（缺值、取值为已知 flag、重复）统一在 [`extract_db_flag`]
/// 完成（R8.1/R8.2）。
fn parse_db_flag(args: &[String]) -> Result<(String, Vec<String>), CliError> {
    let db = extract_db_flag(args)?;
    let mut rest = Vec::new();
    let mut seen_command = false;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if seen_command {
            rest.push(a.to_string());
            continue;
        }
        match a.as_str() {
            "--db" | "--output" | "--request-id" => {
                it.next(); // 消费其取值（--db 取值已由 extract_db_flag 校验）
            }
            "--robot" | "--no-color" | "--help" | "-h" | "--version" | "-V" | "--discover"
            | "--offline" => {}
            other => {
                seen_command = true;
                rest.push(other.to_string());
            }
        }
    }
    let db = db.ok_or_else(|| {
        // 缺 --db 是新手第一道坎：报错带两条路——config paths 找默认数据位置、
        // --help 看用法。已经给出子命令（如 `hello`、`search`）时提示它可能不是命令。
        if rest.is_empty() {
            CliError::usage(
                "需要数据库参数 --db <path>。\n\
                 可先运行 `config paths` 查看默认数据位置；运行 `--help` 查看完整用法。",
            )
        } else {
            CliError::usage(format!(
                "需要数据库参数 --db <path>（而且 `{}` 可能不是有效命令）。\n\
                 可先运行 `config paths` 查看默认数据位置；运行 `--help` 查看完整用法。",
                rest[0]
            ))
        }
    })?;
    Ok((db, rest))
}

/// 提取纯位置参数（跳过已知带值 flag 及其取值、已知裸 flag）——供 doctor/config
/// 这类绕过 parse_db_flag 的命令做多余参数校验。
///
/// 未知的 `-` 开头 token 不是已知 flag，按多余位置参数计入（R8.3）：`doctor
/// --bogus` 不能静默丢弃 `--bogus` 后假装成功（exit 0 + db:not-checked）。
fn bare_positionals(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--db" | "--output" | "--request-id" | "--cursor" | "--max-items" | "--max-bytes"
            | "--max-messages" | "--max-evidence" | "--max-tokens" | "--policy" | "--level"
            | "--provider" | "--since" | "--until" | "--session" | "--around" | "--tool-kind"
            | "--tool-name" | "--from" | "--to" | "--alias-ttl-days" | "--plan" | "--backup" => {
                it.next(); // 消费其取值
            }
            "--robot" | "--no-color" | "--help" | "-h" | "--version" | "-V" | "--discover"
            | "--offline" => {}
            s => out.push(s.to_string()),
        }
    }
    out
}

/// `--offline` fail-closed 网关（design D5）：未来需要联网的能力（模型下载、
/// 外部 Embedding API、telemetry）在尝试连接前必须先经过本网关。offline 下
/// 以 `capability_not_supported` 拒绝，绝不静默降级。当前没有任何能力需要
/// 联网，网关由未来命令调用、测试直接覆盖。
fn offline_capability_gate(offline: bool, capability: &str) -> Result<(), CliError> {
    if offline {
        return Err(CliError(ProtocolError::new(
            CanonicalCode::CapabilityNotSupported,
            format!("capability {capability} requires network; refused under --offline"),
        )));
    }
    Ok(())
}

/// Parsed relocation scalars. Validation precedes catalog opens, including the
/// write open which can otherwise create or migrate a catalog on invalid input.
struct RelocationArgs {
    provider: String,
    from: String,
    to: String,
    alias_ttl_days: u32,
    apply: Option<RelocationApplyArgs>,
}

struct RelocationApplyArgs {
    plan: String,
    backup: String,
}

/// Reuse the command flag extractor, with relocation's single-value contract.
/// Messages name the flag, never its private path or opaque token value.
fn relocation_flag(args: &mut Vec<String>, name: &str) -> Result<Option<String>, CliError> {
    if let Some(index) = args.iter().position(|arg| arg == name) {
        if args.iter().filter(|arg| *arg == name).count() != 1 {
            return Err(CliError::usage(format!("duplicate {name} flag")));
        }
        let value = args
            .get(index + 1)
            .ok_or_else(|| CliError::usage(format!("{name} requires a value")))?;
        if value.trim().is_empty() || value.starts_with('-') || value.chars().any(char::is_control)
        {
            return Err(CliError::usage(format!(
                "{name} requires a non-empty scalar value"
            )));
        }
    }
    extract_flag(args, name)
}

fn parse_relocation_args(rest: &[String]) -> Result<RelocationArgs, CliError> {
    let mut args = rest.to_vec();
    let provider = relocation_flag(&mut args, "--provider")?
        .ok_or_else(|| CliError::usage("relocate requires --provider <provider>"))?;
    let provider = canonical_search_provider(&provider)
        .ok_or_else(|| {
            CliError::usage("unknown relocation provider; use `providers` to list providers")
        })?
        .as_str()
        .to_string();
    let from = relocation_flag(&mut args, "--from")?
        .ok_or_else(|| CliError::usage("relocate requires --from <old-root>"))?;
    let to = relocation_flag(&mut args, "--to")?
        .ok_or_else(|| CliError::usage("relocate requires --to <new-root>"))?;
    let ttl = relocation_flag(&mut args, "--alias-ttl-days")?
        .map(|value| {
            value.parse::<u32>().map_err(|_| {
                CliError::usage("--alias-ttl-days requires an integer between 1 and 365")
            })
        })
        .transpose()?
        .unwrap_or(DEFAULT_ALIAS_TTL_DAYS);
    validate_alias_ttl_days(ttl).map_err(CliError::usage)?;
    let apply = take_bool_flag(&mut args, "--apply");
    let plan = relocation_flag(&mut args, "--plan")?;
    let backup = relocation_flag(&mut args, "--backup")?;
    no_extra_args(
        &args,
        0,
        "relocate --provider <provider> --from <old-root> --to <new-root> [--alias-ttl-days <1..365>] [--apply --plan <opaque-plan> --backup <new-file>]",
    )?;
    agent_session_grep_application::relocation::validate_root_mapping(&from, &to)
        .map_err(ProtocolError::from_private_port_error)?;
    if !std::path::Path::new(&to).is_absolute() {
        return Err(CliError::usage(
            "relocate requires a host-local absolute destination",
        ));
    }
    let apply = if apply {
        Some(RelocationApplyArgs {
            plan: plan
                .ok_or_else(|| CliError::usage("relocate --apply requires --plan <opaque-plan>"))?,
            backup: backup
                .ok_or_else(|| CliError::usage("relocate --apply requires --backup <new-file>"))?,
        })
    } else {
        if plan.is_some() || backup.is_some() {
            return Err(CliError::usage(
                "--plan and --backup are valid only with relocate --apply",
            ));
        }
        None
    };
    Ok(RelocationArgs {
        provider,
        from,
        to,
        alias_ttl_days: ttl,
        apply,
    })
}

/// Storage owns fingerprints, backup ordering and atomic activation. No source
/// files are moved by this command, and no private inputs enter its result.
fn relocate_command(
    store: &SqliteStore,
    request: &RelocationArgs,
) -> Result<RelocationResult, CliError> {
    let result = match &request.apply {
        Some(apply) => store.apply_relocation(
            &request.provider,
            &request.from,
            &request.to,
            request.alias_ttl_days,
            &apply.plan,
            &apply.backup,
        ),
        None => store.relocation_preview(
            &request.provider,
            &request.from,
            &request.to,
            request.alias_ttl_days,
        ),
    };
    result.map_err(|error| CliError(ProtocolError::from_private_port_error(error)))
}

/// 分发子命令。`store` 同时充当 CatalogStore 与 SearchIndex（同一 SqliteStore）。
/// `db` 用于 resume 首次预览标记的 data-root 定位（与 writer lease 同一根）。
/// `mode`/`request_id` 只喂给需要发协议 frame 的子命令（sync 的 jsonl progress）。
/// `offline` 是 `--offline` 全局 flag 的 fail-closed 网关（design D5）：当前没有任何
/// 子命令需要联网，flag 不改动既有行为；未来需要联网的能力（模型下载、外部
/// Embedding API、telemetry）必须先经过 [`offline_capability_gate`]，在 offline
/// 下以 `capability_not_supported` 拒绝，绝不静默降级。
fn dispatch(
    store: &SqliteStore,
    db: &str,
    rest: &[String],
    mode: protocol::OutputMode,
    request_id: Option<&str>,
    offline: bool,
) -> Result<
    (
        &'static str,
        protocol::Outcome,
        serde_json::Value,
        protocol::Page,
        Vec<String>,
    ),
    CliError,
> {
    let cmd = rest
        .first()
        .ok_or_else(|| CliError::usage("missing subcommand"))?
        .as_str();
    // Design D5 fail-closed gate: 未来需要联网的子命令必须在此登记，并在连接前
    // 经过 [`offline_capability_gate`]。当前没有任何子命令需要联网（模型下载、
    // 外部 Embedding API、telemetry 均未实现），列表为空——offline 是稳定显式
    // 模式，不改动既有命令行为；测试直接覆盖网关语义。
    const NETWORK_REQUIRING_SUBCOMMANDS: &[&str] = &[];
    for network_cmd in NETWORK_REQUIRING_SUBCOMMANDS {
        if cmd == *network_cmd {
            offline_capability_gate(offline, cmd)?;
        }
    }
    match cmd {
        "relocate" => {
            let request = parse_relocation_args(rest)?;
            let command = if request.apply.is_some() {
                "relocate.apply"
            } else {
                "relocate.preview"
            };
            let result = relocate_command(store, &request)?;
            Ok((
                command,
                protocol::Outcome::Success,
                serde_json::to_value(result).map_err(|_| {
                    CliError(ProtocolError::new(
                        CanonicalCode::Internal,
                        "relocation result serialization failed",
                    ))
                })?,
                protocol::Page::default(),
                Vec::new(),
            ))
        }
        // index 是切片期的写入入口：派生一个 Reconstructed 消息 id，写 catalog + 索引。
        // 它不经 Application（Application 首片只暴露读用例），直接用端口写入。
        // `index rebuild` 是维护子命令：从权威 catalog 全量重投影 FTS 索引。
        "index" => {
            if rest.get(1).map(String::as_str) == Some("rebuild") {
                no_extra_args(rest, 1, "index rebuild")?;
                let reindexed = store.rebuild_index().map_err(ProtocolError::from)?;
                let generation = store.active_generation().map_err(ProtocolError::from)?;
                Ok((
                    "index.rebuild",
                    protocol::Outcome::Success,
                    serde_json::json!({
                        "reindexed": reindexed,
                        "generation": generation,
                    }),
                    protocol::Page::default(),
                    Vec::new(),
                ))
            } else if rest.get(1).map(String::as_str) == Some("embeddings") {
                // 语义向量索引构建（#3）：从权威 catalog 重投影 message_vec。
                // 与 `index rebuild` 同级——向量表是 catalog 的投影，不是权威数据，
                // 可随时整表重建。构建后 semantic/hybrid 检索才会离开
                // lexical_fallback。
                no_extra_args(rest, 1, "index embeddings")?;
                let (data, warnings) = build_embeddings(store)?;
                Ok((
                    "index.embeddings",
                    protocol::Outcome::Success,
                    data,
                    protocol::Page::default(),
                    warnings,
                ))
            } else if rest.get(1).map(String::as_str) == Some("purge-activities") {
                // 工具活动保留策略（v12）的维护命令：确定性修剪孤儿活动行
                // （无 catalog 消息的活动、指向不存在活动的 claim）。与
                // `index rebuild` 同一 writer lease/CAS 纪律；无孤儿时不写库。
                // 活动行是 catalog 的投影，修剪绝不触碰 catalog/FTS。
                no_extra_args(rest, 1, "index purge-activities")?;
                let (removed_activities, removed_memberships) = store
                    .purge_orphaned_activities()
                    .map_err(ProtocolError::from)?;
                let generation = store.active_generation().map_err(ProtocolError::from)?;
                Ok((
                    "index.purge-activities",
                    protocol::Outcome::Success,
                    serde_json::json!({
                        "removed_activities": removed_activities,
                        "removed_memberships": removed_memberships,
                        "generation": generation,
                    }),
                    protocol::Page::default(),
                    Vec::new(),
                ))
            } else {
                no_extra_args(rest, 2, "index <id-fact> <text>")?;
                let fact = arg(rest, 1, "index <id-fact> <text>")?;
                let text = arg(rest, 2, "index <id-fact> <text>")?;
                Ok((
                    "index",
                    protocol::Outcome::Success,
                    index_one(store, fact, text)?,
                    protocol::Page::default(),
                    Vec::new(),
                ))
            }
        }
        // ingest 打通 ingestion→storage→search 全链路：读取原始 .jsonl →
        // provider.probe 判定 variant → provider.parse 流式产出 Canonical 消息 →
        // 每条写 catalog + FTS 索引。文件严格只读（RFC-0002 §7）。
        "ingest" => {
            no_extra_args(rest, 1, "ingest <file>")?;
            let path = arg(rest, 1, "ingest <file>")?;
            let (data, warnings) = ingest_file(store, path)?;
            Ok((
                "ingest",
                protocol::Outcome::Success,
                data,
                protocol::Page::default(),
                warnings,
            ))
        }
        // sync 对显式列出的多个源执行同一套只读快照 + staging，并在全部成功后
        // 通过一次 durable batch 提交，避免部分 source 已写入、后续 source 失败。
        // jsonl 模式下逐源发 progress frame（contract §4；--robot/Json 禁 progress）。
        // `--discover` 是 opt-in：遍历各 provider 的数据根自动发现源，不传路径。
        "sync" => {
            let mut args = rest.to_vec();
            let discover = take_bool_flag(&mut args, "--discover");
            let (data, warnings) = if discover {
                // --discover 不接受额外参数（路径由发现填充）。未知 flag 也不能
                // 静默忽略，否则拼写错误会伪装成成功的空发现。
                if args.len() > 1 {
                    return Err(CliError::usage(
                        "sync --discover 不接受路径或额外 flag；路径由 provider 数据根自动发现",
                    ));
                }
                sync_discover(store, mode == protocol::OutputMode::Jsonl, request_id)?
            } else {
                sync_files(
                    store,
                    &args[1..],
                    mode == protocol::OutputMode::Jsonl,
                    request_id,
                )?
            };
            Ok((
                "sync",
                protocol::Outcome::Success,
                data,
                protocol::Page::default(),
                warnings,
            ))
        }
        "search" => {
            let mut args = rest.to_vec();
            let cursor = extract_flag(&mut args, "--cursor")?;
            let max_items = extract_flag(&mut args, "--max-items")?;
            let max_bytes = extract_flag(&mut args, "--max-bytes")?;
            let providers = extract_repeated_flag(&mut args, "--provider")?;
            let since = extract_flag(&mut args, "--since")?;
            let until = extract_flag(&mut args, "--until")?;
            let repo = extract_flag(&mut args, "--repo")?;
            let include_system = take_bool_flag(&mut args, "--include-system");
            let group_by_session = take_bool_flag(&mut args, "--group-by-session");
            // #3 检索模式：--mode lexical|semantic|hybrid；默认 lexical。
            let retrieval_mode = match extract_flag(&mut args, "--mode")?.as_deref() {
                None | Some("lexical") => RetrievalMode::Lexical,
                Some("semantic") => RetrievalMode::Semantic,
                Some("hybrid") => RetrievalMode::Hybrid,
                Some(other) => {
                    return Err(CliError::usage(format!(
                        "--mode must be lexical|semantic|hybrid, got {other}"
                    )));
                }
            };
            // 结构化 facet 过滤（additive；默认无过滤，行为与旧版一致）。
            let main_only = take_bool_flag(&mut args, "--main-only");
            let subagent_only = take_bool_flag(&mut args, "--subagent-only");
            let include_sidechain = take_bool_flag(&mut args, "--include-sidechain");
            let tool_kind = extract_flag(&mut args, "--tool-kind")?;
            let tool_name = extract_flag(&mut args, "--tool-name")?;
            let app = resume_app(store);
            let filters = search_filters_from_flags(
                &providers,
                since.as_deref(),
                until.as_deref(),
                repo.as_deref(),
                app.now_ms(),
            )?;
            let budget = budget_from_flags(max_items.as_deref(), max_bytes.as_deref(), None)?;
            no_extra_args(&args, 1, "search <query>")?;
            let sidechain = match (main_only, subagent_only) {
                (true, true) => {
                    return Err(CliError::usage(
                        "--main-only and --subagent-only are mutually exclusive",
                    ));
                }
                (true, false) => {
                    if include_sidechain {
                        return Err(CliError::usage(
                            "--include-sidechain conflicts with --main-only",
                        ));
                    }
                    SidechainFacet::MainOnly
                }
                (false, true) => {
                    if include_sidechain {
                        return Err(CliError::usage(
                            "--include-sidechain conflicts with --subagent-only",
                        ));
                    }
                    SidechainFacet::SubagentOnly
                }
                (false, false) => SidechainFacet::Include,
            };
            let kind = match tool_kind.as_deref() {
                None => None,
                Some("file") | Some("command") | Some("web") | Some("query") | Some("unknown") => {
                    Some(tool_kind.unwrap())
                }
                Some(other) => {
                    return Err(CliError::usage(format!(
                        "--tool-kind must be one of file|command|web|query|unknown, got {other}"
                    )));
                }
            };
            let facets = SearchFacets {
                sidechain,
                tool_kind: kind,
                tool_name,
            };
            // 页大小旋钮即 --max-items；未给时保守默认 20（App 内仍与 budget 取小）。
            let limit = if max_items.is_some() {
                budget.max_items
            } else {
                20
            };
            // #3 语义/混合模式：注入 SqliteStore 作为 SemanticIndex，并用同一
            // vectorizer 生成查询向量。`index embeddings` 未跑过时向量表为空，
            // store.is_ready(dimension) 为 false，Application 显式降级为 lexical_fallback
            // + warning（禁止静默切换）。
            let query = arg(&args, 1, "search <query>")?.to_string();
            let query_embedding =
                prepare_search_embedding(store, retrieval_mode, &query).map_err(CliError)?;
            // Construction resolves the current repo via Git; finish that
            // before pinning a DB view. Lexical mode never reads semantic data.
            let app = resume_semantic_app(store);
            // Human rows are loaded after App returns; share its read view.
            // Model loading/embedding must stay outside this scope.
            let snapshot = store
                .begin_read_snapshot()
                .map_err(|error| CliError(error.into()))?;
            let response = app.handle(AppRequest::Search {
                query,
                filters,
                facets: facets.clone(),
                limit,
                cursor,
                budget,
                include_system,
                group_by_session,
                mode: retrieval_mode,
                query_embedding,
            })?;
            let (outcome, mut data, page, warnings) = render(response);
            if mode == protocol::OutputMode::Human {
                attach_session_resume_rows(store, &mut data)?;
            }
            drop(snapshot);
            // Robot/机器面如实回显本次应用的 facet（默认值不回显——输出字节不变）。
            if !facets.is_default() {
                let echo = data.as_object_mut().expect("search data is an object");
                echo.insert(
                    "facets".into(),
                    serde_json::json!({
                        "sidechain": facets.sidechain.as_str(),
                        "tool_kind": facets.tool_kind,
                        "tool_name": facets.tool_name,
                    }),
                );
            }
            Ok(("search", outcome, data, page, warnings))
        }
        "handoff" => {
            let mut args = rest.to_vec();
            let max_evidence = extract_flag(&mut args, "--max-evidence")?;
            let max_tokens = extract_flag(&mut args, "--max-tokens")?;
            let max_bytes = extract_flag(&mut args, "--max-bytes")?;
            let providers = extract_repeated_flag(&mut args, "--provider")?;
            let since = extract_flag(&mut args, "--since")?;
            let until = extract_flag(&mut args, "--until")?;
            let app = resume_app(store);
            let filters = search_filters_from_flags(
                &providers,
                since.as_deref(),
                until.as_deref(),
                None,
                app.now_ms(),
            )?;
            no_extra_args(&args, 1, "handoff <query>")?;
            let query = arg(&args, 1, "handoff <query>")?.to_string();
            let search_limit = 50usize;
            let snapshot = store
                .begin_read_snapshot()
                .map_err(|error| CliError(error.into()))?;
            // 检索作为装配源：用宽松的 fetch-all 预算（含全文级 snippet），pack
            // 预算由包构建器单一执行（设计 D3——Context 路径的独立 clamp 会双重
            // 应用用户预算）。max_snippet_chars 放大到 schema 上限，让证据尽量
            // 携带原文而非 512 字符截断摘要。
            let response = app.handle(AppRequest::Search {
                query: query.clone(),
                filters: filters.clone(),
                facets: SearchFacets::default(),
                limit: search_limit,
                cursor: None,
                budget: ResponseBudget {
                    max_items: search_limit,
                    max_response_bytes: 64 * 1024 * 1024,
                    max_snippet_chars: 65536,
                    max_messages: search_limit,
                    max_evidence_spans: 512,
                },
                include_system: false,
                group_by_session: false,
                mode: RetrievalMode::Lexical,
                query_embedding: None,
            })?;
            let (hits, generation) = match &response {
                AppResponse::Search {
                    hits, generation, ..
                } => (hits.clone(), *generation),
                _ => return Err(CliError::usage("handoff: unexpected search response")),
            };
            let max_evidence_n = max_evidence
                .as_deref()
                .map(|v| {
                    v.parse::<usize>()
                        .map_err(|_| CliError::usage("--max-evidence 需要正整数"))
                })
                .transpose()?
                .unwrap_or(20);
            let max_tokens_n = max_tokens
                .as_deref()
                .map(|v| {
                    v.parse::<usize>()
                        .map_err(|_| CliError::usage("--max-tokens 需要正整数"))
                })
                .transpose()?
                .unwrap_or(8000);
            let max_bytes_n = max_bytes
                .as_deref()
                .map(|v| {
                    v.parse::<usize>()
                        .map_err(|_| CliError::usage("--max-bytes 需要正整数"))
                })
                .transpose()?
                .unwrap_or(2_000_000);
            // 权威 source locator：批量解析每条命中的 source document + span。
            let source_locations =
                agent_session_grep_application::handoff_pack::resolve_source_locations(
                    store, &hits,
                )
                .map_err(|e| CliError(e.into()))?;
            // Tool activities for the hit messages (schema v12). Empty when none.
            let hit_ids: Vec<_> = hits.iter().map(|h| h.id.clone()).collect();
            let tool_activities = store
                .tool_activities_for_messages(&hit_ids)
                .map_err(|e| CliError(e.into()))?;
            let message_facts: Vec<_> = store
                .message_facts_for(&hit_ids)
                .map_err(|e| CliError(e.into()))?
                .into_iter()
                .map(|(message_id, role, is_sidechain)| {
                    agent_session_grep_application::handoff_pack::MessageFact {
                        message_id,
                        role,
                        is_sidechain,
                    }
                })
                .collect();
            // All DB-backed evidence is materialized; do not retain the read
            // transaction while formatting/trimming the deterministic pack.
            drop(snapshot);
            let pack = agent_session_grep_application::handoff_pack::generate_deterministic(
                HandoffInput {
                    query_terms: std::slice::from_ref(&query),
                    retrieval_mode: agent_session_grep_ports::RetrievalMode::Lexical,
                    filters: agent_session_grep_ports::handoff::HandoffFilters {
                        providers: filters
                            .providers
                            .iter()
                            .map(|p| p.as_str().to_string())
                            .collect(),
                        since: filters
                            .since
                            .map(|s| format!("{}.{:09}Z", s.unix_seconds, s.nanosecond)),
                        until: filters
                            .until
                            .map(|s| format!("{}.{:09}Z", s.unix_seconds, s.nanosecond)),
                    },
                    hits: &hits,
                    source_locations: &source_locations,
                    tool_activities: &tool_activities,
                    message_facts: &message_facts,
                    catalog_generation: generation,
                    max_tokens: max_tokens_n as u64,
                    max_bytes: max_bytes_n as u64,
                    max_evidence: max_evidence_n,
                    target: None,
                },
            )
            .map_err(|error| CliError(error.into()))?;
            // 预算截断 → partial（exit 10），绝不伪装 success（contract §5）。
            let outcome = if pack.truncation.truncated {
                protocol::Outcome::Partial
            } else {
                protocol::Outcome::Success
            };
            Ok((
                "handoff",
                outcome,
                serde_json::to_value(&pack)
                    .map_err(|e| CliError::usage(format!("handoff: serialization error: {e}")))?,
                protocol::Page::default(),
                Vec::new(),
            ))
        }
        "get-message" => {
            let mut args = rest.to_vec();
            let session = extract_flag(&mut args, "--session")?;
            let around_value = extract_flag(&mut args, "--around")?;
            let around = around_value
                .as_deref()
                .map(str::parse::<usize>)
                .transpose()
                .map_err(|_| CliError::usage("--around must be a non-negative integer"))?
                .unwrap_or(0);
            let max_items = extract_flag(&mut args, "--max-items")?;
            let max_bytes = extract_flag(&mut args, "--max-bytes")?;
            let budget = budget_from_flags(max_items.as_deref(), max_bytes.as_deref(), None)?;
            no_extra_args(&args, 1, "get-message <message-wire-id>")?;
            let wire = arg(&args, 1, "get-message <message-wire-id>")?;
            let message_id = StableId::from_wire(wire)
                .filter(|id| id.kind() == IdKind::Message)
                .ok_or_else(|| CliError::usage(format!("not a valid message id: {wire}")))?;
            let session_id = session
                .as_deref()
                .map(|wire| {
                    StableId::from_wire(wire)
                        .filter(|id| id.kind() == IdKind::Session)
                        .ok_or_else(|| CliError::usage(format!("not a valid session id: {wire}")))
                })
                .transpose()?;
            let app = resume_app(store);
            let response = app.handle(AppRequest::Message {
                message_id,
                session_id,
                around,
                budget,
            })?;
            let (outcome, data, page, warnings) = render(response);
            Ok(("get-message", outcome, data, page, warnings))
        }
        "get" => {
            no_extra_args(rest, 1, "get <wire-id>")?;
            let wire = arg(rest, 1, "get <wire-id>")?;
            let id = StableId::from_wire(wire)
                .ok_or_else(|| CliError::usage(format!("not a valid entity id: {wire}")))?;
            let app = resume_app(store);
            let response = app.handle(AppRequest::Get { id: id.clone() })?;
            // 未找到实体：按 error catalog 映射 exit 4，而非当成功渲染 "not found"
            // （10 角色体验测试缺陷：show/get 不存在 ID 返回 exit 0，脚本无法区分）。
            // 消息固定为通用文案，不回显 wire/native ID（R2.1 隐私）。
            if matches!(&response, AppResponse::Get { payload: None }) {
                return Err(CliError(ProtocolError::new(
                    CanonicalCode::NotFound,
                    "entity not found",
                )));
            }
            let (outcome, data, page, warnings) = render(response);
            Ok(("get", outcome, data, page, warnings))
        }
        "show" => {
            no_extra_args(rest, 1, "show <wire-id>")?;
            let wire = arg(rest, 1, "show <wire-id>")?;
            let id = StableId::from_wire(wire)
                .ok_or_else(|| CliError::usage(format!("not a valid entity id: {wire}")))?;
            let app = resume_app(store);
            let response = app.handle(AppRequest::Show { id: id.clone() })?;
            if matches!(&response, AppResponse::Show { payload: None }) {
                return Err(CliError(ProtocolError::new(
                    CanonicalCode::NotFound,
                    "entity not found",
                )));
            }
            let (outcome, data, page, warnings) = render(response);
            Ok(("show", outcome, data, page, warnings))
        }
        "resume" => {
            // Resume 执行层（#5 + audit P1-2）：默认 dry-run 预览完整命令；
            // `--yes` 显式 opt-in 才实际 spawn provider 进程。首次使用无论是否
            // `--yes` 都强制只预览一次（持久标记），标记确认后才允许 `--yes`
            // 直接执行。未核验 resume 命令的 provider 恒为不可恢复（null/—），
            // 绝不编造命令。
            let mut args = rest.to_vec();
            let confirmed = take_bool_flag(&mut args, "--yes");
            if confirmed && mode != protocol::OutputMode::Human {
                return Err(CliError::usage(
                    "resume --yes requires Human output; omit machine output flags to execute, or omit --yes to preview",
                ));
            }
            no_extra_args(&args, 1, "resume <session-id>")?;
            let wire = arg(&args, 1, "resume <session-id>")?;
            let (preview, mut data) = load_resume_preview(store, wire)?;
            // 不可恢复不是错误：历史恒可检索，只是不可恢复（ADR-0009）。
            if !preview.available {
                return Ok((
                    "resume",
                    protocol::Outcome::Success,
                    data,
                    protocol::Page::default(),
                    Vec::new(),
                ));
            }
            let mut warnings = Vec::new();
            // 首次强制预览（PRD Q24）：持久标记缺失时，即使 `--yes` 也只预览
            // 不执行，并落标记；标记写失败时安全侧继续强制预览（fail closed）。
            let marker_root = resume_marker_root(db);
            let first_run =
                !agent_session_grep_application::resume::resume_preview_acknowledged(&marker_root);
            if first_run {
                match agent_session_grep_application::resume::acknowledge_resume_preview(
                    &marker_root,
                ) {
                    Ok(()) => {
                        if confirmed {
                            data["first_run_preview"] = serde_json::json!(true);
                            warnings.push(
                                "首次使用 resume：已强制预览未执行。再次运行 resume --yes <session-id> 确认后才会真正执行。"
                                    .to_string(),
                            );
                        }
                    }
                    Err(error) => {
                        warnings.push(format!(
                            "resume: 首次预览标记写入失败（将继续强制预览）：{:?}",
                            error.kind()
                        ));
                    }
                }
                return Ok((
                    "resume",
                    protocol::Outcome::Success,
                    data,
                    protocol::Page::default(),
                    warnings,
                ));
            }
            if confirmed {
                execute_resume(&preview.descriptor)?;
                data["executed"] = serde_json::json!(true);
            } else {
                warnings.push("dry-run：未执行。确认命令无误后加 --yes 实际恢复会话。".to_string());
            }
            Ok((
                "resume",
                protocol::Outcome::Success,
                data,
                protocol::Page::default(),
                warnings,
            ))
        }
        "get-session-resume" => {
            // 只读 Resume Metadata（ADR-0009）：只返回结构化字段，绝不构造/执行
            // shell 命令、绝不返回 transcript/source path。
            no_extra_args(rest, 1, "get-session-resume <session-id>")?;
            let wire = arg(rest, 1, "get-session-resume <session-id>")?;
            let id = StableId::from_wire(wire)
                .ok_or_else(|| CliError::usage(format!("not a valid entity id: {wire}")))?;
            let app = resume_app(store);
            let response = app.handle(AppRequest::GetSessionResume {
                session_id: id.clone(),
            })?;
            let (outcome, data, page, warnings) = render(response);
            Ok(("get-session-resume", outcome, data, page, warnings))
        }
        "list" => {
            let mut args = rest.to_vec();
            let cursor = extract_flag(&mut args, "--cursor")?;
            let max_items = extract_flag(&mut args, "--max-items")?;
            let max_bytes = extract_flag(&mut args, "--max-bytes")?;
            let budget = budget_from_flags(max_items.as_deref(), max_bytes.as_deref(), None)?;
            no_extra_args(&args, 1, "list [limit]")?;
            let limit = args
                .get(1)
                .map(|s| s.parse::<usize>())
                .transpose()
                .map_err(|_| CliError::usage("list [limit]: limit must be an integer"))?
                .unwrap_or(if max_items.is_some() {
                    budget.max_items
                } else {
                    20
                });
            let app = resume_app(store);
            let response = app.handle(AppRequest::List {
                limit,
                cursor,
                budget,
                sessions_only: false,
            })?;
            let (outcome, data, page, warnings) = render(response);
            Ok(("list", outcome, data, page, warnings))
        }
        // context：装配一个会话的分支消息链 + 证据区间（CONTRACT §1-2）。
        "context" => {
            let mut args = rest.to_vec();
            let policy = match extract_flag(&mut args, "--policy")?.as_deref() {
                None | Some("mainline") => ContextPolicy::Mainline,
                Some("full") => ContextPolicy::Full,
                Some(other) => {
                    return Err(CliError::usage(format!(
                        "--policy must be mainline|full, got {other}"
                    )));
                }
            };
            let max_messages = extract_flag(&mut args, "--max-messages")?;
            let max_bytes = extract_flag(&mut args, "--max-bytes")?;
            let level = match extract_flag(&mut args, "--level")?.as_deref() {
                None | Some("raw") => ContextLevel::Raw,
                Some("talks") => ContextLevel::Talks,
                Some("sessions") => ContextLevel::Sessions,
                Some(other) => {
                    return Err(CliError::usage(format!(
                        "--level must be raw|talks|sessions, got {other}"
                    )));
                }
            };
            let budget = budget_from_flags(None, max_bytes.as_deref(), max_messages.as_deref())?;
            no_extra_args(&args, 1, "context <session-wire-id>")?;
            let wire = arg(&args, 1, "context <session-wire-id>")?;
            let session_id = StableId::from_wire(wire)
                .filter(|id| id.kind() == IdKind::Session)
                .ok_or_else(|| CliError::usage(format!("not a valid session id: {wire}")))?;
            let app = resume_app(store);
            let response = app.handle(AppRequest::Context {
                session_id,
                policy,
                level,
                budget,
            })?;
            let (outcome, data, page, warnings) = render(response);
            Ok(("context", outcome, data, page, warnings))
        }
        "status" => {
            no_extra_args(rest, 0, "status")?;
            let app = resume_app(store);
            let response = app.handle(AppRequest::Status)?;
            let (outcome, data, page, warnings) = render(response);
            Ok(("status", outcome, data, page, warnings))
        }
        _ => Err(CliError::usage(format!(
            "unknown subcommand（可用命令：{}；运行 --help 查看完整用法）",
            KNOWN_SUBCOMMANDS.join("、")
        ))),
    }
}

/// 从参数向量中取走一个带值 flag；不在场返回 `None`，在场缺值是用法错误。
fn extract_flag(args: &mut Vec<String>, name: &str) -> Result<Option<String>, CliError> {
    let Some(i) = args.iter().position(|a| a == name) else {
        return Ok(None);
    };
    if i + 1 >= args.len() {
        return Err(CliError::usage(format!("{name} requires a value")));
    }
    let value = args.remove(i + 1);
    args.remove(i);
    Ok(Some(value))
}

/// 可重复带值 flag 的收集变体（`--provider`）：按出现顺序取走全部取值；
/// 在场缺值是用法错误，重复出现是合法累积（provider 维度按 OR 语义）。
fn extract_repeated_flag(args: &mut Vec<String>, name: &str) -> Result<Vec<String>, CliError> {
    let mut values = Vec::new();
    while let Some(i) = args.iter().position(|a| a == name) {
        if i + 1 >= args.len() {
            return Err(CliError::usage(format!("{name} requires a value")));
        }
        values.push(args.remove(i + 1));
        args.remove(i);
    }
    Ok(values)
}

/// 布尔 flag 提取（`--include-system`/`--group-by-session`）：在场移除该 token
/// 并返回 true，缺场返回 false。不消费取值；重复出现视为在场一次。
fn take_bool_flag(args: &mut Vec<String>, name: &str) -> bool {
    if let Some(i) = args.iter().position(|a| a == name) {
        args.remove(i);
        true
    } else {
        false
    }
}

/// CLI 检索过滤参数归一化：provider 别名 → 规范 id；时间值接受 RFC3339/ISO-8601
/// 绝对时间或 `1h|1d|1w` 紧凑相对量（相对量以注入的 application 时钟 `now_ms`
/// 为基准，全程同一时钟源）。取值非法是用法错误（exit 2）。
fn search_filters_from_flags(
    providers: &[String],
    since: Option<&str>,
    until: Option<&str>,
    repo: Option<&str>,
    now_ms: i64,
) -> Result<SearchFilters, CliError> {
    let mut filters = SearchFilters::default();
    for provider in providers {
        filters
            .providers
            .push(canonical_search_provider(provider).ok_or_else(|| {
                CliError::usage(format!(
                    "unknown provider: {provider} (expected {})",
                    provider_value_hint()
                ))
            })?);
    }
    filters.since = parse_time_flag("--since", since, now_ms)?;
    filters.until = parse_time_flag("--until", until, now_ms)?;
    // repo slug（schema v16）：逐字等值过滤；空白/空串是用法错误（拼写
    // 错误伪装成"匹配零结果"比报错更糟）。形状不校验——未命中即诚实空页。
    filters.repo = match repo {
        Some(raw) if raw.trim().is_empty() => {
            return Err(CliError::usage("--repo must not be empty"));
        }
        Some(raw) => Some(raw.to_string()),
        None => None,
    };
    Ok(filters)
}

/// Accepted provider spellings come from the capability registry used by every
/// search entrypoint and by the MCP schema.
pub(crate) fn provider_value_hint() -> String {
    agent_session_grep_ports::capability::search_provider_filter_values().join("|")
}

/// Normalize one provider request value to [`SearchProvider`]; `None` = unknown.
///
/// Accepts the canonical provider ids that every machine surface already
/// publishes ([`SearchProvider::as_str`], `providers` / `list_providers` /
/// `/api/providers` `provider_id`) plus the historical short aliases. The
/// embedded Web UI fills its provider selector from `/api/providers`, so a
/// canonical id must be a first-class value here — otherwise
/// `/api/search?provider=claude-code` fails `invalid_request` while the
/// equivalent CLI alias succeeds, which is exactly the CLI/Web fork the
/// loopback surface forbids. Unknown values stay fail-closed (usage error at
/// the caller), never silently dropped.
fn canonical_search_provider(provider: &str) -> Option<SearchProvider> {
    SearchProvider::parse(provider)
}

/// 从 HookConfig 构建检索过滤（#8）：provider 白名单（空 = 全部）、时间衰减
/// （`decay_days` > 0 时 `since = now - decay_days`，旧历史整体排除；0 = 不过滤）、
/// repo slug（schema v16；`None` = 不限仓库）。
/// provider 值经 [`parse_provider_value`] 归一，与 search `--provider` 同一套
/// canonical id 与别名。
/// 注入文本保持跨边界脱敏（ADR-0009）由调用方 hook 分支负责，本层只出过滤条件。
fn hook_search_filters(config: &hooks::HookConfig, now_ms: i64) -> Result<SearchFilters, CliError> {
    let mut filters = SearchFilters::default();
    for provider in &config.providers {
        filters
            .providers
            .push(canonical_search_provider(provider).ok_or_else(|| {
                CliError::usage(format!(
                    "hook --provider: unknown provider {provider} (expected {})",
                    provider_value_hint()
                ))
            })?);
    }
    if config.decay_days > 0 {
        let day_ms = 86_400_000i64;
        let since_ms = now_ms.saturating_sub(i64::from(config.decay_days).saturating_mul(day_ms));
        filters.since = Some(SearchInstant::from_unix_millis(since_ms));
    }
    // repo slug：与 search `--repo` 同一门禁——空/纯空白是用法错误，绝不静默
    // 降级为"不过滤"（拼写错误伪装成全库注入比报错更糟）。
    filters.repo = match config.repo.as_deref() {
        Some(raw) if raw.trim().is_empty() => {
            return Err(CliError::usage("hook --repo must not be empty"));
        }
        Some(raw) => Some(raw.to_string()),
        None => None,
    };
    Ok(filters)
}

/// 解析单个时间 flag：先按绝对 RFC3339/ISO-8601，失败再按紧凑相对量；两者都
/// 不成立即用法错误（错误信息不含原始值回显以外的后端细节）。
fn parse_time_flag(
    name: &str,
    value: Option<&str>,
    now_ms: i64,
) -> Result<Option<agent_session_grep_ports::SearchInstant>, CliError> {
    let Some(raw) = value else {
        return Ok(None);
    };
    if let Some(instant) = parse_search_instant(raw) {
        return Ok(Some(instant));
    }
    if let Some(instant) = parse_relative_search_instant(raw, now_ms) {
        return Ok(Some(instant));
    }
    Err(CliError::usage(format!(
        "{name} must be an RFC3339/ISO-8601 timestamp or a compact duration (1h|1d|1w), got {raw:?}"
    )))
}

/// 用 flag 覆盖默认预算；数值解析失败是用法错误，下限校验由 App 层统一执行。
fn budget_from_flags(
    max_items: Option<&str>,
    max_bytes: Option<&str>,
    max_messages: Option<&str>,
) -> Result<ResponseBudget, CliError> {
    let mut budget = ResponseBudget::default();
    if let Some(v) = max_items {
        budget.max_items = v
            .parse()
            .map_err(|_| CliError::usage("--max-items must be a non-negative integer"))?;
    }
    if let Some(v) = max_bytes {
        budget.max_response_bytes = v
            .parse()
            .map_err(|_| CliError::usage("--max-bytes must be a non-negative integer"))?;
    }
    if let Some(v) = max_messages {
        budget.max_messages = v
            .parse()
            .map_err(|_| CliError::usage("--max-messages must be a non-negative integer"))?;
    }
    Ok(budget)
}

/// Stateless preview for read-only entrypoints. Never acknowledges or executes.
pub(crate) fn preview_resume(
    store: &SqliteStore,
    wire: &str,
) -> Result<
    (
        protocol::Outcome,
        serde_json::Value,
        protocol::Page,
        Vec<String>,
    ),
    CliError,
> {
    let (_, data) = load_resume_preview(store, wire)?;
    Ok((
        protocol::Outcome::Success,
        data,
        protocol::Page::default(),
        Vec::new(),
    ))
}

fn load_resume_preview(
    store: &SqliteStore,
    wire: &str,
) -> Result<
    (
        agent_session_grep_application::resume::ResumePreview,
        serde_json::Value,
    ),
    CliError,
> {
    let id = StableId::from_wire(wire)
        .filter(|id| id.kind() == IdKind::Session)
        .ok_or_else(|| CliError::usage(format!("not a valid session id: {wire}")))?;
    let app = resume_app(store);
    let response = app.handle(AppRequest::GetSessionResume {
        session_id: id.clone(),
    })?;
    let AppResponse::SessionResume(metadata) = response else {
        return Err(CliError::usage("resume: unexpected response"));
    };
    let preview = agent_session_grep_application::resume::build_resume_descriptor(&metadata);
    let data = serde_json::json!({
        "session_id": metadata.session_id.as_str(),
        "provider_id": metadata.provider_id,
        "available": preview.available,
        "command": if preview.command_string.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::json!(preview.command_string)
        },
        "working_directory": preview.descriptor.working_directory,
        "permission_mode": preview.descriptor.permission_mode,
        // 诚实口径：permission mode 恒未核验（metadata/配置不携带真实
        // 模式），如实标注，绝不宣称已校验（audit P1-2）。
        "permission_mode_verified": false,
        "unavailable_reason": preview.unavailable_reason,
        "executed": false,
    });
    Ok((preview, data))
}

/// 执行 resume：在原 cwd 下 spawn provider 进程并等待其退出（前台接管）。
///
/// 执行前校验 cwd 存在（存在但不可访问也在此暴露）与 provider 二进制在 PATH
/// 上（缺失即结构化错误、提示安装，不再等到 spawn 才报）；cwd 不存在、二进制
/// 缺失、进程非零退出都返回结构化错误，绝不静默。permission_mode 只在用户
/// 显式选择时出现在 descriptor 中（默认 None，不自动带 yolo）。
fn execute_resume(
    descriptor: &agent_session_grep_application::resume::ResumeDescriptor,
) -> Result<(), CliError> {
    if let Some(dir) = &descriptor.working_directory
        && !std::path::Path::new(dir).is_dir()
    {
        return Err(CliError(
            ProtocolError::new(
                CanonicalCode::SourceIo,
                "resume working directory does not exist",
            )
            .with_details(serde_json::json!({ "stage": "cwd_check" })),
        ));
    }
    // provider 二进制 preflight（audit P1-2）：缺失在 spawn 之前就报结构化
    // 错误并提示安装；dry-run 不经过这里（预览不校验二进制）。
    if !provider_binary_on_path(&descriptor.provider_binary) {
        return Err(CliError(
            ProtocolError::new(
                CanonicalCode::ProviderError,
                "provider binary not found on PATH; install the provider CLI or add it to PATH before resuming",
            )
            .with_details(serde_json::json!({
                "stage": "binary_preflight",
                "binary": descriptor.provider_binary,
            })),
        ));
    }
    let mut command = std::process::Command::new(&descriptor.provider_binary);
    command.args(&descriptor.args);
    if let Some(dir) = &descriptor.working_directory {
        command.current_dir(dir);
    }
    let status = command.status().map_err(|e| {
        // 二进制缺失理论上已被 preflight 拦截；此处兜底处理竞态（preflight 后
        // 才被移除）。不回显完整路径，只给可操作原因。
        let reason = if e.kind() == std::io::ErrorKind::NotFound {
            "provider binary not found on PATH"
        } else {
            "failed to start provider process"
        };
        CliError(
            ProtocolError::new(CanonicalCode::ProviderError, reason)
                .with_details(serde_json::json!({ "stage": "spawn" })),
        )
    })?;
    if !status.success() {
        return Err(CliError(
            ProtocolError::new(
                CanonicalCode::ProviderError,
                "provider exited with a non-zero status",
            )
            .with_details(serde_json::json!({
                "stage": "provider_exit",
                "exit_code": status.code(),
            })),
        ));
    }
    Ok(())
}

/// 解析 resume 首次预览标记所在的 data root：与 writer lease 同一根——
/// db 文件所在目录（裸相对文件名按当前工作目录）。
fn resume_marker_root(db: &str) -> std::path::PathBuf {
    let db_path = std::path::Path::new(db);
    match db_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    }
}

/// provider 二进制是否能在 PATH 上解析（`Command::new` 的查找近似）。
///
/// Unix 要求是普通文件且带执行位；Windows 的 CreateProcess 会依次查找
/// `.exe`/`.com`/`.bat`/`.cmd`，这里覆盖 `.exe`/`.cmd`/`.bat`。
fn provider_binary_on_path(binary: &str) -> bool {
    let Some(path_var) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path_var).any(|dir| {
        let candidate = dir.join(binary);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::metadata(&candidate)
                .ok()
                .is_some_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        }
        #[cfg(not(unix))]
        {
            candidate.is_file()
                || ["exe", "cmd", "bat"]
                    .iter()
                    .any(|ext| candidate.with_extension(ext).is_file())
        }
    })
}

/// 从权威 catalog 重建语义向量索引（`index embeddings`，#3）。
///
/// 与 FTS rebuild 同一语义：向量表是 catalog 的投影。以有界 keyset 批次扫描，
/// 替换向量与 generation 在同一事务提交；编码失败保留旧投影。只对 Message
/// 实体建向量——session/document 没有检索正文。
///
/// Model selection:
/// - With `--features semantic-candle` and a verified local E5 bundle under the
///   platform cache (`config paths` → cache/models/...), uses the real Candle
///   multilingual-e5-small encoder.
/// - Otherwise falls back to the honest bigram-hash fuzzy-lexical vectorizer
///   and emits the experimental warning (default release path).
#[cfg(feature = "semantic-candle")]
fn installed_local_model_dir() -> Result<Option<std::path::PathBuf>, ProtocolError> {
    let paths = platform_paths().map_err(|error| error.0)?;
    let cache = paths
        .get("cache")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            ProtocolError::new(
                CanonicalCode::Internal,
                "model cache configuration is unavailable",
            )
        })?;
    let dir = agent_session_grep_application::candle_embedding::default_model_dir(
        std::path::Path::new(cache),
    );
    match std::fs::symlink_metadata(&dir) {
        Ok(_) => Ok(Some(dir)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(ProtocolError::new(
            CanonicalCode::CatalogError,
            "local model bundle could not be inspected",
        )
        .with_details(serde_json::json!({"stage": "model_discovery"}))),
    }
}

#[cfg(feature = "semantic-candle")]
fn model_load_error(error: agent_session_grep_ports::PortError) -> ProtocolError {
    // Preserve the existing private diagnostic path without putting backend
    // paths, manifest content or native IDs in the public error.
    let mut error = ProtocolError::from(error);
    error.message = "local model bundle could not be loaded; verify or re-import the bundle, then restart the process before retrying".into();
    error.with_details(serde_json::json!({"stage": "model_load"}))
}

fn build_embeddings(store: &SqliteStore) -> Result<(serde_json::Value, Vec<String>), CliError> {
    use agent_session_grep_application::embedding::BigramHashModel;
    use agent_session_grep_ports::EmbeddingModel;

    enum ActiveModel {
        Bigram(BigramHashModel),
        // Boxed: the loaded BERT model is far larger than BigramHashModel, and
        // the enum only lives for the duration of this rebuild (clippy
        // large_enum_variant). Semantics unchanged.
        #[cfg(feature = "semantic-candle")]
        Candle(Box<agent_session_grep_application::candle_embedding::CandleE5Model>),
    }

    impl ActiveModel {
        fn embed(
            &self,
            text: &str,
            is_query: bool,
        ) -> agent_session_grep_ports::PortResult<Vec<f32>> {
            match self {
                Self::Bigram(m) => m.embed(text, is_query),
                #[cfg(feature = "semantic-candle")]
                Self::Candle(m) => m.embed(text, is_query),
            }
        }
        fn dimension(&self) -> usize {
            match self {
                Self::Bigram(m) => m.dimension(),
                #[cfg(feature = "semantic-candle")]
                Self::Candle(m) => m.dimension(),
            }
        }
        fn model_id(&self) -> &str {
            match self {
                Self::Bigram(m) => m.manifest().model_id.as_str(),
                #[cfg(feature = "semantic-candle")]
                Self::Candle(m) => m.manifest().model_id.as_str(),
            }
        }
        fn manifest_json(&self) -> serde_json::Value {
            match self {
                Self::Bigram(m) => {
                    let man = m.manifest();
                    serde_json::json!({
                        "model_id": man.model_id,
                        "dimension": man.dimension,
                        "license": man.license,
                        "file_hash": man.file_hash,
                        "backend": "bigram-hash",
                    })
                }
                #[cfg(feature = "semantic-candle")]
                Self::Candle(m) => {
                    let man = m.manifest();
                    serde_json::json!({
                        "model_id": man.model_id,
                        "dimension": man.dimension,
                        "license": man.license,
                        "file_hash": man.file_hash,
                        "backend": "semantic-candle",
                    })
                }
            }
        }
        fn warning(&self) -> Option<String> {
            match self {
                Self::Bigram(_) => Some(
                    "当前向量化器是 bigram-hash（模糊词法相似，非语义）；semantic/hybrid 因此仅供实验，lexical 仍是默认。"
                        .to_string(),
                ),
                #[cfg(feature = "semantic-candle")]
                Self::Candle(_) => None,
            }
        }
    }

    let model = {
        #[cfg(feature = "semantic-candle")]
        {
            match installed_local_model_dir()? {
                Some(dir) => ActiveModel::Candle(Box::new(
                    agent_session_grep_application::candle_embedding::CandleE5Model::load_from_dir(
                        &dir,
                    )
                    .map_err(model_load_error)?,
                )),
                None => ActiveModel::Bigram(BigramHashModel::new()),
            }
        }
        #[cfg(not(feature = "semantic-candle"))]
        {
            ActiveModel::Bigram(BigramHashModel::new())
        }
    };

    let model_id = model.model_id().to_string();
    store.set_semantic_model(&model_id);
    let (indexed, skipped, cleared) = store
        .rebuild_embeddings_from_catalog(&model_id, model.dimension(), 128, |entry| {
            let payload = serde_json::from_slice::<serde_json::Value>(&entry.payload).ok();
            let text = payload
                .as_ref()
                .and_then(|value| value.get("text"))
                .and_then(serde_json::Value::as_str);
            match text.filter(|text| !text.trim().is_empty()) {
                Some(text) => model.embed(text, false).map(Some),
                None => Ok(None),
            }
        })
        .map_err(ProtocolError::from)?;

    let mut data = model.manifest_json();
    if let Some(obj) = data.as_object_mut() {
        obj.insert("indexed".into(), serde_json::json!(indexed));
        obj.insert("skipped".into(), serde_json::json!(skipped));
        obj.insert("cleared".into(), serde_json::json!(cleared));
    }
    let warnings = model.warning().into_iter().collect();
    Ok((data, warnings))
}

/// 用与 CLI `index`/`get` 一致的派生路径，从一个 fact 造出 message id。
fn message_id(fact: &str) -> StableId {
    StableId::derive(
        IdKind::Message,
        Stability::Reconstructed,
        &[fact.as_bytes()],
    )
}

/// 写入一条：派生 id → durable outbox → 原子提交 catalog + FTS + generation。
fn index_one(store: &SqliteStore, fact: &str, text: &str) -> Result<serde_json::Value, CliError> {
    let id = message_id(fact);
    // FTS 投影有界（借鉴清单 #3，与 ingest/sync 入口同一截断）。
    let entries = [(
        id.clone(),
        text.as_bytes().to_vec(),
        bounded_index_text(text),
    )];
    store.commit_batch(&entries).map_err(ProtocolError::from)?;
    let generation = store.active_generation().map_err(ProtocolError::from)?;
    Ok(serde_json::json!({
        "indexed": id.as_str(),
        "generation": generation,
    }))
}

/// 借用 store 作为端口 trait 对象的辅助——App 需要两个泛型参数各持一份。
/// 由于 App<C,S> 按值持有两个后端，而我们只有一个 SqliteStore 实例，
/// 这里用 `&SqliteStore` 满足两个 trait 约束（trait 对 &T 亦实现）。
fn store_ref(store: &SqliteStore) -> &SqliteStore {
    store
}

/// 应用时钟注入点：`ASG_CLOCK_MS`（Unix 毫秒）存在时返回该固定值——仅供
/// 确定性 e2e 测试使用；未设置时回落系统时钟，生产行为与旧版一致。
/// rank signals 的时效衰减与 cursor 签发/校验共用同一时钟源。
fn app_clock_ms() -> i64 {
    std::env::var("ASG_CLOCK_MS")
        .ok()
        .and_then(|raw| raw.parse::<i64>().ok())
        .unwrap_or_else(agent_session_grep_application::system_now_ms)
}

/// 组合根统一构造带 Resume 槽的 App（catalog/index/resume 共用同一 store），
/// 时钟经 [`app_clock_ms`] 注入——测试固定、生产系统时钟；当前仓库偏好经
/// [`current_repo_slug`] 注入。
fn resume_app(store: &SqliteStore) -> App<&SqliteStore, &SqliteStore, &SqliteStore> {
    App::with_resume_and_clock(
        store_ref(store),
        store_ref(store),
        store_ref(store),
        app_clock_ms,
    )
    .with_current_repo(current_repo_slug())
}

/// 同上，但额外注入语义索引槽（#3 semantic/hybrid 模式）。
fn resume_semantic_app(
    store: &SqliteStore,
) -> App<&SqliteStore, &SqliteStore, &SqliteStore, &SqliteStore> {
    App::with_resume_semantic_and_clock(
        store_ref(store),
        store_ref(store),
        store_ref(store),
        store_ref(store),
        app_clock_ms,
    )
    .with_current_repo(current_repo_slug())
}

/// 调用方当前工作目录派生的 repo slug（当前仓库偏好排序信号的唯一来源）。
///
/// CLI 有明确的 cwd 语义，因此在这里派生一次；不在 git 工作树内、无 `origin`、
/// 或 cwd 不可读时一律 `None`（信号关闭，排序与注入前逐字节一致）。
/// `ASG_CURRENT_REPO` 覆盖派生结果，供 e2e 固定该信号——与 `ASG_CLOCK_MS`
/// 同一注入纪律：生产不设该变量时走真实 git 探测。
fn current_repo_slug() -> Option<String> {
    if let Ok(value) = std::env::var("ASG_CURRENT_REPO") {
        let trimmed = value.trim();
        return if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
    }
    let cwd = std::env::current_dir().ok()?;
    let resolver = repo_identity::GitRepoSlugResolver::default();
    agent_session_grep_adapters_sqlite::RepoSlugResolver::resolve(&resolver, &cwd.to_string_lossy())
}

/// Resolve the local query encoder once at the composition root so CLI and
/// MCP semantic requests use the same model identity and fallback behavior.
pub(crate) fn prepare_search_embedding(
    store: &SqliteStore,
    mode: RetrievalMode,
    query: &str,
) -> Result<Option<Vec<f32>>, ProtocolError> {
    if matches!(
        mode,
        RetrievalMode::Lexical | RetrievalMode::LexicalFallback
    ) {
        return Ok(None);
    }

    use agent_session_grep_application::embedding::{BIGRAM_HASH_MODEL_ID, BigramHashModel};
    use agent_session_grep_ports::EmbeddingModel;

    let bigram_embedding = || -> Result<(String, Vec<f32>), ProtocolError> {
        let model = BigramHashModel::new();
        let embedding = model.embed(query, true).map_err(ProtocolError::from)?;
        Ok((BIGRAM_HASH_MODEL_ID.to_string(), embedding))
    };

    #[cfg(feature = "semantic-candle")]
    let (model_id, embedding) = {
        match installed_local_model_dir()? {
            Some(dir) => {
                let model =
                    agent_session_grep_application::candle_embedding::CandleE5Model::load_cached(
                        dir,
                    )
                    .map_err(model_load_error)?;
                (
                    model.manifest().model_id.clone(),
                    model.embed(query, true).map_err(ProtocolError::from)?,
                )
            }
            None => bigram_embedding()?,
        }
    };

    #[cfg(not(feature = "semantic-candle"))]
    let (model_id, embedding) = bigram_embedding()?;

    store.set_semantic_model(&model_id);
    Ok(Some(embedding))
}

/// Human search 的会话表格行按 canonical Session 去重后批量解析 Resume
/// Metadata。此投影只在 Human 模式附加，Robot/MCP 协议形状不受影响。
///
/// 日期取该 Session 最近活动 timestamp 的 `YYYY-MM-DD`（批量一次查询，无 N+1）；
/// 标题取当前页中该 Session 的最高相关度命中 `text`——标题跟随搜索排序，
/// 与“相关度优先”不变量一致。两者缺失渲染为 `—`。
fn attach_session_resume_rows(
    store: &SqliteStore,
    data: &mut serde_json::Value,
) -> Result<(), CliError> {
    let Some(hits) = data.get("hits").and_then(serde_json::Value::as_array) else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    let session_ids: Vec<StableId> = hits
        .iter()
        .filter_map(|hit| hit.get("session_id").and_then(serde_json::Value::as_str))
        .filter(|wire| seen.insert((*wire).to_string()))
        .filter_map(StableId::from_wire)
        .filter(|id| id.kind() == IdKind::Session)
        .collect();
    let metadata = store.resume_of(&session_ids).map_err(ProtocolError::from)?;
    let latest_ymd = store
        .latest_activity_ymd_for_sessions(&session_ids)
        .map_err(ProtocolError::from)?;
    let rows: Vec<serde_json::Value> = metadata
        .iter()
        .map(|metadata| {
            let session_wire = metadata.session_id.as_str();
            let date = latest_ymd.get(session_wire).cloned();
            let title = hits
                .iter()
                .filter_map(|hit| {
                    let hit_session = hit.get("session_id").and_then(serde_json::Value::as_str)?;
                    (hit_session == session_wire)
                        .then_some(())
                        .and_then(|_| hit.get("text").and_then(serde_json::Value::as_str))
                })
                .next()
                .map(|text| text.to_string());
            serde_json::json!({
                "date": date,
                "provider": metadata.provider_id,
                "title": title,
                "working_directory": metadata.original_working_directory,
                "session_id": metadata.provider_session_id,
            })
        })
        .collect();
    if let Some(object) = data.as_object_mut() {
        object.insert("session_resume_rows".into(), serde_json::Value::Array(rows));
    }
    Ok(())
}

/// 组合根持有的 provider adapter 清单。ingest/sync 用它 probe-select，
/// 由 [`select_and_stage_source`] 挑出认领此源的 adapter（见 RFC-0002 §3）。
/// 新增 provider 只需在此登记一行。
fn provider_registry() -> Vec<Box<dyn ProviderAdapter>> {
    vec![
        Box::new(ClaudeCodeAdapter::new()),
        Box::new(AiderAdapter::new()),
        Box::new(CodexAdapter::new()),
        Box::new(GrokBuildAdapter::new()),
        Box::new(PiAdapter::new()),
        Box::new(QoderAdapter::new()),
        Box::new(KimiCodeAdapter::new()),
        Box::new(OpenClawAdapter::new()),
        Box::new(OpenCodeAdapter::new()),
        Box::new(CodeBuddyAdapter::new()),
        Box::new(ClineAdapter::new()),
        Box::new(AntigravityAdapter::new()),
        Box::new(OpenHermesAdapter::new()),
        Box::new(CursorAdapter::new()),
    ]
}

/// Whether a provider's adapter consumes its source as a record stream
/// (line-delimited JSONL). Derived from the adapter manifest — the single
/// source of truth for `streaming_support`; `None` means no adapter claims
/// this provider id. Whole-source formats (SQLite/JSON/Markdown) must never be
/// triaged by the JSONL tail-health heuristic.
fn provider_is_record_stream(provider_id: &str) -> Option<bool> {
    provider_registry()
        .iter()
        .find(|adapter| adapter.provider_id() == provider_id)
        .map(|adapter| {
            matches!(
                adapter.manifest().streaming_support,
                agent_session_grep_ports::StreamingSupport::RecordStream
            )
        })
}

/// 对一个可重复打开的只读 source probe-select 并 stage，同时返回选中 variant。
///
/// 空源保留整源清空/tombstone 语义；其它源的每次 probe/parse 都由 source
/// 重新打开 bounded reader（JSONL 逐行 / 整档格式按 manifest 上限），生产路径
/// 绝不把完整 transcript 变成 Vec（RFC-0002 §7）。
///
/// `provider_hint` 是**规范根派生**的 provider 身份：路径落在某个
/// [`PROVIDER_DISCOVERY_ROOTS`] 登记根之下时，该路径的 provider 是布局事实而非猜测。
/// `sync --discover` 由逐根扫描直接得到它，显式 `sync <file>` / `ingest <file>` 由
/// [`provider_for_source_path`] 反查同一张表得到。给出时候选集收缩到该 provider 的
/// adapter，probe 仍照常执行——身份来自路径，格式判定仍来自内容。
///
/// 这一收缩是必需的，不是优化：pi 与 openclaw 的 transcript 是**同一种** v3 JSONL
/// （`{type:session,...}` 头 + `{type:message,message:{role,content}}`，见
/// provider-openclaw 模块文档"the same v3 JSONL shape as the Pi adapter"），两者
/// probe 同为 `Confirmed`，内容里没有任何可区分的判别位。全 registry probe 因此
/// 必然命中 `select_and_stage_source` 的 tie 分支并拒绝整个源——`~/.pi` 与
/// `~/.openclaw` 下的源在修复前一律无法索引。歧义只能由路径消解。
///
/// 路径不在任何登记根之下（临时目录、导出的副本、未登记 provider）时没有路径事实
/// 可用，仍走全 registry probe：那里的 tie 拒绝是诚实行为，不得靠猜测绕过。
fn stage_with_source(
    source: &dyn agent_session_grep_ports::ReadOnlySource,
    provider_hint: Option<&str>,
) -> Result<(StagedBatch, String), CliError> {
    if source.is_empty() {
        return Ok((
            StagedBatch {
                messages: Vec::new(),
                activities: Vec::new(),
                usage_events: Vec::new(),
                report: ParseReport {
                    committed: 0,
                    skipped: 0,
                    diagnostics: Vec::new(),
                    session_native_id: None,
                    session_observation: ProviderSessionObservation::default(),
                },
                session_native_id: None,
            },
            "empty".into(),
        ));
    }
    let registry = provider_registry();
    let refs: Vec<&dyn ProviderAdapter> = match provider_hint {
        // 路径事实存在：只让该 provider 的 adapter 参与。命中 0 个 adapter 时
        // 不静默退回全 registry——那会让 hint 形同虚设，且退回后仍会撞上同一
        // tie。此处的空候选集由 select_and_stage_source 报"无 provider 认领"。
        Some(pid) => registry
            .iter()
            .filter(|a| a.provider_id() == pid)
            .map(|a| a.as_ref())
            .collect(),
        None => registry.iter().map(|a| a.as_ref()).collect(),
    };
    select_and_stage_source(&refs, source).map_err(Into::into)
}

/// 0 字节源是合法的"整源清空"批次：它必须触发 source replacement 推导
/// tombstone，但空源没有任何 provider/variant 证据——绝不派生
/// `provider="empty"` 的伪实体或伪安装绑定。安装归属由已证明的既有绑定在提交
/// 时解析；`provider_id` 只承载 discovery 的所有权事实。
fn empty_source_batch(
    path: &str,
    fingerprint: &str,
    discovered_provider_id: Option<&str>,
) -> SourceBatch {
    SourceBatch {
        source_path: path.to_string(),
        entries: Vec::new(),
        placements: Vec::new(),
        edges: Vec::new(),
        activities: Vec::new(),
        usage_events: Vec::new(),
        relation_complete: true,
        len_bytes: Some(0),
        fingerprint: Some(fingerprint.to_string()),
        provider_id: discovered_provider_id.map(str::to_string),
        resume_claims: Vec::new(),
    }
}

struct StagedMessageEntity {
    id: StableId,
    role: String,
    text: String,
    timestamp: Option<String>,
    occurrences: Vec<(MessagePlacement, Option<StableId>, Option<String>)>,
}

/// 把一个源的完整 staging 产物转成稳定实体 + contextual relations。
///
/// 三类实体与 placements/edges 随同一 [`SourceBatch`] 单事务提交：
///
/// - **消息**：身份优先用 provider-native id（Claude Code 的 `uuid`，tier `Native`），
///   provider 未给 native id 时使用 provider/variant/document/ordinal 的 path-free
///   `Unstable` fallback。重复 stable id 只保留一个实体，全部 occurrences 仍保留。
/// - **会话**：id 优先取 provider 报告的 native 会话 id（`ses_v1_` Native tier），
///   缺失回退对 document wire id 的 `Reconstructed` 派生。payload 引用 document
///   与按首次 occurrence 排序的去重成员消息 wire id。
/// - **文档**：内容寻址 `Reconstructed` 派生（provider/variant/fingerprint），
///   不含路径——身份不编码位置（RFC-0001）。payload 携 provider/variant/fingerprint/len。
///
/// `ParseReport.skipped > 0` 会使 source relation-incomplete；observed facts 可提交，
/// 但存储层不会推导 tombstone，且会撤销旧 completeness marker。
///
/// 从源路径推导该 provider 安装的 namespace：优先使用路径上最后一个 provider
/// 数据根（`.claude` / `.codex`）的完整路径，使同一安装下的 transcript
/// 共享 namespace、不同安装分离。手动 ingest 的 Source 若不在已知数据根下，
/// 以其共同父目录作为未知安装边界，避免同一 Session 分散在多个文件时被拆开。
fn installation_namespace(path: &str, provider_id: &str) -> String {
    agent_session_grep_application::relocation::legacy_installation_namespace(path, provider_id)
}

/// `sync --discover` 的 provider → (home 相对数据根, 源文件扩展名) 映射表（唯一来源）。
///
/// 一个 provider 出现在此表 ⟺ `capability.rs` 的 `discover` 列必须非
/// `Unsupported`；`discover_roots_match_capability_discover_claims` 双向守护该
/// 等价关系。
///
/// 扩展名（精确匹配，不含点）是 per-provider 事实，不是全局常量：JSONL
/// transcript 用 `jsonl`，opencode 的源是 SQLite 故用 `db`。登记前必须确认该 root
/// 下的源确实是这个扩展名——[`sync_discover`] 会把"完整扫描"未重新发现的已存路径
/// diff 成空批 tombstone，扩展名写错会让扫描完整但为空，从而抹掉该 provider 此前
/// 的索引。
const PROVIDER_DISCOVERY_ROOTS: &[(&str, &str, &str)] = &[
    ("claude-code", ".claude/projects", "jsonl"),
    ("codex", ".codex/sessions", "jsonl"),
    ("openclaw", ".openclaw/agents", "jsonl"),
    ("tencent-codebuddy", ".codebuddy/projects", "jsonl"),
    ("antigravity", ".gemini/antigravity-cli/brain", "jsonl"),
    // OpenCode 的源是单个 SQLite DB。`opencode.db-wal` / `-shm` 旁文件的
    // extension 是 `db-wal` / `db-shm`（最后一个点之后），精确匹配 `db` 天然把
    // 它们排除；实测忽略 WAL 不会少读任何行（session/message/part 计数与带
    // WAL 打开完全一致），所以只收 `.db` 是完整的。
    ("opencode", ".local/share/opencode", "db"),
    // Pi 的 transcript 按 cwd 编码分子目录（`sessions/<encoded-cwd>/*.jsonl`），
    // 递归扫描天然覆盖；本机该 root 下 7 个文件全为 `.jsonl`，无其他扩展名混杂。
    ("pi", ".pi/agent/sessions", "jsonl"),
    // Hermes 的根来自本 adapter 所移植的上游 hstry：其 hermes adapter 把
    // `join(homedir(), '.hermes', 'sessions')` 硬编码为 DEFAULT_HERMES_PATH，并用
    // `isUnderCanonicalRoot` 做纵深防御——即上游自己把这个根当作规范根强制执行，
    // 不是文档里的一句描述。扩展名取 `json` 而非 `jsonl`：canonical 源是
    // `session_<id>.json`（完整 transcript + metadata），同目录的 `<id>.jsonl`
    // 只存部分近期状态，上游明确忽略；精确扩展名匹配天然把它们排除。
    ("hermes", ".hermes/sessions", "json"),
    // Grok Build 与 Kimi Code 的根同样来自各自 adapter 所移植的上游 fast-resume
    // （`src/config.rs`）：`grok_sessions_dir()` = `~/.grok/sessions`，
    // `kimi_sessions_dir()` = `~/.kimi-code/sessions`。两者都还支持 `GROK_HOME` /
    // `KIMI_CODE_HOME` 覆盖，本表只登记 home 相对的默认根——环境变量覆盖时该
    // provider 的扫描会是"根不存在"从而 partial（绝不 tombstone），比猜测更诚实。
    //
    // 两个根的源都是 `.jsonl`，但都与同目录的其他文件混放：Grok 每个 session 目录
    // 是 `updates.jsonl` + `summary.json`，Kimi 是 `wire.jsonl` + `state.json`，
    // 根下另有 `session_index.jsonl`。精确扩展名匹配排除 `.json` 旁文件；
    // `session_index.jsonl` 确实是 `.jsonl`，会被交给 probe——它不含
    // `context.append_message` 记录，adapter 如实 `AmbiguousVariant` 拒绝，
    // 这正是 probe 该做的判定，不是可以靠猜文件名绕过的事。
    ("grok-build", ".grok/sessions", "jsonl"),
    ("kimi-code", ".kimi-code/sessions", "jsonl"),
    // Qoder 有两个互不相干的会话面，本 adapter 只认第一个：
    //   1. 官方 transcript JSONL 树 `~/.qoder/projects/<project>/transcript/*.jsonl`
    //      —— provider-qoder 解析的就是它（ctx 的 `qoder_transcript_jsonl_tree` 行
    //      同样只实现这一面，并显式声明不解析 Electron 状态库）；
    //   2. Qoder IDE（VS Code fork）的 Electron SQLite 库
    //      `AppData/Roaming/Qoder/SharedClientCache/cache/db/local.db`
    //      （`chat_session`/`chat_message`/`chat_record` 表，与 Lingma 同构）。
    // 只登记第 (1) 面的根：adapter 能解析的就是这一面，登记它不会让扫描收到
    // 解析不了的源。第 (2) 面需要一个独立的 SQLite variant，未实现故不登记——
    // 若把 `.qoder` 整体登记为根，`extensions/` 下成百上千个 `.json` 会被当成源
    // 交给 probe，纯噪音。root 不存在时扫描为 partial（绝不 tombstone），因此
    // 只装了 IDE、没有 CLI transcript 树的机器上登记它也是安全的。
    ("qoder", ".qoder/projects", "jsonl"),
    // Cline 的 task 目录根来自 ctx 的 provider-support-matrix：
    // `~/.cline/data/tasks/*/{api_conversation_history.json, ui_messages.json,
    // context_history.json, task_metadata.json}`，ctx 的 fixture 就按这个形状铺
    // （`cline/data/tasks/cline-task-1/` 下四个文件齐全）。本 adapter 只解析
    // `api_conversation_history.json`（顶层 JSON 数组，元素带 `role`）。
    //
    // 扩展名 `json` 会把同目录另外三个旁文件也交给 probe，这是安全的而非漏洞：
    // 三者顶层都是 object 而不是数组（`ui_messages.json` = {type,say,text,ts}，
    // `context_history.json` = {context}，`task_metadata.json` =
    // {taskId,createdAt,...}），probe 第一道 `value.as_array()` 判定就以
    // `AmbiguousVariant` 拒绝。`data/state/taskHistory.json` 同理（object）。
    // 也就是说 probe 自己有能力区分这一面，不需要靠猜文件名过滤。
    //
    // 另一处 ctx 记录的位置是 VS Code 扩展的 globalStorage
    // （`saoudrizwan.claude-dev/tasks/*/`），它在 `AppData/Roaming/Code/User` 下
    // 而非 home 相对的稳定路径，且随 VS Code 变体（Code/Code - Insiders/VSCodium）
    // 漂移；本表只登记 `~/.cline/data/tasks` 这一条自证的 home 相对根，
    // 不猜 globalStorage。CLINE_DATA_DIR / CLINE_DIR 环境变量覆盖同样不猜。
    ("cline", ".cline/data/tasks", "json"),
];

/// 解析当前用户 home 目录下某 provider 的规范化 transcript 数据根。
///
/// 与 [`installation_namespace`] 复用同一组 marker 常量（`.claude` / `.codex`）。
/// home 目录优先取 `HOME`（Unix），回退 `USERPROFILE`（Windows）；两者都缺失返回
/// `None`，调用方应跳过该 provider 的发现（R4：不猜路径）。
///
/// 映射表见 [`PROVIDER_DISCOVERY_ROOTS`]；未登记的 provider 返回 `None`。返回值
/// 同时带上该 provider 源文件的扩展名，调用方无需再查表（也就无从写出与 root
/// 不匹配的扩展名）。
fn provider_discovery_target(provider_id: &str) -> Option<(std::path::PathBuf, &'static str)> {
    let (_, sub, extension) = PROVIDER_DISCOVERY_ROOTS
        .iter()
        .find(|(id, _, _)| *id == provider_id)?;
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(std::path::PathBuf::from)?;
    Some((home.join(sub), extension))
}

/// 反查一条源路径所属的 provider：路径落在某个 [`PROVIDER_DISCOVERY_ROOTS`] 登记根
/// 之下时返回该 provider id。
///
/// 与 [`provider_discovery_target`] 是同一张表的两个方向——正向（provider → root）供
/// `sync --discover` 逐根扫描，反向（path → provider）供显式 `sync <file>` /
/// `ingest <file>`。两者共用一张表，因此不会出现"discover 认得这条路径、显式 sync 不
/// 认得"的分裂。
///
/// 比较在 [`source_path_identity`] 归一之后进行（Windows 反斜杠 → 正斜杠、盘符小写），
/// 并按路径分段而非字符串前缀匹配：`.pi/agent/sessions` 不得匹配到
/// `.pi/agent/sessions-backup`。root 不存在于磁盘上也照常匹配——这里判定的是路径归属
/// 这一布局事实，不是文件可读性。
///
/// 多个 root 同时匹配时取最长（分段最多）的那个，使将来登记嵌套根不会静默取错。
///
/// 只看 root 归属，**不要求**扩展名与该 provider 登记的扩展名一致：显式 `sync <file>`
/// 是用户点名的意图，`~/.pi/.../foo.jsonl.bak` 这类改过名的真实 transcript 应当照常
/// 索引，而不是因为撞上 pi/openclaw 的 tie 被整源拒绝。
///
/// 返回值只用于收缩 probe 候选集，**不**写入 `source_scans.provider_id`——后者的
/// tombstone diff 语义要求该行确实来自一次"按 root + 扩展名枚举"的完整扫描
/// （见 [`sync_discover`]）。若显式 sync 也写入 provider_id，上面那个 `.bak`
/// 路径就会在下一次完整 discover 扫描中因未被重新发现而被合成空批抹掉。
fn provider_for_source_path(path: &str) -> Option<&'static str> {
    let normalized = source_path_identity(path);
    let segments: Vec<&str> = normalized
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let mut best: Option<(usize, &'static str)> = None;
    for (id, sub, _) in PROVIDER_DISCOVERY_ROOTS {
        let root_segments: Vec<&str> = sub.split('/').filter(|s| !s.is_empty()).collect();
        if root_segments.is_empty() {
            continue;
        }
        // root 是 home 相对的，源路径是绝对的，因此在任意位置对齐分段窗口即可
        // （同时天然支持非默认 home / 测试用 HOME 覆盖）。窗口之后必须还有分段：
        // 路径恰好停在 root 上时它是数据根目录本身，不是根下的某个源。
        let matched = segments
            .windows(root_segments.len())
            .enumerate()
            .any(|(start, window)| {
                window == root_segments.as_slice() && start + root_segments.len() < segments.len()
            });
        if matched && best.is_none_or(|(len, _)| root_segments.len() > len) {
            best = Some((root_segments.len(), id));
        }
    }
    best.map(|(_, id)| id)
}

fn source_path_identity(path: &str) -> String {
    if !cfg!(windows) {
        return path.to_string();
    }
    let mut normalized = path.replace('\\', "/");
    let bytes = normalized.as_bytes();
    if bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_uppercase() {
        let drive = (bytes[0].to_ascii_lowercase() as char).to_string();
        normalized.replace_range(..1, &drive);
    }
    normalized
}

/// Resolve a newly supplied filesystem path before creating its first registry
/// binding. Never silently replace a previously indexed non-normal locator.
fn source_input_path(store: &SqliteStore, path: &str) -> Result<String, CliError> {
    let input = std::path::Path::new(path);
    if input.is_absolute()
        && !input.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Ok(source_path_identity(path));
    }
    if store
        .source_fingerprints(&[path.to_string()])
        .map_err(ProtocolError::from)?
        .contains_key(path)
    {
        return Err(CliError::usage(
            "existing source locator needs explicit provenance resolution",
        ));
    }
    let canonical = std::fs::canonicalize(input).map_err(|_| {
        ProtocolError::new(CanonicalCode::SourceIo, "source path could not be resolved")
    })?;
    let canonical = canonical
        .to_str()
        .ok_or_else(|| CliError::usage("source path must be valid Unicode"))?;
    let path = if cfg!(windows) {
        if let Some(unc) = canonical.strip_prefix(r"\\?\UNC\") {
            format!(r"\\{unc}")
        } else {
            canonical
                .strip_prefix(r"\\?\")
                .unwrap_or(canonical)
                .to_string()
        }
    } else {
        canonical.to_string()
    };
    Ok(source_path_identity(&path))
}

/// Root hints apply only to previously scanned sources. New files in the same
/// directory may belong to another provider and still require ordinary probes.
/// Discovery ownership remains separate from source_scans.provider_id.
fn registered_provider_hints(
    store: &SqliteStore,
    paths: &[String],
) -> Result<BTreeMap<String, String>, CliError> {
    let known_sources = store
        .source_fingerprints(paths)
        .map_err(ProtocolError::from)?;
    let mut candidates: BTreeMap<String, Option<String>> = BTreeMap::new();
    for adapter in provider_registry() {
        let provider = adapter.provider_id();
        let roots = store
            .active_installation_roots(provider)
            .map_err(ProtocolError::from_private_port_error)?;
        for path in paths {
            if !known_sources.contains_key(path) {
                continue;
            }
            let mut contains = false;
            for root in &roots {
                if agent_session_grep_application::relocation::path_is_within(path, root)
                    .map_err(ProtocolError::from_private_port_error)?
                {
                    contains = true;
                    break;
                }
            }
            if contains {
                candidates
                    .entry(path.clone())
                    .and_modify(|candidate| {
                        if candidate.as_deref() != Some(provider) {
                            *candidate = None;
                        }
                    })
                    .or_insert_with(|| Some(provider.to_string()));
            }
        }
    }
    Ok(candidates
        .into_iter()
        .filter_map(|(path, provider)| provider.map(|id| (path, id)))
        .collect())
}

/// 递归遍历 `root`，收集扩展名恰为 `extension`（不含点）的文件路径（正斜杠归一）。
///
/// 返回 `(paths, complete)`：`complete = false` 表示遍历中途遇到不可读目录
/// （权限错误等），此时返回已收集到的路径并标记不完整——调用方据此对受影响
/// provider 的源设置 `relation_complete = false`，从而不推导 tombstone（R2）。
///
/// 不跟随符号链接（避免循环 / 越出数据根）；不读取文件内容，只枚举路径。
fn discover_provider_sources(root: &std::path::Path, extension: &str) -> (Vec<String>, bool) {
    let mut paths = Vec::new();
    let mut complete = true;
    let root_type = match std::fs::symlink_metadata(root) {
        Ok(metadata) => metadata.file_type(),
        Err(_) => return (paths, false),
    };
    if root_type.is_symlink() || !root_type.is_dir() {
        return (paths, false);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => {
                complete = false;
                continue;
            }
        };
        for entry_result in entries {
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(_) => {
                    complete = false;
                    continue;
                }
            };
            let file_type = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => {
                    complete = false;
                    continue;
                }
            };
            let path = entry.path();
            // 不跟随符号链接：file_type() 对 symlink 返回 symlink 类型而非目标。
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file()
                && path.extension().and_then(|e| e.to_str()) == Some(extension)
            {
                paths.push(source_path_identity(&path.to_string_lossy()));
            }
        }
    }
    paths.sort();
    (paths, complete)
}

/// `sync --discover` 的 per-provider 发现结果。
struct ProviderDiscovery {
    id: String,
    found: usize,
    removed: usize,
    complete: bool,
}

/// 执行 `sync --discover`：遍历所有已知 provider 的数据根，按各自登记的扩展名收集
/// 源文件，diff 已存路径合成空批 tombstone（仅完整扫描时），并把发现的路径与合成批
/// 一起交给 [`sync_files`] 的核心流程。
///
/// 隐私：结果只报计数与 provider id，绝不包含绝对 transcript 路径。
fn sync_discover(
    store: &SqliteStore,
    progress: bool,
    request_id: Option<&str>,
) -> Result<(serde_json::Value, Vec<String>), CliError> {
    let mut all_paths: Vec<String> = Vec::new();
    let mut providers_out: Vec<ProviderDiscovery> = Vec::new();
    let mut discovered_provider_ids: BTreeMap<String, String> = BTreeMap::new();
    let mut overall_complete = true;
    // 每个 provider 的 (provider_id, discovered_paths, complete)
    let mut per_provider: Vec<(String, Vec<String>, bool)> = Vec::new();
    for adapter in provider_registry() {
        let pid = adapter.provider_id().to_string();
        let extension = PROVIDER_DISCOVERY_ROOTS
            .iter()
            .find(|(provider, _, _)| *provider == pid)
            .map(|(_, _, extension)| *extension);
        let mut roots: BTreeMap<String, std::path::PathBuf> = BTreeMap::new();
        if extension.is_some() {
            for root in store
                .active_installation_roots(&pid)
                .map_err(ProtocolError::from_private_port_error)?
            {
                let key =
                    agent_session_grep_application::relocation::normalize_absolute_path(&root)
                        .map_err(ProtocolError::from_private_port_error)?;
                roots.insert(key, std::path::PathBuf::from(root));
            }
            if let Some((root, _)) = provider_discovery_target(&pid) {
                // A missing default home root is not evidence against an
                // explicitly relocated installation. Missing registered roots
                // still make the provider scan incomplete.
                if roots.is_empty() || root.is_dir() {
                    let key = agent_session_grep_application::relocation::normalize_absolute_path(
                        &root.to_string_lossy(),
                    )
                    .map_err(ProtocolError::from_private_port_error)?;
                    roots.entry(key).or_insert(root);
                }
            }
        }
        let mut paths = BTreeMap::new();
        let mut complete = !roots.is_empty();
        if let Some(extension) = extension {
            for root in roots.into_values() {
                let (found, root_complete) = discover_provider_sources(&root, extension);
                for path in found {
                    let key =
                        agent_session_grep_application::relocation::normalize_absolute_path(&path)
                            .map_err(ProtocolError::from_private_port_error)?;
                    paths.entry(key).or_insert(path);
                }
                complete = complete && root_complete;
            }
        }
        let paths: Vec<String> = paths.into_values().collect();
        overall_complete = overall_complete && complete;
        per_provider.push((pid.clone(), paths.clone(), complete));
        let found = paths.len();
        for path in &paths {
            discovered_provider_ids.insert(path.clone(), pid.clone());
        }
        all_paths.extend(paths.iter().cloned());
        providers_out.push(ProviderDiscovery {
            id: pid,
            found,
            removed: 0,
            complete,
        });
    }
    // dedup all_paths preserving order
    let mut unique: Vec<String> = Vec::with_capacity(all_paths.len());
    for p in &all_paths {
        if !unique.iter().any(|e| e == p) {
            unique.push(p.clone());
        }
    }
    let incomplete_providers: BTreeSet<String> = per_provider
        .iter()
        .filter(|(_, _, complete)| !complete)
        .map(|(pid, _, _)| pid.clone())
        .collect();
    let incomplete_paths: BTreeSet<String> = per_provider
        .iter()
        .filter(|(_, _, complete)| !complete)
        .flat_map(|(_, paths, _)| paths.iter().cloned())
        .collect();
    let relation_recovery_paths = store
        .source_paths_requiring_relation_scan(&unique)
        .map_err(ProtocolError::from)?;
    // Prior-path diff per provider：仅完整扫描时合成空批 tombstone。
    let mut synthetic_batches: Vec<SourceBatch> = Vec::new();
    for (i, (pid, paths, complete)) in per_provider.iter().enumerate() {
        if !complete {
            continue;
        }
        let prior = store
            .source_paths_for_provider(pid.as_str())
            .map_err(ProtocolError::from)?;
        let discovered_for_provider: BTreeSet<String> = paths
            .iter()
            .map(|path| agent_session_grep_application::relocation::normalize_absolute_path(path))
            .collect::<Result<_, _>>()
            .map_err(ProtocolError::from_private_port_error)?;
        let mut removed = 0usize;
        for prior_path in &prior {
            let key =
                agent_session_grep_application::relocation::normalize_absolute_path(prior_path)
                    .map_err(ProtocolError::from_private_port_error)?;
            if !discovered_for_provider.contains(&key) {
                // 源曾在该 provider 下被 sync，本次完整扫描未出现在磁盘上 → 合成空批。
                synthetic_batches.push(SourceBatch {
                    source_path: prior_path.clone(),
                    entries: Vec::new(),
                    placements: Vec::new(),
                    edges: Vec::new(),
                    activities: Vec::new(),
                    usage_events: Vec::new(),
                    relation_complete: true,
                    len_bytes: None,
                    fingerprint: None,
                    provider_id: Some(pid.clone()),
                    resume_claims: Vec::new(),
                });
                removed += 1;
            }
        }
        providers_out[i].removed = removed;
    }
    // 把发现的路径交给 sync_files 核心（绕过目录拒绝 guard）。
    // 若既无发现的源也无被删除的源（例如本机未安装任何 provider），返回一个
    // 不推进 generation 的空成功，而非 usage error——discover 空跑是合法状态。
    let (sync_data, warnings) = if unique.is_empty() && synthetic_batches.is_empty() {
        let generation = store.active_generation().map_err(ProtocolError::from)?;
        (
            serde_json::json!({
                "sources": 0,
                "emitted": 0,
                "messages": 0,
                "committed": 0,
                "unchanged": 0,
                "skipped": 0,
                "diagnostics": 0,
                "generation": generation,
            }),
            Vec::new(),
        )
    } else {
        sync_files_inner(
            store,
            &unique,
            &SyncContext {
                synthetic_batches,
                incomplete_providers,
                relation_recovery_paths,
                incomplete_paths,
                discovered_provider_ids,
            },
            true,
            progress,
            request_id,
        )?
    };
    // 组装 discovery 结果对象（绝不含绝对路径）。
    let providers_json: Vec<serde_json::Value> = providers_out
        .iter()
        .map(|p| {
            serde_json::json!({
                "id": p.id,
                "found": p.found,
                "removed": p.removed,
                "complete": p.complete,
            })
        })
        .collect();
    let mut data = sync_data;
    if let Some(obj) = data.as_object_mut() {
        obj.insert(
            "discovery".into(),
            serde_json::json!({
                "complete": overall_complete,
                "providers": providers_json,
            }),
        );
    }
    Ok((data, warnings))
}

/// 派生一条消息的稳定 id：native id 优先，缺失回退 document-scoped 派生。
///
/// 消息实体与工具活动锚点必须用同一规则，否则 anchor 解析会指向不存在的实体。
/// 回退事实刻意 path-free（provider + variant + document id + seq），因此源文件
/// 移动位置不改变 id；但 seq 会随 provider 记录过滤规则变化而漂移，故只能标
/// `Unstable`，不承诺跨运行稳定。
fn derive_message_id(
    native_id: &str,
    seq: u32,
    provider_id: &str,
    variant: &str,
    document_wire: &str,
) -> Result<StableId, CliError> {
    if native_id.is_empty() {
        Ok(StableId::derive(
            IdKind::Message,
            Stability::Unstable,
            &[
                provider_id.as_bytes(),
                variant.as_bytes(),
                document_wire.as_bytes(),
                &seq.to_le_bytes(),
            ],
        ))
    } else {
        Ok(StableId::native_checked(IdKind::Message, native_id)?)
    }
}

#[cfg(test)]
fn staged_to_source(
    path: &str,
    staged: &StagedBatch,
    provider_id: &str,
    variant: &str,
    fingerprint: &str,
    source_len: u64,
) -> Result<SourceBatch, CliError> {
    staged_to_source_with_provider(
        path,
        staged,
        provider_id,
        variant,
        fingerprint,
        source_len,
        None,
    )
}

#[cfg(test)]
fn staged_to_source_with_provider(
    path: &str,
    staged: &StagedBatch,
    provider_id: &str,
    variant: &str,
    fingerprint: &str,
    source_len: u64,
    discovered_provider_id: Option<&str>,
) -> Result<SourceBatch, CliError> {
    staged_to_source_with_provider_namespace(
        path,
        staged,
        provider_id,
        variant,
        fingerprint,
        source_len,
        discovered_provider_id,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn staged_to_source_with_provider_namespace(
    path: &str,
    staged: &StagedBatch,
    provider_id: &str,
    variant: &str,
    fingerprint: &str,
    source_len: u64,
    discovered_provider_id: Option<&str>,
    persisted_installation_namespace: Option<&str>,
) -> Result<SourceBatch, CliError> {
    if staged.report.committed != staged.messages.len() {
        return Err(DomainError::InvariantViolation(format!(
            "provider reported {} committed messages but emitted {}",
            staged.report.committed,
            staged.messages.len()
        ))
        .into());
    }

    // 文档实体：内容寻址——同字节重 ingest 得到同一 id（幂等）。
    let document_id = StableId::derive(
        IdKind::Document,
        Stability::Reconstructed,
        &[
            provider_id.as_bytes(),
            variant.as_bytes(),
            fingerprint.as_bytes(),
        ],
    );
    // Report-level identity remains the single-session compatibility path.
    // Explicit per-message identities below take precedence for multi-session sources.
    let install_ns = persisted_installation_namespace
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| installation_namespace(path, provider_id));
    let fallback_session_id = match staged.report.session_native_id.as_deref() {
        Some(sid) if !sid.trim().is_empty() => StableId::native_session_scoped(
            &SessionIdentityNamespace {
                provider_id,
                installation_namespace: &install_ns,
            },
            sid,
        ),
        _ => StableId::derive(
            IdKind::Session,
            Stability::Reconstructed,
            &[document_id.as_str().as_bytes()],
        ),
    };

    if staged.report.session_observation.multi_session
        && staged
            .messages
            .iter()
            .any(|message| message.session.is_none())
    {
        return Err(DomainError::InvalidRequest(
            "provider reported multiple sessions without per-message session identities".into(),
        )
        .into());
    }

    let mut session_members = BTreeMap::<String, (StableId, Vec<String>, BTreeSet<String>)>::new();
    let mut resume_claims_by_session = BTreeMap::<String, SourceResumeClaim>::new();
    let mut message_session_ids = Vec::with_capacity(staged.messages.len());
    for message in &staged.messages {
        let (session_id, observation) = match message.session.as_ref() {
            Some(identity) => {
                if identity.source_key.trim().is_empty() {
                    return Err(DomainError::InvalidRequest(
                        "provider emitted an empty source-local session key".into(),
                    )
                    .into());
                }
                let native_id = match &identity.observation.provider_session_id {
                    agent_session_grep_ports::MetadataResolution::Resolved(value)
                        if !value.trim().is_empty() =>
                    {
                        Some(value.as_str())
                    }
                    _ => None,
                };
                let session_id = if let Some(native_id) = native_id {
                    StableId::native_session_scoped(
                        &SessionIdentityNamespace {
                            provider_id,
                            installation_namespace: &install_ns,
                        },
                        native_id,
                    )
                } else {
                    StableId::derive(
                        IdKind::Session,
                        Stability::Unstable,
                        &[
                            provider_id.as_bytes(),
                            variant.as_bytes(),
                            document_id.as_str().as_bytes(),
                            identity.source_key.as_bytes(),
                        ],
                    )
                };
                (session_id, Some(&identity.observation))
            }
            None => (
                fallback_session_id.clone(),
                Some(&staged.report.session_observation),
            ),
        };

        let session_key = session_id.as_str().to_string();
        session_members
            .entry(session_key.clone())
            .or_insert_with(|| (session_id.clone(), Vec::new(), BTreeSet::new()));
        if provider_id != "empty"
            && let Some(observation) = observation
        {
            let claim =
                SourceResumeClaim::from_observation(provider_id, session_id.as_str(), observation);
            if let Some(existing) = resume_claims_by_session.get(&session_key) {
                if existing != &claim {
                    return Err(DomainError::InvalidRequest(
                        "conflicting resume metadata for one source session".into(),
                    )
                    .into());
                }
            } else {
                resume_claims_by_session.insert(session_key, claim);
            }
        }
        message_session_ids.push(session_id);
    }
    if staged.messages.is_empty() {
        let session_key = fallback_session_id.as_str().to_string();
        session_members.insert(
            session_key.clone(),
            (fallback_session_id.clone(), Vec::new(), BTreeSet::new()),
        );
        if provider_id != "empty" {
            resume_claims_by_session.insert(
                session_key,
                SourceResumeClaim::from_observation(
                    provider_id,
                    fallback_session_id.as_str(),
                    &staged.report.session_observation,
                ),
            );
        }
    }

    let mut entities = BTreeMap::<String, StagedMessageEntity>::new();
    let mut placements = Vec::with_capacity(staged.messages.len());
    let mut edges = Vec::new();
    for (message, session_id) in staged.messages.iter().zip(&message_session_ids) {
        if let Some((start, end)) = message.span
            && (end < start || end > source_len)
        {
            return Err(DomainError::InvariantViolation(
                "provider emitted a span outside the verified source document".into(),
            )
            .into());
        }
        let id = derive_message_id(
            &message.native_id,
            message.seq,
            provider_id,
            variant,
            document_id.as_str(),
        )?;
        let span = message.span.map(|(start, end)| EvidenceSpan { start, end });
        let placement = MessagePlacement::new(
            session_id.clone(),
            document_id.clone(),
            id.clone(),
            message.seq,
            message.is_sidechain,
            span,
        );
        let parent_id = message
            .parent_native_id
            .as_deref()
            .filter(|parent| !parent.is_empty())
            .map(|parent| StableId::native_checked(IdKind::Message, parent))
            .transpose()?;
        if let Some(parent_message_id) = &parent_id {
            edges.push(MessageEdge {
                child_placement_id: placement.id.clone(),
                parent_message_id: parent_message_id.clone(),
                parent_native_id: message.parent_native_id.clone(),
                // 会话树血缘诚实分类：provider 格式唯一能证明的边类型是 claude
                // 的 isSidechain（subagent/分支标记，见 provider-claude 字段注释
                // 与 ToolActivityActor 同源分类）→ Subagent；主线消息的边保持
                // Reply。fork/retry/continuation 无格式字段区分（parentUuid 不
                // 携带类型），绝不臆造——hstry fork_type 三分类的诚实子集。
                relation: if message.is_sidechain {
                    MessageRelation::Subagent
                } else {
                    MessageRelation::Reply
                },
            });
        }
        let (member_session, members, seen_members) = session_members
            .get_mut(session_id.as_str())
            .expect("every staged message session was registered");
        if member_session != session_id {
            return Err(DomainError::InvariantViolation(
                "session member map has a conflicting canonical identity".into(),
            )
            .into());
        }
        if seen_members.insert(id.as_str().to_string()) {
            members.push(id.as_str().to_string());
        }
        match entities.entry(id.as_str().to_string()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(StagedMessageEntity {
                    id: id.clone(),
                    role: message.role.clone(),
                    text: message.text.clone(),
                    timestamp: message.timestamp.clone(),
                    occurrences: vec![(
                        placement.clone(),
                        parent_id,
                        message.parent_native_id.clone(),
                    )],
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let entity = entry.get_mut();
                if entity.id != id
                    || entity.role != message.role
                    || entity.text != message.text
                    || entity.timestamp != message.timestamp
                {
                    return Err(DomainError::InvariantViolation(
                        "message has conflicting stable projections within one source".into(),
                    )
                    .into());
                }
                entity.occurrences.push((
                    placement.clone(),
                    parent_id,
                    message.parent_native_id.clone(),
                ));
            }
        }
        placements.push(placement);
    }

    let mut entries = Vec::with_capacity(entities.len() + 2);
    for entity in entities.into_values() {
        let mut occurrences = entity.occurrences;
        occurrences.sort_by(|left, right| {
            (left.0.source_ordinal, left.0.id.as_str())
                .cmp(&(right.0.source_ordinal, right.0.id.as_str()))
        });
        let parent_facts: Vec<Option<String>> = occurrences
            .iter()
            .map(|(_, parent_id, _)| parent_id.as_ref().map(|parent| parent.as_str().to_string()))
            .collect();
        let parent = match parent_facts.first() {
            Some(first) if parent_facts.iter().all(|fact| fact == first) => first.clone(),
            _ => None,
        };
        let parent_native_facts: Vec<Option<String>> = occurrences
            .iter()
            .map(|(_, _, parent_native_id)| parent_native_id.clone())
            .collect();
        let parent_native_id = match parent_native_facts.first() {
            Some(first) if parent_native_facts.iter().all(|fact| fact == first) => first.clone(),
            _ => None,
        };
        let is_sidechain = match occurrences.first() {
            Some((first, _, _))
                if occurrences
                    .iter()
                    .all(|(placement, _, _)| placement.is_sidechain == first.is_sidechain) =>
            {
                serde_json::Value::Bool(first.is_sidechain)
            }
            _ => serde_json::Value::Null,
        };
        let spans: Vec<serde_json::Value> = occurrences
            .iter()
            .filter_map(|(placement, _, _)| {
                placement.span.as_ref().map(|span| {
                    serde_json::json!({
                        "placement_id": placement.id.as_str(),
                        "document": placement.source_document_id.as_str(),
                        "start": span.start,
                        "end": span.end,
                    })
                })
            })
            .collect();
        let session_ids: BTreeSet<String> = occurrences
            .iter()
            .map(|(placement, _, _)| placement.session_id.as_str().to_string())
            .collect();
        let session_ids: Vec<String> = session_ids.into_iter().collect();
        let primary_session = (session_ids.len() == 1).then(|| session_ids[0].clone());
        let payload = serde_json::json!({
            "role": entity.role,
            "text": entity.text,
            "timestamp": entity.timestamp,
            "parent": parent,
            "parent_native_id": parent_native_id,
            "is_sidechain": is_sidechain,
            "session": primary_session,
            "sessions": session_ids,
            "span": spans.first().map(|span| serde_json::json!({
                "start": span["start"],
                "end": span["end"],
            })),
            "spans": spans,
        })
        .to_string()
        .into_bytes();
        // FTS 投影有界（借鉴清单 #3）：text 元素截断到 MESSAGE_FTS_MAX_CHARS，
        // 与索引侧/重投影侧同一上限，current 判定两侧才比较同一有界文本；
        // payload 保留 provider 原文全文（Catalog 不在索引期改写原文）。
        entries.push((entity.id, payload, bounded_index_text(&entity.text)));
    }

    let document_payload = serde_json::json!({
        "provider": provider_id,
        "variant": variant,
        "fingerprint": fingerprint,
        "len": source_len,
    })
    .to_string();
    let document_wire = document_id.as_str().to_string();
    entries.reserve(session_members.len() + 1);
    for (session_id, member_ids, _) in session_members.values() {
        let session_payload = serde_json::json!({
            "documents": [document_wire],
            "document": document_wire,
            "messages": member_ids,
        })
        .to_string();
        entries.push((
            session_id.clone(),
            session_payload.into_bytes(),
            String::new(),
        ));
    }
    entries.push((document_id, document_payload.into_bytes(), String::new()));

    // 工具活动锚点解析（设计 R5.3）：把 provider-native 锚点 id 解析为本批的
    // 稳定消息 id。规则与消息实体去重一致（seq 顺序首现匹配、native 优先、
    // 缺省回退派生）；锚点消息未被 emit（skipped/非对话）→ 活动丢弃，绝不臆造。
    let mut activities = Vec::new();
    for staged_activity in &staged.activities {
        // 空锚点 id 无法标识任何一条消息：`find` 会无差别命中首条同样缺 native id
        // 的消息，把活动挂到错误的消息上。缺 native id 的 provider 不发 activity，
        // 故此处 fail-closed 丢弃，绝不猜锚点。
        if staged_activity.message_native_id.trim().is_empty() {
            continue;
        }
        let Some(anchor) = staged
            .messages
            .iter()
            .find(|message| message.native_id == staged_activity.message_native_id)
        else {
            continue;
        };
        let id = derive_message_id(
            &anchor.native_id,
            anchor.seq,
            provider_id,
            variant,
            &document_wire,
        )?;
        activities.push(SourceActivity {
            message_id: id,
            activity: staged_activity.activity.clone(),
        });
    }

    // token 用量事件锚点解析（usage 维度）：message_native_id 非空时按与消息
    // 实体去重一致的规则解析本批稳定消息 id；空串是 session 级观察（如 Codex
    // token_count），message_id 记 None。锚点消息未被 emit → 事件丢弃，绝不臆造。
    let mut usage_events = Vec::new();
    for staged_usage in &staged.usage_events {
        let anchor_index = if staged_usage.message_native_id.trim().is_empty() {
            None
        } else {
            let matching = staged
                .messages
                .iter()
                .enumerate()
                .filter(|(_, message)| message.native_id == staged_usage.message_native_id)
                .collect::<Vec<_>>();
            if matching.is_empty()
                || matching.iter().any(|(index, _)| {
                    message_session_ids[*index] != message_session_ids[matching[0].0]
                })
            {
                continue;
            }
            Some(matching[0].0)
        };
        let message_id = anchor_index
            .map(|index| {
                let anchor = &staged.messages[index];
                derive_message_id(
                    &anchor.native_id,
                    anchor.seq,
                    provider_id,
                    variant,
                    &document_wire,
                )
            })
            .transpose()?;
        let session_id = match anchor_index {
            Some(index) => message_session_ids[index].clone(),
            None if session_members.len() == 1 => session_members
                .values()
                .next()
                .expect("one session")
                .0
                .clone(),
            None => continue,
        };
        usage_events.push(SourceUsage {
            session_id,
            message_id,
            usage: staged_usage.usage.clone(),
        });
    }

    let resume_claims = resume_claims_by_session.into_values().collect();

    Ok(SourceBatch {
        source_path: path.to_string(),
        entries,
        placements,
        edges,
        activities,
        usage_events,
        relation_complete: staged.report.skipped == 0,
        len_bytes: Some(source_len as i64),
        fingerprint: Some(fingerprint.to_string()),
        provider_id: discovered_provider_id.map(str::to_string),
        resume_claims,
    })
}

/// 读取原始 .jsonl 文件并 ingest：
/// capture 快照 → stage（probe+缓冲 parse）→ verify 快照 → 单事务原子提交。
///
/// 落实：
/// - RFC-0002 §4 ReadOnlySourceSnapshot：len+mtime+fingerprint 复核；
/// - RFC-0002 §5 source-level staging：parse 只缓冲，成功后才 commit_batch。
///
/// 会话 fact 用文件路径，保证同文件重 ingest 得到稳定 id（幂等重索引）。
fn ingest_file(
    store: &SqliteStore,
    path: &str,
) -> Result<(serde_json::Value, Vec<String>), CliError> {
    let normalized = source_input_path(store, path)?;
    let path = normalized.as_str();
    let path_ref = std::path::Path::new(path);
    // 1) 捕获只读源快照（只计算 len/mtime/fingerprint，不保留源字节）。
    let snap = capture(path_ref).map_err(ProtocolError::from)?;
    let source = open_snapshot_source(path_ref, &snap).map_err(ProtocolError::from)?;

    // 0 字节源没有 provider/variant 证据：首次见到就是空的源只报 warning，
    // 不写绑定、scan 行或占位实体；已扫描过的源走诚实空替换（沿用既有绑定）。
    if source.is_empty() {
        let known = store
            .source_fingerprints(&[path.to_string()])
            .map_err(ProtocolError::from)?
            .contains_key(path);
        let mut warnings = Vec::new();
        if known {
            verify_snapshot(path_ref, &snap).map_err(ProtocolError::from)?;
            let batch = empty_source_batch(path, &snap.fingerprint, None);
            store
                .commit_source_batches_if_changed(std::slice::from_ref(&batch))
                .map_err(ProtocolError::from)?;
        } else {
            warnings.push(
                "empty source has no provider evidence and nothing indexed; nothing was \
                 committed — re-run ingest once it contains data"
                    .into(),
            );
        }
        let generation = store.active_generation().map_err(ProtocolError::from)?;
        return Ok((
            serde_json::json!({
                "variant": "empty",
                "emitted": 0,
                "committed": 0,
                "unchanged": 0,
                "skipped": 0,
                "diagnostics": warnings.len(),
                "generation": generation,
                "source_fp": snap.fingerprint,
            }),
            warnings,
        ));
    }

    // 2) probe-select + stage：每个 probe/parse 都从只读 source 重新打开 bounded reader。
    //    路径落在某个登记根之下时该归属是布局事实，按 root 表反查后收缩候选集
    //    （见 stage_with_source）；否则为 None，走全 registry probe。
    let registered = registered_provider_hints(store, &[path.to_string()])?;
    let hint = registered
        .get(path)
        .map(String::as_str)
        .or_else(|| provider_for_source_path(path));
    let (staged, variant) = stage_with_source(&source, hint)?;

    // 3) 提交前复核：源在 stage 期间被改写则拒绝提交（RFC-0002 §4）。
    verify_snapshot(path_ref, &snap).map_err(ProtocolError::from)?;

    // 4) 派生 id + 构造该源的完整 scan 结果（消息 + 会话/文档目录行），按
    //    source membership 提交。同文件重 ingest 时，本次消失的 id 会被推导为 tombstone。
    let provider = variant.split('/').next().unwrap_or(&variant).to_string();
    let legacy_namespace = installation_namespace(path, &provider);
    let persisted_namespace = store
        .resolve_or_allocate_installation_namespace(&provider, path, &legacy_namespace)
        .map_err(ProtocolError::from_private_port_error)?;
    let source = staged_to_source_with_provider_namespace(
        path,
        &staged,
        &provider,
        &variant,
        &snap.fingerprint,
        snap.len,
        None,
        Some(persisted_namespace.as_str()),
    )?;
    let changed = store
        .commit_source_batches_if_changed(std::slice::from_ref(&source))
        .map_err(ProtocolError::from)?;

    let generation = store.active_generation().map_err(ProtocolError::from)?;
    let partial_warning = (staged.report.skipped > 0).then_some(PARTIAL_SOURCE_WARNING);
    let diagnostic_count = staged.report.diagnostics.len() + usize::from(partial_warning.is_some());
    let warnings = diagnostic_warnings(
        partial_warning
            .into_iter()
            .chain(staged.report.diagnostics.iter().map(String::as_str)),
        diagnostic_count,
    );
    Ok((
        serde_json::json!({
            "variant": variant,
            "emitted": staged.messages.len(),
            "committed": if changed { staged.messages.len() } else { 0 },
            "unchanged": if changed { 0 } else { staged.messages.len() },
            "skipped": staged.report.skipped,
            "diagnostics": diagnostic_count,
            "generation": generation,
            "source_fp": snap.fingerprint,
        }),
        warnings,
    ))
}

/// JSONL 源健康度三态分类。改编自 fast-resume 的 `jsonl_health`
/// （`src/adapters/shared.rs`，MIT License，Copyright (c) 2025 Stanislas Lange）：
///
/// - `Clean`：全部非空行都是合法 JSON；
/// - `Partial`：存在坏行但之后仍有合法行——recoverable，解析时逐行跳过
///   （provider 既有的 recoverable-skip 语义，此处只做源级分类）；
/// - `Invalid`：坏行之后没有合法行——典型是尾部截断（EOF 落在记录中间，
///   agent 正在写文件）。对已索引源采取 Retain：保留旧索引、不重 parse。
///
/// 行切分按字节（`\n`，容忍 `\r\n`），首行剥离 UTF-8 BOM，与 provider
/// 解析语义一致。经 [`for_each_bounded_source_line`] 流式遍历，内存上界为
/// 单条记录（RFC-0002 §7），绝不把整源读入内存。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonlHealth {
    Clean,
    Partial,
    Invalid,
}

fn jsonl_health(
    path: &std::path::Path,
    snapshot: &agent_session_grep_ports::SourceSnapshot,
) -> Result<JsonlHealth, agent_session_grep_ports::ProviderError> {
    let source = open_snapshot_source(path, snapshot)
        .map_err(|error| agent_session_grep_ports::ProviderError::Io(error.to_string()))?;
    let mut valid_rows = 0usize;
    let mut malformed_rows = 0usize;
    let mut valid_after_last_malformed = false;
    agent_session_grep_ports::for_each_bounded_source_line(
        &source,
        agent_session_grep_ports::STREAM_RECORD_MAX_BYTES,
        |line| {
            if line.bytes.iter().all(u8::is_ascii_whitespace) {
                return Ok(());
            }
            if serde_json::from_slice::<serde_json::Value>(line.bytes).is_err() {
                malformed_rows += 1;
                valid_after_last_malformed = false;
            } else {
                valid_rows += 1;
                if malformed_rows > 0 {
                    valid_after_last_malformed = true;
                }
            }
            Ok(())
        },
    )?;
    Ok(match (valid_rows, malformed_rows) {
        (_, 0) => JsonlHealth::Clean,
        (0, _) => JsonlHealth::Invalid,
        _ if valid_after_last_malformed => JsonlHealth::Partial,
        _ => JsonlHealth::Invalid,
    })
}

/// 同步显式给定的源文件：所有文件先完成 capture + stage + verify，之后才提交
/// 一个 durable batch。这样任一文件失败都不会留下其它文件的部分更新。
/// `progress` 为 true（仅 jsonl 模式）时逐源发 progress frame——staging 是
/// 长任务里唯一逐文件推进的阶段，提交本身是单事务不可分。
///
/// 扫描期健康分诊（fast-resume `jsonl_health` 模式）：已索引过的源若检出截断尾
/// （`JsonlHealth::Invalid`，agent 正在写），Retain——不重 parse、不推进指纹、
/// 不提交，保留旧索引并报诊断，避免 rebuild churn 与误 tombstone；写完后再 sync
/// 会因指纹不匹配走完整重扫。新源（无缓存指纹）没有旧索引可保留，照常走
/// recoverable-skip：有效前缀提交、截断行计入 skipped + 诊断（relation_complete
/// = false，store 层不推导 tombstone）。
fn sync_files(
    store: &SqliteStore,
    paths: &[String],
    progress: bool,
    request_id: Option<&str>,
) -> Result<(serde_json::Value, Vec<String>), CliError> {
    if paths.is_empty() {
        return Err(CliError::usage("sync <file>... requires at least one file"));
    }
    // 新手第一本能是给 sync 传整个目录；目录不是 .jsonl 文件，捕获要读它时会
    // 报"拒绝访问 (os error 5)"，误导新手去折腾权限/杀毒（10 角色体验测试缺陷）。
    // 这里显式拦截并给出正确用法。消息不带路径（隐私：用户目录布局不外泄），
    // 展开示例保持平台中立（不给 PowerShell-only 的 Get-ChildItem 例子，R2.2）。
    // discover 路径不走此 guard——它已经枚举了文件而非目录。
    for path in paths {
        if std::path::Path::new(path).is_dir() {
            return Err(CliError::usage(
                "sync 接受一个或多个 transcript 文件，不接受目录；\
                 需要同步整个目录时，请用你的 shell 展开文件列表，把文件逐个传给 sync",
            ));
        }
    }
    // 重复路径去重（保持出现顺序）：同一文件列两次是书写冗余而非两个源；
    // 不去重会让 store 层把同一路径当两个 source batch 提交而判 catalog_error
    // （exit 6）——sync 幂等语义下应提前归一为单个源。
    let mut unique: Vec<String> = Vec::with_capacity(paths.len());
    for path in paths {
        let path = source_input_path(store, path)?;
        if !unique.iter().any(|existing| existing == &path) {
            unique.push(path);
        }
    }
    sync_files_inner(
        store,
        &unique,
        &SyncContext::default(),
        false,
        progress,
        request_id,
    )
}

/// `sync_files_inner` 的 discover 扩展上下文：plain sync 全部为空，`sync --discover`
/// 注入合成空批、完整性降级集合与 path→provider 归属表。
#[derive(Default)]
struct SyncContext {
    /// discover 为已删除源合成的空批（`relation_complete = true`），不经过
    /// capture（文件已不在磁盘上），直接追加进提交批次以触发 tombstone。
    synthetic_batches: Vec<SourceBatch>,
    /// 部分 root scan 的 provider：其批次 relation_complete 降级为 false。
    incomplete_providers: BTreeSet<String>,
    /// 强制重扫之前关系不完整的源（无视指纹缓存）。
    relation_recovery_paths: BTreeSet<String>,
    /// 之前扫描 skipped>0 的源路径（同样绕过指纹跳过）。
    incomplete_paths: BTreeSet<String>,
    /// discover 归属表：源路径 → provider id；落入 `source_scans.provider_id`。
    discovered_provider_ids: BTreeMap<String, String>,
}

/// `sync_files` / `sync_discover` 共享的核心流程。
///
/// `paths` 应已去重且不含目录（调用方负责）。
fn sync_files_inner(
    store: &SqliteStore,
    paths: &[String],
    ctx: &SyncContext,
    allow_empty: bool,
    progress: bool,
    request_id: Option<&str>,
) -> Result<(serde_json::Value, Vec<String>), CliError> {
    let synthetic_batches = &ctx.synthetic_batches;
    let incomplete_providers = &ctx.incomplete_providers;
    let relation_recovery_paths = &ctx.relation_recovery_paths;
    let incomplete_paths = &ctx.incomplete_paths;
    let discovered_provider_ids = &ctx.discovered_provider_ids;
    if paths.is_empty() && synthetic_batches.is_empty() && !allow_empty {
        return Err(CliError::usage("sync <file>... requires at least one file"));
    }
    let registered_providers = registered_provider_hints(store, paths)?;
    let mut sources = Vec::with_capacity(paths.len() + synthetic_batches.len());
    let mut snapshots = Vec::with_capacity(paths.len());
    let mut message_count = 0usize;
    let mut skipped_count = 0usize;
    let mut retained_count = 0usize;
    let mut diagnostic_count = 0usize;
    let mut diagnostics = Vec::new();
    // Env-gated measurement scaffolding; zero-cost unless ASG_INDEX_TRACE is set.
    let mut trace_prelude = std::time::Duration::ZERO;
    let mut trace_namespace = std::time::Duration::ZERO;
    let mut trace_capture = std::time::Duration::ZERO;
    let mut trace_health = std::time::Duration::ZERO;
    let mut trace_stage = std::time::Duration::ZERO;
    let mut trace_assemble = std::time::Duration::ZERO;

    // 指纹缓存：capture 后先与已存指纹比对，未变化的源跳过重复解析
    // （parse 是大语料重扫的主导成本）。指纹缓存缺失/不匹配才走完整路径。
    let trace_started = trace::begin();
    let cached = store
        .source_fingerprints(paths)
        .map_err(ProtocolError::from)?;
    let mut unchanged_messages = 0usize;
    let mut provider_id_backfills = Vec::new();
    // 每个源本次实际 staged 的消息数，供推迟（deferred）时从 emitted 里扣回，
    // 否则报告会把没提交的消息算进 emitted。
    let mut staged_messages_by_path: BTreeMap<String, usize> = BTreeMap::new();
    let unchanged_counts = store
        .source_message_counts(paths)
        .map_err(ProtocolError::from)?;
    trace_prelude += trace::elapsed(trace_started);
    for (index, path) in paths.iter().enumerate() {
        let hint = discovered_provider_ids
            .get(path)
            .map(String::as_str)
            .or_else(|| registered_providers.get(path).map(String::as_str))
            .or_else(|| provider_for_source_path(path));
        let trace_started = trace::begin();
        let resolved_namespace = hint
            .map(|provider| {
                store
                    .resolve_or_allocate_installation_namespace(
                        provider,
                        path,
                        &installation_namespace(path, provider),
                    )
                    .map_err(ProtocolError::from_private_port_error)
            })
            .transpose()?;
        trace_namespace += trace::elapsed(trace_started);
        let path_ref = std::path::Path::new(path);
        let trace_started = trace::begin();
        let snap = capture(path_ref).map_err(ProtocolError::from)?;
        let source = open_snapshot_source(path_ref, &snap).map_err(ProtocolError::from)?;
        trace_capture += trace::elapsed(trace_started);
        let cached_scan = cached.get(path);
        let cached_fp = cached_scan.and_then(|(_, fp, _)| fp.clone());
        // 解析语义版本参与 unchanged 判定（借鉴 Recall 的 parser_version 增量
        // 同步）：字节未变但已存版本落后于 PARSER_SEMANTIC_VERSION 的源必须
        // 重解析（targeted backfill），而不是滞留旧解析结果直到手动 rebuild
        // 或源文件变化。
        let cached_version = cached_scan.map(|(_, _, version)| *version);
        let version_stale = cached_fp.as_deref() == Some(snap.fingerprint.as_str())
            && cached_version != Some(i64::from(PARSER_SEMANTIC_VERSION));
        // 0 字节源必须先于任何 provider 探测处理：它没有 provider/variant
        // 证据。首次见到就是空的源只报诊断，不写绑定、scan 行或占位实体；
        // 已扫描过的源是诚实的整源清空批次，由既有 replacement 推导
        // tombstone，并沿用已证明的安装绑定。
        let mut retained = false;
        let mut empty_cleared = false;
        let mut empty_skipped = false;
        let (staged, variant) = if source.is_empty() {
            let repeat_of_empty =
                cached_scan.is_some_and(|(stored_len, stored_fp, stored_version)| {
                    *stored_len == Some(0)
                        && stored_fp.as_deref() == Some(snap.fingerprint.as_str())
                        && *stored_version == i64::from(PARSER_SEMANTIC_VERSION)
                        && !relation_recovery_paths.contains(path)
                        && !incomplete_paths.contains(path)
                });
            if repeat_of_empty {
                unchanged_messages += unchanged_counts.get(path).copied().unwrap_or(0);
            } else if cached_scan.is_some() {
                sources.push(empty_source_batch(
                    path,
                    &snap.fingerprint,
                    discovered_provider_ids.get(path).map(String::as_str),
                ));
                empty_cleared = true;
            } else {
                diagnostics.push(format!(
                    "source {} of {}: empty source has no provider evidence and nothing indexed; \
                     nothing was committed — re-run sync once it contains data",
                    index + 1,
                    paths.len()
                ));
                diagnostic_count += 1;
                empty_skipped = true;
            }
            (None, None)
        } else if resolved_namespace.is_some()
            && cached_fp.as_deref() == Some(snap.fingerprint.as_str())
            && !version_stale
            && !relation_recovery_paths.contains(path)
            && !incomplete_paths.contains(path)
        {
            // 字节未变：跳过 parse。store 层仍会做 no-op 判定（entries 为空时
            // 会走 membership/scan 对比），因此这里只需空 staged 占位。
            unchanged_messages += unchanged_counts.get(path).copied().unwrap_or(0);
            (None, None)
        } else if cached_fp.is_some()
            && hint.and_then(provider_is_record_stream).unwrap_or(false)
            && {
                // JSONL 截断尾分诊只适用于 manifest 声明为 record-stream 的
                // provider。SQLite/整档 JSON/Markdown 的字节不是记录流，
                // 逐行 JSON 健康检查会把合法更新误判成截断尾并永久保留旧索引。
                let trace_started = trace::begin();
                let health = jsonl_health(path_ref, &snap).map_err(ProtocolError::from)?;
                trace_health += trace::elapsed(trace_started);
                health == JsonlHealth::Invalid
            }
        {
            // 截断尾（EOF 落在记录中间）＝agent 正在写这个源。已索引过的源
            // 必须 Retain：不重 parse、不推进指纹、不提交——旧索引原样保留，
            // 不产生 rebuild churn，也不误 tombstone（fast-resume
            // Invalid→Retain 语义）。只报诊断；文件写完后再 sync 会因指纹
            // 不匹配走完整重扫。新源（cached_fp 为 None）无旧索引可保留，
            // 落到下一分支走既有 recoverable-skip。
            retained = true;
            retained_count += 1;
            diagnostics.push(format!(
                "source {} of {}: truncated tail (a JSON record is cut off at EOF, \
                 the file may still be written); keeping previously indexed content \
                 — re-run sync when the file is complete",
                index + 1,
                paths.len()
            ));
            diagnostic_count += 1;
            (None, None)
        } else {
            if version_stale {
                // 解析语义升级（PARSER_SEMANTIC_VERSION 递增）：字节未变但已存
                // 解析版本落后——targeted backfill，重解析 + 提交写回当前版本。
                diagnostics.push(format!(
                    "source {} of {}: parser semantics upgraded (stored parser_version {}, \
                     current {}) — re-parsing unchanged bytes to backfill",
                    index + 1,
                    paths.len(),
                    cached_version.unwrap_or(0),
                    PARSER_SEMANTIC_VERSION,
                ));
                diagnostic_count += 1;
            }
            // provider 身份优先取 discover 的扫描事实；显式 sync 没有扫描结果，
            // 则按同一张 root 表反查路径归属（见 provider_for_source_path）。两条
            // 入口因此对同一个文件得到同一个 provider——不再出现 discover 能索引、
            // 显式 sync 撞 tie 的分裂。路径不在任何登记根下时仍为 None，走全
            // registry probe。
            let trace_started = trace::begin();
            let (staged, variant) = stage_with_source(&source, hint)?;
            trace_stage += trace::elapsed(trace_started);
            (Some(staged), Some(variant))
        };
        if progress {
            // 措辞如实区分三种路径：指纹命中只是 checked（未 parse），
            // 走完整解析的才是 scanned，截断尾 retain 是 kept——不得谎报
            // 缓存命中的源为 "staged (0 messages)"。
            let message = if empty_cleared {
                format!(
                    "cleared source {}/{} (empty replacement)",
                    index + 1,
                    paths.len()
                )
            } else if empty_skipped {
                format!(
                    "empty source {}/{} (nothing indexed)",
                    index + 1,
                    paths.len()
                )
            } else {
                match (&staged, retained) {
                    (Some(staged), _) => format!(
                        "scanned source {}/{} ({} messages)",
                        index + 1,
                        paths.len(),
                        staged.messages.len()
                    ),
                    (None, true) => format!(
                        "retained source {}/{} (truncated tail — keeping previous index)",
                        index + 1,
                        paths.len()
                    ),
                    (None, false) => {
                        format!("checked source {}/{} (unchanged)", index + 1, paths.len())
                    }
                }
            };
            protocol::write_stdout_line(&protocol::progress_frame("sync", &message, request_id));
        }
        if let (Some(staged), Some(variant)) = (&staged, &variant) {
            message_count += staged.messages.len();
            staged_messages_by_path.insert(path.clone(), staged.messages.len());
            skipped_count += staged.report.skipped;
            diagnostic_count += staged.report.diagnostics.len();
            diagnostics.extend(staged.report.diagnostics.iter().cloned());
            let provider = variant.split('/').next().unwrap_or(variant).to_string();
            let legacy_namespace = installation_namespace(path, &provider);
            let trace_started = trace::begin();
            let persisted_namespace = match resolved_namespace {
                Some(namespace) => namespace,
                None => store
                    .resolve_or_allocate_installation_namespace(&provider, path, &legacy_namespace)
                    .map_err(ProtocolError::from_private_port_error)?,
            };
            trace_namespace += trace::elapsed(trace_started);
            let trace_started = trace::begin();
            let mut source = staged_to_source_with_provider_namespace(
                path,
                staged,
                &provider,
                variant,
                &snap.fingerprint,
                snap.len,
                discovered_provider_ids.get(path).map(String::as_str),
                Some(persisted_namespace.as_str()),
            )?;
            trace_assemble += trace::elapsed(trace_started);
            if incomplete_providers.contains(&provider) {
                source.relation_complete = false;
            }
            sources.push(source);
        } else if let Some(provider_id) = discovered_provider_ids.get(path) {
            provider_id_backfills.push((path.clone(), provider_id.clone()));
        }
        snapshots.push((path_ref.to_path_buf(), snap));
    }

    // 快照复核：字节在读取期间变化的源被**推迟**，从提交批次里摘掉，其余源照常
    // 提交。这不是放宽 fail-closed——被推迟的源一个字节也不会入库，torn 数据依然
    // 不可能出现；改变的只是"一个源的漂移不再作废整次 sync"。
    let deferred_paths =
        defer_sources_changed_during_read(&snapshots, &mut sources, &mut diagnostics, paths.len())
            .map_err(ProtocolError::from)?;
    for path in &deferred_paths {
        if let Some(staged) = staged_messages_by_path.get(path) {
            message_count = message_count.saturating_sub(*staged);
        }
    }
    diagnostic_count += deferred_paths.len();

    // discover 合成的空批（已删除源的 tombstone）追加进提交批次。
    sources.extend(synthetic_batches.iter().cloned());

    let trace_commit = trace::begin();
    let changed = store
        .commit_source_batches_if_changed(&sources)
        .map_err(ProtocolError::from)?;
    let trace_commit_ms = trace::elapsed(trace_commit);
    store
        .backfill_source_provider_ids(&provider_id_backfills)
        .map_err(ProtocolError::from)?;
    let generation = store.active_generation().map_err(ProtocolError::from)?;
    // `emitted` 只统计本次实际解析的消息；指纹缓存命中的源按已存消息数
    // 计入 unchanged（与 emitted 同单位：消息数）。截断尾被 retain 的源既不
    // 解析也不提交，单列 `retained`（源数），其诊断进 warnings 通道。
    let committed = if changed { message_count } else { 0 };
    // Put the partial-state contract first so the diagnostic cap cannot hide
    // retained history / temporary copies behind individual provider defects.
    let partial_warning = (skipped_count > 0).then_some(PARTIAL_SOURCE_WARNING);
    diagnostic_count += usize::from(partial_warning.is_some());
    let warnings = diagnostic_warnings(
        partial_warning
            .into_iter()
            .chain(diagnostics.iter().map(String::as_str)),
        diagnostic_count,
    );
    trace::emit(
        "cli:sync",
        &[
            ("prelude", trace_prelude),
            ("namespace", trace_namespace),
            ("capture", trace_capture),
            ("jsonl_health", trace_health),
            ("stage", trace_stage),
            ("assemble", trace_assemble),
            ("commit", trace_commit_ms),
        ],
        &format!(
            "sources={} emitted={} unchanged={} changed={} deferred={}",
            paths.len(),
            message_count,
            unchanged_messages,
            changed,
            deferred_paths.len()
        ),
    );
    let source_count = paths.len() + synthetic_batches.len();
    Ok((
        serde_json::json!({
            "sources": source_count,
            "emitted": message_count,
            "messages": message_count,
            "committed": committed,
            "unchanged": if changed { unchanged_messages } else { message_count + unchanged_messages },
            "retained": retained_count,
            "deferred": deferred_paths.len(),
            "skipped": skipped_count,
            "diagnostics": diagnostic_count,
            "generation": generation,
        }),
        warnings,
    ))
}

/// 复核每个源的快照，把"读取期间字节发生变化"的源从提交批次里摘掉，返回被推迟
/// 的源路径。
///
/// 为什么不是整次失败：`sync --discover` 的常态就是**一边有 agent 在写自己的
/// transcript**（用户往往正是在一个 coding-agent 会话里运行它）。旧实现在这一步
/// 用 `?` 直接返回 `source_changed`，于是一个正在增长的文件就作废了本次已经扫完
/// 的全部源——一次也提交不成。被推迟的源不写入任何字节，torn 数据依旧不可能
/// 出现；它上一次的索引原样保留，文件写完后下一次 sync 会因指纹不匹配走完整重扫。
///
/// 只有 [`PortError::SnapshotChanged`] 会被推迟。其余错误（真正的 I/O 失败）仍然
/// 原样上抛：那不是"文件正在被写"，而是源不可读。
fn defer_sources_changed_during_read(
    snapshots: &[(std::path::PathBuf, SourceSnapshot)],
    sources: &mut Vec<SourceBatch>,
    diagnostics: &mut Vec<String>,
    total: usize,
) -> Result<Vec<String>, PortError> {
    // Only sources that produced a commit batch consumed their bytes during
    // staging. A fingerprint-matched source was dropped from the batch without
    // being parsed, so re-reading and re-hashing it here would double the no-op
    // I/O for bytes that are never written; the next sync re-captures any new
    // content because the stored fingerprint no longer matches.
    let staged: BTreeSet<&str> = sources
        .iter()
        .map(|source| source.source_path.as_str())
        .collect();
    let mut deferred: Vec<String> = Vec::new();
    for (index, (path, snapshot)) in snapshots.iter().enumerate() {
        if !staged.contains(path.to_string_lossy().as_ref()) {
            continue;
        }
        match verify_snapshot(path, snapshot) {
            Ok(()) => {}
            Err(PortError::SnapshotChanged(detail)) => {
                diagnostics.push(format!(
                    "source {} of {}: changed while being read ({detail}); deferred — \
                     nothing was committed for it and any previously indexed content is \
                     untouched; re-run sync once the file is no longer being written",
                    index + 1,
                    total
                ));
                deferred.push(path.to_string_lossy().into_owned());
            }
            Err(other) => return Err(other),
        }
    }
    if !deferred.is_empty() {
        sources.retain(|source| !deferred.contains(&source.source_path));
    }
    Ok(deferred)
}

/// 把应用结果投影为 (outcome, data, page, warnings)：截断 → partial（exit 10），
/// 分页令牌 → envelope `page`，可核验的降级事实 → warnings。前端只做投影，
/// 不再解释语义。
fn render(
    response: AppResponse,
) -> (
    protocol::Outcome,
    serde_json::Value,
    protocol::Page,
    Vec<String>,
) {
    match response {
        AppResponse::Search {
            hits,
            next_cursor,
            generation,
            truncation,
            retrieval_mode: effective_mode,
            fallback_warning,
        } => {
            let outcome = outcome_of(&truncation);
            let page = protocol::Page {
                has_more: next_cursor.is_some(),
                next_cursor,
            };
            let mut warnings = Vec::new();
            if let Some(warning) = fallback_warning {
                warnings.push(warning);
            }
            let data = serde_json::json!({
                "retrieval_mode": effective_mode.as_str(),
                "hits": hits
                    .into_iter()
                    .map(|hit| {
                        // search-match-guidance：guidance 为追加字段——空集合时
                        // 整个键省略，与既有机器人输出字节兼容。
                        let mut json = serde_json::json!({
                            "id": hit.id.as_str(),
                            "score": hit.score,
                            // R4（ADR-0008）：命中携带所属会话 wire id 与正文摘要
                            // （追加字段，schema minor：不删除任何既有字段）。
                            // `text` 字节已计入 Application 的 clamp_items 预算
                            // （R4.2）；人类渲染器把同一摘要打印为 snippet 行。
                            "session_id": hit.session_id,
                            "text": hit.text,
                        });
                        if !hit.why_matched.is_empty() {
                            json["why_matched"] = serde_json::json!(hit.why_matched);
                        }
                        if !hit.suggested_next_commands.is_empty() {
                            json["suggested_next_commands"] =
                                serde_json::json!(hit.suggested_next_commands);
                        }
                        // R3 occurrences：非归并命中恒为 1，与 guidance 一致采用
                        // "等于默认值即省略"的追加字段约定，保持既有输出字节兼容。
                        if hit.occurrences > 1 {
                            json["occurrences"] = serde_json::json!(hit.occurrences);
                        }
                        // resume_available（ADR-0009）：恒序列化，schema 1.1 声明。
                        json["resume_available"] = serde_json::json!(hit.resume_available);
                        json
                    })
                    .collect::<Vec<_>>(),
                "generation": generation,
                "truncation": truncation_json(&truncation),
            });
            (outcome, data, page, warnings)
        }
        AppResponse::Get { payload } => (
            protocol::Outcome::Success,
            serde_json::json!({
                "payload": payload.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()),
            }),
            protocol::Page::default(),
            Vec::new(),
        ),
        // Resume Metadata（ADR-0009）：固定可空字段恒在；缺失统一 null，
        // 绝不回显 transcript/source path。
        AppResponse::SessionResume(metadata) => (
            protocol::Outcome::Success,
            serde_json::json!({
                "session_id": metadata.session_id.as_str(),
                "provider_id": metadata.provider_id,
                "resume_available": metadata.resume_available,
                "provider_session_id": metadata.provider_session_id,
                "original_working_directory": metadata.original_working_directory,
                "unavailable_reason": metadata.unavailable_reason,
            }),
            protocol::Page::default(),
            Vec::new(),
        ),
        // show 与 get 的区别：get 回原始 payload 字节，show 把存储的 canonical
        // JSON payload 展开成结构化 entity（含 role/text/parent/timestamp/threading）。
        // 未找到时 entity 为 null；payload 非合法 JSON 时按裸文本兜底。
        AppResponse::Show { payload } => (
            protocol::Outcome::Success,
            serde_json::json!({
                "entity": payload.map(|bytes| {
                    match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(value) => value,
                        Err(_) => serde_json::json!({
                            "role": null,
                            "text": String::from_utf8_lossy(&bytes),
                        }),
                    }
                }),
            }),
            protocol::Page::default(),
            Vec::new(),
        ),
        AppResponse::List {
            entries,
            peeks,
            titles,
            next_cursor,
            generation,
            truncation,
        } => {
            let outcome = outcome_of(&truncation);
            let page = protocol::Page {
                has_more: next_cursor.is_some(),
                next_cursor,
            };
            // Peek（#7）与标题（#6，schema v13）与条目逐位对齐是 Application 的
            // 不变量；render 只投影。
            debug_assert_eq!(peeks.len(), entries.len(), "peeks must align with entries");
            debug_assert_eq!(
                titles.len(),
                entries.len(),
                "titles must align with entries"
            );
            let data = serde_json::json!({
                "entries": entries
                    .into_iter()
                    .zip(peeks)
                    .zip(titles)
                    .map(|((entry, peek), title)| {
                        let mut value = serde_json::json!({
                            "id": entry.id.as_str(),
                            "payload": String::from_utf8_lossy(&entry.payload),
                        });
                        if let Some(peek) = peek {
                            value["peek"] = serde_json::json!(peek);
                        }
                        if let Some(title) = title {
                            value["title"] = serde_json::json!(title);
                        }
                        value
                    })
                    .collect::<Vec<_>>(),
                "generation": generation,
                "truncation": truncation_json(&truncation),
            });
            (outcome, data, page, Vec::new())
        }
        AppResponse::Context {
            session_id,
            session,
            branch_leaf,
            branch_leaf_placement_id,
            messages,
            evidence,
            tool_activities,
            requested_level,
            effective_level,
            talks,
            summary,
            hint,
            truncation,
            generation,
        } => {
            let outcome = outcome_of(&truncation);
            // 降级如实上报：legacy（无 span）行的证据精度是 unknown，调用方应知道
            // 重新 ingest 可恢复字节级定位（design §0.2）。
            let unknown = evidence
                .iter()
                .filter(|dto| dto.precision == Precision::Unknown)
                .count();
            let warnings = if unknown > 0 {
                vec![format!(
                    "{unknown} of {} evidence spans have unknown precision \
                     (legacy rows; re-ingest to restore byte spans)",
                    evidence.len()
                )]
            } else {
                Vec::new()
            };
            let data = serde_json::json!({
                "session_id": session_id,
                "session": session,
                "branch_leaf": branch_leaf,
                "branch_leaf_placement_id": branch_leaf_placement_id,
                "tool_activities": tool_activities,
                "messages": messages
                    .into_iter()
                    .map(|message| serde_json::json!({
                        "id": message.id,
                        "placement_id": message.placement_id,
                        "message_id": message.message_id,
                        "payload": message.payload,
                    }))
                    .collect::<Vec<_>>(),
                "evidence": evidence,
                "requested_level": requested_level,
                "effective_level": effective_level,
                "talks": talks,
                "summary": summary,
                "hint": hint,
                "truncation": truncation_json(&truncation),
                "generation": generation,
            });
            (outcome, data, protocol::Page::default(), warnings)
        }
        AppResponse::Message { window } => {
            let outcome = outcome_of(&window.truncation);
            let data = serde_json::json!({
                "message_id": window.message_id,
                "session_id": window.session_id,
                "anchor_placement_id": window.anchor_placement_id,
                "messages": window.messages
                    .into_iter()
                    .map(|message| serde_json::json!({
                        "id": message.id,
                        "placement_id": message.placement_id,
                        "message_id": message.message_id,
                        "payload": message.payload,
                    }))
                    .collect::<Vec<_>>(),
                "truncation": truncation_json(&window.truncation),
                "generation": window.generation,
            });
            (outcome, data, protocol::Page::default(), Vec::new())
        }
        AppResponse::MessageContexts {
            message_id,
            candidates,
        } => (
            protocol::Outcome::Success,
            serde_json::json!({
                "message_id": message_id,
                "candidates": candidates,
            }),
            protocol::Page::default(),
            Vec::new(),
        ),
        AppResponse::Status {
            catalog_count,
            active_generation,
            placements,
            source_placement_claims,
            usage,
            repos,
        } => (
            protocol::Outcome::Success,
            serde_json::json!({
                "catalog_count": catalog_count,
                "generation": active_generation,
                "placements": placements,
                "source_placement_claims": source_placement_claims,
                // usage 维度（None = 存储无 usage 投影，不渲染该键）。
                "usage": usage.map(|totals| serde_json::json!({
                    "sessions": totals.sessions,
                    "input_tokens": totals.input_tokens,
                    "output_tokens": totals.output_tokens,
                    "cache_read_tokens": totals.cache_read_tokens,
                    "cache_write_tokens": totals.cache_write_tokens,
                    "reasoning_tokens": totals.reasoning_tokens,
                    "observed_events": totals.observed_events,
                    "derived_events": totals.derived_events,
                })),
                // repo identity（schema v16）：空列表 = 无 repo 事实（未知 ≠ 零）。
                "repos": repos.iter().map(|totals| serde_json::json!({
                    "repo_slug": totals.repo_slug,
                    "sessions": totals.sessions,
                })).collect::<Vec<_>>(),
            }),
            protocol::Page::default(),
            Vec::new(),
        ),
    }
}

fn outcome_of(truncation: &Truncation) -> protocol::Outcome {
    if truncation.truncated {
        protocol::Outcome::Partial
    } else {
        protocol::Outcome::Success
    }
}

fn truncation_json(truncation: &Truncation) -> serde_json::Value {
    serde_json::json!({
        "truncated": truncation.truncated,
        "reason": truncation.reason,
    })
}

fn arg<'a>(rest: &'a [String], index: usize, usage: &str) -> Result<&'a str, CliError> {
    rest.get(index)
        .map(String::as_str)
        .ok_or_else(|| CliError::usage(format!("missing argument: {usage}")))
}

/// 校验位置参数个数不超过 `expected`（命令名之外的裸参数）：多余的参数是
/// 用法错误（exit 2），不静默忽略——静默丢弃会让调用方误以为参数被接受。
fn no_extra_args(rest: &[String], expected: usize, usage: &str) -> Result<(), CliError> {
    if rest.len() > expected + 1 {
        return Err(CliError::usage(format!(
            "unexpected extra argument(s); usage: {usage}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_session_grep_application::{EvidenceSpanDto, StagedMessage};
    use agent_session_grep_ports::ParseReport;
    use agent_session_grep_ports::capability::CapabilityLevel;

    fn staged_batch(
        messages: Vec<StagedMessage>,
        skipped: usize,
        session_native_id: &str,
    ) -> StagedBatch {
        let committed = messages.len();
        StagedBatch {
            messages,
            activities: Vec::new(),
            usage_events: Vec::new(),
            session_native_id: Some(session_native_id.into()),
            report: ParseReport {
                committed,
                skipped,
                diagnostics: if skipped == 0 {
                    Vec::new()
                } else {
                    vec!["synthetic skipped record".into()]
                },
                session_native_id: Some(session_native_id.into()),
                session_observation: ProviderSessionObservation::default(),
            },
        }
    }

    fn staged_message(seq: u32, native_id: &str, span: (u64, u64)) -> StagedMessage {
        StagedMessage {
            session: None,
            seq,
            native_id: native_id.into(),
            parent_native_id: None,
            role: "user".into(),
            text: "synthetic body".into(),
            timestamp: Some("2026-07-28T00:00:00Z".into()),
            is_sidechain: false,
            span: Some(span),
        }
    }

    #[test]
    fn boundary_diagnostic_warnings_redact_before_bounding() {
        let secret = format!("sk_live_{}", "a".repeat(600));
        let diagnostic = format!("skipped provider row: {secret}");
        let warnings = diagnostic_warnings([diagnostic.as_str()], 1);
        assert_eq!(warnings, std::slice::from_ref(&diagnostic));
        let (machine, count) = render_warnings("ingest", protocol::OutputMode::Json, &warnings);
        assert_eq!(machine, ["skipped provider row: [redacted:stripe_key]"]);
        assert_eq!(count, 1);
        let (human, count) = render_warnings("ingest", protocol::OutputMode::Human, &warnings);
        assert_eq!(
            human[0],
            diagnostic.chars().take(512).collect::<String>() + "…"
        );
        assert_eq!(count, 0);
        let (plain, count) =
            render_warnings("sync", protocol::OutputMode::Json, &["x".repeat(600)]);
        assert_eq!(plain[0].chars().count(), DIAGNOSTIC_WARNING_CHARS + 1);
        assert_eq!(count, 0, "truncation alone is not redaction");
    }

    #[test]
    fn boundary_unknown_command_name_is_constant() {
        for token in ["sk_live_abcdef1234567890xyz", "C:/private/unknown"] {
            assert_eq!(command_name(&[token.to_string()]), "unknown");
        }
    }

    #[test]
    fn provider_discovery_target_rejects_unknown_provider() {
        assert!(provider_discovery_target("unknown-provider").is_none());
    }

    #[test]
    fn discover_roots_match_capability_discover_claims() {
        // capability.rs 的 `discover` 列此前无任何测试把声明对照真实布线，
        // 已漏过 3 个少报（openclaw/tencent-codebuddy/antigravity 有 root 却记
        // Unsupported）。这里双向锁定：登记进 PROVIDER_DISCOVERY_ROOTS ⟺ 声明非
        // Unsupported。`sync --discover` 遍历整个 registry 并对每个 provider 调
        // provider_discovery_target，所以这张表就是运行期的发现能力事实。
        let matrix = ProviderCapabilityMatrix::current();
        let wired: BTreeSet<&str> = PROVIDER_DISCOVERY_ROOTS
            .iter()
            .map(|(id, _, _)| *id)
            .collect();
        assert_eq!(
            wired.len(),
            PROVIDER_DISCOVERY_ROOTS.len(),
            "PROVIDER_DISCOVERY_ROOTS 不得有重复 provider id"
        );

        for provider in &matrix.providers {
            let id = provider.provider_id.as_str();
            let claims_discover = provider.discover != CapabilityLevel::Unsupported
                && provider.discover != CapabilityLevel::Unknown;
            assert_eq!(
                claims_discover,
                wired.contains(id),
                "{id}: capability.rs discover={:?} 与 PROVIDER_DISCOVERY_ROOTS 登记状态\
                 （{}）不一致——有 root 就不能记 Unsupported，无 root 就不能宣称可发现",
                provider.discover,
                if wired.contains(id) {
                    "已登记"
                } else {
                    "未登记"
                },
            );
        }

        // 登记的 root 必须是 home 相对路径（provider_discovery_target 直接 join 到
        // home），绝对路径或上跳会越出用户数据根。扩展名必须是不含点的裸扩展名，
        // 因为 `Path::extension()` 返回的就是不含点的形式，写成 `.db` 永不匹配。
        for (id, sub, extension) in PROVIDER_DISCOVERY_ROOTS {
            let path = std::path::Path::new(sub);
            assert!(
                path.is_relative(),
                "{id}: 发现根必须是 home 相对路径，实际 `{sub}`"
            );
            assert!(
                !sub.contains(".."),
                "{id}: 发现根不得包含 `..`，实际 `{sub}`"
            );
            assert!(
                !extension.is_empty() && !extension.starts_with('.'),
                "{id}: 扩展名必须是不含点的裸扩展名（如 `jsonl`/`db`），实际 `{extension}`"
            );
        }
    }

    #[test]
    fn cursor_is_not_registered_as_discoverable() {
        // cursor 的源是 VS Code workspaceStorage 下的 `state.vscdb`，路径形如
        // `AppData/Roaming/<Cursor 变体>/User/workspaceStorage/<hash>/state.vscdb`
        // —— 既非 home 直接相对（Windows 有 Roaming 层，macOS/Linux 布局不同），
        // 也非单一 root，本机实测 `~/.cursor` 与 Roaming/Cursor 均不存在，无法确认
        // 真实布局。R4：不猜路径。tombstone 风险使猜错的代价是抹掉已索引内容：
        // sync_discover 对"完整扫描"的 provider 会把已存路径 diff 成空批删除索引，
        // 所以 root 写错会让扫描完整但为空。等拿到真实布局证据再登记。
        assert!(
            provider_discovery_target("cursor").is_none(),
            "cursor 的 workspaceStorage 布局尚无本机证据；登记错误的 root 会让完整\
             扫描返回 0 条路径并 tombstone 已索引内容"
        );
    }

    /// 各已实现 provider 的 pinned golden fixture 字节（编译期内联）。
    ///
    /// 用真实 fixture 而非手写样本：twin 守卫要判定的是"真实 transcript 字节能否被
    /// 单一 provider 认领"，手写样本可能碰巧带上真实格式里没有的判别位。
    /// SQLite 源（opencode/cursor 的 `.db`）不含在内——它们的 probe 走整档打开，
    /// 与 JSONL 记录流不构成同格式歧义。
    const TWIN_FIXTURES: &[(&str, &[u8])] = &[
        (
            "aider",
            include_bytes!("../../agent-session-grep-provider-aider/tests/golden/basic.md"),
        ),
        (
            "antigravity",
            include_bytes!(
                "../../agent-session-grep-provider-antigravity/tests/golden/basic.jsonl"
            ),
        ),
        (
            "claude-code",
            include_bytes!("../../agent-session-grep-provider-claude/tests/golden/basic.jsonl"),
        ),
        (
            "cline",
            include_bytes!("../../agent-session-grep-provider-cline/tests/golden/basic.json"),
        ),
        (
            "codex",
            include_bytes!("../../agent-session-grep-provider-codex/tests/golden/basic.jsonl"),
        ),
        (
            "grok-build",
            include_bytes!("../../agent-session-grep-provider-grok/tests/golden/basic.jsonl"),
        ),
        (
            "hermes",
            include_bytes!("../../agent-session-grep-provider-hermes/tests/golden/basic.json"),
        ),
        (
            "kimi-code",
            include_bytes!("../../agent-session-grep-provider-kimi/tests/golden/basic.jsonl"),
        ),
        (
            "openclaw",
            include_bytes!("../../agent-session-grep-provider-openclaw/tests/golden/basic.jsonl"),
        ),
        (
            "pi",
            include_bytes!("../../agent-session-grep-provider-pi/tests/golden/basic.jsonl"),
        ),
        (
            "qoder",
            include_bytes!("../../agent-session-grep-provider-qoder/tests/golden/basic.jsonl"),
        ),
        (
            "tencent-codebuddy",
            include_bytes!("../../agent-session-grep-provider-codebuddy/tests/golden/basic.jsonl"),
        ),
    ];

    /// 对一份字节跑全 registry probe，返回并列最高置信度的 variant 集合。
    ///
    /// 复刻 `select_and_stage_source` 的排序规则（Confirmed>High>Low，Ambiguous
    /// 不参与），因此"返回多于一个 variant" ⟺ 生产路径会命中 tie 分支并整源拒绝。
    fn top_confidence_variants(bytes: &[u8]) -> Vec<String> {
        fn rank(c: agent_session_grep_ports::Confidence) -> Option<u8> {
            use agent_session_grep_ports::Confidence;
            match c {
                Confidence::Confirmed => Some(3),
                Confidence::High => Some(2),
                Confidence::Low => Some(1),
                Confidence::Ambiguous => None,
            }
        }
        let source = agent_session_grep_ports::SliceSource::new(bytes);
        let mut best: Option<u8> = None;
        let mut variants: Vec<String> = Vec::new();
        for adapter in provider_registry() {
            let Ok(probe) = adapter.probe_source(&source) else {
                continue;
            };
            let Some(r) = rank(probe.confidence) else {
                continue;
            };
            match best {
                Some(best_rank) if r < best_rank => {}
                Some(best_rank) if r == best_rank => {
                    if !variants.contains(&probe.variant_id) {
                        variants.push(probe.variant_id);
                    }
                }
                _ => {
                    best = Some(r);
                    variants = vec![probe.variant_id];
                }
            }
        }
        variants
    }

    #[test]
    fn twin_fixture_table_covers_every_record_stream_provider() {
        // 新增 provider 而未登记 fixture 时立即失败，否则下面的 twin 守卫会静默
        // 漏检新来者——pi/openclaw 的碰撞正是"没人对照过"才活到运行期的。
        let matrix = ProviderCapabilityMatrix::current();
        let covered: BTreeSet<&str> = TWIN_FIXTURES.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            covered.len(),
            TWIN_FIXTURES.len(),
            "TWIN_FIXTURES 不得有重复 provider id"
        );
        // 整档 SQLite 源单列：probe 靠 magic header + 表结构，不与 JSONL 争同一字节。
        let whole_file_sqlite: BTreeSet<&str> = ["opencode", "cursor"].into_iter().collect();
        let implemented: BTreeSet<&str> = matrix
            .providers
            .iter()
            .filter(|p| p.maturity != ProviderMaturity::Unsupported)
            .map(|p| p.provider_id.as_str())
            .filter(|id| !whole_file_sqlite.contains(id))
            .collect();
        assert_eq!(
            covered, implemented,
            "TWIN_FIXTURES 必须恰好覆盖除整档 SQLite 源外的全部已实现 provider"
        );
    }

    #[test]
    fn ambiguous_formats_are_always_separable_by_a_registered_root() {
        // 本仓库已经踩过一次：pi 与 openclaw 是同一种 v3 JSONL，两者 probe 同为
        // Confirmed，`select_and_stage_source` 因此命中 tie 分支拒绝整个源——两个
        // provider 的**任何**源都无法索引，而唯一的既有信号是运行期报错。
        //
        // 这条断言把不变量前移到编译-测试期：对每份真实 golden fixture，要么全
        // registry probe 只有一个最高置信度 variant（内容自带判别位），要么所有
        // 并列者都在 PROVIDER_DISCOVERY_ROOTS 里登记了各自的规范根（歧义可由路径
        // 消解）。两者都不满足即为可发布缺陷：那些源既无法靠内容区分，也没有路径
        // 事实可依。
        let wired: BTreeSet<&str> = PROVIDER_DISCOVERY_ROOTS
            .iter()
            .map(|(id, _, _)| *id)
            .collect();
        for (provider_id, bytes) in TWIN_FIXTURES {
            let variants = top_confidence_variants(bytes);
            assert!(
                !variants.is_empty(),
                "{provider_id}: golden fixture 必须至少被一个 adapter 认领"
            );
            if variants.len() == 1 {
                continue;
            }
            // 并列：每个并列 provider 都必须有登记根，否则其源不可索引。
            let claimants: Vec<&str> = variants
                .iter()
                .map(|v| v.split('/').next().unwrap_or(v))
                .collect();
            for claimant in &claimants {
                assert!(
                    wired.contains(claimant),
                    "{provider_id} 的 fixture 被 {claimants:?} 并列认领（生产路径会\
                     整源拒绝），但 `{claimant}` 未登记规范根——该 provider 的源\
                     既无法靠内容区分也无路径事实可依，属可发布缺陷"
                );
            }
        }
    }

    #[test]
    fn pi_and_openclaw_fixtures_are_mutually_indistinguishable_by_content() {
        // 正向锚定上面那条守卫的现实前提：这两个 provider 的真实 fixture 确实互相
        // 被对方 Confirmed 认领。若将来任一方的格式引入判别位而使二者可分，此断言
        // 失败——那是好事，但需要同步更新 stage_with_source 的文档依据。
        for provider_id in ["pi", "openclaw"] {
            let bytes = TWIN_FIXTURES
                .iter()
                .find(|(id, _)| *id == provider_id)
                .expect("TWIN_FIXTURES 必须含 pi/openclaw")
                .1;
            let variants = top_confidence_variants(bytes);
            let claimants: BTreeSet<&str> = variants
                .iter()
                .map(|v| v.split('/').next().unwrap_or(v))
                .collect();
            assert!(
                claimants.contains("pi") && claimants.contains("openclaw"),
                "{provider_id} 的 fixture 应同时被 pi 与 openclaw 认领，实际 {claimants:?}"
            );
        }
    }

    #[test]
    fn provider_for_source_path_resolves_registered_roots() {
        // 正向：登记根下的路径归属该 provider（两个 twin 各自解析正确即为修复本身）。
        assert_eq!(
            provider_for_source_path("C:/Users/x/.pi/agent/sessions/--work--/one.jsonl"),
            Some("pi")
        );
        assert_eq!(
            provider_for_source_path("C:/Users/x/.openclaw/agents/main/sessions/one.jsonl"),
            Some("openclaw")
        );
        // 非默认 home（测试用 HOME 覆盖、多用户 profile）也匹配：按分段窗口对齐，
        // 不依赖 home 前缀。
        assert_eq!(
            provider_for_source_path("/tmp/fake-home/.pi/agent/sessions/x/one.jsonl"),
            Some("pi")
        );
    }

    #[cfg(windows)]
    #[test]
    fn provider_for_source_path_resolves_windows_separators() {
        // Windows 反斜杠与大写盘符经 source_path_identity 归一后同样匹配。
        assert_eq!(
            provider_for_source_path(r"C:\Users\x\.pi\agent\sessions\--work--\one.jsonl"),
            Some("pi")
        );
    }

    #[test]
    fn provider_for_source_path_rejects_non_root_paths() {
        // 未登记根 / 任意目录 → None，走全 registry probe（不猜身份）。
        assert_eq!(provider_for_source_path("C:/tmp/export/one.jsonl"), None);
        assert_eq!(
            provider_for_source_path("C:/Users/x/.cursor/sessions/one.jsonl"),
            None
        );
        // 分段匹配而非字符串前缀：`sessions-backup` 不是 `sessions`。
        assert_eq!(
            provider_for_source_path("C:/Users/x/.pi/agent/sessions-backup/one.jsonl"),
            None
        );
        // 路径就是 root 自身（目录）→ 不算归属。
        assert_eq!(
            provider_for_source_path("C:/Users/x/.pi/agent/sessions"),
            None
        );
    }

    #[test]
    fn provider_matrix_data_adds_semantic_surface_without_touching_rows() {
        let data = provider_matrix_data();
        // Provider rows stay unchanged; shared metadata adds a CLI-only operation.
        assert_eq!(data.as_object().unwrap().len(), 3);
        assert_eq!(data["relocation"]["interfaces"], serde_json::json!(["cli"]));
        assert_eq!(
            data["providers"].as_array().unwrap().len(),
            ProviderCapabilityMatrix::current().providers.len()
        );
        let semantic = &data["semantic"];
        // 键集跨构建稳定。
        let keys = semantic
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            keys,
            ["default_model", "feature", "runtime"]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
        // 诚实默认向量化器事实：任何构建都是 bigram-hash-v1。
        assert_eq!(
            semantic["default_model"],
            agent_session_grep_application::embedding::BIGRAM_HASH_MODEL_ID
        );
        // feature/runtime 严格跟随 cfg(feature = "semantic-candle")：
        // 默认构建断言 null 形状，feature 构建断言稳定标识（CI 两种构建都会跑到）。
        if cfg!(feature = "semantic-candle") {
            assert_eq!(semantic["feature"], "semantic-candle");
            assert_eq!(semantic["runtime"], "candle-e5-local");
        } else {
            assert!(semantic["feature"].is_null());
            assert!(semantic["runtime"].is_null());
        }
    }

    #[test]
    fn doctor_semantic_feature_flag_tracks_build() {
        let flag = semantic_feature_flag();
        if cfg!(feature = "semantic-candle") {
            assert_eq!(flag, serde_json::json!(true));
        } else {
            assert!(flag.is_null());
        }
    }

    /// 构造一个只带路径的最小提交批次，用于 deferral 过滤的测试。
    fn empty_batch(path: &str) -> SourceBatch {
        SourceBatch {
            source_path: path.to_string(),
            entries: Vec::new(),
            placements: Vec::new(),
            edges: Vec::new(),
            activities: Vec::new(),
            usage_events: Vec::new(),
            relation_complete: true,
            len_bytes: None,
            fingerprint: None,
            provider_id: None,
            resume_claims: Vec::new(),
        }
    }

    #[test]
    fn a_source_growing_during_the_read_is_deferred_and_its_siblings_still_commit() {
        // 真实缺陷：`sync --discover` 的常态是一边有 agent 在写自己的 transcript
        // （用户往往正是在一个 coding-agent 会话里跑它）。旧实现在快照复核处用 `?`
        // 直接返回 source_changed，一个正在增长的文件就作废本次扫完的全部源——
        // 在本机上等于"只要 Claude Code 开着就永远同步不完"。
        let dir = tempfile::tempdir().unwrap();
        let growing = dir.path().join("growing.jsonl");
        let stable = dir.path().join("stable.jsonl");
        std::fs::write(&growing, b"{\"a\":1}\n").unwrap();
        std::fs::write(&stable, b"{\"b\":2}\n").unwrap();
        let snapshots = vec![
            (growing.clone(), capture(&growing).unwrap()),
            (stable.clone(), capture(&stable).unwrap()),
        ];
        // 捕获之后文件继续增长：这正是竞态产生的状态。
        std::fs::write(&growing, b"{\"a\":1}\n{\"a\":2}\n").unwrap();

        let growing_path = growing.to_string_lossy().into_owned();
        let stable_path = stable.to_string_lossy().into_owned();
        let mut sources = vec![empty_batch(&growing_path), empty_batch(&stable_path)];
        let mut diagnostics = Vec::new();
        let deferred =
            defer_sources_changed_during_read(&snapshots, &mut sources, &mut diagnostics, 2)
                .expect("一个源漂移不该让整次 sync 失败");

        assert_eq!(deferred, vec![growing_path]);
        assert_eq!(
            sources.len(),
            1,
            "被推迟的源必须从提交批次里摘掉，其余源照常提交"
        );
        assert_eq!(sources[0].source_path, stable_path);
        assert_eq!(diagnostics.len(), 1);
        assert!(
            diagnostics[0].contains("deferred"),
            "推迟必须留下可见诊断，不能静默跳过：{}",
            diagnostics[0]
        );
    }

    #[test]
    fn an_unreadable_source_still_fails_the_sync() {
        // 推迟只适用于"文件正在被写"。源不可读是另一回事，必须照旧上抛，
        // 否则真正的 I/O 故障会被伪装成"稍后重试即可"。
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("gone.jsonl");
        std::fs::write(&missing, b"{\"a\":1}\n").unwrap();
        let snapshots = vec![(missing.clone(), capture(&missing).unwrap())];
        std::fs::remove_file(&missing).unwrap();
        let mut sources = vec![empty_batch(&missing.to_string_lossy())];
        let mut diagnostics = Vec::new();
        let err = defer_sources_changed_during_read(&snapshots, &mut sources, &mut diagnostics, 1)
            .expect_err("源不可读必须报错，不能当成推迟");
        assert!(!matches!(err, PortError::SnapshotChanged(_)));
        assert_eq!(sources.len(), 1, "报错路径不改动提交批次");
    }

    #[test]
    fn discover_provider_sources_collects_jsonl_files() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("one.jsonl"), b"fixture").unwrap();
        std::fs::write(nested.join("two.txt"), b"not a source").unwrap();
        let (paths, complete) = discover_provider_sources(dir.path(), "jsonl");
        assert!(complete);
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("nested/one.jsonl"));
    }

    #[test]
    fn discover_provider_sources_collects_sqlite_db_without_wal_sidecars() {
        // opencode 的源是单个 `.db`。旁文件 `opencode.db-wal` / `-shm` 的
        // extension 是 `db-wal` / `db-shm`，精确匹配把它们排除——否则每个 sidecar
        // 都会被当成一个源交给 probe（非 SQLite magic → 报错噪音）。
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("opencode.db"), b"fixture").unwrap();
        std::fs::write(dir.path().join("opencode.db-wal"), b"fixture").unwrap();
        std::fs::write(dir.path().join("opencode.db-shm"), b"fixture").unwrap();
        std::fs::write(dir.path().join("notes.jsonl"), b"fixture").unwrap();

        let (paths, complete) = discover_provider_sources(dir.path(), "db");
        assert!(complete);
        assert_eq!(paths.len(), 1, "只应收到 opencode.db，实际 {paths:?}");
        assert!(paths[0].ends_with("opencode.db"));
    }

    #[test]
    fn discover_provider_sources_extension_filter_is_exact() {
        // 扩展名比对是精确相等而非后缀包含：`jsonl` 不得收走 `.json`，
        // `db` 不得收走 `.dbx`。
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.json"), b"fixture").unwrap();
        std::fs::write(dir.path().join("b.dbx"), b"fixture").unwrap();

        assert!(discover_provider_sources(dir.path(), "jsonl").0.is_empty());
        assert!(discover_provider_sources(dir.path(), "db").0.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn discover_provider_sources_does_not_follow_symlink_root() {
        let dir = tempfile::tempdir().unwrap();
        let real_root = dir.path().join("real-root");
        std::fs::create_dir(&real_root).unwrap();
        std::fs::write(real_root.join("hidden.jsonl"), b"fixture").unwrap();
        let linked_root = dir.path().join("linked-root");
        std::os::unix::fs::symlink(&real_root, &linked_root).unwrap();

        let (paths, complete) = discover_provider_sources(&linked_root, "jsonl");
        assert!(paths.is_empty());
        assert!(!complete);
    }

    #[test]
    fn installation_namespace_groups_sources_by_provider_root() {
        assert_eq!(
            installation_namespace("C:/profiles/one/.claude/projects/a.jsonl", "claude-code"),
            installation_namespace("C:/profiles/one/.claude/projects/b.jsonl", "claude-code")
        );
        assert_ne!(
            installation_namespace("C:/profiles/one/.claude/projects/a.jsonl", "claude-code"),
            installation_namespace("D:/profiles/two/.claude/projects/a.jsonl", "claude-code")
        );
        assert_ne!(
            installation_namespace("C:/profiles/one/.claude/projects/a.jsonl", "claude-code"),
            installation_namespace("C:/profiles/one/.codex/sessions/a.jsonl", "codex")
        );
    }

    #[test]
    fn installation_namespace_fallback_groups_sibling_sources() {
        assert_eq!(
            installation_namespace("C:/fixtures/head.jsonl", "synthetic"),
            installation_namespace("C:/fixtures/tail.jsonl", "synthetic")
        );
        assert_ne!(
            installation_namespace("C:/fixtures/head.jsonl", "synthetic"),
            installation_namespace("D:/other/head.jsonl", "synthetic")
        );
    }

    /// 把内容写入临时文件并 capture，再按生产路径的流式分类。
    fn health_of(content: &[u8]) -> JsonlHealth {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.jsonl");
        std::fs::write(&path, content).unwrap();
        let snap = capture(&path).unwrap();
        jsonl_health(&path, &snap).unwrap()
    }

    #[test]
    fn jsonl_health_classifies_clean_partial_and_truncated() {
        // 完全合法（含无尾换行、CRLF、空行、首行 UTF-8 BOM、空文件）→ Clean。
        assert_eq!(health_of(b"{\"a\":1}\n{\"b\":2}\n"), JsonlHealth::Clean);
        assert_eq!(health_of(b"{\"a\":1}"), JsonlHealth::Clean);
        assert_eq!(health_of(b"{\"a\":1}\r\n{\"b\":2}\r\n"), JsonlHealth::Clean);
        assert_eq!(health_of(b"\n  \n{\"a\":1}\n"), JsonlHealth::Clean);
        assert_eq!(
            health_of(b"\xEF\xBB\xBF{\"a\":1}\n{\"b\":2}\n"),
            JsonlHealth::Clean
        );
        assert_eq!(health_of(b""), JsonlHealth::Clean);
        // 坏行后仍有合法行 → Partial（recoverable-skip，provider 逐行跳过）。
        assert_eq!(
            health_of(b"{\"a\":1}\n{\n{\"b\":2}\n"),
            JsonlHealth::Partial
        );
        // 尾部截断（EOF 落在记录中间）与全垃圾 → Invalid（已索引源触发 Retain）。
        assert_eq!(health_of(b"{\"a\":1}\n{\"b\":2"), JsonlHealth::Invalid);
        assert_eq!(health_of(b"{\"a\":1\n"), JsonlHealth::Invalid);
        assert_eq!(health_of(b"{\n"), JsonlHealth::Invalid);
        assert_eq!(health_of(b"garbage"), JsonlHealth::Invalid);
        // 与 fast-resume 测试矩阵对齐（shared.rs tests::classifies_clean_partial_and_invalid_jsonl）。
        assert_eq!(
            health_of(b"{\"valid\":true}\n{\n{\"later\":true}\n"),
            JsonlHealth::Partial
        );
        assert_eq!(health_of(b"{\"valid\":true}\n{\n"), JsonlHealth::Invalid);
    }

    #[test]
    fn jsonl_health_never_claims_completeness_on_error() {
        // failed-scan 不变量在分类层的体现：任何无法完整解析的状态（截断/垃圾/
        // 不可读）都不得给出 Clean——Clean 是"可安全替换旧索引"的唯一信号。
        assert_ne!(health_of(b"{\"a\":1}\n{\"b\":2"), JsonlHealth::Clean);
        assert_ne!(health_of(b"{\n"), JsonlHealth::Clean);
        assert_ne!(health_of(b"{\"a\":1}\n{\n"), JsonlHealth::Clean);
    }

    #[test]
    fn fallback_message_id_is_path_independent_but_placement_is_installation_scoped() {
        let staged = staged_batch(vec![staged_message(0, "", (0, 4))], 0, "session-1");
        let first = staged_to_source(
            "C:/one/transcript.jsonl",
            &staged,
            "synthetic",
            "synthetic/jsonl-v1",
            "same-fingerprint",
            8,
        )
        .unwrap();
        let second = staged_to_source(
            "D:/moved/transcript.jsonl",
            &staged,
            "synthetic",
            "synthetic/jsonl-v1",
            "same-fingerprint",
            8,
        )
        .unwrap();

        let first_message = first
            .entries
            .iter()
            .find(|(id, _, _)| id.kind() == IdKind::Message)
            .unwrap();
        let second_message = second
            .entries
            .iter()
            .find(|(id, _, _)| id.kind() == IdKind::Message)
            .unwrap();
        assert_eq!(first_message.0, second_message.0);
        assert_eq!(first_message.0.stability(), Stability::Unstable);
        assert_ne!(first.placements[0].id, second.placements[0].id);
        assert_ne!(first.source_path, second.source_path);
    }

    #[test]
    fn duplicate_native_message_keeps_one_entity_and_every_placement() {
        let staged = staged_batch(
            vec![
                staged_message(0, "shared-native", (0, 4)),
                staged_message(1, "shared-native", (5, 9)),
            ],
            0,
            "session-1",
        );
        let source = staged_to_source(
            "synthetic.jsonl",
            &staged,
            "synthetic",
            "synthetic/jsonl-v1",
            "fingerprint",
            16,
        )
        .unwrap();

        assert_eq!(
            source
                .entries
                .iter()
                .filter(|(id, _, _)| id.kind() == IdKind::Message)
                .count(),
            1
        );
        assert_eq!(source.placements.len(), 2);
        let message_payload: serde_json::Value = serde_json::from_slice(
            &source
                .entries
                .iter()
                .find(|(id, _, _)| id.kind() == IdKind::Message)
                .unwrap()
                .1,
        )
        .unwrap();
        assert_eq!(message_payload["spans"].as_array().unwrap().len(), 2);
        let session_payload: serde_json::Value = serde_json::from_slice(
            &source
                .entries
                .iter()
                .find(|(id, _, _)| id.kind() == IdKind::Session)
                .unwrap()
                .1,
        )
        .unwrap();
        assert_eq!(session_payload["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn multi_session_staging_preserves_shared_messages_and_scopes_usage() {
        use agent_session_grep_application::StagedUsage;
        use agent_session_grep_domain::{TokenSource, UsageObservation};
        use agent_session_grep_ports::{
            ContextGraphStore, MetadataResolution, ProviderSessionIdentity,
        };

        let identity = |id: &str, cwd: &str| ProviderSessionIdentity {
            source_key: id.into(),
            observation: ProviderSessionObservation {
                provider_session_id: MetadataResolution::Resolved(id.into()),
                original_working_directory: MetadataResolution::Resolved(cwd.into()),
                pair_observed: true,
                multi_session: false,
            },
        };
        let mut staged = staged_batch(
            vec![
                staged_message(0, "shared", (0, 4)),
                staged_message(1, "only-a", (5, 9)),
                staged_message(2, "shared", (10, 14)),
                staged_message(3, "only-b", (15, 19)),
            ],
            0,
            "session-a",
        );
        for (index, message) in staged.messages.iter_mut().enumerate() {
            message.session = Some(if index < 2 {
                identity("session-a", "/synthetic/a")
            } else {
                identity("session-b", "/synthetic/b")
            });
        }
        staged.report.session_observation.multi_session = true;
        for anchor in ["only-a", "only-b", "shared", ""] {
            staged.usage_events.push(StagedUsage {
                message_native_id: anchor.into(),
                usage: UsageObservation {
                    input_tokens: 10,
                    output_tokens: 2,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                    reasoning_tokens: 0,
                    token_source: TokenSource::Observed,
                },
            });
        }
        let source = staged_to_source(
            "synthetic.db",
            &staged,
            "opencode",
            "opencode/sqlite-v1",
            "synthetic-fingerprint",
            20,
        )
        .unwrap();
        assert_eq!(source.placements.len(), 4);
        let session_a = source.placements[0].session_id.clone();
        let session_b = source.placements[2].session_id.clone();
        assert_ne!(session_a, session_b);
        assert_eq!(source.placements[1].session_id, session_a);
        assert_eq!(source.placements[3].session_id, session_b);
        let shared = source
            .entries
            .iter()
            .find(|(id, _, _)| id.as_str() == "msg_v1_shared")
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&shared.1).unwrap();
        assert!(payload["session"].is_null());
        let members: BTreeSet<&str> = payload["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap())
            .collect();
        assert_eq!(
            members,
            BTreeSet::from([session_a.as_str(), session_b.as_str()])
        );
        assert_eq!(source.resume_claims.len(), 2);
        for (session, native, cwd) in [
            (&session_a, "session-a", "/synthetic/a"),
            (&session_b, "session-b", "/synthetic/b"),
        ] {
            let claim = source
                .resume_claims
                .iter()
                .find(|claim| claim.session_id == session.as_str())
                .unwrap();
            assert_eq!(claim.provider_session_id.as_deref(), Some(native));
            assert_eq!(claim.original_working_directory.as_deref(), Some(cwd));
            assert!(claim.pair_observed);
        }
        assert_eq!(
            source.usage_events.len(),
            2,
            "ambiguous usage anchors must not be assigned to the first session"
        );
        assert_eq!(source.usage_events[0].session_id, session_a);
        assert_eq!(source.usage_events[1].session_id, session_b);
        let dir = tempfile::tempdir().unwrap();
        let store =
            SqliteStore::open_for_write(&dir.path().join("catalog.db").to_string_lossy()).unwrap();
        store.commit_source_batches_if_changed(&[source]).unwrap();
        for (session, unique) in [(&session_a, "msg_v1_only-a"), (&session_b, "msg_v1_only-b")] {
            let graph = store.load_session_graph(session).unwrap();
            let messages: BTreeSet<&str> = graph
                .messages
                .iter()
                .map(|message| message.id.as_str())
                .collect();
            assert_eq!(messages, BTreeSet::from(["msg_v1_shared", unique]));
            assert!(
                graph
                    .placements
                    .iter()
                    .all(|placement| &placement.session_id == session)
            );
        }
    }

    #[test]
    fn stable_projection_conflict_does_not_disclose_native_id() {
        let private_native_id = "private-provider-native-id";
        let first = staged_message(0, private_native_id, (0, 4));
        let mut second = staged_message(1, private_native_id, (5, 9));
        second.text = "different stable text".into();
        let staged = staged_batch(vec![first, second], 0, "session-1");
        let error = staged_to_source(
            "synthetic.jsonl",
            &staged,
            "synthetic",
            "synthetic/jsonl-v1",
            "fingerprint",
            16,
        );
        let error = match error {
            Err(error) => error,
            Ok(_) => panic!("expected stable projection conflict"),
        };
        assert!(error.0.message.contains("conflicting stable projections"));
        assert!(!error.0.message.contains(private_native_id));
    }

    #[test]
    fn skipped_records_keep_source_relation_incomplete() {
        let staged = staged_batch(vec![staged_message(0, "native", (0, 4))], 1, "session-1");
        let source = staged_to_source(
            "synthetic.jsonl",
            &staged,
            "synthetic",
            "synthetic/jsonl-v1",
            "fingerprint",
            8,
        )
        .unwrap();
        assert!(!source.relation_complete);
    }

    #[test]
    fn sidechain_edges_classify_as_subagent_while_mainline_edges_stay_reply() {
        // 会话树血缘分类：provider 格式唯一能证明的边类型是 claude 的
        // isSidechain（subagent/分支标记）→ 其 parentUuid 入边必须标
        // Subagent；主线消息的边保持 Reply。fork/retry/continuation 在
        // provider 格式里无显式字段区分（parentUuid 不携带类型），绝不
        // 臆造三分类——这是 hstry fork_type 三分类的诚实子集。
        let mut main_parent = staged_message(0, "main-native", (0, 9));
        main_parent.parent_native_id = None;
        let mut main_child = staged_message(1, "main-child", (10, 19));
        main_child.parent_native_id = Some("main-native".into());
        let mut side_child = staged_message(2, "side-native", (20, 29));
        side_child.is_sidechain = true;
        side_child.parent_native_id = Some("main-child".into());
        let staged = staged_batch(vec![main_parent, main_child, side_child], 0, "session-1");
        let source = staged_to_source(
            "synthetic.jsonl",
            &staged,
            "synthetic",
            "synthetic/jsonl-v1",
            "fingerprint",
            32,
        )
        .unwrap();

        let mut relations_by_child = BTreeMap::new();
        for edge in &source.edges {
            relations_by_child.insert(edge.child_placement_id.as_str().to_string(), edge.relation);
        }
        assert_eq!(relations_by_child.len(), 2, "只有带 parent 的消息产生边");
        let side_placement = source
            .placements
            .iter()
            .find(|placement| placement.is_sidechain)
            .unwrap();
        assert_eq!(
            relations_by_child[side_placement.id.as_str()],
            MessageRelation::Subagent,
            "sidechain 消息的入边必须标 Subagent，不得伪装成 Reply"
        );
        for (placement_id, relation) in &relations_by_child {
            if placement_id != side_placement.id.as_str() {
                assert_eq!(
                    *relation,
                    MessageRelation::Reply,
                    "主线消息的入边保持 Reply（fork/retry 无格式区分，不臆造）"
                );
            }
        }
    }

    #[test]
    fn staged_to_source_caps_fts_text_but_keeps_full_payload() {
        // 借鉴清单 #3：entry 的 FTS text 元素截断到 MESSAGE_FTS_MAX_CHARS（与
        // 索引侧同一常量），而 catalog payload 保留 provider 原文全文
        // （THREAT-MODEL：Catalog 不在索引期改写原文）。入口截断保证 current
        // 判定两侧比较同一有界文本，重同步幂等不被截断破坏。
        let full = format!(
            "visible-head {}",
            "f".repeat(agent_session_grep_application::MESSAGE_FTS_MAX_CHARS)
        );
        let mut message = staged_message(0, "long-native", (0, 4));
        message.text = full.clone();
        let staged = staged_batch(vec![message], 0, "session-1");
        let source = staged_to_source(
            "synthetic.jsonl",
            &staged,
            "synthetic",
            "synthetic/jsonl-v1",
            "fingerprint",
            8,
        )
        .unwrap();

        let entry = source
            .entries
            .iter()
            .find(|(id, _, _)| id.kind() == IdKind::Message)
            .unwrap();
        assert_eq!(
            entry.2.chars().count(),
            agent_session_grep_application::MESSAGE_FTS_MAX_CHARS,
            "entry FTS text 必须截断到索引侧同一上限"
        );
        let payload: serde_json::Value = serde_json::from_slice(&entry.1).unwrap();
        assert_eq!(
            payload["text"].as_str().unwrap(),
            full,
            "payload 必须保留 provider 原文全文"
        );
    }

    #[test]
    fn parse_report_committed_count_must_match_emitted_messages() {
        let mut staged = staged_batch(vec![staged_message(0, "native", (0, 4))], 0, "session-1");
        staged.report.committed = 2;
        let error = staged_to_source(
            "synthetic.jsonl",
            &staged,
            "synthetic",
            "synthetic/jsonl-v1",
            "fingerprint",
            8,
        );
        let error = match error {
            Err(error) => error,
            Ok(_) => panic!("expected committed/emitted mismatch"),
        };
        assert_eq!(error.0.code, CanonicalCode::Internal);
    }

    fn dto(precision: Precision) -> EvidenceSpanDto {
        EvidenceSpanDto {
            occurrence_id: "occ".into(),
            message_id: "msg_v1_x".into(),
            source_document_id: None,
            generation: 1,
            source_fingerprint: None,
            byte_start: None,
            byte_end: None,
            line_start: None,
            line_end: None,
            record_ordinal: Some(0),
            snippet_char_start: None,
            snippet_char_end: None,
            precision,
        }
    }

    fn context_response(evidence: Vec<EvidenceSpanDto>) -> AppResponse {
        AppResponse::Context {
            session_id: "ses_v1_s".into(),
            session: serde_json::json!({}),
            branch_leaf: None,
            branch_leaf_placement_id: None,
            messages: Vec::new(),
            evidence,
            tool_activities: Vec::new(),
            requested_level: ContextLevel::Raw,
            effective_level: ContextLevel::Raw,
            talks: Vec::new(),
            summary: None,
            hint: None,
            truncation: Truncation {
                truncated: false,
                reason: None,
            },
            generation: 1,
        }
    }

    #[test]
    fn context_render_warns_on_unknown_precision_evidence() {
        // 3 条证据中 2 条 unknown → 一条如实计数的 warning（design §0.2）。
        let (_, _, _, warnings) = render(context_response(vec![
            dto(Precision::Byte),
            dto(Precision::Unknown),
            dto(Precision::Unknown),
        ]));
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("2 of 3"), "{warnings:?}");
        assert!(warnings[0].contains("re-ingest"), "{warnings:?}");
    }

    #[test]
    fn context_render_stays_silent_on_full_precision() {
        let (_, _, _, warnings) = render(context_response(vec![dto(Precision::Byte)]));
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    // ---- 参数解析（Minor-2 / Minor-3）----

    #[test]
    fn parse_db_flag_keeps_flag_named_tokens_after_command_as_query_text() {
        // 命令名之后的 token 即使形如 flag 也是位置参数（查询文本），
        // 不得被全局 flag 跳过逻辑吞掉。
        let (db, rest) = parse_db_flag(&[
            "--db".into(),
            "store.db".into(),
            "search".into(),
            "--output".into(),
        ])
        .expect("valid db flag");
        assert_eq!(db, "store.db");
        assert_eq!(rest, vec!["search".to_string(), "--output".to_string()]);

        let (_, rest) = parse_db_flag(&[
            "--db".into(),
            "store.db".into(),
            "--robot".into(),
            "search".into(),
            "--help".into(),
        ])
        .expect("valid db flag");
        assert_eq!(rest, vec!["search".to_string(), "--help".to_string()]);
    }

    #[test]
    fn parse_db_flag_treats_flags_after_command_as_positionals() {
        // `status --db x`：命令后的 --db 是多余位置参数（由 no_extra_args 拒绝），
        // 不再被当全局 flag 消费——flag 只在前缀位置识别。
        let (db, rest) = parse_db_flag(&[
            "--db".into(),
            "s.db".into(),
            "status".into(),
            "--db".into(),
            "x".into(),
        ])
        .expect("valid db flag");
        assert_eq!(db, "s.db");
        assert_eq!(
            rest,
            vec!["status".to_string(), "--db".to_string(), "x".to_string(),]
        );
    }

    #[test]
    fn extra_positional_arguments_are_usage_errors() {
        assert!(no_extra_args(&["get".into(), "id".into()], 1, "get <wire-id>").is_ok());
        assert!(
            no_extra_args(
                &["get".into(), "id".into(), "extra".into()],
                1,
                "get <wire-id>"
            )
            .is_err()
        );
        assert!(no_extra_args(&["status".into()], 0, "status").is_ok());
        assert!(no_extra_args(&["status".into(), "x".into()], 0, "status").is_err());
        assert!(no_extra_args(&["index".into(), "rebuild".into()], 1, "index rebuild").is_ok());
        assert!(
            no_extra_args(
                &["index".into(), "rebuild".into(), "x".into()],
                1,
                "index rebuild"
            )
            .is_err()
        );
    }

    #[test]
    fn bare_positionals_skip_value_and_bare_flags() {
        assert_eq!(
            bare_positionals(&[
                "--robot".into(),
                "doctor".into(),
                "--db".into(),
                "s.db".into(),
                "--output".into(),
                "json".into(),
            ]),
            vec!["doctor".to_string()]
        );
        // doctor 多余位置参数 → 用法错误。
        assert_eq!(
            bare_positionals(&["doctor".into(), "bogus".into()]),
            vec!["doctor".to_string(), "bogus".to_string()]
        );
    }

    #[test]
    fn extract_request_id_only_reads_prefix_position() {
        // 前缀位置（命令名之前）的 --request-id 正常抽取。
        assert_eq!(
            extract_request_id(&[
                "--db".into(),
                "s.db".into(),
                "--request-id".into(),
                "corr.1".into(),
                "status".into(),
            ])
            .expect("valid request id"),
            Some("corr.1".to_string())
        );
        // 命令名之后的同名 token 是查询文本，不是 flag。
        assert_eq!(
            extract_request_id(&["search".into(), "--request-id".into()]).expect("no flag"),
            None
        );
        // 带值 flag 的取值跳过，不会误停扫描；缺失值仍是用法错误。
        assert_eq!(
            extract_request_id(&["--db".into(), "s.db".into()]).expect("no flag"),
            None
        );
        assert!(extract_request_id(&["--request-id".into()]).is_err());
    }

    // ---- Provider capability matrix entry-point projection ----

    #[test]
    fn provider_output_has_every_current_matrix_row_and_enum_maturity() {
        let matrix = ProviderCapabilityMatrix::current();
        let output = provider_matrix_data();
        let rows = output["providers"].as_array().expect("providers array");
        assert_eq!(rows.len(), matrix.providers.len());
        for (row, capability) in rows.iter().zip(&matrix.providers) {
            assert_eq!(row["provider_id"], capability.provider_id);
            assert_eq!(
                row["maturity"],
                serde_json::to_value(capability.maturity).expect("serialize maturity")
            );
            assert_eq!(
                row["maturity_target"],
                serde_json::to_value(ProviderMaturity::target_for(&capability.provider_id))
                    .expect("serialize maturity target")
            );
        }
    }

    #[test]
    fn deferred_provider_output_has_null_maturity_target() {
        let output = provider_matrix_data();
        let rows = output["providers"].as_array().expect("providers array");
        for provider_id in ["deepseek-harness", "zcode"] {
            let row = rows
                .iter()
                .find(|row| row["provider_id"] == provider_id)
                .unwrap_or_else(|| panic!("missing deferred provider {provider_id}"));
            assert!(row["maturity_target"].is_null(), "{row}");
        }
    }

    // ---- help/version 提前拦截（ADR-0006，R3）----

    const KNOWN_COMMANDS: [&str; 20] = [
        "ingest",
        "sync",
        "index",
        "search",
        "handoff",
        "get-message",
        "get-session-resume",
        "resume",
        "hook",
        "get",
        "show",
        "list",
        "context",
        "status",
        "mcp",
        "tui",
        "doctor",
        "providers",
        "config",
        "model",
    ];

    #[test]
    fn intercept_top_level_help_and_version() {
        assert_eq!(intercept_help_or_version(&[]), None);
        assert_eq!(
            intercept_help_or_version(&["--help".into()]),
            Some(HelpRequest::TopLevelHelp)
        );
        assert_eq!(
            intercept_help_or_version(&["-h".into()]),
            Some(HelpRequest::TopLevelHelp)
        );
        assert_eq!(
            intercept_help_or_version(&["--version".into()]),
            Some(HelpRequest::TopLevelVersion)
        );
        assert_eq!(
            intercept_help_or_version(&["-V".into()]),
            Some(HelpRequest::TopLevelVersion)
        );
        // 带值 flag 的取值跳过，不影响顶层拦截。
        assert_eq!(
            intercept_help_or_version(&["--db".into(), "s.db".into(), "--help".into()]),
            Some(HelpRequest::TopLevelHelp)
        );
        assert_eq!(
            intercept_help_or_version(&["--output".into(), "json".into(), "--version".into()]),
            Some(HelpRequest::TopLevelVersion)
        );
        // 两者同时出现时帮助优先（与旧行为一致）。
        assert_eq!(
            intercept_help_or_version(&["--help".into(), "--version".into()]),
            Some(HelpRequest::TopLevelHelp)
        );
    }

    #[test]
    fn intercept_subcommand_help_for_every_known_command() {
        for cmd in KNOWN_COMMANDS {
            assert_eq!(
                intercept_help_or_version(&[cmd.into(), "--help".into()]),
                Some(HelpRequest::SubcommandHelp(cmd.into())),
                "{cmd} --help"
            );
            assert_eq!(
                intercept_help_or_version(&[cmd.into(), "-h".into()]),
                Some(HelpRequest::SubcommandHelp(cmd.into())),
                "{cmd} -h"
            );
            // --db 前置时同样拦截——help 不要求 --db（R3.1）。
            assert_eq!(
                intercept_help_or_version(&[
                    "--db".into(),
                    "s.db".into(),
                    cmd.into(),
                    "--help".into()
                ]),
                Some(HelpRequest::SubcommandHelp(cmd.into())),
                "--db s.db {cmd} --help"
            );
        }
        // index rebuild --help / -h 也覆盖。
        assert_eq!(
            intercept_help_or_version(&["index".into(), "rebuild".into(), "--help".into()]),
            Some(HelpRequest::SubcommandHelp("index".into()))
        );
        assert_eq!(
            intercept_help_or_version(&[
                "--db".into(),
                "s.db".into(),
                "index".into(),
                "rebuild".into(),
                "-h".into()
            ]),
            Some(HelpRequest::SubcommandHelp("index".into()))
        );
    }

    #[test]
    fn help_flag_after_query_text_is_not_intercepted() {
        // `search foo --help`：--help 不在命令名的紧跟位，是查询文本（R3.1/R9.4）。
        assert_eq!(
            intercept_help_or_version(&["search".into(), "foo".into(), "--help".into()]),
            None
        );
        // 未知命令后的 --help 不拦截，留给 dispatch 报 unknown subcommand。
        assert_eq!(
            intercept_help_or_version(&["bogus".into(), "--help".into()]),
            None
        );
        // 无帮助旗标的普通命令也不拦截。
        assert_eq!(intercept_help_or_version(&["status".into()]), None);
        assert_eq!(
            intercept_help_or_version(&["index".into(), "rebuild".into()]),
            None
        );
    }

    #[test]
    fn every_known_subcommand_has_help_text() {
        for cmd in KNOWN_COMMANDS {
            let text = subcommand_help_text(cmd);
            assert!(text.contains(cmd), "{cmd}: {text}");
            assert!(!text.is_empty(), "{cmd}");
        }
        // 顶层帮助列出 index rebuild，且能到达（R3.1 可达性）。
        assert!(help_text().contains("index rebuild"));
    }

    /// README 原文：Quickstart 是访客照抄的第一组命令，却是手写的。
    const README_SOURCE: &str = include_str!("../../../README.md");

    #[test]
    fn readme_documented_commands_are_all_dispatchable() {
        // 与能力列同一条纪律，往上挪一层：README 的 Quickstart 命令此前没有任何
        // 测试对照真实命令表。命令改名/下线后 README 会静默变成谎言，而它正是
        // 新用户照抄的那几行——文档漂移在这里的代价是"开箱即错"。
        //
        // 做法：从 README 的 fenced 代码块里抽出所有 `asg`/`agent-session-grep`
        // 调用行，取其子命令 token（跳过 `--flag` 与其值占位符），要求每个都在
        // KNOWN_COMMANDS 里。KNOWN_COMMANDS 已由
        // `intercept_subcommand_help_for_every_known_command` 与
        // `every_known_subcommand_has_help_text` 钉在真实 dispatch 上，故这里
        // 传递地锚定到真实行为，而不是另一份手抄表。
        let mut in_fence = false;
        let mut documented: std::collections::BTreeSet<String> = Default::default();
        for line in README_SOURCE.lines() {
            if line.trim_start().starts_with("```") {
                in_fence = !in_fence;
                continue;
            }
            if !in_fence {
                continue;
            }
            let trimmed = line.trim();
            let rest = trimmed
                .strip_prefix("asg ")
                .or_else(|| trimmed.strip_prefix("agent-session-grep "));
            let Some(rest) = rest else { continue };
            // 第一个不以 `-` 开头、且不是前一个 flag 的值占位符的 token 即子命令。
            let mut expect_flag_value = false;
            for token in rest.split_whitespace() {
                if token.starts_with('-') {
                    // `--db <path>` 形式的 flag 带值；`--discover` 不带。
                    expect_flag_value = !token.contains('=');
                    continue;
                }
                if expect_flag_value {
                    expect_flag_value = false;
                    continue;
                }
                documented.insert(token.to_string());
                break;
            }
        }

        assert!(
            documented.len() >= 5,
            "README Quickstart 解析出的命令过少（{}），解析逻辑可能失效：{documented:?}",
            documented.len()
        );
        for cmd in &documented {
            assert!(
                KNOWN_COMMANDS.contains(&cmd.as_str()),
                "README 记录的命令 `{cmd}` 不在 KNOWN_COMMANDS 里——命令已改名/下线，\
                 而 README 是新用户照抄的第一组命令"
            );
        }
    }

    #[test]
    fn machine_mode_help_is_a_success_envelope() {
        let envelope = help_envelope(
            "search",
            serde_json::json!({ "help_text": "search <query>..." }),
            Some("req-9"),
        );
        let v: serde_json::Value = serde_json::from_str(&envelope).expect("valid JSON");
        assert_eq!(v["frame_type"], "response");
        assert_eq!(v["command"], "search");
        assert_eq!(v["ok"], true);
        assert_eq!(v["request_id"], "req-9");
        assert_eq!(v["data"]["help_text"], "search <query>...");

        let version = help_envelope(
            "version",
            serde_json::json!({ "version": "agent-session-grep 0.1.0" }),
            None,
        );
        let v: serde_json::Value = serde_json::from_str(&version).expect("valid JSON");
        assert_eq!(v["command"], "version");
        assert_eq!(v["data"]["version"], "agent-session-grep 0.1.0");
    }

    #[test]
    fn subcommand_help_works_without_db() {
        // run() 在拦截阶段就返回，不进入 parse_db_flag / 存储打开（R3.1：help
        // 无需 --db，且不创建任何文件）。
        assert!(run(&["--help".into()], protocol::OutputMode::Human, None, false).is_ok());
        assert!(
            run(
                &["--version".into()],
                protocol::OutputMode::Human,
                None,
                false
            )
            .is_ok()
        );
        assert!(
            run(
                &["search".into(), "--help".into()],
                protocol::OutputMode::Human,
                None,
                false
            )
            .is_ok()
        );
        assert!(
            run(
                &["index".into(), "rebuild".into(), "--help".into()],
                protocol::OutputMode::Human,
                None,
                false
            )
            .is_ok()
        );
        // 机器模式同样成功（exit 0），不发裸文本。
        assert!(
            run(
                &["--robot".into(), "--help".into()],
                protocol::OutputMode::Json,
                None,
                false
            )
            .is_ok()
        );
    }

    // ---- 解析健壮性（R8）----

    #[test]
    fn db_flag_rejects_flag_named_value() {
        // `--db --robot status` 曾造出名为 `--robot` 的文件；取值是已知 flag
        // 一律用法错误（R8.1），任何取值都不落入文件系统。
        for value in [
            "--robot",
            "--output",
            "--request-id",
            "--help",
            "--version",
            "--db",
            "-h",
            "-V",
            "--cursor",
            "--max-items",
            "--max-bytes",
            "--max-messages",
            "--policy",
            "--level",
            "--provider",
            "--since",
            "--until",
            "--session",
            "--around",
            "--snapshot-json",
            "--offline",
        ] {
            let error = parse_db_flag(&["--db".into(), value.into(), "status".into()])
                .expect_err("flag-named value must be rejected");
            assert_eq!(error.0.code, CanonicalCode::InvalidRequest, "{value}");
        }
        // 缺值同样是用法错误。
        let error = parse_db_flag(&["--db".into()]).expect_err("missing value rejected");
        assert_eq!(error.0.code, CanonicalCode::InvalidRequest);
    }

    #[test]
    fn db_flag_rejects_duplicate() {
        // `--db a --db b status` 曾静默 last-wins；重复 --db 是用法错误（R8.2）。
        let error = parse_db_flag(&[
            "--db".into(),
            "a.db".into(),
            "--db".into(),
            "b.db".into(),
            "status".into(),
        ])
        .expect_err("duplicate --db rejected");
        assert_eq!(error.0.code, CanonicalCode::InvalidRequest);
        assert!(error.0.message.contains("duplicate"));
    }

    #[test]
    fn request_id_rejects_flag_value_and_duplicate() {
        // `--request-id --robot` 的 --robot 能通过 id 字符集校验，曾静默当作合法
        // id；取值是已知 flag 与重复 --request-id 都是用法错误（R8.1/R8.2）。
        assert!(
            extract_request_id(&["--request-id".into(), "--robot".into(), "status".into()])
                .is_err()
        );
        assert!(
            extract_request_id(&[
                "--request-id".into(),
                "a".into(),
                "--request-id".into(),
                "b".into(),
                "status".into()
            ])
            .is_err()
        );
        assert!(extract_request_id(&["--request-id".into()]).is_err());
        // 合法取值不受影响。
        assert_eq!(
            extract_request_id(&["--request-id".into(), "corr.1".into(), "status".into()])
                .expect("valid id"),
            Some("corr.1".to_string())
        );
    }

    #[test]
    fn doctor_and_config_reject_unknown_positionals() {
        // `--robot doctor --bogus` 曾静默丢弃 --bogus 后 exit 0（R8.3）。
        assert!(
            doctor(
                &["doctor".into(), "--bogus".into()],
                protocol::OutputMode::Human,
                None,
                false
            )
            .is_err()
        );
        // doctor 的 --db 同样受取值守卫保护：flag 当取值 / 重复 / 缺值都拒绝。
        assert!(
            doctor(
                &["doctor".into(), "--db".into(), "--robot".into()],
                protocol::OutputMode::Human,
                None,
                false
            )
            .is_err()
        );
        assert!(
            doctor(
                &[
                    "doctor".into(),
                    "--db".into(),
                    "a.db".into(),
                    "--db".into(),
                    "b.db".into()
                ],
                protocol::OutputMode::Human,
                None,
                false
            )
            .is_err()
        );
        assert!(
            doctor(
                &["doctor".into(), "--db".into()],
                protocol::OutputMode::Human,
                None,
                false
            )
            .is_err()
        );
        // `--robot config paths --bogus` 曾 exit 0。
        assert!(
            run(
                &["config".into(), "paths".into(), "--bogus".into()],
                protocol::OutputMode::Human,
                None,
                false
            )
            .is_err()
        );
        assert!(
            run(
                &[
                    "--robot".into(),
                    "config".into(),
                    "paths".into(),
                    "--bogus".into()
                ],
                protocol::OutputMode::Json,
                None,
                false
            )
            .is_err()
        );
    }

    #[test]
    fn command_name_normalizes_unknown_tokens_without_skipping_them() {
        // 未知 '-' 开头 token 不是 flag——它是命令名笔误，错误 envelope 的
        // command 使用安全的 unknown，而不是跳到后面的真命令。
        assert_eq!(
            command_name(&["--bogus".into(), "--robot".into(), "status".into()]),
            "unknown"
        );
        assert_eq!(
            command_name(&[
                "--db".into(),
                "s.db".into(),
                "--bogus".into(),
                "status".into()
            ]),
            "unknown"
        );
        // 已知 flag 与其取值跳过，命令名正常识别。
        assert_eq!(command_name(&["--robot".into(), "status".into()]), "status");
        assert_eq!(
            command_name(&[
                "--db".into(),
                "s.db".into(),
                "--output".into(),
                "json".into(),
                "search".into()
            ]),
            "search"
        );
        assert_eq!(
            command_name(&[
                "--request-id".into(),
                "r1".into(),
                "sync".into(),
                "a.jsonl".into()
            ]),
            "sync"
        );
        assert_eq!(
            command_name(&["--robot".into(), "--help".into()]),
            "unknown"
        );
    }

    // ---- `--offline` 全局 flag（design D5 / PRD 08-15-offline-privacy-hooks）----

    #[test]
    fn extract_offline_flag_only_reads_prefix_position() {
        // 前缀位置（命令名之前）识别；命令名之后同名 token 是位置参数。
        assert!(extract_offline_flag(&["--offline".into(), "status".into()]));
        assert!(extract_offline_flag(&[
            "--db".into(),
            "s.db".into(),
            "--offline".into(),
            "status".into()
        ]));
        assert!(extract_offline_flag(&["--offline".into()]));
        assert!(!extract_offline_flag(&["status".into()]));
        assert!(!extract_offline_flag(&[
            "status".into(),
            "--offline".into()
        ]));
        // 带值 flag 的取值跳过，不会把取值误当命令名。
        assert!(extract_offline_flag(&[
            "--db".into(),
            "s.db".into(),
            "--offline".into(),
            "search".into()
        ]));
        assert!(!extract_offline_flag(&["--db".into(), "--offline".into()]));
    }

    #[test]
    fn offline_is_registered_in_all_prefix_scanners() {
        // command_name 跳过 --offline，命令名正确识别。
        assert_eq!(
            command_name(&["--offline".into(), "status".into()]),
            "status"
        );
        // is_known_flag_name 覆盖 --offline：`--db --offline` 不得把 --offline 当路径。
        assert!(is_known_flag_name("--offline"));
        assert!(parse_db_flag(&["--db".into(), "--offline".into(), "status".into()]).is_err());
        // bare_positionals 跳过 --offline：doctor 的多余参数校验不受影响。
        assert_eq!(
            bare_positionals(&["--offline".into(), "doctor".into()]),
            vec!["doctor".to_string()]
        );
        // parse_db_flag 跳过 --offline：命令名之后是 rest，不吞命令。
        let (db, rest) = parse_db_flag(&[
            "--db".into(),
            "s.db".into(),
            "--offline".into(),
            "search".into(),
        ])
        .expect("offline must not break parse_db_flag");
        assert_eq!(db, "s.db");
        assert_eq!(rest, vec!["search".to_string()]);
        // help 拦截：--offline --help 仍是顶层帮助。
        assert_eq!(
            intercept_help_or_version(&["--offline".into(), "--help".into()]),
            Some(HelpRequest::TopLevelHelp)
        );
    }

    #[test]
    fn offline_gate_refuses_network_capabilities_and_passes_local_ones() {
        // 未来联网命令在 offline 下必须拒绝，且 code 稳定为 capability_not_supported。
        let error = offline_capability_gate(true, "model-download")
            .expect_err("offline must refuse network capability");
        assert_eq!(error.0.code, CanonicalCode::CapabilityNotSupported);
        assert_eq!(error.0.code.exit_code(), 7);
        assert!(!error.0.code.retryable());
        assert!(error.0.message.contains("model-download"));
        // 非 offline 时正常放行；任何命令在非 offline 下都不该被 gate 拦。
        assert!(offline_capability_gate(false, "model-download").is_ok());
        assert!(offline_capability_gate(false, "telemetry").is_ok());
    }

    #[test]
    fn offline_combines_with_sync_and_search_dispatch() {
        // offline + 既有本地命令（sync/search）必须正常：flag 不改动既有行为。
        let store = SqliteStore::open_in_memory().expect("in-memory store opens");
        let (command, outcome, data, _, _) = dispatch(
            &store,
            "test.db",
            &["search".into(), "foo".into()],
            protocol::OutputMode::Json,
            None,
            true,
        )
        .expect("offline search must succeed");
        assert_eq!(command, "search");
        assert_eq!(outcome, protocol::Outcome::Success);
        assert_eq!(data["hits"].as_array().expect("hits").len(), 0);
        // sync 目录拒绝在 offline 下同样走 usage error（不吞错误）。
        let dir = std::env::temp_dir().to_string_lossy().into_owned();
        let error = dispatch(
            &store,
            "test.db",
            &["sync".into(), dir.clone()],
            protocol::OutputMode::Json,
            None,
            true,
        )
        .expect_err("offline sync directory must still be rejected");
        assert_eq!(error.0.code, CanonicalCode::InvalidRequest);
        assert!(!error.0.message.contains(&dir), "{:?}", error.0.message);
    }

    #[test]
    fn doctor_accepts_offline_without_error() {
        // doctor 在 --offline 下照常自检（offline 是稳定显式模式，不新增错误路径）；
        // offline 字段由 emit_result 输出，unit 层只验证成功与诊断字段存在性。
        assert!(doctor(&["doctor".into()], protocol::OutputMode::Human, None, true).is_ok());
        // 与不带 --db 的默认 doctor 等价，offline 不改变退出语义。
        assert!(
            doctor(
                &["--offline".into(), "doctor".into()],
                protocol::OutputMode::Human,
                None,
                true
            )
            .is_ok()
        );
    }

    #[test]
    fn hook_search_filters_maps_providers_and_decay() {
        // 空配置：不过滤（EMPTY filters）。
        let empty = hooks::HookConfig::default();
        let filters = hook_search_filters(&empty, 1_000_000).expect("empty config is valid");
        assert!(filters.providers.is_empty());
        assert!(filters.since.is_none());
        assert!(filters.until.is_none());

        // provider 白名单：claude/codex 别名映射到 SearchProvider。
        let providers = hooks::HookConfig {
            providers: vec!["claude".into(), "codex".into()],
            ..Default::default()
        };
        let filters = hook_search_filters(&providers, 0).expect("providers valid");
        assert_eq!(
            filters.providers,
            vec![SearchProvider::Claude, SearchProvider::Codex]
        );
        assert!(filters.since.is_none());

        // The hook consumes the same provider values as search. Canonical ids
        // emitted by machine surfaces must resolve exactly like legacy aliases.
        let canonical = hooks::HookConfig {
            providers: vec!["claude-code".into()],
            ..Default::default()
        };
        let filters = hook_search_filters(&canonical, 0).expect("canonical id valid");
        assert_eq!(filters.providers, vec![SearchProvider::Claude]);

        // 未知 provider 是用法错误，不静默忽略（fail-closed）；错误信息与
        // search --provider 同风格回显取值（provider 值不是路径/secret）。
        let bad = hooks::HookConfig {
            providers: vec!["nope".into()],
            ..Default::default()
        };
        let error = hook_search_filters(&bad, 0).expect_err("unknown provider rejected");
        assert_eq!(error.0.code, CanonicalCode::InvalidRequest);
        assert!(error.0.message.contains("nope"), "{:?}", error.0.message);

        // 时间衰减：decay_days = 7 → since ≈ now - 7 天。
        let decay = hooks::HookConfig {
            decay_days: 7,
            ..Default::default()
        };
        let now_ms = 1_000_000_000i64;
        let filters = hook_search_filters(&decay, now_ms).expect("decay valid");
        let since_ms = filters.since.expect("decay sets since").unix_seconds * 1_000;
        assert_eq!(since_ms, now_ms - 7 * 86_400_000);

        // repo（schema v16）：缺省不限仓库；有值逐字进 filters.repo；空/纯空白
        // 是用法错误（与 search --repo 同一门禁，绝不静默变成全库注入）。
        assert!(
            hook_search_filters(&hooks::HookConfig::default(), now_ms)
                .expect("default valid")
                .repo
                .is_none()
        );
        let scoped = hooks::HookConfig {
            repo: Some("github.com/synthetic-owner/synthetic-repo".into()),
            ..Default::default()
        };
        assert_eq!(
            hook_search_filters(&scoped, now_ms)
                .expect("repo valid")
                .repo
                .as_deref(),
            Some("github.com/synthetic-owner/synthetic-repo")
        );
        for bad in ["", "   "] {
            let blank = hooks::HookConfig {
                repo: Some(bad.into()),
                ..Default::default()
            };
            let error = hook_search_filters(&blank, now_ms).expect_err("blank repo rejected");
            assert_eq!(error.0.code, CanonicalCode::InvalidRequest);
            assert!(error.0.message.contains("--repo"), "{:?}", error.0.message);
        }
    }

    #[test]
    fn search_filters_from_flags_binds_repo_parameter() {
        // 有值：逐字进 filters.repo（形状不校验——未命中即诚实空页）。
        let filters =
            search_filters_from_flags(&[], None, None, Some("github.com/o/app"), 1_000_000)
                .expect("repo valid");
        assert_eq!(filters.repo.as_deref(), Some("github.com/o/app"));
        assert!(
            !filters.is_empty(),
            "repo-only filter must not read as empty"
        );

        // 缺省：repo 无限制。
        let filters = search_filters_from_flags(&[], None, None, None, 1_000_000).expect("no repo");
        assert!(filters.repo.is_none());
        assert!(filters.is_empty());

        // 空串/纯空白是用法错误（拼写错误不得伪装成零结果）。
        for bad in ["", "   "] {
            let error =
                search_filters_from_flags(&[], None, None, Some(bad), 1_000_000).unwrap_err();
            assert_eq!(error.0.code, CanonicalCode::InvalidRequest);
            assert!(error.0.message.contains("--repo"), "{:?}", error.0.message);
        }
    }

    // ---- 隐私（R2）----

    #[test]
    fn not_found_error_never_echoes_the_wire_id() {
        let store = SqliteStore::open_in_memory().expect("in-memory store opens");
        let wire = "msg_v1_private-wire-id";
        for cmd in ["get", "show"] {
            let error = dispatch(
                &store,
                "test.db",
                &[cmd.into(), wire.into()],
                protocol::OutputMode::Human,
                None,
                false,
            )
            .expect_err("missing entity must fail");
            // exit 4 契约（ADR-0005）不变，只改消息。
            assert_eq!(error.0.code, CanonicalCode::NotFound, "{cmd}");
            assert_eq!(error.0.message, "entity not found", "{cmd}");
            assert!(
                !error.0.message.contains(wire),
                "{cmd} message must not echo the wire id"
            );
        }
    }

    #[test]
    fn sync_directory_rejection_is_path_free_and_platform_neutral() {
        let store = SqliteStore::open_in_memory().expect("in-memory store opens");
        let dir = std::env::temp_dir();
        let dir_str = dir.to_string_lossy().into_owned();
        let error = dispatch(
            &store,
            "test.db",
            &["sync".into(), dir_str.clone()],
            protocol::OutputMode::Json,
            None,
            false,
        )
        .expect_err("directory must be rejected");
        assert_eq!(error.0.code, CanonicalCode::InvalidRequest);
        let message = &error.0.message;
        assert!(!message.contains(&dir_str), "path must not leak: {message}");
        assert!(
            !message.contains("PowerShell"),
            "platform-neutral: {message}"
        );
        assert!(
            !message.contains("Get-ChildItem"),
            "platform-neutral: {message}"
        );
    }

    #[test]
    fn sync_discover_rejects_extra_flags() {
        let store = SqliteStore::open_in_memory().expect("in-memory store opens");
        let error = dispatch(
            &store,
            "test.db",
            &["sync".into(), "--discover".into(), "--bogus".into()],
            protocol::OutputMode::Json,
            None,
            false,
        )
        .expect_err("discover must reject unknown extra flags");
        assert_eq!(error.0.code, CanonicalCode::InvalidRequest);
    }

    #[test]
    fn take_bool_flag_removes_and_reports_presence() {
        // R2/R3 布尔旗标：在场移除 token 并返回 true，缺场返回 false；不消费取值。
        let mut args = vec![
            "foo".into(),
            "--include-system".into(),
            "--group-by-session".into(),
        ];
        assert!(take_bool_flag(&mut args, "--include-system"));
        assert!(take_bool_flag(&mut args, "--group-by-session"));
        assert!(!take_bool_flag(&mut args, "--include-system"));
        assert_eq!(args, vec![String::from("foo")]);
    }

    #[test]
    fn read_snapshot_composition_releases_after_success_and_error() {
        let store = SqliteStore::open_in_memory().unwrap();
        for (args, mode, succeeds) in [
            (vec!["search", "needle"], protocol::OutputMode::Human, true),
            (vec!["handoff", "needle"], protocol::OutputMode::Json, true),
            (
                vec!["handoff", "needle", "--max-tokens", "invalid"],
                protocol::OutputMode::Json,
                false,
            ),
        ] {
            let args: Vec<String> = args.into_iter().map(str::to_owned).collect();
            assert_eq!(
                dispatch(&store, "test.db", &args, mode, None, false).is_ok(),
                succeeds
            );
            doctor_store_data(&store, true).unwrap();
            let before = store.active_generation().unwrap();
            let id = StableId::native(IdKind::Message, &format!("snapshot-{before}"));
            store
                .commit_batch(&[(id, br#"{"text":"needle"}"#.to_vec(), "needle".into())])
                .expect("completed read scopes must not leave a transaction open");
            assert_eq!(store.active_generation().unwrap(), before + 1);
        }
    }

    #[test]
    fn search_dispatch_accepts_r2r3_flags_on_empty_store() {
        // 空库上 search 返回零命中但不应把 R2/R3 旗标当多余参数拒绝（R2/R3 是
        // 合法命令级 flag）。缺实现时 --include-system 会触发 usage error。
        let store = SqliteStore::open_in_memory().expect("in-memory store opens");
        let (command, outcome, data, _, _) = dispatch(
            &store,
            "test.db",
            &[
                "search".into(),
                "foo".into(),
                "--include-system".into(),
                "--group-by-session".into(),
            ],
            protocol::OutputMode::Json,
            None,
            false,
        )
        .expect("search with r2r3 flags must succeed");
        assert_eq!(command, "search");
        assert_eq!(outcome, protocol::Outcome::Success);
        assert_eq!(data["hits"].as_array().expect("hits").len(), 0);
    }

    #[test]
    fn render_search_emits_session_context_fields() {
        // SearchHit.session_id/text 由 application 装配（批量 session_of +
        // 批量取 payload 截取摘要）；render 原样投影——Some → 字符串，
        // None → null（不臆造会话/摘要；human 渲染器不补行）。
        let response = AppResponse::Search {
            hits: vec![
                agent_session_grep_ports::SearchHit {
                    id: StableId::from_wire("msg_v1_aaaa").expect("valid id"),
                    score: 2.0,
                    session_id: Some("ses_v1_aaaa".into()),
                    text: Some("正文预览".into()),
                    why_matched: Vec::new(),
                    suggested_next_commands: Vec::new(),
                    occurrences: 1,
                    resume_available: false,
                },
                agent_session_grep_ports::SearchHit {
                    id: StableId::from_wire("msg_v1_bbbb").expect("valid id"),
                    score: 1.0,
                    session_id: None,
                    text: None,
                    why_matched: Vec::new(),
                    suggested_next_commands: Vec::new(),
                    occurrences: 1,
                    resume_available: false,
                },
            ],
            next_cursor: None,
            generation: 3,
            truncation: Truncation {
                truncated: false,
                reason: None,
            },
            retrieval_mode: RetrievalMode::Lexical,
            fallback_warning: None,
        };
        let (_, data, _, _) = render(response);
        assert_eq!(data["hits"][0]["session_id"], "ses_v1_aaaa");
        assert_eq!(data["hits"][0]["text"], "正文预览");
        assert_eq!(data["hits"][1]["session_id"], serde_json::Value::Null);
        assert_eq!(data["hits"][1]["text"], serde_json::Value::Null);
        assert!(
            data["hits"][0].get("why_matched").is_none(),
            "empty collections must be omitted: {}",
            data["hits"][0]
        );
        assert!(
            data["hits"][0].get("suggested_next_commands").is_none(),
            "empty collections must be omitted: {}",
            data["hits"][0]
        );
    }

    #[test]
    fn render_search_emits_guidance_when_present() {
        let response = AppResponse::Search {
            hits: vec![agent_session_grep_ports::SearchHit {
                id: StableId::from_wire("msg_v1_aaaa").expect("valid id"),
                score: 2.0,
                session_id: Some("ses_v1_aaaa".into()),
                text: Some("preview".into()),
                why_matched: vec!["needle".into()],
                suggested_next_commands: vec![
                    "agent-session-grep get-message msg_v1_aaaa --session ses_v1_aaaa --around 2"
                        .into(),
                    "agent-session-grep context ses_v1_aaaa".into(),
                ],
                occurrences: 1,
                resume_available: false,
            }],
            next_cursor: None,
            generation: 3,
            truncation: Truncation {
                truncated: false,
                reason: None,
            },
            retrieval_mode: RetrievalMode::Lexical,
            fallback_warning: None,
        };
        let (_, data, _, _) = render(response);
        let hit = &data["hits"][0];
        assert_eq!(hit["why_matched"], serde_json::json!(["needle"]));
        assert_eq!(
            hit["suggested_next_commands"],
            serde_json::json!([
                "agent-session-grep get-message msg_v1_aaaa --session ses_v1_aaaa --around 2",
                "agent-session-grep context ses_v1_aaaa"
            ])
        );
    }

    #[test]
    fn machine_render_never_emits_snippet_field() {
        // SearchHit 已无 snippet 字段（ADR-0008 由 text 承接摘要）：render 的
        // 命中 JSON 只携带 {id, score, session_id, text}，任何输出模式下都
        // 不出现 snippet 键（机器模式协议兼容约束由构造层保证，不再需要剥除）。
        let response = AppResponse::Search {
            hits: vec![agent_session_grep_ports::SearchHit {
                id: StableId::from_wire("msg_v1_aaaa").expect("valid id"),
                score: 2.0,
                session_id: Some("ses_v1_aaaa".into()),
                text: Some("preview".into()),
                why_matched: Vec::new(),
                suggested_next_commands: Vec::new(),
                occurrences: 1,
                resume_available: false,
            }],
            next_cursor: None,
            generation: 3,
            truncation: Truncation {
                truncated: false,
                reason: None,
            },
            retrieval_mode: RetrievalMode::Lexical,
            fallback_warning: None,
        };
        let (_, data, _, _) = render(response);
        let hit = &data["hits"][0];
        assert!(hit.get("snippet").is_none(), "{hit}");
        assert_eq!(hit["id"], "msg_v1_aaaa");
        assert!(hit["score"].is_number(), "{hit}");
        assert_eq!(hit["session_id"], "ses_v1_aaaa");
        assert_eq!(hit["text"], "preview");
    }

    #[test]
    fn render_message_window_projects_typed_placement_fields() {
        let response = AppResponse::Message {
            window: agent_session_grep_application::MessageWindow {
                message_id: "msg_v1_anchor".into(),
                session_id: "ses_v1_s".into(),
                anchor_placement_id: "plc_v1_anchor".into(),
                messages: vec![agent_session_grep_application::ContextMessage {
                    id: "msg_v1_anchor".into(),
                    placement_id: "plc_v1_anchor".into(),
                    message_id: "msg_v1_anchor".into(),
                    payload: serde_json::json!({"role": "user", "text": "hi"}),
                }],
                truncation: Truncation {
                    truncated: false,
                    reason: None,
                },
                generation: 5,
            },
        };
        let (outcome, data, page, warnings) = render(response);
        assert!(matches!(outcome, protocol::Outcome::Success));
        assert!(warnings.is_empty());
        assert!(!page.has_more);
        assert_eq!(data["message_id"], "msg_v1_anchor");
        assert_eq!(data["session_id"], "ses_v1_s");
        assert_eq!(data["anchor_placement_id"], "plc_v1_anchor");
        assert_eq!(data["generation"], 5);
        let messages = data["messages"].as_array().expect("messages");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["placement_id"], "plc_v1_anchor");
        assert_eq!(messages[0]["payload"]["text"], "hi");
    }
}

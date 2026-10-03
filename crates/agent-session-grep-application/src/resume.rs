//! Resume command descriptor and dry-run preview (#5).
//!
//! Builds the resume command from `SessionResumeMetadata`. Default is
//! dry-run preview (prints the command, does not execute). `--yes` / config
//! opt-in is required for actual execution; first install forces preview
//! once regardless.
//!
//! This module owns the resume descriptor/preview; the actual provider process
//! spawn is deferred to the execution layer (requires owner authorization per
//! CLAUDE.md). Handoff pack (#4) only consumes the descriptor, never executes.

use agent_session_grep_domain::{IdKind, StableId};
use agent_session_grep_ports::SessionResumeMetadata;

/// Provider resume command descriptor: the command + args + cwd + permission
/// mode that would restore the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeDescriptor {
    /// Provider binary name (e.g. `claude`, `codex`, `pi`).
    pub provider_binary: String,
    /// Command arguments (e.g. `["--resume", "<session-id>"]`).
    pub args: Vec<String>,
    /// Original working directory to restore (if known).
    pub working_directory: Option<String>,
    /// Permission/approval mode flag (e.g. `--dangerously-skip-permissions`).
    /// `None` means default (no yolo/auto mode).
    pub permission_mode: Option<String>,
}

/// Dry-run preview result: what would be executed, without executing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumePreview {
    pub descriptor: ResumeDescriptor,
    /// Full command as a displayable string (e.g. `claude --resume abc-123`).
    pub command_string: String,
    /// Whether the session is resumable at all.
    pub available: bool,
    /// Why the session is not resumable (when `available` is false).
    pub unavailable_reason: Option<String>,
}

/// Build a resume descriptor from session metadata.
///
/// Unknown/unverified providers return `available:false` with a reason —
/// never fabricates a command. Only providers with known resume commands
/// produce a descriptor.
pub fn build_resume_descriptor(metadata: &SessionResumeMetadata) -> ResumePreview {
    if !metadata.resume_available {
        return ResumePreview {
            descriptor: ResumeDescriptor {
                provider_binary: String::new(),
                args: Vec::new(),
                working_directory: metadata.original_working_directory.clone(),
                permission_mode: None,
            },
            command_string: String::new(),
            available: false,
            unavailable_reason: metadata
                .unavailable_reason
                .clone()
                .or_else(|| Some("resume not available".to_string())),
        };
    }

    let provider_id = metadata.provider_id.as_deref().unwrap_or("");
    let session_id = metadata.provider_session_id.as_deref().unwrap_or("");

    // Use the checked identity contract for both preview and execution.
    // An argv operand starting with a hyphen can still be parsed as an option.
    let missing_session_id = session_id.trim().is_empty();
    if missing_session_id
        || session_id.starts_with('-')
        || StableId::native_checked(IdKind::Session, session_id).is_err()
    {
        return ResumePreview {
            descriptor: ResumeDescriptor {
                provider_binary: String::new(),
                args: Vec::new(),
                working_directory: metadata.original_working_directory.clone(),
                permission_mode: None,
            },
            command_string: String::new(),
            available: false,
            unavailable_reason: Some(
                if missing_session_id {
                    "resume metadata is missing the provider session id"
                } else {
                    "resume metadata contains an invalid provider session id"
                }
                .to_string(),
            ),
        };
    }

    let (binary, args) = match provider_id {
        "claude-code" => (
            "claude",
            vec!["--resume".to_string(), session_id.to_string()],
        ),
        "codex" => ("codex", vec!["resume".to_string(), session_id.to_string()]),
        "pi" => ("pi", vec!["--session".to_string(), session_id.to_string()]),
        "grok-build" => ("grok", vec!["--resume".to_string(), session_id.to_string()]),
        // 证据：fast-resume antigravity.rs `resume_command` = `agy --conversation
        // <id>`（同一 `~/.gemini/antigravity-cli` 源面，brain/*/logs JSONL 为
        // 回退面）；agent-sessions 的 AntigravityResumeCommandBuilder 同形并
        // 以 `agy --help` 校验 `--conversation` 存在。双源一致。
        "antigravity" => (
            "agy",
            vec!["--conversation".to_string(), session_id.to_string()],
        ),
        // 证据：fast-resume kimi.rs `resume_command` = `kimi --session <id>`。
        // 同源已核验：fast-resume 解析 `$KIMI_CODE_HOME/sessions/**/agents/main/
        // wire.jsonl` + `state.json`（默认 `~/.kimi-code/sessions`），与本
        // provider-kimi 的 wire.jsonl 面（`context.append_message` 封套）同一
        // CLI；provider-kimi PROVENANCE.md 亦明言结构适配自 fast-resume。
        "kimi-code" => (
            "kimi",
            vec!["--session".to_string(), session_id.to_string()],
        ),
        // 证据：AgentRecall platform.ts `getResumeCommand` 对 codebuddy-cli
        // （读 `~/.codebuddy/projects/*.jsonl`，与本 provider 同一源面）生成
        // `cd <repo> && codebuddy --resume <id>`；其 live-detection 亦识别真实
        // `codebuddy --resume <id>` 进程行。
        "tencent-codebuddy" => (
            "codebuddy",
            vec!["--resume".to_string(), session_id.to_string()],
        ),
        // 证据：fast-resume opencode.rs `resume_command` =
        // `opencode <directory> --session <id>`——directory 是 positional 参数，
        // 来自会话原始工作目录（SQLite 源同面）。目录缺失时省略 positional
        // 参数：cc-switch（`opencode -s <id>`，目录由 shell cwd 承担）与 agf
        // （`opencode -s '<id>'`）均已验证明该省略形态。
        "opencode" => {
            let mut args = Vec::new();
            if let Some(dir) = metadata.original_working_directory.as_deref()
                && !dir.trim().is_empty()
            {
                args.push(dir.to_string());
            }
            args.push("--session".to_string());
            args.push(session_id.to_string());
            ("opencode", args)
        }
        // Unknown/unverified providers: resume command is null/— (not fabricated).
        // These include: hermes（各参考项目 resume 命令冲突：agf `hermes
        // --resume <id>`、hstry `hermes --session <id>`、cc-switch/AgentRecall
        // 无 CLI resume，无权威结论）、qoder（参考项目无 resume 命令证据）、
        // cursor（fast-resume 的 `agent --resume` 属 Cursor CLI store.db 面，
        // 与本 provider 的 VS Code vscdb 面不同源），及所有未实现 provider。
        _ => {
            return ResumePreview {
                descriptor: ResumeDescriptor {
                    provider_binary: String::new(),
                    args: Vec::new(),
                    working_directory: metadata.original_working_directory.clone(),
                    permission_mode: None,
                },
                command_string: String::new(),
                available: false,
                unavailable_reason: Some(format!(
                    "resume command for provider '{provider_id}' is unverified or unsupported"
                )),
            };
        }
    };

    let mut full_args = args.clone();
    if let Some(mode) = &metadata_provider_permission_hint(provider_id) {
        full_args.push(mode.clone());
    }

    let command_string = format_command(binary, &full_args, &metadata.original_working_directory);

    ResumePreview {
        descriptor: ResumeDescriptor {
            provider_binary: binary.to_string(),
            args: full_args,
            working_directory: metadata.original_working_directory.clone(),
            permission_mode: metadata_provider_permission_hint(provider_id),
        },
        command_string,
        available: true,
        unavailable_reason: None,
    }
}

/// Format a resume command as a displayable string for dry-run preview.
///
/// Every interpolated token is provider-transcript data, which the threat model
/// treats as untrusted input (`docs/security/THREAT-MODEL.md` §2). The preview
/// exists to be copied into a shell — and an MCP/Robot client may hand it to one
/// directly — so an unquoted token turns a poisoned `cwd` into command
/// execution: a session whose recorded working directory ends in
/// `…\ws" && <injected> && cd "…` renders as a command string that runs
/// `<injected>` before ever reaching the provider.
///
/// Tokens are therefore quoted when they need it, and the whole preview is
/// refused — empty string, projected as `command: null` — when a token contains
/// a character that can still escape or expand *inside* double quotes in some
/// common shell. No single quoting style is safe across cmd.exe, PowerShell and
/// POSIX shells simultaneously, so for such a token the honest answer is no
/// command rather than a plausible-looking one; `working_directory` is still
/// reported structurally. Actual execution is unaffected either way: `--yes`
/// spawns argv directly with `current_dir` and never goes through a shell.
fn format_command(binary: &str, args: &[String], cwd: &Option<String>) -> String {
    let mut parts = Vec::with_capacity(args.len() + 1);
    for token in std::iter::once(binary).chain(args.iter().map(String::as_str)) {
        match quote_preview_token(token) {
            Some(quoted) => parts.push(quoted),
            None => return String::new(),
        }
    }
    let cmd = parts.join(" ");
    match cwd {
        Some(dir) => match quote_preview_token(dir) {
            Some(quoted) => format!("(cd {quoted} && {cmd})"),
            None => String::new(),
        },
        None => cmd,
    }
}

/// Render one preview token, or `None` when it cannot be rendered safely.
///
/// - Rejected outright: `"` (closes the quote in every shell), `$` and
///   `` ` `` (expand inside double quotes in POSIX shells and PowerShell), `%`
///   and `!` (expand inside double quotes in cmd.exe), and any control
///   character (a newline splits the command line). A token *ending* in `\` is
///   rejected too: quoted, the `\"` is a literal quote in both POSIX shells and
///   Windows argv parsing, so the closing quote never closes and the rest of the
///   line is swallowed into the argument; bare, POSIX reads it as a line
///   continuation. Interior backslashes are fine, so ordinary Windows paths
///   still preview.
/// - Double-quoted: anything carrying whitespace or a shell metacharacter that
///   double quotes *do* neutralise everywhere (`& | ; < > ( ) ^ ' * ? [ ] { } ~
///   #`), so an ordinary path with spaces still previews correctly.
/// - Left bare: plain tokens, keeping the common preview byte-identical to a
///   hand-typed command.
fn quote_preview_token(token: &str) -> Option<String> {
    if token
        .chars()
        .any(|c| c.is_control() || matches!(c, '"' | '$' | '`' | '%' | '!'))
    {
        return None;
    }
    if token.ends_with('\\') {
        return None;
    }
    let needs_quotes = token.is_empty()
        || token.chars().any(|c| {
            c.is_whitespace()
                || matches!(
                    c,
                    '&' | '|'
                        | ';'
                        | '<'
                        | '>'
                        | '('
                        | ')'
                        | '^'
                        | '\''
                        | '*'
                        | '?'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                        | '~'
                        | '#'
                )
        });
    if needs_quotes {
        Some(format!("\"{token}\""))
    } else {
        Some(token.to_string())
    }
}

/// Provider-specific permission mode hint (none by default — user must opt-in).
/// Returns `None` for all providers: resume never auto-carries yolo/full-auto.
///
/// 诚实口径：permission mode 目前恒未核验（metadata/配置均不携带真实模式），
/// 由 CLI 层在 preview 中如实标注 `permission_mode_verified: false`，不编造。
fn metadata_provider_permission_hint(_provider_id: &str) -> Option<String> {
    None
}

/// 首次 resume 强制预览的持久标记（PRD Q24 / audit P1-2）。
///
/// 契约：安装后第一次 `resume`（无论是否 `--yes`）只预览不执行，并落一个
/// 持久标记；标记存在后 `--yes` 才允许实际执行。标记按 db 所在 data root
/// 放置（与 writer lease 同一根），同一 data root 的多库共享"已看过预览"状态。
pub const RESUME_PREVIEW_ACK_FILE: &str = ".agent-session-grep-resume-ack";

/// 返回 data root 下首次预览标记的路径。
pub fn resume_preview_ack_path(data_root: &std::path::Path) -> std::path::PathBuf {
    data_root.join(RESUME_PREVIEW_ACK_FILE)
}

/// 首次预览是否已被确认（标记文件存在）。
pub fn resume_preview_acknowledged(data_root: &std::path::Path) -> bool {
    resume_preview_ack_path(data_root).is_file()
}

/// 落首次预览标记（幂等）。写失败返回错误——调用方必须保持强制预览（fail closed）。
pub fn acknowledge_resume_preview(data_root: &std::path::Path) -> std::io::Result<()> {
    let path = resume_preview_ack_path(data_root);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_session_grep_domain::StableId;
    use agent_session_grep_ports::SessionResumeMetadata;

    fn metadata(
        provider: &str,
        available: bool,
        session_id: &str,
        cwd: Option<&str>,
    ) -> SessionResumeMetadata {
        SessionResumeMetadata {
            session_id: StableId::from_wire("ses_v1_test").unwrap(),
            provider_id: Some(provider.to_string()),
            resume_available: available,
            provider_session_id: if available {
                Some(session_id.to_string())
            } else {
                None
            },
            original_working_directory: cwd.map(|s| s.to_string()),
            unavailable_reason: if !available {
                Some("no resume metadata claims".to_string())
            } else {
                None
            },
        }
    }

    #[test]
    fn boundary_resume_rejects_option_like_and_unchecked_native_ids() {
        let invalid = vec![
            "--dangerously-skip-permissions".to_string(),
            "--".to_string(),
            "-h".to_string(),
            "id with space".to_string(),
            " id".to_string(),
            String::from_utf8(vec![97, 0, 98]).unwrap(),
            "x".repeat(257),
        ];
        for provider in [
            "claude-code",
            "codex",
            "pi",
            "grok-build",
            "antigravity",
            "opencode",
            "kimi-code",
            "tencent-codebuddy",
        ] {
            for native_id in &invalid {
                let preview = build_resume_descriptor(&metadata(provider, true, native_id, None));
                assert!(!preview.available, "{provider}: {native_id:?}");
                assert!(preview.command_string.is_empty());
                assert!(preview.descriptor.provider_binary.is_empty());
                assert!(preview.descriptor.args.is_empty());
                assert!(preview.unavailable_reason.is_some());
            }
        }
    }

    #[test]
    fn boundary_resume_preserves_valid_native_ids_and_preview_quoting() {
        for native_id in ["abc-123", "part_1:2.3", "会话-1", "a;b"] {
            let preview = build_resume_descriptor(&metadata(
                "claude-code",
                true,
                native_id,
                Some("C:/work space"),
            ));
            assert!(preview.available);
            assert_eq!(preview.descriptor.args, ["--resume", native_id]);
            assert!(
                preview
                    .command_string
                    .starts_with("(cd \"C:/work space\" && ")
            );
            if native_id == "a;b" {
                assert!(preview.command_string.contains("\"a;b\""));
            }
            assert_eq!(preview.descriptor.permission_mode, None);
        }
    }

    #[test]
    fn preview_command_refuses_shell_injection_from_transcript_cwd() {
        // 真实缺陷（安全审计复现）：`original_working_directory` 逐字来自
        // provider transcript（不可信输入），旧实现把它裸插进 `(cd {dir} && …)`。
        // 一条被污染的会话记录即可让 dry-run 预览变成"复制即执行"的注入串，
        // 而这个串正是给人和 MCP 客户端照抄的。
        let evil = "C:\\ws\" && echo INJECTED && cd \".";
        let m = metadata("claude-code", true, "abc-123", Some(evil));
        let preview = build_resume_descriptor(&m);
        // 结构化执行面不受影响：--yes 走 argv + current_dir，不经 shell。
        assert!(preview.available);
        assert_eq!(preview.descriptor.working_directory.as_deref(), Some(evil));
        // 展示面 fail-closed：宁可没有命令，也不给一个能注入的命令。
        assert!(
            preview.command_string.is_empty(),
            "不安全的 cwd 必须拒绝出预览串，实际：{}",
            preview.command_string
        );

        // `$`/反引号/`%`/`!`/控制字符同样必须拒绝（分别对应 POSIX shell、
        // PowerShell、cmd.exe 的引号内展开，以及换行拆命令）。
        for hostile in [
            "/tmp/$(id)",
            "/tmp/`id`",
            "C:\\ws\\%USERPROFILE%",
            "C:\\ws\\!DELAYED!",
            "/tmp/a\nrm -rf /",
        ] {
            let m = metadata("claude-code", true, "abc-123", Some(hostile));
            assert!(
                build_resume_descriptor(&m).command_string.is_empty(),
                "cwd `{hostile}` 仍产出了预览串"
            );
        }

        // 会话 id 也来自 transcript：同一纪律适用于 args。
        let m = metadata(
            "claude-code",
            true,
            "abc\" && echo INJECTED && echo \"",
            None,
        );
        assert!(build_resume_descriptor(&m).command_string.is_empty());
    }

    #[test]
    fn preview_command_refuses_a_cwd_ending_in_a_backslash() {
        // 真实缺陷：以 `\` 结尾的 cwd（Windows 上非常常见的"带尾分隔符"写法）
        // 被引号包起来后是 `"C:\ws\"`——`\"` 在 POSIX shell 和 Windows argv
        // 解析里都是"字面引号"，收尾引号因此不收尾，后面的 `&& claude …`
        // 被整段吞进同一个参数，预览串照抄过去只会报语法错误。
        let m = metadata("claude-code", true, "abc-123", Some("C:\\ws\\"));
        assert!(
            build_resume_descriptor(&m).command_string.is_empty(),
            "尾随反斜杠的 cwd 必须拒绝出预览串"
        );

        // 但只有"尾随"才危险：路径中间的反斜杠是 Windows 常态，必须照常出串。
        let m = metadata("claude-code", true, "abc-123", Some("C:\\ws\\proj"));
        assert_eq!(
            build_resume_descriptor(&m).command_string,
            "(cd C:\\ws\\proj && claude --resume abc-123)"
        );
    }

    #[test]
    fn preview_command_quotes_ordinary_paths_with_spaces() {
        // 普通含空格路径不该被拒绝，只需要引号——预览仍然可以照抄执行。
        let m = metadata(
            "claude-code",
            true,
            "abc-123",
            Some("C:\\Program Files (x86)\\proj"),
        );
        let preview = build_resume_descriptor(&m);
        assert_eq!(
            preview.command_string,
            "(cd \"C:\\Program Files (x86)\\proj\" && claude --resume abc-123)"
        );
    }

    #[test]
    fn builds_claude_resume_command() {
        let m = metadata("claude-code", true, "abc-123", Some("/home/user/proj"));
        let preview = build_resume_descriptor(&m);
        assert!(preview.available);
        assert_eq!(preview.descriptor.provider_binary, "claude");
        assert_eq!(preview.descriptor.args, vec!["--resume", "abc-123"]);
        assert!(preview.command_string.contains("claude --resume abc-123"));
        assert!(preview.command_string.contains("/home/user/proj"));
    }

    #[test]
    fn builds_codex_resume_command() {
        let m = metadata("codex", true, "sess-456", None);
        let preview = build_resume_descriptor(&m);
        assert!(preview.available);
        assert_eq!(preview.descriptor.provider_binary, "codex");
        assert_eq!(preview.descriptor.args, vec!["resume", "sess-456"]);
    }

    #[test]
    fn builds_pi_resume_command() {
        let m = metadata("pi", true, "uuid-789", None);
        let preview = build_resume_descriptor(&m);
        assert!(preview.available);
        assert_eq!(preview.descriptor.provider_binary, "pi");
        assert_eq!(preview.descriptor.args, vec!["--session", "uuid-789"]);
    }

    #[test]
    fn unverified_provider_returns_unavailable() {
        // hermes：参考项目 resume 命令冲突（agf `--resume` vs hstry `--session`
        // vs cc-switch/AgentRecall 无 CLI），无权威结论——必须保持 unavailable。
        let m = metadata("hermes", true, "k-sess", None);
        let preview = build_resume_descriptor(&m);
        assert!(!preview.available);
        assert!(
            preview
                .unavailable_reason
                .as_deref()
                .unwrap()
                .contains("unverified")
        );
    }

    #[test]
    fn not_available_returns_unavailable() {
        let m = metadata("claude-code", false, "", None);
        let preview = build_resume_descriptor(&m);
        assert!(!preview.available);
        assert!(preview.unavailable_reason.is_some());
        assert!(preview.command_string.is_empty());
    }

    #[test]
    fn no_permission_mode_by_default() {
        let m = metadata("claude-code", true, "abc", None);
        let preview = build_resume_descriptor(&m);
        // Resume never auto-carries yolo/full-auto — user must opt-in.
        assert!(preview.descriptor.permission_mode.is_none());
    }

    #[test]
    fn grok_build_resume_command() {
        let m = metadata("grok-build", true, "grok-sess", None);
        let preview = build_resume_descriptor(&m);
        assert!(preview.available);
        assert_eq!(preview.descriptor.provider_binary, "grok");
        assert_eq!(preview.descriptor.args, vec!["--resume", "grok-sess"]);
    }

    #[test]
    fn builds_antigravity_resume_command() {
        let m = metadata("antigravity", true, "agy-conv-1", None);
        let preview = build_resume_descriptor(&m);
        assert!(preview.available);
        assert_eq!(preview.descriptor.provider_binary, "agy");
        assert_eq!(
            preview.descriptor.args,
            vec!["--conversation", "agy-conv-1"]
        );
    }

    #[test]
    fn builds_opencode_resume_command_with_directory() {
        let m = metadata("opencode", true, "ses_opencode_1", Some("/work/opencode"));
        let preview = build_resume_descriptor(&m);
        assert!(preview.available);
        assert_eq!(preview.descriptor.provider_binary, "opencode");
        // fast-resume 形态：directory 为 positional 参数。
        assert_eq!(
            preview.descriptor.args,
            vec!["/work/opencode", "--session", "ses_opencode_1"]
        );
        assert!(
            preview
                .command_string
                .contains("opencode /work/opencode --session ses_opencode_1")
        );
    }

    #[test]
    fn builds_opencode_resume_command_without_directory() {
        // 目录缺失时省略 positional 参数（cc-switch/agf 的 `opencode -s <id>`
        // 已验证形态，目录由 shell cwd 承担），不臆造目录。
        let m = metadata("opencode", true, "ses_opencode_2", None);
        let preview = build_resume_descriptor(&m);
        assert!(preview.available);
        assert_eq!(preview.descriptor.args, vec!["--session", "ses_opencode_2"]);
    }

    #[test]
    fn builds_kimi_code_resume_command() {
        let m = metadata("kimi-code", true, "kimi-sess-1", None);
        let preview = build_resume_descriptor(&m);
        assert!(preview.available);
        assert_eq!(preview.descriptor.provider_binary, "kimi");
        assert_eq!(preview.descriptor.args, vec!["--session", "kimi-sess-1"]);
    }

    #[test]
    fn builds_tencent_codebuddy_resume_command() {
        let m = metadata("tencent-codebuddy", true, "cb-sess-1", None);
        let preview = build_resume_descriptor(&m);
        assert!(preview.available);
        assert_eq!(preview.descriptor.provider_binary, "codebuddy");
        assert_eq!(preview.descriptor.args, vec!["--resume", "cb-sess-1"]);
    }

    #[test]
    fn resume_available_without_provider_session_id_fails_closed() {
        // audit P1-2：`resume_available=true` 但 provider_session_id 缺失——
        // 已知 provider 也会生成空 SID 命令，必须降级为不可用 + 原因。
        let m = SessionResumeMetadata {
            session_id: StableId::from_wire("ses_v1_test").unwrap(),
            provider_id: Some("claude-code".to_string()),
            resume_available: true,
            provider_session_id: None,
            original_working_directory: None,
            unavailable_reason: None,
        };
        let preview = build_resume_descriptor(&m);
        assert!(!preview.available);
        assert!(preview.command_string.is_empty());
        let reason = preview.unavailable_reason.as_deref().unwrap();
        assert!(
            reason.contains("missing the provider session id"),
            "reason: {reason}"
        );

        // 空白字符串同样视为缺失。
        let m_blank = SessionResumeMetadata {
            session_id: StableId::from_wire("ses_v1_test").unwrap(),
            provider_id: Some("codex".to_string()),
            resume_available: true,
            provider_session_id: Some("   ".to_string()),
            original_working_directory: None,
            unavailable_reason: None,
        };
        let preview = build_resume_descriptor(&m_blank);
        assert!(!preview.available);
        assert!(preview.command_string.is_empty());
    }

    #[test]
    fn capability_matrix_resume_level_matches_builder_support() {
        // audit P1-2 drift 测试：capability.rs 的 resume 级别必须与 resume
        // builder 的实际支持一致，防止矩阵与 builder 漂移。
        use agent_session_grep_ports::capability::{CapabilityLevel, ProviderCapabilityMatrix};
        let matrix = ProviderCapabilityMatrix::current();
        for capability in &matrix.providers {
            let m = SessionResumeMetadata {
                session_id: StableId::from_wire("ses_v1_drift").unwrap(),
                provider_id: Some(capability.provider_id.clone()),
                resume_available: true,
                provider_session_id: Some("synthetic-session".to_string()),
                original_working_directory: None,
                unavailable_reason: None,
            };
            let builder_supports = build_resume_descriptor(&m).available;
            assert_eq!(
                capability.resume == CapabilityLevel::Derived,
                builder_supports,
                "provider {}: capability matrix resume={:?} but resume builder support={}",
                capability.provider_id,
                capability.resume,
                builder_supports
            );
        }
    }

    #[test]
    fn first_run_preview_marker_starts_unacknowledged_and_is_idempotent() {
        let dir = unique_temp_dir("marker-ack");
        assert!(!resume_preview_acknowledged(&dir));
        acknowledge_resume_preview(&dir).expect("acknowledge");
        assert!(resume_preview_acknowledged(&dir));
        // 幂等：重复落标记不报错。
        acknowledge_resume_preview(&dir).expect("acknowledge again");
        assert!(resume_preview_acknowledged(&dir));
    }

    #[test]
    fn first_run_preview_marker_is_scoped_to_data_root() {
        let a = unique_temp_dir("marker-a");
        let b = unique_temp_dir("marker-b");
        acknowledge_resume_preview(&a).expect("acknowledge a");
        assert!(resume_preview_acknowledged(&a));
        assert!(!resume_preview_acknowledged(&b));
    }

    /// 测试专用唯一临时目录（std-only，避免新增依赖）。
    fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "asg-resume-marker-{tag}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }
}

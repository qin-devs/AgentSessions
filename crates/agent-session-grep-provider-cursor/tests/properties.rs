//! Cursor adapter（`cursor/vscdb-chat-v1`）的确定性 property 套件。
//!
//! Cursor 源是 SQLite（VS Code `state.vscdb` 的 ItemTable KV 表），两个键
//! 各持一份 JSON 文档：chatdata（tabs → bubbles）与 prompts（conversationId
//! 分组）。生成器在内存里用 rusqlite 建随机库（多 tab / 多会话 / 无 id tab /
//! 仅 rawText 的 bubble / 无 timing / 仅 prompt 无 response / 坏值），经
//! VACUUM INTO 落成字节再喂给 adapter，逐条验证：
//!
//! 1. span 恒 None（SQLite 无文件内字节坐标）；
//! 2. seq 从 0 连续，committed == emit 数 == ground truth 消息数；
//! 3. 确定性：同一字节两次解析，事件流与报告完全一致；
//! 4. 元数据透传：role/text 原样（chatdata 按 startTime、prompts 按 createdAt
//!    排序）、timestamp == startTime/createdAt 的毫秒 UTC 表示、native_id 恒空；
//! 5. 会话身份：session_native_id == 首个带 id 的会话（tab id 或
//!    conversationId），多会话 fail-closed 为 Ambiguous；
//! 6. probe 对任意字节永不 panic：Ok 时 confidence 非 Ambiguous 且 variant 恒为自身；
//! 7. golden fixture 的 seeded 确定性变异（截断 / 插入 / 删除 / 翻转字节）下
//!    parse 永不 panic：Ok 则消息字段合法（committed==emit、正文非空、span 恒
//!    None），Err 则 recoverable 由上层回滚；变异源字节前后不变
//!    （RFC-0002 §7，经 testkit `assert_read_only`）。
//!
//! 零新依赖（repo 惯例）：本地 xorshift64* PRNG + 固定种子表；rusqlite 是 crate
//! 自身依赖。断言信息一律携带 seed，失败可用该 seed 单独重放；不落盘（临时库
//! 用后即删）、不联网。

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};

use agent_session_grep_ports::MetadataResolution;
use agent_session_grep_ports::{
    CanonicalEventSink, Confidence, MessageEvent, ParseReport, ProviderAdapter,
};
use agent_session_grep_provider_cursor::CursorAdapter;
use agent_session_grep_testkit::assert_read_only;
use rusqlite::{Connection, params};

const VARIANT_ID: &str = "cursor/vscdb-chat-v1";
const FIXTURE_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/basic.db");
const CHAT_DATA_KEY: &str = "workbench.panel.aichat.view.aichat.chatdata";
const PROMPTS_KEY: &str = "aiService.prompts";

/// 收集 emit 的消息事件；派生 PartialEq 以支撑"两次解析逐字段一致"断言。
#[derive(Default)]
struct CollectingSink {
    messages: Vec<Captured>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Captured {
    seq: u32,
    native_id: String,
    parent_native_id: Option<String>,
    role: String,
    text: String,
    timestamp: Option<String>,
    is_sidechain: bool,
    span: Option<(u64, u64)>,
}

impl CanonicalEventSink for CollectingSink {
    fn emit_message(
        &mut self,
        event: MessageEvent<'_>,
    ) -> agent_session_grep_ports::PortResult<()> {
        self.messages.push(Captured {
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
}

/// xorshift64*（Marsaglia 2003）：十几行的确定性 PRNG，避免引入随机数依赖。
struct XorShift64Star(u64);

impl XorShift64Star {
    fn new(seed: u64) -> Self {
        // 0 是 xorshift 的不动点，换成任意固定非零常量。
        XorShift64Star(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// 均匀取 `[0, n)`；n 必须 > 0。取模偏差对测试生成器无关紧要。
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// 以 num/den 概率返回 true。
    fn chance(&mut self, num: u64, den: u64) -> bool {
        self.next_u64() % den < num
    }

    fn pick<'a>(&mut self, pool: &'a [&'a str]) -> &'a str {
        pool[self.below(pool.len())]
    }
}

/// 64 个固定种子：常量步进展开（黄金比例增量），跨平台跨运行完全一致。
fn fixed_seeds() -> [u64; 64] {
    let mut seeds = [0u64; 64];
    let mut acc: u64 = 0x0198_C0DE_5EED_0001;
    for slot in &mut seeds {
        *slot = acc;
        acc = acc.wrapping_add(0x9E37_79B9_7F4A_7C15);
    }
    seeds
}

/// Unicode 文本池：CJK、emoji、RTL（阿拉伯/希伯来）、全角、星面字符与转义敏感片段。
const TEXT_POOL: [&str; 12] = [
    "你好，世界",
    "cursor 库自检",
    "emoji 🚀😀✅",
    "مرحبا بالعالم",
    "שלום עולם",
    "café Ünïcode",
    "line\nbreak inside",
    "tab\tand \"quotes\" and \\backslash",
    "混合 mixed ASCII 与 CJK",
    "ｆｕｌｌｗｉｄｔｈ　ＡＢＣ",
    "𝔞𝔰𝔱𝔯𝔞𝔩 𝖕𝖑𝖆𝖓𝖊 chars",
    "尾随空格  ",
];

/// 一条预期产出的消息。
struct ExpectedMessage {
    role: String,
    text: String,
    timestamp: Option<String>,
}

/// 一个生成的 Cursor vscdb 用例：序列化字节 + 全部预期值 + 语料覆盖度标志。
struct Case {
    bytes: Vec<u8>,
    expected: Vec<ExpectedMessage>,
    session_id: Option<String>,
    session_count: usize,
    timestamp_diagnostics: usize,
    has_chatdata: bool,
    has_prompts: bool,
    saw_multi_tab: bool,
    saw_idless_tab: bool,
    saw_raw_text: bool,
    saw_untimed: bool,
    saw_prompts: bool,
    saw_prompt_only: bool,
}

fn gen_text(rng: &mut XorShift64Star) -> String {
    let n = 1 + rng.below(3);
    (0..n)
        .map(|_| rng.pick(&TEXT_POOL))
        .collect::<Vec<_>>()
        .join(" ")
}

/// ~256 KiB 大字段：多字节图样重复，压大行路径与 UTF-8 处理。
fn big_text() -> String {
    let unit = "大字段填充🚀0123456789abcdef ";
    let mut s = String::with_capacity(262_144 + unit.len());
    while s.len() < 262_144 {
        s.push_str(unit);
    }
    s
}

/// 进程内唯一临时路径：pid + 原子计数器，避免并行测试互相截断。
fn temp_path(tag: &str) -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "asg-prop-cursor-{}-{}-{tag}.db",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

/// 把内存库序列化成字节（与 crate 单测同款 VACUUM INTO 路径）。
fn vacuum_to_bytes(conn: &Connection) -> Vec<u8> {
    let path = temp_path("build");
    conn.execute_batch(&format!("VACUUM INTO '{}'", path.display()))
        .expect("VACUUM INTO 必须成功");
    let bytes = std::fs::read(&path).expect("read vacuumed db");
    let _ = std::fs::remove_file(&path);
    bytes
}

/// 一个 bubble 的计划语义 + 其排序/过滤后的产出（若有）。
struct PlannedBubble {
    r#type: Option<String>,
    text: Option<String>,
    raw_text: Option<String>,
    start: Option<i64>,
    emitted: Option<(String, String, Option<String>)>, // (role, text, timestamp)
}

/// 一个 tab 的计划语义。
struct PlannedTab {
    id: Option<String>,
    created_at: Option<i64>,
    bubbles: Vec<PlannedBubble>,
}

/// 一条 prompt 记录的计划语义。
struct PlannedPrompt {
    prompt: Option<String>,
    response: Option<String>,
    created_at: Option<i64>,
    conversation_id: Option<String>,
}

fn build_case(seed: u64, with_big_field: bool) -> Case {
    let mut rng = XorShift64Star::new(seed);
    let conn = Connection::open_in_memory().expect("open in-memory db");
    conn.execute_batch("CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT);")
        .expect("create ItemTable");

    // Independent timestamp oracle: SQLite's calendar conversion, with whole
    // seconds and the Euclidean millisecond remainder passed separately so no
    // floating-point rounding can alter a half-open boundary.
    let mut timestamp_diagnostics = 0;
    let mut expected_timestamp = |millis: i64| {
        let formatted: Option<String> = conn
            .query_row(
                "SELECT strftime('%Y-%m-%dT%H:%M:%S', ?1, 'unixepoch') \
                 || printf('.%03dZ', ?2)",
                params![millis.div_euclid(1_000), millis.rem_euclid(1_000)],
                |row| row.get(0),
            )
            .expect("reference timestamp conversion");
        // SQLite also accepts negative years; the formatter emits four
        // unsigned year digits (24 bytes including the millisecond part).
        let formatted = formatted.filter(|value| value.len() == 24);
        timestamp_diagnostics += usize::from(formatted.is_none());
        formatted
    };

    let mut saw_multi_tab = false;
    let mut saw_idless_tab = false;
    let mut saw_raw_text = false;
    let mut saw_untimed = false;
    let mut saw_prompts = false;
    let mut saw_prompt_only = false;

    // ---- chatdata：tabs → bubbles，与生产 parse_chat_data 同源判定 ----
    let n_tabs = rng.below(4); // 0..=3：约 1/4 种子没有 chatdata 输出
    let mut tabs: Vec<PlannedTab> = Vec::with_capacity(n_tabs);
    for t in 0..n_tabs {
        if n_tabs > 1 {
            saw_multi_tab = true;
        }
        let id = if rng.chance(3, 4) {
            Some(format!("tab-{seed:016x}-{t}"))
        } else {
            saw_idless_tab = true;
            None
        };
        let created_at = rng.chance(3, 5).then(|| rng.next_u64() as i64);
        let n_bubbles = rng.below(5);
        let mut bubbles: Vec<PlannedBubble> = Vec::with_capacity(n_bubbles);
        for b in 0..n_bubbles {
            let r#type = match rng.below(20) {
                0..=7 => Some("user".to_string()),
                8..=15 => Some("assistant".to_string()),
                16 => Some(String::new()), // 空 type：过滤
                _ => None,                 // 缺 type：过滤
            };
            let (text, raw_text) = if with_big_field && t == 0 && b == 0 {
                (Some(big_text()), None)
            } else if rng.chance(4, 5) {
                (Some(gen_text(&mut rng)), None)
            } else {
                // 只有 rawText：adapter 回退取它。
                saw_raw_text = true;
                (None, Some(gen_text(&mut rng)))
            };
            let start = if rng.chance(3, 4) {
                Some(rng.next_u64() as i64)
            } else {
                saw_untimed = true;
                None
            };
            let effective_text = text.clone().or_else(|| raw_text.clone());
            let emitted = if let Some(r#type) = r#type.as_deref()
                && !r#type.is_empty()
                && let Some(text) = effective_text.as_deref()
                && !text.trim().is_empty()
            {
                let role = if r#type == "user" {
                    "user"
                } else {
                    "assistant"
                };
                Some((
                    role.to_string(),
                    text.to_string(),
                    start.and_then(&mut expected_timestamp),
                ))
            } else {
                None
            };
            bubbles.push(PlannedBubble {
                r#type,
                text,
                raw_text,
                start,
                emitted,
            });
        }
        tabs.push(PlannedTab {
            id,
            created_at,
            bubbles,
        });
    }

    // ---- prompts：conversationId 分组，与生产 parse_prompts 同源判定 ----
    let n_prompts = rng.below(5); // 0..=4：约 1/5 种子没有 prompts 输出
    let mut prompts: Vec<PlannedPrompt> = Vec::with_capacity(n_prompts);
    for p in 0..n_prompts {
        saw_prompts = true;
        let prompt = match rng.below(10) {
            0..=6 => Some(gen_text(&mut rng)),
            7 => Some("   ".to_string()), // 空白：过滤
            _ => {
                saw_prompt_only = true;
                None
            }
        };
        let response = if rng.chance(3, 4) {
            Some(gen_text(&mut rng))
        } else {
            None
        };
        let created_at = rng.chance(3, 4).then(|| rng.next_u64() as i64);
        let conversation_id = match rng.below(10) {
            0..=4 => Some(format!("conv-{seed:016x}-{p}")),
            5..=7 => Some(String::new()), // 空串 == 缺席
            _ => None,
        };
        prompts.push(PlannedPrompt {
            prompt,
            response,
            created_at,
            conversation_id,
        });
    }

    // 至少一个键必须存在：probe 的前提（ItemTable 没有任何 Cursor 键会被判拒绝）。
    if n_tabs == 0 && n_prompts == 0 {
        tabs.push(PlannedTab {
            id: Some(format!("forced-tab-{seed:016x}")),
            created_at: Some(1),
            bubbles: vec![PlannedBubble {
                r#type: Some("user".to_string()),
                text: Some("forced valid record".to_string()),
                raw_text: None,
                start: Some(1),
                emitted: Some((
                    "user".to_string(),
                    "forced valid record".to_string(),
                    Some("1970-01-01T00:00:00.001Z".to_string()),
                )),
            }],
        });
    }

    // ---- ground truth：tab 排序 → bubble 排序 → prompt 分组排序 ----
    let mut expected: Vec<ExpectedMessage> = Vec::new();
    let mut session_count = 0usize;
    let mut session_id: Option<String> = None;

    // 会话注册语义（与 register_session 同源）：计数每个会话，首个非空 id 胜出。
    let mut register = |sid: Option<&str>| {
        session_count += 1;
        let Some(id) = sid.map(str::trim).filter(|s| !s.is_empty()) else {
            return;
        };
        if session_id.is_none() {
            session_id = Some(id.to_string());
        }
    };

    let mut sorted_tabs: Vec<usize> = (0..tabs.len()).collect();
    sorted_tabs.sort_by_key(|&i| tabs[i].created_at.unwrap_or(i64::MAX));
    for &ti in &sorted_tabs {
        let tab = &tabs[ti];
        let mut kept: Vec<usize> = (0..tab.bubbles.len())
            .filter(|&bi| tab.bubbles[bi].emitted.is_some())
            .collect();
        if kept.is_empty() {
            continue;
        }
        register(tab.id.as_deref());
        kept.sort_by_key(|&bi| tab.bubbles[bi].start.unwrap_or(i64::MAX));
        for &bi in &kept {
            let (role, text, timestamp) = tab.bubbles[bi].emitted.clone().expect("kept");
            expected.push(ExpectedMessage {
                role,
                text,
                timestamp,
            });
        }
    }

    // 分组：BTreeMap 按 conversation_id 字典序，稳定排序按组内最早 createdAt。
    let mut grouped: std::collections::BTreeMap<String, Vec<usize>> =
        std::collections::BTreeMap::new();
    for (i, prompt) in prompts.iter().enumerate() {
        grouped
            .entry(prompt.conversation_id.clone().unwrap_or_default())
            .or_default()
            .push(i);
    }
    let mut groups: Vec<(String, Vec<usize>)> = grouped.into_iter().collect();
    groups.sort_by_key(|(_, idxs)| {
        idxs.iter()
            .filter_map(|&i| prompts[i].created_at)
            .min()
            .unwrap_or(i64::MAX)
    });
    for (conversation_id, idxs) in groups {
        let mut sorted: Vec<usize> = idxs;
        sorted.sort_by_key(|&i| prompts[i].created_at.unwrap_or(i64::MAX));
        let mut group_msgs: Vec<(String, String, Option<String>)> = Vec::new();
        for &i in &sorted {
            let p = &prompts[i];
            if let Some(text) = p.prompt.as_deref().filter(|t| !t.trim().is_empty()) {
                group_msgs.push((
                    "user".to_string(),
                    text.to_string(),
                    p.created_at.and_then(&mut expected_timestamp),
                ));
            }
            if let Some(text) = p.response.as_deref().filter(|t| !t.trim().is_empty()) {
                group_msgs.push((
                    "assistant".to_string(),
                    text.to_string(),
                    p.created_at.and_then(&mut expected_timestamp),
                ));
            }
        }
        if group_msgs.is_empty() {
            continue;
        }
        let id = (!conversation_id.trim().is_empty()).then_some(conversation_id.as_str());
        register(id);
        for (role, text, timestamp) in group_msgs {
            expected.push(ExpectedMessage {
                role,
                text,
                timestamp,
            });
        }
    }

    // 属性前提是"存在有效消息"；极端种子下若一条都产不出则强制补一条
    // （会话计数与 id 同步推进，保证与 adapter 在最终库上的行为一致）。
    if expected.is_empty() {
        let forced_id = format!("forced-tab-{seed:016x}");
        tabs.push(PlannedTab {
            id: Some(forced_id.clone()),
            created_at: Some(1),
            bubbles: vec![PlannedBubble {
                r#type: Some("user".to_string()),
                text: Some("forced valid record".to_string()),
                raw_text: None,
                start: Some(1),
                emitted: Some((
                    "user".to_string(),
                    "forced valid record".to_string(),
                    Some("1970-01-01T00:00:00.001Z".to_string()),
                )),
            }],
        });
        session_count += 1;
        if session_id.is_none() {
            session_id = Some(forced_id);
        }
        expected.push(ExpectedMessage {
            role: "user".to_string(),
            text: "forced valid record".to_string(),
            timestamp: Some("1970-01-01T00:00:00.001Z".to_string()),
        });
    }

    // ---- 落库：chatdata 与 prompts 两个键（各自按概率缺席）----
    let chatdata_json = {
        let tabs_json: Vec<serde_json::Value> = tabs
            .iter()
            .map(|tab| {
                let mut obj = serde_json::Map::new();
                if let Some(id) = &tab.id {
                    obj.insert("id".into(), id.as_str().into());
                }
                if let Some(ts) = tab.created_at {
                    obj.insert("createdAt".into(), ts.into());
                }
                let bubbles_json: Vec<serde_json::Value> = tab
                    .bubbles
                    .iter()
                    .map(|b| {
                        let mut bo = serde_json::Map::new();
                        if let Some(t) = &b.r#type {
                            bo.insert("type".into(), t.as_str().into());
                        }
                        if let Some(t) = &b.text {
                            bo.insert("text".into(), t.as_str().into());
                        }
                        if let Some(t) = &b.raw_text {
                            bo.insert("rawText".into(), t.as_str().into());
                        }
                        if let Some(s) = b.start {
                            bo.insert("timingInfo".into(), serde_json::json!({"startTime": s}));
                        }
                        serde_json::Value::Object(bo)
                    })
                    .collect();
                obj.insert("bubbles".into(), bubbles_json.into());
                serde_json::Value::Object(obj)
            })
            .collect();
        // ChatData 是 `{ "tabs": [...] }` 对象（不是裸数组）。
        serde_json::json!({"tabs": tabs_json}).to_string()
    };
    if !tabs.is_empty() {
        conn.execute(
            "INSERT INTO ItemTable (key, value) VALUES (?1, ?2)",
            params![CHAT_DATA_KEY, chatdata_json],
        )
        .expect("insert chatdata");
    }

    let prompts_json = {
        let arr: Vec<serde_json::Value> = prompts
            .iter()
            .map(|p| {
                let mut obj = serde_json::Map::new();
                if let Some(t) = &p.prompt {
                    obj.insert("prompt".into(), t.as_str().into());
                }
                if let Some(t) = &p.response {
                    obj.insert("response".into(), t.as_str().into());
                }
                if let Some(ts) = p.created_at {
                    obj.insert("createdAt".into(), ts.into());
                }
                if let Some(c) = &p.conversation_id {
                    obj.insert("conversationId".into(), c.as_str().into());
                }
                serde_json::Value::Object(obj)
            })
            .collect();
        serde_json::Value::Array(arr).to_string()
    };
    if !prompts.is_empty() {
        conn.execute(
            "INSERT INTO ItemTable (key, value) VALUES (?1, ?2)",
            params![PROMPTS_KEY, prompts_json],
        )
        .expect("insert prompts");
    }

    let bytes = vacuum_to_bytes(&conn);

    Case {
        bytes,
        expected,
        session_id,
        session_count,
        timestamp_diagnostics,
        has_chatdata: !tabs.is_empty(),
        has_prompts: !prompts.is_empty(),
        saw_multi_tab,
        saw_idless_tab,
        saw_raw_text,
        saw_untimed,
        saw_prompts,
        saw_prompt_only,
    }
}

fn parse_case(seed: u64, bytes: &[u8]) -> (ParseReport, Vec<Captured>) {
    let mut sink = CollectingSink::default();
    let report = CursorAdapter::new()
        .parse(bytes, &mut sink)
        .unwrap_or_else(|e| panic!("seed={seed}: 合法生成的 cursor 库 parse 失败：{e}"));
    (report, sink.messages)
}

/// 逐固定种子运行 body；index 0 的迭代携带 ~256 KiB 大字段。
fn for_each_seed(mut body: impl FnMut(u64, &Case)) {
    for (idx, &seed) in fixed_seeds().iter().enumerate() {
        let case = build_case(seed, idx == 0);
        body(seed, &case);
    }
}

/// 性质 1：SQLite 没有行内字节坐标——所有消息 span 恒 None。
#[test]
fn prop_spans_are_always_none() {
    for_each_seed(|seed, case| {
        let (_, captured) = parse_case(seed, &case.bytes);
        for got in &captured {
            assert_eq!(
                got.span, None,
                "seed={seed} seq={}: cursor 为 SQLite 源，不得归因行内 span",
                got.seq
            );
        }
    });
}

/// 性质 2：seq 从 0 连续，且 committed == emit 数 == ground truth 消息数。
#[test]
fn prop_seq_contiguous_and_counts_committed() {
    for_each_seed(|seed, case| {
        let (report, captured) = parse_case(seed, &case.bytes);
        for (i, got) in captured.iter().enumerate() {
            assert_eq!(got.seq, i as u32, "seed={seed}: seq 必须从 0 连续递增");
        }
        assert_eq!(
            report.committed,
            captured.len(),
            "seed={seed}: committed 必须等于实际 emit 的消息数"
        );
        assert_eq!(
            report.committed,
            case.expected.len(),
            "seed={seed}: committed 必须等于 ground truth 有效消息数"
        );
    });
}

/// 性质 3：同一字节输入解析两次，事件流与报告完全一致（确定性）。
#[test]
fn prop_parse_is_deterministic() {
    for_each_seed(|seed, case| {
        let (report_a, captured_a) = parse_case(seed, &case.bytes);
        let (report_b, captured_b) = parse_case(seed, &case.bytes);
        assert_eq!(report_a, report_b, "seed={seed}: 两次解析的报告必须一致");
        assert_eq!(
            captured_a, captured_b,
            "seed={seed}: 两次解析收集的事件必须一致"
        );
    });
}

/// 性质 4：role/text 原样（chatdata 按 startTime、prompts 按 createdAt 排序）、
/// timestamp == startTime/createdAt 毫秒 UTC 表示、native_id 恒空（tab id 与
/// prompt id 是会话级身份，不是消息级 id）、无 parent、无 sidechain。
#[test]
fn prop_metadata_survives_verbatim() {
    for_each_seed(|seed, case| {
        let (_, captured) = parse_case(seed, &case.bytes);
        assert_eq!(
            captured.len(),
            case.expected.len(),
            "seed={seed}: 消息数必须等于 ground truth"
        );
        for (got, want) in captured.iter().zip(&case.expected) {
            let seq = got.seq;
            assert_eq!(
                got.role, want.role,
                "seed={seed} seq={seq}: role 必须原样透传"
            );
            assert_eq!(
                got.text, want.text,
                "seed={seed} seq={seq}: 文本必须原样抽取（text 优先、rawText 回退）"
            );
            assert_eq!(
                got.timestamp, want.timestamp,
                "seed={seed} seq={seq}: timestamp 必须等于 startTime/createdAt 毫秒 UTC 表示"
            );
            assert_eq!(
                got.native_id, "",
                "seed={seed} seq={seq}: cursor 无消息级原生 id，native_id 恒空"
            );
            assert_eq!(
                got.parent_native_id, None,
                "seed={seed} seq={seq}: cursor 无 threading 边"
            );
            assert!(
                !got.is_sidechain,
                "seed={seed} seq={seq}: cursor 无 sidechain"
            );
        }
    });
}

/// 性质 5：会话身份——session_count 与首个非空 id 与 ground truth 一致；
/// 多会话 fail-closed 为 Ambiguous；只有不可表示的时间戳产生字段丢失诊断。
#[test]
fn prop_session_identity_matches_ground_truth() {
    for_each_seed(|seed, case| {
        let (report, _) = parse_case(seed, &case.bytes);
        assert_eq!(
            report.session_native_id, case.session_id,
            "seed={seed}: session_native_id 必须等于首个带 id 的会话"
        );
        let multi = case.session_count > 1;
        assert_eq!(
            report.session_observation.multi_session, multi,
            "seed={seed}: multi_session 必须与 ground truth 一致"
        );
        assert_eq!(
            report.diagnostics.len(),
            case.timestamp_diagnostics,
            "seed={seed}: only out-of-range timestamps need diagnostics"
        );
        assert!(report.diagnostics.iter().all(|note| {
            note == "cursor ItemTable: out-of-range epoch-millisecond timestamp omitted"
        }));
        match (case.session_id.as_deref(), multi) {
            (Some(id), false) => assert_eq!(
                report.session_observation.provider_session_id,
                MetadataResolution::Resolved(id.to_string()),
                "seed={seed}: 单会话 provider_session_id 必须 Resolved"
            ),
            (_, true) => assert_eq!(
                report.session_observation.provider_session_id,
                MetadataResolution::Ambiguous,
                "seed={seed}: 多会话 provider_session_id 必须 fail-closed 为 Ambiguous"
            ),
            (None, false) => assert_eq!(
                report.session_observation.provider_session_id,
                MetadataResolution::Missing,
                "seed={seed}: 无 id 时 provider_session_id 必须 Missing"
            ),
        }
        assert_eq!(
            report.skipped, 0,
            "seed={seed}: 合成库读取不得失败（skipped 恒 0）"
        );
    });
}

/// 语料覆盖度：64 个固定种子必须实际exercise过多 tab、无 id tab、仅 rawText、
/// 无 timing、prompts、仅 prompt 无 response，以及两个键各自"存在/缺席"——
/// 防止生成器退化成空转语料。
#[test]
fn prop_corpus_coverage_is_not_degenerate() {
    let mut saw_multi_tab = false;
    let mut saw_idless = false;
    let mut saw_raw = false;
    let mut saw_untimed = false;
    let mut saw_prompts = false;
    let mut saw_prompt_only = false;
    let mut saw_chat = false;
    let mut saw_no_chat = false;
    let mut saw_prompts_key = false;
    let mut saw_no_prompts_key = false;
    for (i, seed) in fixed_seeds().iter().copied().enumerate() {
        let case = build_case(seed, i == 0);
        saw_multi_tab |= case.saw_multi_tab;
        saw_idless |= case.saw_idless_tab;
        saw_raw |= case.saw_raw_text;
        saw_untimed |= case.saw_untimed;
        saw_prompts |= case.saw_prompts;
        saw_prompt_only |= case.saw_prompt_only;
        saw_chat |= case.has_chatdata;
        saw_no_chat |= !case.has_chatdata;
        saw_prompts_key |= case.has_prompts;
        saw_no_prompts_key |= !case.has_prompts;
        assert!(
            !case.expected.is_empty(),
            "seed={seed}: 每个用例必须至少含一条有效消息"
        );
    }
    assert!(
        saw_multi_tab
            && saw_idless
            && saw_raw
            && saw_untimed
            && saw_prompts
            && saw_prompt_only
            && saw_chat
            && saw_no_chat
            && saw_prompts_key
            && saw_no_prompts_key,
        "生成器语料退化：multi_tab={saw_multi_tab} idless={saw_idless} raw={saw_raw} \
         untimed={saw_untimed} prompts={saw_prompts} prompt_only={saw_prompt_only} \
         chat={saw_chat} no_chat={saw_no_chat} prompts_key={saw_prompts_key} \
         no_prompts_key={saw_no_prompts_key}"
    );
}

/// 只读契约：合成库的 probe 与 parse 前后字节长度与 BLAKE3 指纹不变
/// （RFC-0002 §7 的可执行守护）。
#[test]
fn prop_probe_and_parse_are_read_only() {
    for_each_seed(|seed, case| {
        let report = assert_read_only(&case.bytes, |src| {
            let mut sink = CollectingSink::default();
            CursorAdapter::new().parse(src, &mut sink)
        })
        .unwrap_or_else(|e| panic!("seed={seed}: 合成库 parse 失败：{e}"));
        assert!(report.committed > 0, "seed={seed}: 合成库必须产出消息");
        let _ = assert_read_only(&case.bytes, |src| CursorAdapter::new().probe(src));
    });
}

/// probe 对任意字节永不 panic：纯随机 / 随机 ASCII / 假 SQLite 头 / golden
/// 随机截断。Ok 时 confidence 非 Ambiguous 且 variant 恒为自身。
#[test]
fn prop_probe_never_panics_on_arbitrary_bytes() {
    let adapter = CursorAdapter::new();
    let fixture = std::fs::read(FIXTURE_PATH).expect("read golden fixture");

    for seed in fixed_seeds() {
        let mut rng = XorShift64Star::new(seed.wrapping_mul(0x1234_5678_9ABC_DEF1).max(1));
        for k in 0..8 {
            let bytes = match k % 4 {
                0 => {
                    let n = rng.below(2048);
                    (0..n).map(|_| rng.next_u64() as u8).collect::<Vec<_>>()
                }
                1 => {
                    let n = 1 + rng.below(16);
                    let mut s = String::new();
                    for _ in 0..n {
                        let len = rng.below(64);
                        for _ in 0..len {
                            s.push((0x20 + rng.below(0x5F)) as u8 as char);
                        }
                        s.push('\n');
                    }
                    s.into_bytes()
                }
                2 => {
                    // 假 SQLite 头 + 随机尾：magic 通过但打开必败。
                    let mut b = b"SQLite format 3\0".to_vec();
                    let n = rng.below(1024);
                    b.extend((0..n).map(|_| rng.next_u64() as u8));
                    b
                }
                _ => {
                    let mut b = fixture.clone();
                    b.truncate(rng.below(b.len()));
                    b
                }
            };
            let probed = catch_unwind(AssertUnwindSafe(|| adapter.probe(&bytes)));
            match probed {
                Ok(Ok(r)) => {
                    assert_ne!(
                        r.confidence,
                        Confidence::Ambiguous,
                        "seed={seed} k={k}: probe Ok 时 confidence 不得为 Ambiguous"
                    );
                    assert_eq!(
                        r.variant_id, VARIANT_ID,
                        "seed={seed} k={k}: probe 必须报告自身 variant"
                    );
                }
                Ok(Err(_)) => {}
                Err(payload) => panic!(
                    "seed={seed} k={k}: probe 在 {} 字节任意输入上 panic: {payload:?}",
                    bytes.len()
                ),
            }
        }
    }
}

/// 合成库上的 probe：不 panic、两次调用结果一致、恒 Ok(Confirmed)（至少一个键
/// 存在）且 variant 为自身。
#[test]
fn prop_probe_on_generated_dbs_confirms_own_variant() {
    for_each_seed(|seed, case| {
        let adapter = CursorAdapter::new();
        let a = adapter.probe(&case.bytes);
        let b = adapter.probe(&case.bytes);
        assert_eq!(a, b, "seed={seed}: probe 两次调用必须确定一致");
        let r = a.unwrap_or_else(|e| panic!("seed={seed}: 合成库 probe 必须成功：{e}"));
        assert_eq!(
            r.confidence,
            Confidence::Confirmed,
            "seed={seed}: 有键即 Confirmed"
        );
        assert_eq!(
            r.variant_id, VARIANT_ID,
            "seed={seed}: probe 必须报告自身 variant"
        );
    });
}

/// golden fixture 的 seeded 确定性变异（SQLite 是二进制源：截断 / 插入 / 删除 /
/// 翻转字节，无行级操作）。
fn mutate_bytes(rng: &mut XorShift64Star, base: &[u8]) -> Vec<u8> {
    let mut out = base.to_vec();
    match rng.below(5) {
        0 => {
            // 随机位置截断。
            let pos = rng.below(out.len());
            out.truncate(pos);
        }
        1 => {
            // 随机位置插入 1..=24 个随机字节。
            let pos = rng.below(out.len());
            let n = 1 + rng.below(24);
            let filler: Vec<u8> = (0..n).map(|_| rng.next_u64() as u8).collect();
            out.splice(pos..pos, filler);
        }
        2 => {
            // 随机位置删除 1..=24 个字节。
            let pos = rng.below(out.len());
            let n = (1 + rng.below(24)).min(out.len() - pos);
            out.drain(pos..pos + n);
        }
        3 => {
            // 翻转一个字节。
            let pos = rng.below(out.len());
            out[pos] = rng.next_u64() as u8;
        }
        _ => {
            // 翻转一个 1..=24 字节的连续区段。
            let pos = rng.below(out.len());
            let n = (1 + rng.below(24)).min(out.len() - pos);
            for b in &mut out[pos..pos + n] {
                *b ^= 0xFF;
            }
        }
    }
    out
}

/// 变异语料上的 parse：永不 panic；Err 即 recoverable 拒绝（上层回滚 staging）；
/// Ok 则绝不 partial commit——committed == emit 数、seq 连续、span 恒 None、
/// 正文非空、角色合法。同时 probe 变异字节不 panic，且源字节在 parse 前后不变
/// （RFC-0002 §7）。
#[test]
fn prop_golden_mutations_never_panic_and_output_stays_legal() {
    let fixture = std::fs::read(FIXTURE_PATH).expect("read golden fixture");
    let adapter = CursorAdapter::new();

    for seed in fixed_seeds() {
        let mut rng = XorShift64Star::new(seed.wrapping_add(0xA5A5_5A5A_DEAD_BEEF).max(1));
        for op in 0..12 {
            let mutated = mutate_bytes(&mut rng, &fixture);

            let (parsed, sink) = assert_read_only(&mutated, |src| {
                let mut sink = CollectingSink::default();
                let parsed = catch_unwind(AssertUnwindSafe(|| adapter.parse(src, &mut sink)));
                (parsed, sink)
            });
            let report = match parsed {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => continue, // recoverable 错误路径：允许并继续
                Err(payload) => {
                    panic!("seed={seed} op={op}: parse 在变异输入上 panic: {payload:?}")
                }
            };

            assert_eq!(
                report.committed,
                sink.messages.len(),
                "seed={seed} op={op}: committed 必须等于实际 emit 数（绝不 partial commit）"
            );
            for (i, m) in sink.messages.iter().enumerate() {
                assert_eq!(m.seq, i as u32, "seed={seed} op={op}: seq 必须从 0 连续");
                assert!(
                    matches!(m.role.as_str(), "user" | "assistant"),
                    "seed={seed} op={op}: 角色必须是 user/assistant，实际 {}",
                    m.role
                );
                assert!(
                    !m.text.trim().is_empty(),
                    "seed={seed} op={op}: seq={} 正文为空，committed 计入它等于宣称索引了检索不到的内容",
                    m.seq
                );
                assert_eq!(
                    m.span, None,
                    "seed={seed} op={op}: seq={} SQLite 不得归因行内 span",
                    m.seq
                );
            }

            // probe 同样必须对变异字节不 panic。
            let probed = catch_unwind(AssertUnwindSafe(|| adapter.probe(&mutated)));
            match probed {
                Ok(Ok(r)) => {
                    assert_ne!(
                        r.confidence,
                        Confidence::Ambiguous,
                        "seed={seed} op={op}: probe Ok 时 confidence 不得为 Ambiguous"
                    );
                    assert_eq!(
                        r.variant_id, VARIANT_ID,
                        "seed={seed} op={op}: probe 必须报告自身 variant"
                    );
                }
                Ok(Err(_)) => {}
                Err(payload) => {
                    panic!("seed={seed} op={op}: probe 在变异输入上 panic: {payload:?}")
                }
            }
        }
    }
}

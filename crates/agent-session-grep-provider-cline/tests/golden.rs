//! Golden 契约测试：固定 fixture 字节 → pinned canonical 输出。
//!
//! 任何 parser 行为漂移（角色映射、文本抽取、计数口径）或 fixture 字节漂移
//! （git 行尾转换、误编辑）都必须在此响亮失败，作为 Beta 认证的可复核证据。
//! fixture 为纯合成数据，来源与覆盖点见 `tests/golden/PROVENANCE.md`。
//!
//! Cline 是单文件 JSON 数组（`api_conversation_history.json`）。适配器不产出
//! 字节 span（`span: None`）——span round-trip 标记 N/A；capability.rs 的
//! `source_span` 诚实声明为 `unsupported`。
//!
//! 全字段捕获 sink、fixture 读取与 BLAKE3 校验、canonical JSON 投影复用
//! `agent_session_grep_testkit::golden`，本文件只保留 cline 特有的断言。

use agent_session_grep_ports::{Confidence, ParseReport, ProviderAdapter};
use agent_session_grep_provider_cline::ClineAdapter;
use agent_session_grep_testkit::assert_read_only;
use agent_session_grep_testkit::golden::{self, CapturingSink};

const FIXTURE_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/basic.json");
const EXPECTED_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/basic.expected.json"
);

const TIMESTAMP_FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/epoch-millis-bom.json"
);

const TIMESTAMP_EXPECTED_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/epoch-millis-bom.expected.json"
);

/// 解析 fixture：经共享 sink 全字段捕获，返回报告与 sink。
fn parse_fixture(bytes: &[u8]) -> (ParseReport, CapturingSink) {
    golden::parse_golden(&ClineAdapter::new(), bytes)
}

#[test]
fn probe_never_mutates_source_bytes() {
    let expected = golden::read_expected(EXPECTED_PATH);
    let bytes = golden::read_fixture_verified(FIXTURE_PATH, &expected);
    assert_read_only(&bytes, |source| ClineAdapter::new().probe(source))
        .expect("golden fixture probe must succeed");
}

#[test]
fn parse_never_mutates_source_bytes() {
    let expected = golden::read_expected(EXPECTED_PATH);
    let bytes = golden::read_fixture_verified(FIXTURE_PATH, &expected);
    let mut sink = CapturingSink::default();
    let report = assert_read_only(&bytes, |source| {
        ClineAdapter::new().parse(source, &mut sink)
    })
    .expect("golden fixture parse must succeed");
    assert!(report.committed > 0, "fixture must exercise message output");
    assert!(!sink.messages.is_empty(), "fixture must emit messages");
}

#[test]
fn golden_provenance_revision_matches_manifest() {
    assert_eq!(ClineAdapter::new().manifest().fixture_revision, Some(2));
}

#[test]
fn golden_canonical_output_is_pinned() {
    let expected = golden::read_expected(EXPECTED_PATH);
    let bytes = golden::read_fixture_verified(FIXTURE_PATH, &expected);
    let (report, sink) = parse_fixture(&bytes);
    let hash = blake3::hash(&bytes).to_hex().to_string();
    let actual = golden::canonical_json(&hash, &report, &sink.messages);
    let actual_pretty = serde_json::to_string_pretty(&actual).expect("serialize actual");
    assert_eq!(
        actual, expected,
        "canonical 输出与 pinned 期望不一致——parser 行为漂移或 fixture 未经评审变更。actual =\n{actual_pretty}"
    );
}

#[test]
fn golden_probe_confirms_fixture() {
    // Cline 是单文档 JSON 数组：probe 直接解析整体，无"破损行"概念。fixture 必须
    // 被确认为 Cline（数组 + 含 role 字段的记录）。
    let expected = golden::read_expected(EXPECTED_PATH);
    let bytes = golden::read_fixture_verified(FIXTURE_PATH, &expected);
    let r = ClineAdapter::new()
        .probe(&bytes)
        .expect("golden fixture probe must succeed");
    assert_eq!(r.confidence, Confidence::Confirmed);
}

#[test]
fn golden_messages_carry_no_byte_span() {
    // JSON 数组无行式字节坐标：span round-trip 标记 N/A，全部消息 span 为 None。
    let expected = golden::read_expected(EXPECTED_PATH);
    let bytes = golden::read_fixture_verified(FIXTURE_PATH, &expected);
    let (_, sink) = parse_fixture(&bytes);
    let messages = &sink.messages;
    assert!(!messages.is_empty(), "golden fixture must emit messages");
    assert!(
        messages.iter().all(|m| m.span.is_none()),
        "cline 不应归因字节 span（N/A，pseudo-span 已移除）"
    );
}

/// 手动再生辅助：
/// ```text
/// cargo test -p agent-session-grep-provider-cline --test golden -- --ignored --nocapture
/// ```
#[test]
#[ignore = "manual regeneration helper — prints canonical JSON for basic.expected.json"]
fn print_actual_canonical_output_for_regeneration() {
    let bytes = std::fs::read(FIXTURE_PATH).expect("read basic.json fixture");
    let hash = blake3::hash(&bytes).to_hex().to_string();
    let (report, sink) = parse_fixture(&bytes);
    println!(
        "{}",
        serde_json::to_string_pretty(&golden::canonical_json(&hash, &report, &sink.messages))
            .unwrap()
    );
}

#[test]
fn golden_epoch_millis_bom_preserves_text_and_reports_metadata_loss() {
    let expected = golden::read_expected(TIMESTAMP_EXPECTED_PATH);
    let bytes = golden::read_fixture_verified(TIMESTAMP_FIXTURE_PATH, &expected);
    assert!(bytes.starts_with(b"\xef\xbb\xbf"));
    let (report, sink) = parse_fixture(&bytes);
    let hash = blake3::hash(&bytes).to_hex().to_string();
    assert_eq!(
        golden::canonical_json(&hash, &report, &sink.messages),
        expected
    );
    assert_eq!(report.committed, 7);
    assert_eq!(report.skipped, 0);
    assert_eq!(report.session_native_id, None);
    assert_eq!(report.diagnostics.len(), 1);
    assert!(report.diagnostics[0].starts_with("2 message timestamps"));
    let timestamps: Vec<_> = sink
        .messages
        .iter()
        .map(|message| message.timestamp.as_deref())
        .collect();
    assert_eq!(
        timestamps,
        vec![
            Some("2025-01-01T00:00:00.123Z"),
            Some("1970-01-01T00:00:00.000Z"),
            Some("1969-12-31T23:59:59.999Z"),
            Some("2026-01-01T00:01:00Z"),
            None,
            None,
            None,
        ]
    );
    assert_eq!(
        sink.messages[0].text,
        "  synthetic millisecond boundary \u{feff} retained  "
    );
    assert!(
        sink.messages
            .iter()
            .all(|message| message.span.is_none() && message.native_id.is_empty())
    );
}

#[test]
#[ignore = "manual regeneration helper — prints canonical JSON for epoch-millis-bom.expected.json"]
fn print_epoch_millis_bom_canonical_output_for_regeneration() {
    let bytes = std::fs::read(TIMESTAMP_FIXTURE_PATH).expect("read timestamp fixture");
    let hash = blake3::hash(&bytes).to_hex().to_string();
    let (report, sink) = parse_fixture(&bytes);
    println!(
        "{}",
        serde_json::to_string_pretty(&golden::canonical_json(&hash, &report, &sink.messages))
            .unwrap()
    );
}

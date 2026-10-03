//! Cline provider adapter.
//!
//! Parses Cline's `api_conversation_history.json` format: a JSON array of
//! message objects with `role` (user/assistant) and `content`. Unlike JSONL
//! providers, Cline uses a single JSON array file per task.
//!
//! Format evidence: ctx (Apache-2.0) provider-support-matrix.json confirms
//! `cline_task_directory_json` source_format with
//! `api_conversation_history.json`, `ui_messages.json`, `context_history.json`,
//! `task_metadata.json` in task directories under CLINE_DATA_DIR/tasks/*/
//! and ~/.cline/data/tasks/*/.

use agent_session_grep_ports::{
    AdapterManifest, CanonicalEventSink, Confidence, MessageEvent, ParseReport, ProbeResult,
    ProviderAdapter, ProviderError, SearchInstant, manifest_for,
};

/// Variant id surfaced in probe results.
const VARIANT_ID: &str = "cline/api-conversation-history-v1";

/// Cline adapter: parses `api_conversation_history.json` (JSON array of messages).
pub struct ClineAdapter;

impl ClineAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClineAdapter {
    fn default() -> Self {
        Self::new()
    }
}

/// Minimal deserialization target for a Cline message record.
#[derive(serde::Deserialize)]
struct ClineMessage {
    #[serde(default)]
    role: String,
    #[serde(default)]
    content: Option<serde_json::Value>,
}

impl ProviderAdapter for ClineAdapter {
    fn provider_id(&self) -> &str {
        "cline"
    }

    fn manifest(&self) -> AdapterManifest {
        manifest_for(
            self.provider_id(),
            Some(2),
            &[
                "no session id in the JSON array file; session_native_id is left unset",
                "no byte spans (whole-file JSON array); native message ids are not preserved (ids are derived, not native)",
            ],
        )
    }

    fn probe(&self, bytes: &[u8]) -> Result<ProbeResult, ProviderError> {
        let text = cline_document_text(bytes)?;

        let mut matched = Vec::new();
        let unmatched = Vec::new();

        // Cline is a JSON array (not JSONL). Try to parse as a JSON array.
        let value: serde_json::Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                return Err(ProviderError::AmbiguousVariant(format!(
                    "not valid JSON array: {e}"
                )));
            }
        };

        let arr = match value.as_array() {
            Some(a) => a,
            None => {
                return Err(ProviderError::AmbiguousVariant(
                    "expected a JSON array, got a different shape".into(),
                ));
            }
        };

        if arr.is_empty() {
            return Err(ProviderError::AmbiguousVariant(
                "empty array: no messages to probe".into(),
            ));
        }

        // Sample up to 8 records to check for conversational messages.
        let sample = arr.iter().take(8);
        let mut json_records = 0usize;
        let mut conversational = 0usize;
        let mut has_role_field = 0usize;

        for record in sample {
            json_records += 1;
            if let Some(role) = record.get("role").and_then(serde_json::Value::as_str) {
                has_role_field += 1;
                if matches!(role, "user" | "assistant") {
                    conversational += 1;
                }
            }
        }

        matched.push(format!("{json_records} sampled records are valid JSON"));

        // Cline is distinct: JSON array with role/content. Refuse if no
        // role field present (could be a different JSON array format).
        if has_role_field == 0 {
            return Err(ProviderError::AmbiguousVariant(
                "no records with a `role` field found in sampled array".into(),
            ));
        }

        let confidence = if conversational > 0 {
            matched.push(format!(
                "{conversational} conversational messages (user/assistant)"
            ));
            Confidence::Confirmed
        } else {
            matched.push(format!("{has_role_field} records with role field"));
            Confidence::High
        };

        Ok(ProbeResult {
            variant_id: VARIANT_ID.to_string(),
            confidence,
            matched_evidence: matched,
            unmatched_evidence: unmatched,
        })
    }

    fn parse(
        &self,
        bytes: &[u8],
        sink: &mut dyn CanonicalEventSink,
    ) -> Result<ParseReport, ProviderError> {
        let text = cline_document_text(bytes)?;

        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ProviderError::StructuralFatal(format!("not valid JSON: {e}")))?;

        let arr = value
            .as_array()
            .ok_or_else(|| ProviderError::StructuralFatal("expected JSON array".into()))?;

        let mut report = ParseReport::default();
        let mut seq: u32 = 0;
        let mut invalid_timestamps = 0usize;

        for (idx, record) in arr.iter().enumerate() {
            let rec: ClineMessage = match serde_json::from_value(record.clone()) {
                Ok(r) => r,
                Err(e) => {
                    report.skipped += 1;
                    report
                        .diagnostics
                        .push(format!("record {idx}: invalid shape, skipped ({e})"));
                    continue;
                }
            };

            let role = rec.role.as_str();
            if !matches!(role, "user" | "assistant") {
                continue;
            }

            let content = rec.content.as_ref().unwrap_or(&serde_json::Value::Null);
            let text = cline_content_text(content);
            if text.trim().is_empty() {
                continue;
            }

            let timestamp = match cline_timestamp(record) {
                Ok(timestamp) => timestamp,
                Err(()) => {
                    invalid_timestamps += 1;
                    None
                }
            };

            sink.emit_message(MessageEvent {
                session: None,
                seq,
                native_id: "",
                parent_native_id: None,
                role,
                text: &text,
                timestamp: timestamp.as_deref(),
                is_sidechain: false,
                // Cline is a single JSON array, not a line-delimited format: the
                // adapter cannot attribute a byte range to one message without
                // byte-level JSON parsing. Emitting array-index pseudo-spans
                // would violate the span contract (byte offsets into the source),
                // so span is left None (capability.rs `source_span: unsupported`).
                span: None,
            })
            .map_err(|e| ProviderError::StructuralFatal(e.to_string()))?;
            seq += 1;
            report.committed += 1;
        }

        if invalid_timestamps > 0 {
            report.diagnostics.push(format!(
                "{invalid_timestamps} message timestamps invalid or unsupported; omitted"
            ));
        }
        Ok(report)
    }
}

/// Decode a single JSON document without changing snapshot bytes. The bounded
/// reader delegates here too; never strip BOMs from individual records or text.
fn cline_document_text(bytes: &[u8]) -> Result<&str, ProviderError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| ProviderError::StructuralFatal(format!("not valid UTF-8: {e}")))?;
    Ok(text.strip_prefix('\u{feff}').unwrap_or(text))
}

/// Native Cline `ts` is Date.now() milliseconds (see tests/golden/PROVENANCE.md).
/// The existing `timestamp` compatibility field accepts validated strings; its
/// numeric units are unproven, so they must not be guessed or emitted empty.
fn cline_timestamp(record: &serde_json::Value) -> Result<Option<String>, ()> {
    if let Some(value) = record.get("ts") {
        return if value.is_null() {
            Ok(None)
        } else {
            cline_epoch_millis(value).map(Some).ok_or(())
        };
    }
    match record.get("timestamp") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(text)) if valid_timestamp_text(text).is_some() => {
            Ok(Some(text.clone()))
        }
        _ => Err(()),
    }
}

/// Accept the integer JSON tokens written by Date.now(), within four-digit ISO
/// years. Never convert through f64: deserialization can already have rounded a
/// fractional value to an integer, or underflowed it to zero.
fn cline_epoch_millis(value: &serde_json::Value) -> Option<String> {
    const MIN_MILLIS: i64 = -62_167_219_200_000; // 0000-01-01T00:00:00.000Z
    const MAX_MILLIS: i64 = 253_402_300_799_999; // 9999-12-31T23:59:59.999Z
    let millis = value.as_i64()?;
    if !(MIN_MILLIS..=MAX_MILLIS).contains(&millis) {
        return None;
    }
    let instant = SearchInstant::from_unix_millis(millis);
    let days = instant.unix_seconds.div_euclid(86_400);
    let time = instant.unix_seconds.rem_euclid(86_400);

    // Gregorian civil-from-days arithmetic, also used by the Hermes adapter.
    // Euclidean division keeps negative epoch values on the correct UTC day.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    let (hour, minute, second) = (time / 3_600, (time % 3_600) / 60, time % 60);
    let fraction = instant.nanosecond / 1_000_000;
    Some(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{fraction:03}Z"
    ))
}

/// Validate ISO date-time compatibility text without changing its spelling.
/// Accept the existing UTC/offset forms (T or space, optional zone and fraction).
fn valid_timestamp_text(text: &str) -> Option<()> {
    let bytes = text.trim().as_bytes();
    if bytes.len() < 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = timestamp_digits(&bytes[..4])?;
    let month = timestamp_digits(&bytes[5..7])?;
    let day = timestamp_digits(&bytes[8..10])?;
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => return None,
    };
    if day == 0
        || day > days_in_month
        || timestamp_digits(&bytes[11..13])? > 23
        || timestamp_digits(&bytes[14..16])? > 59
        || timestamp_digits(&bytes[17..19])? > 59
    {
        return None;
    }
    let mut zone = &bytes[19..];
    if zone.first() == Some(&b'.') {
        let digits = zone[1..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if digits == 0 {
            return None;
        }
        zone = &zone[digits + 1..];
    }
    match zone {
        [] | [b'Z'] => Some(()),
        [b'+' | b'-', h1, h2, b':', m1, m2] | [b'+' | b'-', h1, h2, m1, m2] => {
            (timestamp_digits(&[*h1, *h2])? <= 23 && timestamp_digits(&[*m1, *m2])? <= 59)
                .then_some(())
        }
        _ => None,
    }
}

fn timestamp_digits(bytes: &[u8]) -> Option<u32> {
    bytes.iter().try_fold(0, |value, byte| {
        if byte.is_ascii_digit() {
            Some(value * 10 + u32::from(*byte - b'0'))
        } else {
            None
        }
    })
}

/// Extract text from Cline message content.
///
/// Content can be a string or an array of content parts.
fn cline_content_text(content: &serde_json::Value) -> String {
    match content {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(blocks) => {
            let mut buf = String::new();
            for block in blocks {
                if let Some(t) = block.get("text").and_then(serde_json::Value::as_str) {
                    if !buf.is_empty() {
                        buf.push('\n');
                    }
                    buf.push_str(t);
                }
            }
            buf
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_matches_provider_matrix() {
        let adapter = ClineAdapter::new();
        let manifest = adapter.manifest();
        assert_eq!(manifest.provider_id, adapter.provider_id());
        assert_eq!(manifest.supported_variants, vec![VARIANT_ID.to_string()]);
        assert_eq!(manifest.capabilities.provider_id, adapter.provider_id());
        assert_eq!(manifest.capabilities.variant_id, VARIANT_ID);
        assert!(manifest.last_certified_targets.is_empty());
        assert_eq!(manifest.fixture_revision, Some(2));
    }

    struct CountSink {
        count: usize,
    }
    impl CanonicalEventSink for CountSink {
        fn emit_message(
            &mut self,
            _event: MessageEvent<'_>,
        ) -> agent_session_grep_ports::PortResult<()> {
            self.count += 1;
            Ok(())
        }
    }

    #[test]
    fn probe_rejects_empty_input() {
        let adapter = ClineAdapter::new();
        assert!(adapter.probe(b"").is_err());
    }

    #[test]
    fn probe_rejects_non_json() {
        let adapter = ClineAdapter::new();
        assert!(adapter.probe(b"not json at all").is_err());
    }

    #[test]
    fn probe_rejects_json_object_not_array() {
        let adapter = ClineAdapter::new();
        assert!(adapter.probe(b"{\"role\":\"user\"}").is_err());
    }

    #[test]
    fn probe_rejects_empty_array() {
        let adapter = ClineAdapter::new();
        assert!(adapter.probe(b"[]").is_err());
    }

    #[test]
    fn probe_confirms_cline_json_array() {
        let adapter = ClineAdapter::new();
        let fixture = r#"[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]"#;
        let result = adapter.probe(fixture.as_bytes()).unwrap();
        assert_eq!(result.variant_id, VARIANT_ID);
        assert_eq!(result.confidence, Confidence::Confirmed);
    }

    #[test]
    fn probe_rejects_array_without_role() {
        let adapter = ClineAdapter::new();
        let fixture = r#"[{"foo":"bar"},{"baz":"qux"}]"#;
        assert!(adapter.probe(fixture.as_bytes()).is_err());
    }

    #[test]
    fn parse_extracts_messages() {
        let adapter = ClineAdapter::new();
        let fixture = r#"[{"role":"user","content":"hello world"},{"role":"assistant","content":"hi there"}]"#;
        let mut sink = CountSink { count: 0 };
        let report = adapter.parse(fixture.as_bytes(), &mut sink).unwrap();
        assert_eq!(sink.count, 2);
        assert_eq!(report.committed, 2);
    }

    #[test]
    fn parse_skips_non_conversational_roles() {
        let adapter = ClineAdapter::new();
        let fixture =
            r#"[{"role":"system","content":"system msg"},{"role":"user","content":"real msg"}]"#;
        let mut sink = CountSink { count: 0 };
        let report = adapter.parse(fixture.as_bytes(), &mut sink).unwrap();
        assert_eq!(report.committed, 1);
    }

    #[test]
    fn parse_handles_array_content() {
        let adapter = ClineAdapter::new();
        let fixture = r#"[{"role":"assistant","content":[{"type":"text","text":"part1"},{"type":"text","text":"part2"}]}]"#;
        let mut sink = CountSink { count: 0 };
        let report = adapter.parse(fixture.as_bytes(), &mut sink).unwrap();
        assert_eq!(report.committed, 1);
    }

    #[test]
    fn parse_skips_empty_content() {
        let adapter = ClineAdapter::new();
        let fixture = r#"[{"role":"user","content":""},{"role":"assistant","content":"real"}]"#;
        let mut sink = CountSink { count: 0 };
        let report = adapter.parse(fixture.as_bytes(), &mut sink).unwrap();
        assert_eq!(report.committed, 1);
    }

    struct TextSink {
        texts: Vec<String>,
    }
    impl CanonicalEventSink for TextSink {
        fn emit_message(
            &mut self,
            event: MessageEvent<'_>,
        ) -> agent_session_grep_ports::PortResult<()> {
            self.texts.push(event.text.to_string());
            Ok(())
        }
    }

    #[test]
    fn parse_passes_noise_shaped_user_text_through_verbatim() {
        // 钉住测试：Cline 的 api_conversation_history.json 没有 system-reminder /
        // AGENTS.md / 环境上下文等注入概念（系统层走 role:"system"，已被角色门
        // 跳过）。形似噪声的 user 文本必须逐字透传，防止将来把别家格式的过滤
        // 规则盲目搬来造成 silent drift。
        let adapter = ClineAdapter::new();
        let fixture = r##"[{"role":"user","content":"<system-reminder>reminder text</system-reminder>"},{"role":"user","content":"# AGENTS.md instructions"}]"##;
        let mut sink = TextSink { texts: vec![] };
        let report = adapter.parse(fixture.as_bytes(), &mut sink).unwrap();
        assert_eq!(report.committed, 2);
        assert_eq!(report.skipped, 0);
        assert_eq!(
            sink.texts,
            vec![
                "<system-reminder>reminder text</system-reminder>".to_string(),
                "# AGENTS.md instructions".to_string(),
            ]
        );
    }
}

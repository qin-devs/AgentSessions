//! Synthetic timestamp and single-document BOM regressions.
//! Upstream field/unit evidence is pinned in golden/PROVENANCE.md.

use agent_session_grep_ports::{Confidence, ParseReport, ProviderAdapter, SliceSource};
use agent_session_grep_provider_cline::ClineAdapter;
use agent_session_grep_testkit::golden::{self, CapturingSink};
use serde_json::{Value, json};

fn parse_record(record: Value) -> (ParseReport, CapturingSink) {
    let bytes = serde_json::to_vec(&json!([record])).unwrap();
    golden::parse_golden(&ClineAdapter::new(), &bytes)
}

#[test]
fn native_ts_is_epoch_milliseconds_not_a_digit_count_heuristic() {
    let cases = [
        (json!(1_735_689_600_123_i64), "2025-01-01T00:00:00.123Z"),
        (json!(1_735_689_600_i64), "1970-01-21T02:08:09.600Z"),
        (json!(0), "1970-01-01T00:00:00.000Z"),
        (json!(1), "1970-01-01T00:00:00.001Z"),
        (json!(-1), "1969-12-31T23:59:59.999Z"),
        (json!(1_234), "1970-01-01T00:00:01.234Z"),
        (json!(-62_167_219_200_000_i64), "0000-01-01T00:00:00.000Z"),
        (json!(253_402_300_799_999_i64), "9999-12-31T23:59:59.999Z"),
        (json!(951_782_400_000_i64), "2000-02-29T00:00:00.000Z"),
    ];
    for (ts, expected) in cases {
        let (report, sink) = parse_record(json!({
            "role": "user", "content": "  synthetic timestamp \n", "ts": ts,
            "taskId": "ignored-task", "id": "ignored-message", "parentId": "ignored-parent"
        }));
        assert_eq!(report.committed, 1, "ts={ts}");
        assert_eq!(report.skipped, 0, "ts={ts}");
        assert!(report.diagnostics.is_empty(), "ts={ts}: {report:?}");
        assert_eq!(report.session_native_id, None);
        let message = &sink.messages[0];
        assert_eq!(message.timestamp.as_deref(), Some(expected), "ts={ts}");
        assert_eq!(message.seq, 0);
        assert_eq!(message.role, "user");
        assert_eq!(message.text, "  synthetic timestamp \n");
        assert!(message.native_id.is_empty());
        assert_eq!(message.parent_native_id, None);
        assert!(!message.is_sidechain);
        assert_eq!(message.span, None);
    }
}

// Keep numeric lexemes as source bytes: json!(a Rust float) or a pre-parsed
// Value would lose exactly the precision these regressions need to exercise.
fn assert_raw_numeric_ts_is_omitted(raw_ts: &str) {
    let bytes = format!(
        r#"[{{"role":"user","content":"  synthetic raw numeric timestamp \n","ts":{raw_ts}}}]"#
    );
    let (report, sink) = golden::parse_golden(&ClineAdapter::new(), bytes.as_bytes());
    assert_eq!(sink.messages[0].timestamp, None, "raw ts={raw_ts}");
    assert_eq!(report.committed, 1);
    assert_eq!(report.skipped, 0);
    assert_eq!(report.diagnostics.len(), 1);
    assert!(report.diagnostics[0].len() < 128);
    assert!(!report.diagnostics[0].contains(raw_ts));
    assert_eq!(sink.messages[0].role, "user");
    assert_eq!(
        sink.messages[0].text,
        "  synthetic raw numeric timestamp \n"
    );
    assert!(sink.messages[0].native_id.is_empty());
    assert_eq!(sink.messages[0].span, None);
}

#[test]
fn raw_fractional_ts_cannot_round_to_an_integer_millisecond() {
    for raw_ts in [
        "1735689600123.00001",
        "1735689600123.99999",
        "-1735689600123.00001",
    ] {
        assert_raw_numeric_ts_is_omitted(raw_ts);
    }
}

#[test]
fn raw_underflowing_ts_cannot_round_to_the_epoch() {
    for raw_ts in ["1e-400", "-1e-400"] {
        assert_raw_numeric_ts_is_omitted(raw_ts);
    }
}

#[test]
fn raw_float_and_exponent_ts_tokens_are_not_native_integer_timestamps() {
    for raw_ts in ["1234.0", "0.0", "-1.0", "1e3", "1735689600123e0"] {
        assert_raw_numeric_ts_is_omitted(raw_ts);
    }
}

#[test]
fn missing_and_null_timestamps_stay_absent_without_loss_diagnostics() {
    for fields in [json!({}), json!({"ts": null}), json!({"timestamp": null})] {
        let mut record = json!({"role": "assistant", "content": "synthetic missing metadata"});
        record
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let (report, sink) = parse_record(record);
        assert_eq!(sink.messages[0].timestamp, None);
        assert_eq!(report.committed, 1);
        assert_eq!(report.skipped, 0);
        assert!(report.diagnostics.is_empty());
    }
}

#[test]
fn invalid_native_timestamps_preserve_messages_but_not_metadata() {
    for ts in [
        json!("1735689600123"),
        json!("2025-01-01T00:00:00Z"),
        json!(1_735_689_600_123.5),
        json!(true),
        json!([]),
        json!({"private": "synthetic-private-timestamp"}),
        json!(i64::MIN),
        json!(i64::MAX),
        json!(u64::MAX),
        json!(1e100),
        json!(-62_167_219_200_001_i64),
        json!(253_402_300_800_000_i64),
    ] {
        let (report, sink) = parse_record(json!({
            "role": "user", "content": "synthetic invalid metadata", "ts": ts
        }));
        assert_eq!(sink.messages[0].timestamp, None, "ts={ts}");
        assert_eq!(report.committed, 1);
        assert_eq!(report.skipped, 0);
        assert_eq!(report.diagnostics.len(), 1, "ts={ts}");
        assert!(report.diagnostics[0].len() < 128);
        assert!(!report.diagnostics[0].contains("synthetic-private-timestamp"));
    }
}

#[test]
fn legacy_string_timestamps_remain_lossless_when_valid() {
    for timestamp in [
        "2026-01-01T00:01:00Z",
        "2025-01-01T05:30:00.123+05:30",
        "2000-02-29T23:59:59.999999999-03:00",
        "2025-01-01 00:00:00+0000",
        " 2025-01-01T00:00:00Z ",
    ] {
        let (report, sink) = parse_record(json!({
            "role": "assistant", "content": "synthetic legacy timestamp", "timestamp": timestamp
        }));
        assert_eq!(sink.messages[0].timestamp.as_deref(), Some(timestamp));
        assert!(report.diagnostics.is_empty(), "{timestamp}: {report:?}");
    }
}

#[test]
fn invalid_or_unproven_legacy_timestamps_never_become_empty_strings() {
    // A numeric `timestamp` has no proven unit in this compatibility shape.
    for timestamp in [
        json!(1_735_689_600_000_i64),
        json!(0),
        json!(-1),
        json!(1.5),
        json!(""),
        json!("  "),
        json!("synthetic-private-timestamp"),
        json!("2025-02-29T00:00:00Z"),
        json!("2025-13-01T00:00:00Z"),
        json!("2025-01-00T00:00:00Z"),
        json!("2025-01-01T24:00:00Z"),
        json!("2025-01-01T00:60:00Z"),
        json!("2025-01-01T00:00:60Z"),
        json!("2025-01-01T00:00:00.Z"),
        json!("2025-01-01T00:00:00+24:00"),
        json!("2025-01-01T00:00:00+01:60"),
        json!("２０２５-01-01T00:00:00Z"),
        json!(true),
        json!([]),
        json!({"private": "synthetic-private-timestamp"}),
    ] {
        let (report, sink) = parse_record(json!({
            "role": "user", "content": "synthetic legacy metadata", "timestamp": timestamp
        }));
        assert_eq!(sink.messages[0].timestamp, None, "timestamp={timestamp}");
        assert_eq!(report.committed, 1);
        assert_eq!(report.skipped, 0);
        assert_eq!(report.diagnostics.len(), 1, "timestamp={timestamp}");
        assert!(!report.diagnostics[0].contains("synthetic-private-timestamp"));
    }
}

#[test]
fn native_ts_is_authoritative_without_falling_back_on_invalid_values() {
    for (ts, expected, diagnostics) in [
        (
            json!(1_735_689_600_123_i64),
            Some("2025-01-01T00:00:00.123Z"),
            0,
        ),
        (json!("bad"), None, 1),
        (json!(null), None, 0),
    ] {
        let (report, sink) = parse_record(json!({
            "role": "user", "content": "synthetic precedence", "ts": ts,
            "timestamp": "2026-01-01T00:00:00Z"
        }));
        assert_eq!(sink.messages[0].timestamp.as_deref(), expected);
        assert_eq!(report.diagnostics.len(), diagnostics);
    }
}

#[test]
fn timestamp_loss_diagnostics_are_aggregate_and_do_not_echo_source_values() {
    let record = json!({
        "role": "user", "content": "synthetic private content",
        "ts": {"secret": "synthetic-private-timestamp"}
    });
    let bytes = serde_json::to_vec(&vec![record; 256]).unwrap();
    let (report, sink) = golden::parse_golden(&ClineAdapter::new(), &bytes);
    assert_eq!(report.committed, 256);
    assert_eq!(report.skipped, 0);
    assert_eq!(report.diagnostics.len(), 1);
    assert!(report.diagnostics[0].contains("256"));
    assert!(report.diagnostics[0].len() < 128);
    assert!(!report.diagnostics[0].contains("private"));
    assert!(
        sink.messages
            .iter()
            .all(|message| message.timestamp.is_none())
    );
}

#[test]
fn initial_bom_is_handled_at_byte_and_reader_boundaries_only() {
    let plain = br#"[{"role":"user","content":"keep \uFEFF inside text","ts":1735689600123}]"#;
    let bytes = [b"\xef\xbb\xbf \r\n".as_slice(), plain].concat();
    let before = bytes.clone();
    let adapter = ClineAdapter::new();
    let source = SliceSource::new(&bytes);
    let probe = adapter.probe(&bytes).unwrap();
    assert_eq!(probe.confidence, Confidence::Confirmed);
    assert_eq!(probe, adapter.probe_source(&source).unwrap());
    let (byte_report, byte_sink) = golden::parse_golden(&adapter, &bytes);
    let mut reader_sink = CapturingSink::default();
    let reader_report = adapter.parse_source(&source, &mut reader_sink).unwrap();
    let (plain_report, plain_sink) = golden::parse_golden(&adapter, plain);
    assert_eq!(byte_report, reader_report);
    assert_eq!(byte_report, plain_report);
    let byte_output = golden::canonical_json("same source", &byte_report, &byte_sink.messages);
    assert_eq!(
        byte_output,
        golden::canonical_json("same source", &reader_report, &reader_sink.messages)
    );
    assert_eq!(
        byte_output,
        golden::canonical_json("same source", &plain_report, &plain_sink.messages)
    );
    assert_eq!(byte_sink.messages[0].text, "keep \u{feff} inside text");
    assert_eq!(byte_sink.messages[0].span, None);
    assert_eq!(
        byte_sink.messages[0].timestamp.as_deref(),
        Some("2025-01-01T00:00:00.123Z")
    );
    assert_eq!(bytes, before);
}

#[test]
fn bom_does_not_broaden_detection_or_allow_mid_document_markers() {
    let bom = b"\xef\xbb\xbf".as_slice();
    let message = br#"[{"role":"user","content":"synthetic"}]"#.as_slice();
    for bytes in [
        [bom, bom, message].concat(),
        [b" ", bom, message].concat(),
        [b"[", bom, br#"{"role":"user","content":"synthetic"}]"#].concat(),
        [bom, br#"{"role":"user","content":"synthetic"}"#].concat(),
        [
            bom,
            br#"[{"ts":1735689600000,"type":"say","text":"synthetic UI message"}]"#,
        ]
        .concat(),
        [bom, message, b"\n", message].concat(),
    ] {
        let adapter = ClineAdapter::new();
        let source = SliceSource::new(&bytes);
        assert!(adapter.probe(&bytes).is_err());
        assert!(adapter.probe_source(&source).is_err());
    }
    for bytes in [[bom, bom, message].concat(), [b" ", bom, message].concat()] {
        let adapter = ClineAdapter::new();
        let mut sink = CapturingSink::default();
        assert!(adapter.parse(&bytes, &mut sink).is_err());
        assert!(sink.messages.is_empty());
        assert!(
            adapter
                .parse_source(&SliceSource::new(&bytes), &mut sink)
                .is_err()
        );
        assert!(sink.messages.is_empty());
    }
}

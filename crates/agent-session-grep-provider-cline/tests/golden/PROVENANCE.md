# Golden fixture 来源声明（PROVENANCE）

> 依据 `docs/security/FIXTURE-REDACTION-POLICY.md`：合成优先、禁止真实 transcript、
> fixture 目录必须附本声明。

## fixture_revision

- `basic.json` — revision 1（2026-08-16 引入）。
- `epoch-millis-bom.json` — revision 2 (2026-10-03); manifest corpus revision 2.
- 格式修复必须新增 fixture 而非只改 parser（政策 §Provider fixture 要求）。

## 构造方式

`basic.json` 为**人工手写的合成 JSON 数组文档**（单文件
`api_conversation_history.json`，非行式），不基于任何真实会话记录做删减或脱敏。
字节即权威：UTF-8（无 BOM）、LF 行尾，BLAKE3 pin 在 `basic.expected.json`
（`cdf89cc8c2ea720591e36776b59ece29102b4a73e12acee75ca9007f7cf5bd46`），根
`.gitattributes` 的 `-text` 规则防止行尾改写。

## 逐条覆盖

顶层为 message 对象数组（`role`/`content`/`timestamp`）。

| 条目 | 内容 | 覆盖点 |
|------|------|--------|
| 0 | `role:"user"`，字符串 content | 用户消息 |
| 1 | `role:"assistant"`，字符串 content | 助手消息 |
| 2 | `role:"system"` | 系统消息跳过 |
| 3 | `role:"user"`，数组 content | `{type:"text",text}` block 以 `\n` 拼接 |
| 4 | `role:"assistant"`，空 content | 空正文跳过 |
| 5 | `role:"assistant"`，字符串 content + `timestamp` | 消息级时间戳透传 |

## 编码的真实格式知识（仅字段名与封套形状，无真实内容）

来自 Cline `api_conversation_history.json`（`~/.cline/data/tasks/*/`，单 JSON 数组）
的格式观察，仅复用结构：message 对象带 `role` 与 `content`（字符串或 content
part 数组）。适配器不产出字节 span——single JSON 数组不是行式格式，数组下标
pseudo-span 违反 `MessageEvent.span` 的字节区间契约，已移除（`span: None`），
capability.rs `source_span` 诚实声明为 `unsupported`。

## 脱敏与合规声明

- **不含任何真实数据**：时间戳为 `2026-01-01T00:01:00Z` 固定基准、正文为自述性
  合成文案；
- 无人名、邮箱、token、密钥、真实项目名或真实主机路径；
- 未从任何同类项目复制 fixture（政策 §许可证边界）。

## span round-trip

**N/A**：单文档 JSON 数组无行式字节坐标，adapter 全部消息 `span: None`
（golden 测试 `golden_messages_carry_no_byte_span`）。

## Timestamp/BOM remediation — fixture revision 2 (2026-10-03)

`epoch-millis-bom.json` is a new hand-written synthetic fixture, not an edited
copy of `basic.json` or an upstream transcript. Its bytes are UTF-8 with exactly
one initial BOM and LF line endings (824 bytes). BLAKE3
`15070b8268d0eb44ed93a5b33f87b16c2da109275f101a42a84fbbd8642c42d3` and
all canonical message fields are pinned in `epoch-millis-bom.expected.json`.
The manual helper `print_epoch_millis_bom_canonical_output_for_regeneration`
prints this output without modifying the fixture. All content, IDs and timestamp
values are synthetic. The original revision-1 fixture and its canonical output
stay intact.

### Primary format and unit evidence

The following Cline sources are pinned to commit
`791d2389966d927396830470c51d456d04687b69` (2026-05-25), not a moving branch:

- [`ClineStorageMessage`](https://github.com/cline/cline/blob/791d2389966d927396830470c51d456d04687b69/apps/vscode/src/shared/messages/content.ts#L70-L105)
  declares optional `ts: number` on the API conversation message.
- [`Task` message creation](https://github.com/cline/cline/blob/791d2389966d927396830470c51d456d04687b69/apps/vscode/src/core/task/index.ts#L2626-L2630)
  passes `role`, `content` and `ts: Date.now()` to
  `addToApiConversationHistory`; assistant creation also sets `ts: Date.now()`
  (lines 2767, 3172 and 3262).
- [`MessageStateHandler.addToApiConversationHistory`](https://github.com/cline/cline/blob/791d2389966d927396830470c51d456d04687b69/apps/vscode/src/core/task/message-state.ts#L178-L184)
  appends that object and calls `saveApiConversationHistory`.
- [`saveApiConversationHistory`](https://github.com/cline/cline/blob/791d2389966d927396830470c51d456d04687b69/apps/vscode/src/core/storage/disk.ts#L245-L254)
  serializes the array with `JSON.stringify(apiConversationHistory)` to the
  `api_conversation_history.json` filename declared at line 45.
- ECMAScript [`Date.now`](https://tc39.es/ecma262/multipage/numbers-and-dates.html#sec-date.now)
  returns a time value; [time values](https://tc39.es/ecma262/multipage/numbers-and-dates.html#sec-time-values-and-time-range)
  are integral milliseconds since the Unix epoch. Thus **native `ts` is Unix
  milliseconds**, independently of digit count. This is Cline API-history
  evidence, not Roo Code or Cline's separate `ui_messages.json` format.

The older pinned Cline commit `cc0d4ae6cb8a9645db689bcdbecf758fe39047ef`
(2026-01-13) does not declare `ts` on `ClineStorageMessage` and creates API
messages without it. Missing timestamps are therefore normal optional metadata,
not a reason to skip conversation text or manufacture a time.

### Supported timestamp interpretation

- Native `ts` is authoritative when the key is present. Only integer JSON
  tokens accepted by `Value::as_i64` normalize from milliseconds to
  `YYYY-MM-DDTHH:MM:SS.mmmZ`, matching the persisted `Date.now()` representation.
  Decimal or exponent spellings (including `1234.0` and `1e3`), other JSON types,
  and values outside the four-digit ISO year range `0000..9999` are unsupported;
  they are not rounded, interpreted as seconds, or replaced by another field.
  No `as_f64` conversion or post-deserialization `fract() == 0` check is used:
  decoding may already have rounded `1735689600123.00001` to an integer or
  underflowed `1e-400` to zero.
- `timestamp` is the adapter's existing compatibility field, **not** an
  upstream numeric-field assertion. Valid ISO date-time strings retain their
  exact spelling when `ts` is absent. Numeric `timestamp` values have no proven
  unit in this shape and become `None`, not `Some("")` or a guessed instant.
- A missing field or JSON null remains `None` without a diagnostic. Invalid or
  unsupported timestamp metadata also becomes `None` while the conversation
  message is retained. One bounded aggregate diagnostic counts affected emitted
  messages; it never includes timestamp values, transcript text, IDs or paths.
  `skipped` remains a message-loss count, not a metadata-loss count.

### New fixture and regression coverage

The new array exercises native milliseconds (including zero and negative one),
legacy string preservation, missing metadata, an unproven numeric compatibility
field, an invalid native field, Unicode/BOM inside actual text, and unchanged
role/empty-text exclusion. Synthetic `id`/`taskId` fields remain ignored: no
native session/message identity is added. `tests/timestamps.rs` additionally
covers millisecond precision, a seconds-shaped numeric value, Gregorian leap
and range boundaries, malformed fields, field precedence, and bounded diagnostics.
Precision regressions feed raw JSON bytes for fractional, underflowing and
integral-looking float/exponent tokens, rather than constructing a Rust float
or pre-parsing a `Value` that would erase the original numeric spelling. These
metadata defects retain the message and its text with `skipped == 0`.

Probe and parse remove exactly one BOM only at byte zero of the single JSON
document. The default bounded `ReadOnlySource` path delegates to those same byte
entry points. A repeated or mid-document BOM remains invalid; U+FEFF inside
conversation text is preserved. No shared JSONL reader changes are involved.
All emitted spans remain `None`; the BOM never creates pseudo-byte coordinates.

Timestamp conversion does not change source bytes, the variant, or emitted
sequence order. Canonical identity derivation and sequential multi-source
reparse/storage behavior are integration concerns owned by the main task, not
claims certified by these provider-only tests. Provider maturity is unchanged.

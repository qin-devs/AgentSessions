# Golden fixture 来源声明（PROVENANCE）

> 依据 `docs/security/FIXTURE-REDACTION-POLICY.md`：合成优先、禁止真实 transcript、
> fixture 目录必须附本声明。

## fixture_revision

- `basic.db` — revision 1（2026-08-16 引入）。
- 格式修复必须新增 fixture 而非只改 parser（政策 §Provider fixture 要求）。

## 构造方式

`basic.db` 为 **SQLite 二进制 fixture**，由本目录 `generate_fixture.py`（Python 3
标准库 `sqlite3`）生成：先 `CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value
TEXT)`，再插入两个 key 的 JSON 值，`commit` 后关闭——JSON 内容逐字见生成脚本，
是唯一来源。**纯合成**：不基于任何真实 `state.vscdb` 做删减或脱敏。字节即权威，
BLAKE3 pin 在 `basic.expected.json`
（`26556f79bc187baa0169ca383fd411e086bdad2438301a40a4a8f15e4d857ab9`）。

再生（仅审计用；committed 字节与 pinned BLAKE3 才是契约）：
```text
python crates/agent-session-grep-provider-cursor/tests/golden/generate_fixture.py
```

## 逐 key 覆盖

| key | 内容 | 覆盖点 |
|-----|------|--------|
| `workbench.panel.aichat.view.aichat.chatdata` | 1 个 tab（`tab-1`），3 个 bubble | bubble 按 `timingInfo.startTime` 排序；`text` 优先于 `rawText`；无 `text` 的 bubble 用 `rawText`；tab `createdAt` 排序 |
| `aiService.prompts` | 1 个 conversation（`conv-a`），2 个 prompt | prompt/response 展开为 user+assistant；同 conversation 按 `createdAt` 排序；空 response 跳过；第二会话 → `multi_session` fail-closed |

## 编码的真实格式知识（仅字段名与封套形状，无真实内容）

来自 Cursor `state.vscdb`（VS Code workspaceStorage ItemTable KV）的格式观察，
仅复用结构：chatdata key 的 JSON 文档含 `tabs[]`（`id`/`createdAt`/`bubbles[]`
含 `type`/`text`/`rawText`/`timingInfo.startTime`）；prompts key 的 JSON 数组含
`{prompt, response, createdAt, conversationId}`。形状适配自 hstry（MIT）的
idea-level 结构，未复制任何字节。

## 脱敏与合规声明

- **不含任何真实数据**：tab/prompt/conversation id 为 `tab-1`/`pN`/`conv-a` 固定
  假值、时间戳为 `101`–`201` 合成整数、正文为自述性合成文案；
- 无人名、邮箱、token、密钥、真实项目名或真实主机路径；
- 未从任何同类项目复制 fixture（政策 §许可证边界）。

## span round-trip

**N/A**：SQLite 源无文件内字节坐标，adapter 全部消息 `span: None`
（golden 测试 `golden_messages_carry_no_byte_span`）。只读契约由
`assert_read_only` 守护（adapter 以 `SQLITE_OPEN_READONLY` 打开临时副本）。

---

## `disk-kv.db` / `disk-kv-shuffled.db` — `cursor/disk-kv-v1`（revision 1，2026-09-29 引入）

### 构造方式

两个文件都是 **SQLite 二进制 fixture**，由本目录 `generate_diskkv_fixture.py`
（Python 3 标准库 `sqlite3`）生成：`CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY,
value BLOB)`，插入固定顺序的 `composerData:<composerId>` 元数据行与
`bubbleId:<composerId>:<bubbleId>` 正文行（含 NULL 值与一个 0xFF BLOB），`commit`
后关闭。Python 数据表是唯一来源；**纯合成**：不基于任何真实 `state.vscdb` 做删减
或脱敏，也未从任何参考项目复制 fixture。两个文件逻辑内容相同、KV 插入序不同
（`disk-kv.db`：composer 按 [A, B] 插入、bubble 按 header 逆序插入；
`disk-kv-shuffled.db`：composer 交换为 [B, A]、bubble 按 key 降序插入），用来把
“输出只认 `fullConversationHeadersOnly`、不认 KV key/rowid/插入序”钉成字节级契约。

再生（仅审计用；committed 字节与 pinned BLAKE3 才是契约，SQLite 页面布局随库版本
可能变化）：

```text
python crates/agent-session-grep-provider-cursor/tests/golden/generate_diskkv_fixture.py
```

BLAKE3 pin 在 `disk-kv.expected.json`：`disk-kv.db` = `62f266422cf76f08d5fc42eb0030a4d711d959d386c3d0f9c5481ad788d35051`，
`disk-kv-shuffled.db` = `182e1430b59edcdc66fe8b2eadccba5662f6895eed2c847366988fd4b1dba7ce`。

### 逐槽位覆盖

composer `cmp-2`（key 字节序在 `cmp:雪/#%_e\u0301` 之前，插入序在其后）2 个槽位、
composer `cmp:雪/#%_e\u0301`（含 `:`/`#`/`%`/`_`、CJK、组合字符的 verbatim id）
15 个槽位；`disk-kv.expected.json` pin 了 emitted 9 / skipped 8 的完整投影。

| 槽位 | 内容 | 覆盖点 |
|------|------|--------|
| `cmp:雪/#%_e\u0301` 槽 0（`z:💡`，type 1） | 正文两侧带空格 | header 序 + 非 BMP/分隔符 id 的精确 key 命中；正文逐字透传，不 trim |
| 槽 1/2（`dup` 两次，type 2） | 同一 storage cell | 重复 bubble id 保留独立 header 序位（两条消息各自 emit） |
| 槽 3（`tool-raw`） | `rawArgs` JSON 串 + 干扰性 `params` | 工具输入 `rawArgs` 优先；JSON 字符串编码按 verbatim 文本索引，绝不执行 |
| 槽 4（`tool-params`） | 只有 `params` 对象 | `params` 兜底；对象编码渲染为紧凑 JSON |
| 槽 5（`tool-text`） | `rawArgs` 纯文本（不可解析） | 纯文本参数原样保留 |
| 槽 6（`bad:json/雪#%`） | 原始 `{` | `malformed_json`；诊断里给出 verbatim storage key |
| 槽 7（`missing`） | 无行 | `missing_row`（不静默归零） |
| 槽 8（`null`） | SQL NULL | `null_value` |
| 槽 9（`array`） | `[]` | `invalid_bubble_shape` |
| 槽 10（`utf8`） | 0xFF BLOB | `invalid_utf8`（不 CAST 成 U+FFFD） |
| 槽 11（无 `bubbleId`） | header `{"type":1}` | `invalid_header`；未知角色不猜成 assistant |
| 槽 12（`unknown-role`，type 77） | 正文存在 | `unknown_role`，不猜测角色 |
| 槽 13（`blank`） | `{"text":"   "}` | `empty_bubble`（不 emit 空正文） |
| 槽 14（`tool-null-raw`） | `rawArgs` 字符串 `"null"` + `params` | `rawArgs` 选中后不被 `params` 覆盖（探针同款 `must_not_replace_null`） |
| `cmp-2` 槽 0/1 | 两个正常槽位 | 多 composer 的 per-message session 归属与 report 级 multi_session fail closed |

断言位置：`tests/disk_kv_golden.rs`（pinned 投影 = 消息 + session + 计数 + 诊断；
插入序不变性比较两个 fixture；只读断言；ItemTable fixture 仍归 ItemTable variant）
与 `src/disk_kv.rs` 的单元测试（header 序/插入序、五种坏行状态、工具输入选择、
unknown_role/invalid_header、重复 id 序位、多 composer 身份、分隔符歧义、
duplicate_row、全部有界路径、私有副本清理、WAL 并发提交下的源不可变）。

### 编码的真实格式知识（仅字段名与封套形状，无真实内容）

固定 Wake 实现 `iAmCorey/Wake` 提交 `71aeca67ec80f8645d1f9d5199290c2c732036ce`
（v0.8.5）的 `crates/wake-core/src/adapters/cursor_ide.rs`：L189-L196
（`composerData:<id>` 取值）、L198-L222（`bubbleId:<cid>:` 正文行，清理过的气泡是
NULL 行）、L224-L236（顺序只认 `fullConversationHeadersOnly`，不按 KV key 序）、
L346-L384（单会话解析：header 顺序 + header `type` 定角色，1=user、2=assistant）、
L458-L545（bubble → 消息；`json_field` 对同一字段兼容 JSON 字符串与对象，
`tool_from` 的输入选择为 `rawArgs` 优先、`params` 兜底）。另有归档的合成结构探针
（`.trellis/tasks/archive/2026-09/09-29-wake-reuse-study/research/experiments/providers/`，
35 例含本变体 17 例）。只复用结构事实；未复制任何字节或 fixture。Wake 为 MIT
（Copyright (c) 2026 Corey Chiu）。

**Cursor 的私有存储没有取得官方版本化契约**：没有可以引用的上游 schema 快照或
版本承诺，本变体的证据边界仅是上述固定实现 + 合成探针/golden，不构成任何
shipped Cursor 版本的兼容性认证，也不授权 resume。

### 已知边界（未认证项，如实标注）

- **身份**：`bubbleId` 是 `bubbleId:<composerId>:<bubbleId>` 的存储键组件，
  `composerData:<id>` 的 id 也没有官方“全局稳定”证明。消息以空 `native_id` 上报，
  由组合根派生 document-scoped id（与 hermes rowid、pi 记录 id 同一决定）；composer
  id 作为会话身份观测原样保留，槽位诊断在点名时给出逐字 storage key——不重写、
  不归一化、不 hash，ok 槽位的 bubble id 不单独回显（其“原样使用”由精确 key 命中
  与 golden 中的 verbatim 正文证明）。
- **分隔符歧义**：`:` 连接意味着 (composer `a:b`, bubble `c`) 与 (composer `a`,
  bubble `b:c`) 拼出同一 storage key；adapter 只做精确 key 查找，不拆分、不猜解
  （单元测试 `parse_treats_two_native_tuples_that_spell_one_key_as_the_storage_cell_it_is`）。
- **角色**：只认 header `type`（1=user、2=assistant）；未知/缺失角色计入
  `unknown_role`，绝不静默折成 assistant。
- **空正文**：canonical 流没有 tool-call 槽位，bubble 自身 text 为空时以选中的工具
  输入文本作为正文；两者都空计入 `empty_bubble`（不 emit 空消息）。
- **工具输入只作文本**：`rawArgs` 优先、`params` 兜底，字符串/对象两种编码均接受，
  绝不执行输入；`rawArgs` 为字符串 `"null"` 时仍算已选中，不被 `params` 覆盖。
- **重复 JSON key**：冻结的合成探针把顶层重复 key 视为 `malformed_json`；本 adapter
  用 `serde_json`（保留最后一个值），不检测重复 key——记录为边界，不是兼容承诺。
- **source span**：SQLite 行在已验证快照里没有连续字节区间，恒 `span: None`
  （同 opencode/cursor ItemTable）。
- **发现根**：cursor 的 `discover` 在 capability matrix 保持 `unsupported`
  （VS Code workspaceStorage 布局无本机证据，不猜路径），disk-kv 源同样按显式路径
  ingest；本变体不新增 discovery 承诺。
- **有界**：composers 4096 / 单 composer header 100_000 / 全库 header 250_000 /
  单 cell 8 MiB / 库内 cell 合计 64 MiB；adapter 内部快照上限 128 MiB
  （`SQLITE_MAX_SOURCE_BYTES`，与 cursor manifest 的整源上限一致，生产路径与直调
  都可达）。任一超限显式失败（`SourceTooLarge` / `RecordTooLarge`），绝不截断当成功。
- **capture 层副作用（如实记录，非 adapter 行为）**：与 hermes 相同——整链路
  `sync` 一个 WAL 活跃的源时，`state.vscdb` 与 `-wal` 的字节不变，但 `-shm` 可能被
  既有 capture 路径写入读标记；adapter 本身从不打开源文件，只在私有副本上单次固定
  读事务（证据：`source_database_files_stay_byte_identical_across_a_concurrent_wal_commit`、
  `tests/disk_kv_golden.rs::probe_and_parse_never_mutate_the_received_snapshot_bytes`）。


---

## `timestamps.json` — P1-2 timestamp regression (revision 1, 2026-10-03)

### Construction and scope

This is a new, entirely synthetic JSON fixture of SQLite row payloads, not a
capture or redaction of a real database. `tests/timestamps.rs::database` creates
an in-memory `ItemTable` or `cursorDiskKV`, inserts the selected fixture rows,
and materializes a temporary SQLite snapshot with `VACUUM INTO`. The test owns
and removes only that generated file. No source corpus is opened or modified.
The fixture's `fixture_revision` is 1; the existing provider maturity and
manifest fixture revision are unchanged.

The fixed epoch anchor is `1704067200123` milliseconds
(`2024-01-01T00:00:00.123Z`). The values one millisecond before and after it
pin the precision needed for half-open `[since, until)` filters. Values -1, 0
and 1 pin the unit independently of magnitude and the pre-epoch remainder.
Missing/null fields remain absent. The disk-kv fixture adds an offset timestamp
with nine fractional digits, repeated header references, out-of-time-order
headers, and a composer timestamp that must never supply missing bubble time.
All IDs and text are invented, including the Unicode/whitespace sentinel.
Invalid-type and out-of-range cases are generated synthetically in the test.

`basic.db`, `disk-kv.db`, and `disk-kv-shuffled.db` are not regenerated or changed.
The old ItemTable golden projection is corrected only for its timestamp strings;
source hashes, text, ordering, identity and accounting stay the same.

### Revalidated primary implementation evidence

These are pinned implementations of a private format, not official Cursor
schemas or a compatibility certification. Only format facts are reused; no
implementation or fixture bytes are copied.

* **ItemTable milliseconds:** `byteowlz/hstry` (MIT), commit
  `af32c07c5baf190105c1cd39530bcab25d464595`,
  `adapters/cursor/adapter.ts`, L544-L562 assigns bubble
  `timingInfo.startTime` directly to message `createdAt`; L595-L609 does the
  same for both messages of an `aiService.prompts` exchange. L671-L674 renders
  this message field with `new Date(msg.createdAt).toISOString()`, establishing
  milliseconds, not seconds. Tab `createdAt` remains an ordering observation,
  not a replacement for missing message time.
* **disk-kv bubble strings:** the already-pinned Wake commit
  `71aeca67ec80f8645d1f9d5199290c2c732036ce`,
  `crates/wake-core/src/adapters/cursor_ide.rs`, L466-L470 reads bubble
  `createdAt` as a string and sends it to `iso_ms`. Composer numeric
  `createdAt`/`lastUpdatedAt` are separate session metadata (L578-L579).
* **Independent field distinction:** `skillsynchq/txcript` (Apache-2.0), commit
  `8a20761fc57e5d5201cc4f52f28cebef541a8049`,
  `docs/formats/cursor-desktop.md`, L75-L78 distinguishes composer epoch
  milliseconds from bubble RFC3339 `createdAt`.
  `src/harness/cursor_desktop.rs`, L196-L201 accepts only string bubble
  timestamps for RFC3339 parsing; its writer emits bubble ISO strings at
  L618-L620 and composer epoch milliseconds at L645-L646/L673-L676.

The hstry composer helper `toMilliseconds` (L320-L331) uses a magnitude
heuristic. It is deliberately **not** unit evidence and is not reused.
There is no proven numeric-seconds or numeric-milliseconds bubble alternative
in the pinned disk-kv evidence above. Numeric bubble `createdAt` therefore
must not be interpreted as either unit, and composer time must not be copied
to messages as a fallback.

### Parser boundary and remaining integration limits

ItemTable integer epochs become UTC strings with exact millisecond precision
in the formatter's four-digit year range (0000 through 9999); values outside
that range retain the message but omit its timestamp with a diagnostic.
Raw epoch integers still control the existing sort, so formatting or omission
of an out-of-range timestamp does not reorder, drop or re-identify messages.
Unsupported ItemTable JSON field types retain the existing recoverable
whole-value parse failure; no numeric value is converted to an empty string.

Disk-kv source timestamp strings are preserved verbatim under the generic
`MessageEvent.timestamp` contract, including timezone and fractional spelling;
this change is not a new calendar validator. Missing/null time stays absent.
Unsupported JSON types and empty/whitespace-only strings yield no timestamp
and a field-loss diagnostic, not a skipped message or a guessed instant.
The existing header traversal, storage-key identity, body/role/tool handling,
slot counts, source bounds and temporary-database behavior are unchanged.

Provider tests pin the emitted representations; CLI filtering and parser-version
re-ingestion are owned by the main session. A migration caveat remains: Cursor
emits no native message ID, but two sources with identical snapshot fingerprints
share document-scoped message IDs (`provider + variant + document + seq`).
Timestamps do not participate in those IDs. Sequentially reprocessing two old,
identical ItemTable snapshots can therefore compare an old raw numeric string
with its new UTC spelling under the same message ID; integration must exercise
that cross-source intrinsic-conflict boundary rather than assuming that empty
native IDs make it impossible.

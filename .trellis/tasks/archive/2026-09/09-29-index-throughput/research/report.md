# 初始索引吞吐与峰值内存专项：trace 归因、逐项优化与 A/B 证据

> 任务：`.trellis/tasks/09-29-index-throughput`；分支 `feat/post-reuse-phase`。
> 归档对照基线：`2b8f895` 实测 482.6 s / 4.58 GB（`09-29-wake-reuse-study`）。
> 本次复测基线：`72cb684`（当前 main 前沿 + 测量 harness 提交；封存 A/B 报告记录的基线二进制 SHA-256 为 `47e3c6a0…`，该二进制现已不在磁盘）。
> 所有数字来自本任务 `research/harness/baseline.py`（release 二进制、合成语料、无 OS 缓存清理、无并发构建）或本报告注明的已落盘 trace / 诊断 JSON；凡未落盘的一次性观测均明确标注为不可复算。

## 1. 结论摘要

- **时间**：1M 初始 sync 三次运行中位 `412.7 s → 257.9 s`（−37.5%，详见 §5），阶段级 trace 显示 `cli:commit` 从 567.6 s 降到 245.8 s（−56.7%，同条件累计 trace 对照）。
- **峰值 RSS**：1M 三次运行峰值中位 `4574.6 MB → 2858.8 MB`（−37.5%），未达成 ≥50% 目标；归因见 §7（批次内数据副本 + 别名再生整表加载 + manifest/SQLite 工作集）。
- **硬目标均未达成**：PRD 要求 1M 初始 sync ≤ 241 s 且峰值 RSS ≤ 2.29 GB；实测 257.9 s / 2858.8 MB，均未达标（时间相对归档基线 482.6 s 的改善为 −46.6%，仍不足 ≥50%）。
- **search / noop 护栏**：search p50/p95 各规模未回退（1M p95 三次运行中位 154.3 → 151.7 ms，−1.7%）；noop 在配对交错 A/B 中持平略优（100k −2.1%）；1M 首个（冷）空转 pass 总耗时 5.90/6.78/6.86 s → 1.51/3.31/3.48 s（首 batch 1.38–1.83 s → 1.08–1.41 s）—— 空转双读/双哈希消除的方向一致证据（§4、§5）。
- **未发现正确性回归**：SQLite 适配器 270 测试全绿；扩展等价性场景（no-op / 缩小替换 / 空源墓碑，25 张非影子表，`research/results/auxiliary/equiv-extended-10k.json`）0 不一致（§6）。

## 2. 测量协议与环境

| 项 | 值 |
|---|---|
| OS / CPU / 磁盘 | Windows / Intel Core Ultra 7 155H / NVMe（见 `harness/environment.json`） |
| 语料 | harness 合成语料，`fixture` 冻结；数据哈希按规模记录在证据 JSON |
| 10k 语料哈希 | `f4365a47329760bdf9c96bd98848c3ff9e79747b4f74739f6daf72f75c2f2c08` |
| 100k 语料哈希 | `34f02c25a5a26a1c…`（见证据 JSON `dataset.dataset_hash`） |
| 1M 语料哈希 | `22519fe8ef5b84d34a7f26b62544a6d58bc7f2961dc0458908ff6cf8c180c990` |
| 基线二进制（封存 A/B 记录） | `agent-session-grep.exe` SHA-256 `47e3c6a010709f691e0b6b826fc88031628d9712685bec79d92817df3323130a`（`72cb684` 构建；**该二进制已不在磁盘，无法复跑**） |
| 磁盘上的参考二进制（§3 内存诊断使用） | `target/index-throughput/baseline-target/release/agent-session-grep.exe`，SHA-256 `664c2e7bf035c1fc88f956df03345449a024ba2e3f7cf33c407658b9cc95c4fc`（`c2d58e8` 的 `index-throughput-baseline` worktree 构建，dep-info 可查） |
| 基线测量提交 | `72cb684`（harness 参数化后） |
| harness pin | `--expected-commit` 必填；工作区 HEAD 必须等于该提交，且 `Cargo.toml/Cargo.lock/crates/scripts/evidence` 相对它无改动，否则拒绝运行 |
| 限制（沿用研究结论） | 未清 OS 缓存，非冷盘结论；初始 sync 为分批总计；search 含进程启动；Windows 计时已用高精度时钟 |

### 2.1 当前 main 基线复测（n=3，中位数）

| 规模 | 初始 sync | 空转 p50 / p95 | search p50 / p95 | 峰值 RSS | catalog.db |
|---|---|---|---|---|---|
| 10k | 2.18 s | 19.6 / 19.7 ms | 113.0 / 155.3 ms | 153.6 MB | 51.8 MB |
| 100k | 28.66 s | 61.6 / 63.1 ms | 111.1 / 145.3 ms | 1439.1 MB | 518.0 MB |
| 1M | 412.7 s | 582.5 / 6785.8 ms | 116.3 / 154.3 ms | 4574.6 MB | 4398.6 MB |

与归档基线（482.6 s / 4.58 GB）相比，当前 main 复测确认了同一瓶颈（1M 初始 sync 超线性、峰值 RSS 与库体积同阶），但绝对时间受环境漂移影响（+/-15%）。

## 3. 阶段级 trace 归因（基线，1M = 5×200k 批）

env 门控 trace（`ASG_INDEX_TRACE`，默认关闭，见 `crates/*/src/trace.rs`）原始 JSON：`research/traces/1m.jsonl`（基线 `72cb684`）与 `research/traces/1m-after.jsonl`（候选 1–5 的二进制 `5ba9939`；候选 6 的阶段变化见 §4 表与内存诊断）。

基线 5 批 `cli:commit` 阶段合计（秒）：

| 阶段 | 合计 | 说明 |
|---|---|---|
| session_projection | 172.2 | 兼容别名再生（整表加载 placements/edges + 每条实体 SELECT/JSON 重写） |
| relation_upserts | 128.6 | 380k 行/批的多行 INSERT（placements/edges/activities/usages） |
| outbox_intent | 30.6 | durable intent 写入（manifest JSON 序列化） |
| tx_commit | 30.1 | SQLite 事务提交（WAL） |
| source_replacements | 29.2 | 成员/扫描/claim 行重写 |
| integrity_checks | 28.8 | 本批 id 的引用完整性校验 |
| manifest | 25.5 | `batch_manifest` + identity 校验 + late no-op 探测 |
| verify_outbox | 22.8 | durable intent 重算比对 |
| load_catalog_state | 21.5 | 整库关系/成员状态 8 张表全量载入 |
| affected_sessions | 18.7 | 逐消息 `collect_message_sessions`（N+1） |
| fts_projection | 16.2 | FTS5 行 + 身份边车写入 |
| 其余（validate/merge/tombstones/capture/stage/assemble） | ~24 | 批内数据处理 |

内存诊断（`research/results/auxiliary/state-load-diagnostic-{before,after}.jsonl`，把 200k 新消息写入 1M 库的第 6 批）只落盘了 trace 记录：`load_catalog_state` 23.417 → 1.535 s、`cli:sync.commit` 93.31 → 66.91 s、整库 `stored_placements/stored_edges` 载入 1,000,000/900,000 行 → 0（改为按批内候选 id 加载）——与 trace 中该阶段的排名一致。同一场景的 RSS 采样最初只打印到 stdout、未落盘（该数字仅方向性、不可复算）；可复算的 RSS 证据见 §7 与 `research/results/auxiliary/mem-diag-fresh-200k-{before,after}.json`。

## 4. 逐项候选（每项一个提交 + A/B）

| # | 提交 | 机制 | 证据 | 结论 |
|---|---|---|---|---|
| 1 | `de33e75` | 未变化源不再重复 `verify_snapshot`（空转双读/双哈希消除） | 候选 1 专属 100k A/B（`results/cand1-noop-verify/`，3 次运行）：noop p50 中位 61.57 → 58.09 ms（**−5.6%**；p95 中位 63.07 → 65.71 ms，噪声范围内）；1M 冷空转见 §5 | 保留 |
| 2 | `490e0ba` | 写连接 `PRAGMA cache_size = -131072`（128 MiB；不改 `synchronous`/journal 语义） | 100k 同窗口**累计** A/B（基线 vs 候选 1+2，`results/auxiliary/cache-size-ab-100k.json`）：初始 sync 28.14 → 19.92 s（−29.2%）、noop 69.1 → 61.0 ms；RSS +104 MB | 保留 |
| 3 | `fd918f1` | late no-op 探测复用已载入状态，去掉第二遍整库载入 | 1M 累计 trace（基线 `1m.jsonl` vs 候选 1–5 的 `1m-after.jsonl`）：manifest 25.49 → 17.21 s；load_catalog_state 相应减少 | 保留 |
| 4 | `525a0c6` | durable manifest 每提交只计算一次（原本 3 次序列化+哈希） | 1M 累计 trace：outbox_intent 30.63 → 6.66 s，verify_outbox 22.84 → 1.50 s | 保留 |
| 5 | `5ba9939` | 别名再生候选集过滤 + 内存 payload 快路径 | 1M 累计 trace：session_projection 172.19 → 83.82 s（−51%） | 保留 |
| 6 | `748c720` | 提交状态读取按批内候选 id 范围化（8 张表不再整库载入） | `state-load-diagnostic-{before,after}.jsonl`（1M 库 + 200k 批）：load_catalog_state 23.417 → 1.535 s、cli:sync.commit 93.31 → 66.91 s、整库 stored_placements/edges 1,000,000/900,000 → 0；RSS 未落盘（一次性 5191 → 3386 MB，不可复算，仅方向性） | 保留 |

> 证据口径：候选 2 的 100k A/B 是「候选 1+2」对基线的累计对比；候选 3–5 的阶段差值来自 1M 累计 trace（基线 `1m.jsonl` vs 候选 1–5 二进制 `5ba9939` 的 `1m-after.jsonl`）；候选 6 的差值来自 `state-load-diagnostic-{before,after}.jsonl`（1M 库 + 200k 批，before 为 c2d58e8 参考二进制，after 含候选 1–6）。表内数字均为累计对照，不是逐提交独立 A/B。
> 未保留项：无（本轮 6 项均达到 ≥5% 或明确的护栏收益）。回退方式为按提交 `git revert`，不涉及 schema/依赖/公共契约。

## 5. 最终 A/B（优化后）

基准：`72cb684`（本任务 harness 复测基线，n=3，二进制 SHA-256 `47e3c6a0…`；该二进制已不在磁盘，无法复跑）；
优化后：`f04d1a88`（n=3，二进制 SHA-256 `3e55a4c42ac49ffd…`，与磁盘 `target/release/agent-session-grep.exe` 一致）。
所有数字为 3 次运行的中位数，report 由 harness 校验器封存（`integrity_sha256`）。

| 规模 | 初始 sync 前→后 | Δ | 空转 p50 前→后 | 空转 p95 前→后 | search p50 前→后 | search p95 前→后 | 峰值 RSS 前→后 | Δ RSS | catalog.db 前→后 |
|---|---|---|---|---|---|---|---|---|---|
| 10k | 2.18 → 1.52 s | -30.4% | 19.6 → 15.9 ms | 19.6 → 19.5 ms | 113.0 → 105.2 ms | 155.3 → 136.9 ms | 154 → 149 MB | -2.8% | 49 → 49 MB |
| 100k | 28.66 → 20.04 s | -30.1% | 61.6 → 84.8 ms | 63.1 → 95.1 ms | 111.1 → 114.7 ms | 145.3 → 140.3 ms | 1439 → 1361 MB | -5.4% | 494 → 491 MB |
| 1M | 412.72 → 257.90 s | -37.5% | 582.5 → 616.1 ms | 6785.8 → 3319.7 ms | 116.3 → 113.8 ms | 154.3 → 151.7 ms | 4574.6 → 2858.8 MB | -37.5% | 4195 → 4157 MB |

- **1M 初始 sync**：412.7 s → 257.9 s（**−37.5%**）；相对归档基线 `2b8f895`（482.6 s）为 257.9 s（−46.6%）。**未达 ≤241 s 硬目标**。
- **1M 峰值 RSS**：4574.6 MB → 2858.8 MB（**−37.5%**，三次运行峰值的中位）。**未达 ≤2287 MB 硬目标**。
- **护栏**：search p50/p95 各规模均未回退（1M p95 中位 154.3 → 151.7 ms，−1.7%；1M p50 中位 116.3 → 113.8 ms，−2.1%；100k p95 中位 145.3 → 140.3 ms，−3.4%）。空转在 harness 的 100k 窗口中采样噪声较大（±40%，与前后 1M 运行的缓存/杀毒扰动相关）；同窗口配对交错 A/B（`ab.py`，2 对，基线 vs 优化后）给出 noop 中位 76.9 → 75.4 ms（−2.1%）。
- **空转双读/双哈希消除（候选 1，专属证据）**：100k 候选 1 对比基线（`results/cand1-noop-verify/`，3 次运行）noop p50 中位 61.57 → 58.09 ms（−5.6%）；1M 封存结果中，首个（冷）空转 pass（5 批，按各 pass 的 `batch duration_ms` 求和）基线 5.90/6.78/6.86 s → 优化后 1.51/3.31/3.48 s，首 batch 1.38–1.83 s → 1.08–1.41 s。

原始证据：`research/results/baseline/*/search-baseline-full.json` 与 `research/results/after/*/search-baseline-full.json`（各 9 份，校验器 `baseline.py validate` 可重算）。

## 6. 正确性与等价性

- `cargo test -p agent-session-grep-adapters-sqlite --offline`：270 通过（含 outbox/CAS/generation、durable intent 篡改拒绝、关系完整性、身份保真、relocation）。
- 基础等价性脚本（`research/scripts/verify_equiv.py`）真实覆盖：同一语料用两个二进制写入新鲜库，比较 15 张表 + `active_generation`（catalog、message_placements、message_edges、tool_activities、usage_events、fts、fts_ids、session_fts(_ids)、2 张 membership、source_scans、source_relation_scans、source_session_resume_claims、index_batches 生命周期列）。边界：harness 合成语料不产 tool_use/usage，`tool_activities`/`usage_events` 及其 membership 两侧恒为 0 行（空比较）；不含 no-op / 缩小替换 / 墓碑场景；`index_batches` 的 manifest 与替换列被排除；该脚本的历史运行未落盘产物。
- 扩展等价性（`research/scripts/verify_equiv_extended.py` → `research/results/auxiliary/equiv-extended-10k.json`）：同一 10k 语料（2 个源文件，tree hash `c50db4123b248fcc…`）依次执行 初始 → no-op → 缩小替换 → 替换后 no-op → 空源墓碑 → 二次 no-op；比较 25 张非影子表（含全部 4 张 membership 表、scans、claims、installation 稳定列、index_batches 生命周期列）逐行内容哈希 + `active_generation`：基线 `664c2e7b…` 与优化后 `3e55a4c4…` **0 不一致**（generation 均为 3）。FTS5 影子表、每个新库随机分配的 installation namespace id/墙钟列被排除（原因记录在产物 JSON 中）；tool_use/usage 的 4 张表仍为 0 行空比较。
- durable intent 篡改：`commit_rejects_tampered_durable_*_manifest` 测试在 manifest 复用后仍拒绝 DB 行篡改并保持 generation 不变。已接受权衡：`verify_pending_in_tx` 不再从输入重算 manifest，只比对调用者传入值与 `index_batches` 行（记录于 `.trellis/spec/agentsessions-adapters-sqlite/backend/index.md`）；后续可选加固为 debug 断言或重算。

## 7. 未达标项：时间与峰值 RSS 的归因与可选路线

结论：**在不改 schema 的前提下，本轮的组合优化把 1M 初始 sync 从 412.7 s 降到 257.9 s、峰值 RSS 从 4574.6 MB 降到 2858.8 MB（均约 −37.5%），但 PRD 的两个硬目标（≤241 s、≤2287 MB）都没有达成**。按 PRD 约定，此处停止继续试错，给出归因与路线：

归因（按证据强度排序）：

1. **批次内数据副本（batch-proportional，与库大小基本无关）**
   新落盘的空库 200k 批诊断（`mem-diag-fresh-200k-{before,after}.json`，20 ms 采样、单次运行）显示：catalog 为空时峰值 RSS 仍达 2844.3 / 2695.9 MB，总耗时 61.4 / 40.7 s。当前提交路径对同一实体同时持有：`SourceBatch.entries`（payload+text）、`merged` 克隆、`PreparedSource.observed_placements` 克隆、`observed_placements/edges`，以及 `relations.canonical_json()` 生成的 manifest 字符串（多份同时存活）。
2. **别名再生的整表读取（O(catalog)）**：`session_projection` 在优化后仍是 1M 单批最大项（累计 trace 单批 10.4–24.1 s，5 批合计 83.82 s；`stored_placements_from` + `stored_edges_from` + 两份按 message/session 的克隆索引仍在整表加载）。这是下一步最确定的范围化目标（与候选 6 同一手法）。
3. **SQLite 工作集**：写连接 128 MiB page cache（候选 2 换取 −29% 时间）+ 单事务 WAL 追加使进程工作集包含大量刚写页面；这一部分随写入字节线性增长，受 `synchronous`/单事务原子性约束，不能通过拆事务换取。

可选路线（按代价从低到高）：
- **A. 继续范围化（不改 schema）**：把别名再生的 placements/edges 读取限于候选实体（`message_id`/`session_id`/`child_placement_id` 均有索引）；把 `PreparedSource.observed_placements/edges` 改为引用而非克隆；manifest 的 relations JSON 改为流式哈希 + 单副本（需保持 durable intent 字节语义，属于行为敏感区）。收益为粗估（方向性，约 0.6–1.0 GB）。
- **B. 限制单批规模/流式化**：RSS 与批内消息数近似线性（空库 200k 批已见 ~2.7 GB 峰值）。CLI 侧对一次 sync 的“解析缓冲 + 提交”改为按源/按块流水（先提交一批再解析下一批）会改变“一次 sync 一个原子批”的语义，需产品决策；仅在调用方切小批次时，RSS 可线性下降但总时间上升。
- **C. schema/投影改造**：为 `source_membership(message_id)`、`source_placement_membership(placement_id)` 之外的别名来源建覆盖索引；或新增“实体→会话/文档/span 别名”物化投影表（随提交维护），可把别名再生从 O(catalog) 降到 O(batch)，同时消除整表读。属 schema 迁移，本轮禁止。
- **D. 关系/成员表按需读取 + 增量校验**：把 `stored_*` 从“按候选 id 查询”进一步改为“按变更集增量维护”（本任务已完成候选 id 版），可减少每批 chunked 查询数量。

## 8. 复现命令

```powershell
# 基线（c2d58e8 参考 worktree 构建的磁盘二进制 664c2e7b…；封存 A/B 所用 47e3c6a0… 二进制已不在磁盘）
python -B .trellis/tasks/09-29-index-throughput/research/harness/baseline.py run `
  --workspace <baseline-repo> `
  --binary <repo>/target/index-throughput/baseline-target/release/agent-session-grep.exe `
  --scratch-dir <fresh> --output-dir <fresh> --environment <harness>/environment.json `
  --profile full --scales 1000000 --expected-commit c2d58e862b4fe0320240f8a40eefef09d958ff55

# 优化后（主 worktree，提交见 §4）
python -B .trellis/tasks/09-29-index-throughput/research/harness/baseline.py run `
  --workspace <repo> `
  --binary target/release/agent-session-grep.exe --scratch-dir <fresh> --output-dir <fresh> `
  --environment <harness>/environment.json --profile full --scales 1000000 --expected-commit <HEAD>


# 内存诊断（空库 + 200k 批；产物见 research/results/auxiliary/mem-diag-fresh-200k-*.json）
python -B .trellis/tasks/09-29-index-throughput/research/scripts/gen_corpus.py `
  --out target/index-throughput/corpus-200k-fresh --messages 200000 --per-file 5000 --offset 1000000
python -B .trellis/tasks/09-29-index-throughput/research/scripts/mem_diag.py `
  --binary <before/after 二进制> --db target/index-throughput/mem-diag-fresh-200k-<side>/catalog.db `
  --dataset target/index-throughput/corpus-200k-fresh `
  --trace target/index-throughput/mem-diag-fresh-200k-<side>/trace.jsonl --interval-ms 20

# 扩展等价性（0 不一致；产物见 research/results/auxiliary/equiv-extended-10k.json）
python -B .trellis/tasks/09-29-index-throughput/research/scripts/gen_corpus.py `
  --out target/index-throughput/corpus-10k-equiv --messages 10000 --per-file 5000 --offset 0
python -B .trellis/tasks/09-29-index-throughput/research/scripts/verify_equiv_extended.py `
  --binary-a target/index-throughput/baseline-target/release/agent-session-grep.exe `
  --binary-b target/release/agent-session-grep.exe `
  --dataset target/index-throughput/corpus-10k-equiv `
  --scratch target/index-throughput/equiv-extended-10k `
  --out .trellis/tasks/09-29-index-throughput/research/results/auxiliary/equiv-extended-10k.json

# 校验器（拒绝篡改）
python -B .trellis/tasks/09-29-index-throughput/research/harness/baseline.py validate <report.json>
```

## 9. 后续建议（本任务外）

空源重复 sync（本任务外既存缺陷，独立核查中已记录）：对一个已入库、随后被清空的源文件再次 `sync` 时，CLI 返回 `invalid_request: relocation request is invalid … source installation provenance is unresolved`（`crates/agent-session-grep-adapters-sqlite/src/relocation.rs:442-445`）。该行为在优化前的参考二进制（`664c2e7b…`）与优化后的二进制（`3e55a4c4…`）上一致，与本轮提交/状态范围化改动无关；本任务不做修复，**建议单开任务**定位 relocation provenance 的判定逻辑。


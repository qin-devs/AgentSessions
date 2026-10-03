# 初始索引吞吐与峰值内存专项

## Goal

把 1M 消息的初始索引时间与峰值 RSS 各降低至少一半（对照 `2b8f895` 实测：482.6s / 4.58GB），且 search 与空转 sync 不回退。优化必须建立在阶段级 profile 证据上，逐项验证；不允许用 schema 迁移换性能。

## Requirements

- 基线冻结：初始 sync 10k/100k/1M = 2.42s / 35.0s / 482.6s；峰值 RSS = 154MB / 1.44GB / 4.58GB；库 = 51MB / 512MB / 4.34GB；空转 p50/p95 = 22/24ms、59/79ms、646/2920ms。合并 PR #12 后先在当前 main 复测确认基线可复现（同机、无并发负载、不清系统缓存，不作为冷盘结论）。
- 硬目标：1M 初始 sync ≤ 241s 且峰值 RSS ≤ 2.29GB；10k/100k 不得回退超过噪声；search p50/p95 与空转 sync 不得回退（±10% 护栏）。
- 先证据后优化：复制研究 harness（归档路径）到本任务并把 commit pin 参数化；先产出阶段级 trace（源读取/指纹、解析 staging、catalog 写入、FTS 投影、身份 sidecar、placement/membership/关系、outbox/CAS/generation、finalize）与候选排序。
- 每个优化单独提交，A/B 实测；中位收益 <5% 或造成任何正确性/性能回归即单独回退。保留单次 sync 的原子提交语义，不接受拆分事务换速度。
- 候选（按证据排序，允许 profile 后调整顺序）：批量语句/预编译复用；整目录状态读取范围（历史上有“非瓶颈”回退记录，必须重新验证）；`serde/id_json` 复用与避免重复 payload 解析/克隆；事务内 PRAGMA 调优（不得牺牲持久性契约）；空转双读/双哈希消除。
- 禁止：schema 迁移、新依赖、公共 API/wire 变更、语句缓存落地、默认 trigram/短词 LIKE。
- 若减半目标在不改 schema 前提下不可达：停止并提交归因证据与选项（投影改造/分片/迁移）。

## Acceptance Criteria

- [x] 当前 main 复测确认基线（含语料哈希与二进制哈希）。
- [x] profile 报告给出阶段归因与候选排序（原始 trace JSON：`research/traces/1m.jsonl`、`1m-after.jsonl`）。
- [x] 硬目标判定：**未达成，按 PRD「不改 schema 不可达则停止并归因」条款执行** —— 1M 初始 sync 412.7 s → 257.9 s（-37.5%，目标 ≤241 s）、峰值 RSS 4574.6 → 2858.8 MB（-37.5%，目标 ≤2287 MB）；10k/100k 时间与内存均无回退；search 未回退（1M p95 中位 154.3 → 151.7 ms）；noop 在 ±10% 护栏内。归因与路线 A–D 见 `research/report.md` §7。
- [x] 空转双读/双哈希消除单独报告前后差值（候选 1 专属证据 `research/results/cand1-noop-verify/`：noop p50 中位 61.57 → 58.09 ms，-5.6%）。
- [x] workspace fmt/clippy/test 与 outbox/CAS/generation/关系完整性/身份测试全绿（独立核查复跑：fmt/clippy rc=0、adapters-sqlite 274 passed、workspace 1786 passed）。
- [x] 每个优化独立提交且可单独回退（6 个 perf 提交在临时 clone 中逐个 `git revert --no-commit` 均干净应用）；证据 JSON 经校验器重算且拒绝篡改（21/21 深度重算通过；改数未重封条即被拒）。
- [x] 未发生 schema/依赖/公共契约变更（diff 无根 `Cargo.toml/Cargo.lock`、无 `schemas/`、无 `pub` API 增删）。

## Verification

- 提交链：`72cb684` harness → `c2d58e8` trace+基线 → 6 个 perf 提交（`de33e75`/`490e0ba`/`fd918f1`/`525a0c6`/`5ba9939`/`748c720`）→ `f04d1a8` 测试适配 → `4ea3a2c` 证据落盘 → `1e1cef6`/`4c57d96`/`392b098` 证据补硬与报告/规范更正。
- 独立核查：verdict **PASS-with-findings**；11 条 findings 全部为非阻断，F1–F8 已在 `4c57d96`/`392b098`/`1e1cef6` 中修正（不可复算数字改为封存值或明示为方向性），F9–F11 已如实记录。
- 未达标归因（report §7）：批内数据副本（~2.8 GB/200k 批，见 `results/auxiliary/mem-diag-fresh-200k-*.json`）、别名再生仍整表读取（31.3 s/批）、SQLite 工作集随写入线性增长（受单事务/持久性约束）。路线 A 继续范围化（不改 schema）、B 小批次/流式（改原子批语义，需产品决策）、C schema/物化投影（本轮禁止）、D 关系表增量维护。
- 已接受权衡（spec 已记录，F6）：`verify_pending_in_tx` 不再从输入重算 manifest，只比对调用者传入值与 `index_batches` 行；durable intent 篡改测试仍拒绝 DB 行篡改；后续可选加固 = debug 断言 / opt-in 重算。
- 本任务外既存缺陷（F10，建议单开任务）：已记录的空源再次 sync 会报 `invalid_request: relocation request is invalid … source installation provenance is unresolved`（`crates/agent-session-grep-adapters-sqlite/src/relocation.rs:442-445`），优化前后行为一致；报告 §9 已记录。
- 复现前提：封存基线二进制 `47e3c6a0…`（`72cb684` 构建）已不在磁盘；磁盘上的参考基线为 `664c2e7b…`（`<baseline-repo>` worktree @ `c2d58e8`）。逐位复跑需重建该提交。

## Notes

- 研究测量限制：Windows 计时曾出现 15.6ms 量化，harness 已改用高精度时钟；1M 初始 sync 是分批总计。
- 历史性能线（`perf/commit-write-path`、`perf/scope-commit-reads` 等）基线落后 main 246 提交，只作为线索；其中 commit 占 sync 98.2%、一次 commit-read scoping 被实测回退，必须在当前 main 重新验证。

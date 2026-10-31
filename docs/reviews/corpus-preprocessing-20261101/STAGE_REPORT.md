# 全候选池预处理：三个 batch

状态：Batch 1 已通过第 2 轮、Batch 2 已通过第 1 轮正式审阅；即将进入 Batch 3，本 step 尚未完成。

## 固定范围与权限

用户指令：按照 batch-dev-review Skill，将已保存候选池内的 scope 论文处理分成三个 batch 并完成。LLM API 不调用；筛选、提取、语义判断由本会话 Agent 完成，现有代码负责校验、SQLite 写入、dense+sparse 候选与增量索引。主库、原始来源及旧提取保留；原始数据不进入公开 Git。没有 main 合并授权。

- 仓库：cao8011158/kg-builder；分支 work/retrieval-pilot-20261031。
- 基线完整 SHA：24c56654b27b4e7118beb9e8c7a8474ab83f2657；合并目标 main（仅记录，不合并）。
- 数据基线：365 篇、46 条提取（其中 6 篇为已完成精简试验）；340 篇有原始摘要、25 篇缺摘要。
- 已有语义库：26 个共享节点、37 个描述关联、6 条贡献。
- 时间：首次公开 2025-01-01 起；领域 agent_memory、context_management、skill_evolution。一般 tool use、RL 后训练、泛化的递归自改进不独立纳入，实质交叉相关才纳入。
- 按已保存日期、标题、ID 确定排序并固定不重叠清单。Batch 1：122 篇（含复用 6）；Batch 2：122；Batch 3：121。
- 每批均执行：逐篇筛选（include/exclude/pending 与理由）→新纳入论文四字段提取→结构／版本／原文覆盖检查→候选检索与明确语义审阅→增量入库／索引→独立检查与保存检查点。
- 原始摘要可支持摘要级提取，缺失原文或信息不确定时明确标 pending，不补造 benchmark 或数字。source_scope 如实区分 abstract_only_metadata_snapshot 与正文选定章节。
- 必要时补查缺失来源，不重新开展无限扩张采集。Scope 之外记录保留而排除本次处理。
- 每批最多三轮正式审阅；本 step 问题单独编号，不重置既有试验 step 的审阅记录。

## 验收与角色

开发／数据提取子 Agent 使用 GPT-6 SOL / medium；主 Agent 固定范围、独立语义及结构检查、保存与裁决。每批检查所有清单覆盖、字段合法性、源／版本对应、已完成六篇复用、重复导入幂等与原始数据保留，复跑快速回归；最终增加真实当前规模查询、恢复和必要局部性对照。测试不替代科学结论验证。

## 阶段 0：基线核对与清单固定

已读取当前 scope 和执行契约，仓库工作区干净；目标仓库未发现 AGENTS.md。当前保存主库 metadata 与上轮一致；从保存位置下载的哈希与本地基线相符后才修改工作副本。每批数据清单和 source hashes 保存在私有任务目录，并随检查点保存。

## Batch 1 — 开发／提取完成（未写入主库）

固定清单 122/122 已检查。两个提取子 Agent（GPT-6 SOL / medium）报告合计 55 include、64 exclude、3 pending；其中 6 篇复用，49 篇新增精简提取。两个分片分别完成 schema、清单覆盖、ID／版本／source URL 检查；根目录快速回归 56/56 通过。原主库与 sidecar 尚未修改，独立 scope 检查正在对照。

实际依据以已保存摘要为主；Zep、A²SE、SkillOS 的缺失摘要已从实际主来源补查，声明 primary_source_abstract_review，没有把重述写成原始摘要。源证据不足的三条保留 pending，正在补查，不计入已提取。API 默认关闭，未运行付费调用。下一步：固定数据哈希，核对独立 scope 分歧，然后候选检索与明确语义审阅。

## Batch 1 — 修正与字段复核完成

补查后两个初始 pending（CCP Lab 与 SAGE）获得了可读主来源。独立 scope 检查全部 122 条后，Layered Mutability 因直接研究记忆积累的可逆性与行为漂移而纳入；当前最终筛选为 58 include、63 exclude、1 pending。6 篇复用，新增精简提取 52 篇。NSP-05 主来源不可访问，保留 pending，未补造内容。

独立字段复核逐条检查 52 篇，纠正 benchmark／taxonomy 被误建为 Method 的标签；Graphiti 从“提出”改为“使用”；Mem-alpha validation set 与 MemoryBank evaluation data 明确为评测资源，LME 不与 LongMemEval／LOCCO 强行统一。补读材料如实声明 primary_source_abstract_review 或 full_text_selected_sections，未把 Agent 重述冒充原始摘要。

新增确定性 apply_corpus_batch.py（开发者 corpus_batch_runner，GPT-6 SOL / medium）校验整批覆盖、版本 pin、复用和增量导入；默认只读校验。实际输入反馈修正了额外 version_status 字段与 exclude/pending 空主题列表的兼容，6 项定向／62 项全仓回归通过。主库事务原子；sidecar 为独立、可重试阶段，不声称跨库分布式原子。

实际校验还发现 Acon 的“最佳可用摘要快照”为未确认版本，而先前已审阅的是 v1。已将复用输入 pin 固定到已审阅 v1，保留元数据摘要来源；没有重写或覆盖旧提取。导入工作副本成功：52 新提取、122 条新 scope 审阅；sidecar 增加 52 个来源。原主库未覆盖，已先保存恢复备份。当前执行逐条语义候选审阅，尚未宣布 Batch 1 验收通过。

## Batch 1 — 语义整理完成，准备正式审阅

数据实现者 corpus_resolution（GPT-6 SOL / medium）完成 9 个候选包、162 条明确语义判断；48 条匹配既有节点，114 条新建归属指向 113 个新实体，没有未处理描述。语义库现为 58 个来源、199 条描述关联、139 个共享节点（46 个问题、60 个方法、33 个 benchmark）。原先七组未重新聚类；新方向按检索候选与语义判断增量建立。

SAGE、CAT、SWE-Compressor、AgentSwing 的预印本／正式出版描述按具体机制关联同一 Method，未仅凭标题合并 Paper。LME 保留独立、窄定义的临时评测身份，未推断不可靠别名。所有候选包、审阅决定与输入数据保存在私有任务目录；公开报告只保留计数和哈希。

下一步固定提交与 batch1-data-receipt.json，独立复跑当前 62 项测试，并验证 122 条筛选覆盖、52 条源／版本 pin、旧 46 条提取逐字节保留、关键共享实体、重复导入幂等和当前规模查询。尚未宣布正式 review 通过。

## Batch 1 — 第 1/3 轮正式独立审阅：通过

固定远端提交 `e20e63698d80b16e5ba6c0edd2f487f444e11aa4`，已取回并核对本地内容一致。当前快速回归 62/62 通过；独立检查 122 条筛选与固定清单完全一致、58 include／63 exclude／1 pending，52 个新提取符合 v0.3。原有 46 条 extraction payload 逐字节未变；重复执行结果 master_new=0、sidecar_imported=0。

语义库独立核对：58 来源、199 描述关联、139 实体，其中 46 问题／60 方法／33 benchmark。SAGE、CAT、AgentSwing 均跨两份来源关联同一 Method；SAGE、LoCoMo、CAT 的实际混合检索首项正确。实际 SQLite 计划通过 mentions_entity 索引定位相关描述，外键和 integrity_check 均通过。没有确认未解决的阻塞缺陷；NSP-05 因主来源不可读仍 pending，不计为已提取。

已保存完整批次检查点（含主库／sidecar 一致备份、输入、所有语义审阅和哈希清单），恢复包 SHA-256 为 `aff27ce39d8cbebc2d74fb5f479e8a81e2df5c2664dc7e3e400d7d8a1fbee4a7`。证据：logs/batch1-fixed-head-regression.log、logs/batch1-independent-data-audit.log、logs/batch1-repeat.log。接下来进入 Batch 2 的 122 条；不重做已处理数据，不重建全量问题分组。

合并 gate 已实际执行：唯一 BLOCKED 原因为当前任务没有 main 合并授权；没有测试、日志或审阅缺失。该 gate 不执行合并，本轮继续已授权数据处理，不把 BLOCKED 伪称 READY。

## Batch 2 — 已启动

独立固定清单 122 条（part1/part2 各 61），没有复用项；两个提取子 Agent 与独立 scope 检查并行阅读原始摘要。沿用 Batch 1 的分类修正，提取版本为 2026-11-01-corpus-batch2-v1；当前尚未写入本批主库或语义数据。

## Batch 2 — 两个分片开发完成

part1 开发者 corpus_b1_part1 检查 61 条，36 include／25 exclude；part2 开发者 corpus_b1_part2 检查 61 条，34 include／27 exclude。当前合计 70 include、52 exclude、0 pending，无复用，70 份新提取已通过开发者 schema、清单覆盖、版本和 source pin 自检。两个分片均未直接修改数据库或生产代码。

补查来源用于 EASE、AutoCompact 及两篇 skill survey；如实区分 primary_source_abstract_review 与原始摘要快照。独立 Agent 先盲读全部 122 条进行 scope 判定，随后复核字段，重点裁决泛 RSI、长期视频记忆与可复用技能的边界。本阶段尚未写入 Batch 2 数据，也尚未正式验收。

## Batch 1 — 补查来源后的追加裁决（第 2 轮准备）

在后续有界来源补查中，NSP-05 的 SSRN 官方索引摘要变得可读；直接正文／PDF 仍被拒绝。来源 Agent、独立 scope Agent 与主 Agent 均确认其为通用 NEXUS 双曲记忆理论及模拟，没有建立 LLM agent 的实质关联，因此由 pending 裁决为 exclude。原 pending 审阅不覆盖，新增一条明确 supersedes_review_id 的来源裁决；四字段提取和语义节点均无新增或变化。Batch 1 当前有效结果为 58 include／64 exclude／0 pending。

依赖该批的 Batch 2 入库暂缓，先固定来源补充证据和当前提交，按同一 Batch 1 历史执行第 2/3 轮审阅，不重置轮次。新增独立数据审计脚本检查全部原始元数据、旧提取 payload、每个来源及 item hash、归属类型和真实查询。审计脚本自测去除了超出实际契约的旧节点别名断言：既有节点允许显式语义匹配新表述，不要求后续每个表述均改写为 canonical alias；这不涉及生产数据修正。

## Batch 1 — 第 2/3 轮正式独立审阅：通过

固定远端提交 `95eb1f95fa9a06621ab0f39302a9dfed1a5346e8`，本地与远端 tree 一致。62 项快速回归通过；独立审计核对全部原始 papers／paper_versions 及旧 46 个 payload 不变，199 条 mention 的原始 item 与 hash、来源指纹和实体类型一致，SAGE／CAT／LoCoMo 混合检索首项正确，实体明细 limit=1 实际只读 3 行。追加来源裁决与原 pending 行均存在且互相明确引用。Batch 1 有效结果 58 include／64 exclude／0 pending；52 新提取及全部语义结果保持不变。没有未解决阻塞，继续 Batch 2。

## Batch 2 — 范围裁决与字段修正完成

独立盲读 122 条后，与开发者原始判断逐项对照：MedRSI 因持续构造、验证并注册可复用临床能力而纳入；CARL 因跨迭代维护压缩决策上下文而纳入；选择性记忆 survey 在实际读取第 5 节和 6.4 节后确认实质覆盖 agent memory／trajectory compaction，标 full_text_selected_sections。Twin Agent 仅压缩权限分离的通信提示而未管理累积历史，仍排除；FocusGUI 为局部感知适配器，未建立长期历史管理，排除。Zenodo 泛化 skill 文章缺少 LLM 特定证据且主文不可取得，保留 pending。

两个开发者保留 pre-independent／pre-field 文件后修正：MemGUI 从“已发布”改为“构造”数据；ACM 明确 offload 被丢弃的内容；EVAF 不再主张已证实为本论文提出；TGVC／Skill-GNN 标 involved；加入原摘要明确使用的 Memora event streams 诊断资源。独立 Agent 复核全部当前 71 份提取，确认 6 处字段修正及新增 3 份提取，没有未解决字段问题。最终本批 71 include／50 exclude／1 pending，71 新提取；尚未正式验收。

## Batch 2 — 语义整理与提交前验证完成

corpus_resolution 完成 11 个候选包、213 条明确判断：56 条匹配，157 条新建归属对应 156 个新 canonical 实体，0 pending。当前语义库共 129 来源／412 描述关联／295 实体。保留 FOCUS 与旧 Focus 的不同机制身份，也区分不同 SWE、Mind2Web 评测对象；LongMemEval-S 的原始子集保留在 mention。所有决定、候选和来源指纹均留在私有证据包。

corpus_batch_runner 在实际提交 95eb1f95fa9a06621ab0f39302a9dfed1a5346e8 上复跑 6 项定向和 62 项快速全仓测试，均 exit 0；原始日志已收录当前仓库 evidence/logs。当前还需固定远端新提交及 batch2-data-receipt.json，再独立检查累计数据、幂等、当前规模检索和恢复检查点。本批尚未宣告正式 review 通过。

## Batch 2 — 第 1/3 轮正式独立审阅：通过

固定远端提交 `9ac0a1184f687837cd1bd9380a967c0c50ff0a74`，取回并核对 tree；main 实际仍为 fc114851d5017aed7c8d5477b58b2ff8dac6bbfa。开发者与主 Agent 分别复跑该提交 62 项全仓测试，另 6 项定向通过。独立 scope、71 份字段及 213/213 语义决定均完成；未发现已确认误合并或明显漏合并。

独立累计审计覆盖 244 条清单，原始 papers／paper_versions 和旧 46 个 extraction payload 不变；412 个 mention 原始 item／hash／来源指纹及归属类型一致。语义库 129 来源、412 描述、295 节点（98 问题／120 方法／77 benchmark）；SAGE 三份来源关联同一方法。SAGE／CAT／LoCoMo 的实际混合查询和索引明细读取通过；重复导入 master_new=0、sidecar_imported=0。

检查点解包后逐文件 hash 及四个 SQLite integrity_check 均通过，ZIP SHA-256 `70fba7bed2b8c23a57986b5ddb2f499712574fe8ca845fc5e0b20afc368554d6`，已保存。合并 gate 唯一 BLOCKED 项仍是未授权 main 合并，不影响本轮已授权数据处理。有效本批结果 71 include／50 exclude／1 pending，进入最后 Batch 3 的 121 条。

## Batch 3 — 已启动

最后 121 条固定清单分为 61／60 两片；两个开发者与独立盲读 Agent 已开始逐篇检查。前两批已验收，不重新执行；仍只在工作副本追加数据。最后一批完成后执行整池 365 条覆盖、恢复、真实当前规模和无关数据扩容局部性验收，再更新既有保存文件。

## 补充功能探针（Batch 2 当前规模，无新增正式轮次）

在 129 来源／295 实体的真实副本上，贡献混合检索重复结果一致，热调用新增向量计算和索引构建均为 0。LoCoMo 明细真实读取 5 行／1353 字节、113 个 SQLite VM 操作；加入 500 个无关实体及约 10MB 文本后，读取行数、字节、返回体与 VM 操作均不变，实际计划无全表 SCAN。该探针只使用一次性 SQLite 备份，未污染正式 sidecar。最终将以完整 Batch 3 后的数据再执行功能验收。

## Batch 3 — 两个分片开发完成

part1 检查 61 条，28 include／33 exclude，28 新提取；part2 检查 60 条，17 include／42 exclude／1 pending，17 新提取。开发者分别完成全部清单覆盖、v0.3 schema、主题及精确版本/source pin 自检。合计当前 45 include／75 exclude／1 pending，尚未入库或正式验收。

Research Square compaction-pay 仍只有标题且来源不可访问，保留 pending。Hebbian／Kairos 从实际 primary PDF 选定章节提取；本地存量年份为 2025，CEUR 相关发表版可核实为 2026-02-07，但最早公开具体日期和 OpenReview／CEUR 版本等同关系未证实，review 明确 stored_year_unverified_first_public_date，原始 first_published 仍为空，未补造日期或重写原文。主 Agent 实际打开 CEUR PDF 验证标题、作者和 LLM agent 记忆机制支持，独立复核继续检查全部 121 条与 45 份字段。

## Batch 3 — 范围裁决、字段独立复核与提交前验证完成

独立盲读全部 121 条后，主 Agent 对照原始摘要逐项裁决。排除新增误纳入的 restart-monitor（压缩仅为决策充分性的例子）、world-model compaction（未建立实际 LLM agent 角色）与 Traversal（正文实验为 Transformer-XL／PPO MiniGrid）。保留原有 PREreview、路由蒸馏、critic RL、静态技能攻击及通用视频对话 QA 排除，未仅凭 compaction／skill／memory 词汇放宽范围。两片原始结果及修正结果分开保存。

当前有效 42 include／78 exclude／1 pending；42 新提取。独立 Agent 已检查原先全部 45 份字段，移除三条 out-of-scope 后的全部 42 份无未解决字段问题；确认命名方法、评测资源角色和组件 involved 标记。corpus_batch_runner 在实际 HEAD 9ac0a1184f687837cd1bd9380a967c0c50ff0a74 复跑 6 项定向与 62 项全仓回归均 exit 0。下一步入库及增量语义整理，尚未正式验收。

## Batch 3 — 语义整理完成，准备最终正式审阅

corpus_resolution 完成 7 个候选包、126 条判断：28 条匹配，98 个新实体，0 pending。当前完整语义库 171 个来源、538 个源描述关联、393 个实体（132 ResearchProblem、157 Method、104 Benchmark）、171 条论文内贡献。Kairos 的方法节点保留实际来源说明，没有推断论文版本或发布日期。Terminal-Bench 等明确编号发布版保持区分。

主 Agent 已读取全部本批目标归属及定义，当前未确认误合并；完整预审检查覆盖 365 个不重叠候选、165 新提取与 6 复用、全部旧主库行保留、所有 mention 原始 item／hash／source fingerprint 和实体类型、真实混合查询及明细索引，均通过。已导出 365 条有效筛选、171 份精简提取和共享实体 JSON。当前有效总计 171 include／192 exclude／2 pending；尚需固定远端新 head 后正式复跑、完整规模／恢复及持久化验收。

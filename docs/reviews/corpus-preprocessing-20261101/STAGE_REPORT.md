# 全候选池预处理：三个 batch

状态：Batch 1 开始；本 step 尚未完成。

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

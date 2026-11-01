# 建图规则修订与46篇复测

状态：固定需求与基线，开发中。正式review尚未开始，真实API尚未调用。

## 范围与授权

- 用户授权更新建图代码，使用上次已清洗/提取/审核的数据重新建图并测试；测试结果必须报告，新发现问题先商量再决定处理，不自动扩大修复。
- 仓库cao8011158/kg-builder；分支work/retrieval-pilot-20261031；base e873c43c0bc022ba43daee257813d43878577fe2，tree c63023c088262ac7fef0d23bb4412533f8f922a3；main目标fc114851d5017aed7c8d5477b58b2ff8dac6bbfa；无main合并授权。无适用AGENTS.md。
- 输入固定private/parallel60-20261101/preparation/prepared.json；46篇/346批准条目，hash65015c638bb028e74139e248abdfbb391fb8875f3beddb0d5fc47229c4700b41。旧图和源数据只读；新图在private/graph-policy-20261101/graph。
- 阶段A：代码与定向/全仓、独立review。阶段B：新图实测、同15检索问题/原身份样本分层对照、新领域抽样。阶段C：保存与恢复及结果/问题交付。不再次采集、scope、抽取或字段审核。
- 生产实现由开发Agent负责；root只写契约/报告并独立review。按skill使用GPT-6 SOL/medium独立上下文。Jev默认4并行，每请求有界批量；生成式命名仍由外部Agent并行，仓库保留API路径。

## 已决定的行为

1. research_problems保留每条原始具体问题节点，连接原论文；不使用同一性判断归并它们。Leiden实际形成候选领域群，LLM生成领域名称/定义。孤立问题仍可搜索，不强行建立单项领域。
2. Jev逐成员检查领域归属，仅明确“不属于”且confidence严格>0.7才移出领域；其他结果保留。移出不是删除问题，不整组冻结。
3. methods/benchmarks沿用全量各自HNSW近邻→去重候选对→Jev同一性（原0.7门槛）→same关系组。第二轮改为逐成员排除：仅明确不是同一实体且confidence>0.7移出。其余保留。移出/落单项建立单独实体，可检索。
4. 取消pending候选边阻塞整个实体组；保留未决关系及原始审计。不同关系不强行合并；若新图发现same链与明确different冲突，报告给用户决定，不静默丢边。
5. ResearchArea与ResearchProblem不同层级；领域有成员连接和来源扩展，问题原文不被领域总结替换。所有已批准条目必须在可检索节点中保留。
6. 新建policy domain-v1固定到job，旧job按legacy恢复，不原地改写旧图。源hash/version、lease、审计、并发统计、完成时间和缓存复用继续有效。
7. 索引完成标记缺失的历史观察不在本轮主动改根因范围；本轮关闭重开验收如再次出现先保存证据、报告讨论。

## 基线与已知反例

旧图结构通过但检索Hit@5 8/15、身份25标签中7resolved/18pending，85/229mentions待定；186组Jev通过但44组被pending边连带拦截。WebShop/LongMemEval-S整组低confidence同样阻塞。旧图唯一same链内部different冲突属于ResearchProblem（Long agent histories vs Reasoning-history growth），本轮改为领域而不要求具体问题相同；Method/Benchmark旧图无此冲突。

## 验证计划

- strict>0.7边界、所有非排除结果保留、待定边不冻结、离群项单独实体、RP逐条与领域分离、Leiden实际成员被消费；字段/来源/版本不变量。
- 离线API乱序/并发、外部命名claim/submit、旧job恢复/新policy固定、失败恢复、已完成重放；定向+全仓。
- 固定远端head独立review最多3轮；真实46篇图计时与阶段/API数/并发峰值、冻结参考样本对照（不把RP领域共组误算实体同一）；新增问题不擅自修复。
- 新图结构/来源/可检索覆盖、领域成员排除证据；整包SQLite backup、上传下载hash、干净目录无API重放及查询。代码与报告走既有Git/草稿PR，数据走既有Drive项目。


## 基线固定完成

原113项全仓通过，7.593秒，exit0，baseline-full.log。远端PR1 head e873c43c0bc022ba43daee257813d43878577fe2，main fc114851d5017aed7c8d5477b58b2ff8dac6bbfa，draft且未合并。6个原输入/数据库/参考案例哈希保存在新任务historical-hashes.json。

复用旧快照向量的Leiden结果为59个问题、3群（57/1/1）。用户未授权调resolution等参数，保持旧参数并真实测试，不擅自划小群或把粗粒度现象隐藏。三名开发Agent分工：store/API、graph orchestration、validator，均GPT-6 SOL/medium独立上下文；共享DESIGN.md接口，生产文件不交叉写。


## 存储/API本地开发完成（未提交）

开发者graph_policy_store，GPT-6 SOL/medium。semantic_store.py新增ResearchArea及成员/FTS表、严格来源RP成员导入、领域详情与dense+sparse检索、默认检索含领域、缓存索引维护；decision_api.py新增review_group_members，按membership_id关联批量答案，明确not_belongs且confidence>0.7才排除。源条目/现有实体来源约束保持。

开发定向：decision15项0.273秒、semantic10项1.512秒通过；完整测试仍待三组件冻结统一运行。领域详情真实EXPLAIN使用area-members主键后mention主键，公开get_entity冷/热/新增1000无关大字段区域后SQLite VM均69，2行、605B响应；FTS有索引，温搜索VM207→233、响应约400B。首次dense全索引18299VM为有意初始化。日志store-decision.log/store-semantic.log。没有真实API。root尚未完成独立review。

已记录限制：未来源替换后区域摘要的增量失效/重新聚类尚未设计；外键可移除失效成员，但摘要可能需重算。本轮冻结快照不涉及源替换，不擅自扩大范围。

## 建图首稿与冻结前顺序纠正（仍在开发，未提交）

开发者graph_policy_graph，GPT-6 SOL/medium。首稿加入domain-v1/旧joblegacy、MB近邻候选、Leiden实际群组、逐成员审核/独立RP节点/区域链接、移出项singleton、待定边不冻结及冲突报告。13项定向1.4秒通过，graph-snapshot-tests.log。

root首稿检查发现RP逐成员审核在LLM生成区域定义之前发生，使用了第一条问题描述，不符合已定顺序。已明确改为Leiden群→全体原成员命名→对生成区域逐成员Jev审阅→物化；单项RP无区域无需区域审核。正在补测试验证审核实际收到LLM定义，claim/submit仍绑定原始成员。此为开发冻结前纠正，不计正式review轮次，不改变用户策略。


## 建图顺序纠正和单项边界完成（未提交）

生成区域名称/定义后才调用_review_area_members，审核保存area_revision；命名任务始终绑定原始完整Leiden成员，成员审阅后才物化保留链接。RP单项不造区域、不伪造Jev结果，保留独立节点。14项图定向1.99秒通过。显式same链/different矛盾保存identity_conflicts并停止物化，等待用户决定，不自动丢弃判断。

## 验收工具开发完成，准备冻结（未提交）

graph_policy_validation，GPT-6 SOL/medium。新增domain-v1结构分支，逐一核验源条目、原始RP节点、实体保留/移出singleton、严格排除证据与生成区域revision、Leiden实际分区、命名任务、区域链接和FTS；保留旧job审阅。旧研究问题的3个有标签身份案例不再用单项实体是否相等评分，另报not_applicable_new_semantics；22个Method/Benchmark明确标签仍可对照，5个unknown保持未计分。完整15源检索问题仍可比较。

冻结前root指出验证器逐mention扫描全部预期实体与complete_resolution忽略pending关系，已改为一次构建mention→entity映射，并把源节点可用与关系全部确定分别报告。开发全仓pytest127项、56subtests通过（14.50秒，validator-full.log）。此前store-full.log在共享树演进中126项有2处验证夹具失败，已修正，保留原日志，不计正式review。尚未真实Jev调用、尚未正式review。

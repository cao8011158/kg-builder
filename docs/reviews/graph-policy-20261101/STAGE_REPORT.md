# 建图规则修订与46篇复测

状态：代码独立review第1轮通过；46篇重建、结构/效果测试及下载恢复完成。保留3处漏归一、领域过宽等问题供用户讨论；未继续改策略，未合并main。

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


## 正式独立review第1轮 — 通过（含1项非阻塞接口限制）

固定远端3995129e8a71e38cd9d7c56d2fd2fe610c8faf89，tree4a8693b5753b71443059d3f96ec88273a72db242；已取回并与本地核对一致，主Agent未改生产实现。审阅完整base e873c43..head差异、实际共享helper和CLI消费者；127项unittest11.525秒通过，review-r1-full.log。开发阶段pytest127/56subtests不重复计作独立证据。

独立3篇贯通夹具通过，0.350秒（review-r1-integration.log）：真实decision批量helper/graph run/claim/submit/validator链，Method负向0.7保留、0.700001单条移出；RP负向0.9只移领域链接且原节点保留；belongs低confidence及insufficient高confidence均保留。3条benchmark pending候选边不影响3独立节点检索；LLM命名前无区域审核，命名后审阅实际生成定义。所有5类冷索引复用向量、终态重放0API、completion不变；源节点全可用但关系未全确定正确分报。

G-P3-CLI：semantic_cli.py search --kind枚举尚不含ResearchArea。Python search(kind='ResearchArea')及无kind默认混合检索已支持，区域详情也可查；因此不阻塞本轮按现有API建图/验收，但命令行无法单独过滤区域。root未擅自更改，已向用户报告，后续是否补选项留待讨论。增量源替换后区域摘要重算同样保留为范围外限制。无确认P0/P1、无阻塞代码缺陷；进入46篇真实重建，不合并main。


## 46篇真实重建与首次效果测试完成

代码3995129e8a71e38cd9d7c56d2fd2fe610c8faf89，输入hash与原快照一致。完整建图409.677秒（6分50秒），旧图851.879秒；观察wall缩短51.9%，但本轮省去研究问题身份候选判断且命名任务30→1，API延迟/新判断也不同，不能当纯并行加速比。2371个MB候选：2317different/26same/28pending，未发现same链内部different冲突。153请求/153HTTP、无重试，实测峰值4，HTTP累计1196.944秒、并集359.635秒。

候选对wall327.934秒，MB逐成员24.196秒，实际区域逐成员8.407秒，初次索引1.690秒。命名任务排队45.504秒单列为交接开销；0.331秒领取至提交并非完整LLM推理耗时，区域摘要已提前阅读原57成员准备。全部5类索引验证，复用328缓存向量、仅新区域摘要计算1条向量。关闭后只读重开完成标记仍在，未重现旧异常；不据此声称已查明旧根因。

346批准项全部保留：229语义mentions全部进入211实体（含59独立原始RP），另117贡献；1ResearchArea连接57问题，2单项RP没有强造领域。227成员审阅（MB170+域57）全部保留，实际排除0；严格边界/排除分支由独立故障夹具验证，不冒称本次真实发生排除。没有待定节点或待定整组；28身份候选关系仍待定，complete_resolution=false，而all_source_nodes_available=true。

首次结构验收0失败。冻结15问题Hit@5由8/15到15/15，MRR0.48→0.83；RP2/5→5/5、Method5/5→5/5、Benchmark1/5→5/5。验证器计时1.442秒，细分排名和来源诊断另计comparison.json。22个相同MB明确参考案例，旧7符合/15pending，本次19符合/3漏归一；3个MB未知标签不计分。原RP3个明确身份标签不再适用，另外2个RP未知保持未计分，不能用领域共组当实体相同。所有参考为原先Agent源文本代理标签，不是人工金标准。

## 新测试问题：保留结果，等待用户讨论，不自动修正

G-SEM-01：MemGallery参考对仍分开。候选已召回，Jev倾向same但confidence0.24，故第一轮pending，两条分别建立可检索节点。

G-SEM-02：LongMemEval-S参考对仍分开。候选已召回，same confidence0.40，第一轮pending；节点可搜但未归一。

G-SEM-03：SWE-bench Verified参考对未进入HNSW候选。两条描述分别强调目标编码benchmark和其中19个SymPy问题子集；在现有k20下未召回，因此没交Jev比较。没有私自加名称匹配/改k/改门槛。

这3项是漏合并，不是Jev明确判不同或错误合并。效验器wrong3表示最终实体划分与参考不符，不能解读成3个高置信错判；28待定关系仍有审计。冻结样本未发现误合并，不保证全图无误。

G-SEM-04：Leiden默认参数实际57/1/1，仅生成1个宽领域“Reliable memory, context and skill adaptation for long-running LLM agents”；具体问题仍各自保留。领域全覆盖只能证明功能，不证明细粒度分类有效。未擅自调resolution、分层或拆组。与G-P3-CLI及未来增量摘要失效限制一并交用户讨论。继续仅做保存/下载恢复验证，不改语义策略。


## 保存与下载恢复验收完成

[完整新图检查点](https://drive.google.com/file/d/19bmgFIZCKZ_gxsZYg8wf7r9VVHZgA8vM/view?usp=drivesdk)，3,682,710字节，SHA256 a6944d74198952c4a252b3179732b0883121b5fe25805bca4dcd95dacd3c03f8。SQLite backup API导出新图3数据库，含固定输入、原案例、所有判断审计、领域命名提交、参数/计时、结构与新问题诊断；无凭据或模型权重，密钥扫描0匹配。

重新下载SHA一致、27个manifest文件逐一核验；新目录3库quick_check正常/FK无违规。SnapshotGraph终态重放无API、原completed_at不变、153历史请求和峰值4保持；5类冷索引缓存329命中、重算节点向量0。三类原实体名称查询rank1；领域详情57原成员、CLI默认混合搜索含ResearchArea。完整验收JSON与初次逐字段相同。下载后恢复检查5.546秒，restore-acceptance.log。原6个输入/数据库/参考文件hash保持。未重现完成标记丢失，不声称查明历史根因。

实际效果测试1.442秒、详细排名诊断0.568秒，与建图409.677秒和开发/review开销分开。原15问题逐项排名在evidence/comparison.json，无换题、无调整预期论文。新增区域只做名称查找/成员来源功能检查，没有独立人工领域金标准；57条一大组的细粒度实用性需用户讨论，不能用15/15原问题检索代替领域分类质量。

## 当前结论与待讨论项

已落实用户决定的规则并完成相同输入复测：节点可检索覆盖恢复、关系不确定不再冻结整组，检索代理样本8/15→15/15。Method/Benchmark22明确身份案例19符合、3漏归一（MemGallery、LongMemEval-S、SWE-bench Verified），未发现代理样本误合并；全图仍28未决候选关系。实际无高置信离群项被移出，删除边界由独立夹具证明。

后续不自动处理：①是否为同名benchmark补充身份来源/候选召回，②如何处理第一轮低置信同一性，③领域是否需要更细聚类，④CLI区域筛选选项、未来增量区域摘要失效。报告可选方向不代表已实施。先与用户讨论，再决定处理办法。


证据门禁对代码head3995129e8a71e38cd9d7c56d2fd2fe610c8faf89核验日志/轮次一致；仅因没有当前任务main合并授权而BLOCKED。不把此状态说成测试失败，也不请求额外合并。本轮保持草稿PR，后续证据提交完成后核对最终远端head并补充固定提交验证。

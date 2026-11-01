# 并发与条目级审核：全新60篇测试

状态：代码独立review通过；全新60条源记录准备与46篇建图已完成；结构验收通过，语义效果不足。下载恢复验收通过，记录交付证据；未合并main。

## 固定范围

- 用户最新授权：补齐Jev和生成式LLM的并发调度、条目级两次审核规则；从头采集清洗60候选，重新提取/审查/建图并验证效果。此前47篇回放方案已由本轮全新60篇替代。
- 基线01fd0078afd326247a47cc16a561b9c16922fa13，tree103c59ff267c1ff209faa9ea6bdb3fd01083bdc1，仓库cao8011158/kg-builder，分支work/retrieval-pilot-20261031；main目标fc114851d5017aed7c8d5477b58b2ff8dac6bbfa，草稿PR1；没有main合并授权。原工作树干净，祖先无适用AGENTS.md。
- 分三个验收阶段：A代码开发/独立review；B全新60篇准备和独立图实测；C图来源/归并/检索抽样与恢复验收。A含API、准备队列、图任务三个代码组件。每batch最多3轮正式review，不重置旧任务历史。
- 保留0.7 confidence门槛；初次条目审核未过，仅修正该条目一次，再未过则剔除下游，保留审计。已通过条目不可随修正稿被重写；完整性题结果只记录，不触发改稿/整篇拦截。四数组任一有通过条目即可进入下游；全空只保留源记录。
- 清洗与ID/版本/来源校验仍由代码；Jev用于scope和语义审查；本轮生成式工作仍由外部Agent并行完成，仓库必须保留并验证API并发路径。真实Jev仅由root调用。
- 新目录private/parallel60-20261101；保留原365库、原60篇测试库及所有旧稿，不复用原提取/判断。只使用OpenAlex/arXiv。凭据仅从任务目录外owner-only文件注入，日志/报告/上传无密钥。
- 并发总量有界，默认Jev4、LLM3；独立stage可重叠，状态领取原子化、版本pin/租约/恢复/首次完成时间不变。旧运行依原policy读取，不静默改写历史。
- 对比旧测试718.613秒准备、295.603秒11篇图、11ready/44pending/5excluded；本轮样本/规则同时改变，不能把全部wall差异归因并行。记录实际并发峰值、API时间并集/累计、排队和外部handoff。

## 测试与验收

基线独立archive运行87项全仓；开发定向并发上限/真实重叠/乱序对应/失败隔离、条目stable key及两次上限、已通过条目不可改、全空和部分有效、租约恢复与终态重放；固定远端head后独立全仓/集成/locality验证。新60篇通过同一workflow领取提交，独立冻结后建图；图验收含结构与源追溯、身份/检索样本效果、明确pending及漏合并，上传下载后异地恢复。无人工标注基准时不声称普遍准确率。

## API组件首轮开发完成（未提交）

实现者parallel_decision_api，GPT-6 SOL/medium。新增共享Jev HTTP上限4、并发dispatch及乱序结果关联、线程隔离request审计、失败request_audit/request_audits、逐条目支持性与独立完整性适配器。冻结前root检查发现请求计时混入semaphore排队，已修正为HTTP耗时与queue分开，并新增每次HTTP attempt时间区间，避免重试backoff虚增并发峰值。14项定向通过（api-developer-target.log）；该开发者较早全仓执行在准备组件演进时停滞并中断exit130，api-developer-full.log只作为不完整历史，不当作通过。无真实API调用。完整全仓由整批稳定后统一独立复跑。

## 图组件首轮开发完成（未提交）

实现者parallel_graph，GPT-6 SOL/medium。生成式API命名有界并发，HTTP线程不访问SQLite；主线程claim/submit且仅领取当前并发容量，失败释放本次自己的lease。新快照校验approved key和精确payload hash，保留source/status/restore约束。Jev并发继承共享API helper，HTTP attempt区间落库，区别logical interval与真实HTTP peak/union。9项定向、当时103项全仓通过（13.6秒），graph-developer-target/full.log。尚无真实图验收；原k/门槛/语义分组规则未改，pending不强制放行。

## 准备组件首轮开发与冻结前修正（未提交）

实现者item_preparation，GPT-6 SOL/medium。NEW item-v1逐条审核：最多2轮支持性审查（初稿+一次修正），冻结已通过payload，稳定key/hash匹配，修正只处理请求条目；失败终态dropped、全无可用条目no_usable_items、过滤输出有独立派生身份。原policy旧运行保留legacy-paper。Scope/review分别有工作池，生成式API3worker，Jev共享HTTP预算4；原子stage领取、source pin及master写锁、lease和首次completed保持。开发17项定向、当时110项全仓通过；1000条无关done任务下claim使用partial index、341 SQLite VM指令。冻结前补齐no_usable计数、一次性key集合、并发submit写锁和plural失败审计。诊断性完整性仍询问并保存，不触发改稿；诊断调用失败也保留审计并标unknown。另在补齐状态重开后读取持久并发统计，避免新client显示零而冒充运行峰值。

## 验收工具开发完成与冻结前修正（未提交）

实现者graph_acceptance，GPT-6 SOL/medium。新增graph_validation.py与validate_snapshot_graph.py，支持只读结构全审与可选标签样本效果。读取snapshot/graph/semantic/master检查精确批准payload、来源版本文本、成员关系、pending、FTS和来源命名；无标签明确quality未测，不把全pending当质量通过。root冻结前确认输出路径可误指向源文件，已补齐snapshot/cases/3数据库及symlink/hardlink冲突拒绝；新增master原文/抽取记录完整对照，名称归属检查一次遍历。8项定向、当时112项全仓通过（8.869秒），accept-developer-target/full.log；尚未真实图验收。

## 主Agent冻结前独立贯通探针

真实公开run/claim/submit模拟6篇，故意使每篇m1初审失败后修正通过、m2始终失败，完整性统一omission且低confidence。结果6篇均保留，18条有效内容，6条m2剔除；已通过contribution/RP每条只审一次，方法失败条目最多两次；只提交失败键的修正稿、重复submit、终态重放、独立图命名/索引及只读结构验收均通过。日志root-prefreeze-integration.log。首次探针fixture把新HTTP interval字典误写成list，修正后通过；原错误日志单独保留invalid-fixture，不将fixture错误当生产缺陷。

这些是开发／冻结前检查，正式review尚未开始；新60篇仍未采集或调用Jev。最终代码稳定后统一固定提交，全仓及独立审查一次，不将共享工作树演进时的测试失败计作正式review轮次。

## 准备组件最后计时修正完成（未提交）

在run记录持久化实际Jev并发统计；重新打开status、重放已完成任务时保留原peak及累计计数，不用新client的零值替换。开发18项定向、113项当时全仓通过，prep-developer-target/full.log。真实Jev及新60篇仍未运行。三个代码组件和验收工具现冻结，接下来由root固定提交并独立review。

## 正式独立review第1轮 — 通过，进入全新60篇实测

固定远端head9282c61d9f1eed6d1b6a88c364cd21eb5d655acf；完整base01fd0078afd326247a47cc16a561b9c16922fa13。已取回Git并核对远端/本地tree均da5fe0d75337301ff4d0c5055f712db88728a8e7；PR metadata再次确认head和main目标未变。root未修改生产实现，独立检查完整batch及关键边界。

固定head全仓113项通过（9.275秒），review-r1-full.log；六篇真实接口模拟外部任务/故意不通过条目/部分保留/完整性非阻塞/graph/索引/结构验收再次通过，review-r1-integration.log。实际claim入口增加1000done+1000未到期leased任务后VM341→370，返回1条约510B；queued partial index及expired kind/state/lease_until范围均实测，review-r1-locality.log。并发上限、乱序及失败审计、条目hash/key、approved冻结、来源变化、lease恢复、终态time/stat持久化及只读验收覆盖保留。没有确认的未解决代码阻塞。

先前CLI Git取回比预期慢，但已完成，并未跳过固定提交验证。下一阶段开始全新60候选；配置item-v1/2reviews/0.7、Jev4/scope2/review2、外部生成式worker3，per-query40、仅OpenAlex+arXiv。新run不读取上次提取或scope判断，旧数据hash保护不变。生成式API并发只由离线transport测试验证，本轮实际生成仍外部Agent。

## 全新60篇准备实测完成 — 进入独立图验收

新run8bee626f2b164f58883da8e7f2b5d36f，代码9282c61d9f1eed6d1b6a88c364cd21eb5d655acf，Jev0.7，item-v1最多2轮条目审核。重新从两源获取240条候选，选入60个可靠ID记录；与上次60的ID集合相同，但源请求、scope和生成稿全部重新执行，不复用旧LLM结果。

实际流水线318.955秒（5分19秒），进程319.356秒；46scope include、5exclude、9pending，46篇全部保留可用内容。346条最终approved，其中342初审通过、4在一次修正后通过；3个修正任务（可包含多个条目），实际0dropped、0no_usable。删除分支由独立故障夹具验证，本轮没有需要二次失败删除的真实条目。生成式外部3worker全部新稿，API生成路径仍未实调。

Jev实际峰值4，47个请求/47次HTTP attempt、无重试。scope13请求199.594秒、item support23请求304.723秒、完整性诊断11请求165.251秒；累计HTTP669.567秒，实际HTTP时间区间并集284.359秒，排队累计<0.001秒。与上次100请求/累计693.625秒相比请求数量减少；新轮平均单请求14.246秒，上次6.936秒，外部响应约2倍更慢。整体引擎wall仍比718.613秒缩短55.6%，不能把这55.6%完全归因并发，规则/新生成稿/请求分组也变了。

346vector及快照另8.069秒，hash65015c638bb028e74139e248abdfbb391fb8875f3beddb0d5fc47229c4700b41。从run起点到快照完成567.051秒，其中引擎终止至手动触发snapshot的控制交接约240.027秒，单独列出，不冒充模型/代码执行。快照完成后立即启动独立图任务。机器可读preparation-timing.json包含所有计时口径。

独立图输入46篇/346条有效内容，k20、请求打包64、Jev4、外部并行命名预算3。初始全量候选3205对（上次11篇为483对），因此图总wall不能直接与旧295.603秒当同规模速度比较。此时仍在身份判断，未宣称图质量通过。

## 图效果参考样本冻结（模型输出盲测）

独立Agent仅据原始摘要和本次保留条目拟定15个英文检索查询（RP/Method/Benchmark各5）及30个跨paper-ID身份对；未读取图结果。原始标签文件保留author-v1，source-only复核把一对Skill1/CoSkill具体目标不足以等同的案例改为unknown。最终14same/11different/5unknown，其中4对属于同标题不同ID的疑似版本/来源控制样本，效果另分层报告，不能把它们当独立跨作品样本。短证据片段和所有item引用均核验存在。i28的问题同一性按context-growth失败模式层次判断，有粒度说明。

源数据存在2组同标题记录（SkillShift与context-compression survey的不同标识/发表载体）；可靠ID去重未获明确ID关联，因此未擅自按标题合并。后续验收会报告此类来源重复，不把60个唯一ID记录直接宣称60个已确认不同作品。这些参考标签是Agent来源判读，不是人工金标准；检索/身份指标只作小规模效果检查。


## 初次建图完成：计时和规模

初次执行receipt graph-execution.json保持不变：851.879秒（14分12秒），Jev227请求/227 HTTP attempt，无重试，真实并发峰值4。累计HTTP2105.267秒，HTTP区间并集629.663秒；候选对阶段594.859秒、组审27.502秒、最终名称审8.646秒，初次索引2.761秒。3205候选为3123different、27same、55pending；206组中142accepted、64pending。229语义mentions中144归属实体、85pending；另外117条贡献保留paper-local，总计346条批准内容。30个RP名称由3外部Agent完成。

数据准备318.955秒已包含106.860秒采集/清洗及流水重叠，不能再次相加。准备+快照8.069秒+图851.879秒的阶段wall之和1178.903秒（约19分39秒）；另外有手动触发快照的240.027秒空档、外部命名调度及开发/review/验收开销。逐任务队列累计5881.187秒不是用户等待wall，不能直接相加。

对比旧图仅11篇/483候选/295.603秒，本次46篇/3205候选，规模不同，不报告总wall加速比。候选对吞吐约5.39对/秒，旧约2.14对/秒（观察值约2.5倍）；输入长度/API延迟也不同，不能视为受控并发基准。整体准备引擎比旧718.613秒缩短55.6%，可用论文11→46，修正任务81→3；政策与新稿同样改变，不能把收益全部归因并发。

## 图有效性验收：结构通过，效果尚不充分

validate_snapshot_graph.py在恢复索引标记后退出0；live-graph-acceptance.log、graph-acceptance.json有完整结果。346批准条目/46来源均能追溯，成员和来源版本/hash、FTS、名称等结构检查0失败。142实体；85/229 mentions仍pending（37.1%），不能称完整消歧。

盲测15个来源参考查询，Hit@5=8/15（53.3%），MRR=0.48。RP2/5、Method5/5、Benchmark1/5。30身份对中25有明确参考标签：7resolved且本小样本未发现错误、18pending；另5unknown不计分。去掉4个疑似来源版本控制案例后，26对含5正确resolved、16pending、5unknown。不能把7个resolved无错写成普遍100%准确率，更不能忽略18个弃判。这些标签由独立Agent仅阅读源文本制定，不是人工金标准。

诊断graph-acceptance-diagnostics.json：186组通过Jev一致性，其中44组仍被pending候选边规则拦截；另外20组未通过一致性。本轮沿用原有“待定边阻塞关联组”的策略，局部不确定性扩大到整组，是待定覆盖较大的一个直接机制。14个same参考对中13对被召回，11对候选判断same、2对pending，1对未召回；候选召回/判断正确不等于最终组已放行。当前结果不足以支持立即扩到大规模；应先明确pending传播范围及暂存内容的检索方式，再做相同快照对照，而非再采集60篇。

## 持久化异常观察与恢复（根因未确认）

初次运行exit0，receipt/status显示completed和index verified，但稍后重开graph.sqlite时index_*、completed_at、retrieval_index_cpu标记缺失；候选/组/命名/API审计/semantic向量缓存仍保留。先以SQLite backup API保存观察状态于index-state-observation/。备份上冷/热本地索引、并发真实CLI空claim探针均未复现；没有证据足以归因SQLite版本或某个worker，因此未擅自升级依赖或声称找到了根因。

在本次新图原job调用受支持的SnapshotGraph.run()恢复，仅重建本地缓存索引，0.092秒，新增API=0，227请求不变。恢复后关闭重开verified保留，三个并发CLI空claim后也保留。index-state-recovery.json保留前后证据；恢复后的completed_at不冒充初次运行完成时间。当前已恢复可验收，但该未解释观察限制了无人值守持久性结论。

## 完整数据检查点

parallel60-complete.zip，7,558,095字节，SHA256 673f9c9acb93b09164a38b9fd068707bc2cf2f41d243592b62b795bf2025030f；108文件含5个一致SQLite backup、准备快照/所有worker提交、初次图receipt、API审计、参考样本/效果诊断、异常观察副本/恢复记录和逐文件manifest。密钥扫描0匹配，7个历史库/文件hash全部保持。

已保存[完整检查点](https://drive.google.com/file/d/1yI3GO2obOXQIhqKsn2GMSOdbhs5md2lJ/view?usp=drivesdk)，重新下载SHA一致、107个manifest条目逐一验证。下一步记录恢复后的无API重放与查询结果。


## 下载恢复验收通过

从远端完整ZIP下载到新目录restored-complete，SHA和manifest全部一致。5个数据库quick_check=ok、foreign_key_check无违规。已完成PreparationWorkflow.run(collect=False)无API重放：46ready、46extract+3repair，首次wall318.955秒和真实peak4不变；已完成SnapshotGraph.run无API重放：227请求、verified、恢复后的首次completed_at全部不变。冷启动4个HNSW索引缓存命中259、重新计算实体/贡献向量0；RP/Method/Benchmark各1名称查询均rank1且有源论文。完整15查询/30身份对验收重跑与原JSON逐字段相等。恢复验收2.300秒，0新增付费请求。证据restore-verification.json。

新任务Review R1通过；真实运行发现的语义覆盖不足及未解释完成标记异常均如实保留，没有把结构检查或恢复成功当作整体质量已通过。代码停留草稿PR，不合并main。下一步若改pending组传播策略，应在本46篇冻结快照上开展独立对照，避免再次采集与重做批准字段。

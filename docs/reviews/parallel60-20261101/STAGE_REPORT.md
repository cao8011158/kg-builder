# 并发与条目级审核：全新60篇测试

状态：需求固定，开始开发；未开始新一轮真实采集。

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

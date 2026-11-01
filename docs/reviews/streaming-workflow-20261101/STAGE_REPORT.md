# 流式数据准备与独立建图计时

状态：开发开始；尚未运行新一轮 60 篇语义处理。

## 固定边界

- 用户授权：补齐流式数据准备代码，重新通过流水线采集并处理60篇；随后独立建图计时。Jev实调负责scope与字段复核／图阶段身份判断；生成式任务本轮由外部Agent领取并提交，保留API模式。
- 代码基线：ffdff5cd09b789072bc0fc46f13c9ebf87f7e2f7；cao8011158/kg-builder；分支work/retrieval-pilot-20261031；main目标fc114851d5017aed7c8d5477b58b2ff8dac6bbfa仅记录，本轮没有main合并授权。
- 数据来源仅OpenAlex与arXiv；Semantic Scholar停止后续采集，历史保留。新测试用独立目录和数据库，不覆盖已有365篇主库或171来源语义库；即使样本重叠也不复用旧LLM提取。
- 本 step 的代码 batch 含三个独立组件：数据准备、Jev／LLM适配器、独立快照建图。统一固定提交后独立review，最多3轮；真实准备60篇与独立建图分别进行验收和计时。
- 数据准备包含采集、确定性清洗、一次Jev scope、LLM提取／必要修正、Jev字段复核、向量准备和不可变快照。该阶段不创建共享概念或执行聚类。
- 图阶段单独读取快照，整批构建稀疏候选图，再做Jev语义归属和必要LLM命名、统一提交与检索索引。图阶段计时不混入准备阶段。
- 逐阶段记录排队、Jev请求、外部任务领取至提交（包含交接等待）、代码执行和总wall；并发任务耗时之和不冒充wall。
- API key仅保存在任务目录外的本地owner-only凭据文件，不进入仓库、日志、报告或上传包。
- 无适用AGENTS.md；原工作区干净。现有快速回归基线62项。

## 验收

真实两源采集60个唯一候选；Jev真实调用和模型／usage／请求时延可追溯；外部任务通过持久队列领取提交，校验、状态推进和重试由同一workflow执行；中断恢复、重复提交、旧lease和来源变化拒绝；导出快照哈希固定；完整快照独立建图计时与读取恢复检查。原始数据、key和真实请求内容不进入公开Git。

## API组件 — 开发完成，冻结前修正中

开发者 decision_adapters（GPT-6 SOL / medium）新增 decision_api.py，修改 extraction_api.py，并增加两个测试模块；本地未提交。开发者最终定向7项、全仓69项均通过，日志 decision-developer-targeted/full.log。Jev真实接口已由主Agent单独验证：GET models HTTP200，Choice POST使用jev-1.14.0返回typedchoice／probabilities／confidence和usage355input42output；实测往返9.16秒，未将厂商推理延迟当成本环境调用耗时。

主Agent的冻结前契约检查发现 SFW-01：批量问题只在question ID里区分paper／字段，instructions未明确指向state中对应条目；ID不应被当作模型可见的定位指令。此问题属于本step API组件P2，尚未用于60篇正式运行，已给出明确修正：每题绑定items[index]及字段路径，并增加双paper多字段断言。另将scope topics统一为canonical key、限制完整请求大小并记录限流退避。保留开发自测历史；当前不是正式review轮次或通过结论。

## 三组件首轮开发与冻结前检查（尚未提交）

- API组件SFW-01已修复：逐题显式绑定paper及字段路径，canonical topic keys，40k字符state与60k字节请求上限、有限重试和退避。开发者10项定向通过、74项当时全仓通过，日志decision-developer-prefreeze-*.log。正在补充独立request ID，防止批量审计重复计时。
- 数据准备组件：持久状态、带租约的claim/submit、来源pin、修正队列和快照已实现。开发者最新全仓78项通过。冻结前检查确认SFW-03（P2，本step）：首个OpenAlex查询可能填满60而遗漏其他来源/主题；API模式没有实例化extractor；采集/向量/冻结wall计时不完整。已给出均衡采集、API模式早期校验、独立计时的具体修正指令，尚未真实运行60篇。
- 建图组件：整批候选、Jev判断、外部命名与隔离写入已实现。开发者3项定向、78项当时全仓通过，日志graph-developer-*.log。冻结前检查确认SFW-04（P2，本step）：快照text未写回graph-master abstract；最终生成的显示名称未再校验；检索索引构建和实际API请求去重计时尚需明确落实。已派修复，保留pending状态。
- 主Agent独立固定基线测试62项通过（2.687秒），baseline-full.log；先前误从工作树运行的一份日志已重命名initial-working-tree-check.log，不作为基线证据。

以上属于冻结前开发完善，不计正式远端review轮次；开发者测试不代替独立验收。

## API组件计时修正与真实批量定位检查

开发者完成unique request_id、批量共享request审计、最终失败的sanitized last_request_audit和按完整payload大小自适应切包；11项定向离线测试通过，decision-developer-timing-targeted.log。主Agent真实Jev双样本同请求测试：agent长期记忆include，DRAM硬件论文exclude；本环境往返10.821秒。真实输入和响应仅保存到私有jev-batch-contract.json。未调用生成式LLM API，未将共享请求耗时按两篇重复求和。

## 冻结前修正完成（尚未提交）

数据准备组件SFW-03：两源×三主题按份额采集并轮询补足，已完成查询恢复不重取；API模式显式实例化和凭据早期校验，修正任务传递此前结果与问题；采集不足明确报错并拒绝冻结；完整计时分开记录采集、规范化、scope、字段复核、外部服务、向量，批量API按request_id去重；来源限定vector key避免跨paper冲突。开发者5项定向、82项全仓通过。locality日志记录队列/已知task查询索引，以及0/1000无关10KB论文下固定目标查询VM/输出不增长。日志preparation-developer-{targeted,full,locality}.log。

建图组件SFW-04：保留精确source text；生成显示名称后Jev校验并保留失败pending；按请求ID去重API审计、外部命名等待与纯图/索引耗时分开；快照向量paper限定key；冷HNSW索引实际构建与查询检查，保留兼容向量cache，CLI支持模型cache目录。开发者4项定向、82项全仓通过，graph-developer-*.log。另在冻结前补齐空快照和索引后wall终点边界检查。

正式review尚未开始，真实60篇尚未处理；接下来固定代码提交并由主Agent独立验证。

## 最终开发边界修正完成，准备固定提交

数据准备新增append-only decision_audits，保留scope及每轮字段复核完整结果与失败API审计；恢复成功清除暂时API错误但保留采集不足错误。开发者6项定向、84项全仓通过。建图空快照可完成而无需模型；wall终点移到索引成功之后，开发者5项定向、83项当时全仓通过。均尚未提交。

主Agent独立synthetic跨组件探针通过：公开run/claim/submit六源查询流转，6篇新提取、重复提交、24个paper限定vector、独立图任务6个外部名称、冷索引查询、graph-master原文保留及prep-master不被建图修改。日志root-integration-prefreeze.log。此探针使用fake Jev/embedding，不冒充真实60篇验收。

## 正式独立review第1轮 — 需要修正

固定远端head：248600395ebd927be4bd8562ba1de649b4500d11；base：ffdff5cd09b789072bc0fc46f13c9ebf87f7e2f7；main目标未变。远端tree10878a75f08fb910f037805bf8b802b84ce893b3与本地候选一致，已取回核验。主Agent未修改生产实现，独立审阅完整batch差异与契约。

独立验证：固定head全仓84项通过（6.904秒），review-r1-full.log；六篇synthetic公开流水线到独立图任务、索引与源库隔离探针通过，review-r1-integration.log；真实Jev双条目绑定先前已验证，不作为本轮fake测试数量重复计算。

确认SFW-05（P2，新引入，本step数据准备组件）：claim队列的OR条件只使用idx_tasks_claim(kind=?)，扫描同kind已完成历史。固定1个可领取任务时，增加1000个无关done任务后SQLite VM从108增至7074，输出仍1行58B；EXPLAIN显示kind前缀扫描和临时排序。已知task详情使用task PK+paper PK，不受此问题影响。证据review-r1-locality.log。根因为kind前缀加OR未定位活跃状态，不能用“EXPLAIN有INDEX”代替实际局部性。修正方案：queued与expired-leased分支各自限定状态/索引，先合并并限制任务ID，再读取详细source；保留CAS租约及顺序、fingerprint行为。增加同kind大量done任务反例，并重跑定向/全仓；之后第2轮复审同一问题。当前未开始真实60篇，旧数据不受影响。

## 第1轮后修正开发总结（尚未再次提交）

SFW-05：queue按queued／expired-leased分支定位活跃ID，再取限定任务详情；BEGIN IMMEDIATE与CAS保留。开发者7项定向、85项当时全仓通过。主Agent实际claim入口独立测得：加入1000条同kind已完成10KB任务，VM341→370，输出仍1条约510B；queued使用partial index，expired使用kind/state/lease_until范围，fix-root-locality-probe.log。曾发现开发者新增进度计数挂在非claim连接上而返回0；已明确纠正为实际连接工厂并要求baseline>0，保留原失败测量历史，不冒充通过。

SFW-06（P2，本step建图恢复边界）已修正：同字节快照下载／复制导致mtime变化时，先核验完整digest与file hash，再更新stat缓存；原先会错误拒绝恢复。变更字节继续拒绝。开发者6项定向通过。尚待固定修正提交后第2轮独立复审。

另对开发者提到的synthetic并发SQLITE_BUSY_SNAPSHOT作有限诊断，要求保留采集期间提交／复核覆盖；未将尚未定位的疑点计作已确认生产缺陷。

## 并发写入诊断及修正完成

SFW-07确认为P2、本step数据准备集成引入：collector.upsert_paper与字段复核ingest_extractions在master.sqlite3并发写入，复核读pin后SAVEPOINT升级写锁可能发生SQLITE_BUSY_SNAPSHOT；claim的workflow.sqlite3锁不能解决master写入。修正是在本step _review_batch对master先BEGIN IMMEDIATE，再读pin与ingest，未修改既有storage.py。恢复采集未结束前提交的原测试，并明确断言首篇ready时collection仍open，随后才释放producer。开发者重复5轮、定向7项、全仓86项通过。日志preparation-developer-streaming-repeat/targeted/full。撤销等待collectionsealed以避开冲突的测试改法；不把该改法当作有效修复。

## 正式独立review第2轮 — 通过代码审阅，进入真实验收

固定远端head：b5af7c94d028b85be1668e4931a05fcdc2a5e02b；完整base仍ffdff5cd09b789072bc0fc46f13c9ebf87f7e2f7。已fetch，远端／本地tree均c8ed4f975386d93d8f4c04b44ae1814c5c6ddbd0，main目标未变。主Agent独立复跑86项全仓通过（7.223秒），review-r2-full.log；跨组件六篇外部worker／24vector／独立图与索引探针再次通过，review-r2-integration.log。额外10个全新synthetic流式实例全部成功，root-streaming-stress.log。

SFW-01/03/04/05/06/07均addressed：逐条模型问题定位；平衡来源与API模式；精确源文、最终显示名审计与索引；活跃队列实际读取量不随done历史增长；异地同字节快照恢复；master写入先取写锁且采集未结束已完成首篇复核。来源变化、旧lease、幂等、repair上限及pending、空快照和篡改反例均保留。没有未解决代码阻塞；真实60篇和真实Jev图语义尚待验收，不由模拟通过代替。

开始真实运行：全新preparation目录，target60唯一候选、两来源三查询、per-query40、batch5、jev-1.14.0、外部生成式worker。生成式LLM API key不注入；不读取旧extractions。准备与图阶段各自计时，开发／审阅耗时不混入真实处理wall。

## 真实准备运行进度（尚未完成）

运行ID3810ed2e0cba440c8e384de9f88f9d5e，代码b5af7c94d028b85be1668e4931a05fcdc2a5e02b。6个源查询成功，返回240条候选；按两源三方向分配60个唯一候选（OpenAlex30、arXiv30）。source fetch/解析累计44.258秒，候选入库／规范化1.340秒，六查询采集阶段累计46.824秒；collection_request.calls=240是generator yield计数，不是HTTP请求数，源HTTP查询6次。scope完成47 include／5 exclude／8 pending，60篇scope请求累计75.14秒（此阶段与采集重叠，不能相加当wall）。

47篇全新初次LLM提取均由外部worker经claim/submit完成，不复用旧输出，不调用生成式API。Jev低于0.7置信度的完整性检查触发大量修正，尚在继续；当前不能把结构接收稿数当作ready数。首个活动检查点保存并readback验证到既有项目子目录：Drive file1Am4YyKQ-thuuLc8QiXk1GHMlFD8yCWpv，833684bytes，SHA256 4d0c02c68a7c063c596e23346eaf6580e5272fed3e9ff367092f7a9e54978497，解包扫描未发现凭据；这是active checkpoint，不是最终快照。数据库按workflow先、master后做SQLite backup并quick_check，不直接复制写入中的数据库。

## 真实准备60候选 — 运行完成，质量门保留待定

- 运行进程exit0；流水线wall718.613秒（11分58.6秒），向量/快照另3.222秒（含81条384维向量，local encode3.169秒）。从run起点到快照完成约753秒，包含命令交接，不能用阶段相加掩盖此间隔。
- scope47 include／5 exclude／8 pending；47篇全部完成新LLM提取，另81次修正，全部经过队列与Jev复核。最终11 ready、36字段复核pending，加8scope pending，共44pending；不是从60里只抽11篇处理。待定稿和全部128次复核历史均保留。
- 真实Jev scope12请求75.141秒、字段复核88请求618.484秒，均无HTTP重试；usage分别input61487/output9696及input419684/output71146。字段复核占流水线wall约86%，目前是主瓶颈。
- 外部claim→submit累计2986.104秒，时间区间并集492.323秒；多worker/batch重叠，二者都不等于纯模型推理时间，不与API耗时相加作为总wall。
- 这轮完整性置信度0.7规则引发大量重复修正，许多低置信度问题修正后仍未消失。代码流程完成不代表质量门已适合生产；11篇可用于当前通过子集的功能／时间测试，不能当成60篇通过或质量准确率。
- 冻结snapshot hash7fec3ae663e457a67a2035c29e1f83e3d9b20bec973670fbffbe26e216eef412，counts60collected/11ready/5excluded/44pending。准备阶段未建共享实体／HNSW／Leiden。机器可读preparation-timing.json和snapshot-execution.json在私有验收数据目录。

接下来仅从该11篇、81vector快照独立建图计时，明确规模限制；不放行待定记录，也不覆盖旧365篇库。

## 独立图任务 — 真实运行完成，进入恢复验收

输入仅冻结的11篇ready数据：11 ResearchProblem、26 Method、17 Benchmark描述及27 paper-local contributions，共81vector。整批候选483对，480 different／1 same／2 pending；执行一次ResearchProblem Leiden提案后保留语义判定。外部worker领取提交11个RP名称定义，并由Jev检查最终显示内容。

图任务wall295.603秒（4分55.6秒）。本地快照导入0.151秒、候选图0.058秒、写入0.027秒、冷检索索引构建并查询1.448秒，合计1.684秒；这些是本地阶段elapsed，不是process CPU-seconds。Jev35个请求257.589秒、无重试；pair阶段225.215秒、group23.470秒、最终名称检查9.098秒。外部名称任务claim→submit累计121.122秒、排队累计125.105秒，都包含多任务重叠，不当成wall相加。

产物46canonical entities、54原始描述，其中46建立来源连接、8仍pending；7pending groups及2pending pairs保留，27contributions保持paper-local。4个冷HNSW索引完成实际查询，62cache hits，11个新canonical RP文本向量本地计算；不重复计算兼容快照mentions。该规模只能证明11篇通过子集的功能与时间，不能外推60／365篇全量建图吞吐或身份准确率。

旧365篇工作主库及旧171来源语义库SHA检查未变；准备快照已上传下载并核验SHA256、两库quick_check及60/11/5/44counts。接下来完成图包上传下载、异地恢复及cold dense+sparse/source定位验证。

## 恢复验收发现SFW-08，重开本代码batch

SFW-08（P2，本step数据准备、当前实现引入）：对已完成运行再次调用run，会无条件重写completed时间，导致历史处理wall增长。主Agent在真实60篇数据库backup副本复现：718.613秒变为1311.596秒；未调用API，原运行数据库未动。证据私有terminal-replay-prefixed.json。根因是终态时间UPDATE未保留第一次完成值。修正只需COALESCE／completed IS NULL，增加完成重放后wall、task、audit不变的定向反例。已派开发修复；随后固定提交进行第3轮正式review，保留前两轮历史。无需重新付费处理60篇；在恢复副本验证终态重放和完整查询即可。暂停最终交付，图和准备结果保留。

## SFW-08修正开发完成（待第3轮固定提交审阅）

completed改为COALESCE保留首次终态时间。新增重放反例验证completed、wall、tasks、audit均不变；开发者8项定向、87项全仓通过。对真实60篇备份副本重放，无API调用，718.612883秒、128tasks、188audits均不变；日志preparation-developer-terminal-replay.log。原数据库未动，真实性能数据仍对应b5af7c94d028b85be1668e4931a05fcdc2a5e02b；这次终态重放修复不改变已测量的初次处理工作，后续用修正代码验证恢复，不重新计费处理60篇。

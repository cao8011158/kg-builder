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

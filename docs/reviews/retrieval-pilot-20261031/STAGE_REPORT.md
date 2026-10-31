# 精简研究知识图谱：实现与首轮试验

状态：开发中；LLM API 保持关闭。

## 任务边界与基线

- 用户授权：执行简化 schema，并在小批真实论文上跑通提取、聚类／消歧、保存关联与检索。
- KG Builder 基线：`fc114851d5017aed7c8d5477b58b2ff8dac6bbfa`；工作分支 `work/retrieval-pilot-20261031`。
- 主仓库基线：`13bded9d1ca8a5aa6eca0596777cefdf6126a88a`。不合并主分支，不发布网站，不运行付费模型 API。
- 数据：已恢复原始 SQLite（365 papers / 403 versions / 40 extractions）；原始文件只读保存，试验在副本与派生 sidecar 进行。
- Batch 1：新增 v0.3 精简提取协议、校验、关闭的 API 接口；旧格式兼容。开发者 schema_development（GPT-6 SOL / medium）。
- Batch 2：真实 HNSW、Leiden、CPU embedding，SQLite 语义索引、候选审阅、增量归组与检索。开发者 vector_development、workflow_development（GPT-6 SOL / medium）。
- 主 Agent：原文阅读、手动提取和语义判断，独立代码审阅、端到端验收。主 Agent 不实现生产模块。
- 测试：定向测试、快速全仓库回归、真实论文与真实 embedding 试验、幂等／回滚／陈旧审阅反例、按 ID 查询 EXPLAIN 与无关数据扩容对照。

## 环境恢复

工作目录曾被环境维护清理，已从 Git 与已保存数据恢复；仓库内未发现适用 AGENTS.md。依赖安装正在进行；首次 hnswlib wheel 构建失败，正在诊断本地编译环境，不构成算法功能缺陷。

## 阶段 1：Batch 1 开发完成（未提交）

新增 `research-kg-extraction/0.3-retrieval`，只要求贡献、问题、方法和 benchmark；拒绝旧版冗余字段。方法保留本文角色，benchmark 同时支持 introduced/used，并保留版本或子集。旧 v0.2／legacy 不迁移、不删数据。API 接口默认关闭，返回内容必须符合新协议及输入论文／版本身份。

定向测试 5/5 通过；开发者早期完整回归 37/37，通过后补充版本隔离测试。并发开发出现后最新全套 45 项有 5 个 hnswlib 缺失错误，均来自尚在开发的向量模块，不将此全套记为通过。日志见 `logs/schema-targeted.log` 与 `logs/schema-full.log`。真实 API 请求未运行。下一步：独立检查新协议与兼容性，冻结 Batch 1 提交。

## 阶段 2：Batch 1 第 1/3 轮独立审阅：通过

固定远端提交：`eef0d11ea944ad1241c3a4f0e5ae7e51d5962fd6`（与本地开发提交 `80b1f5db5d5aea06f117ff00565633a0106a2462` 内容树完全一致）。审阅完整基线差异；在独立导出的提交树复跑 38 项测试，全部通过。覆盖 legacy/v0.2、新协议、版本隔离、错误输入、重复导入、批次回滚与离线 reconciliation。新协议不强加完整证据审查；API 默认关闭。

原始日志：`logs/schema-review1.log`。首次独立导出时因目标目录路径写错未能启动测试，保留在 `logs/schema-review-env-failure.log`，纠正目录后复跑成功；该环境错误不计正式失败轮次。没有发现阻塞缺陷。真实 LLM API 未运行；此轮也不声称向量／检索完成。已保存工作分支，不合并 main。

## 阶段 3：Batch 2 向量计算开发完成（工作区未提交）

开发者 vector_development（GPT-6 SOL / medium）。已实现本地 FastEmbed、稳定 ID HNSW 更新／删除、余弦候选重算和稀疏图 Leiden CPM。真实编码得到 384 维归一化 float32；真实 HNSW/Leiden 定向 8/8；当时集成回归 49/49。后续补充模型版本标识与大 k 搜索参数检查，最终测试以冻结提交为准。

安装问题根因：Python 构建配置指定不存在的 clang++；显式使用已安装的 GCC 后 hnswlib 编译成功。模型下载所需 SOCKS 支持已补齐。没有替换 HNSW／Leiden，也未调用付费 API。

工作流仍在开发：主 Agent 发现候选路径尚未接 sparse、混合类型 ANN 可能挤掉目标类型候选，已明确交开发者修正；这属于开发反馈，尚未开始 Batch 2 正式审阅。

## 阶段 4：Batch 2 工作流开发完成（未提交）

开发者 workflow_development（GPT-6 SOL / medium）。新增独立 SQLite 语义 sidecar：按明确 extraction ID 读主库；原始提取、论文版本、角色／子集保留；向量缓存按模型和文本指纹复用；问题聚类只输出候选。方法／benchmark 候选现已接 exact-name、FTS5 sparse、按类型 HNSW dense 与 RRF；审阅 JSON 显式决定新建、匹配或待定，并检查源／候选修订。贡献独立检索。

开发反馈修正：跨类型候选挤占、初始别名 LoCoMo/LOCOMO 同批关联、模型切换后的旧候选、索引更新失败后的同进程恢复。SQL 审阅已提交而索引更新失败时返回 rebuild_required，下一次查询重建受影响类型。分页有边界，初始聚类无静默固定上限。当前工作流定向 7/7、全套 54/54 通过（开发者证据），待主 Agent 在冻结提交独立复跑。

已开始实际数据试验：主库副本导入 5 篇精简提取，未修改旧 40 条。初始 10 条问题使用 k=4、similarity=0.65、CPM resolution=0.1 时错误地得到单一大组；这是参数不合适，未作为最终语义归属。小样本阈值／分辨率对照已保存，后续需 LLM 检查拆分。付费 API 始终关闭。

## 阶段 5：Batch 2 第 1/3 轮独立审阅：需要修改

固定远端提交 `96b61e8ab2df74483d6e38acd928f7e54f2a8426`。已取回远端，核对与本地开发内容树一致。完整回归 54/54 通过；独立反例仍确认两个当前 batch 内 P2 缺口：

- **SEM-01（P2，未关闭）**：`SemanticStore.search` 对 Contribution 只查询 FTS，没有 dense 路径。独立调用 `search('remember earlier conversations', kind='Contribution')` 对“A memory method”返回空，且编码器未参与。与每类语义内容可做 dense/sparse 检索的目标不符。修复：按贡献类型建立可复用 HNSW 缓存，新增／替换提取时局部维护或明确失效，融合 sparse+dense 结果；增加语义改写召回及相关／无关修改测试。
- **SEM-02（P2，未关闭）**：`_candidate` 将 name+换行+description 传入 `_rank_entities`，精确匹配分支随后用整个串与 `entities.name` 比较，实际无法命中同名条目。源码跟踪证据 SQL：`name='Memory retrieval\nRetrieve memory'`。修复：分开传表面名称与语义查询；为规范化名称／别名建立索引，候选精确查询按类型直接定位，仍保留 sparse+dense 后续候选。

证据：`logs/workflow-review1.log`、`logs/review1-counterexamples.log`。前述问题均处于首次实现的试验代码中，尚未合并或投入使用；本轮没有确认 P0/P1。允许开发子 Agent 按现有授权修复，保留此轮历史，第 2/3 轮复审检查原问题及根因变体。实际原文／向量试验继续准备，最终验收等待修复。

## 阶段 6：Batch 2 修复 1 开发完成（未提交）

SEM-01：贡献新增 dense HNSW 与 FTS 融合；源提取替换时只删除／更新受影响贡献的热索引，未变化源不重建。SEM-02：新增 `(kind, normalized_name, entity_id)` 表面形式索引，候选分别使用名称精确查询与描述语义查询，规范化名称／别名优先，保留 dense+sparse 候选。已处理已有 sidecar 的一次性索引迁移。

开发者 workflow_development，定向 9/9、完整 56/56 通过；日志 `logs/workflow-fix1-targeted.log`、`logs/workflow-fix1-regression.log`。新增反例包含贡献语义改写、同进程源替换、无关写入复用、大小写别名与强相似干扰候选。主 Agent 已读取完整修改，下一步冻结修复提交并第 2/3 轮独立复审；开发自测不代替复审。

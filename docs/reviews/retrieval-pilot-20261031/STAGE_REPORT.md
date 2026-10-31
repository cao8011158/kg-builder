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

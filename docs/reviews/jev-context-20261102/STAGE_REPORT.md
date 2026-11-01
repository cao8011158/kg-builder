# Jev概率门槛与上下文精简：仅代码更新

状态：需求固定，开发中；真实API与原60样本重建均未授权，禁止执行。

## 范围和权限

用户最新授权：先修改代码并通知，之后用户再授权使用原60样本做制图测试。覆盖概率判据、语义输入精简、同请求共享证据/候选对象、可配置更细领域聚类。本轮只做合成离线定向/回归/接口测试，不采集、不读密钥、不调用真实Jev或LLM、不对旧样本运行聚类或建图，不调原59向量参数，不改旧数据库。

仓库cao8011158/kg-builder，分支work/retrieval-pilot-20261031；base7ff332f659d456e2f4403421cdfcf5c6d5b906d2，treea9f744c5fc2e31560c5996c65b23751ee1c2eddd；main目标fc114851d5017aed7c8d5477b58b2ff8dac6bbfa。既有分支push权限沿用；无main合并授权。无适用AGENTS.md。生产代码委派开发Agent，root做契约、独立review和裁决。

## 固定需求

- 所有新业务Choice门槛使用对应选项概率，而非confidence。肯定支持/同一性等>=0.7；仅not_belongs概率严格>0.7才排除。confidence保留原始审计，不额外拦截。原历史结果不重写、不重解释；保存判据版本，恢复旧job不静默换规则。
- 同一性输入只投影对象name/description、benchmark version_or_subset及已有直接身份信息；论文ID/标题/论文版本/网址/hash、技术key、重复source整包不送模型。业务role在支持性审核保留，在身份比较中不当对象身份属性。原数据、pins、hash和审计映射仍在代码/数据库中保留。
- 模型上下文按请求共享sources/entities/groups等：一个A+20邻居为21对象、20身份问题，不扩成210对；每条独立答案和原始pair/item ID对应正确。按当前去重候选对流程，不实施LLM自由分组、不增加固定锚点、不改k20和相似度过滤。
- 字段支持性只审语义主张，不要求证明key/hash/来源地址。完整性仅诊断；删管理元数据、不删原文证据。相同原文在同一请求只放一次。
- 共享/投影后的真实序列化大小决定拆包；max24questions/state40000chars/body60000bytes、并发上限和失败审计保留，不静默截断；不同原文或不同版本不得误共享。
- Leiden resolution开放为显式CLI/构造参数并持久绑定job，验证器按保存参数重算合成分区。保留旧job0.05兼容默认；更细领域目标已定，授权后用现有向量对照较高值选择，不在本轮擅定已验证最佳值。
- 历史scope/fields/graph审计按旧confidence含义读取；新job保存probability metric，不用新规则验证旧排除审计。旧partial job恢复规则需一致，保持原有版本语义。

## 开发分工与验证

A：decision_api语义投影、共享上下文、概率消费及API定向。B：preparation/CLI概率集成与恢复。C：snapshot_graph/graph_validation/CLI判据与resolution集成。三个文件所有权不重叠，均依共享契约协调。root不写生产代码，冻结提交后独立review最多3轮。

离线验收：高P低confidence通过、低P高confidence不通过、等于0.7不排除/大于才排除；同源8条仅1份证据、不同源绑定；A20邻居21对象20问题、无论文元数据；拆包/大字段/重复ID/乱序/失败审计；workflow二次检查不再误用confidence；新/旧metric恢复；resolution参数固定/恢复/CLI/验证器；全仓回归和独立合成贯通。原样本文件哈希保护。


## 基线固定完成

本地与远端head7ff332f659d456e2f4403421cdfcf5c6d5b906d2一致；main目标未变，草稿未合并。基线全仓127项14.877秒通过，exit0，baseline-full.log。6个旧输入/数据库哈希保存在private任务目录。三个开发Agent使用GPT-6 SOL/medium独立上下文，生产所有权不交叉。JevAPI保持原生confidence审计，但所有新业务门槛换为选项概率；保留旧job含义。未授权真实API/原样本测试。


## 准备流水线本地开发与边界补充完成（未提交）

实现者jev_prep_runtime，GPT-6 SOL/medium。新config固定decision_metric=probability，旧缺省config恢复confidence；原生适配器接收metric，注入回调签名兼容。支持审核二次门槛改为P(supported)，status/review_metadata显式记录metric/threshold/probability与原confidence；修正期间已批准条目的审计值保留。CLI新增--min-probability。源pin/hash、最多两次语义审核、approved冻结和完整性非阻塞保持。

开发初始23项通过，root冻结前发现非法数值2可越过二次gate，已补0..1/finite/bool检查；同时明确缺失/非法P应在关联校验try内走技术错误路径，阶段回queued、attempts保持0，不让故障消耗修正预算。最终26项定向6.49秒通过，prep-tests.log。没有真实API或旧样本执行，尚未独立review。

## 建图/验证器本地开发完成（未提交）

实现者jev_graph_runtime，GPT-6 SOL/medium。新job固定probability，旧job缺metric仍confidence；成员审核保存并核对namedoption概率与raw分布、confidence仅保留审计。新增--problem-resolution与构造参数，默认0.05兼容、显式高值可用于后续授权测试，恢复读取保存值、显式不一致拒绝。候选生成与验证器均消费保存的resolution，status展示配置。

34项定向6.1秒通过（graph-target.log），含合成0.3分区和validator重算、CLI状态/参数冲突、旧metric恢复、概率篡改。图开发者全仓144项在共享树演进时有2处prep新夹具错误；prep已修正，保留graph-full.log，不当正式审阅结果。冻结后统一跑完整回归。

## API投影冻结前保留必要证据检查（开发中）

root发现实际graph调用把benchmark version_or_subset放在source内部，首稿投影只读顶层会漏掉该证据。已要求共享投影从嵌套source提取该业务字段，保留版本差异而不带入key/url/hash，并覆盖pair和member真实形状。按已定“不删必要证据”要求纠正，不改变业务策略、不新增真实API测试。


## API本地开发完成，进入冻结验证（未提交）

实现者jev_semantic_api，GPT-6 SOL/medium。所有原生Choice业务判断新增decision_metric，probability为新默认、confidence为历史兼容；成员排除读取P(not_belongs)>0.7，保留原confidence/raw，不伪装概率。模型state按语义白名单投影，原ID/hash仅在本地结果和审计映射；sources/entities/groups在请求内共享，拆包按真实投影state和body大小；问题索引与原结果精确对应。

支持性保留role/roles，身份仅对象语义及版本/直接证据；明确使用情境不同不代表实体不同、未知版本不是冲突。root发现的source内版本遗漏已修正，pair/member两个真实输入形状均测试；source去重同时绑定本地paper身份与原文，身份不送模型。21项最终定向0.488秒通过，api-directed.log。包体测量api-packets.log：8项冷/热均792state字节/4657body字节、原文1份；扩大无关管理信息后不变；A20有21对象/20题、14430body字节。均为合成输入，不代表真实token/延迟收益。

准备补充后的26项6.216秒通过；API最后改动后由root统一冻结全仓验证。旧api-full.log/graph-full.log在共享树演进时144项中有2个prep夹具错误，后由技术错误路径修正；保留历史，不计正式review。三组件本地开发完成，无真实API/原样本测试、尚未正式review。

## 三组件冻结回归完成

当前共享树最终全仓146项14.403秒通过，exit0，developer-full-frozen.log；这是开发冻结验证，尚不计正式review。后续固定远端提交后由root独立审阅。

## 独立审阅 R1：通过

固定远端提交 `6a737f3879d5665f607e6f7c4376cc5d1b439aa6`，tree `3dac3cec26cd203a32d2dbcbc9d5a63c1323df90`，覆盖整个 base `7ff332f659d456e2f4403421cdfcf5c6d5b906d2`..head。main仍为 `fc114851d5017aed7c8d5477b58b2ff8dac6bbfa`，草稿PR1未合并。root未实现生产代码，独立检查全部生产diff和相关调用路径；三开发者自测仅作导航。

需求结论：新job业务门槛读取命名选项概率，confidence仅审计；成员只在明确not_belongs且P>0.7时排除。元数据仅在本地关联/审计，必要角色、版本/子集、直接证据保留。原生API、preparation二次gate、graph持久化与validator统一判据。旧配置缺metric仍恢复confidence，缺resolution仍0.05；显式新值固定并恢复，不能覆盖已有job。未改变候选集合、HNSW k、向量、算法、独立节点及完整性非阻塞规则。

独立全仓146项13.334秒通过，exit0，`logs/review-r1-full.log`。独立HTTP拦截验收 `offline_acceptance.py` 通过，exit0，23个本地模拟请求：原生适配器→准备审核→快照、合成3篇图→外部命名提交→领域审核→物化/索引→只读validator→重开恢复全链路通过。使用合成向量，resolution0.3仅验证参数传递和恢复，绝非原样本参数对照。另验证低confidence/.76正概率通过、.69不通过、历史confidence模式、.7/.700001严格排除边界、Unicode拆包后关联。原样本6个受保护文件sha256全部相同。0真实API请求、0原样本重跑。

### 输入共享与访问范围证据

| 公开路径 | 必要输入/内部范围 | 独立实测 | 一致性与边界 |
| --- | --- | --- | --- |
| review_items→_projection→JevClient.decide→HTTP序列化 | 本批8条语义条目+同篇原文；来源/指纹仍由既有代码读取校验 | 冷/热包相同，正文5239字节，source1份；每条无关URL扩大到约160KB，包体不变 | 角色/子集保留；ID/hash不进入模型；不宣称减少DB内部读量 |
| judge_pairs→_projection→JevClient.decide | A与20近邻，共21对象20题 | 包体15413字节；A仅一份 | 嵌套source版本S保留；不新增210对比较；原pairID本地映射 |
| 大型Unicode pair批次→拆包→并发消费 |25对，本批必要语义 |9个HTTP模拟包，每个≤24题、state≤40000字符、body≤60000UTF8字节 | 每包重建索引；25结果原顺序/原绑定；超大单条显式错误由定向回归覆盖 |
| 准备/图→原生适配器→持久化→validator/restore |合成1篇准备、3篇图 |全链路通过 |新metric/resolution恢复；raw概率审计核对；原样本未写入 |

此变更没有修改数据库查询形状、长期缓存或向量索引访问策略，DB EXPLAIN/PROFILE专项不适用本次输入投影修改；现有全仓局部查询反例继续通过。共享注册表仅存活于单请求，拆包重建，无跨版本缓存失效。数据读取/原文证据校验未删减。数值是本地合成包体证据，不是实际token、API时延或准确率收益。

剩余问题：本轮没有确认阻塞代码缺陷。验收脚本初稿错误读取counts.accepted/顶层passed，按既有返回结构改为counts.ready/structural.passed后通过，属于验收脚本自身修正，无生产代码变化，不消耗额外正式review轮次。已记录的历史低召回、漏归一、粗领域是否改善仍未验证；细分参数尚未选定，兼容默认0.05保持。需要你另行授权后才用之前60篇来源的已清洗数据重建及比较，发现效果问题先与你商量。

下一步仅持久化审阅证据及最终提交核对；不触发样本测试或main合并。

证据检查器仅因未授权main合并返回BLOCKED（exit2）；测试/审阅门槛无其他阻塞。这不影响代码交付，本轮不请求或执行合并。

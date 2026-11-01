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

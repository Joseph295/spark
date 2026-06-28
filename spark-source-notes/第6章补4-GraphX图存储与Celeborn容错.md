# 第 6 章补4 — GraphX 图存储 与 Celeborn 副本容错

> 对照源码:`graphx/.../PartitionStrategy.scala`、`graphx/.../impl/{GraphImpl,RoutingTablePartition,ReplicatedVertexView}.scala`(apache/spark master);Celeborn 据公开设计(非本仓库)

---

# 一、GraphX 图存储:为什么扛万亿边

## 1.1 双 RDD + 点割 vs 边割
图 = VertexRDD(顶点) + EdgeRDD(边),两个特化 RDD。核心抉择=怎么切图:
- **边割(edge-cut,传统如 METIS)**: 顶点分到各机器,跨分区边被切断→需通信。幂等律图很糟(超高度数顶点产生海量跨分区边)
- **点割(vertex-cut,GraphX 选)**: 边分到各机器,横跨多分区的顶点被复制
> ★ 选点割因真实图幂等律(少数顶点度数极高,同 LLM 语料倾斜)。点割按边均分天然负载均衡,只复制少数高度数顶点(复制一个 wikipedia.org 远比切散它几百万条边省)。PowerGraph 论文核心洞见,决定 GraphX 能处理 LSH 去重"热点文档连海量候选边"的图。选对数据切分=能否扛幂等律的分水岭。

## 1.2 EdgePartition2D:复制上界钉死 2√P(`PartitionStrategy.scala` 74–95)
```scala
76  ceilSqrtNumParts = ceil(sqrt(numParts))           // P 个分区排成 √P×√P 网格
80  col = abs(src * mixingPrime) % ceilSqrtNumParts   // 边按 src 决定列
81  row = abs(dst * mixingPrime) % ceilSqrtNumParts   // 按 dst 决定行
82  (col * ceilSqrtNumParts + row) % numParts
```
矩阵网格: 边(src,dst)落在(col=hash(src),row=hash(dst))。碰到顶点 v 的边要么在 v 的"列"要么"行"→最多 2√P-1 个格子→**v 最多复制到 2√P 台机器**(63-64注释)。mixingPrime 打散倾斜。
- EdgePartition1D(101): 只按 src,同源边聚一起但高度数 src 倾斜
- RandomVertexCut/CanonicalRandomVertexCut(113-133): 按(src,dst)哈希,聚平行/双向边

## 1.3 路由表 + 复制视图(`RoutingTablePartition`, `ReplicatedVertexView`)
边计算需两端顶点属性。不广播全部顶点,用路由表定向投送:
- RoutingTablePartition: 每个顶点被哪些边分区引用(src/dst)
- shipVertexAttributes(ReplicatedVertexView 66): 按路由表把顶点属性**只投送到需要它的边分区**(定向非广播)

## 1.4 aggregateMessages:每超步免全图 shuffle(`GraphImpl.scala` 207–243)
```scala
207  preAgg = view.edges.partitionsRDD.mapPartitions(...)  // ① map over【边分区】
210    activeFraction = numActives / indexSize
       // 顶点属性已复制在边分区(view),本地跑 sendMsg + 本地 mergeMsg 合并,此时无 shuffle
214    aggregateMessagesIndexScan(...)  // 活跃<0.8→走索引只扫活跃边
217    aggregateMessagesEdgeScan(...)   // 活跃多→全扫
243  vertices.aggregateUsingIndex(preAgg, mergeMsg)  // ② 唯一 shuffle:已预聚合的小消息投回顶点分区
```
每超步=边分区本地计算(顶点已预投送)+ 一次预聚合后的小 shuffle，非全图 shuffle。
> ★ 让迭代便宜的四件事: ①本地预聚合 ②小 shuffle ③索引扫描(210-238,活跃<0.8只扫活跃边,CC后期几乎免费) ④增量复制(outerJoinVertices 254-258/updateVertices 106,只投送变化顶点 diff(newVerts))。四者叠加才让 GraphX 万亿边跑几十超步扛得住,LSH 去重海量候选边图的连通分量能跑完。GraphX 快=把每超步要动的数据量压到最小。

---

# 二、Celeborn 副本与容错(据公开设计,非本仓库源码)

> Apache Celeborn(前身阿里 RSS)独立 Apache 项目,不在 spark 仓库。配置名/ack 语义随版本不同。

## 2.1 架构三件套
- Master(Raft HA): 管 workers,为每 partition 分配 slot;元数据 Raft 复制,故障切换不丢
- Worker: 收 push/按 partition 聚合/存储/服务读
- Client: driver LifecycleManager(管生命周期) + executor ShuffleClient(推/读)

## 2.2 Push 复制:复制在推送时完成
开复制(spot 推荐默认开):
- master 为每 partition 分配 Primary+Replica(两台不同机器)
- map 端 push 给 Primary→Primary 流式转发给 Replica→两副本落盘才 ack
- **复制在 push 链路顺带完成(primary→replica 转发),无需额外拷贝作业**。partition 在 worker 上 append-only 文件

## 2.3 Slot 与 partition split(对抗倾斜)
partition 文件太大/磁盘吃紧→partition split 换新 slot(可能新 worker)继续写,reduce 读所有 split。治第6章补2.1 倾斜:巨型 partition 摊到多 slot/worker。

## 2.4 容错协议(对照 vanilla Spark)
| 故障 | vanilla Spark(第3章) | Celeborn |
|---|---|---|
| executor 丢 | 本地 shuffle 没了→FetchFailed→重算 map stage;INDETERMINATE 整链回滚 | shuffle 在远端 worker→不丢/无FetchFailed/无重算 |
| worker 故障(push中) | — | client 检测 push 失败→向 master 要新(primary,replica)→revive 继续推;replica 有先前数据不丢 |
| worker 故障(read时) | — | reduce 从 primary 读;primary 挂→failover replica |
| master 故障 | driver 单点 | Raft 复制元数据,切换不丢 |
| map 重跑重复数据 | MapStatus 按 attempt 去重 | commit 按 attempt/epoch 只保留一份有效 |

commit 阶段: map stage 结束 client 让 workers flush+finalize 报告已提交 partition;未提交→重推。同 Spark 对推测/重试 map 输出去重思路。

> ★ Celeborn 颠倒容错优先级: vanilla Spark 第一招是血缘重算(第0/3章),PB+spot 下 executor 一丢就 FetchFailed 反复回滚跑不完。**Celeborn 用 push 时双副本把节点故障在变成 FetchFailed 前就吸收**,只有 partition 的 primary+replica 同时丢(极罕见)才退回重算。**血缘重算从第一道防线降级成最后兜底。** 这是第0章"血缘是廉价容错"在云原生极端规模的边界: 节点 churn 高到血缘重算都扛不住时,就要在 shuffle 层引副本,把容错从"事后重算"提前到"事中冗余"。Celeborn 补的正是血缘容错在超高 churn 下的缺口。

## 2.5 Celeborn vs Magnet
- Magnet(补2): map 既写本地又推一份给 merger(node-local);本地副本是真相之源,merge best-effort
- Celeborn: map 不写本地,远端 worker 就是真相之源,有显式副本。存算分离/云原生设计起点更彻底

## 2.6 LLM 最佳实践
- 开复制(celeborn.client.push.replicate.enabled): 2x 存储/网络换"节点丢失零重算",反复回滚面前划算
- 调 partition split 阈值对抗倾斜
- worker 跑独立节点/存储层(与 spot 计算分开),计算 churn 时 worker 稳
- Master HA(Raft 3/5 节点)

> ★ 解耦递进: 本地文件(经典)→节点本地服务器(external shuffle service:服务 executor 本地文件,无副本仍绑节点)→远程复制服务(Celeborn:自己拥有 shuffle 数据,带副本,与计算彻底解耦)。每步削弱"shuffle 数据"与"计算节点"耦合: external shuffle service 让 executor 可死(第13章),数据还在那台机器盘上;Celeborn 让整台机器可死。尽头=把 shuffle 做成像 HDFS 一样独立/可靠/弹性的基础设施。推动力=大模型预训练"几千 spot 跑几小时节点不停 churn"的极端工况。

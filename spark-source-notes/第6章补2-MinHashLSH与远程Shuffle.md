# 第 6 章补2 — MinHash LSH 去重 与 远程 Shuffle 原理

> 对照源码:`mllib/.../ml/feature/{MinHashLSH,LSH}.scala`、`core/.../shuffle/ShuffleBlockPusher.scala`、`common/network-shuffle/.../RemoteBlockPushResolver.java`(apache/spark master)

---

# 一、MinHash LSH 去重在 Spark 上的完整实现

## 1.1 问题:近似重复
LLM 语料大量"近似重复"(转载/模板页/轻改写),精确去重(hash全等)抓不住。要 Jaccard 相似度>阈值即算重复。全量两两比较 O(N²),N=万亿不可能。**LSH 把 O(N²) 降到近线性。**

## 1.2 三步原理
1. **Shingling**: 文档→n-gram 集合→稀疏向量(每 shingle 一维)
2. **MinHash**: K 个哈希函数,每个对文档所有 shingle 取最小值→K 维签名。核心: 两文档签名某位相等概率=Jaccard 相似度
3. **LSH banding**: K 维切成 b 个 band(每 band r 行,K=b×r),任一 band 完全相同即"候选对"。调 b/r 控 S 曲线

## 1.3 Spark 源码
**签名 MinHashLSHModel.hashFunction(MinHashLSH.scala 63-72):**
```scala
65  hashValues = randCoefficients.map { (a,b) =>          // K 对随机系数
66    elems.nonZeroIterator.map { (i,_) => ((1L+i)*a+b) % HASH_PRIME }.min  // 每函数对所有shingle取min
```
**Jaccard keyDistance(75-109):** 双指针求交集,1-交集/并集。
**候选对 approxSimilarityJoin(LSH.scala 219-297)——巨型 shuffle:**
```scala
229  posexplode(col(outputCol))   // ① 每文档炸成 K 行:(entry=band号, hashValue=签名值)
286  explodedA.join(explodedB, ["entry","hashValue"])  // ② 在(band,签名)等值join ← all-to-all shuffle,任一band相同即候选对
287    .distinct()                // ③ 去重候选对 ← 又一shuffle
290  keyDistance 算精确Jaccard → 296 filter < threshold
```
> ★ MLlib 局限: banding 是 r=1(numHashTables=b),本质"OR-only"(任一hash table相等即候选),S曲线平缓→召回太多假候选对或漏真重复。**工业级几乎都自己实现 b×r banding**(r个签名拼成band key再hash)才能调陡S曲线。PB级直接套 approxSimilarityJoin 会被假候选对淹没。API能跑≠适配你的规模。

## 1.4 为何 shuffle+倾斜地狱
- explode 把 N→N×K 行,数据膨胀 K 倍
- join on (band,签名值) 高频签名桶爆炸(boilerplate 对应签名被几亿文档共享挤进一分区,第3/4章长尾)
- 巨桶产生 O(桶²)候选对,候选对也爆炸
- distinct 再全量 shuffle

## 1.5 候选对之后:连通分量(为何必须 checkpoint)
候选对(docA,docB)成图(边=相似)。去重需把传递相似聚成簇(A~B,B~C⟹同簇),每簇留一个=**连通分量**。Spark 用 GraphX/GraphFrames 或迭代标签传播(每轮节点标签=邻居最小,shuffle传播到收敛)。
```
连通分量=迭代算法,每轮一shuffle,血缘线性增长
跑20轮→血缘叠20层→driver血缘对象堆积→失败全量重算→StackOverflowError(第0章0.4!)
```
> ★ LSH 去重在 Spark 最大工程坑,解药是第0章伏笔: 迭代算法血缘无限增长(第0章0.4"几万层血缘爆栈"真实发生)。**必须每N轮 checkpoint 斩断血缘**(第0章2.7/第3章)。不checkpoint=driver OOM/一失败从头重算/栈溢出。第0章 checkpoint 还是抽象概念,到 LSH 去重是作业能否跑完的生死线。

## 1.6 实战调优
- 自己实现 b×r banding(别只用 MLlib OR-only),按阈值反推 b/r
- transform 签名结果 cache 避免重复算
- 倾斜: 统计并过滤超大桶(几亿文档的 band 多是 boilerplate,当垃圾滤或单独处理)或加盐
- 候选对去重用 reduceByKey/dropDuplicates 别 groupByKey(第2章OOM)
- 连通分量必周期 checkpoint(每几轮)
- LSH 整阶段前后各 checkpoint(全管线最贵最易失败)

---

# 二、Push-based shuffle(Magnet)与 Celeborn

## 2.1 pull-based 病根(第6章)
经典 shuffle=pull: map 写本地盘(data+index),reduce 主动去每个 map 拉自己那份。四病:
1. reduce 从 M 个 map 各拉小块→M 次小IO+大量小块
2. 小随机读(每块是 data 文件一小段,HDD惨)
3. 容错脆: map executor 挂→shuffle 全没→FetchFailed(第3章)
4. =LLM spot 噩梦(第6章补2.2)

## 2.2 Push-based shuffle(Magnet,SPARK-30602)
核心: **map 写完本地后,额外把块"推"到一批 merger(通常 external shuffle service);merger 把不同 map 但同一 reduce 分区的块提前合并成大 merged block;reduce 优先读 merged 大块。**
```scala
// ShuffleBlockPusher.initiateBlockPush(105-129):
117  prepareBlockPushRequests(..., dep.getMergerLocs, ...)  // 按 merger 位置分组打包(mergerLocs 来自第3章 prepareShuffleServicesForShuffleMapStage[1513])
121  pushRequests ++= Utils.randomize(requests)             // 打乱顺序避免多mapper同时推同段造成merger冲突
125  submitTask(() => tryPushUpToMax())                     // 异步推不阻塞map
```
- merger端 RemoteBlockPushResolver: 按(shuffleId,reduceId)把推来的块顺序append到 merged 文件(AppShufflePartitionInfo);多map推同分区→都append同文件
- **冲突 best-effort**: 并发推用先到先写/后到延迟跳过避免写坏;**推失败/冲突块 reduce fallback 回 pull**
- map stage 结束 driver finalize(markShuffleMergeFinalized,第0章Dependency/第6章convertMapStatuses的mergeStatuses)
收益: reduce 从拉M小块变读1大merged块(大顺序读);merged副本→map挂了reduce仍可读→增容错。
> ★ "合并失败fallback回pull"=第8/10章价值观: 优化必有正确退路。push-merge是best-effort,正确性由原始pull兜底→可安全默认开启。同codegen fallback(第8)/AQE成本守卫(第10)同一纪律。
但 Magnet merged 文件仍存节点本地盘、仍和节点绑定→引出 Celeborn。

## 2.3 Celeborn(remote/disaggregated shuffle)
Magnet=map写本地+推一份;**Celeborn 更激进: map 不写本地盘,直接 push 到独立 Celeborn 集群;worker 按 reduce 分区聚合成文件存 Celeborn 自己存储(本地盘/HDFS/对象存储,可副本);reduce 从 Celeborn 顺序读。**
架构: Celeborn Master(类RM,管worker与分区分配)+Workers(收push/按partition聚合/存储/服务read)。
- map: 每partition数据push给负责worker→worker把该partition来自所有map的数据append成文件(可副本)
- reduce: 向Celeborn要"我这partition",顺序读一个大文件
云原生/spot 根本解(直击第6章补2.2):
1. **compute与shuffle完全解耦**: executor抢占/缩容/漂移shuffle都在Celeborn不丢→几乎消灭FetchFailed与INDETERMINATE整链回滚
2. 随机写变顺序写(按partition顺序append+顺序读)
3. 副本/可靠存储容忍worker故障
4. K8s友好: executor变无状态可任意伸缩抢占
5. 缓解小文件/磁盘压力(聚合后文件少而大)
代价: 多一个Celeborn集群运维+多一跳网络。但PB级+spot远小于FetchFailed反复回滚代价。

## 2.4 三代对照
| | pull(经典第6章) | push/Magnet | Celeborn(remote) |
|---|---|---|---|
| map写哪 | 本地盘 | 本地盘+推一份merger | 直接push Celeborn不写本地 |
| reduce读 | 从M个map拉M小块 | 优先merged大块,失败fallback pull | Celeborn顺序读大文件 |
| executor挂 | shuffle丢→FetchFailed→重算 | merged副本部分容忍 | shuffle在Celeborn不丢 |
| IO | 小随机读 | 大顺序读 | 顺序append+顺序读 |
| spot/K8s | 脆 | 改善 | 根本解(compute无状态) |

> ★ 三代演进一以贯之: **把"小/多/随机/与节点绑定"的 shuffle IO 变成"大/少/顺序/与节点解耦"。** Hash→Sort 单机层合并(M×R→2M文件+索引);Magnet 集群层合并(reduce的M块→1 merged块);Celeborn 整个shuffle状态搬离compute。三代回答同一问题: shuffle数据怎么组织才能既快又不怕节点丢失。LLM的PB级+spot把"不怕节点丢失"推到极致(8小时几千spot的去重作业,经典pull几乎跑不完),催生Celeborn成LLM数据管线标配。从第6章"M×R文件爆炸"到大模型时代解耦shuffle,是同一条工程主线在不同规模的连续展开。

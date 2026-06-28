# 第 6 章补5 — VertexRDD 索引实现 与 Spark RAPIDS GPU 加速

> 对照源码:`graphx/.../impl/{VertexPartitionBaseOps,VertexRDDImpl}.scala`(apache/spark master);spark-rapids 据公开设计(NVIDIA 独立插件,非本仓库)

---

# 一、VertexRDD/EdgeRDD 索引:迭代 join 为何便宜

## 1.1 VertexPartition 三件套(`VertexPartitionBaseOps.scala`)
每顶点分区由三个并列结构组成:
- index: VertexIdToIndexMap(OpenHashSet),vertexId → 数组位置
- values: Array[VD],第 i 位置的顶点属性
- mask: BitSet,哪些位置存在/活跃

## 1.2 核心魔法:共享 index 跳过哈希与 shuffle
leftJoin/diff/innerJoin/minus 全是同一模式:
```scala
129  if (self.index != other.index) {            // 索引不同→慢路径
131    leftJoin(createUsingIndex(other.iterator))(f)  // 先把 other 重建到 self 索引
132  } else {                                     // 索引相同→快路径
135    var i = self.mask.nextSetBit(0)
138    newValues(i) = f(self.index.getValue(i), self.values(i), otherV)  // 按位置走数组+位图,无哈希无shuffle
```
aggregateUsingIndex(215-223): 收到消息按 getPos(vid) 塞进现有索引位置→**产出 VertexRDD 与输入顶点共享同一索引**。
> ★ 接 Pregel(补3): messages=aggregateUsingIndex 与顶点索引对齐→ g.joinVertices(messages) 命中快路径(索引相同)→不shuffle只走数组→每超步顶点更新是 O(活跃顶点)数组操作而非哈希join。补4=边侧少动数据,此处=点侧join免哈希免shuffle,两层合起来才是 GraphX 万亿边迭代的真因。
> ★ mask(BitSet)结构共享: filter/minus/innerJoin 不重建 index/values,只翻转位图(95 mask.andNot/160 mask&)。衍生 VertexPartition 物理共享同一 index+values,只 mask 不同=IndexedRDD 式原地更新(不可变+结构共享,同持久化数据结构思想)。

## 1.3 分区与索引策略
- VertexRDD 按 vertexId 哈希分区→两 VertexRDD 天然 co-partitioned→join 免 shuffle(leftZipJoin 分区对分区 zip);diff(增量复制用)=共享索引+位图比较,极廉价
- EdgeRDD 的 EdgePartition: 边列式、按 src 排序、src 聚簇索引(补4 1.4 aggregateMessagesIndexScan"只扫活跃边"所依赖);本地存引用到的顶点属性
一句话: VertexRDD 按vid共分区(join免shuffle)+分区内复用索引(衍生RDD按位置数组join免哈希)+EdgePartition列式+src聚簇索引(只扫活跃边)+不可变结构共享(操作只改mask/values)。把"迭代图算法"变成"一串共分区索引对齐的join + 每超步一次小shuffle"。

---

# 二、Spark RAPIDS:把算子搬到 GPU(NVIDIA 独立插件,据公开设计)

## 2.1 怎么挂进 Spark:复用第9章插件点
spark-rapids 不改 Spark 源码,通过列式插件 API(SparkPlugin/ColumnarRule)挂入——即第9章 prepareForExecution 那批规则的扩展点(2.6 ApplyColumnarRulesAndInsertTransitions)。物理计划生成后拦截,把支持的算子换 GPU 版: FilterExec→GpuFilterExec、HashAggregateExec→GpuHashAggregateExec、ShuffleExchangeExec→GpuShuffleExchangeExec。
> ★ 同第8章 codegen(CollapseCodegenStages)、第10章 AQE(InsertAdaptiveSparkPlan)同一机制——往第9章 prepareForExecution 规则列表插一条做算子替换。三种加速共存: Tungsten(第8章,CPU行式,绕JVM对象+codegen)/AQE(第10章,运行时改计划)/spark-rapids(GPU列式算子)。一个物理规则扩展点养活整个加速生态。

## 2.2 列式是前提:与 Tungsten 分野
GPU 要列式(对整列 SIMT 并行),Tungsten 的 UnsafeRow 是行式。spark-rapids 绕开 codegen/UnsafeRow 全程列式: ColumnarBatch(第8章 supportsColumnar)→cuDF(GPU列式dataframe,Arrow兼容)→GPU kernel。
- 行↔列转换有成本: GPU 算不了的 fallback CPU,插 GpuColumnarToRow/GpuRowToColumnar(GPU↔CPU拷贝)。让 GPU 算子连成长串减少转换。
- 又是"优化必有fallback"(第8/10章): 不支持算子降级回 CPU 行式不影响正确性。

## 2.3 GPU Shuffle:与 Celeborn 正交的另一根轴
spark-rapids 有基于 UCX/RDMA 的 GPU shuffle: 数据尽量留显存,NVLink/RDMA GPU→GPU 直传,避免 GPU→CPU→网络→CPU→GPU 拷贝。
> ★ RAPIDS shuffle 与 Celeborn 优化 shuffle 两个正交维度: Celeborn=容错与解耦(副本/存算分离,解 spot churn); RAPIDS shuffle=吞吐(数据不下GPU走RDMA)。一个让 shuffle"不怕节点丢失",一个"更快"。先进 LLM 管线可两者并用。理解"shuffle 可从容错和吞吐两独立方向优化",看任何 shuffle 方案就知归哪根轴。

## 2.4 LLM 数据准备适用面与边界(诚实)
**适合 GPU**: 大规模列式变换——filter/project/hash(精确去重哈希!)/join/sort,尤其 Parquet 列式数据。CPU 密集过滤/哈希阶段 GPU 常数倍吞吐。
**边界(别神化)**:
- UDF 不自动上 GPU: Scala/Python 自定义 UDF、正则默认 CPU(除非 RAPIDS UDF/cuDF 化 pandas UDF)。LLM 文本清洗大量定制 UDF/正则→不改写享受不到 GPU,最大现实落差
- 显存小(几十GB远小于主机内存)→batch/spill 要调;倾斜(巨型group)易 OOM GPU
- 只对 SQL/DataFrame 列式算子有效,任意 RDD 代码无能为力
- 经济性: GPU 贵,只在负载 GPU 友好且吞吐受限时划算
一句话: spark-rapids="把 Spark SQL 列式算子从 CPU 搬到 GPU"的第三种加速,经第9章物理规则插件点接入、走列式 cuDF、保留 CPU fallback;加速哈希/过滤/join/sort,但对定制 UDF/正则无能为力,受显存与倾斜约束。

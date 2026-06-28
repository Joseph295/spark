# 第 2 章 — Transformation 如何只建血缘:KV 算子与分区器

> 对照源码:`core/.../rdd/PairRDDFunctions.scala`、`core/.../Partitioner.scala`、`core/.../util/random/SamplingUtils.scala`(apache/spark master)

---

## 一、宏观:三个设计决策

1. **所有按 key 聚合的算子都收敛到 `combineByKeyWithClassTag`。** reduceByKey/groupByKey/aggregateByKey/foldByKey 都是它的特例,差别只在三个函数 + 一个布尔 `mapSideCombine`。
2. **map 端预聚合(map-side combine)是 reduceByKey 碾压 groupByKey 的唯一原因**,源码里就是一个布尔参数。
3. **分区器是"key→分区号"的确定性纯函数**,决定 shuffle 每条记录去哪个 reduce 分区(容错重算依赖其可重现)。`RangePartitioner` 藏了水塘抽样 + 两阶段采样。

---

## 二、细节

### 2.1 统一抽象 combineByKeyWithClassTag(`PairRDDFunctions.scala` 72–103)
三个函数(对应 MR combiner 三段):
```scala
73  createCombiner: V => C       // 首次遇到某 key:value → 累加器
74  mergeValue: (C, V) => C      // 同分区内同 key:并入累加器
75  mergeCombiners: (C, C) => C  // 跨分区(reduce 端):合并两个累加器
77  mapSideCombine: Boolean = true
```
- **79** `require(mergeCombiners != null)`:reduce 端合并是聚合语义下限,不可省。
- **92–96 免 shuffle 短路**:`if (self.partitioner == Some(partitioner))` 父 RDD 已按目标分区器分好 → 直接 `mapPartitions` 本地聚合,`preservesPartitioning=true`,**不 shuffle**。否则(97–102)`new ShuffledRDD` 走宽依赖。→ "先 partitionBy 再多次聚合复用分区器"省 shuffle 的原理。
- ShuffledRDD 链式 `.setAggregator().setMapSideCombine()` → 信息进入 ShuffleDependency,决定 shuffle 怎么写。

### 2.2 reduceByKey vs groupByKey
```scala
// reduceByKey 305-307:
combineByKeyWithClassTag[V]((v) => v, func, func, partitioner)   // func 复用两次 → 必须结合律+交换律
                                                                  // mapSideCombine 默认 true
// groupByKey 497-507:
createCombiner = (v) => CompactBuffer(v); ...; mapSideCombine = false   // 505 关键
// 498-500 注释:group 的 map 端聚合不减少 shuffle 数据量,反而建大 hash 表塞老年代 → 纯亏,关掉
```
> ★ reduceByKey 快不是算法不同,是 mapSideCombine=true:shuffle 数据量从"原始记录数"降到"map 端 distinct key 数"(即 MR combiner)。
> ★ groupByKey 危险(518-519 注释):单个 key 所有 value 必须放进内存,热点 key OOM。reduceByKey 边读边聚不囤积。

### 2.3 defaultPartitioner(`Partitioner.scala` 67–103)
- **85-87** 优先复用上游已有分区器(若够格或分区数 ≥ 默认并行度)→ 命中免 shuffle 短路。
- **89** 否则新建 `HashPartitioner(defaultNumPartitions)`。
- **77-81** defaultNumPartitions:设了 spark.default.parallelism 用它,否则取**上游最大分区数**(61-63 注释:max 最不易 OOM)。
- **98-103** isEligiblePartitioner:`log10(max) - log10(候选) < 1`,即"同数量级"才复用,防止把万分区数据挤进 5 分区造成倾斜。

### 2.4 HashPartitioner(114–132)
```scala
119  getPartition(key) = key match { case null => 0; case _ => nonNegativeMod(key.hashCode, n) }
```
nonNegativeMod 保证非负(Java % 对负数返回负值,当下标越界)。
> ★ 110-112 注释:数组 hashCode 基于身份非内容 → 内容相同的数组分到不同分区,聚合错乱。2.1 的 80-86 显式异常拦截此坑。

### 2.5 RangePartitioner(176–324)—— 重头戏
**为何需要:** sortByKey 全局排序要求"分区 0 所有 key < 分区 1 ...",Hash 做不到。任务:找 N-1 个分界点 rangeBounds,把 key 空间切 N 段且数据量均衡。难点:不能读全量,必须抽样估分布。

**2.5.1 抽样规模(198-206):**
```scala
204  sampleSize = min(20 * partitions, 1e6)          // 总样本,封顶 100 万(防爆内存)
206  sampleSizePerPartition = ceil(3.0 * sampleSize / 输入分区数)   // ×3 过采样(防分区不均)
```

**2.5.2 sketch 每分区水塘抽样(335-348):**
```scala
342  reservoirSampleAndCount(iter, sampleSizePerPartition, seed)
345  .collect()    // ← 构造期触发真实 job!打破"transformation 全 lazy"
```
返回 (分区号, 分区总数 n, 样本数组)。

**2.5.3 水塘抽样算法详解(`SamplingUtils.scala` 33-70):**
问题:流式一遍扫描、总数 n 未知,等概率抽 k 个使每元素入选概率 = k/n。
```scala
40-45  前 k 个直接入池
57-67  第 l 个(l>k):replacementIndex = floor(rand * l)  // [0,l) 均匀
       if (replacementIndex < k) reservoir(replacementIndex) = item  // 概率 k/l 命中,随机替换一个旧样本
```
**正确性证明(裂项连乘):** 第 l 个元素最终留池概率
= (到达被选 k/l) × ∏_{j=l+1}^{n} (不被 j 覆盖 (j-1)/j)
= (k/l) × (l/n)  [中间裂项全消]
= **k/n**,与 l 无关。前 k 个同理。故 O(k) 内存、一遍扫描即无偏。
> ★ 56 行用 XORShiftRandom 非 JDK Random:每条未入池记录都要调一次随机数,JDK Random 的 synchronized+线性同余在亿级数据上慢且有锁竞争。
> ★ 341 种子 byteswap32(idx ^ shift<<16):分区独立 + 可复现(容错确定性)。

**2.5.4 倾斜二次采样(213-234):** 若 `fraction*n > sampleSizePerPartition`(分区采样不足)→ 标记 imbalanced → 用 PartitionPruningRDD 只对倾斜分区重采。两阶段:先廉价均匀采,发现倾斜再补采。样本带 weight = n/样本数(还原真实分布)。

**2.5.5 determineBounds 加权定界(358-387):** 带权样本排序后,沿累积权重每隔 step=总权重/分区数 取一个分界点;377 跳过重复值防空分区。本质按权重等分。

**2.5.6 getPartition(244-268):** rangeBounds ≤128 线性扫,>128 二分。小数据不上重武器。

**2.5.7 自定义序列化(289-323):** rangeBounds 可能很大,默认 Java 序列化慢且大 → 改用配置的 Kryo 等(299-303),小字段走默认。分区器随 task 发到每个 executor,序列化效率影响调度开销。

---

## 三、Trick 与 Bug

**① groupByKey 单 key OOM(518-519)。** 换 reduceByKey/aggregateByKey。
**② HashPartitioner + 数组 key 静默错误(110-112)。** 80-86 显式异常拦截。
**③ reduceByKey 的 func 必须结合律+交换律。** 306 把 func 同时当 mergeValue/mergeCombiners 且 map 端预聚合顺序不定。非交换 func(如减法)结果随分区变化。
**④ RangePartitioner 定义期触发 job(207/345 collect)。** sortByKey 看似 lazy 实则跑采样 job。启发:凡需"看一眼数据才能规划"的算子(范围分区、部分 AQE 决策)都在规划期偷跑 job。
**⑤ SPARK-22160 字节码 3 参构造器(183-188)。** 加带默认值的第 4 参在 Scala 源码兼容但 JVM 字节码不兼容,手写旧签名构造器保兼容。
**⑥ 倾斜两阶段采样(213-234)。** 通用抗倾斜思路,AQE 再现。

---

## 四、本章总纲
```
KV 算子 → combineByKeyWithClassTag(3函数 + mapSideCombine)  [72-103]
   ├─ 父分区器==目标 ⟹ 本地聚合免 shuffle [92-96]
   ├─ reduceByKey: func 复用, mapSideCombine=true [305-307]  ← 快
   └─ groupByKey: mapSideCombine=false, 单key全量入内存 [497-507] ← OOM

Partitioner (key→分区号, 确定性)
   ├─ defaultPartitioner: 优先复用上游(log差<1同数量级), 否则 Hash(max分区) [67-103]
   ├─ HashPartitioner: nonNegativeMod(hashCode); 数组key坑 [114-132]
   └─ RangePartitioner [176-324]:
        构造期 collect 采样 job [207] → 水塘抽样(k/l替换, 裂项证 k/n) [SamplingUtils 33-70]
        → 倾斜二次采样 [213-234] → 加权定界 [358-387] → getPartition ≤128线性/>128二分 [244-268]
        → 自定义序列化省开销 [289-323]
```

# 第 0 章 — RDD:一个为"容错"而生的抽象

> 对照源码:`core/src/main/scala/org/apache/spark/rdd/RDD.scala`、`core/src/main/scala/org/apache/spark/Dependency.scala`、`core/src/main/scala/org/apache/spark/rdd/MapPartitionsRDD.scala`(apache/spark master 分支)

---

## 一、宏观:作者到底在解决什么问题

要理解 RDD,得先理解作者当年面对的**真正矛盾**,而这个矛盾不是"MR 太慢"这么肤浅。

### 1.1 失败的前辈:分布式共享内存(DSM)

在 RDD 之前,让集群内存"被用起来"的主流思路是 **DSM(Distributed Shared Memory)**:把集群内存抽象成一块全局可读写的地址空间,**细粒度(fine-grained)**随机读写。

致命问题:**容错做不起来**。允许细粒度随机写,要容错就只有两条路:

1. **跨机器复制每一次写**(像数据库 replication)——网络带宽被写流量吃光;
2. **周期性 checkpoint 全部内存**——又回到"落盘"老路,且极其昂贵。

MapReduce 是 DSM 的反面极端:**牺牲表达力**(只有 map/reduce 两段),换来简单容错(每步落盘 + 重算)。MR 慢的本质,是它用"全程落盘"这个最笨但最可靠的方式换容错。

### 1.2 RDD 的核心洞察:用"粗粒度变换"换"廉价容错"

Matei 的关键洞察:

> **限制编程模型只能做"粗粒度变换"(coarse-grained transformation),反而能换来"细粒度、廉价的容错"。**

粗粒度变换 = `map`/`filter`/`reduceByKey` 这种"对整个数据集施加同一个算子"的操作。你不能"把第 173 号记录改成 5",只能"把所有记录都 +1"。

为什么关键?**只要记下"对哪个父数据集、施加了哪个算子"(很小的信息),就能在任意分区丢失时只重算那一个分区——既不复制数据,也不 checkpoint。** 这条"算子序列"就是**血缘(lineage)**。

> ★ Insight
> - `dependencies`(我依赖谁)和 `compute`(怎么从父亲算出我)合起来 = "重算这个分区的全部信息"。**容错就是 RDD 这个抽象被发明出来的唯一理由**,其他(内存驻留、流水线、DAG)都是顺带红利。
> - 很多限制因此豁然开朗:为什么不可变?为什么 `compute` 要确定性?为什么不能随机写?——任何破坏"分区可被廉价、精确重算"的东西,都是在拆容错的台。

---

## 二、细节:逐段对照源码

### 2.1 类骨架与两个构造器参数(`RDD.scala` 84–104)

```scala
84   abstract class RDD[T: ClassTag](
85       @transient private var _sc: SparkContext,
86       @transient private var deps: Seq[Dependency[_]]
87     ) extends Serializable with Logging {
102    def this(@transient oneParent: RDD[_]) =
103      this(oneParent.context, List(new OneToOneDependency(oneParent)))
```

- **84–87**:主构造器只要 `_sc` + `deps`。**一个 RDD 的本体 = "上下文 + 血缘"**。
- **85–86 的 `@transient private var`**:
  - `@transient`:序列化发到 executor 时不带 `_sc`/`deps`(executor 只算自己那个分区,完整血缘是 driver 的东西)。
  - `private var`(非 `val`):checkpoint 后要把 `deps` 置 null,必须可变。
- **102–104**:"一个父亲、一对一依赖"的语法糖,自动包 `OneToOneDependency`。

### 2.2 五个抽象方法(`RDD.scala` 107–139)

```scala
116  def compute(split, context): Iterator[T]                // 抽象,必须实现
125  protected def getPartitions: Array[Partition]           // 抽象,必须实现
131  protected def getDependencies: Seq[Dependency[_]] = deps // 默认返回构造器 deps
136  protected def getPreferredLocations(split): Seq[String] = Nil  // 默认无偏好
139  @transient val partitioner: Option[Partitioner] = None  // 默认无分区器
```

- **116 `compute` 返回 `Iterator[T]`**:拉模型(pull-based),下游 `next()` 驱动上游算一条 → 算子在单线程串成一条"火车"。与 MR 的推+落盘+拉相反。
- **131 默认 `= deps`**:多数 RDD 不重写;`ShuffledRDD`/`CoGroupedRDD` 重写以声明宽依赖。
- **139 `@transient val`**:分区器只在 driver 规划 shuffle 时用,不序列化到 executor。

### 2.3 依赖体系(`Dependency.scala` 全文)—— Stage 划分的唯一依据

```scala
40  abstract class Dependency[T] { def rdd: RDD[T] }                      // 根
51  abstract class NarrowDependency[T](_rdd) extends Dependency[T] {
57    def getParents(partitionId: Int): Seq[Int]                         // 子分区 → 父分区号
60  }
235 class OneToOneDependency  → getParents(i) = List(i)                  // map/filter
249 class RangeDependency     → 区间平移映射                              // union
```

- **47–48 注释点题**:"Narrow dependencies allow for pipelined execution." 窄依赖 = 子分区只依赖父的少数、可在编译期确定的分区。
- **57 `getParents` 是窄依赖的灵魂**:映射确定、无需 shuffle → 父子分区可在同机同 task 连续算完 = 流水线。

**宽依赖 `ShuffleDependency`(79–227)—— 构造即副作用的重型对象:**

```scala
80    @transient private val _rdd                                  // executor 端不需要父 RDD
89    if (mapSideCombine) require(aggregator.isDefined, ...)        // map 端预聚合必须有 aggregator
101   val shuffleId = _rdd.context.newShuffleId()                   // 副作用1:分配全局 shuffleId
103   val shuffleHandle = ...shuffleManager.registerShuffle(...)    // 副作用2:向 ShuffleManager 注册
213   if (numPartitions * partitioner.numPartitions > (1L<<30))     // OOM 预警(见坑④)
225   ...cleaner.registerShuffleForCleanup(this)                    // 副作用3
```

> ★ Insight
> - 窄/宽分界 = `getParents` 能不能写出来。宽依赖**没有** `getParents`——reduce 分区的数据来自所有父分区的一部分,映射依赖运行时每条记录的 key,编译期无法确定,必须物化(落 shuffle 文件)。**"能不能写 getParents" = pipeline 与 barrier 的物理边界。**
> - 写下 `reduceByKey` 那一刻(driver 端)就 new 了 `ShuffleDependency`,**立刻分配 shuffleId 并向 ShuffleManager 注册** —— shuffle 的"登记"远早于真正计算。

### 2.4 `map` 如何"记账"(`RDD.scala` 424–427 + `MapPartitionsRDD.scala` 47–52)

```scala
// RDD.scala
424  def map[U](f: T => U): RDD[U] = withScope {
425    val cleanF = sc.clean(f)                                      // 闭包清洗(见坑①)
426    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))  // 只是 new,什么都不算
427  }

// MapPartitionsRDD.scala
47   override val partitioner = if (preservesPartitioning) firstParent.partitioner else None
49   override def getPartitions = firstParent[T].partitions          // 复用父分区
51   override def compute(split, context) =
52     f(context, split.index, firstParent[T].iterator(split, context))  // 拿父 iterator 套自己的 f
```

- **51–52 是流水线的字面实现**:`compute` = 父 RDD 同分区的 iterator 套上 `f`。调父亲的 `iterator`(非 `compute`),因为父亲可能被 cache/checkpoint 了,必须走统一入口。
- **47 `preservesPartitioning` 默认 false**:普通 `map` 可能改 key → 分区器失效;只有 `mapValues` 等保证不动 key 才传 true 保留分区器(下游 join 可免 shuffle)。

> ★ Insight
> - `MapPartitionsRDD` 是"万能 RDD":map/flatMap/filter/mapPartitions/glom 全复用它,只是传入的 `f` 不同。**用"一个通用 RDD + 不同函数"消灭了为每个算子写一个类的重复。**
> - 62–68 的 `getOutputDeterministicLevel`:order-sensitive 函数 + UNORDERED 父 → INDETERMINATE,标记一路传到 DAGScheduler,决定重算策略(SPARK-23207 落点)。

### 2.5 惰性物化:双检锁(`RDD.scala` 260–311)

```scala
260  final def dependencies = checkpointRDD.map(...).getOrElse {
262    if (dependencies_ == null) stateLock.synchronized {
264      if (dependencies_ == null) dependencies_ = getDependencies   // 只调一次
296  final def partitions = checkpointRDD.map(_.partitions).getOrElse {
301    ... partitions_ = getPartitions
302    partitions_.zipWithIndex.foreach { case (p, i) => require(p.index == i, ...) }  // 强校验不变式
```

- 两方法都 `final`:模板方法模式——外层 final 管缓存/加锁/checkpoint 短路,内层 `get*` 管纯逻辑。
- **261/297 checkpointRDD 短路**:已 checkpoint 则直接指向 checkpoint 文件,绕过原血缘。
- **双检锁**:`getPartitions`/`getDependencies` 只调一次(HadoopRDD 要发 RPC 问 NameNode,RangePartitioner 要抽样,重复调既贵又破坏确定性)。
- **302–304 require**:强制 `partitions(i).index == i`,不变式被破坏时第一现场 fail-fast。

### 2.6 计算入口:`iterator` 模板 + cache 路径(`RDD.scala` 334–409)

```scala
334  final def iterator(split, context) =
335    if (storageLevel != NONE) getOrCompute(split, context)        // 设了 cache
337    else computeOrReadCheckpoint(split, context)
369  def computeOrReadCheckpoint(split, context) =
371    if (isCheckpointedAndMaterialized) firstParent.iterator(...)  // 读 checkpoint
373    else compute(split, context)                                  // 真算
381  def getOrCompute(partition, context) = {
382    val blockId = RDDBlockId(id, partition.index)                 // 缓存块身份 = rddId + 分区号
385    SparkEnv.get.blockManager.getOrElseUpdateRDDBlock(...)        // get-or-compute 原子操作
```

- **384–385 注释**:executor 上必须用 `SparkEnv.get` 而非 `sc.env`(sc 只在 driver,executor 上 `_sc` 是 @transient 没传过来 → 否则 NPE)。
- **385 `getOrElseUpdateRDDBlock`**:读缓存/写缓存/算 三合一原子,避免并发 task 重复算同一块。
- **396/403/407 `InterruptibleIterator`**:task 被 kill(stage 重试/推测执行)时在 `next()` 检查中断标志并抛出,让长任务及时停。

### 2.7 checkpoint:安全切断血缘(`RDD.scala` 1941–1980)

```scala
1941  def doCheckpoint() =                                            // job 跑完后由 SC 调用
1945    if (checkpointData.isDefined) checkpointData.get.checkpoint()  // 真正写可靠存储
1955    else dependencies.foreach(_.rdd.doCheckpoint())               // 递归问父亲
1965  def markCheckpointed() = stateLock.synchronized {
1966    legacyDependencies = new WeakReference(dependencies_)          // 弱引用留旧血缘
1967    clearDependencies()    // 1979: dependencies_ = null
1968    partitions_ = null
1969    deps = null            // 连构造器参数也忘掉
```

- **1937–1939 注释**:`doCheckpoint` 在 job 完成后调用 → 数据算两遍(故官方建议 checkpoint 前先 cache)。
- **1965–1970 切断血缘**:deps/partitions 全置 null,之后 `dependencies` 走 checkpointRDD 短路指向文件,前面的父 RDD 可被 GC。
- **1966 WeakReference**:旧血缘放弱引用——INDETERMINATE 重算判断时还想看一眼原血缘,但不能强引用住它(否则省内存目的白费)。"想看你但绝不阻止你被回收"。

> ★ Insight
> cache 改"读取路径"(iterator 先查 BlockManager),血缘不动 → 丢了能重算;checkpoint 改"血缘本身"(markCheckpointed 置 null 指向可靠文件)→ 丢了真没了。所以 checkpoint 必须写 HDFS,cache 可只在内存。

---

## 三、Trick 与 Bug 汇总

**① 闭包清洗 `sc.clean(f)`(425)。** Scala 闭包隐式持有外部类 `$outer` 引用 → 序列化时拖走整个外部对象,甚至 `NotSerializableException`。`ClosureCleaner` 用字节码分析把没用到的 `$outer` 置空。这是 `Task not serializable` 的根因与解药。

**② 用 `stateLock` 而非 `synchronized(this)`(229–242)。** RDD 用户可见,用户可能加自己的锁,内部锁 `this` 会与之混用导致死锁。**写库铁律:永不在用户能拿到引用的对象上 synchronized。**

**③ `ShuffleDependency` 构造即副作用(101–104)。** new 宽依赖立刻分配全局 shuffleId 并注册到 ShuffleManager。血缘构建期(还没 action)就已登记 shuffle,可解释"还没 action 就状态膨胀"。

**④ shuffle 块爆炸 OOM 预警(213–223)。**
```scala
if (numPartitions.toLong * partitioner.numPartitions.toLong > (1L << 30)) logWarning(...)
```
块总数 = map 分区 × reduce 分区。超 ~10 亿时 `HighlyCompressedMapStatus` 位图膨胀到 128MB+,撑爆 driver。启发:`spark.sql.shuffle.partitions` 不是越大越好,它与 map 数相乘会爆 driver 元数据。

**⑤ SPARK-23207:INDETERMINATE 重算的数据正确性 bug(62–68 落点)。** `repartition`(round-robin)后聚合,某 reduce 因 FetchFailed 失败 → 默认只重算丢失 map 输出 → 重算时记录分配与首次不同 → **静默重复/丢数据**。修复:`outputDeterministicLevel`,INDETERMINATE stage 失败时**强制全量重算**。启发:"部分重试"的正确性依赖"计算确定性",假设破裂即静默数据损坏——最危险的 bug。

**⑥ SPARK-5063:嵌套 RDD 警告(89–92)。** `rdd1.map(x => rdd2.filter(...))` 不可能工作(rdd2 是 driver 对象,executor 上 sc 为 null)。作者选 warning 而非异常(怕误伤定义了但从不执行的程序)。API 设计的分寸感。

---

## 四、本章总纲

```
设计意图: 限制为"粗粒度变换" ⟹ 用血缘(dependencies+compute)做廉价容错  ← 一切的源头
   ├─ 数据结构: RDD = (_sc, deps) + 五抽象  [84-139]，compute 返回 Iterator ⟹ 拉模型流水线
   ├─ 血缘: 窄依赖有 getParents(可流水线) / 宽依赖无(必落 shuffle)  [Dependency.scala]
   │        宽依赖构造即注册 shuffle  [101-104]
   ├─ 记账: map = clean闭包 + new MapPartitionsRDD  [424-427, MPRDD 47-52]
   ├─ 惰性: partitions/dependencies 双检锁只算一次  [260-311]
   ├─ 计算: iterator(final模板) → cache? → checkpoint? → compute  [334-409]
   └─ 容错: cache改读取路径(留血缘) / checkpoint切血缘(markCheckpointed置null)  [1941-1980]
```

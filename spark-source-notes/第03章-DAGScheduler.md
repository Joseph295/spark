# 第 3 章 — DAGScheduler:从血缘到 Stage 的惊险一跃

> 对照源码:`core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala`(3221 行,apache/spark master)

---

## 一、宏观

`DAGScheduler` 架在"用户血缘 DAG"与"executor 只认的 task"之间的鸿沟上,只做一件事:
> 把以"算子"为节点的血缘 DAG,翻译成以"Stage"为节点的执行 DAG,按依赖顺序喂给 TaskScheduler。

三层调度的最上层:
```
DAGScheduler   —— 逻辑层:Stage,谁依赖谁/谁先跑/谁挂了重算  (本章)
TaskScheduler  —— 物理层:Task 发到哪个 executor          (第4章)
SchedulerBackend —— 资源层:和 cluster manager 要资源       (第4章)
```

**核心信念:Stage 的边界 = shuffle 的边界。** 连续窄依赖可流水线 → 打包进同一 Stage;遇宽依赖(ShuffleDependency)流水线被截断 → 切新 Stage。
- **ShuffleMapStage**:产出 shuffle 数据(对应 MR Map 端,可多层)
- **ResultStage**:job 最后一个 Stage,产出交给 action(对应 MR Reduce 端)

---

## 二、细节:跟着一个 action 走到底

### 2.1 入口 runJob:同步外壳 + 异步内核(930–1013)
```scala
989  runJob: val waiter = submitJob(...); awaitReady(waiter.completionFuture)  // 阻塞等
930  submitJob:
938-943  边界检查(分区号越界抛异常)
948      eagerlyComputePartitionsForRddAndAncestors(rdd)   // SPARK-23626:提前算所有分区
968      eventProcessLoop.post(JobSubmitted(...))           // 把"提交job"变成事件投递,立即返回
972      waiter
```

### 2.2 单线程事件循环:串行换无锁(3094–3200)
所有核心状态(shuffleIdToMapStage/stageIdToStage/waitingStages/runningStages/failedStages)都是**无锁**可变集合。
```scala
3111  doOnReceive(event) = event match {
3112    case JobSubmitted => handleJobSubmitted
3164    case CompletionEvent => handleTaskCompletion
3173    case ResubmitFailedStages => resubmitFailedStages   // 20+ 种事件
```
所有事件串进一个队列,唯一线程顺序处理 → 状态集合无需加锁(任何时刻只有一个线程碰它们)。整个调度=状态机。
> ★ 代价1:处理函数不能干慢活(故 SPARK-23626 提前算分区)。
> ★ 代价2:3186 onError 一旦事件循环抛异常 → sc.stopInNewThread() 整体重启(单点串行无优雅降级)。

### 2.3 反向回溯切 Stage(1320, 646, 715, 470)
```scala
1342  handleJobSubmitted → createResultStage(finalRDD, ...)
646   createResultStage:
652     val (shuffleDeps, _) = getShuffleDependenciesAndResourceProfiles(rdd)
657     val parents = getOrCreateParentStages(shuffleDeps, jobId)
659     new ResultStage(...)
```
**getShuffleDependencies(715):只找最近一圈宽依赖,遇 shuffle 即停**(A<--B<--C 用 C 调只返回 B<--C):
```scala
727  toVisit.dependencies.foreach {
728    case shuffleDep => parents += shuffleDep            // 宽依赖:记下,到此为止
730    case dependency => waitingForVisit.prepend(dependency.rdd)  // 窄依赖:继续上溯(同一stage)
```
**getOrCreateShuffleMapStage(470):递归剥洋葱建祖先 stage:**
```scala
473  shuffleIdToMapStage.get(shuffleId) match {
474    case Some(stage) => stage                            // 全局缓存,同 shuffle 只建一次
477    case None => getMissingAncestorShuffleDependencies(...).foreach(createShuffleMapStage(_))  // 先建更老祖先
490               createShuffleMapStage(shuffleDep, ...)    // 最后建自己
```
createShuffleMapStage 还会 `mapOutputTracker.registerShuffle`(535-543)登记。
> ★ 遍历方法全用 ListBuffer 手工栈做迭代 BFS 而非递归(682-683/767-768 注释),防血缘数万层爆栈。

### 2.4 按序提交 submitStage 深度优先(1452–1482)
```scala
1465  val missing = getMissingParentStages(stage).sortBy(_.id)
1467  if (missing.isEmpty) submitMissingTasks(stage, jobId)       // 父就绪 → 提交自己
1471  else { missing.foreach(submitStage); waitingStages += stage } // 否则递归提交父,自己进等待区
```
getMissingParentStages(764)判"就绪":
```scala
774  val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)  // 先看 cache!cache了就不回溯父辈
783  if (!mapStage.isAvailable) missing += mapStage                  // 父 shuffle 输出不全 → 缺失
```

### 2.5 submitMissingTasks:Stage → Task(1547–1719)
**动作1 只算缺的(1561):** `findMissingPartitions` — Stage 级"部分重算"的秘密(成功的 map 分区不重算)。
**动作2 算本地性(1594-1603):** 每分区调 getPreferredLocs → taskIdToLocations(传给第4章延迟调度)。
**动作3 序列化+广播 task 二进制(1631-1656):**
```scala
1640  RDDCheckpointData.synchronized {                       // 防并发 checkpoint 改写
1644    ShuffleMapStage → serialize((rdd, shuffleDep))        // map 端发"怎么 shuffle"
1646    ResultStage     → serialize((rdd, func))              // result 端发用户函数
1656  taskBinary = sc.broadcast(taskBinaryBytes)             // 广播,非塞进每个task
```
- 广播原因(1626-1630注释):几千 task 共享一份 RDD,避免 N 份重复传输打爆网络。
- 每 task 独立反序列化副本:规避 Hadoop JobConf 非线程安全 → 用 CPU 换 task 间隔离(真实 bug 教育)。
- 用 closureSerializer(Java 序列化,第1章),因发的是带闭包对象。
**动作4 造 Task 提交(1675-1719):** 一分区一 ShuffleMapTask/ResultTask,带 locs + taskBinary → 打包 TaskSet → taskScheduler.submitTasks → 第4章。

### 2.6 数据本地性的传播 getPreferredLocs(3015–3063)
```scala
3037  cache 位置?      → return cached                       // ① 优先级最高
3042  RDD 自身偏好?    → return rddPrefs                      // ② HadoopRDD 知道 block 在哪
3050  case n: NarrowDependency => getPreferredLocsInternal(n.rdd, n.getParents(partition)...)  // ③ 沿窄依赖上溯继承
```
优先级:cache > 数据源 locality > 沿窄依赖递归继承父分区偏好。宽依赖处断(3059 啥都不做,shuffle 后无单一偏好)。
> ★ SPARK-695(3032):visited 集合防菱形血缘递归指数爆炸,压回线性。

### 2.7 高潮:FetchFailed 与 Stage 级容错(2022–2176)
reduce 拉不到 map 输出 → FetchFailed → 2022:
1. **忽略过气 attempt(2026):** 一次机器故障同时触发几十个 FetchFailed,旧 attempt 一律忽略。
2. **注销丢失 map 输出(2072-2074):** `mapOutputTracker.unregisterMapOutput` → 该 map 分区变回 missing → 重提时 findMissingPartitions 自然捞出重算。**容错=改账本+复用正常流程,状态机的优雅。**
3. **攒 200ms 再重提(RESUBMIT_TIMEOUT=3206):** 把一连串失败攒成一批一次性重提。
4. **INDETERMINATE 整链回滚(2131-2176)—— SPARK-23207 引爆点:**
```scala
2131  if (mapStage.isIndeterminate) {
2140    def collectStagesToRollback(...)                      // Stage 只记父不记子!
2157    activeJobs.foreach(collectStagesToRollback(finalStage::Nil))  // 从 ResultStage 反向找所有下游
```
确定 map stage → 丢啥补啥(下游拿到数据不变);不确定 → 重算划分不同,新旧拼接错乱 → 必须连所有下游整体回滚重算;已 commit 无法回滚 → abort,报著名错误 "checkpoint the RDD before repartition"。
> ★ 确定性决定容错成本:确定→廉价补算;不确定→整链重算;不确定且已commit→失败无解。checkpoint(0章2.7)把不确定中间结果钉成确定文件,斩断不确定性传播。一句报错=RDD抽象+血缘+确定性+checkpoint 的合谋。

---

## 三、Trick 与 Bug
**① 单线程事件循环=无锁状态机(3094-3185)。** 串行换简单;处理函数不能慢;循环死则整体重启。
**② SPARK-23626:分区计算移出事件循环(948)。**
**③ 手工栈替代递归(682-683等)。** 血缘数万层防爆栈。
**④ SPARK-695:getPreferredLocs visited 去重(3032)。** 菱形血缘防指数爆炸。
**⑤ SPARK-13902:getOrCreateShuffleMapStage 并发创建竞态(480-485)。** 建前再判 !contains。
**⑥ task 二进制广播+每task独立反序列化(1626-1656)。** 省 N 份传输 + 规避 Hadoop JobConf 非线程安全。
**⑦ FetchFailed 忽略旧 attempt + 200ms 批量重提(2026,3206)。** 防故障风暴式重复重试。
**⑧ SPARK-23207:INDETERMINATE 整链回滚(2131-2176)。** 确定性决定容错成本,全书最深因果闭环。

---

## 四、本章总纲
```
action → runJob[989](同步外壳)
 └─ submitJob[930]: 边界检查 + 提前算分区(SPARK-23626) + post(JobSubmitted) → JobWaiter(异步)
      ↓ 单线程事件循环[3094] 无锁状态机
    handleJobSubmitted[1320]
      └─ createResultStage[646] ─反向回溯→ getShuffleDependencies(最近一圈宽依赖)[715]
             └─ getOrCreateShuffleMapStage[470] 递归建祖先,shuffleIdToMapStage 缓存
      └─ submitStage[1452] 深度优先:getMissingParentStages[764] 缺父?递归提交父+进waitingStages;不缺→
           submitMissingTasks[1547]: findMissingPartitions(只算缺) → getPreferredLocs[3015]
             → 序列化(rdd,dep)/(rdd,func)+广播taskBinary(每task独立反序列化隔离)[1631]
             → ShuffleMapTask/ResultTask → submitTasks(TaskSet)[1717] →→ 第4章
容错 FetchFailed[2022]:忽略旧attempt → MapOutputTracker注销丢失输出(变missing,复用提交) → 攒200ms批量重提
   → mapStage不确定:反向回溯整条下游链回滚重算;已commit则abort(SPARK-23207)
```

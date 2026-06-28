# 第 4 章 — TaskScheduler:资源 offer 模型与延迟调度的艺术

> 对照源码:`core/.../scheduler/TaskSchedulerImpl.scala`、`TaskSetManager.scala`、`cluster/CoarseGrainedSchedulerBackend.scala`(apache/spark master)

---

## 一、宏观:一次方向的倒转

DAGScheduler 回答"要算什么"(交来 TaskSet),本章回答"在哪台机器上算"。

**方向倒转(vs YARN):**
```
请求式 (YARN):  应用说"我要节点Y的container" → RM 去找 → 可能等很久
供给式 (Spark): executor说"我空了4个核"       → Scheduler 立刻决定塞哪些task   ← 资源 offer 模型(源自 Mesos)
```
意义:把"资源协商"(executor 启停时)和"任务放置"(高频内存循环)彻底解耦。executor 粗粒度长驻,日常 task 调度变成纯内存的"给定空闲槽位如何最优放置",每秒解几十上百次,必须极快。

**三层:**
- `TaskSchedulerImpl`:裁判。持有所有 TaskSet,管多 TaskSet 间优先级(FIFO/Fair),驱动 offer 循环。
- `TaskSetManager`(TSM):每 TaskSet 一个。管 Stage 内决策——offer 给哪个 task、延迟调度、推测执行、重试。**精华在此。**
- `SchedulerBackend`:管道。RPC 通信,空闲资源变 offer,决定的 task 序列化发出。

**贯穿主线:本地性 vs 利用率。** 要本地性就得等数据所在机器空出(浪费);要利用率就来槽位塞 task(放太远又慢)。**延迟调度化解这对矛盾。**

---

## 二、细节:一个 TaskSet 如何落到 executor

### 2.1 submitTasks 与僵尸 TSM(`TaskSchedulerImpl.scala` 251–293)
```scala
257  val manager = createTaskSetManager(taskSet, maxTaskFailures)
271  stageTaskSets.foreach { case (_, ts) => ts.isZombie = true }   // 同 stage 旧 TSM 标僵尸
275  schedulableBuilder.addTaskSetManager(manager, ...)             // 加入调度池
292  backend.reviveOffers()                                          // 信号:来一轮 offer
```
262-270 注释:Stage 重提产生新 attempt,旧 TSM 必须标 zombie,否则两 TSM 对 Stage 完成认知分裂→重复提交。**"一个 Stage 任何时刻只能有一个 active TSM"不变式。**

### 2.2 offer 产生(`CoarseGrainedSchedulerBackend.scala` 376–400)
```scala
376  makeOffers: activeExecutors.map(buildWorkerOffer) → scheduler.resourceOffers(workOffers, true)
391  buildWorkerOffer: 每 executor 的(空闲核+空闲资源+主机)→ WorkerOffer
412  makeOffers(executorId): 单 executor 版(某 task 完成空出核时,只针对它发 offer)
```
driver 维护 executorDataMap(每 executor 剩多少核),把空闲量主动喂给调度——"供给式"的字面体现。

### 2.3 裁判核心循环 resourceOffers(`TaskSchedulerImpl.scala` 496–599)
1. **黑名单过滤(522-529):** healthTracker 排除反复失败的 executor/节点(excludeOnFailure,有超时)。
2. **打散防热点(531):** shuffleOffers 随机打乱,避免顺序填满 executor 0 倾斜。
3. **按策略排序 TaskSet(539):** rootPool.getSortedTaskSetQueue,FIFO/Fair(spark.scheduler.mode)。多 TaskSet 间裁决。
4. **双层循环(551-585):**
```scala
551  for (taskSet <- sortedTaskSets)                       // 外:按优先级
574    for (currentMaxLocality <- taskSet.myLocalityLevels)  // 中:本地性 紧→松
576      do { resourceOfferSingleTaskSet(taskSet, currentMaxLocality, ...) }
584      while (launchedTaskAtCurrentMaxLocality)            // 内:当前级别能塞就一直塞
```
"先紧后松、榨干每个本地性级别"→ task 优先放离数据最近处。是否允许放宽由 TSM 延迟调度决定。
- resourceOfferSingleTaskSet(386):轮流喂每个 offer,调 taskSet.resourceOffer(413),成功累加 tasks(i) 并扣 availableCpus(i)。

### 2.4 延迟调度——皇冠明珠(`TaskSetManager.scala` 455, 614, 1323)
```scala
// resourceOffer 455:
473  allowedLocality = getAllowedLocalityLevel(curTime)   // 此刻允许放宽到哪级?
482  dequeueTask(execId, host, allowedLocality)           // 在允许范围找 task

// getAllowedLocalityLevel 614:
655  if (!moreTasks) { lastLocalityWaitResetTime=curTime; currentLocalityIndex+=1 }  // SPARK-4939 无task直接跳级
663  else if (curTime - lastReset >= localityWaits(idx)) { ...; currentLocalityIndex+=1 }  // 等够→放宽一级
670  else return myLocalityLevels(currentLocalityIndex)   // 没等够→坚持当前级别
```
**Delay Scheduling 论文落地:** 坚持最严本地性,等待超 `localityWait`(默认 3s,getLocalityWait 1323)才放宽一级(PROCESS_LOCAL→NODE_LOCAL→RACK_LOCAL→ANY)。
> ★ 3 秒为何有效:繁忙集群里 task 平均几秒~几十秒,"再等 3 秒大概率有本地 executor 空出"。小的有界延迟换大的概率性本地命中,最坏只多等 3s。与 FetchFailed 攒 200ms 同源。
> ★ SPARK-4939(655-662):当前级别无待调度 task 则立即跳级,不空等。
> ★ computeValidLocalityLevels(1345):myLocalityLevels 动态算(是否真有 task 偏好存活 executor/host),新 executor 加入重算→原本只能 ANY 的 task 可能突获 PROCESS_LOCAL。本地性级别是活的。

### 2.5 发射 launchTasks(`CoarseGrainedSchedulerBackend.scala` 430–460)
```scala
432  val serializedTask = TaskDescription.encode(task)    // 不含 RDD 逻辑(已广播),只带分区号/本地性/依赖/广播引用
433  if (serializedTask.limit() >= maxRpcMessageSize) taskSetMgr.abort("...use broadcast variables")  // 默认128MB
450  executorData.freeCores -= task.cpus                  // 供给账本实时更新
457  executorData.executorEndpoint.send(LaunchTask(...))  // RPC 发射
```
> ★ 闭包误引大对象会撑爆 RPC 消息,报错建议广播变量(与第3章广播 taskBinary 同理)。
> ★ 发射即扣 freeCores,下轮 offer 不重发;task 完成加回并触发该 executor 的 makeOffers。循环转起来。

### 2.6 推测执行(`TaskSetManager.scala` 1284–1321)
```scala
1299  if (numSuccessfulTasks >= minFinishedForSpeculation)        // 完成够多(默认75%)
1300    medianDuration = successfulTaskDurations.percentile()      // 中位时长
1301    threshold = max(speculationMultiplier * median, minTime)   // 中位×1.5
1305    checkAndSubmitSpeculatableTasks(timeMs, threshold)         // 超阈值的 task 另起副本
```
Stage 完成时间取决于最慢 task。完成≥75%后算中位数,超"中位×1.5"的在另一机器起副本,先完成者胜,另一个 kill。
> ★ 危险暗面:有副作用的 task(写 DB/HDFS)起两副本会重复写。靠第1章 OutputCommitCoordinator 仲裁——同分区输出只批准第一个 commit 请求,其余拒绝。两个相隔三章的模块在此咬合。
> ★ barrier stage 禁用推测(1288):起副本破坏协同同步语义。

---

## 三、Trick 与 Bug
**① 资源 offer 模型(Mesos)。** 供给式非请求式,解耦资源协商与任务放置。
**② 僵尸 TSM 维护单 active TSM 不变式(262-273)。** 10 分区例子是真实坑。
**③ 延迟调度。** localityWait 默认 3s 才放宽,小有界延迟换概率性本地命中。
**④ SPARK-4939 无 task 级别不空等(655-662)。**
**⑤ shuffleOffers 防热点(531)。**
**⑥ 任务描述大小限制+广播药方(433-440)。** 默认 128MB。
**⑦ 黑名单 excludeOnFailure(522-529)。** 坏节点临时排除,防撞光重试,有超时。
**⑧ 推测执行 + OutputCommitCoordinator 咬合(1284 + 第1章)。** 防重复 commit;barrier 禁用。
**⑨ starvation timer(277-289)。** 长时间拿不到资源打印 "Initial job has not accepted any resources"。

---

## 四、本章总纲
```
submitTasks → TaskSchedulerImpl.submitTasks[251]: 包TSM + 旧TSM标zombie[271] + 入调度池 + reviveOffers
  ↓ 供给式 offer
makeOffers[376]: executor空闲核→WorkerOffer→resourceOffers
  ↓
resourceOffers[496](裁判): 黑名单过滤[522] → shuffleOffers打散[531] → FIFO/Fair排序[539]
  → 双层循环 for TaskSet × for 本地性(紧→松) × do-while(能塞就塞)[551-585]
       └─ TSM.resourceOffer[455]: getAllowedLocalityLevel[614] 等够localityWait(3s)才放宽 ← 延迟调度
            (SPARK-4939无task跳级; computeValidLocalityLevels动态算)
  ↓
launchTasks[430]: 序列化TaskDescription → 查maxRpcMessageSize(超则abort建议广播)[433]
  → 扣freeCores → RPC send(LaunchTask) → executor(第5章)
旁路 推测执行[1284]: 完成≥75%后慢于中位×1.5起副本,先完成者胜(OutputCommitCoordinator防重复commit; barrier禁用)
```

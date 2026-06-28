# 第 5 章 — Executor:计算真正发生,以及闭环的合拢

> 对照源码:`core/.../executor/CoarseGrainedExecutorBackend.scala`、`executor/Executor.scala`、`scheduler/Task.scala`、`ShuffleMapTask.scala`、`ResultTask.scala`(apache/spark master)

---

## 一、宏观:所有抽象在这里兑现

前四章都在 driver 端的"图纸"世界:RDD=血缘(0/2章),Stage=图纸分块(3章),Task=施工单(4章)。本章是搬砖的地方——executor。

**合拢感:** 第 0 章 2.6 那句"iterator 由 executor 上的 task 调用",在本章兑现——亲眼看到 `rdd.iterator(partition, context)` 在 executor 线程里被真正调用。

**executor 物理模型:**
- **长驻 JVM 进程**(coarse-grained):一次启动跑成百上千 task,省掉 MR"每 task 起一个 JVM"的开销。Spark 快的物理基础之一。
- **内部线程池**:4 核 executor 用 4 线程并发跑 4 task。**真线程并发** → 解释第 3 章"每 task 独立反序列化 RDD 副本规避 Hadoop JobConf 非线程安全"。

**暗线:闭环合拢。** task 算完,结果逆着来路传回 driver,唤醒第 3 章 runJob 阻塞的 JobWaiter,用户 collect() 返回。执行链路从第 3 章 action 出发到第 5 章画成一个圆。

---

## 二、细节:一个 Task 在 executor 上的一生

### 2.1 接收与派发(`CoarseGrainedExecutorBackend.scala` 181, `Executor.scala` 374)
```scala
181  case LaunchTask(data) => val taskDesc = TaskDescription.decode(data.value); executor.launchTask(this, taskDesc)
                              // 只反序列化轻量 TaskDescription,RDD 重活留到 task 线程
374  launchTask: tr = createTaskRunner(...); runningTasks.put(taskId, tr); threadPool.execute(tr)  // 不阻塞
```
launchTask 把 TaskRunner 扔进线程池就返回,RPC 线程立即处理下一个 LaunchTask。runningTasks 表供 kill/推测执行找到并中断 task。

### 2.2 TaskRunner.run 生命周期(`Executor.scala` 562–650)
```scala
565  isolatedSession = isolatedSessionCache...           // ① 类加载器隔离(按 session,多用户不污染)
580  setContextClassLoader(isolatedSession.replClassLoader)
581  val ser = env.closureSerializer.newInstance()       // ② closureSerializer(Java,第1章)
583  statusUpdate(taskId, RUNNING, EMPTY)                // ③ 干重活前先上报"开跑"(反序列化/拉依赖可能慢)
594  updateDependencies(files, jars, archives, ...)      // 拉 task 需要的 jar/file
602  task = ser.deserialize[Task[Any]](serializedTask)   // 反序列化 Task 对象
622  if (!isLocal) mapOutputTracker.updateEpoch(task.epoch)  // ③ 接第3章 FetchFailed:epoch 涨了→作废本地 shuffle 位置缓存
641  val value = task.run(taskId, attemptNumber, ...)    // THE 调用
```
> ★ epoch(618-625):executor 端 MapOutputTrackerWorker 缓存 shuffle 位置;driver 因 FetchFailed 改账本时 epoch+1;task 带新 epoch 触发缓存作废,强制重拉最新位置。第3章容错在 executor 端生效的最后一环。

### 2.3 Task.run 模板搭舞台(`Task.scala` 87–170)
```scala
105  val taskContext = new TaskContextImpl(stageId, partitionId, taskAttemptId, ..., taskMemoryManager, ...)
119  context = if (isBarrier) new BarrierTaskContext(taskContext) else taskContext
126  TaskContext.setTaskContext(context)                 // 放进 ThreadLocal!
147    context.runTaskWithListeners(this)                // → runTask(context)
148  } finally {
152    releaseUnrollMemoryForThisTask(...)               // 释放内存
161    memoryManager.synchronized { memoryManager.notifyAll() }  // 唤醒等内存的其他 task
```
> ★ ThreadLocal(126):一 task 独占一线程 → thread-local 即 task-local,用户代码任何位置 TaskContext.get() 都能拿到。
> ★ finally notifyAll(148-161):防"task 永远睡着等内存无人唤醒"死局。第7章内存争用的接缝。

### 2.4 高潮:rdd.iterator 被调用(`ShuffleMapTask.scala` 82, `ResultTask.scala` 93)
```scala
// ShuffleMapTask 82-112:
90   rddAndDep = ser.deserialize[(RDD, ShuffleDependency)](taskBinary.value)   // 从广播解出 (rdd, dep)
106  dep.shuffleWriterProcessor.write(rdd.iterator(partition, context), dep, mapId, partitionId, context)  // ★第0章那行
     // 返回 MapStatus(输出多大/在哪) → 第6章 reduce 端要拉的东西

// ResultTask 78-93:
93   func(context, rdd.iterator(partition, context))     // ★ 用户 action 函数作用在分区迭代器上
```
两种 task 殊途同归,核心都是 `rdd.iterator(partition, context)`(第0章2.6 final 模板),拉动整条窄依赖流水线 iterator→compute→firstParent.iterator→...,一条记录从数据源穿过所有算子流到这里。
> ★ Spark 执行模型全部秘密:一切计算 = 对一个分区调 rdd.iterator,再决定把迭代器"写给下游(shuffle)"还是"交给用户(result)"。一个 Stage 几十个算子折叠进这一次 iterator 调用、以迭代器嵌套流水线执行。第0章用 Iterator 而非 Array 的威力在此兑现。

### 2.5 结果回传:三级阶梯(`Executor.scala` 239–243, 740–775)
```scala
239  maxDirectResultSize = min(TASK_MAX_DIRECT_RESULT_SIZE, RpcUtils.maxMessageSize)  // 小,~1MB级
243  maxResultSize = conf.get(MAX_RESULT_SIZE)                                         // 大,默认 1GB
748  if (resultSize > maxResultSize)        → IndirectTaskResult(只回传大小), 丢弃结果  // 【第三档】超限
755  else if (resultSize > maxDirectResultSize) → blockManager.putBytes; IndirectTaskResult(blockId)  // 【第二档】间接
764  else                                   → serializedDirectResult.toByteBuffer      // 【第一档】直接内联回传
```
两个硬约束间走钢丝:RPC 消息有上限(不能内联 1GB),driver 内存有限(不能收回海量结果)。
- 第一档:小,随 statusUpdate RPC 内联,最快。
- 第二档:偏大超 RPC 量 → 存本 executor BlockManager,只回传 blockId,driver 由 TaskResultGetter 异步去取。
- 第三档:超 maxResultSize(默认1GB)→ 直接丢弃,只回传大小;driver 汇总超限则失败 job,报 "Total size of serialized results is bigger than maxResultSize"。
> ★ 第三档=撑爆 driver 的防护栏。大 RDD 直接 collect() 是经典错。**核心价值观:宁可主动失败给清晰报错,绝不让 driver 被动 OOM。** 同一个 IndirectTaskResult:第二档 blockId 指向真实块(driver会取),第三档指向已丢弃块(只看大小报错)。

### 2.6 闭环合拢(`Executor.scala` 775 起)
```
statusUpdate(FINISHED, result)[775]
 → CoarseGrainedExecutorBackend.statusUpdate → CoarseGrainedSchedulerBackend(driver收)
 → TaskSchedulerImpl.statusUpdate → TaskResultGetter(反序列化;IndirectTaskResult则先去executor拉回真实块)
 → TaskSetManager.handleSuccessfulTask → DAGScheduler CompletionEvent(回到第3章单线程事件循环!)
 → job完成 → JobWaiter.taskSucceeded → completionFuture完成 → 第3章 runJob awaitReady 解除阻塞 → collect()返回
```
从第3章 collect() 阻塞到第5章结果传回 runJob 苏醒,执行链路画成完整的圆。
> ★ 结果回到 DAGScheduler 又变成 CompletionEvent 投进单线程事件循环。成功/失败/推测/重试在 DAGScheduler 眼里都只是事件流里一个事件——第3章无锁状态机的一致性到此完整体会。

---

## 三、Trick 与 Bug
**① coarse-grained 长驻 JVM + 线程池。** 省 JVM 启停;真线程并发故需每 task 独立反序列化 RDD 隔离。
**② maxResultSize 三级阶梯(239-243, 748-769)。** 直接/BlockManager间接/超限丢弃失败。宁可主动失败绝不 driver OOM。
**③ epoch 作废 shuffle 位置缓存(618-625)。** 第3章 FetchFailed 容错在 executor 端生效最后一环。
**④ TaskContext 用 ThreadLocal(Task 126)。** 一task一线程,任何位置 TaskContext.get()。
**⑤ finally 释放内存+notifyAll(Task 148-161)。** 防等内存死局,第7章接缝。
**⑥ 类加载器按 session 隔离(564-569)。** REPL/Spark Connect 多会话 jar 不污染。
**⑦ 早发 RUNNING(583)。** 慢活前上报,UI/推测计时准确。
**⑧ launchTask 不阻塞+runningTasks 表(374-383)。** RPC线程扔线程池即返回;kill/推测靠该表中断(配合第0章 InterruptibleIterator)。

---

## 四、本章总纲
```
LaunchTask RPC → executor:
 CoarseGrainedExecutorBackend[181]: decode → launchTask
   └─ launchTask[374]: 包TaskRunner → runningTasks登记 → threadPool.execute(不阻塞)
        └─ TaskRunner.run[562]: 类加载器隔离[564] → statusUpdate(RUNNING)[583] → updateDependencies[594]
             → 反序列化Task[602] → updateEpoch作废过期shuffle缓存[618](接第3章)
             → task.run[Task 87]: TaskContextImpl→setTaskContext(ThreadLocal)[126]
                  → runTask: ShuffleMapTask[82] 广播解(rdd,dep)→dep.write(rdd.iterator)[106]→MapStatus→第6章
                             ResultTask[93]  func(context, rdd.iterator)→用户结果
                             ★核心都是 rdd.iterator,第0章承诺兑现,流水线真实跑起
             → finally 释放内存+notifyAll[148-161](接第7章)
             → 结果三级阶梯[748-769]: 小→直接 / 偏大→BlockManager间接 / 超maxResultSize→丢弃失败job
             → statusUpdate(FINISHED)[775]
                 ↑闭环→backend→TaskSchedulerImpl→TaskResultGetter→TSM.handleSuccessfulTask
                      →DAGScheduler CompletionEvent(第3章事件循环)→JobWaiter→runJob苏醒→collect()返回
```

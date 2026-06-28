# 第 15 章 — Structured Streaming:把批引擎跑成一个循环

> 对照源码:`sql/core/.../execution/streaming/{MicroBatchExecution,StreamExecution,IncrementalExecution}.scala`(apache/spark master)

---

## 一、宏观

> 流=随时间无限增长的表; 流式查询=你在批表上写的同一段 DataFrame/SQL,只不过 Spark 反复、增量地跑在每批新数据上。

**流批一体是字面事实**: 流式查询用和批一模一样的 Catalyst 计划(第9章),放进循环对一个个微批执行。**流复用前14章整个引擎**(RDD/Catalyst/调度/shuffle/Tungsten 都没重造)。
流真正新增仅4样: ①微批循环 ②跨批状态(StateStore) ③WAL(exactly-once) ④水位线(迟到数据+状态封顶)。

> ★ 第9章"窄腰"(LogicalPlan)第三次结清红利: 第9章让 SQL/DataFrame 汇聚 LogicalPlan; 第14章让 Connect protobuf 汇聚; 本章让"流"=同一 LogicalPlan 反复编译执行。**Spark 不造第二个引擎就支持流,正因"编译到 LogicalPlan→toRdd→DAGScheduler"足够通用,放进循环喂每批新数据即可。** 好的核心抽象让整座大厦(批/SQL/Connect/流)都站在它上面。

---

## 二、细节

### 2.1 微批循环(`MicroBatchExecution.scala` 335–409)
```scala
335  runActivatedStream: triggerExecutor.execute(executeOneBatch(_, ...))   // trigger 驱动的无限循环
347  executeOneBatch:
375    constructNextBatch(...)   // ① 算"从上次到现在源里新增的 offset 区间"
394    runBatch(execCtx, ...)    // ② 把该区间当有界表执行
```
triggerExecutor 按策略(ProcessingTime 每N秒/AvailableNow 处理完即停)反复调。每批:先算新 offset 区间,再当有界表跑一遍。
> ★ 即使无新数据也可能跑"空批"(393)——为状态清理(水位线驱逐过期状态)。流不只是"有数据才动"。

### 2.2 WAL:exactly-once 基石(`MicroBatchExecution.scala` 472–506, 910–989)
checkpoint 目录两个日志=经典预写日志(WAL):
```scala
// ① offsetLog 处理【前】写意图(910-918):
911  offsetLog.add(batchId, endOffsets.toOffsetSeq(...))   // 处理批N之前先记"我即将处理 offset [X,Y)"
// ② commitLog 处理【后】写完成(986-989):
986  commitLog.add(batchId, CommitMetadata(watermark, ...)) // 批成功(sink写完)后才记"批N已完成"+水位线
// ③ 恢复(472-506):
476  offsetLog.getLatest() → batchId = latestBatchId        // 先假设要重做 offsetLog 最后这批
502  commitLog.getLatest() → 提交了? 提交了→前进下一批; 没提交→重放批N
```
重启读两日志: **offsetLog 有批N 但 commitLog 没有** → 批N 开始了没提交(崩中间)→ **用 offsetLog 记的相同 offset 区间 [X,Y) 重跑批N**。
> ★ 教科书级 WAL/redo 模式(同数据库 redo 日志): 动作前写意图,动作后写完成,崩中间用意图重放。exactly-once 两前提=全书两主线交汇:
> - **源可重放+计算确定**: 重跑批N 读相同输入(offsetLog 给相同区间,故 Kafka/文件可按offset重读)+产相同结果(确定性,第0章!)
> - **sink 幂等或事务**: 重跑会再写一遍输出,sink 要么去重(幂等 upsert)要么和 batchId 原子提交(事务,文件sink _spark_metadata/Delta)
> - **WAL重放(相同输入)+确定性计算(相同输出)+幂等sink(吸收重复写)=端到端 exactly-once**
> ★ "确定性"一个性质撑起 Spark 全部三个容错故事: 第0章 cache 丢了血缘重算 / 第3章 SPARK-23207 不确定stage整链回滚 / 第15章流式精确一次。从"RDD必须不可变"一路贯穿到此。

### 2.3 增量执行:接回第9章(`IncrementalExecution.scala`)
IncrementalExecution 是第9章 QueryExecution 的**子类**: 跑第9章整套 Catalyst 流水线+几条流规则——把有状态逻辑算子换成流式物理实现(StateStoreSaveExec/StreamingSymmetricHashJoinExec),穿入"状态checkpoint位置+batchId+水位线"。
"增量"=只处理"这批新offset区间"+"上批结转的状态",非从头重算历史。流式聚合不每批重加所有历史,而是读上批部分聚合结果(StateStore)只并入新数据。

### 2.4 状态(StateStore)与水位线(watermark)
**StateStore=跨批记忆。** 有状态操作(流式聚合/去重/流流join/mapGroupsWithState)需把中间状态带到下批。StateStore=按版本(每batchId一版)checkpoint 的KV存储: 每批读上版本/更新/写新版本; 恢复还原到 commitLog 记录的已提交版本。**与分区同位**(每shuffle分区有自己的state store,故有状态算子先按key shuffle 第6章,相同key和其状态落同分区)。
**watermark=给无限状态封顶。** 流根本难题=状态可能无限增长(小时窗口聚合难道永远留所有历史窗口?)。水位线声明: **"不再接受比(已见最大事件时间−阈值)更早的数据"**。每批推进(单调不减),做两件事:
1. 判定窗口/聚合"已定稿"→输出最终结果+**驱逐其状态**(状态不再无限涨)
2. 丢弃晚于水位线的迟到数据
水位线存进 commitLog 的 CommitMetadata(987)→重启精确恢复到崩溃前水位线。
> ★ 水位线=延迟vs资源权衡旋钮: 阈值大→容忍更多迟到(更完整)但状态留更久/结果更晚; 小→状态及时驱逐/低延迟但迟到太多被丢。同第4章延迟调度3s、第13章空闲超时——"在两对立目标间用超时参数取平衡"。Spark 这类旋钮形态高度一致。

(简记)**连续处理**: 微批外另一模式,每分区长驻task逐条处理,毫秒级延迟,代价是语义更弱算子有限。微批是默认且远更常用。

---

## 三、Trick 与 Bug
**① 流=批跑成循环(2.1,2.3)。** IncrementalExecution=QueryExecution子类+流规则,跑第9章同套Catalyst。不造第二引擎=LogicalPlan窄腰第三次红利。
**② WAL两日志(472-506,910-989)。** offsetLog前写意图、commitLog后写完成;崩中间用相同offset重放。redo日志模式。
**③ exactly-once=WAL重放+确定性计算+幂等/事务sink(2.2)。** 确定性是命根子,撑起全部三个容错故事(第0/3/15章)。
**④ 空批做状态清理(393)。** 无新数据也跑,用水位线驱逐过期状态。
**⑤ StateStore按batchId版本化(2.4)。** 每批读上版本/写新版本;恢复还原已提交版本;与分区同位(先按key shuffle 第6章)。
**⑥ 水位线存commitLog精确恢复(987)。** 重启后对迟到判断与崩前一致。延迟vs资源旋钮(同第4/13章)。
**⑦ 一次只处理一个批(668-670)。** 日志清理正确性依赖无流水线并行。

---

## 四、本章总纲
```
核心: 流=增长的表; 流式查询=批同一DataFrame/SQL反复增量执行。流批一体=同一Catalyst计划(第9章)放进循环
  新增仅4样: 微批循环/跨批状态(StateStore)/WAL(exactly-once)/水位线; 其余全复用前14章
微批循环[335-409]: triggerExecutor.execute(executeOneBatch)无限循环; constructNextBatch(新offset区间)→runBatch(当有界表); 空批也跑(状态清理393)
WAL[472-506,910-989]: offsetLog前写意图 → commitLog后写完成(+水位线); 恢复: offsetLog有批N但commitLog没→相同offset重放批N
  ★ exactly-once=WAL重放(相同输入)+确定性计算(相同输出,第0章)+幂等/事务sink(吸收重复写)
  ★ 确定性撑起全部三容错: cache重算(第0)/SPARK-23207整链回滚(第3)/流式精确一次(本章)
增量执行[IncrementalExecution]: QueryExecution(第9章)子类+流规则(StateStoreSaveExec),只处理新区间+结转状态
状态+水位线: StateStore按batchId版本化、与分区同位(先按key shuffle第6章); watermark="不收比(maxEventTime-阈值)更早的"
  →定稿窗口+驱逐状态(封顶)+丢弃迟到; 存commitLog精确恢复; 延迟vs资源旋钮(同第4/13章)
```

---

## 全卷收束:贯穿全书的几条主线
- **确定性+不可变=容错物理前提**(第0→3→7→15): RDD不可变→血缘重算→不确定stage整链回滚→流式精确一次,同一性质撑起全部容错。
- **窄腰抽象(所有东西汇聚的最简单一层)**: Iterator(第0)/LogicalPlan(第9,14,15)/BlockId(第12)/SchedulerBackend(第4,13)/RpcEndpointRef(第11)——上下随便换中间不动。
- **并发收敛成串行消息队列消灭锁**(第3 DAGScheduler、第11 RPC端点)。
- **用有界延迟/滞后换概率性大收益**(第3攒200ms、第4延迟调度3s、第10 AQE等shuffle、第13空闲超时、第15水位线)。
- **绕开JVM对象模型+编译而非解释**(第6/8 Tungsten); **优化必须安全,永留正确退路**(第8 codegen fallback、第10 AQE成本守卫)。
- **分层复用不造第二引擎**: SQL编译成RDD(第9)、Connect喂LogicalPlan(第14)、流跑成循环(第15),高级能力全站在朴素健壮的RDD引擎上。
```

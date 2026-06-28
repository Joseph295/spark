# 第 10 章 — AQE:让优化器"边跑边改主意"

> 对照源码:`sql/core/.../execution/adaptive/AdaptiveSparkPlanExec.scala`(apache/spark master)

---

## 一、宏观

Catalyst(第9章)的原罪:**只在开跑前优化一次,全凭静态统计**(metastore 过期/中间结果无统计/UDF黑盒)。统计一错计划就错:
- 高估表大小 → 该 BroadcastHashJoin 选成 SortMergeJoin
- shuffle.partitions=200 但 filter 后只剩一点 → 200 个空转小 task
- 某 key 倾斜 → 一个分区 100 倍大,拖垮 stage 的长尾 task

AQE(3.0)洞察:
> 最准的统计是运行时量出来的;量它的最佳时机=一个 shuffle 刚完成(map 全写完,每个输出精确大小已在 MapStatus 里,第6章)。

⟹ **每个 shuffle 边界停下,读真实统计,重优化剩余计划,再继续。** 把第9章一次性静态优化器变成"跑一段→量→重优化→再跑"的反馈循环。本质=Catalyst 规则框架拿真实数据反复跑。三招牌:动态合并分区、动态切 join、动态处理倾斜。

---

## 二、细节

### 2.1 整个 AQE 是一个物理节点 AdaptiveSparkPlanExec
InsertAdaptiveSparkPlan(第9章 prepareForExecution)把物理计划包进 AdaptiveSparkPlanExec,**拦截 execute()** 跑反馈循环。
**QueryStage:** 物理计划在每个 exchange(shuffle/broadcast)边界切开,每段包成 QueryStageExec。**每段先完整物化(跑完产出 shuffle 输出+统计)才规划下一段** → shuffle 边界=重优化检查点(同第3章按 shuffle 切 stage,多了"切完重新思考")。

### 2.2 反馈循环 getFinalPhysicalPlan(`AdaptiveSparkPlanExec.scala` 280–389)
```scala
284  result = createQueryStages(currentPhysicalPlan, firstRun=true)   // ① 找可独立物化的 stage
288  while (!result.allChildStagesMaterialized) {
299    reorderedNewStages = 广播 stage 排前(SPARK-33933,294-304 防 broadcast 超时)
309    stage.materialize().onComplete { events.offer(StageSuccess(...)) }  // ② 异步物化(提交 job 给 DAGScheduler)
334    val nextMsg = events.take()                                    // ③ 阻塞等某 stage 完成(新统计到手)
336    events.drainTo(rem)                                            //    批量收同时完成的,减少重规划
361    logicalPlan = replaceWithQueryStagesInLogicalPlan(...)
362    afterReOptimize = reOptimize(logicalPlan)                      // ④ 用真实统计重优化
365    origCost = costEvaluator.evaluateCost(currentPhysicalPlan)
367    if (newCost < origCost || ...) currentPhysicalPlan = newPhysicalPlan  // ⑤ 成本守卫:只在不更差时采纳
380    result = createQueryStages(currentPhysicalPlan, firstRun=false)// ⑥ 再找可物化 stage,循环
```
- ② materialize 把 stage 作为独立 job 提交 DAGScheduler(第3章)+ onComplete 回调;广播优先(SPARK-33933)。
- ③ take 阻塞等完成,drainTo 批量收同时完成的(一次重优化处理多事件)。
- ⑤ 成本守卫:newCost ≤ origCost 才采纳,**AQE 绝不把计划改更差**。
> ★ 一条 SQL 在 AQE 下=提交给 DAGScheduler 的一串 job + 中间重规划。第2章 RangePartitioner 定义期偷跑采样 job 的彻底一般化:规划与执行交错(执行一段→拿真实数据→规划下一段)。故 UI 上一条 SQL 对应多个 job。

### 2.3 重优化 reOptimize(`AdaptiveSparkPlanExec.scala` 793–816)
```scala
795  logicalPlan.invalidateStatsCache()                  // ① 扔掉过期估算统计
796  val optimized = optimizer.execute(logicalPlan)      // ② 重跑 Catalyst 逻辑优化(第9章!)
797  planner.plan(ReturnAnswer(optimized)).next()        // ③ 重新物理规划
```
AQE 不造新轮子=把第9章优化器+planner 再跑一遍。区别仅输入:已物化 stage 是 LogicalQueryStage 叶子,携带**物化后精确行数/字节数**而非估算。795 invalidateStatsCache 关键:必须先清旧估算,否则基于陈旧估算推出同样错误计划。
> ★ AQE 魔力不在新算法,在"用真相替换猜测,让同一个优化器重新决策"。

### 2.4 三大招牌优化(`AdaptiveSparkPlanExec.scala` 143–151)
```scala
146  OptimizeSkewInRebalancePartitions    // 倾斜处理
147  CoalesceShufflePartitions(...)       // 动态合并分区
150  OptimizeShuffleWithLocalRead
```
**① CoalesceShufflePartitions(147)最常见收益:** shuffle.partitions=200 但 filter 后真实 50MB → 200 个 250KB 小分区=200 个开销>计算的小 task。AQE 看真实大小把相邻小分区合并成更少合适的(5 个各 10MB)。**不用再纠结 shuffle.partitions——设大点 AQE 自动合并。**
**② OptimizeSkewedJoin(146 + 逻辑规则):** 某分区远大于中位数→拆成多子分区,倾斜 key 不再变巨型长尾 task。呼应第3/4章"stage 完成时间取决于最慢 task"。
**③ DynamicJoinSelection/DemoteBroadcastHashJoin(逻辑规则):** 物化后发现 join 一边 < 广播阈值 → SortMergeJoin 改 BroadcastHashJoin,省一次 shuffle。开篇"静态选错 join 运行时纠正"。

### 2.5 妙处:只动读端,不重算 map(AQEShuffleReadExec)
合并/拆分分区听起来要重 shuffle?**完全不用。** 第6章:map 输出已写成"一数据文件+一索引",reduce 分区=文件里一段连续字节。AQE 这些优化全是改"reduce 端怎么读"同一批已写好的文件:
- 合并分区 = 一个 reduce task 读原分区 [0,5)(读索引几段)
- 拆倾斜分区 = 多个 reduce task 各读同一大分区的一个子区间
由 AQEShuffleReadExec 重映射完成,**map 端一字节不重写**。
> ★ 第6章"一数据文件+一索引"设计在此二次结清红利。reduce 分区是"大文件里索引界定的字节区间",故 AQE 可任意纯读端重切 reduce 侧(合并/再切细)而不碰 map 输出。若是 Hash Shuffle 的 M×R 独立文件则无从谈起。**好的底层数据结构会在意想不到的上层再开花——分层设计"底层多想一步,上层海阔天空"的回报。**

补充边界: ① 只在 shuffle 边界生效,无 shuffle 查询无 AQE 收益,第一个 stage 仍跑静态统计; ② 最终 stage 用户指定分区方式则跳过部分 shuffle 优化(167-169); ③ 成本守卫保证永不劣化(同第8章 codegen 必有 fallback 价值观:优化必须安全)。

---

## 三、Trick 与 Bug
**① AQE=Catalyst 拿真实数据反复跑(2.3)。** shuffle 边界天然检查点;invalidateStatsCache 必须先清旧估算。
**② 一条 SQL=多个 DAGScheduler job+中间重规划(2.2)。** 规划与执行交错,第2章采样 job 思想一般化。
**③ 成本守卫(363-368)。** newCost≤origCost 才采纳,永不劣化。同第8章 fallback 价值观。
**④ SPARK-33933 广播 stage 优先物化(294-304)。** 防 broadcast 超时。
**⑤ 三大优化(143-151)。** 合并小分区(不纠结 shuffle.partitions)/拆倾斜(治长尾,呼应第3/4章)/SMJ→BHJ 省shuffle。
**⑥ 纯读端重映射不重算 map(2.5)。** AQEShuffleReadExec 改 reduce 读第6章数据文件+索引的字节区间,map 零重写。第6章设计二次红利。
**⑦ 边界。** 只在 shuffle 边界;首 stage 静态统计;无 shuffle 无收益。

---

## 四、本章总纲
```
原罪: Catalyst 只优化一次+凭静态统计(过期/无统计/UDF黑盒)→选错 join/分区数/不处理倾斜
洞察: 最准统计=运行时量的; 时机=shuffle 完成(MapStatus 精确大小,第6章) ⟹ 每 shuffle 边界停→读真实统计→重优化→再跑

载体 AdaptiveSparkPlanExec(InsertAdaptiveSparkPlan 第9章插入,拦截 execute):
  QueryStage 在 exchange 边界切; 每段先物化才规划下一段=重优化检查点
反馈循环[280-389]: createQueryStages→while(!全物化){异步materialize(提交job给DAGScheduler,广播优先SPARK-33933)
  →events.take等完成+批量收→reOptimize注入真实统计重跑Catalyst[362]→成本守卫newCost≤origCost采纳[367]→再createQueryStages}
reOptimize[793]: invalidateStatsCache→optimizer.execute(第9章)→planner.plan; 同套Catalyst喂真相
三大优化[143-151]: CoalesceShufflePartitions(合并小分区)/OptimizeSkewedJoin(拆倾斜治长尾)/DynamicJoinSelection(SMJ→BHJ)
妙处: 合并/拆分纯改reduce读端(AQEShuffleReadExec重映射第6章数据文件+索引字节区间),map零重写——第6章设计二次红利
边界: 只在shuffle边界;首stage静态统计;无shuffle无收益;成本守卫永不劣化
```

# 第 9 章 — Catalyst:一条 SQL 如何变成 RDD 上的 Task

> 对照源码:`sql/core/.../execution/QueryExecution.scala`、`sql/catalyst/.../trees/TreeNode.scala`、`catalyst/.../rules/RuleExecutor.scala`、`catalyst/.../optimizer/Optimizer.scala`(apache/spark master)

---

## 一、宏观

> Spark SQL 把"查询"建模成"一棵树",把"优化与编译"建模成"对树施加一系列 Rule 变换"。

Catalyst 是通用"树+规则"框架:每个优化(谓词下推/列裁剪/常量折叠)不是特殊代码路径,而是一条**小巧、可组合、可独立测试**的 Rule。加优化=往批次加一条 Rule。这种可扩展性是 Catalyst 论文核心卖点、Spark SQL 胜出的结构性优势。

**流水线(主轴):**
```
SQL/DataFrame →(parser)→ Unresolved LogicalPlan(名字未绑定)
 →(Analyzer)→ Analyzed(名字解析/类型检查/合法)
 →(Optimizer)→ Optimized(谓词下推/列裁剪/常量折叠后)
 →(Planner/Strategy)→ Physical SparkPlan(Join→SortMerge/BroadcastHash)
 →(prepareForExecution: 插Exchange/codegen第8章/AQE第10章)→ Executed
 → toRdd = executedPlan.execute() → RDD[InternalRow] → DAGScheduler(第3章)→ 整个执行链路
```
**题眼:toRdd 把 SQL 全部精巧坍缩成一个 RDD,走第0-5章那套。SQL 是编译到 RDD 执行引擎的前端,非另一个引擎。**

---

## 二、细节

### 2.1 一切皆 TreeNode(`TreeNode.scala` 413–490)
LogicalPlan/Expression/SparkPlan 全是 TreeNode。核心武器 transform:
```scala
463  transformDownWithPruning(cond, ruleId)(rule):
466    if (!cond.apply(this) || isRuleIneffective(ruleId)) return this   // ruleId 剪枝:跳过无效子树
469    afterRule = rule.applyOrElse(this, identity)                      // 对当前节点应用规则
474    if (this fastEquals afterRule)                                    // 规则没改动?
475      mapChildren(_.transformDownWithPruning(...)(rule))              // 递归子节点,复用未变子树
```
Rule 本质=一次对树的 transform 调用,递归应用偏函数,匹配不上的节点原样保留。
> ★ TreeNode 不可变,transform 返回【新树】——同第0章 RDD 不可变的函数式哲学。优化是纯函数 Tree⇒Tree,易测易组合。Spark 两大支柱(RDD/Catalyst)底层共享"不可变+变换 替代 可变+修改"。
> ★ fastEquals(474)复用未变子树防 GC churn;ruleId(466)跳过已证无效的子树。大树上反复跑多遍的性能命脉。

### 2.2 Rule/Batch/不动点(`RuleExecutor.scala` 120–290)
```scala
133  Once: maxIterations=1                       // 跑一遍
139  FixedPoint                                  // 反复跑到不动点
145  Batch(name, strategy, rules*)
227  while (continue) {                          // 不动点循环
228    curPlan = batch.rules.foldLeft(curPlan){ (plan,rule) =>
231      result = rule(plan); effective = !result.fastEquals(plan); result }
269    if (iteration > maxIterations) 停          // 或整批跑完计划不变(到不动点)
```
为何反复跑到不动点?**优化互相触发**:下推 filter→某列没用→裁列→子树剩常量→折叠……反复跑到树不再变才榨干。
> ★ 双护栏防失控: ① maxIterations(269,超了打印"请调大X")防死循环 ② Once 批次幂等检查(288,测试下校验跑两次还变=规则bug) ③ 每规则后计划合法性校验(240-258, PLAN_VALIDATION_FAILED)。自由度越高越需护栏。

### 2.3 流水线即惰性阶段(`QueryExecution.scala` 162–255)
```scala
183  lazyOptimizedPlan = LazyTry { optimizer.executeAndTrack(withCachedData.clone(), tracker) }
205  lazySparkPlan = LazyTry { createSparkPlan(planner, optimizedPlan.clone()) }          // 逻辑→物理
220  lazyExecutedPlan = LazyTry { prepareForExecution(preparations, sparkPlan.clone()) }  // 准备(codegen/AQE)
241  lazyToRdd = LazyTry { new SQLExecutionRDD(executedPlan.execute(), ...) }             // →RDD!
```
> ★ 每阶段 .clone():analyze/optimize/plan 共享同一树会让一阶段状态(如 analyzed 标记)污染另一阶段。各阶段独立克隆=流水线层防御性不可变(同 2.1 思想更大尺度复现)。

### 2.4 Analyzer:名字绑定到实体
parser 产出 Unresolved 计划(UnresolvedRelation("orders")/UnresolvedAttribute("user_id") 名字未绑定)。Analyzer 一批 Rule:
- ResolveRelations:UnresolvedRelation 去 catalog 查真实表绑 schema
- ResolveReferences:UnresolvedAttribute 绑到具体表的具体列(连类型)
- 类型检查/隐式转换/函数解析
产出完全解析、类型明确、合法的逻辑计划。
> ★ Analyzer 赋予"每列类型"——正是第8章 Tungsten 前提(知类型才能定长布局+codegen)。Analyzer 是 Tungsten 威力的上游源头。

### 2.5 Optimizer:经典优化就是一条条 Rule(`Optimizer.scala` 100–272, 968)
```scala
162  Batch("Operator Optimization...", fixedPoint,      // 反复跑到不动点
106    PushDownPredicates,     // 谓词下推:filter 推近数据源
112    ColumnPruning,          // 列裁剪:只读用到的列
135    ConstantFolding,        // 常量折叠:1+1 编译期算成 2
139    BooleanSimplification,
264    PushPredicateThroughJoin)
968  object ColumnPruning extends Rule[LogicalPlan] { ... 一个 transformDown }
```
> ★ 祛魅:"谓词下推/列裁剪"这些高深优化,在 Catalyst 里就是 `extends Rule` 的小对象 + transformDown 模式重写。威力不在单条聪明,在**小正交规则 + fixedPoint 不动点彼此触发**:下推谓词→某列没用→裁列→子树剩常量→折叠……组合爆发远超单条之和。故贡献新优化门槛=写一个 Rule + 一个测试。
> ★ 谓词下推为何快:把过滤推到最近数据源(读 Parquet 跳过 row group、shuffle 前减数据)。"把减少数据的操作尽量提前"=查询优化第一性原理。

### 2.6 Planner + prepareForExecution + toRdd(`QueryExecution.scala` 205–242)
**逻辑→物理(Planner/Strategy):** createSparkPlan[212] 用 Strategy 把逻辑 Join 翻成物理:BroadcastHashJoin(一边小广播免shuffle)/SortMergeJoin(两边大各排序归并)/ShuffledHashJoin。选哪个看统计/hint——第10章 AQE 运行时修正。
**prepareForExecution[227]:** 一批准备规则:
- EnsureRequirements:join 两边分区不匹配→插 Exchange(shuffle)节点——SQL 里 shuffle 的来源(第6章)
- CollapseCodegenStages:连续可 codegen 算子包进 WholeStageCodegenExec(第8章)
- InsertAdaptiveSparkPlan:插 AQE 框架(第10章)
**toRdd[242]:** executedPlan.execute() → SparkPlan.execute() → 各算子 doExecute() → codegen 算子的 doExecute = rdds.mapPartitionsWithIndex{跑生成迭代器}(第8章) → RDD[InternalRow]。
> ★ executedPlan.execute() = SQL 与 RDD 的接缝。一条 SQL 历经 parse/analyze/optimize/plan/codegen,到这坍缩成一个普通 RDD(有血缘/分区/compute 跑生成代码)。然后 collect 触发 action 进 DAGScheduler 切 stage,EnsureRequirements 插的 Exchange 成宽依赖/stage 边界。第0-8章一切复用。SQL 没另造引擎,而是编译成千锤百炼的 RDD 引擎——分层架构的胜利。
> ★ 边界: eagerlyExecuteCommands(131-160) 对 Command(CREATE TABLE/INSERT)立即执行(有副作用,不像查询惰性)。

---

## 三、Trick 与 Bug
**① 树+规则=整个架构。** 全是 TreeNode;每优化是可组合可测试的 Rule。Catalyst 可扩展性根本。
**② 不可变树+transform 返回新树(463-490)。** 同第0章 RDD 哲学;fastEquals/ruleId 剪枝是大树性能命脉。
**③ 不动点迭代+双护栏(227-290)。** FixedPoint 跑到不变(优化互相触发);maxIterations+Once幂等+合法性校验防失控。
**④ 每阶段 clone()(173-227)。** 防跨阶段状态污染,流水线层防御性不可变。
**⑤ Analyzer 赋予类型=Tungsten 上游(2.4)。**
**⑥ 经典优化即朴素 Rule(100-272,968)。** 威力来自小正交规则+不动点彼此触发,非单条聪明。
**⑦ toRdd 是 SQL 与 RDD 接缝(242)。** 整条 SQL 坍缩成 RDD,复用第0-8章;Exchange 成宽依赖。
**⑧ Command 急切执行(131-160)。** DDL/DML 有副作用不惰性。

---

## 四、本章总纲
```
哲学: 查询=树, 优化编译=对树施加 Rule 变换(通用"树+规则"框架→可扩展性=Catalyst 胜出根本)

流水线[QueryExecution 惰性,每阶段 .clone() 防污染]:
  SQL→Unresolved →(Analyzer 名字绑定+类型→Tungsten前提)→ Analyzed →(Optimizer)→ Optimized
   →(Planner/Strategy: Join→Broadcast/SortMerge)→ Physical
   →(prepare: EnsureRequirements插Exchange第6章 + CollapseCodegenStages第8章 + InsertAdaptive第10章)→ Executed
   → toRdd=executedPlan.execute() → RDD[InternalRow] → DAGScheduler第3章 → 第0-5章复用

TreeNode[413-490]: 全是树; transform(rule)递归应用返回新树(同RDD不可变); fastEquals/ruleId剪枝
RuleExecutor[120-290]: Batch(rules,strategy); Once vs FixedPoint; 不动点循环跑到树不变; maxIterations+幂等+校验护栏
Optimizer[100-272,968]: PushDownPredicates/ColumnPruning/ConstantFolding = extends Rule + transformDown
   威力=小正交规则+不动点彼此触发; "减少数据操作尽量提前"=优化第一性原理
题眼: executedPlan.execute()[242]=SQL与RDD接缝,SQL坍缩成RDD,SQL=编译到RDD引擎的前端
```

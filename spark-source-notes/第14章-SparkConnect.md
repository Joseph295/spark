# 第 14 章 — Spark Connect:把 driver 从用户进程里剥离

> 对照源码:`sql/connect/common/src/main/protobuf/spark/connect/{base,relations}.proto`、`sql/connect/server/.../planner/SparkConnectPlanner.scala`、`service/SparkConnectService.scala`(apache/spark master)

---

## 一、宏观

经典架构前提:**driver=用户进程**(SparkContext 活在你 JVM,第1章),应用与 Spark 共享进程+classpath。四个痛点:
1. **客户端必须 JVM**(PySpark 靠 Py4J 塞 JVM 绕开)。notebook/IDE/Go/Rust 瘦应用无干净办法连上。
2. **依赖地狱**: 用户 classpath=Spark classpath,库版本冲突(第11章 Akka 被换部分因此)。升级 Spark 要重编译重部署应用。
3. **稳定性差**: driver 里用户代码 OOM/崩溃→整个 app 陪葬。多用户无法共享。胖 driver 又重又脆。
4. **无远程/多租户**: 无法搭长驻 Spark 服务让瘦客户端连。

Spark Connect(3.4+): **瘦客户端 + 远程服务端,gRPC 通信。** 客户端把未解析逻辑计划序列化成 protobuf 发过去;服务端(持真正 SparkSession/driver)做 analyze/optimize/plan/execute(第9章),结果流式回传。

> ★ 最深设计: **协议本身就是第9章未解析逻辑计划,用 protobuf 序列化(窄腰)。** 客户端=会构造 protobuf 的 DataFrame API,服务端=第1-13章一切。任何能生成 proto+说 gRPC 的语言都能当客户端。同 BlockId(第12章)/toRdd(第9章)窄腰哲学:找到所有东西汇聚的最简单的东西定为边界。Connect 找的边界=逻辑计划(早存在于第9章),只是从进程内对象变成跨网络 protobuf。

---

## 二、细节

### 2.1 协议:protobuf 计划(`base.proto`, `relations.proto`)
```protobuf
38  message Plan { oneof op_type { Relation root = 1;  Command command = 2; } }  // 查询=Relation树; 副作用=Command
37  message Relation { oneof rel_type {  // ~40 种,就是序列化的 DataFrame API
40    Read read; Project project; Filter filter; Join join; Aggregate aggregate; SQL sql; ... }}
```
df.filter(...).select(...) 在客户端=往 proto 树叠 Filter/Project 节点。**未解析**(列名未绑定、不访 catalog)——客户端不需 schema/任何 Spark 内部状态,只结构性描述"想要什么"。
> ★ 客户端构造 proto 树**也惰性**: filter/select 只叠节点,action 才 ExecutePlan 发出整树。第0章 RDD 惰性在 Connect 层复现。惰性从 RDD(第0章)→Catalyst树(第9章)→Connect proto(本章)贯穿每层抽象。

### 2.2 gRPC 服务(`base.proto` 1092–1119)
```protobuf
1097  rpc ExecutePlan(...) returns (stream ExecutePlanResponse)   // 返回流!边算边推
1100  rpc AnalyzePlan(...)                                        // 拿 schema/explain 需服务端
1107  rpc AddArtifacts(stream ...)                                // 上传 UDF/jar 到服务端
1119  rpc ReattachExecute(...) returns (stream ...)               // 断线重连
```
- ExecutePlan 返回流: 结果可能很大,服务端边算边推批次,客户端流式接收。
- AnalyzePlan: **连 .schema 都要发 RPC**(客户端无 catalog 无法自己解析)=解耦的代价。
- AddArtifacts: UDF/jar 上传服务端(代码在服务端执行)。
- ReattachExecute: 网络抖动后重新挂回执行中的查询,不从头再来。

### 2.3 服务端: proto → LogicalPlan → 第9章原班流水线(`SparkConnectPlanner.scala` 146–165, 1586–1595)
```scala
148  val plan = rel.getRelTypeCase match {
153    case PROJECT => transformProject(...); 154 case FILTER => transformFilter(...)
158    case JOIN => transformJoinOrJoinWith(...); 165 case AGGREGATE => transformAggregate(...)
1586 transformFilter(rel): baseRel = transformRelation(rel.getInput)        // 递归转子树
1593   logical.Filter(condition = transformExpression(cond), child = baseRel) // → Catalyst Filter
```
递归把整棵 proto 树翻译成等价的未解析 Catalyst LogicalPlan 树。**然后交给第9章那一整套**(Analyzer→Optimizer→Planner→executedPlan→toRdd→DAGScheduler)。服务端=普通 Spark driver,Connect 只改"逻辑计划从哪来"(进程内 DataFrame API → 网络 protobuf)。
> ★ 三前端汇聚同一 LogicalPlan: SQL 字符串经 parser(第9章) / 本地 DataFrame API / Connect protobuf(transformRelation),殊途同归后跑完全相同 Catalyst 优化执行。transformRelation 之于 Connect 如 parser 之于 SQL——又一个"把某表示翻译成 LogicalPlan"的前端。故 Connect 复用整个引擎几乎不动核心:只在第9章流水线最前面接一个新输入源。**好抽象层(LogicalPlan)让你上换前端、下换执行、中间纹丝不动。**

### 2.4 结果回传 Apache Arrow
执行结果经 ExecutePlan 流以 Apache Arrow 批次传回。Arrow=列式/跨语言/近零拷贝→Python/Go/Rust 客户端可直接读,无需 JVM 序列化。语言无关的另一半(proto 解决"发计划",Arrow 解决"收结果")。

### 2.5 多租户:一服务端,多隔离会话
SparkConnectService 每客户端会话一个 SessionHolder。许多客户端共享一个服务端 JVM,各自隔离会话(独立 SQL 配置/临时视图/上传 artifacts 和类加载器——第5章"按 session 隔离类加载器"在此发挥)。**胖 driver 变多租户共享服务**——痛点3/4 的解药。

---

## 三、Trick 与 Bug
**① 协议=未解析逻辑计划 protobuf(窄腰,2.1)。** 任何能生成 proto+gRPC 的语言都能当客户端。同 BlockId/toRdd 窄腰哲学。
**② 客户端惰性构造 proto 树(2.1)。** action 才发。第0章惰性精神复现(RDD→Catalyst→Connect proto 贯穿)。
**③ ExecutePlan 流式 + ReattachExecute 断线重连(2.2)。** 大结果边算边推;网络抖动重挂回。
**④ Arrow 结果格式(2.4)。** 列式跨语言近零拷贝,客户端无需 JVM 序列化。
**⑤ transformRelation: proto→LogicalPlan 然后第9章流水线(2.3)。** 三前端(SQL/DataFrame/Connect proto)殊途同归 LogicalPlan;复用整个引擎不动核心。
**⑥ AddArtifacts + 会话类加载器隔离(2.2,2.5)。** UDF/jar 上传服务端按 session 隔离(第5章)。
**⑦ .schema 需 RPC 往返(2.2)。** 客户端无 catalog,解耦的代价。

---

## 四、本章总纲
```
原罪(driver=用户进程,第1章): ①客户端必须JVM ②依赖地狱 ③用户崩溃整app死 ④无多租户
Spark Connect(3.4+): 瘦客户端+远程服务端 gRPC; 协议=未解析逻辑计划 protobuf(窄腰)
协议: Plan=oneof{Relation|Command}; Relation=~40种oneof=序列化DataFrame API; 未解析; 客户端惰性叠节点action才发(第0章惰性)
gRPC[1092-1119]: ExecutePlan(流式边算边推)/AnalyzePlan(拿schema需往返)/AddArtifacts(传UDF)/ReattachExecute(断线重连)
服务端[SparkConnectPlanner]: transformRelation[146]大match proto树→Catalyst LogicalPlan树 → 第9章原班流水线
  ★ 三前端(SQL parser/本地DataFrame/Connect proto)殊途同归 LogicalPlan; 复用整个引擎不动核心(上换前端下换执行中间不变)
结果: Arrow 批次流式回传(列式/跨语言/近零拷贝,无需JVM序列化)
多租户: SparkConnectService 每会话 SessionHolder,共享JVM隔离会话(第5章类加载器隔离)→胖driver变多租户服务
```

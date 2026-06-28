# 第 1 章 — 执行链路的起点:`SparkContext` 与 `SparkEnv`

> 对照源码:`core/src/main/scala/org/apache/spark/SparkContext.scala`、`core/src/main/scala/org/apache/spark/SparkEnv.scala`(apache/spark master 分支)

---

## 一、宏观:为什么是"两个对象",而不是一个

| | `SparkContext` | `SparkEnv` |
|---|---|---|
| 角色 | driver 端**控制平面门面**(control plane) | 运行时子系统的**装配总线**(substrate) |
| 存在于 | **只在 driver** | **driver 和每个 executor 各一份** |
| 持有 | 用户 API + 调度三件套(DAG/Task/SchedulerBackend) | RpcEnv / BlockManager / ShuffleManager / MapOutputTracker / MemoryManager / Serializer |
| 序列化发 executor | **绝不能**(driver 大脑) | 不传递;executor 自己 `create` 一份 |

**为什么分?** executor 需要运行时基础设施(读写 block、shuffle、管内存、RPC 心跳),但**不需要**调度器和用户 API(切 stage/发 task 是 driver 的事)。塞一个对象里会导致 executor 持有无用逻辑或序列化爆炸。

> 设计:把"每个进程都需要的运行时子系统"抽到 `SparkEnv`(两端各自装配),把"只有 driver 的控制逻辑"留在 `SparkContext`。

> ★ Insight
> - `SparkEnv` 是**进程级服务定位器**,有全局可变单例 `SparkEnv.get`(236–251)。这就是为什么 executor 上用 `SparkEnv.get.blockManager` 而非 `sc.env`——executor 进程里 `sc` 不存在,但 `SparkEnv.get` 永远拿得到本进程那份环境。
> - `SparkContext`(2000+ 行,"上帝对象")的庞大有内聚性:它是 driver 端所有控制状态的唯一持有者和生命周期管理者。这些状态初始化顺序强耦合,硬拆反而制造时序 bug。

---

## 二、细节:逐段对照源码

### 2.1 spark-submit → main(launcher 模块,简述)
`spark-submit`(bash)→ Java 的 `SparkSubmit`:① 解析参数/解决依赖;② 按 `--master`/`--deploy-mode` 决定在哪起 driver;③ 反射 `invoke` 你的 main。**spark-submit 不创建 SparkContext,SparkContext 是你 main 里 new 的。**

### 2.2 SparkContext 初始化顺序(`SparkContext.scala` 490–624)—— 顺序即知识

```scala
495  _env = createSparkEnv(_conf, isLocal, listenerBus)   // ① 先建运行时环境
496  SparkEnv.set(_env)
513  _ui = ... SparkUI.create(...)
523  _ui.foreach(_.bind())          // UI 先 bind 拿端口(521-523 注释:先于调度器,为向 cluster manager 上报地址)
584  _heartbeatReceiver = env.rpcEnv.setupEndpoint(HeartbeatReceiver.ENDPOINT_NAME, ...)  // ② 先于 createTaskScheduler(SPARK-6640)
588  _plugins = PluginContainer(this, ...)
589  _env.initializeShuffleManager()                       // ③ 延迟初始化(SPARK-45762)
590  _env.initializeMemoryManager(numDriverCores(...))
593  val (sched, ts) = SparkContext.createTaskScheduler(this, master)  // ④ 调度三件套
594  _schedulerBackend = sched
595  _taskScheduler = ts
596  _dagScheduler = new DAGScheduler(this)                //    构造时回填自己到 TaskScheduler
622  _taskScheduler.start()                                // ⑤ 必须在 DAGScheduler 构造后(620 注释)
624  _applicationId = _taskScheduler.applicationId()
```

- 495:第一件事建 SparkEnv(后面都依赖它)。
- 584(SPARK-6640):HeartbeatReceiver 必须先注册——executor 构造时要查这个端点,晚了 executor 起不来。
- 593–596:SchedulerBackend(资源)→ TaskScheduler(task)→ DAGScheduler(stage),三层在这一刻串好。

### 2.3 SparkEnv.create:子系统装配(`SparkEnv.scala` 318–500)

driver/executor **共用** `create`,靠布尔量分叉:
```scala
330  val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER   // 唯一的"我是谁"
350  val rpcEnv = RpcEnv.create(...)                       // ① RPC 最先——通信底座
358  val serializer = ...                                  // ② 用户数据序列化器(可配 Kryo/Java)
363  val closureSerializer = new JavaSerializer(conf)      //    闭包专用 Java 序列化(强制,正确性>性能)
376  val broadcastManager = new BroadcastManager(isDriver, conf)
378  val mapOutputTracker = if (isDriver) new MapOutputTrackerMaster(...)  // ③ 主/从分叉
381                         else new MapOutputTrackerWorker(conf)
411  val blockManagerMaster = new BlockManagerMaster(...)  // ④
442  val blockManager = new BlockManager(..., _shuffleManager = null, _memoryManager = null, ...) // ⑤ 传 null!
477  val envInstance = new SparkEnv(executorId, rpcEnv, ...)
```

**365–374 `registerOrLookupEndpoint` —— driver/executor 不对称的核心模式:**
```scala
368  if (isDriver) rpcEnv.setupEndpoint(name, endpointCreator)  // driver: 创建并注册端点
372  else RpcUtils.makeDriverRef(name, conf, rpcEnv)            // executor: 只查 driver 引用
```
MapOutputTracker / BlockManagerMaster / OutputCommitCoordinator 全用此套路。**"driver 权威、executor 从属"在装配代码里的字面体现。**

**378–388 MapOutputTracker 主/从:** driver=`Master`(权威,掌握所有 map 输出位置),executor=`Worker`(缓存代理,本地无则问 Master)。第 6 章 shuffle 基础设施。

**442–453 BlockManager 传 null:** SPARK-45762,见坑①。

### 2.4 SparkEnv holder(`SparkEnv.scala` 60–84, 223–233)
```scala
60   class SparkEnv(executorId, rpcEnv, serializer, ..., blockManager, ..., conf)  // 瘦 holder,全是 val 字段
76   @volatile private var _shuffleManager  // 延迟
82   private var _memoryManager             // 延迟
223  def initializeShuffleManager() { checkState(null==_shuffleManager); _shuffleManager = ShuffleManager.create(...) }
229  def initializeMemoryManager(cores) { _memoryManager = UnifiedMemoryManager(conf, cores) }
```
它是"总线"不是"引擎",逻辑都在它持有的 manager 里。

### 2.5 driver env vs executor env(`SparkEnv.scala` 256–313)
```scala
256  createDriverEnv(...)   → create(..., DRIVER_IDENTIFIER, listenerBus=listenerBus)  // 带 listenerBus(只 driver 用)
291  createExecutorEnv(...) → create(...); env.initializeMemoryManager(cores); SparkEnv.set(env)  // 不带 bus,立即初始化 memoryMgr
```
- driver 带 listenerBus(282),executor 不带(332–335 assert)。事件监听是 driver 职责。
- memoryManager:executor 立即初始化(310);driver 推迟到 SC 590 行(等 DriverPlugin 加载完可改内存配置)。

---

## 三、Trick 与 Bug 汇总

**① SPARK-45762:ShuffleManager 延迟初始化打破鸡生蛋(437–441 注释)。** 为支持用户 jar 自定义 ShuffleManager,其创建推迟到 SC/Executor 阶段(用户 jar 加载后)。但 BlockManager 构造需要 ShuffleManager。解法:BlockManager 内部 **lazy val** 从 SparkEnv 取(构造传 null)。扩展性 vs 初始化时序的权衡——用延迟求值破循环依赖。

**② SPARK-6640:HeartbeatReceiver 先于 TaskScheduler 注册(584)。** executor 构造要查此端点,晚了起不来。隐蔽的分布式端点时序 bug,用注释把隐式约束钉死。

**③ 闭包 Java 序列化,数据可配序列化(358 vs 363)。** 正确性敏感的闭包走稳的 Java,性能敏感的数据走快的 Kryo。

**④ `_hadoopConfiguration.size()` 预热 trick(526–534)。** Hadoop Configuration 的 properties 惰性解析 XML(昂贵 IO)。故意提前触发一次缓存父配置,子 Configuration 直接 clone,避免每次新建都重解析 XML。预热惰性字段,但必须配注释否则被当垃圾删。

**⑤ SPARK-41188/28843:OMP_NUM_THREADS 线程爆炸(572–580)。** 不设则用 `spark.task.cpus` 覆盖。OpenBLAS/OpenMP 默认按物理核开线程池,多 task executor 下线程数=task×核,爆炸。native 库不受 JVM 线程管理,是隐藏资源炸弹。

**⑥ `SparkEnv.set` 全局可变单例(242–251)。** local 模式下 driver 和"executor"共用同一单例——这是 local 模拟分布式的基础,也是"local 能跑、集群挂"的根源(local 下 SparkEnv.get 永远同一个,掩盖序列化/隔离问题)。

---

## 四、本章总纲

```
spark-submit (launcher/SparkSubmit, 反射拉起 main)
 └─ new SparkContext [490-624]   ← driver 控制平面
      ├─ createSparkEnv [495] → SparkEnv.create [318-500]   ← 进程级运行时总线
      │     isDriver 分叉 [330]；RpcEnv [350] → Serializer×2 [358,363]
      │     → MapOutputTracker 主/从 [378] → BlockManagerMaster [411] → BlockManager(shuffleMgr=null) [442]
      │     registerOrLookupEndpoint: driver建端点 / executor查引用 [365-374]  ← 主从架构本体
      ├─ UI bind(先于调度器) [523]
      ├─ HeartbeatReceiver(先于TaskScheduler, SPARK-6640) [584]
      ├─ initializeShuffle/MemoryManager(延迟, SPARK-45762) [589-590]
      ├─ createTaskScheduler → Backend/Task/DAGScheduler [593-596]
      └─ taskScheduler.start() → applicationId [622-624]
executor: createExecutorEnv [291] 走同一 create, 但 registerOrLookup 走"查引用"分支
```

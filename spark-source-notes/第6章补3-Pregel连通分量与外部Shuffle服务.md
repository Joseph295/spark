# 第 6 章补3 — GraphX Pregel/连通分量 与 External Shuffle Service

> 对照源码:`graphx/.../Pregel.scala`、`graphx/.../lib/ConnectedComponents.scala`、`common/network-shuffle/.../ExternalShuffleBlockResolver.java`(apache/spark master)

---

# 一、GraphX Pregel 与连通分量(LSH 去重下游)

## 1.1 Pregel = 图上的 BSP 模型
Pregel="图上的 MapReduce"。执行模型 BSP(整体同步并行): 计算分超步(superstep),每超步=每顶点接收消息→vprog 更新自己→sendMsg 向邻居发消息,超步间有屏障。GraphX 实现在 RDD 之上。连通分量/PageRank/标签传播底层都是它。

## 1.2 三函数 + 循环(`Pregel.scala` 116–177)
三个用户函数:
- vprog(vid, vdata, msg): 顶点收到合并消息如何更新自己
- sendMsg(triplet): 对一条边给哪端点发什么消息
- mergeMsg(a,b): 同顶点多消息怎么合并(交换+结合律=第2章 reduceByKey 的 combiner!)
```scala
131  var g = graph.mapVertices((vid,vdata) => vprog(vid,vdata,initialMsg))      // 初始化
137  var messages = mapReduceTriplets(g, sendMsg, mergeMsg)                     // 超步:发消息+按目标顶点合并 ← shuffle
146  while (isActiveMessagesNonEmpty && i < maxIterations) {
149    g = g.joinVertices(messages)(vprog)                                      // 用消息更新顶点 ← 又一shuffle
156    messages = mapReduceTriplets(g, sendMsg, mergeMsg, Some((oldMessages, activeDirection)))  // 下一超步
162    isActiveMessagesNonEmpty = !messages.isEmpty()                           // 无活跃消息=收敛
```
mapReduceTriplets(对每条边跑sendMsg+按目标顶点mergeMsg聚合)=一次shuffle; joinVertices=又一shuffle。每超步1-2 shuffle,迭代到无活跃消息。156 activeDirection 优化: 只对"端点上步收到消息"的边跑sendMsg。

## 1.3 精密舞蹈: checkpoint + cache + unpersist(129–176)
**"迭代算法必须 checkpoint"的源码级证据。**
```scala
132/138  PeriodicGraphCheckpointer / PeriodicRDDCheckpointer(checkpointInterval)
150/161  graphCheckpointer.update(g) / messageCheckpointer.update(messages)  // 周期 checkpoint,斩断血缘
167/168/169  oldMessages.unpersist(); prevG.unpersistVertices(); prevG.edges.unpersist()  // 释放上轮
```
1. **周期 checkpoint**: 每 checkpointInterval 超步 checkpoint g/messages 截断血缘(第0章2.7)。没它跑50超步血缘叠50+层→driver OOM/全量重算/StackOverflowError(第0章0.4)。LSH 连通分量正依赖它。
2. **cache→物化→unpersist 顺序(152-169)**: messageCheckpointer.update 内部 count() 强制物化新 messages("挡住"oldMessages/prevG)→物化后才能安全 unpersist 上轮。
> ★ "先cache新→物化新→再unpersist旧"=所有 Spark 迭代算法黄金模板,正确性命悬一线: unpersist太早(新messages血缘还依赖旧)→触发对已释放数据重算(thrash); 永不unpersist→内存爆。Pregel 一次性封装好,故 connectedComponents 才十几行。跑 LSH 连通分量=白嫖这套千锤百炼的 checkpoint 舞蹈。好抽象(Pregel/BSP on RDD)把分布式迭代最凶险的内存/血缘管理收敛到一处,上层只需定义三函数。

## 1.4 连通分量=Pregel 一行实例(`ConnectedComponents.scala` 42–57)
```scala
42  ccGraph = graph.mapVertices((vid,_) => vid)         // 初始标签=自己id
43  sendMessage(edge) = if (srcAttr<dstAttr) (dstId,srcAttr) else if (srcAttr>dstAttr) (srcId,dstAttr) else empty  // 较小标签传邻居
53  Pregel(ccGraph, Long.MaxValue, ...)(
55    vprog = (id,attr,msg) => min(attr,msg),           // 采纳听到的最小标签
57    mergeMsg = (a,b) => min(a,b))                      // 多消息取最小
```
标签传播: 初始自己id,每轮把较小标签传邻居+采纳最小。收敛时每顶点持有其连通分量最小顶点id=分量标识。
> ★ 连通分量就十几行(重活全在 Pregel:每超步shuffle/checkpoint舞蹈/收敛循环)。闭环 LSH 去重: 候选对(补2)→建图(边=相似)→ConnectedComponents→每分量=重复簇→每簇留一。现在能源码级回答"为何LSH去重必checkpoint": ConnectedComponents 调的 Pregel 每超步叠一层血缘,PeriodicGraphCheckpointer 才是能跑几十轮不爆栈的功臣。

---

# 二、External Shuffle Service 协议(第13章伏笔兑现)

## 2.1 要解决什么(回第13章)
第13章动态分配坑: 杀空闲 executor→其本地 shuffle 文件没了→FetchFailed。解药=外部 shuffle 服务。核心:
> 把"提供 shuffle 文件"从 executor 进程剥离,交给每节点一个独立长驻守护进程→shuffle 文件寿命超过写它的 executor。

## 2.2 它是什么进程
独立的、每节点一个的长驻守护进程:
- YARN: YarnShuffleService 作为辅助服务跑在 NodeManager 内部(yarn.nodemanager.aux-services),与 NodeManager 同寿
- Standalone/K8s: 独立进程/DaemonSet
属 network-shuffle 模块(和补2 push-merge RemoteBlockPushResolver 同模块),复用第11章 Netty 传输。

## 2.3 协议两步(`ExternalShuffleBlockResolver.java`)
**① registerExecutor(151-169): executor 启动告诉服务文件位置**
```scala
151  registerExecutor(appId, execId, ExecutorShuffleInfo{localDirs[], subDirsPerLocalDir, shuffleManager})
160  if (db != null && recoveryEnabled) db.put(key, executorInfo)   // 持久化本地 LevelDB(服务重启可恢复)
168  executors.put(fullId, executorInfo)
```
服务从此知道"这 executor 把 shuffle 写在本地盘哪些目录"。
**② getSortBasedShuffleBlockData(174-340): reduce 来要块,服务读盘返回**
```scala
194  executor = executors.get(AppExecId(appId, execId))           // 查注册信息
327  indexFilePath = ExecutorDiskUtils.getFilePath(localDirs, ..., "shuffle_S_M_0.index")
333  shuffleIndexCache.get(indexFilePath)                         // 读索引(缓存避免重复解析)
334  getIndex(startReduceId, endReduceId)                         // 查这reduce分区字节区间
336  return new FileSegmentManagedBuffer(..., .data 那一段)
```
用和 IndexShuffleBlockResolver(第6章)完全相同的路径约定+索引格式(320注释"This logic is from IndexShuffleBlockResolver")。

## 2.4 为何 executor 死了还能服务(第13章兑现)
- shuffle 文件在**节点本地盘**(executor localDirs),不在 executor 进程内存→executor 死文件不删
- 服务是独立进程,通过注册(持久化DB)知道文件在哪
- 故 executor 死后 reduce 连**服务**(非死掉的executor),服务读盘发出
> ★ 第13章伏笔兑现: 有外部 shuffle 服务,动态分配才能安全杀空闲 executor(shuffle 输出留盘由节点本地服务提供),executor 在"提供shuffle"上变无状态可随意伸缩。
> ★ 最小侵入性: **不改 shuffle 文件格式**(直接复用 IndexShuffleBlockResolver 路径约定+索引),只在同批文件前加一个寿命更长的读取者。故是小改动却解锁动态分配——不重新发明存储,只在前面放稳定服务器。第6章"data+index"设计**第三次结清红利**(一拉取本身/二第10章AQE纯读端重切/三此处): reduce分区="文件里索引界定的字节区间",故完全独立进程只要能读索引就能服务,无需 executor 配合。

## 2.5 与三代 shuffle 的关系(收束)
- 外部 shuffle 服务=push-based(Magnet)的地基: 补2 的 merger 节点就是外部 shuffle 服务(加 RemoteBlockPushResolver 扩展)。同一守护进程既服务 pull 拉取(本节)又服务 push 合并(补2)。
- Celeborn 彻底取代它: 专用远程服务(非节点本地),shuffle 文件不落 compute 节点盘。

演进图:
```
经典pull(第6章): map写本地data+index, reduce直连executor拉
  ↓ 加节点本地长驻服务,寿命超executor
外部shuffle服务(本节): 同样data+index,独立进程读盘服务→executor可死/可杀→解锁动态分配(第13章)
  ↓ 服务不只"读",还在reduce端"预合并"
push/Magnet(补2): 同服务+RemoteBlockPushResolver,M块merge成1大块
  ↓ 整个shuffle状态搬离compute节点
Celeborn(补2): 专用远程集群,shuffle不落compute本地盘→compute完全无状态(spot/K8s根本解)
```
> ★ 全书连续主线: 从第6章"M×R小文件爆炸"→Sort Shuffle"data+index"→外部服务把文件寿命与executor解绑(第13章动态分配)→Magnet服务端预合并→Celeborn把shuffle搬出compute。每步回答同一问题: shuffle数据怎么组织、由谁持有,才能既快又不怕节点丢失。LLM的PB级+spot把"不怕节点丢失"推到顶点,故终点Celeborn成大模型数据管线标配。现在能沿源码一节节推导从单机文件到云原生解耦服务的演进——这正是"知其所以然"最终的样子。
```

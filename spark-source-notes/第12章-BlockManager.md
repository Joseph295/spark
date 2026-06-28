# 第 12 章 — BlockManager:一身多职的统一存储层

> 对照源码:`core/.../storage/BlockManager.scala`、`storage/BlockId.scala`(apache/spark master)

---

## 一、宏观:四件事,一个抽象

前五章反复出现的名字,底层全落在 BlockManager:
- cache 的 RDD 块(第0章 getOrElseUpdateRDDBlock)
- taskBinary 广播(第3章)
- 大 task 结果 TaskResultBlockId(第5章)
- shuffle 输出块(第6章)
- storage 内存区放的缓存块(第7章驱逐)

> BlockManager = 通用存储层,`BlockId → 数据` 的 KV 存储,后端"内存+磁盘+堆外",支持副本,全集群可寻址。**cache/shuffle/broadcast/task结果 四件看似不同的事,本质都是"存取不同 BlockId 的块"。**

"窄腰"设计:所有东西汇聚到一个简单接口,传输/驱逐/副本只实现一次。
架构=第三次见的**主从模式**(前两次 RPC端点/MapOutputTracker):
- BlockManagerMaster(driver):权威目录,记录哪个块在哪个 executor
- BlockManager(每executor):真正存数据并对外提供

---

## 二、细节

### 2.1 统一的键 BlockId(`BlockId.scala` 36–264)
sealed 类型族,**类型编码用途,存储层不关心**:
```scala
54   RDDBlockId(rddId, splitIndex)              // cache/persist(第0章)
61   ShuffleBlockId(shuffleId, mapId, reduceId) // shuffle 输出(第6章)
159  BroadcastBlockId(broadcastId, field)       // 广播/taskBinary(第3章)
164  TaskResultBlockId(taskId)                  // 大 task 结果(第5章)
```
第0章 RDDBlockId(id, partition.index) 只是这个家族一员。四用途共走同一套 put/get/fetch。

### 2.2 存储层级 StorageLevel/MemoryStore/DiskStore(`BlockManager.scala` 959–1010)
StorageLevel = (useMemory, useDisk, useOffHeap, deserialized, replication) = persist(level) 所设。
```scala
969  if (level.useMemory && memoryStore.contains)      // 在内存?
970    if (level.deserialized) memoryStore.getValues    // 直接拿对象
973    else serializerManager.dataDeserializeStream     // 序列化形式则反序列化
983  else if (level.useDisk && diskStore.contains)     // 在磁盘?
991    maybeCacheDiskValuesInMemory(...)               // 读盘顺便可能回填内存
```
> ★ MemoryStore 管的内存=第7章 UnifiedMemoryManager 的 storage 区。第7章 execution 抢占 storage 调 freeSpaceToShrinkPool 驱逐的就是 MemoryStore 的缓存块。**第7章=会计(谁能用多少/谁抢谁),第12章=仓库(数据怎么放/取)。** 合看才懂 cache 一生:persist设级别→MemoryStore存→storage区记账→execution紧张被驱逐→下次miss→血缘重算(第0章)。

### 2.3 统一取数:本地优先,远程兜底(`BlockManager.scala` 1289–1321)
```scala
1309  get[T](blockId):
1310    local = getLocalValues(blockId); if (defined) return local   // ① 自己的 MemoryStore/DiskStore
1315    remote = getRemoteValues[T](blockId); if (defined) return remote  // ② 去远端拉
1320    None
```
远程路径(getRemoteBytes 1289):本地没有→先问 BlockManagerMaster"谁有这块?"拿 BlockManagerId→经 network-common(第11章)从那 executor 拉回。executor 读另一 executor 的 cache、reduce 拉 map 输出、broadcast 从 peer 拉,全走这条。

### 2.4 闭合第0章 getOrElseUpdateRDDBlock(`BlockManager.scala` 1371–1430)
```scala
1371  getOrElseUpdateRDDBlock(taskId, blockId, level, classTag, makeIterator):
1412    get[T](blockId) match { case Some(block) => return Left(block)   // ① 命中(本地/远程)直接返回
1424    doPutIterator(blockId, iterator, level, ...)                      // ② 没命中→算(makeIterator)并存
```
> ★ get 返回 None(块被驱逐)→ 调 makeIterator()=第0章 getOrCompute 传入的 computeOrReadCheckpoint=重跑 RDD compute 沿血缘重算。**第0章"cache 丢了能重算"在这一行兑现。**

### 2.5 锁与可见性
一 executor 多 task 可能同碰一块,blockInfoManager 给每块加读写锁(961 lockForReading)。
> ★ 锁随迭代消费自动释放(979 CompletionIterator):读块返回的迭代器完整消费后自动释放读锁,类 RAII;task 完成时自动释放该 task 所有块锁(SPARK-27666,1338)双保险。
> ★ 可见性(1377 isRDDBlockVisible):产出块的 task 成功提交前块"不可见"。防读到失败 task 写一半的缓存块,保证命中永远是完整正确数据。确定性/正确性主题在存储层体现。

### 2.6 四用途汇流(尤其 broadcast 的 BitTorrent)
- **cache**: getOrElseUpdateRDDBlock → MemoryStore(第7章可驱逐)→ 丢了血缘重算(第0章)
- **shuffle**: ShuffleBlockId,map 输出由 IndexShuffleBlockResolver 管(第6章),跨节点拉取走同套 transport
- **task 结果**: 大结果 putBytes 成 TaskResultBlockId,driver getRemoteBytes 拉(第5章)
- **broadcast**: TorrentBroadcast 把广播值切成多个 BroadcastBlockId 块存 BlockManager,executor **像 BitTorrent 从已有该块的 peer 拉,不全挤 driver**
> ★ broadcast 的 BitTorrent 是第3章 taskBinary 扩展到上千 executor 的秘密。几千 executor 同时找 driver 拉会打爆网卡(早期 HttpBroadcast 真实瓶颈)。TorrentBroadcast 切块+executor 互传——先拿到块的成为新源,负载病毒式扩散,driver 每块只发一次。**第3章"广播避免N份重复传输"省网络 与 第12章 TorrentBroadcast peer 扩散,是同一目标在两层次的实现。** 故"大对象用广播变量"(第4章报错建议)不只省内存,更把传输从 driver 单点扇出变成集群 P2P 扩散。

副本: StorageLevel 可设副本(MEMORY_ONLY_2),doPut 复制到 peer。但 **cache 副本通常没必要**(丢了血缘重算,第0章),主要用于重算极贵/executor decommission 不丢数据。又一次:血缘容错省掉别系统必须的副本开销。

---

## 三、Trick 与 Bug
**① 统一 BlockId→一存储服务四用途(2.1)。** "窄腰"设计,传输/驱逐/副本只实现一次。
**② MemoryStore=第7章 storage 区(2.2)。** 驱逐缓存=drop MemoryStore 块。会计 vs 仓库两视角。
**③ get 本地优先远程兜底(1309)。** 远程先问 master 定位再 network-common 拉(第11章)。第三次主从。
**④ getOrElseUpdateRDDBlock 闭合第0章(1371)。** miss→makeIterator 沿血缘重算。
**⑤ 块级读写锁+自动释放(961,979,1338)。** CompletionIterator 随迭代消费解锁(RAII);task 完成释放所有锁(SPARK-27666)。
**⑥ 块可见性(1377)。** 失败 task 半成品块不可见,保证命中完整正确。
**⑦ TorrentBroadcast BitTorrent 扩散(2.6)。** 切块 P2P,避免 driver 单点扇出。第3章 taskBinary 扩展秘密。
**⑧ cache 副本通常不必要(2.6)。** 血缘可重算,省别系统的副本开销。

---

## 四、本章总纲
```
统一: BlockManager=BlockId→数据 KV存储(内存+磁盘+堆外,可副本,全集群可寻址)
  四事共用一抽象("窄腰",传输/驱逐/副本实现一次): RDDBlockId/ShuffleBlockId/BroadcastBlockId/TaskResultBlockId
  主从(第3次): BlockManagerMaster(driver目录) + BlockManager(executor存数据)
StorageLevel=(useMemory,useDisk,useOffHeap,deserialized,replication)=persist(level)
getLocalValues[959]: useMemory→MemoryStore / useDisk→DiskStore(可回填内存)
  ★ MemoryStore=第7章 storage 区, 驱逐缓存=drop 块(会计vs仓库)
get[1309]: 本地优先→远程兜底(问master定位+network-common拉,第11章)
getOrElseUpdateRDDBlock[1371]: miss→makeIterator(computeOrReadCheckpoint沿血缘重算)→闭合第0章
锁: 块级读写锁+CompletionIterator自动解锁(RAII)+task完成释放(SPARK-27666); 可见性防失败task半成品块
四用途: cache(可驱逐+重算)/shuffle(IndexResolver+同transport)/结果(putBytes+getRemoteBytes)/broadcast(TorrentBroadcast BitTorrent P2P)
副本: cache通常不必要(血缘重算),用于重算极贵/decommission
```

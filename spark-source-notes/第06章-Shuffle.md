# 第 6 章 — Shuffle:三代演进与"一个文件"的智慧

> 对照源码:`core/.../shuffle/sort/SortShuffleManager.scala`、`SortShuffleWriter.scala`、`scheduler/MapStatus.scala`、`MapOutputTracker.scala`(apache/spark master)

---

## 一、宏观

第 5 章 `ShuffleMapTask` 调 `dep.shuffleWriterProcessor.write(rdd.iterator(...))`,背后是分布式最棘手的问题:**全体对全体的数据重分发**。

M 个 map × R 个 reduce:每个 map 按 R 切开输出,每个 reduce 去 M 个 map 收集属于自己的份。M×R 连接矩阵——最慢(序列化/磁盘/网络)、最易失败(FetchFailed)。

**三代演进(每代被前代痛点逼出):**
```
Hash Shuffle(已删除):  每 map 为每 reduce 写一文件 → M×R 文件爆炸
Sort Shuffle(当前基石): 每 map 只写【1 数据文件 + 1 索引文件】→ 文件数降到 2M
Tungsten/Unsafe(最优):  在 Sort 上对【序列化字节】排指针,绕开对象(第8章伏笔)
```
连接 write/read 两端的是 MapOutputTracker(第1章装配、第3章注销、第5章 epoch 作废,本章看正向使用)。

---

## 二、细节

### 2.1 文件爆炸:Hash Shuffle 之死
map m 给 reduce r 写文件 shuffle_m_r → M×R 文件。1万 map×1千 reduce=千万文件 → 耗尽句柄/随机写崩溃/reduce 端开海量流。**记住 M×R,第二代努力就是降到 M。**

### 2.2 第二代核心:1 数据文件 + 1 索引文件
- 数据文件:R 个 reduce 分区的数据**按分区号顺序拼接**成一个连续文件。
- 索引文件:记录 R 个分区在数据文件里的起始偏移。
- reduce r:读索引查分区 r 的字节区间 → 读数据文件那一段。文件数 M×R → 2M,每 map 一次顺序写。(IndexShuffleBlockResolver)
> ★ "用一层索引把随机写变顺序写";"逻辑分区≠物理文件,用索引建映射"。同思想见 Parquet row group、LSM-Tree。遇"小文件太多"先想"合并大文件+索引"。

### 2.3 三种 writer 自动选择(`SortShuffleManager.scala` 90–108, 225–244)
决策树在 registerShuffle(第0章2.3:宽依赖构造即调,决策此刻定):
```scala
93   if (shouldBypassMergeSort(conf, dep))      → BypassMergeSortShuffleHandle  // ①旁路
101  else if (canUseSerializedShuffle(dep))     → SerializedShuffleHandle       // ②Tungsten
105  else                                       → BaseShuffleHandle             // ③通用
```
**① shouldBypassMergeSort(102-110):** `!mapSideCombine && numPartitions <= bypassMergeThreshold(默认200)` → BypassMergeSortShuffleWriter。借 Hash 思路:每 reduce 开临时文件直接写(不排序),最后拼接成 1 数据+索引。分区少时免排序最快,开 R 文件内存可接受。
**② canUseSerializedShuffle(225-244):**
```scala
228  !serializer.supportsRelocationOfSerializedObjects → false   // 序列化器须支持重定位
232  dependency.mapSideCombine → false                          // 不能 map 端聚合
236  numPartitions > 1677万 → false                            // ≤ PackedRecordPointer.MAX_PARTITION_ID+1
```
→ UnsafeShuffleWriter(Tungsten,见2.5)。
**③ 否则 → SortShuffleWriter:** 兜底,唯一能 map 端聚合,操作反序列化对象。

### 2.4 通用 SortShuffleWriter.write(`SortShuffleWriter.scala` 52–73)
```scala
53   sorter = if (dep.mapSideCombine) ExternalSorter(aggregator, partitioner, keyOrdering) else ExternalSorter(无聚合无排序)
63   sorter.insertAll(records)                          // ① 灌入,内存满则 spill 磁盘(external!)
70   sorter.writePartitionedMapOutput(...)              // ② 内存+spill 归并,按分区号写进一个数据文件
71   partitionLengths = mapOutputWriter.commitAllPartitions(...)  // ③ 原子提交数据文件+索引,得各分区长度
72   mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)  // ④ 产出 MapStatus
```
ExternalSorter:有 aggregator 则灌入时就地按 key 聚合;有 ordering 则排序;总按目标分区号组织。**external=内存放不下 spill 磁盘**,故能处理超内存(与 groupByKey OOM 的本质区别)。

### 2.5 第三代 Tungsten:排指针不排对象
普通 Sort 排 JVM 对象:对象头膨胀、指针跳转 CPU 缓存命中低、GC 压力大。
Tungsten:record 序列化成字节存连续内存,排序只移 8 字节指针。
- **PackedRecordPointer**:把(分区号高位 + 内存地址低位)打包进 64 位 long。排 long = 按分区排,**相邻紧凑 long 数组,CPU 缓存极友好**;不碰 record、零反序列化、零 GC;spill 间可直接拼接字节归并。
- 三前提的硬道理:
  - **序列化器支持 relocation**:字节要在内存搬移、跨 spill 拼接,格式须保证记录搬走后能独立解出。Java 序列化不行(跨对象引用表),Kryo 配置后行 → 用 Tungsten 常配 Kryo。
  - **不能 map 端聚合**:聚合需反序列化对象 merge,Tungsten 全程不反序列化。
  - **分区 ≤ 1677万**:分区号塞进指针高位仅 24 位,2^24≈16M。16M 上限的根源=指针位数物理事实。
> ★ 三 writer 选择 = 三个递进问题:要 map 端聚合?(要→通用) 分区很少?(少且不聚合→旁路免排序) 能绕开对象?(relocation且不聚合→Tungsten)。调优(bypassMergeThreshold/Kryo/避免groupByKey)都是引导这棵树。

### 2.6 MapStatus 压缩(`MapStatus.scala` 74–99)—— 又见第0章 OOM
MapStatus 含该 map 对每个 reduce 分区的输出大小(reduce 估算拉多少)。M 个 ×R 个 = M×R,老实存撑爆 driver(第0章2.3 OOM 警告)。
```scala
78   if (length > minPartitionsToUseHighlyCompress(2000)) HighlyCompressedMapStatus else CompressedMapStatus
92   compressSize: ceil(log_1.1(size)) → 1 字节,256值表 ~35GB,最多 10% 误差
```
- **CompressedMapStatus(<2000)**:每分区 1 字节(log_1.1 编码)。10% 误差可容忍——只给 reduce 估算用,不需精确。用精度换空间且对用途无害。
- **HighlyCompressedMapStatus(≥2000)**:只存 平均大小 + 空块位图(RoaringBitmap) + 少数超大块精确值。压缩几个数量级。第0章 1<<30 OOM 警告的另一面。

### 2.7 连接两端 MapOutputTracker(`MapOutputTracker.scala` 841, 1659–1711)
```scala
841   registerMapOutput(shuffleId, mapIndex, status)         // map 端:账本记一笔
1712  // reduce 端 getMapSizesByExecutorId → convertMapStatuses:
      splitsByAddress.getOrElseUpdate(mapStatus.location, ...) += ((ShuffleBlockId(...), size, mapIndex))
      // 输出 splitsByAddress: BlockManagerId(executor) → 该去它那拉的块清单
```
reduce 的 BlockStoreShuffleReader 拿这张表,**按 executor 分组并发拉取**(可 batch fetch 连续块),拉回归并/聚合/排序。
> ★ 按 BlockManagerId(executor)分组而非按 map:一个 executor 跑过多个 map,对每个 executor 建一次连接一把拉走所有相关块,最小化连接数。MapOutputTracker 顺手做了"按目的地合并请求"。
> ★ 彻底理解 FetchFailed:reduce 拿 splitsByAddress 去拉,executor 挂/块没→FetchFailed→driver unregisterMapOutput 抹账→重算 map。**write 登记账本、read 消费账本、容错改账本——MapOutputTracker 是 shuffle 三动作公共轴。**

---

## 三、Trick 与 Bug
**① 文件爆炸=Hash Shuffle 之死。** Sort 用 1数据+1索引 把 M×R 降 2M。逻辑分区折叠进物理文件+索引。
**② 三 writer 决策树(90-108)。** 聚合→通用;分区少且不聚合→Bypass免排序;relocation且不聚合→Tungsten。
**③ Tungsten 排指针(2.5)。** PackedRecordPointer 8字节(24位分区+40位地址),缓存友好/零反序列化/零GC;前提 relocation+不聚合+≤16M。
**④ MapStatus 两级压缩(74-99)。** <2000每分区1字节(log_1.1,10%误差); ≥2000均值+空块位图+超大块。防 driver OOM。
**⑤ MapOutputTracker 按 executor 聚合拉取(1659-1711)。** 最小化连接数。
**⑥ ExternalSorter 的 external(63)。** 内存满 spill 磁盘归并,处理超内存数据(vs groupByKey OOM)。
**⑦ MapOutputTracker 是 shuffle 三动作公共轴。** 登记/消费/抹账+epoch作废。

---

## 四、本章总纲
```
问题: M map × R reduce 全体对全体重分发
  Hash: shuffle_m_r → M×R 爆炸 → 删除
  Sort: 每 map 写【1数据(R分区按序拼接)+1索引(R偏移)】→ 2M;reduce 读索引查区间→读数据段
写端选择(registerShuffle 90-108,宽依赖构造即定):
  不聚合&分区≤200 → Bypass(免排序)
  relocation序列化器&不聚合&分区≤16M → UnsafeShuffleWriter(Tungsten,排指针)
  else → SortShuffleWriter(唯一能map端聚合) → ExternalSorter(满则spill)→writePartitioned→commit→MapStatus
Tungsten: PackedRecordPointer(分区24位+地址40位)塞8字节long,排long=排分区,缓存友好/零反序列化/零GC
MapStatus压缩: <2000每分区1字节(log_1.1,10%误差); ≥2000均值+空块位图(防driver OOM)
连接两端 MapOutputTracker:
  map registerMapOutput[841] → reduce getMapSizesByExecutorId→convertMapStatuses[1659]→splitsByAddress(按executor)
  → BlockStoreShuffleReader 按 executor 并发拉取
  容错: FetchFailed→unregisterMapOutput(第3章)+epoch作废worker缓存(第5章)
```

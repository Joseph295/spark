# 第 6 章补 — Shuffle 深挖 × 大模型预训练实战

> 对照源码:`core/src/main/java/org/apache/spark/shuffle/sort/{UnsafeShuffleWriter,BypassMergeSortShuffleWriter}.java`(apache/spark master)

---

## 一、源码再深一层:三种 Writer 的 write() 干什么

### 1.1 BypassMergeSortShuffleWriter:R 个文件直写再拼接(138–250)
```java
152  partitionWriters = new DiskBlockObjectWriter[numPartitions];   // 每 reduce 分区一个独立 writer
174  partitionWriters[partitioner.getPartition(key)].write(key, record._2());  // 每条直写对应分区文件
216  if (transferToEnabled) transferTo(...)                         // 最后零拷贝拼成一个
```
无排序无内存缓冲池; 代价=同时 R 个 fd + R×32KB 写缓冲 → R 大就爆。故 bypassMergeThreshold 默认 200。**LLM 场景 reduce 分区成千上万,基本用不上。**

### 1.2 UnsafeShuffleWriter(Tungsten):序列化字节进 sorter,spill,零拷贝归并(241–296)
```java
241  insertRecordIntoSorter(record):
246    serOutputStream.writeKey/writeValue(...)               // ① 先序列化成字节
253    sorter.insertRecord(serBuffer.getBuf(), ..., size, partitionId)  // ② 字节+分区号 塞 sorter
```
ShuffleExternalSorter 只存序列化字节(内存 page)+ LongArray of PackedRecordPointer(分区24位+地址40位); 排序只对 long 数组 radix sort(按分区),不碰记录。是 MemoryConsumer(第7章),内存不够 TaskMemoryManager 逼 spill()。mergeSpills(269)归并 spill 文件。
> ★ mergeSpills 两条路决定 CPU 开销: ① **transferTo 零拷贝**(压缩编解码器支持拼接已压缩流,如 lz4/snappy/zstd)→ spill 不解压直接 sendfile 拼接,几乎不耗 CPU; ② **fileStream**(否则)→ 逐条解压重压缩,CPU 暴涨。故 LLM 大 shuffle: `spark.io.compression.codec=zstd/lz4 + spark.file.transferTo=true` 命中零拷贝快路径。

### 1.3 LLM 视角对照
| Writer | 排序 | map端聚合 | 内存 | LLM |
|---|---|---|---|---|
| Bypass | 无 | 否 | R fd+R×32KB | R太大不用 |
| Unsafe(Tungsten) | 排指针 | 否 | 序列化字节page+LongArray,可spill | dedup/repartition 主力,配Kryo+zstd |
| Sort通用 | 排对象 | 可 | 反序列化对象,可spill | 需 reduceByKey 聚合时 |

---

## 二、大模型预训练管线的 Shuffle 难题

Spark 角色: **预训练数据准备(data curation),非训练本身**(训练是 GPU 上 PyTorch/JAX)。管线:
```
CommonCrawl WARC → 抽正文 → 质量过滤(启发式+分类器) → 去重(精确+模糊) → PII清洗 → 分词 → 转训练格式 → 全局打散
```
几乎每个重活都是 shuffle,PB级+长时作业把第0/3/6章的坑放大到极致。

### 2.1 头号敌人:数据倾斜
LLM 语料 key 天然极度倾斜: 样板文本(导航/版权/cookie提示)hash 相同→一分区上亿条; MinHash band 高频 shingle 桶爆炸; 大站域名占比高。后果=第3/4章"stage 完成取决于最慢 task"。
解法分层:
1. **AQE 动态拆倾斜分区(第10章 OptimizeSkewedJoin)** 首选但主要管 join; 开 spark.sql.adaptive.skewJoin.enabled
2. **加盐 salting**: 热点 key 拼随机前缀(key#0..key#N)打散→部分聚合→去盐→最终聚合。两阶段聚合+加盐把巨桶摊成 N 个
3. **热点 key 隔离**: 先统计 top-K 重 key 单独处理(广播/特殊路径),长尾走正常 shuffle
4. **源头避免**: 别用 groupByKey(第2章单key全量入内存直接OOM),用 reduceByKey/aggregateByKey/dropDuplicates 让 map 端预聚合削量

### 2.2 第二敌人:FetchFailed 级联与 spot 抢占
PB级+几小时+上万task,executor 丢失是常态(尤其 spot)。executor 丢→本地盘 shuffle 输出没了(第13章)→FetchFailed(第3章)→map stage 重算。若中间有 repartition(round-robin INDETERMINATE),按 SPARK-23207 整条下游链回滚→作业反复回滚永远跑不完。
解法:
1. **昂贵 shuffle 前 checkpoint(第0/3章)** 斩断血缘,失败不从头重算。迭代去重(连通分量)尤其需要
2. **外部 shuffle 服务(第13章)** shuffle 独立于 executor 存活
3. **repartition 换 repartition+排序 或避免**,让 stage determinate 避免整链回滚(实战意义="checkpoint the RDD before repartition")
4. **远程/解耦 shuffle 服务(Celeborn)** 云原生大 shuffle 根本解

### 2.3 第三敌人:driver 元数据 OOM 与小文件
- **driver OOM**: 第0章 numPartitions×partitioner.numPartitions>(1<<30) 警告真会发生(百万map×大shuffle.partitions→MapStatus撑爆)。HighlyCompressedMapStatus(>2000自动)扛一部分,但要控 map 分区数+合理 reduce 分区
- **小文件**: 去重/分词后几百万小文件,下游 dataloader 灾难。AQE coalesce(第10章)或显式 repartition/coalesce 控文件大小(128MB–1GB/文件),或写 WebDataset tar shard/MosaicML MDS

---

## 三、大模型行业 Spark 最佳实践(约 2024–2025)

### 3.1 定位
Spark=预训练数据准备主力,尤其重 shuffle 部分(全局去重/质量过滤/聚合统计); 不碰训练。
诚实判断——出现"去 Spark 化"分流:
- **Ray Data** 新管线份额上升(Python/GPU/ML 生态贴合,流式自然,适合管线嵌 GPU 推理)。常见 Ray(GPU环节)+Spark(重shuffle)混合
- 部分公开数据集(HuggingFace FineWeb 用 Rust 工具 datatrove)绕开 Spark
- 但工业级 PB 去重/过滤,Spark(尤其 Databricks)仍事实标准(那个体量 all-to-all shuffle 没有比成熟 Spark 更稳的)

### 3.2 实战清单
**① 远程/解耦 shuffle 服务=当前最重要实践。** Apache Celeborn(原 Aliyun RSS)接近 K8s/spot 大 shuffle 标配: shuffle 写独立弹性服务(push-based,map 推给 worker 按分区聚合),解耦 shuffle 与 compute。直击 2.2: executor 抢占/缩容不丢 shuffle,FetchFailed 大减,随机写变顺序写。同类: Spark push-based shuffle/Magnet(SPARK-30602)、Uber RSS、Cosco。**LLM规模+spot 几乎必上。**
**② 原生执行引擎。** Databricks Photon、开源 Gluten+Velox/ClickHouse,Spark SQL 算子下沉原生,文本处理数倍加速。第8章 Tungsten 是 JVM 内,这是再下沉一层。
**③ shuffle/序列化调参:** AQE(3.x默认)coalesce/skewJoin; zstd/lz4 命中 mergeSpills 零拷贝; Kryo+offHeap(第7/8章); shuffle.partitions 设大交 AQE 合并; 避免 groupByKey。
**④ 去重工程化:** 精确=dropDuplicates 按 hash; 模糊=MinHashLSH(band/row 切分控倾斜)→连通分量聚簇(迭代图算法,血缘极长**必周期 checkpoint**); 加盐两阶段对抗热点桶。
**⑤ 参考管线:** AI2 Dolma、Together RedPajama、DataComp-LM、Meta Llama 流水线——多为"Spark 重 shuffle 去重过滤 + 专用工具抽取分词"。

### 3.3 一句话
> LLM 数据准备的 Spark 最佳实践=用它扛 PB 级全局去重与过滤; 上远程 shuffle 服务(Celeborn/push-based)解决 spot 抢占与 FetchFailed; AQE+加盐两阶段压倾斜; 长血缘迭代(LSH 连通分量)必 checkpoint; zstd/Kryo/堆外命中零拷贝与 Tungsten 快路径; 输出控文件大小给下游训练; GPU 推理交给 Ray,Spark 专注 shuffle/SQL。

> ★ LLM 数据准备恰好把全书坑全踩一遍: 倾斜(第3/4)、FetchFailed与INDETERMINATE回滚(第3)、driver元数据OOM(第0 1<<30)、groupByKey OOM(第2)、spill与零拷贝归并(第6/7)、checkpoint断血缘(第0)、动态分配shuffle文件丢失(第13)。**调优本质不是背配置,而是知道每个配置命中源码哪条分支、绕开哪个坑。**

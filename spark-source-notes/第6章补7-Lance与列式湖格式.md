# 第 6 章补7 — Lance 与列式湖格式:LLM 训练数据为何不只是 Parquet

> 据公开设计(Lance/Parquet/Delta/Iceberg 等均为独立项目,非 spark 仓库),扣回第9章谓词下推、第6章补小文件/元数据OOM、第0/15章确定性可复现

---

## 一、宏观:没有单一格式,因为生命周期有"访问模式冲突"的阶段
> LLM 数据准备(Spark/分析)与模型训练(PyTorch dataloader)访问模式几乎相反;Parquet 为前者设计。"为何不只用 Parquet"=它在准备阶段近乎完美,在喂训练阶段恰是短板。

| 阶段 | 谁做 | 访问模式 | 适配格式 |
|---|---|---|---|
| 采集/清洗/去重 | Spark | 全列扫描/过滤/join/shuffle | Parquet(+Delta/Iceberg 版本) |
| 特征/embedding/向量去重 | Ray/GPU | 随机访问+向量检索 | Lance |
| 喂训练(dataloader) | PyTorch/GPU | 随机访问单条+每epoch shuffle+流式+断点续训+多模态blob | Lance/WebDataset/MDS |

## 二、Parquet 擅长什么,对训练为何不够
本质(第9章): 列式、按 row group(128MB-1GB)组织、column chunk/page编码压缩、footer 存 schema+row group min/max。**正是第9章谓词下推+列裁剪所利用**(只读需要列、min/max跳过row group)。对扫描密集分析查询(准备阶段)完美→Parquet 是 Spark 准备阶段输出标准。
训练访问模式相反,Parquet 硬伤:
1. **随机访问差**: 训练每epoch shuffle,取第N条要解码整个row group(几百MB)→随机读=为一行解码一大块=灾难
2. **加列要重写**: 算出 embedding/质量分加进去,Parquet 重写整文件
3. **无向量索引**: embedding/语义去重/检索需向量近邻索引
4. **多模态blob不友好**
5. **不带版本/增量**: 训练语料演进,裸 Parquet 不管版本/时间旅行/增量
> ★ Parquet 不是不好,是为错误阶段优化。它为分析扫描(全列读+row group跳过)而生=第9章谓词下推用武之地=准备阶段需要。训练要随机访问单条+每epoch重排,row group"要读读一整块"主动碍事。格式优劣永远问"针对哪种访问模式"——Parquet 与训练的矛盾=批量顺序扫描 vs 随机点查的根本对立。同第6章"shuffle 把小随机读变大顺序读"同维度,只是发生在存储格式层。

## 三、两大家族
### A. 湖格式(Delta/Iceberg/Hudi)——Parquet 之上的事务/元数据层
数据文件仍是 Parquet,上面加事务日志/元数据层: ACID事务+快照隔离+时间旅行/schema演进/增量更新(MERGE-UPSERT)/文件管理(小文件compaction、元数据分层)。
LLM价值: 解决"curation 迭代、数据集演进"——版本化训练语料、时间旅行复现某次训练确切数据、增量加新源、追加计算列。Delta/Iceberg 是大规模准备侧湖格式标准。
> ★ 直接治第6章补两痛点: ①去重后几百万小文件→compaction合并 ②driver元数据OOM(第0章 1<<30 同类)→manifest分层管理。**时间旅行/版本化呼应第0/15章"确定性可复现"主线**: 第0章不可变+确定性让计算可重算,第15章WAL让流精确恢复,湖格式快照让你复现一次训练用的确切数据集版本。训练几百万美元一次,必须能精确重现喂的是哪版数据。
> ★ 但底下仍是 Parquet,训练 dataloader 随机访问短板没解决——优化的是分析/ETL侧(准备)非训练喂数侧。

### B. 为训练重新设计的格式
**1. Lance(LanceDB 底层)** 与 Parquet 关键区别:
- **快速随机访问**: 布局让近乎 O(1) 随机读第N行(每列可直接seek到某行不必解码整个row group)。训练shuffle/"给256条随机样本组batch"的命门,相对Parquet最大卖点
- **原生向量索引**: 内建 IVF-PQ 等向量近邻索引→embedding/语义去重/检索在格式层支持
- **零拷贝版本/加列**: 追加新列/出新版本不必重写(补上Parquet硬伤2,加embedding列不重写文本)
- **多模态**: blob+向量+标量混存
**2. WebDataset(tar分片)**: 每样本=tar里一组文件,顺序读,适合多模态流式喂GPU,简单兼容任意对象存储;无列式/分片内无随机访问/无谓词下推。适合最终训练喂数(顺序流+分片级shuffle)
**3. MosaicML MDS(StreamingDataset)**: 对象存储流式到GPU,确定性shuffle+断点续训(checkpoint dataloader位置,崩溃从epoch中间恢复)+弹性伸缩。为训练循环设计
**4. TFRecord**: 顺序protobuf记录,角色类似WebDataset

## 四、阶段—格式映射
```
原始WARC →Spark抽取/过滤/去重(全列扫描+shuffle,谓词下推第9章)→ Parquet(+Delta/Iceberg版本/时间旅行/增量/合并小文件) 准备侧标准
  →Ray/GPU embedding/打分/向量去重(随机访问+向量检索)→ Lance(随机访问+向量索引+零拷贝加列) 特征/向量侧
  →转训练格式→ WebDataset/MDS/Lance(顺序流式+每epoch shuffle+断点续训) 训练喂数侧 → PyTorch/GPU dataloader
```

## 五、为何接力而非统一
> ★ 最终答案: 一份训练数据生命周期经历访问模式冲突的几阶段,没有格式能同时把"批量顺序扫描"(准备)和"随机点查+流式shuffle"(训练)做到极致→不同格式不同阶段接力,存储为界交接。同补5末尾"Spark管重shuffle、Ray管GPU推理,存储为界接力"完全同构: 无论计算框架还是存储格式,LLM管线都是"专用工具按阶段分工+存储为接力棒"。原因一样——每个工具/格式核心取舍决定它只在某访问模式最优。
> ★ 扣回全书最核心一条线: **访问模式决定一切。** 第6章shuffle把小随机读变大顺序读;第8章Tungsten为列式SIMD重排内存;第9章谓词下推为扫描跳过用row group统计;Lance为随机点查+向量检索重新设计磁盘布局。**从内存布局(Tungsten)→shuffle组织(Sort/Celeborn)→磁盘格式(Parquet/Lance),Spark生态每层"快"的本质都是『让数据物理组织匹配访问模式』。** 看任何新格式/优化都问:针对哪种访问模式,为此付出什么代价。

## 六、边界
快速演进领域;Lance 较新采用在增长,准备侧仍 Parquet+Delta/Iceberg 主导;以上据公开设计非读源码。

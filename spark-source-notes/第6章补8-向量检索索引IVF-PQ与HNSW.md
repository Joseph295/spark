# 第 6 章补8 — 向量检索索引:IVF-PQ / HNSW 如何把 embedding 去重做到近线性

> 算法/外部知识(不在 spark 仓库),扣回补2 MinHash LSH、补3 连通分量、补7 Lance、补6 Spark×Ray

---

## 一、问题:语义去重 = 稠密向量上的 ANN
每文档→稠密向量(如768维),cosine 相似度>阈值即近似重复。精确 O(N²) 十亿级不可能→需 ANN(近似最近邻)降到近线性。
> ★ 同补2 MinHash LSH 是同类问题两变种: MinHash LSH=集合 Jaccard 的 ANN(token重叠); 向量 ANN(IVF-PQ/HNSW)=稠密向量 cosine/L2 的 ANN。目标同(避免O(N²)),度量不同。MinHash 抓词面近似(复制/样板/轻改写),embedding ANN 抓语义近似(改写/翻译/换词同义,MinHash 因token重叠少而漏)。现代管线两者并用: 先 MinHash 便宜词面去重,再对幸存者 embedding 语义去重。embedding 先用模型算(GPU=补6 Ray 的活)。

## 二、IVF(倒排文件索引)——分区式
- 粗量化: k-means 把所有向量聚成 K 个簇心,每向量分到最近簇心→每簇心一个倒排列表
- 查询: 找查询向量最近 nprobe 个簇心,只搜这几个列表→只搜 ~nprobe/K 比例数据→单次亚线性
> ★ 同补2 LSH banding 字面同构: 把空间分桶,只在同/邻桶比较。LSH 桶=哈希band; IVF 桶=Voronoi胞。nprobe 之于 IVF 如"查几个band"之于 LSH=召回vs速度旋钮。"用空间划分把二次全配对降近线性"是二者共同套路。

## 三、PQ(乘积量化)——压缩省内存+快算距离
- 问题: 十亿条768维float32=3KB/条×10⁹=3TB 放不进内存
- PQ: 每向量切 m 个子向量,每子空间训256项码本,每子向量编码成1字节→768维→m字节(m=96→96字节vs3KB ~32倍压缩)
- 算距离: 预计算查找表近似距离(ADC),查询到各子空间码本项距离之和,查表+加法又快又省

## 四、IVF-PQ = 结合(FAISS 主力,Lance 向量索引底层)
IVF 决定搜哪些桶(粗)+ PQ 提供压缩向量+桶内快速近似距离。FAISS IVF-PQ/IVFADC,几台机器扛十亿级。补7 Lance 内建向量索引底层即此类。
旋钮: nprobe(搜几桶)/m/nbits(压缩粒度)=召回vs速度/内存可调。

## 五、HNSW(分层可导航小世界图)——图式
- 多层邻近图: 每向量一节点连近邻,上层稀疏(长程边)下层稠密
- 查询: 顶层入口贪心向查询走(每步移到离查询更近的邻居),逐层下降,跳数~log级→极快高召回
- 取舍: 召回/延迟最好适合高QPS; 但内存大(存图+通常全量向量不压缩)、构建贵、大规模增删难
> ★ 三索引落在"召回/速度/内存"权衡曲面不同点,选哪个取决于访问模式(补7视角)。HNSW: 查询最快最准但吃内存(全量向量在RAM)→建一次查很多内存够; IVF-PQ: 压缩到十亿级进内存但召回略低→内存受限超大规模。同补7 Parquet vs Lance: IVF-PQ用精度换内存/速度(如Parquet用随机访问换扫描效率),HNSW用内存换查询速度(如数据留RAM/GPU)。每个索引=访问模式权衡曲面一个点。十亿级内存受限通常IVF-PQ;中等规模高召回用HNSW;常组合(HNSW当IVF粗量化器/HNSW-PQ)。

## 六、为何去重近线性
```
建索引: 全部N个embedding建IVF/HNSW ~O(N log N)(IVF: k-means+分配; HNSW: N次插入每次~logN)
去重查询: 每向量查k近邻(或阈值范围查询),每次亚线性(IVF搜nprobe列表; HNSW~logN跳)→总~O(N×亚线性)=近线性
  ↓ 产出候选近似重复对(相似度>阈值)
建图(边=相似)→连通分量(GraphX,补3!)→每分量=语义重复簇→每簇留一
```
> ★ 最后两步和补2/补3 MinHash LSH 去重下游一模一样!唯一区别=候选对怎么产生(向量ANN vs MinHash band碰撞)。"候选生成"可换,"图去重骨架(连通分量)"共享——都喂GraphX连通分量(补3),都因迭代血缘增长必checkpoint(补3 Pregel checkpoint舞蹈同样救命)。又是"换前端复用骨架"模式——同第9章三前端汇聚LogicalPlan、补6 injectColumnar换执行引擎、本章换候选生成。无论MinHash还是向量ANN,去重真正难点和解法(图+连通分量+checkpoint)都在同一处。

## 七、分布式与 Spark/Ray
- 十亿级索引分片到多机: ①每Spark/Ray分区建本地IVF/HNSW,查询按簇心路由到对应分片=按簇心shuffle(同补2 LSH band shuffle同构) ②用专用向量库(Lance/Milvus)从Spark/Ray查
- 倾斜又来: 稠密簇(热门主题)→某IVF桶/图区域巨大=补2 LSH热点桶同样问题同样缓解(拆大桶)
- embedding由GPU模型算(补6 Ray): 管线常是 Ray算embedding+建/查向量索引(GPU)→候选对→Spark/GraphX连通分量(重shuffle)→去重 = 补6"Ray管GPU、Spark管重shuffle、存储为界接力"形态

## 八、MinHash LSH vs embedding ANN
| | MinHash LSH(补2) | embedding ANN(本章) |
|---|---|---|
| 度量 | Jaccard(token重叠) | cosine(稠密向量) |
| 抓什么 | 词面近似(复制/样板/轻改写) | 语义近似(改写/翻译/换词同义) |
| 成本 | 便宜不需模型 | 贵需embedding模型(GPU) |
| 角色 | 第一道便宜词面去重 | 第二道对幸存者语义去重 |

---

## 收束:"去重"这条线完整了
两种候选生成(MinHash LSH 词面 / 向量 ANN 语义)+ 共享图去重骨架(连通分量+checkpoint)+ 分布式倾斜处理 + Spark/Ray 分工。全归同一底层套路: **用空间划分/locality 结构,把二次全配对降成近线性**。同第6章shuffle、第8章Tungsten、补7存储格式共享同一主线: **让数据的物理组织匹配它的访问模式**。

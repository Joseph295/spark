# 第 6 章补9 — MoE 训练数据的特殊处理

> 领域/外部知识(不在 spark 仓库),扣回补2-补8 管线机制;研究前沿处明确标注

---

## 一、破误区:MoE 数据处理 ≈ dense + 几处真正差异
**MoE 主要是『模型架构』改变(token 路由到专家),其预训练『数据处理』管线(爬取→抽取→过滤→去重→分词)和 dense 几乎一样。** 补2-补8 那套(MinHash/向量去重、Celeborn、倾斜、连通分量)对 MoE 原样适用。别臆想专属魔法。
真正差异来自 MoE 三特性:
| MoE 特性 | 对数据处理影响 |
|---|---|
| 参数高效(每token FLOPs≈小dense,总参数多很多) | 喂更多 token→管线规模压力大 |
| 专家可能专业化(routing) | 数据混合更关键且与路由交互 |
| 专家负载需均衡(load balancing) | 批次组成成为数据侧杠杆 |

## 二、差异1:token 预算暴涨→管线规模压力
MoE 参数高效→用更多 token 喂饱容量。后果: 产出更多且高质量 token→去重/过滤更大规模→前面难题(Celeborn抗spot/倾斜/连通分量必checkpoint)推得更狠; 更看重多样性(专家靠多样数据分化,不能靠重复凑token)→去重和源多样性比dense更重要。

## 三、差异2:数据混合与专家专业化
数据混合=各领域比例(网页/代码/数学/多语言/书籍)。所有 LLM 都重要,MoE 多一层"混合与路由/专家专业化交互"。
> ★ 诚实标注(研究前沿): "专家是否按领域专业化"有争议,别信"每专家对应一领域"。多研究观察专家分化更多按 token类型/句法而非干净主题对齐,路由非清晰领域映射。"为 MoE 按领域设计数据"不能建立在"专家=领域"未证实假设上。准确说法: 数据混合对 MoE 同样重要,且与路由交互我们理解仍不完整。
数据混合是实打实数据工程任务(MoE/dense 都需要): 按目标比例(网页60%/代码15%/数学10%...)加权采样交织; 比例可手工或方法学习(DoReMi 小代理模型学权重 / data mixing laws 小规模外推); 是 Spark/Ray 活: ①Spark groupBy(domain).sum(tokens) 算每领域token数 ②按比例加权采样/重复/交织产训练分片。

## 四、差异3:批次组成与专家负载均衡(最 MoE-specific 数据侧)
训练时 MoE 靠负载均衡辅助损失+专家容量防 token 挤向少数专家。数据侧隐患:
> 全局批次领域同质(整批代码)→路由器大量 token 送少数专家→专家溢出(超capacity)→token dropping/重路由→训练不稳、有效利用率降。
所以希望每全局批次领域均衡=数据布局/shuffle 问题:
- 全局 shuffle 质量: 数据按领域连续存→批次同质→必须打散让任意批次代表性混合
- 分片组成: 输出分片(WebDataset/MDS 补7)在分片内/间交织领域,再靠 dataloader shuffle buffer 细混
- 关键: shuffle 不充分(领域聚簇)对 MoE 伤害比 dense 大(dense 只是梯度偏,MoE 还触发 token dropping 和不稳)
> ★ MoE 把"数据物理布局"和"训练动力学"直接耦合。补7"快的本质=数据物理组织匹配访问模式",那里访问模式是IO;到MoE,访问模式多一维=路由器每批次专家负载。数据物理布局(分片组织/shuffle 充分度)不只为IO效率,更为服务训练时路由均衡。"数据组织匹配下游怎么用它"从存储IO延伸到训练动力学,又深一层。
(边界: 负载均衡主要是训练时loss/容量机制,非数据prep能完全解决;批次组成只是数据侧杠杆。)

## 五、差异4:配比可复现可重调→版本化语料
混合比例是关键超参且反复重调。需要: 每领域token计量+来源溯源; 数据集版本化(哪配比产哪次训练); 重新加权不必重curate。→补7 湖格式(Delta/Iceberg): 版本化可查询带provenance语料,时间旅行复现配比。呼应"确定性/可复现"主线(第0章血缘重算→第15章exactly-once→补7时间旅行→此处配比复现)。

## 六、落到数据管线
```
curated语料(Parquet/Lance,带domain/source标签)
  ①Spark groupBy(domain).sum(tokens) 每领域计量
  ②定混合权重(经验/DoReMi/mixing laws)
  ③Spark/Ray 按比例加权采样/重复/交织领域产训练分片
  ④分片组成领域交织+dataloader shuffle buffer→每全局批次领域均衡(防MoE路由失衡)
  → WebDataset/MDS/Lance分片→MoE训练; Delta/Iceberg记录配比版本时间旅行复现
```

## 七、大模型训练数据处理这条路的地图(待挖)
1. **去污染(decontamination)**: 移除评测基准数据(否则评分虚高),n-gram/子串 match against benchmark=dedup式shuffle/join(补2机器)——下一篇
2. 序列打包(sequence packing): 变长文档打包进定长context,防padding浪费+跨文档注意力污染
3. 大规模分词(tokenizer训练+tokenization)
4. 数据混合定律/DoReMi 细节
5. 质量分类器过滤(FineWeb-Edu式,Ray GPU补6)
6. 课程学习/数据排序
7. 多模态数据对齐

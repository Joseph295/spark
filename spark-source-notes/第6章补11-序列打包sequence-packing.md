# 第 6 章补11 — 序列打包(Sequence Packing):变长文档进定长 context

> 领域/外部知识(不在 spark 仓库),数据 prep 与模型注意力机制的接缝;扣回 bin packing、map-side vs shuffle、补7 格式服务消费者

---

## 一、问题:变长文档 vs 定长 context window
训练样本(文档)变长,模型在定长 context(4096/8192/32k token)上训。怎么塞?
朴素做法的坑:
1. **Pad 到最大长度**: 每文档→一序列,PAD 补到 context 长。平均文档 500 token、context 8192→浪费~94% 计算在 padding。规模下不可接受
2. **截断**: 砍到 context 长。丢数据(长文档截断),短文档仍浪费

## 二、解法:打包(packing)
**把多个文档背靠背拼进一个定长序列填满它。** 8192 窗口可装 ~16 个 500-token 文档,~零 padding 浪费。
但打包引入问题: **注意力的跨文档污染**。直接拼 docA+docB 进一序列,transformer 自注意力让 docB 的 token 能 attend docA(同序列)——错!B 不该"看见"A。两个具体问题:
- 注意力跨文档边界(B attend A)
- position id 跨边界连续(B 位置从 A 结尾接着,而非从 0)

## 三、解法的解法
1. **文档注意力掩码(block-diagonal/intra-document masking)**: 注意力掩码让每 token 只 attend 自己文档内 token。掩码是块对角(每文档一块)。需框架支持变长注意力(FlashAttention varlen / block-diagonal mask / cu_seqlens 累积序列长度)
2. **position id 重置**: 每文档边界把 position id 重置 0,各文档有自己从0开始的位置编码
3. **朴素拼接不masking(GPT-3 时代)**: 直接拼不 mask,接受跨界注意力。更简单曾常见;论点是影响小、模型靠 BOS/EOS 学软边界。但现代有 FlashAttention varlen 后正确 masking 很便宜→best practice 是正确 mask(研究如"Fewer Truncations Improve Language Modeling"显示正确 masking 有帮助,尤其长context/推理)

## 四、打包算法本身=bin packing
N 个变长文档,装进 size=context 长的 bin,最小化浪费(padding)、最好不切分文档=经典 **bin packing(NP-hard)**→启发式:
- **贪心/first-fit/best-fit decreasing**: 文档按长度降序,每个放进第一个/最合适的能装下的 bin
- **concatenate-and-chunk(最简单,经典)**: 全部文档拼成一个大 token 流,按 context 长切片→零 padding 但**文档被切分**(一序列可能含一文档尾+另一文档头)
- **no-split 打包**: bin packing 保持文档完整,接受少量 padding
- **现代 best-fit packing**("Fewer Truncations Improve Language Modeling"): best-fit-decreasing 既保文档完整又最小化 padding
切分 vs 不切分权衡: 切分零浪费但破坏文档连贯(伤推理/长文档);不切分保连贯但有 padding。

## 五、扣回数据管线(Spark/Ray)
- 打包是预处理步(离线产打包训练分片)或 dataloader 在线做
- 离线大规模打包: 数据作业(Spark/Ray)取分词后语料,跑打包启发式,写定长序列+**文档边界元数据(cu_seqlens/mask 信息)**到训练分片(WebDataset/MDS/Lance)
- 格式必须携带每序列: token ids + 文档边界(训练时注意力据此建块对角 mask)→**格式服务消费者(补7 主线): 打包格式不只存 token,还存注意力需要的结构**
- **PB 级打包不需全局最优**: 每分片/每分区贪心打包即可(各 Spark 分区独立打包其文档)=**embarrassingly parallel,无需 shuffle**(对比 dedup 的 shuffle 重)
> ★ 打包是 per-partition、无 shuffle 的 map 操作——对比 dedup(shuffle 重)。管线两类操作: shuffle 重的全局操作(dedup/混合) vs 易并行的本地操作(过滤/分词/打包)。知道哪类=知道成本。

## 六、子细节
- 长 context 训练(128k+): 打包更重要(单个 128k 文档罕见要打包很多;但也想保留些长文档完整训长context能力)。课程: 短context早、长context晚
- 文档间插 EOS/BOS 分隔符
- "文档跨边界切分"伤长文档连贯→best-fit no-split 打包缓解

> ★ 主线呼应: ①打包是 per-partition 无 shuffle map 操作(成本认知) ②跨文档注意力污染+masking 是又一处"数据布局影响正确性"(同第3章 INDETERMINATE 数据布局影响重算正确性): 数据物理组织(怎么打包)与模型正确性(注意力边界)耦合——访问模式主线在 token 布局层再现 ③cu_seqlens 边界元数据随数据存=格式服务消费者(补7): 打包格式携带 token+注意力需要的结构。

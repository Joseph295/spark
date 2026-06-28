# 第 6 章补12 — 大规模分词:tokenizer 训练 + 全量 tokenization

> 领域/外部知识(不在 spark 仓库),扣回第2章 RangePartitioner 采样、补10 broadcast、补11 map vs shuffle、补9 数据混合

---

## 一、两个截然不同的阶段
分词=raw text→token ID(模型吃的整数)。子词分词: BPE / Unigram(SentencePiece) / WordPiece。两阶段:
1. **tokenizer 训练(一次)**: 从语料样本学词表(BPE 的 merge 规则 / unigram LM)。输出 32k-256k token 的词表
2. **tokenization(应用)**: 用训好的 tokenizer 把**整个 PB 语料**转成 token ID。这是大数据作业

## 二、tokenizer 训练:为何不是全量作业
BPE 训练: 从字节/字符起,迭代合并最频繁的相邻对,重复到词表大小。需统计对频。
**关键: 不在全 PB 语料上训,只在『样本』上(几 GB 到几十 GB 足够,merge 统计快速收敛)。** BPE 训练偏顺序(每次 merge 依赖上次新计数),实现(HF tokenizers / SentencePiece)是单机多线程内存内。所以采样语料、本地训。
采样考量:
- **必须代表领域/语言混合**: 欠采样代码或某语言→tokenizer 切碎它(每字符更多 token→浪费 context、建模更差)。扣回补9 数据混合: tokenizer 训练样本应反映目标混合,否则某些领域/语言过度/不足切分
- 是 Spark/数据作业: 跨领域分层采样产训练样本
词表大小权衡: 大词表=每文本更少 token(序列更短更高效)但 embedding/输出矩阵更大(更多参数、softmax 更重计算)。多语言/代码需更大词表。
> ★ 采样学习而非扫描学习: tokenizer 训练采几 GB 学词表,恰如第2章 RangePartitioner 采样估分布再分区。"不需全部数据来学它的一个小摘要"是反复出现的效率原则(也见 DoReMi 代理模型、优化器统计)。

## 三、字节级 BPE
现代 tokenizer(GPT-2 起)用**字节级 BPE**: 操作 UTF-8 字节而非 Unicode 字符。词表覆盖全部 256 字节→无 OOV(任何文本/语言/emoji 都可编码)。对 web 级脏数据的鲁棒性重要。

## 四、全量 tokenization:大数据作业
tokenizer 训好(小产物,几 MB)→应用到整个语料。
**embarrassingly parallel、map-side、无 shuffle**: broadcast tokenizer(小)到所有 executor(补10/第9/12章 broadcast),每分区独立分词其文档。同补10 去污染的 n-gram broadcast。
- 输出: token ID 序列,紧凑存(uint16/uint32 数组,或打包——扣回补11,常 tokenize+pack 一遍过)
- 成本: tokenization 是 **CPU 密集**(BPE apply 是非平凡的逐文档算法,找 merge 序列)。PB 级是巨大 CPU 作业→native/Rust tokenizer(HF tokenizers 是 Rust 后端)、GPU 分词(RAPIDS 有 GPU subword tokenization)对吞吐重要(补5/补6)
- 确定性: 分词必须确定(同文本→同 token)以可复现——扣回确定性主线
> ★ broadcast 小的、map 大的: 训好的 tokenizer 极小→broadcast→map-side apply 无 shuffle。"size 不对称→broadcast 非 shuffle"第三次(第9章 BroadcastHashJoin、补10 去污染、此处)。

## 五、实战问题
- **特殊 token**: BOS/EOS/PAD,以及 chat/结构 token(<|user|> <|assistant|> 工具调用 token、代码 FIM fill-in-the-middle token)。加入词表,分词/打包时插入
- **tokenizer-语料失配**: 训 tokenizer 的混合 vs 实际训练数据混合不同→切分次优。混合大变则重训 tokenizer
- **压缩率(bytes/token 或 chars/token)**: tokenizer 效率关键指标,按领域测(Spark 聚合)
- **tokenize once 存 token ID vs dataloader 在线分词**: PB 级离线分词一次(存 token ID)避免每 epoch 重分词。token ID 比文本小得多(压缩),存 MDS/Lance/numpy 分片
- 数字/空白处理: 有的 tokenizer 单独切每位数字(助数学)。tokenizer 设计选择影响下游

## 六、管线分类(扣回补11)
- tokenizer 训练: 分层采样(Spark 采样,小输出)+本地训(单机非分布式)。计数本可是 shuffle(全局对频)但因采样成小本地作业
- 全量 tokenization: 纯 map(broadcast tokenizer + 逐分区 apply,无 shuffle)
- 故整个 tokenization 阶段 shuffle-light——对比 dedup 的 shuffle 重
- 成本: CPU 密集 apply→native/Rust/GPU 加速

> ★ 四处主线呼应: ①采样学习非扫描学习(同第2章 RangePartitioner) ②broadcast 小 map 大(size 不对称→无 shuffle,第三次) ③tokenizer-混合对齐(补9): 训练样本须匹配数据混合否则碎片化浪费 context——跨阶段数据决策耦合(混合↔tokenizer↔打包效率) ④确定性可复现。

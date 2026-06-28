# 第 6 章补10 — 去污染(Decontamination):从训练集清掉评测数据

> 领域/外部知识(不在 spark 仓库),扣回补2/补8 去重、第9/10章 join 策略、补7 版本化

---

## 一、问题:为何必须做,为何难
爬来的训练数据几乎必然含评测基准(MMLU/GSM8K/HumanEval 在论文/GitHub/论坛到处都是)。训到评测数据→评分虚高、不可信(模型背题)。去污染=测量并移除"训练数据∩评测基准"。
难点: ①诚信问题(污染让模型显得超出真实水平) ②多形态(原文/改写/翻译/重排,精确匹配漏改写,模糊匹配误删) ③规模不对称(PB训练数据 vs 几千条小benchmark="巨vs小")
> ★ 去污染本质="对外部小集合做去重"——同补2/补8 自去重是同一"找重叠"问题。决定性区别: 自去重=巨vs巨(全库两两),去污染=巨vs小(全库vs几千条)。size 不对称把执行从 shuffle 翻转成 broadcast=第9章 planner 选 BroadcastHashJoin/SortMergeJoin、第10章 AQE 动态切换那个决策!同一"找重叠"逻辑问题,不同数据规模选完全不同物理计划。第三次撞见此决策(第9章planner/第10章AQE/此处)。普适: **数据规模决定执行策略。**

## 二、两种技术
### 2.1 精确匹配:n-gram/子串重叠(主流)
benchmark 切 n-gram(8-13gram 或50字符子串)成集合;每训练文档查是否含任何 benchmark n-gram,重叠超阈值→污染→移除文档(或切污染片段)。GPT-3/Llama 都用。
工程(扣回 Spark):
- benchmark n-gram 集**很小**→broadcast 到所有 executor(补6/第12章 TorrentBroadcast),扫训练文档 map-side 成员检查=broadcast join(第9章 BroadcastHashJoin),**不需 shuffle**
- 对比补2 自去重巨型 all-to-all shuffle: 去污染因一侧极小退化成廉价 map-side 过滤
- 省内存+O(1)查询: benchmark n-gram 存 Bloom filter(允许假阳性,宁多误删别漏污染),broadcast 出去
### 2.2 模糊/语义去污染
MinHash LSH(补2)或 embedding 相似度(补8)比训练文档 vs benchmark→抓改写/重排/翻译污染。机器同自去重,对象换成"训练集 vs benchmark"。因 benchmark 小,可对小集合建索引(ANN 补8)让每训练文档查它。

## 三、两方向
- 清训练集(移除污染训练文档)——多数 lab
- 清评测集(把出现在训练里的 benchmark 例子从评测排除)——部分工作
- 常按每 benchmark 分别去污染并报告污染率

## 四、诚实边界
- 阈值权衡: n-gram太长/重叠要求太高→漏污染;太短/太松→误删合法数据。长度+重叠比例都要调
- 永远清不干净: 改写/全新重排抓不住,严肃 lab 报告污染率而非声称零污染
- n-gram 去污染被质疑不够: 抓原文泄漏抓不住语义泄漏(换说法)→模糊/语义那道越重要
- 合成数据也要去污染(LLM 可能背出 benchmark)
> ★ 拧合两主线: ①数据规模决定执行策略(巨vs小→broadcast非shuffle,第9/10章join决策第三次现身) ②确定性/可复现/可信——去污染为让评测可信,是数据侧给"评估可信度"的保证。要能复现"哪版语料用了哪套去污染规则"(provenance补7)。"可复现"线(第0章血缘→第15章exactly-once→补7时间旅行)延伸成"可信评估": 不仅重现训练用了什么数据,还要证明没被评测污染。

## 五、与自去重对照
| | 自去重(补2/补8) | 去污染(本篇) |
|---|---|---|
| 比什么 | 训练集vs自己 | 训练集vs benchmark集 |
| 规模 | 巨vs巨 | 巨vs小 |
| 执行 | all-to-all shuffle(贵) | broadcast map-side过滤(廉价无shuffle) |
| 精确 | hash/dropDuplicates | n-gram/子串+Bloom filter |
| 模糊 | MinHash LSH/向量ANN | 同机器,对benchmark小集合建索引查询 |
| 目的 | 减冗余提质量 | 可信评估(防作弊) |

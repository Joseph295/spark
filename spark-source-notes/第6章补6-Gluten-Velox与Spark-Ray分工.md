# 第 6 章补6 — Gluten+Velox 原生引擎 与 Spark×Ray 分工

> 对照源码:`sql/core/.../SparkSessionExtensions.scala`(injectColumnar 168)、`sql/core/.../execution/Columnar.scala`(ColumnarRule 48-50, 应用 549-551)(apache/spark master);Gluten/Velox/Ray 据公开设计(独立项目,非本仓库)

---

# 一、Gluten + Velox:把执行引擎换成原生 C++

> Gluten(Apache,源于 Intel)+ Velox(Meta)独立项目,不在 spark 仓库。挂载点=下面确认的 injectColumnar/ColumnarRule。

## 1.1 它是什么:CPU 版的"算子换引擎"
spark-rapids 把算子换 GPU 版(补5);**Gluten 把算子换成原生 C++ 向量化引擎(Velox 或 ClickHouse)版本——同思路,落到 CPU 原生代码而非 GPU。** Velox 是 Meta 开源、身经百战的 C++ 向量化执行引擎(Presto/Spark 都用)。

## 1.2 怎么挂进来:第9章那个插件点(`Columnar.scala`)
```scala
// SparkSessionExtensions.scala 168:
168  def injectColumnar(builder: ColumnarRuleBuilder)        // 公开扩展 API
// Columnar.scala 48-50:
48   class ColumnarRule {
49     def preColumnarTransitions: Rule[SparkPlan] = ...      // 插入行↔列转换【前】改写物理计划
50     def postColumnarTransitions: Rule[SparkPlan] = ...
// Columnar.scala 549-551:
549  columnarRules.foreach(r => preInsertPlan = r.preColumnarTransitions(preInsertPlan))
```
Gluten 注册 ColumnarRule,在 preColumnarTransitions 遍历物理计划把支持算子换"原生算子":
- 算子子树翻译成 Substrait(跨引擎计划 IR)
- JNI 把 Substrait 交给 Velox
- Velox 原生 C++ + SIMD 向量化执行(列式 Arrow),结果以 Arrow 列式批次返回
**Spark 的 parse/analyze/optimize/plan(第9章)不变,Gluten 只在物理执行层介入。**

## 1.3 为什么快:把 Tungsten 目标推到原生
| | Tungsten(第8章) | Gluten+Velox |
|---|---|---|
| 执行 | JVM 字节码(codegen) | 原生 C++ 向量化 kernel |
| 数据 | 行式 UnsafeRow | 列式 Arrow |
| GC | 堆外缓解但仍在 JVM | 完全无 JVM/无 GC |
| 向量化 | 有限(行式) | 彻底 SIMD(列式) |
> ★ Tungsten/Gluten+Velox/spark-rapids/Photon=同一架构动作四种落地: 保留 Spark planner(第9章),经 injectColumnar 替换底层物理执行引擎。整个前端(SQL parse/analyze/Catalyst/AQE)原样复用,只换最底层执行(原生/GPU)。第9章"LogicalPlan/SparkPlan 窄腰"+列式插件 API 让执行引擎本身可插拔。Spark 越来越是"最好的查询规划器+可插拔执行后端"。同第14章Connect(换前端)/第15章Streaming——LogicalPlan 抽象放得太好,既能换前端又能换后端,中间不动。
> ★ Substrait 作跨引擎计划 IR 本身又是窄腰: 同一 Velox 引擎服务 Spark/Presto 等多上层。同第14章Connect protobuf逻辑计划、第9章LogicalPlan统一三前端——"找通用中间表示当边界"在跨引擎层再现。

## 1.4 Fallback 与边界(同 spark-rapids)
- Velox 不支持的算子/表达式 fallback 回 vanilla Spark(JVM行式),插列↔行转换。**第8/10章纪律: 优化必有正确退路。**
- UDF 是主要 fallback 触发: 自定义 UDF/复杂正则 Velox 跑不了→回 JVM。LLM 文本清洗大量定制 UDF,落差真实。
- Gluten vs spark-rapids: 都经 injectColumnar; Gluten→原生CPU(Velox/ClickHouse,不需贵GPU跑现有CPU集群), spark-rapids→GPU(cuDF)。Gluten vs Photon: Photon 是 Databricks 闭源原生向量化引擎,Gluten 是开源对应物。
- LLM: filter/join/agg on Parquet 列式重活 Gluten 在便宜 CPU 集群拿原生向量化加速;定制 UDF/正则仍回 JVM。

---

# 二、Spark × Ray:LLM 管线分工与协同

> Ray(Anyscale)独立框架,不在 spark 仓库。据公开实践讲,扣回 Spark 架构。

## 2.1 分工:互补非替代
| | Spark 主场 | Ray 主场 |
|---|---|---|
| 强在 | 大规模分布式 SQL/shuffle: 全局去重(LSH+连通分量)/质量过滤/join/聚合/排序 PB级 | 分布式 Python+GPU 编排: 管线里跑模型推理(质量分类器/毒性/embedding/LLM打分) |
| 引擎 | 成熟 shuffle+容错(第3章)+本书整套 | 细粒度 actor/task、CPU+GPU 异构、流式重叠 CPU 预处理与 GPU 推理 |
| 生态 | JVM 优先(PySpark 经 Py4J/Arrow 桥) | Python 原生,紧贴 PyTorch/vLLM/HF |

## 2.2 为何必须混合:越来越"用模型筛数据"
LLM curation 大量转向用模型过滤: 质量打分(FineWeb-Edu"是否有教育价值"分类器)/困惑度过滤/embedding去重/合成数据。"PB级文本上跑 GPU 模型"是 Ray 甜区(Ray Data+vLLM批量推理),非 Spark(RAPIDS 是给 SQL 算子的;PySpark UDF 塞 vLLM 要每task重载模型、无法跨分区批处理、序列化问题)。

## 2.3 常见架构
1. **存储为界接力(最常见)**: Spark 跑重 shuffle(去重/DataFrame变换)→写 Parquet/Lance→Ray Data 跑 GPU 推理(分类打分/embedding/GPU分词)→写回。阶段间共享存储(S3/HDFS Parquet)交接。
2. **Ray 总编排+Spark-on-Ray(RayDP)**: 一个 Ray 集群里 RayDP 把 Spark 当 Ray actor 跑 SQL,Ray Data 跑 ML,统一调度。
3. **全 Ray**: shuffle 不重、GPU 推理为主的管线直接跳过 Spark。

## 2.4 为何 Spark 吞不下 Ray 的活(架构根因)
> ★ Spark/Ray 分工恰落在本书讲过的架构承诺上,非偶然。Spark=shuffle/SQL/容错问题大师(全局去重需要);Ray=异构GPU编排大师(模型筛数据需要)。**Spark 难吸收 Ray 那部分活,根子在执行模型**: Spark 是 stage屏障+批量同步(BSP-like)(第3章整个调度设计),为块同步 shuffle 而生;不擅长"70B模型一次装进GPU显存、批批数据流过"这种长驻有状态细粒度 GPU actor(Ray actor territory)。**让 Spark 在 shuffle 上无比强大的设计(不可变RDD/stage屏障/确定性重算 第0/3章),恰让它不适合长驻有状态 GPU 推理。** 反过来 Ray shuffle 在 PB级 all-to-all+极端容错不如 Spark(+Celeborn)。两者共存=各自核心架构取舍的自然结果——管线里既有重shuffle关系清洗又有GPU模型推理,没有哪个框架两者都做到极致。

## 2.5 诚实趋势(约2024-2025)
- 新管线势头向 Ray Data 倾斜(Anyscale 推,GPU/Python 集成更好、流式让 GPU 不闲)
- 但 PB级 shuffle 重的 curation(尤其去重)Spark 仍主力/既有底座,尤其 Databricks/Spark 组织
- 混合是务实常态; 也有公开管线(FineWeb datatrove)两者都不用走自研 Rust——无单一赢家
一句话: Spark 管"PB级重shuffle关系清洗+去重",Ray 管"GPU模型推理筛数据",存储为界接力=当前最务实 LLM 数据管线架构。

# 第 8 章 — Tungsten:向 JVM 对象模型宣战

> 对照源码:`sql/catalyst/.../expressions/UnsafeRow.java`、`sql/core/.../execution/WholeStageCodegenExec.scala`(apache/spark master)

---

## 一、宏观:为什么 JVM 是大数据的敌人

RDD 是 `RDD[T]`,装任意 JVM 对象——这是性能天花板。Tungsten 一句话:**引擎知道 schema(每列类型),就能抛弃 JVM 对象模型,像写 C 一样操作内存、像编译器一样生成代码。**

**JVM 三宗罪:**
- **① 内存膨胀**:`"abcd"` 真实 4 字节,JVM 占 ~48 字节(对象头+数组头+UTF16+padding)。装箱 Integer 4 字节用 16 字节。90% 喂给对象脚手架。
- **② GC 灾难**:几十亿记录=几十亿小对象,GC 追踪标记移动每一个。分代 GC 为短命小对象设计,非为"100GB 驻留内存反复扫描"。
- **③ 火山模型虚函数**:每算子是 iterator,每行调 next()(虚函数,无法内联,破坏流水线),行还是通用 Row(装箱)。

**Tungsten 两板斧(钨=最硬金属,寓意贴近裸机):**
- 像 C 管内存:数据存紧凑二进制,堆外/字节数组,绕开对象模型 → **UnsafeRow**。
- 编译查询非解释:一条算子链坍缩成一个 Java 循环,零虚函数 → **whole-stage codegen**。

> ★ "DataFrame/SQL 为何快于 RDD"的根本答案(非魔法):RDD 装不透明对象,引擎只能用对象+虚函数;SQL 知道每列类型→才能定长二进制布局(UnsafeRow)+ 为具体查询生成专门代码(codegen)。Tungsten 一切建立在"引擎知道类型"上。是第6章 Tungsten shuffle"排指针不排对象"在整个引擎的展开。

---

## 二、细节

### 2.1 UnsafeRow:一行就是一段裸字节(`UnsafeRow.java` 61–118)
一个 UnsafeRow 不是字段对象集合,而是**一段连续字节**,经 `baseObject + baseOffset`(104-105)定位(可指堆内 byte[] 或堆外裸地址)。
```
布局: [ null位图 ][ 定长值区(每字段8字节) ][ 变长数据区 ]
```
```java
69   calculateBitSetWidthInBytes = ((numFields+63)/64)*8   // 每字段1bit标记null,64位对齐
116  getFieldOffset(ordinal) = baseOffset + bitSetWidthInBytes + ordinal*8L  // O(1) 纯指针算术!
219  setLong(i, v): Platform.putLong(baseObject, getFieldOffset(i), v)       // Unsafe 直写内存,如C指针赋值
```
- 定长类型(long/int/double 76-86)直存 8 字节;变长(string/array)那 8 字节存 `(偏移<<32 | 长度)` 指向变长区。
- **四收益(精准打击三宗罪):** ①紧凑无对象头(治膨胀) ②byte[]/堆外字节,GC 不追踪内部(治GC) ③字段连续缓存友好 ④自包含连续字节→可不反序列化直接按字节比较/哈希/搬移(**relocation**)。
> ★ ④ relocation 正是第6章 Tungsten shuffle"排指针不排对象"的根本原因,两章咬合。
> ★ baseObject+baseOffset 双参寻址:baseObject=byte[]→堆内; =null→baseOffset 是堆外裸地址。**同一套代码不改一行,堆内堆外通吃**。Spark 把数据搬堆外(offHeap.enabled)逃离 GC,只因行读写代码对"堆内/堆外"无感。用寻址抽象把"逃离GC"变配置开关。

### 2.2 Whole-Stage Codegen:一条算子链编译成一个循环(`WholeStageCodegenExec.scala`)
**问题:** Scan→Filter→Project 火山模型下每行三次虚函数+Row 装箱,CPU 耗在调用而非计算。
**想法:** 同 stage 算子链生成一个 Java 方法,一个循环全做完,filter 判断/project 表达式全**内联**进 scan 循环体,零虚函数。
**produce/consume 模型(48-196):** 每算子实现 CodegenSupport:
```scala
94   produce(ctx, parent)        // "请产出数据"生成驱动循环,自顶向下
121  doProduce(ctx)              // 子类:生成 while 循环骨架
153  consume(ctx, vars, row)     // "这是一行,处理它"把行推给父算子
345  doConsume(ctx, input, row)  // 子类:生成对每行的处理逻辑
```
**方向反转(逻辑拉模型→生成推模型):** produce 自顶向下(父调子的 produce 到 scan 生成 while 循环);doConsume 自底向上(scan 每读一行调父 doConsume 推上去,filter 生成 if(谓词){...} 再推父)。**所有算子逻辑像洋葱内联嵌套进 scan 一个循环体**。
```scala
669  ctx.addNewFunction("processNext", "protected void processNext(){ ${融合整条链的循环} }")
688  final class GeneratedIterator extends BufferedRowIterator { ... }   // doCodeGen[663]
728  doExecute: CodeGenerator.compile(Janino 编译) → 
770  rdds.head.mapPartitionsWithIndex { evaluator.eval(index, iter) }    // 每分区跑生成迭代器
```
codegen 后的 stage 执行时=对每分区跑那个生成迭代器。经 mapPartitionsWithIndex 接回 RDD——**SQL 物理算子最终落成 RDD 上的 mapPartitions**(第9章接缝)。
> ★ codegen 胜火山模型靠**消灭抽象开销**(非更好算法):同样 filter+project,火山每行 N 次虚函数+装箱;codegen 变成内联操作基本类型局部变量的紧凑循环=手写C的样子,跑满流水线/缓存,JIT 再优化。编译vs解释在查询引擎的胜利,同 LLVM/JIT 哲学。

### 2.3 64KB 坑与优雅降级(728–752)
硬约束:JVM 规定单方法字节码 ≤64KB;且 JIT 拒绝编译超 hugeMethodLimit(默认8000字节)的方法→停留解释执行反比不 codegen 还慢。
```scala
731  try CodeGenerator.compile(cleanedSource)
734  catch NonFatal if codegenFallback => return child.execute()   // ① 编译失败→退火山模型
743  if (maxMethodCodeSize > hugeMethodLimit) {
751    return child.execute()                                       // ② 方法太大→退火山模型
```
> ★ 深刻原则:**优化必须永远保留正确退路,绝不因优化失败破坏正确性。** codegen 是优化,火山模型是永远的 fallback。著名"几百列查询生成64KB+方法崩溃"bug 的修法=检测+降级,外加 185-196 splitConsumeFuncByOperator(拆 consume 为多小函数避免单方法超限)。激进优化,永留保命后路。

---

## 三、Trick 与 Bug
**① schema 是前提。** RDD 不透明对象只能用对象模型;SQL 知类型→定长布局+专门代码。DataFrame 快于 RDD 的根本,非魔法。
**② UnsafeRow 定长8字节槽+指针算术(116-117)。** 字段访问 O(1),无解引用。
**③ UnsafeRow 四收益。** 紧凑/无GC追踪/缓存友好/可按字节搬移(relocation→第6章根基)。
**④ baseObject+baseOffset 双参寻址。** 同代码堆内堆外通吃,逃离GC变配置开关。
**⑤ produce/consume 方向反转(94-196)。** 拉计划→推代码,整条链坍缩进一个循环零虚函数。
**⑥ codegen 落回 RDD(770)。** mapPartitionsWithIndex 每分区跑生成迭代器,SQL算子最终是RDD mapPartitions(第9章)。
**⑦ 64KB/hugeMethodLimit 降级(728-752)。** 编译失败或方法过大 fallback 火山模型。优化永留正确退路。

---

## 四、本章总纲
```
敌人=JVM对象模型: ①内存膨胀(对象头) ②GC灾难(十亿小对象) ③火山模型虚函数
前提: SQL知schema(每列类型)→才能抛弃对象模型(RDD做不到→DataFrame快于RDD根本)

板斧一 UnsafeRow[61-118]: 一行=连续字节(baseObject+baseOffset)
  布局[null位图][定长值区每字段8字节][变长区]; getFieldOffset=起点+位图宽+i×8(O(1)指针算术); setLong=Unsafe直写
  四收益: 紧凑/无GC追踪/缓存友好/按字节搬移(relocation→第6章); 双参寻址堆内堆外通吃→逃离GC变开关

板斧二 Whole-Stage Codegen: 算子链→一个循环零虚函数
  produce[94]自顶向下生成驱动循环; doConsume[345]自底向上内联推行给父(拉计划→推代码)
  doCodeGen[663]→BufferedRowIterator.processNext()融合循环; doExecute[728]Janino编译→mapPartitionsWithIndex(落回RDD,第9章)
  胜火山靠消灭抽象开销(如手写C),非更好算法

64KB降级[728-752]: 单方法≤64KB(JVM); 超hugeMethodLimit(8000)JIT不优化
  → 编译失败/方法过大 → fallback child.execute()火山模型。优化永留正确退路。
```

# 第 11 章 — RPC:让"调远程"像"调本地对象"

> 对照源码:`core/.../rpc/RpcEndpoint.scala`、`RpcEndpointRef.scala`、`rpc/netty/NettyRpcEnv.scala`、`Dispatcher.scala`、`Inbox.scala`(apache/spark master)

---

## 一、宏观

前面所有跨进程对话(LaunchTask 第4章、statusUpdate 第5章、心跳第1章、MapOutputTracker 第6章、BlockManagerMaster 第1章)全跑在同一套 RPC 上——第1章 SparkEnv.create 最先装配的 RpcEnv(350行)。

**设计目标:让"给远程进程发消息"看起来像"调本地对象方法"(位置透明)。**

一对核心抽象:
- **RpcEndpoint(端点)**:服务端,能接收处理消息的对象(HeartbeatReceiver/MapOutputTrackerMasterEndpoint)。就是一个 **actor**——有邮箱,一次处理一条。
- **RpcEndpointRef(端点引用)**:客户端遥控器,指向某 endpoint,**可能本地可能远端**。拿 Ref 调 send/ask,不需知道对方在不在本机=位置透明。

> ★ 经典 actor 模型(Akka/Erlang)。Spark 早期真用 Akka,SPARK-5293 换成自研 Netty 实现。**换心脏不惊动任何调用方,正因 RpcEndpoint/Ref 抽象设计得好**——接口(send/ask/receive/receiveAndReply)不变,只换底层传输。面向接口编程的价值在 Akka→Netty 无痛替换中体现。

两种范式:**send**(单向 fire-and-forget)和 **ask**(请求-响应返回 Future)。

---

## 二、细节

### 2.1 服务端 RpcEndpoint(`RpcEndpoint.scala` 46–148)
```scala
69   receive: PartialFunction[Any,Unit]                       // 处理 send 单向消息
77   receiveAndReply(context: RpcCallContext)                 // 处理 ask,用 context.reply 回复
114  onStart() / 122 onStop() / 92 onConnected/onDisconnected/onNetworkError  // 生命周期(executor丢失靠这感知)
137  trait ThreadSafeRpcEndpoint: 消息串行处理(一条 happens-before 下一条)→ 内部字段无需锁/volatile
```
你见过的端点都是此模板:HeartbeatReceiver 在 receiveAndReply 处理心跳回 ack;CoarseGrainedSchedulerBackend driver 端点在 receive 处理 executor 注册/statusUpdate。
> ★ ThreadSafeRpcEndpoint 与第3章 DAGScheduler 单线程事件循环**同一模式**:把并发收敛成串行消息队列消灭锁。每个 ThreadSafeRpcEndpoint=串行消息处理器,内部状态天然线程安全。纠结"共享状态要不要加锁"时,Spark 的答案:别加锁,变成单消费者消息队列。

### 2.2 客户端 RpcEndpointRef(`RpcEndpointRef.scala` 30–99)
```scala
45  send(message)                    // 单向,发完即走
64  ask[T](message): Future[T]       // 请求-响应
99  askSync[T](message, timeout): T  // ask + 阻塞等结果
```
第1章 `_heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)` 就是它——像调本地方法一样发消息拿返回值,感知不到对方在不在另一台机器。

### 2.3 本地 vs 远程:省序列化的捷径(`NettyRpcEnv.scala` 191–256)
```scala
191  send(message):
193    if (remoteAddr == address)                              // 接收方在【本进程】
196      dispatcher.postOneWayMessage(message)                 // 直接投本地邮箱:不序列化、不走网络
200    else postToOutbox(receiver, OneWayOutboxMessage(message.serialize(this)))  // 序列化+Netty
210  ask 同样本地/远程分叉; 258  超时调度器: 到时未响应让 Future 失败
```
> ★ 本地捷径两面: 好—driver 内部调用/local 模式几乎零开销(第1章 local 高效模拟分布式原因之一); 坑—又一个"local能跑集群挂"来源(本地跳过序列化,本该序列化的消息蒙混过关,上集群走远程才暴露 NotSerializable)。位置透明是抽象承诺,序列化这道坎只在跨进程设卡=分布式抽象必然的裂缝。
> ★ ask 必有超时: 远端可能挂/网络断,无超时则 askSync 永久阻塞。远程调用与本地调用最本质区别=远程必然可能"无应答"。

### 2.4 邮箱模型 Dispatcher + Inbox(`Dispatcher.scala`, `Inbox.scala`)
Dispatcher 按端点名把消息(本地或网络来的)路由到对应端点的 Inbox(消息队列,LinkedList)。
```scala
// Inbox.process 86-128:
89   if (!enableConcurrent && numActiveThreads != 0) return   // 已有线程处理→退出(保证串行)
92   message = messages.poll(); numActiveThreads += 1
102  case RpcMessage => endpoint.receiveAndReply(context)      // ask
115  case OneWayMessage => endpoint.receive                    // send
120  case OnStart => endpoint.onStart()
122    if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) enableConcurrent = true  // 非线程安全才允许并发
```
ThreadSafeRpcEndpoint 的 enableConcurrent 恒 false → 任何时刻一个线程处理其邮箱 → 串行无锁。
MessageLoop: DedicatedMessageLoop(高流量端点独占线程,Dispatcher 76) vs SharedMessageLoop(普通端点共享线程池,47)。资源分级。

### 2.5 底层传输 network-common(简述)
远端消息进 Outbox(按地址分组/保序/复用连接)→ TransportClient(Netty)发出。network-common 处理:消息分帧、连接池、shuffle 块**零拷贝**(Netty FileRegion,磁盘 shuffle 文件不经用户态内存直接走 socket,第6章海量传输关键)。

---

## 三、Trick 与 Bug
**① SPARK-5293 Akka→Netty。** 弃 Akka:版本与用户程序冲突/难控大数据传输/减依赖。保留 actor 抽象只换底层,不动调用方=面向接口编程范本。
**② 本地消息跳序列化(193-199)。** driver 内部/local 高效;坑=又一"local能跑集群挂"来源。
**③ ThreadSafeRpcEndpoint 串行=无锁(Inbox 89-126)。** 同第3章单线程事件循环模式。
**④ 每个 ask 必有超时(258+)。** 远端无应答则永久阻塞。远程vs本地本质区别。
**⑤ Dedicated vs Shared MessageLoop(47,76)。** 热点端点独占线程,普通共享池。
**⑥ self 仅 onStart 后有效(54-63)。** 构造函数里调 self 出错。
**⑦ network-common 零拷贝(FileRegion)。** shuffle 文件不经用户态直走 socket。

---

## 四、本章总纲
```
目标: "发远程消息"像"调本地对象方法"(位置透明)。所有跨进程通信底座(第1章 RpcEnv 最先装配)
抽象(actor):
  RpcEndpoint=服务端有邮箱一次一条; receive(send)/receiveAndReply(ask)/onStart/onStop
    ThreadSafeRpcEndpoint 串行处理→无锁(同第3章单线程事件循环)
  RpcEndpointRef=遥控器; send(单向)/ask(Future)/askSync(阻塞)
路由[NettyRpcEnv 191-256]: remoteAddr==address→本地邮箱(不序列化不走网络); else→序列化+Netty; ask 注册超时
邮箱[Dispatcher+Inbox]: Dispatcher 按名路由到 Inbox; process enableConcurrent 对ThreadSafe恒false→串行无锁
  MessageLoop: Dedicated(热点独占线程) vs Shared(普通共享池)
传输 network-common: Outbox(按地址分组保序复用) + TransportClient(Netty); shuffle 零拷贝(FileRegion)
坑: SPARK-5293 Akka→Netty(抽象不变) | 本地跳序列化(local能跑集群挂) | ask必超时 | self仅onStart后有效
```

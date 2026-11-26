# Zhouda RPC

Zhouda RPC 是一个基于 Java 17 的独立异步二进制 RPC 框架，采用 Maven 多模块组织。框架将 NIO Reactor、Promise/Callback、类型化序列化、连接复用和背压整合在一条异步调用链中，适用于低延迟、高并发的进程间通信。

## 特性

- **异步非阻塞 I/O**：基于 Selector 的 Eventloop 驱动网络事件、本地任务和定时任务。
- **连接多路复用**：多个并发请求共享 TCP 连接，通过请求 index 关联乱序响应。
- **类型化协议**：请求和响应使用确定的二进制序列化格式，支持 `null` 消息与可空字段。
- **代码生成序列化**：SerializerFactory 在创建阶段生成序列化实现，减少热路径反射。
- **Promise 与 Callback**：服务端 Handler 返回 `Promise`，客户端支持 Promise API、Callback API 和 `CompletableFuture` 桥接。
- **超时与生命周期**：支持请求超时、优雅关闭、强制关闭和迟到响应隔离。
- **发送策略**：固定服务器、首个可用、首个有效结果、轮询、分片、一致性哈希、随机采样、类型分派和强制策略。
- **背压与过载保护**：区分普通请求和 `RpcMandatoryData` 请求，可限制过载队列。
- **网络能力**：支持自动 flush、KeepAlive、自动重连、AcceptFilter、AcceptOnce 和 SSL/TLS。
- **帧格式**：支持 LZ4 压缩、旧版 LZ4 格式以及自定义复合帧格式。
- **监控统计**：提供请求、连接、异常和响应时间统计，可接入 JMX。

## 环境要求

- JDK 17 或更高版本
- Maven 3.9.x

## 构建与测试

在仓库根目录执行：

```bash
mvn clean verify
```

安装到本地 Maven 仓库，供其他项目引用：

```bash
mvn clean install
```

只运行 RPC 模块及其依赖模块的测试：

```bash
mvn -pl rpc -am test
```

## Maven 依赖

根项目坐标：

```xml
<groupId>com.zhouda</groupId>
<artifactId>zhouda-rpc</artifactId>
<version>1.0.0</version>
```

业务项目通常只需要引用 RPC 模块：

```xml
<dependency>
    <groupId>com.zhouda</groupId>
    <artifactId>rpc</artifactId>
    <version>1.0.0</version>
</dependency>
```

RPC 模块会传递引入网络、数据流、序列化、Promise 和 Eventloop 等运行依赖。

## 模块结构

| 模块 | 职责 |
| --- | --- |
| `rpc-types` | 基础类型 |
| `rpc-common` | 通用工具、校验和内存尺寸 |
| `rpc-jmx-api` | JMX 注解与接口 |
| `rpc-jmx-stats` | 事件、异常和数值统计 |
| `rpc-eventloop` | NIO Reactor、任务队列和定时调度 |
| `rpc-promise` | Promise、Callback 和异步组合 |
| `rpc-bytebuf` | 字节缓冲区 |
| `rpc-net` | Reactor 网络抽象、TCP 与 SSL/TLS |
| `rpc-codegen` | 基于 ASM 的字节码生成 |
| `rpc-serializer` | 二进制序列化与反序列化 |
| `rpc-csp` | 帧处理、压缩和流式协议组件 |
| `rpc-datastream` | ChannelSerializer、ChannelDeserializer 与背压 |
| `rpc` | RPC 客户端、服务端、协议和发送策略 |

## 快速开始

下面的示例在同一台机器上启动服务端和客户端，完成一次类型化请求与响应。实际部署时，服务端和客户端通常运行在不同进程中。

### 1. 定义消息

请求和响应需要注册到同一份消息类型列表中。客户端与服务端的类型顺序必须一致；协议升级时不要随意调整已有类型的顺序。

```java
package example.rpc;

import com.zhouda.rpc.protocol.RpcMandatoryData;
import com.zhouda.rpc.serializer.annotations.SerializeRecord;

public final class Messages {
    private Messages() {}

    @SerializeRecord
    public record HelloRequest(String name) implements RpcMandatoryData {}

    @SerializeRecord
    public record HelloResponse(String message) {}
}
```

`record` 组件可能为 `null` 时，需要添加 `@SerializeNullable`：

```java
@SerializeRecord
public record HelloRequest(
    String name,
    @SerializeNullable String remark
) implements RpcMandatoryData {}
```

### 2. 启动服务端

`listen()`、`close()` 等生命周期操作必须在服务端所在的 Reactor 线程中调用。

```java
package example.rpc;

import com.zhouda.rpc.eventloop.Eventloop;
import com.zhouda.rpc.promise.Promise;
import com.zhouda.rpc.server.RpcServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public final class HelloServer {
    public static void main(String[] args) {
        Eventloop eventloop = Eventloop.builder()
            .withThreadName("hello-server")
            .build();
        eventloop.keepAlive(true);
        new Thread(eventloop, "hello-server").start();

        RpcServer server = CompletableFuture.supplyAsync(() -> {
            RpcServer rpcServer = RpcServer.builder(eventloop)
                .withMessageTypes(Messages.HelloRequest.class, Messages.HelloResponse.class)
                .withHandler(Messages.HelloRequest.class, request ->
                    Promise.of(new Messages.HelloResponse("Hello, " + request.name())))
                .withListenAddress(new InetSocketAddress("127.0.0.1", 8080))
                .build();
            try {
                rpcServer.listen();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return rpcServer;
        }, eventloop).join();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
            eventloop.execute(() -> server.close().subscribe(($, error) -> eventloop.breakEventloop()))));
    }
}
```

服务端 Handler 必须返回 `Promise<O>`。同步结果使用 `Promise.of(...)`，失败结果使用 `Promise.ofException(...)`，也可以返回尚未完成的异步 Promise。

### 3. 启动客户端并发送请求

```java
package example.rpc;

import com.zhouda.rpc.client.RpcClient;
import com.zhouda.rpc.client.sender.strategy.RpcStrategies;
import com.zhouda.rpc.eventloop.Eventloop;
import com.zhouda.rpc.promise.Promise;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class HelloClient {
    public static void main(String[] args) {
        Eventloop eventloop = Eventloop.builder()
            .withThreadName("hello-client")
            .build();
        eventloop.keepAlive(true);
        new Thread(eventloop, "hello-client").start();

        RpcClient client = CompletableFuture.supplyAsync(() ->
            RpcClient.builder(eventloop)
                .withMessageTypes(Messages.HelloRequest.class, Messages.HelloResponse.class)
                .withStrategy(RpcStrategies.server(new InetSocketAddress("127.0.0.1", 8080)))
                .withConnectTimeout(Duration.ofSeconds(3))
                .build(), eventloop).join();

        try {
            CompletableFuture.supplyAsync(client::start, eventloop)
                .thenCompose(Promise::toCompletableFuture)
                .join();

            Promise<Messages.HelloResponse> pending = CompletableFuture.supplyAsync(() ->
                client.<Messages.HelloRequest, Messages.HelloResponse>sendRequest(
                    new Messages.HelloRequest("Zhouda"), 3000), eventloop).join();

            Messages.HelloResponse response = pending.toCompletableFuture().join();

            System.out.println(response.message());
        } finally {
            eventloop.execute(() -> client.stop().subscribe(($, error) -> eventloop.breakEventloop()));
        }
    }
}
```

`sendRequest(request, timeout)` 的超时单位为毫秒，必须大于 `0`。省略超时的 Callback API 也可使用，但不会提供有限超时保护。

## 调用方式

以下代码应在客户端所在的 Reactor 线程中执行。

### Promise API

```java
Promise<Messages.HelloResponse> promise = client.sendRequest(
    new Messages.HelloRequest("Zhouda"), 3000);

promise.subscribe((response, error) -> {
    if (error == null) {
        System.out.println(response.message());
    } else {
        error.printStackTrace();
    }
});
```

### Callback API

```java
client.sendRequest(new Messages.HelloRequest("Zhouda"), (response, error) -> {
    if (error == null) {
        System.out.println(response.message());
    } else {
        error.printStackTrace();
    }
});
```

### CompletableFuture 桥接

```java
CompletableFuture<Messages.HelloResponse> future =
    client.sendRequest(new Messages.HelloRequest("Zhouda"), 3000).toCompletableFuture();
```

桥接只转换完成通知，不会把网络 I/O 变成阻塞调用。不要直接在 Reactor 线程上调用 `join()` 或 `get()` 等待尚未完成的 RPC，否则会阻塞该 Reactor。

## 发送策略

### 固定服务器

```java
.withStrategy(RpcStrategies.server(new InetSocketAddress("127.0.0.1", 8080)))
```

### 轮询

```java
.withStrategy(RpcStrategies.roundRobin(
    RpcStrategies.server(new InetSocketAddress("127.0.0.1", 8080)),
    RpcStrategies.server(new InetSocketAddress("127.0.0.1", 8081))))
```

### 分片

```java
.withStrategy(RpcStrategies.sharding(
    (Messages.HelloRequest request) -> Math.floorMod(request.name().hashCode(), 2),
    RpcStrategies.server(new InetSocketAddress("127.0.0.1", 8080)),
    RpcStrategies.server(new InetSocketAddress("127.0.0.1", 8081))))
```

`FirstValidResult` 会向多个候选服务器并发发送请求，并以第一个通过校验的结果为准。它不是失败重试，会增加下游请求量，只应用于确实需要并发探测的场景。

## 背压与过载队列

普通请求在发送端背压时会立即失败。实现 `RpcMandatoryData` 的请求可以进入过载队列：

```java
RpcClient client = RpcClient.builder(eventloop)
    .withMessageTypes(Messages.HelloRequest.class, Messages.HelloResponse.class)
    .withStrategy(RpcStrategies.server(new InetSocketAddress("127.0.0.1", 8080)))
    .withOverloadedQueueLimit(1024)
    .build();
```

队列上限只作用于 mandatory 请求。生产环境应根据内存容量、单请求大小和可接受延迟设置上限，不要依赖无界队列吸收流量。

## 自动 flush、KeepAlive 与重连

```java
RpcClient client = RpcClient.builder(eventloop)
    .withMessageTypes(Messages.HelloRequest.class, Messages.HelloResponse.class)
    .withStrategy(RpcStrategies.server(new InetSocketAddress("127.0.0.1", 8080)))
    .withAutoFlush(Duration.ofMillis(10))
    .withKeepAlive(Duration.ofSeconds(10))
    .withReconnectInterval(Duration.ofSeconds(1))
    .build();
```

服务端对应配置：

```java
RpcServer server = RpcServer.builder(eventloop)
    .withMessageTypes(Messages.HelloRequest.class, Messages.HelloResponse.class)
    .withHandler(Messages.HelloRequest.class, request ->
        Promise.of(new Messages.HelloResponse("Hello, " + request.name())))
    .withListenAddress(new InetSocketAddress("127.0.0.1", 8080))
    .withAutoFlushInterval(Duration.ofMillis(10))
    .build();
```

自动 flush 用少量延迟换取批量发送；KeepAlive 用于维持连接健康；重连不会自动重放已经结束的请求。

## LZ4 压缩

客户端与服务端必须使用兼容的帧格式：

```java
FrameFormat format = FrameFormats.lz4();

RpcServer server = RpcServer.builder(eventloop)
    // 其他配置保持不变
    .withStreamProtocol(MemSize.kilobytes(16), format)
    .build();

RpcClient client = RpcClient.builder(eventloop)
    // 其他配置保持不变
    .withStreamProtocol(MemSize.kilobytes(16), format)
    .build();
```

使用 LZ4 时，业务项目需要显式引入运行时依赖，因为 RPC 模块只在测试范围内使用它：

```xml
<dependency>
    <groupId>org.lz4</groupId>
    <artifactId>lz4-java</artifactId>
    <version>1.8.0</version>
</dependency>
```

旧版报文兼容可使用 `FrameFormats.lz4Legacy()`。压缩会消耗 CPU 和额外内存，适合可压缩负载或带宽受限链路，不保证所有场景都能降低延迟。

## SSL/TLS

服务端：

```java
RpcServer server = RpcServer.builder(eventloop)
    // 消息类型和 Handler 配置保持不变
    .withSslListenAddress(sslContext, sslExecutor, new InetSocketAddress("127.0.0.1", 8443))
    .build();
```

客户端：

```java
RpcClient client = RpcClient.builder(eventloop)
    // 消息类型配置保持不变
    .withStrategy(RpcStrategies.server(new InetSocketAddress("127.0.0.1", 8443)))
    .withSslEnabled(sslContext, sslExecutor)
    .build();
```

`SSLContext` 和 `Executor` 由应用提供。生产环境应分别配置服务端密钥库和客户端信任库，并启用主机名与证书校验。

## 监控

框架始终维护基础请求计数。开启详细监控后会增加分类、响应时间和处理耗时等统计开销：

```java
eventloop.execute(() -> {
    client.startMonitoring();
    server.startMonitoring();
});
```

`RpcClient` 和 `RpcServer` 提供 JMX 注解与统计 getter，相关 JMX API 与统计模块会随 RPC 依赖传递引入。需要通过 `ReactorJmxBeanAdapter` 等适配器及应用的 MBean 注册逻辑暴露指标；仅调用 `startMonitoring()` 不会自动创建 MBeanServer。

## 异常与关闭

- `AsyncTimeoutException`：请求超时。
- `AsyncCloseException`：在途请求因客户端或连接关闭而失败。
- `RpcRemoteException`：服务端业务异常跨网传播，保留异常类型和消息。
- `RpcException`：客户端不可用、策略切换失败等 RPC 层错误。

默认关闭会等待在途请求结束；需要立即终止时可使用 `withForcedShutdown()`。关闭操作必须在对应 Reactor 线程执行。

## 性能说明与使用边界

框架的高性能主要来自 NIO 事件驱动、同步完成回调、连接复用、代码生成序列化和批量读写，而不是消除所有成本：

- Handler 中执行慢同步任务会阻塞 Eventloop，应交给独立业务线程池，再通过 Reactor 回到回调线程。
- 每个有限超时请求都会增加定时任务和队列维护成本。
- 跨 Reactor 适配会增加任务提交和线程切换。
- SSL/TLS 与 LZ4 用 CPU、内存和延迟换取安全或带宽。
- 大消息、跨缓冲解码和不可压缩数据仍可能产生明显复制与分配成本。

具体吞吐和延迟应使用真实消息大小、Handler 耗时、节点数量和部署环境进行基准测试。

## 测试覆盖

当前测试包含 61 个 JUnit 4 用例，覆盖：

- 类型化协议、`null` 消息、可空字段和非法报文。
- TCP 请求响应、连接复用、乱序响应和并发多路复用。
- 超时、迟到响应、Callback API 和远程异常。
- 客户端与服务端序列化配置。
- 全部发送策略及非法参数。
- 背压、mandatory 队列和过载限制。
- 成功、失败、超时、连接与响应时间统计。
- LZ4 新旧格式、大消息扩容和自动 flush。
- KeepAlive、自动重连、策略切换和生命周期。
- AcceptFilter、AcceptOnce 和 SSL/TLS。
- 跨 Reactor 回调和 JMX 分发。

## 许可证

本项目基于 Apache License 2.0 发布，详见 [LICENSE](LICENSE)。

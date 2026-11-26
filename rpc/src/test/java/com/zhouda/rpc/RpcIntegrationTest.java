package com.zhouda.rpc;

import com.zhouda.rpc.async.exception.AsyncCloseException;
import com.zhouda.rpc.async.exception.AsyncTimeoutException;
import com.zhouda.rpc.client.IRpcClient;
import com.zhouda.rpc.client.RpcClient;
import com.zhouda.rpc.client.sender.strategy.RpcStrategies;
import com.zhouda.rpc.common.MemSize;
import com.zhouda.rpc.csp.process.frame.FrameFormats;
import com.zhouda.rpc.promise.Promise;
import com.zhouda.rpc.protocol.RpcRemoteException;
import com.zhouda.rpc.server.RpcServer;
import com.zhouda.rpc.support.RpcTestEnvironment;
import com.zhouda.rpc.support.TestMessages;
import com.zhouda.rpc.support.TestMessages.AlternateRequest;
import com.zhouda.rpc.support.TestMessages.AlternateResponse;
import com.zhouda.rpc.support.TestMessages.TestRequest;
import com.zhouda.rpc.support.TestMessages.TestResponse;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.zhouda.rpc.support.RpcTestSupport.toFuture;
import static org.junit.Assert.*;

public class RpcIntegrationTest {
	private static final int TIMEOUT = 3000;

	// 特性：TCP 连接上传输类型化请求与响应，并完整保留 DTO 字段。
	@Test
	public void sendsTypedRequestAndResponseOverTcp() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			TestRequest request = TestMessages.request(7);
			TestResponse response = env.await(env.send(client, request, TIMEOUT));
			assertEquals(request.id(), response.id());
			assertEquals(request.payload(), response.payload());
			assertEquals(request.labels(), response.labels());
		}
	}

	// 特性：同一客户端与服务器的连续请求复用一条 TCP 连接。
	@Test
	public void reusesOneConnectionForSequentialRequests() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			for (int i = 0; i < 20; i++) {
				assertEquals(i, roundTrip(i, env, client).id());
			}
			assertEquals(1, env.await(env.call(client::getActiveConnections)).intValue());
			assertEquals(1, env.await(env.call(server::getConnectionsCount)).intValue());
		}
	}

	// 特性：响应乱序到达时按请求 index 关联，不依赖网络顺序。
	@Test
	public void correlatesOutOfOrderResponsesByRequestIndex() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			CompletableFuture<TestResponse> slow = env.send(client,
				new TestRequest(1, "slow", 250, false, List.of()), TIMEOUT);
			CompletableFuture<TestResponse> fast = env.send(client,
				new TestRequest(2, "fast", 0, false, List.of()), TIMEOUT);
			assertEquals(2, env.await(fast).id());
			assertFalse(slow.isDone());
			assertEquals(1, env.await(slow).id());
		}
	}

	// 特性：单连接并发请求不会串值，完成后活跃请求数归零。
	@Test
	public void handlesManyConcurrentRequestsWithoutMixingResults() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			List<CompletableFuture<TestResponse>> responses = new ArrayList<>();
			for (int i = 0; i < 100; i++) {
				responses.add(env.send(client, TestMessages.request(i), TIMEOUT));
			}
			for (int i = 0; i < responses.size(); i++) {
				TestResponse response = env.await(responses.get(i));
				assertEquals(i, response.id());
				assertEquals("payload-" + i, response.payload());
			}
			assertEquals(0, env.await(env.call(client::getActiveRequests)).intValue());
		}
	}

	// 特性：服务端业务异常跨网传播为 RpcRemoteException，保留原始异常信息。
	@Test
	public void propagatesRemoteBusinessExceptionDetails() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			CompletableFuture<TestResponse> result = env.send(client,
				new TestRequest(9, "boom", 0, true, List.of()), TIMEOUT);
			Throwable failure = failure(result, env);
			assertTrue(failure instanceof RpcRemoteException);
			assertEquals(IllegalArgumentException.class.getName(), ((RpcRemoteException) failure).getCauseClassName());
			assertEquals("business failure: boom", ((RpcRemoteException) failure).getCauseMessage());
		}
	}

	// 特性：null 响应消息与 DTO 可空字段均能正确序列化和回调。
	@Test
	public void supportsNullResponseAndNullablePayload() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServerWithHandler((request) ->
				request.id() == -1 ? Promise.of((TestResponse) null) : Promise.of(new TestResponse(request.id(), null, null)));
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			assertNull(env.await(env.send(client, new TestRequest(-1, null, 0, false, null), TIMEOUT)));
			TestResponse response = env.await(env.send(client,
				new TestRequest(2, null, 0, false, null), TIMEOUT));
			assertEquals(2, response.id());
			assertNull(response.payload());
			assertNull(response.labels());
		}
	}

	// 特性：请求超时后移除回调，迟到响应不会污染后续请求。
	@Test
	public void timesOutDelayedRequestAndLateResponseDoesNotCorruptNextRequest() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			CompletableFuture<TestResponse> expired = env.send(client,
				new TestRequest(1, "late", 200, false, List.of()), 30);
			assertTrue(failure(expired, env) instanceof AsyncTimeoutException);
			assertEquals(2, env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(2), TIMEOUT)).id());
		}
	}

	// 特性：非正超时在本地立即失败，不向服务端发送请求。
	@Test
	public void rejectsNonPositiveTimeoutLocally() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			assertTrue(failure(env.send(client, TestMessages.request(1), 0), env) instanceof AsyncTimeoutException);
			assertTrue(failure(env.send(client, TestMessages.request(1), -1), env) instanceof AsyncTimeoutException);
			assertEquals(1, env.await(env.call(server::getConnectionsCount)).intValue());
		}
	}

	// 特性：Callback API 支持不设置超时的请求。
	@Test
	public void supportsCallbackApiWithoutTimeout() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			CompletableFuture<TestResponse> response = env.call(() -> {
				CompletableFuture<TestResponse> future = new CompletableFuture<>();
				client.<TestRequest, TestResponse>sendRequest(TestMessages.request(11), (result, error) -> {
					if (error == null) future.complete(result);
					else future.completeExceptionally(error);
				});
				return future;
			}).thenCompose(future -> future);
			assertEquals(11, env.await(response).id());
		}
	}

	// 特性：同一 RPC 客户端可按请求类型分派到不同业务处理器。
	@Test
	public void dispatchesMultipleRequestTypes() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			CompletableFuture<AlternateResponse> response = env.send(client, new AlternateRequest(42), TIMEOUT);
			assertEquals(42, env.await(response).id());
		}
	}

	// 特性：客户端与服务端都启用自动 flush 时仍能完成请求响应。
	@Test
	public void supportsAutoFlushOnBothEndpoints() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer(builder -> builder.withAutoFlushInterval(java.time.Duration.ofMillis(20)));
			RpcClient client = env.startClient(builder -> builder
				.withStrategy(RpcStrategies.server(env.address(server)))
				.withAutoFlush(java.time.Duration.ofMillis(20)));
			assertEquals(5, roundTrip(5, env, client).id());
		}
	}

	// 特性：初始缓冲区较小时，大消息会自动扩容并完整传输。
	@Test
	public void growsSmallInitialBuffersForLargeMessages() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer(builder -> builder.withStreamProtocol(MemSize.bytes(256)));
			RpcClient client = env.startClient(builder -> builder
				.withStrategy(RpcStrategies.server(env.address(server)))
				.withStreamProtocol(MemSize.bytes(256)));
			String payload = "x".repeat(512 * 1024);
			TestRequest request = new TestRequest(3, payload, 0, false, List.of());
			assertEquals(payload, env.await(env.<TestRequest, TestResponse>send(client, request, TIMEOUT)).payload());
		}
	}

	// 特性：LZ4 FrameFormat 对请求和响应执行透明压缩。
	@Test
	public void compressesTrafficWithLz4FrameFormat() throws Exception {
		assertCompressedRoundTrip(FrameFormats.lz4());
	}

	// 特性：兼容旧版 LZ4 报文格式。
	@Test
	public void supportsLegacyLz4WireFormat() throws Exception {
		assertCompressedRoundTrip(FrameFormats.lz4Legacy());
	}

	// 特性：KeepAlive 控制消息维持健康连接，不影响后续业务请求。
	@Test
	public void keepAliveMaintainsHealthyConnection() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder
				.withStrategy(RpcStrategies.server(env.address(server)))
				.withKeepAlive(java.time.Duration.ofMillis(20)));
			TimeUnit.MILLISECONDS.sleep(180);
			assertEquals(1, env.await(env.call(client::getActiveConnections)).intValue());
			assertEquals(6, roundTrip(6, env, client).id());
		}
	}

	// 特性：切换发送策略后关闭旧连接，新请求只发往新服务器。
	@Test
	public void changesStrategyAndDropsOldConnection() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer first = env.startServer($ -> {});
			RpcServer second = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(first))));
			assertEquals(1, roundTrip(1, env, client).id());
			CompletableFuture<Void> changed = env.call(() -> toFuture(client.changeStrategy(
				RpcStrategies.server(env.address(second)), false))).thenCompose(future -> future);
			env.await(changed);
			assertEquals(2, roundTrip(2, env, client).id());
			InetSocketAddress active = env.await(env.call(() ->
				client.getRequestStatsPerConnection().keySet().iterator().next()));
			assertEquals(env.address(second), active);
		}
	}

	// 特性：retry 策略保持等待，直到必需连接可用或被新策略取代。
	@Test
	public void retriesStrategyUntilRequiredConnectionExists() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			InetSocketAddress unavailable = unusedAddress();
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			CompletableFuture<Void> pending = env.call(() -> toFuture(client.changeStrategy(
				RpcStrategies.server(unavailable), true))).thenCompose(future -> future);
			assertFalse(pending.isDone());
			CompletableFuture<Void> available = env.call(() -> toFuture(client.changeStrategy(
				RpcStrategies.server(env.address(server)), false))).thenCompose(future -> future);
			env.await(available);
			Throwable superseded = failure(pending, env);
			assertTrue(superseded instanceof com.zhouda.rpc.protocol.RpcException);
			assertEquals(4, roundTrip(4, env, client).id());
		}
	}

	// 特性：RoundRobin 在真实多服务器连接间均匀分配请求。
	@Test
	public void roundRobinIntegrationBalancesRealServers() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			AtomicInteger firstCalls = new AtomicInteger();
			AtomicInteger secondCalls = new AtomicInteger();
			RpcServer first = env.startServerWithHandler(request -> {
				firstCalls.incrementAndGet();
				return Promise.of(new TestResponse(request.id(), "first", request.labels()));
			}, $ -> {});
			RpcServer second = env.startServerWithHandler(request -> {
				secondCalls.incrementAndGet();
				return Promise.of(new TestResponse(request.id(), "second", request.labels()));
			}, $ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.roundRobin(
				RpcStrategies.server(env.address(first)), RpcStrategies.server(env.address(second)))));
			for (int i = 0; i < 10; i++) {
				roundTrip(i, env, client);
			}
			assertEquals(5, firstCalls.get());
			assertEquals(5, secondCalls.get());
		}
	}

	// 特性：优雅关闭等待在途请求完成并收到响应。
	@Test
	public void gracefulStopWaitsForInFlightResponse() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			CompletableFuture<TestResponse> response = env.send(client,
				new TestRequest(8, "graceful", 200, false, List.of()), TIMEOUT);
			CompletableFuture<Void> stopping = env.call(() -> toFuture(client.stop())).thenCompose(future -> future);
			assertFalse(stopping.isDone());
			assertEquals(8, env.await(response).id());
			env.await(stopping);
		}
	}

	// 特性：强制关闭立即结束在途请求，并传播 AsyncCloseException。
	@Test
	public void forcedStopFailsInFlightRequest() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder
				.withStrategy(RpcStrategies.server(env.address(server)))
				.withForcedShutdown());
			CompletableFuture<TestResponse> response = env.send(client,
				new TestRequest(8, "forced", 500, false, List.of()), TIMEOUT);
			CompletableFuture<Void> stopping = env.call(() -> toFuture(client.stop())).thenCompose(future -> future);
			env.await(stopping);
			assertTrue(failure(response, env) instanceof AsyncCloseException);
		}
	}

	// 特性：适配到另一 reactor 后，回调仍在调用方 reactor 线程执行。
	@Test
	public void adaptsClientToAnotherReactor() throws Exception {
		try (RpcTestEnvironment serverEnv = RpcTestEnvironment.start(); RpcTestEnvironment clientEnv = RpcTestEnvironment.start()) {
			RpcServer server = serverEnv.startServer($ -> {});
			RpcClient client = clientEnv.startClient(builder ->
				builder.withStrategy(RpcStrategies.server(serverEnv.address(server))));
			IRpcClient adapted = client.adaptToAnotherReactor(serverEnv.getEventloop());
			AtomicReference<String> callbackThread = new AtomicReference<>();
			CompletableFuture<TestResponse> response = serverEnv.call(() -> {
				CompletableFuture<TestResponse> future = new CompletableFuture<>();
				adapted.<TestRequest, TestResponse>sendRequest(TestMessages.request(12), (result, error) -> {
					callbackThread.set(Thread.currentThread().getName());
					if (error == null) future.complete(result);
					else future.completeExceptionally(error);
				});
				return future;
			}).thenCompose(future -> future);
			assertEquals(12, serverEnv.await(response).id());
			assertEquals("rpc-test-eventloop", callbackThread.get());
		}
	}

	private InetSocketAddress unusedAddress() throws Exception {
		try (ServerSocket socket = new ServerSocket(0)) {
			return new InetSocketAddress("127.0.0.1", socket.getLocalPort());
		}
	}

	private void assertCompressedRoundTrip(com.zhouda.rpc.csp.process.frame.FrameFormat format) throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer(builder -> builder.withStreamProtocol(MemSize.kilobytes(16), format));
			RpcClient client = env.startClient(builder -> builder
				.withStrategy(RpcStrategies.server(env.address(server)))
				.withStreamProtocol(MemSize.kilobytes(16), format));
			String payload = "compressible-data-".repeat(20_000);
			assertEquals(payload, env.await(env.<TestRequest, TestResponse>send(client,
				new TestRequest(15, payload, 0, false, List.of("compressed")), TIMEOUT)).payload());
		}
	}

	private TestResponse roundTrip(int id, RpcTestEnvironment env, RpcClient client) throws Exception {
		return env.await(env.send(client, TestMessages.request(id), TIMEOUT));
	}

	private Throwable failure(CompletableFuture<?> future, RpcTestEnvironment env) throws Exception {
		try {
			env.await(future);
			fail("Expected exceptional completion");
			return null;
		} catch (Exception e) {
			return e;
		}
	}
}

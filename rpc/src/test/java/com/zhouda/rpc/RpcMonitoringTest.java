package com.zhouda.rpc;

import com.zhouda.rpc.async.exception.AsyncTimeoutException;
import com.zhouda.rpc.client.RpcClient;
import com.zhouda.rpc.client.jmx.RpcRequestStats;
import com.zhouda.rpc.client.sender.strategy.RpcStrategies;
import com.zhouda.rpc.eventloop.Eventloop;
import com.zhouda.rpc.protocol.RpcRemoteException;
import com.zhouda.rpc.server.RpcServer;
import com.zhouda.rpc.support.RpcTestEnvironment;
import com.zhouda.rpc.support.RpcTestSupport;
import com.zhouda.rpc.support.TestMessages;
import com.zhouda.rpc.support.TestMessages.TestRequest;
import com.zhouda.rpc.support.TestMessages.TestResponse;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.zhouda.rpc.support.RpcTestSupport.toFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class RpcMonitoringTest {

	/** 请求监控分别记录成功、业务失败和超时，并保留响应时间样本。 */
	@Test
	public void recordsSuccessfulFailedAndExpiredRequests() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			client.startMonitoring();
			server.startMonitoring();
			var success = env.<TestRequest, TestResponse>send(client, TestMessages.request(1), 1000);
			var failure = env.<TestRequest, TestResponse>send(client,
				new TestRequest(2, null, 0, true, null), 1000);
			var timeout = env.<TestRequest, TestResponse>send(client,
				new TestRequest(3, null, 100, false, null), 30);

			assertEquals(1, env.await(success).id());
			assertThrows(RpcRemoteException.class, () -> env.await(failure));
			assertThrows(AsyncTimeoutException.class, () -> env.await(timeout));

			RpcRequestStats stats = env.await(env.call(() -> client.getRequestsStatsPerClass().get(TestRequest.class)));
			assertNotNull(stats);
			assertEquals(3, stats.getTotalRequests().getTotalCount());
			assertEquals(1, stats.getFailedRequests().getTotalCount());
			assertEquals(1, stats.getExpiredRequests().getTotalCount());
			assertEquals(2, stats.getResponseTime().getCount());
			assertEquals(3, client.getGeneralRequestsStats().getTotalRequests().getTotalCount());
			assertEquals(1, client.getGeneralRequestsStats().getFailedRequests().getTotalCount());
			assertEquals(1, client.getGeneralRequestsStats().getExpiredRequests().getTotalCount());
			assertEquals(2, client.getGeneralRequestsStats().getResponseTime().getCount());
			assertEquals(1, server.getFailedRequests().getTotalCount());
			assertEquals(1, server.getLastRequestHandlingException().getTotal());
			assertEquals(1, server.getSuccessfulRequests().getTotalCount());
			assertEquals(2, server.getRequestHandlingTime().getCount());
		}
	}

	/** 停止客户端会释放连接；新客户端可重新连接并累计服务端连接次数。 */
	@Test
	public void recordsConnectionLifecycle() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			assertEquals(1, env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(1), 1000)).id());
			assertEquals(1, client.getTotalSuccessfulConnects());
			assertEquals(1, client.getActiveConnections());
			assertEquals(1, server.getTotalConnects().getTotalCount());
			assertEquals(1, server.getConnectionsCount());
			assertEquals(0, client.getLastProtocolError().getTotal());

			env.await(env.call(() -> toFuture(client.stop())).thenCompose(future -> future));
			assertEquals(0, client.getActiveConnections());
			assertEquals(0, server.getConnectionsCount());

			RpcClient replacement = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			assertEquals(2, env.await(env.<TestRequest, TestResponse>send(replacement, TestMessages.request(2), 1000)).id());
			assertEquals(1, replacement.getTotalSuccessfulConnects());
			assertEquals(2, server.getTotalConnects().getTotalCount());
		}
	}

	/** 未开启监控时不产生按请求类型的统计样本。 */
	@Test
	public void monitoringIsOptIn() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			assertEquals(1, env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(1), 1000)).id());
			assertTrue(env.await(env.call(() -> client.getRequestsStatsPerClass().isEmpty())));
			assertEquals(1, client.getGeneralRequestsStats().getTotalRequests().getTotalCount());
			assertEquals(0, client.getGeneralRequestsStats().getResponseTime().getCount());
		}
	}

	/** 运行时可切换监控；详细统计只覆盖开启区间，队列上限对既有客户端生效。 */
	@Test
	public void togglesMonitoringAndOverloadLimitAtRuntime() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			RpcClient client = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(env.address(server))));
			assertEquals(1, env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(1), 1000)).id());
			env.await(env.run(client::startMonitoring));
			assertTrue(client.isMonitoring());
			assertEquals(2, env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(2), 1000)).id());
			env.await(env.run(client::stopMonitoring));
			assertFalse(client.isMonitoring());
			assertEquals(3, env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(3), 1000)).id());

			RpcRequestStats stats = env.await(env.call(() -> client.getRequestsStatsPerClass().get(TestRequest.class)));
			assertNotNull(stats);
			assertEquals(1, stats.getTotalRequests().getTotalCount());
			assertEquals(1, stats.getResponseTime().getCount());
			assertEquals(3, client.getGeneralRequestsStats().getTotalRequests().getTotalCount());
			env.await(env.run(() -> client.setOverloadedQueueLimit(5)));
			assertEquals(5, client.getOverloadedQueueLimit());
			assertEquals(0, client.getOverloadedQueueSize());
			assertTrue(client.getUnresponsiveServers().isEmpty());
		}
	}

	/** 错误连接不会建立 sender，并计入每个地址的连接失败统计。 */
	@Test
	public void recordsFailedConnections() throws Exception {
		Eventloop eventloop = Eventloop.builder().withCurrentThread().build();
		eventloop.keepAlive(true);
		CompletableFuture<RpcClient> started = new CompletableFuture<>();
		InetSocketAddress address;
		try (ServerSocket socket = new ServerSocket(0)) {
			address = new InetSocketAddress("127.0.0.1", socket.getLocalPort());
		}
		Thread eventloopThread = new Thread(eventloop, "rpc-failed-connect-test");
		eventloopThread.start();
		try {
			eventloop.submit(() -> {
				RpcClient client = RpcTestSupport.clientBuilder(eventloop, RpcStrategies.server(address))
					.withConnectTimeout(java.time.Duration.ofMillis(200))
					.build();
				client.start().whenResult($ -> started.complete(client)).whenException($ -> started.complete(client));
			});
			RpcClient client = await(started);
			try {
				CompletableFuture<Long> failedConnects = new CompletableFuture<>();
				eventloop.delayBackground(300, () -> failedConnects.complete(client.getTotalFailedConnects()));
				assertTrue(await(failedConnects) >= 1);
				var stats = CompletableFuture.supplyAsync(() -> client.getConnectsStatsPerAddress().get(address), eventloop)
					.get(5, TimeUnit.SECONDS);
				assertNotNull(stats);
				assertTrue(stats.getFailedConnects() >= 1);
				assertFalse(stats.isConnected());
				assertEquals(0L, CompletableFuture.supplyAsync(client::getTotalSuccessfulConnects, eventloop)
					.get(5, TimeUnit.SECONDS).longValue());
			} finally {
				CompletableFuture<Void> stopped = new CompletableFuture<>();
				eventloop.submit(() -> client.stop().whenResult($ -> stopped.complete(null)).whenException(stopped::completeExceptionally));
				await(stopped);
			}
		} finally {
			eventloop.breakEventloop();
			eventloopThread.join(TimeUnit.SECONDS.toMillis(5));
		}
	}

	private static <T> T await(CompletableFuture<T> future) throws Exception {
		return future.get(5, TimeUnit.SECONDS);
	}
}

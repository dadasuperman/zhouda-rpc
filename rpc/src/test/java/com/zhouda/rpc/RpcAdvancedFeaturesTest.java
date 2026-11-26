package com.zhouda.rpc;

import com.zhouda.rpc.client.RpcClient;
import com.zhouda.rpc.client.sender.strategy.RpcStrategies;
import com.zhouda.rpc.client.sender.strategy.impl.FirstValidResult;
import com.zhouda.rpc.eventloop.Eventloop;
import com.zhouda.rpc.promise.Promise;
import com.zhouda.rpc.protocol.RpcMessage;
import com.zhouda.rpc.server.RpcServer;
import com.zhouda.rpc.serializer.BinarySerializer;
import com.zhouda.rpc.serializer.SerializerFactory;
import com.zhouda.rpc.support.RpcTestEnvironment;
import com.zhouda.rpc.support.TestMessages;
import com.zhouda.rpc.support.TestMessages.TestRequest;
import com.zhouda.rpc.support.TestMessages.TestResponse;
import org.junit.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class RpcAdvancedFeaturesTest {
	private static final int TIMEOUT = 3000;

	// 特性：服务端关闭后客户端按 reconnect interval 自动恢复连接，不重放已结束请求。
	@Test
	public void reconnectsAfterServerRestart() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			AtomicInteger firstCalls = new AtomicInteger();
			RpcServer first = env.startServerWithHandler(request -> {
				firstCalls.incrementAndGet();
				return Promise.of(new TestResponse(request.id(), "first", request.labels()));
			}, $ -> {});
			InetSocketAddress address = env.address(first);
			RpcClient client = env.startClient(builder -> builder
				.withStrategy(RpcStrategies.server(address))
				.withReconnectInterval(java.time.Duration.ofMillis(20)));
			assertEquals(1, env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(1), TIMEOUT)).id());
			env.await(env.call(() -> com.zhouda.rpc.support.RpcTestSupport.toFuture(first.close())).thenCompose(future -> future));

			AtomicInteger secondCalls = new AtomicInteger();
			env.startServerWithHandler(request -> {
				secondCalls.incrementAndGet();
				return Promise.of(new TestResponse(request.id(), "second", request.labels()));
			}, builder -> builder.withListenAddress(address));
			awaitSuccessfulConnects(env, client, 2);
			assertEquals(2, env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(2), TIMEOUT)).id());
			assertEquals(1, firstCalls.get());
			assertEquals(1, secondCalls.get());
			assertEquals(1, client.getActiveConnections());
			assertEquals(2, client.getTotalSuccessfulConnects());
		}
	}

	// 特性：AcceptFilter 可拒绝连接，并把 ssl 标志与过滤次数暴露给监控。
	@Test
	public void filtersRejectedPlaintextConnections() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			AtomicBoolean ssl = new AtomicBoolean(true);
			RpcServer server = env.startServer(builder -> builder.withAcceptFilter((channel, local, remote, secure) -> {
				ssl.set(secure);
				return true;
			}));
			RpcClient client = env.startClient(builder ->
				builder.withStrategy(RpcStrategies.server(env.address(server))));
			assertThrows(Exception.class, () ->
				env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(1), TIMEOUT)));
			assertFalse(ssl.get());
			assertEquals(1, server.getFilteredAccepts().getTotalCount());
			assertEquals(0, server.getConnectionsCount());
		}
	}

	// 特性：AcceptOnce 接受首个连接后停止监听，但已有连接仍可继续处理请求。
	@Test
	public void acceptOnceKeepsFirstConnectionAndStopsListening() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer(builder -> builder.withAcceptOnce());
			InetSocketAddress address = env.address(server);
			RpcClient first = env.startClient(builder -> builder.withStrategy(RpcStrategies.server(address)));
			assertEquals(1, env.await(env.<TestRequest, TestResponse>send(first, TestMessages.request(1), TIMEOUT)).id());
			assertTrue(env.await(env.call(server::getBoundAddresses)).isEmpty());
			assertThrows(Exception.class, () -> env.startClient(builder -> builder.withStrategy(RpcStrategies.server(address))));
			assertEquals(2, env.await(env.<TestRequest, TestResponse>send(first, TestMessages.request(2), TIMEOUT)).id());
			assertEquals(1, server.getTotalConnects().getTotalCount());
		}
	}

	// 特性：SSL/TLS 监听与客户端加密握手成功后，RPC 消息可在加密连接上往返。
	@Test
	public void exchangesRequestsOverSsl() throws Exception {
		Path directory = Files.createTempDirectory("zhouda-rpc-ssl-");
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			SSLContext sslContext = createSslContext(directory);
			RpcServer server = env.startServer(builder -> builder
				.withListenAddresses(List.of())
				.withSslListenAddress(sslContext, executor, new InetSocketAddress("127.0.0.1", 0)));
			RpcClient client = env.startClient(builder -> builder
				.withSslEnabled(sslContext, executor)
				.withStrategy(RpcStrategies.server(server.getSslBoundAddresses().get(0))));
			assertEquals(9, env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(9), TIMEOUT)).id());
			assertTrue(server.getBoundAddresses().isEmpty());
			assertEquals(1, server.getAcceptsSsl().getTotalCount());
		} finally {
			executor.shutdownNow();
			try (Stream<Path> paths = Files.walk(directory)) {
				for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
					Files.delete(path);
				}
			}
		}
	}

	// 特性：网络中的未知消息子类被记录为协议错误，并关闭非法连接。
	@Test
	public void recordsProtocolErrorAndClosesMalformedConnection() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			RpcServer server = env.startServer($ -> {});
			BinarySerializer<RpcMessage> serializer = SerializerFactory.builder()
				.withSubclasses(RpcMessage.SUBCLASSES_ID, com.zhouda.rpc.support.RpcTestSupport.MESSAGE_TYPES)
				.build()
				.create(RpcMessage.class);
			byte[] bytes = new byte[1024];
			int end = serializer.encode(bytes, 0, new RpcMessage(1, TestMessages.request(1)));
			bytes[4] = (byte) 127;
			try (Socket socket = new Socket()) {
				socket.connect(env.address(server), TIMEOUT);
				socket.getOutputStream().write(bytes, 0, end);
				socket.getOutputStream().flush();
			}
			awaitProtocolError(env, server);
			assertTrue(server.getLastProtocolError().getTotal() >= 1);
			assertEquals(0, server.getConnectionsCount());
		}
	}

	// 特性：FirstValidResult 并发探测多个节点，每个节点只处理一次，无效结果不触发请求级重试。
	@Test
	public void firstValidResultProbesEachServerOnce() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			AtomicInteger firstCalls = new AtomicInteger();
			AtomicInteger secondCalls = new AtomicInteger();
			AtomicBoolean valid = new AtomicBoolean(true);
			RpcServer first = env.startServerWithHandler(request -> {
				firstCalls.incrementAndGet();
				return Promise.of(new TestResponse(request.id(), "invalid", request.labels()));
			}, $ -> {});
			RpcServer second = env.startServerWithHandler(request -> {
				secondCalls.incrementAndGet();
				return Promise.of(new TestResponse(request.id(), valid.get() ? "valid" : "invalid", request.labels()));
			}, $ -> {});
			FirstValidResult strategy = FirstValidResult.builder(
				RpcStrategies.server(env.address(first)), RpcStrategies.server(env.address(second)))
				.withResultValidator((TestResponse response) -> response.payload().equals("valid"))
				.withNoValidResultException(new IllegalStateException("no valid result"))
				.build();
			RpcClient client = env.startClient(builder -> builder.withStrategy(strategy));
			TestResponse response = env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(1), TIMEOUT));
			assertEquals("valid", response.payload());
			assertEquals(1, firstCalls.get());
			assertEquals(1, secondCalls.get());

			valid.set(false);
			assertThrows(IllegalStateException.class, () ->
				env.await(env.<TestRequest, TestResponse>send(client, TestMessages.request(2), TIMEOUT)));
			assertEquals(2, firstCalls.get());
			assertEquals(2, secondCalls.get());
		}
	}

	private static void awaitSuccessfulConnects(RpcTestEnvironment env, RpcClient client, long expected) throws Exception {
		CompletableFuture<Void> completed = new CompletableFuture<>();
		env.run(() -> waitForConnects(env.getEventloop(), client, expected, 100, completed));
		env.await(completed);
	}

	private static void waitForConnects(Eventloop eventloop, RpcClient client, long expected, int attempts,
		CompletableFuture<Void> completed) {
		if (client.getTotalSuccessfulConnects() >= expected) {
			completed.complete(null);
		} else if (attempts == 0) {
			completed.completeExceptionally(new IllegalStateException("Client did not reconnect"));
		} else {
			eventloop.delayBackground(20, () -> waitForConnects(eventloop, client, expected, attempts - 1, completed));
		}
	}

	private static void awaitProtocolError(RpcTestEnvironment env, RpcServer server) throws Exception {
		CompletableFuture<Void> completed = new CompletableFuture<>();
		env.run(() -> waitForProtocolError(env.getEventloop(), server, 100, completed));
		env.await(completed);
	}

	private static void waitForProtocolError(Eventloop eventloop, RpcServer server, int attempts,
		CompletableFuture<Void> completed) {
		if (server.getLastProtocolError().getTotal() > 0) {
			completed.complete(null);
		} else if (attempts == 0) {
			completed.completeExceptionally(new IllegalStateException("Protocol error was not recorded"));
		} else {
			eventloop.delayBackground(20, () -> waitForProtocolError(eventloop, server, attempts - 1, completed));
		}
	}

	private static SSLContext createSslContext(Path directory) throws Exception {
		Path keyStorePath = directory.resolve("rpc-test.p12");
		String executable = System.getProperty("os.name").startsWith("Windows") ? "keytool.exe" : "keytool";
		String keytool = Path.of(System.getProperty("java.home"), "bin", executable).toString();
		Process process = new ProcessBuilder(keytool,
			"-genkeypair", "-alias", "rpc-test", "-keyalg", "RSA", "-keysize", "2048",
			"-validity", "1", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
			"-storetype", "PKCS12", "-keystore", keyStorePath.toString(),
			"-storepass", "changeit", "-keypass", "changeit", "-noprompt")
			.redirectErrorStream(true)
			.start();
		try (InputStream output = process.getInputStream()) {
			output.readAllBytes();
		}
		assertTrue(process.waitFor(10, TimeUnit.SECONDS));
		assertEquals(0, process.exitValue());

		char[] password = "changeit".toCharArray();
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		try (InputStream input = Files.newInputStream(keyStorePath)) {
			keyStore.load(input, password);
		}
		KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keyManagers.init(keyStore, password);
		TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagers.init(keyStore);
		SSLContext context = SSLContext.getInstance("TLS");
		context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
		return context;
	}
}

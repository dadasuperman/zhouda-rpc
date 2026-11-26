package com.zhouda.rpc.support;

import com.zhouda.rpc.async.callback.AsyncComputation;
import com.zhouda.rpc.client.RpcClient;
import com.zhouda.rpc.common.function.RunnableEx;
import com.zhouda.rpc.eventloop.Eventloop;
import com.zhouda.rpc.server.RpcServer;
import com.zhouda.rpc.server.RpcRequestHandler;
import com.zhouda.rpc.support.TestMessages.TestRequest;
import com.zhouda.rpc.support.TestMessages.TestResponse;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class RpcTestEnvironment implements AutoCloseable {
	private static final long TIMEOUT_SECONDS = 10;

	private final Eventloop eventloop;
	private final Thread eventloopThread;
	private final List<RpcClient> clients = new ArrayList<>();
	private final List<RpcServer> servers = new ArrayList<>();

	private RpcTestEnvironment() {
		eventloop = Eventloop.builder().withCurrentThread().withThreadName("rpc-test-eventloop").build();
		eventloop.keepAlive(true);
		eventloopThread = new Thread(eventloop, "rpc-test-eventloop");
		eventloopThread.start();
	}

	public static RpcTestEnvironment start() {
		return new RpcTestEnvironment();
	}

	public Eventloop getEventloop() {
		return eventloop;
	}

	public InetSocketAddress address(RpcServer server) {
		return server.getBoundAddresses().get(0);
	}

	public RpcServer startServer(Consumer<RpcServer.Builder> configurer) throws Exception {
		return startServerWithHandler(RpcTestSupport.standardRequestHandler(eventloop), configurer);
	}

	public RpcServer startServerWithHandler(RpcRequestHandler<TestRequest, TestResponse> handler) throws Exception {
		return startServerWithHandler(handler, $ -> {});
	}

	public RpcServer startServerWithHandler(RpcRequestHandler<TestRequest, TestResponse> handler,
		Consumer<RpcServer.Builder> configurer) throws Exception {
		return await(call(() -> {
			RpcServer.Builder builder = RpcTestSupport.serverBuilder(eventloop, handler)
				.withListenAddress(new InetSocketAddress("127.0.0.1", 0));
			if (configurer != null) {
				configurer.accept(builder);
			}
			RpcServer server = builder.build();
			server.listen();
			servers.add(server);
			return server;
		}));
	}

	public RpcClient startClient(Consumer<RpcClient.Builder> configurer) throws Exception {
		CompletableFuture<Void> started = new CompletableFuture<>();
		RpcClient client = await(call(() -> {
			RpcClient.Builder builder = RpcTestSupport.clientBuilder(eventloop, new RpcClient.NoServersRpcStrategy());
			if (configurer != null) {
				configurer.accept(builder);
			}
			RpcClient result = builder.build();
			clients.add(result);
			result.start().whenResult($ -> started.complete(null)).whenException(started::completeExceptionally);
			return result;
		}));
		await(started);
		return client;
	}

	public <I, O> CompletableFuture<O> send(RpcClient client, I request, int timeoutMillis) {
		return eventloop.submit(callback -> client.sendRequest(request, timeoutMillis, callback));
	}

	public <I, O> CompletableFuture<O> sendWithoutTimeout(RpcClient client, I request) {
		return eventloop.submit(callback -> client.sendRequest(request, callback));
	}

	public <T> CompletableFuture<T> call(Callable<T> action) {
		return eventloop.submit(AsyncComputation.of(action::call));
	}

	public CompletableFuture<Void> run(Runnable action) {
		return eventloop.submit((RunnableEx) action::run);
	}

	public static <T> T await(CompletableFuture<T> future) throws Exception {
		try {
			return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof Exception cause) {
				throw cause;
			}
			if (e.getCause() instanceof Error error) {
				throw error;
			}
			throw e;
		}
	}

	@Override
	public void close() throws Exception {
		Exception failure = null;
		try {
			CompletableFuture<Void> closed = new CompletableFuture<>();
			await(run(() -> closeClients(0, closed)));
			await(closed);
		} catch (Exception e) {
			failure = e;
		} finally {
			eventloop.breakEventloop();
			try {
				eventloopThread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				if (failure == null) {
					failure = e;
				}
			}
			if (eventloopThread.isAlive() && failure == null) {
				failure = new IllegalStateException("Eventloop did not stop");
			}
		}
		if (failure != null) {
			throw failure;
		}
	}

	private void closeClients(int index, CompletableFuture<Void> completed) {
		if (index == clients.size()) {
			closeServers(0, completed);
			return;
		}
		RpcClient client = clients.get(index);
		client.stop()
			.whenResult($ -> closeClients(index + 1, completed))
			.whenException(completed::completeExceptionally);
	}

	private void closeServers(int index, CompletableFuture<Void> completed) {
		if (index == servers.size()) {
			completed.complete(null);
			return;
		}
		servers.get(index).close()
			.whenResult($ -> closeServers(index + 1, completed))
			.whenException(completed::completeExceptionally);
	}
}

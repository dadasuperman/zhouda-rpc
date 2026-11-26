package com.zhouda.rpc.support;

import com.zhouda.rpc.client.RpcClient;
import com.zhouda.rpc.client.sender.strategy.RpcStrategy;
import com.zhouda.rpc.eventloop.Eventloop;
import com.zhouda.rpc.promise.Promise;
import com.zhouda.rpc.promise.SettablePromise;
import com.zhouda.rpc.server.RpcRequestHandler;
import com.zhouda.rpc.server.RpcServer;

import java.util.List;

import static com.zhouda.rpc.support.TestMessages.*;

public final class RpcTestSupport {
	public static final List<Class<?>> MESSAGE_TYPES = List.of(
		TestRequest.class, TestResponse.class, AlternateRequest.class, AlternateResponse.class, NormalRequest.class);

	private RpcTestSupport() {}

	public static RpcServer.Builder serverBuilder(Eventloop eventloop) {
		return serverBuilder(eventloop, standardRequestHandler(eventloop));
	}

	public static RpcServer.Builder serverBuilder(Eventloop eventloop, RpcRequestHandler<TestRequest, TestResponse> handler) {
		return RpcServer.builder(eventloop)
			.withMessageTypes(MESSAGE_TYPES)
			.withHandler(TestRequest.class, handler)
			.withHandler(AlternateRequest.class, request -> Promise.of(new AlternateResponse(request.id())));
	}

	public static RpcClient.Builder clientBuilder(Eventloop eventloop, RpcStrategy strategy) {
		return RpcClient.builder(eventloop)
			.withMessageTypes(MESSAGE_TYPES)
			.withStrategy(strategy)
			.withConnectTimeout(java.time.Duration.ofSeconds(2))
			.withReconnectInterval(java.time.Duration.ofMillis(20));
	}

	public static RpcRequestHandler<TestRequest, TestResponse> standardRequestHandler(Eventloop eventloop) {
		return request -> {
			if (request.fail()) {
				return Promise.ofException(new IllegalArgumentException("business failure: " + request.payload()));
			}
			if (request.delayMillis() > 0) {
				SettablePromise<TestResponse> promise = new SettablePromise<>();
				eventloop.delayBackground(request.delayMillis(), () ->
					promise.set(new TestResponse(request.id(), request.payload(), request.labels())));
				return promise;
			}
			return Promise.of(new TestResponse(request.id(), request.payload(), request.labels()));
		};
	}

	public static <T> java.util.concurrent.CompletableFuture<T> toFuture(Promise<T> promise) {
		java.util.concurrent.CompletableFuture<T> future = new java.util.concurrent.CompletableFuture<>();
		promise.whenResult(future::complete).whenException(future::completeExceptionally);
		return future;
	}

}

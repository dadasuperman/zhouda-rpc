package com.zhouda.rpc.client;

import com.zhouda.rpc.client.sender.strategy.RpcStrategies;
import com.zhouda.rpc.protocol.RpcMessage;
import com.zhouda.rpc.protocol.RpcOverloadException;
import com.zhouda.rpc.support.RpcTestEnvironment;
import com.zhouda.rpc.support.TestMessages;
import com.zhouda.rpc.support.TestMessages.NormalRequest;
import com.zhouda.rpc.support.TestMessages.TestRequest;
import com.zhouda.rpc.support.TestMessages.TestResponse;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class RpcClientOverloadTest {
	// 特性：发送端背压时，普通请求立即拒绝，mandatory 请求按上限排队。
	@Test
	public void rejectsNonMandatoryRequestsWhileSenderIsSuspended() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			env.await(env.run(() -> {
				RpcClient client = RpcClient.builder(env.getEventloop())
					.withMessageTypes(TestRequest.class, NormalRequest.class, TestResponse.class)
					.withStrategy(RpcStrategies.server(new InetSocketAddress("127.0.0.1", 1)))
					.withOverloadedQueueLimit(2)
					.build();
				RpcClientConnection connection = new RpcClientConnection(client,
					new InetSocketAddress("127.0.0.1", 1), null, 0);
				List<RpcMessage> sent = new ArrayList<>();
				connection.onSenderReady(sent::add);
				connection.onSenderSuspended();

				ResultCallback<TestResponse> callback = new ResultCallback<>();
				connection.sendRequest(new NormalRequest(1), callback);

				assertTrue(callback.called);
				assertNull(callback.result);
				assertTrue(callback.exception instanceof RpcOverloadException);
				assertEquals(0, connection.getOverloadedQueueSize());
				assertTrue(sent.isEmpty());
			}));
		}
	}

	// 特性：mandatory 队列满后拒绝新请求，响应到达后释放配额并忽略迟到请求。
	@Test
	public void limitsMandatoryQueueAndReleasesSlotsOnResponses() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			env.await(env.run(() -> {
				RpcClient client = RpcClient.builder(env.getEventloop())
					.withMessageTypes(TestRequest.class, TestResponse.class)
					.withStrategy(RpcStrategies.server(new InetSocketAddress("127.0.0.1", 1)))
					.withOverloadedQueueLimit(2)
					.build();
				RpcClientConnection connection = new RpcClientConnection(client,
					new InetSocketAddress("127.0.0.1", 1), null, 0);
				List<RpcMessage> sent = new ArrayList<>();
				connection.onSenderReady(sent::add);
				connection.onSenderSuspended();

				ResultCallback<TestResponse> first = new ResultCallback<>();
				ResultCallback<TestResponse> second = new ResultCallback<>();
				connection.sendRequest(TestMessages.request(1), first);
				connection.sendRequest(TestMessages.request(2), second);
				assertFalse(first.called);
				assertFalse(second.called);
				assertEquals(2, connection.getOverloadedQueueSize());

				ResultCallback<TestResponse> rejected = new ResultCallback<>();
				connection.sendRequest(TestMessages.request(3), rejected);
				assertTrue(rejected.called);
				assertTrue(rejected.exception instanceof RpcOverloadException);
				assertEquals(2, connection.getOverloadedQueueSize());
				assertEquals(2, sent.size());

				connection.accept(new RpcMessage(1, new TestResponse(1, null, null)));
				assertTrue(first.called);
				assertNull(first.exception);
				assertEquals(1, first.result.id());
				assertEquals(1, connection.getOverloadedQueueSize());

				ResultCallback<TestResponse> replacement = new ResultCallback<>();
				connection.sendRequest(TestMessages.request(4), replacement);
				assertFalse(replacement.called);
				assertEquals(2, connection.getOverloadedQueueSize());

				connection.accept(new RpcMessage(2, new TestResponse(2, null, null)));
				connection.accept(new RpcMessage(3, new TestResponse(4, null, null)));
				assertTrue(second.called);
				assertNull(second.exception);
				assertEquals(2, second.result.id());
				assertTrue(replacement.called);
				assertNull(replacement.exception);
				assertEquals(4, replacement.result.id());
				assertEquals(0, connection.getOverloadedQueueSize());
			}));
		}
	}

	private static final class ResultCallback<T> implements com.zhouda.rpc.async.callback.Callback<T> {
		private boolean called;
		private @Nullable T result;
		private @Nullable Exception exception;

		@Override
		public void accept(@Nullable T result, @Nullable Exception e) {
			called = true;
			this.result = result;
			exception = e;
		}
	}
}

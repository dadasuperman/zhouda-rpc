package com.zhouda.rpc;

import com.zhouda.rpc.client.RpcClient;
import com.zhouda.rpc.eventloop.Eventloop;
import com.zhouda.rpc.client.sender.strategy.RpcStrategies;
import com.zhouda.rpc.protocol.RpcMessage;
import com.zhouda.rpc.server.RpcServer;
import com.zhouda.rpc.serializer.BinarySerializer;
import com.zhouda.rpc.serializer.SerializerFactory;
import com.zhouda.rpc.support.RpcTestSupport;
import com.zhouda.rpc.support.TestMessages;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.*;

public class RpcConfigurationTest {
	private final Eventloop eventloop = Eventloop.builder().withCurrentThread().build();

	// 特性：客户端必须同时配置请求与响应序列化器。
	@Test
	public void clientRequiresBothDirectionalSerializers() {
		assertThrows(IllegalStateException.class, () -> RpcClient.builder(eventloop)
			.withStrategy(new RpcClient.NoServersRpcStrategy())
			.build());
		assertThrows(IllegalStateException.class, () -> RpcClient.builder(eventloop)
			.withRequestsSerializer(serializer())
			.withStrategy(new RpcClient.NoServersRpcStrategy())
			.build());
		assertThrows(IllegalStateException.class, () -> RpcClient.builder(eventloop)
			.withResponsesSerializer(serializer())
			.withStrategy(new RpcClient.NoServersRpcStrategy())
			.build());
	}

	// 特性：服务端必须同时配置双向序列化器和至少一个业务处理器。
	@Test
	public void serverRequiresSerializersAndBusinessHandler() {
		assertThrows(IllegalStateException.class, () -> RpcServer.builder(eventloop)
			.withHandler(TestMessages.TestRequest.class,
				request -> com.zhouda.rpc.promise.Promise.of(new TestMessages.TestResponse(1, null, null)))
			.build());
		assertThrows(IllegalStateException.class, () -> RpcServer.builder(eventloop)
			.withMessageTypes(RpcTestSupport.MESSAGE_TYPES)
			.build());
	}

	// 特性：同一请求类型不能重复注册处理器。
	@Test
	public void rejectsDuplicateRequestHandler() {
		assertThrows(IllegalArgumentException.class, () -> RpcServer.builder(eventloop)
			.withMessageTypes(RpcTestSupport.MESSAGE_TYPES)
			.withHandler(TestMessages.TestRequest.class,
				request -> com.zhouda.rpc.promise.Promise.of(new TestMessages.TestResponse(1, null, null)))
			.withHandler(TestMessages.TestRequest.class,
				request -> com.zhouda.rpc.promise.Promise.of(new TestMessages.TestResponse(2, null, null)))
			.build());
	}

	// 特性：服务地址列表不能为空。
	@Test
	public void rejectsEmptyServerAddressList() {
		assertThrows(IllegalArgumentException.class, RpcStrategies::servers);
	}

	// 特性：Builder 在 build 后不可继续修改。
	@Test
	public void buildersCannotBeModifiedAfterBuild() {
		RpcClient.Builder client = RpcClient.builder(eventloop).withMessageTypes(RpcTestSupport.MESSAGE_TYPES);
		client.build();
		assertThrows(IllegalStateException.class, () -> client.withKeepAlive(java.time.Duration.ofSeconds(1)));

		RpcServer.Builder server = RpcTestSupport.serverBuilder(eventloop);
		server.build();
		assertThrows(IllegalStateException.class, () -> server.withAutoFlushInterval(java.time.Duration.ofSeconds(1)));
	}

	// 特性：请求和响应方向可以分别配置兼容的序列化器。
	@Test
	public void acceptsSeparateRequestAndResponseSerializers() {
		RpcClient client = RpcClient.builder(eventloop)
			.withRequestsSerializer(serializer())
			.withResponsesSerializer(serializer())
			.withStrategy(RpcStrategies.server(new InetSocketAddress("127.0.0.1", 1)))
			.build();
		assertNotNull(client.getRequestSerializer());
		assertNotNull(client.getResponseSerializer());
	}

	private BinarySerializer<RpcMessage> serializer() {
		return SerializerFactory.builder()
			.withSubclasses(RpcMessage.SUBCLASSES_ID, RpcTestSupport.MESSAGE_TYPES)
			.build()
			.create(RpcMessage.class);
	}
}

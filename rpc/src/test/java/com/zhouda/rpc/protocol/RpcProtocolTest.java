package com.zhouda.rpc.protocol;

import com.zhouda.rpc.serializer.BinarySerializer;
import com.zhouda.rpc.serializer.CorruptedDataException;
import com.zhouda.rpc.serializer.SerializerFactory;
import com.zhouda.rpc.support.RpcTestSupport;
import com.zhouda.rpc.support.TestMessages;
import org.junit.Test;

import static org.junit.Assert.*;

public class RpcProtocolTest {
	private final BinarySerializer<RpcMessage> serializer = SerializerFactory.builder()
		.withSubclasses(RpcMessage.SUBCLASSES_ID, RpcTestSupport.MESSAGE_TYPES)
		.build()
		.create(RpcMessage.class);

	// 特性：RpcMessage 保留请求 index 和具体 DTO 类型及字段。
	@Test
	public void roundTripsTypedMessageAndIndex() {
		TestMessages.TestRequest request = TestMessages.request(17);
		RpcMessage decoded = roundTrip(new RpcMessage(17, request));
		assertEquals(17, decoded.getIndex());
		assertTrue(decoded.getMessage() instanceof TestMessages.TestRequest);
		assertEquals(request, decoded.getMessage());
	}

	// 特性：协议消息体和 DTO 的可空字段均支持 null。
	@Test
	public void roundTripsNullMessageAndNullableFields() {
		RpcMessage nullMessage = roundTrip(new RpcMessage(3, null));
		assertEquals(3, nullMessage.getIndex());
		assertNull(nullMessage.getMessage());

		TestMessages.TestRequest request = new TestMessages.TestRequest(4, null, 0, false, null);
		assertEquals(request, roundTrip(new RpcMessage(4, request)).getMessage());
	}

	// 特性：CLOSE、PING、PONG 控制消息使用固定 index=0 并可独立往返。
	@Test
	public void roundTripsAllControlMessages() {
		for (RpcControlMessage control : RpcControlMessage.values()) {
			RpcMessage decoded = roundTrip(new RpcMessage(control));
			assertEquals(0, decoded.getIndex());
			assertSame(control, decoded.getMessage());
		}
	}

	// 特性：远程异常保留原始异常类、消息和组合后的描述。
	@Test
	public void roundTripsRemoteExceptionDetails() {
		RpcRemoteException exception = new RpcRemoteException(new IllegalStateException("disk full"));
		RpcRemoteException decoded = (RpcRemoteException) roundTrip(new RpcMessage(9, exception)).getMessage();
		assertEquals(IllegalStateException.class.getName(), decoded.getCauseClassName());
		assertEquals("disk full", decoded.getCauseMessage());
		assertEquals("java.lang.IllegalStateException: disk full", decoded.getMessage());
	}

	// 特性：没有 cause message 的远程异常不会丢失空值语义。
	@Test
	public void roundTripsRemoteExceptionWithNullCauseMessage() {
		RpcRemoteException exception = new RpcRemoteException(new IllegalStateException());
		RpcRemoteException decoded = (RpcRemoteException) roundTrip(new RpcMessage(1, exception)).getMessage();
		assertEquals(IllegalStateException.class.getName(), decoded.getCauseClassName());
		assertNull(decoded.getCauseMessage());
	}

	// 特性：未知消息子类标签被识别为损坏数据。
	@Test
	public void rejectsUnknownMessageSubclass() {
		byte[] bytes = new byte[128];
		int end = serializer.encode(bytes, 0, new RpcMessage(1, TestMessages.request(1)));
		bytes[4] = (byte) 127;
		assertThrows(CorruptedDataException.class, () -> serializer.decode(bytes, 0));
		assertTrue(end > 4);
	}

	// 特性：截断的协议数据会被拒绝，不会构造出消息对象。
	@Test
	public void rejectsTruncatedData() {
		assertThrows(RuntimeException.class, () -> serializer.decode(new byte[0], 0));
	}

	private RpcMessage roundTrip(RpcMessage message) {
		byte[] bytes = new byte[1024];
		int end = serializer.encode(bytes, 0, message);
		return serializer.decode(bytes, 0);
	}
}

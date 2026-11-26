package com.zhouda.rpc.support;

import com.zhouda.rpc.protocol.RpcMandatoryData;
import com.zhouda.rpc.serializer.annotations.SerializeNullable;
import com.zhouda.rpc.serializer.annotations.SerializeRecord;

import java.util.List;

public final class TestMessages {
	private TestMessages() {}

	@SerializeRecord
	public record TestRequest(
		int id,
		@SerializeNullable String payload,
		int delayMillis,
		boolean fail,
		@SerializeNullable List<String> labels
	) implements RpcMandatoryData {}

	@SerializeRecord
	public record TestResponse(
		int id,
		@SerializeNullable String payload,
		@SerializeNullable List<String> labels
	) {}

	@SerializeRecord
	public record AlternateRequest(int id) implements RpcMandatoryData {}

	@SerializeRecord
	public record AlternateResponse(int id) {}

	@SerializeRecord
	public record NormalRequest(int id) {}

	public static TestRequest request(int id) {
		return new TestRequest(id, "payload-" + id, 0, false, List.of("a", "b"));
	}
}

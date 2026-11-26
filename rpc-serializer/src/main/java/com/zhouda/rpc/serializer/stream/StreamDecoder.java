package com.zhouda.rpc.serializer.stream;

import com.zhouda.rpc.serializer.BinaryInput;

import java.io.IOException;

public interface StreamDecoder<T> {
	T decode(StreamInput input) throws IOException;

	default T fromByteArray(byte[] byteArray) throws IOException {
		BinaryInput binaryInput = new BinaryInput(byteArray);
		try (StreamInput streamInput = StreamInput.create(binaryInput)) {
			return decode(streamInput);
		}
	}
}

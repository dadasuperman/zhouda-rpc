package com.zhouda.rpc.serializer;

public class CorruptedDataException extends RuntimeException {

	public CorruptedDataException() {
	}

	public CorruptedDataException(String message) {
		super(message);
	}
}

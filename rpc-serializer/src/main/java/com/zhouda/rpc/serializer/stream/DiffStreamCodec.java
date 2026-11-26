package com.zhouda.rpc.serializer.stream;

public interface DiffStreamCodec<T> extends StreamCodec<T>, DiffStreamEncoder<T>, DiffStreamDecoder<T> {
}

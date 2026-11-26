package com.zhouda.rpc.csp.binary.codec;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufPool;
import com.zhouda.rpc.common.annotation.StaticFactories;
import com.zhouda.rpc.csp.binary.codec.impl.OfDelimiter;
import com.zhouda.rpc.csp.binary.codec.impl.OfStreamCodecs;
import com.zhouda.rpc.csp.binary.decoder.ByteBufsDecoder;
import com.zhouda.rpc.csp.binary.decoder.ByteBufsDecoders;
import com.zhouda.rpc.serializer.stream.StreamCodec;
import com.zhouda.rpc.serializer.stream.StreamDecoder;
import com.zhouda.rpc.serializer.stream.StreamEncoder;

import java.util.function.UnaryOperator;

@StaticFactories(ByteBufsCodec.class)
public class ByteBufsCodecs {
	public static ByteBufsCodec<ByteBuf, ByteBuf> ofDelimiter(ByteBufsDecoder<ByteBuf> delimiterIn, UnaryOperator<ByteBuf> delimiterOut) {
		return new OfDelimiter(delimiterOut, delimiterIn);
	}

	public static ByteBufsCodec<ByteBuf, ByteBuf> nullTerminated() {
		return ByteBufsCodecs
			.ofDelimiter(
				ByteBufsDecoders.ofNullTerminatedBytes(),
				buf -> {
					ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
					buf1.put((byte) 0);
					return buf1;
				});
	}

	public static <I, O> ByteBufsCodec<I, O> ofStreamCodecs(StreamDecoder<I> decoder, StreamEncoder<O> encoder) {
		return new OfStreamCodecs<>(decoder, encoder);
	}

	public static <T> ByteBufsCodec<T, T> ofStreamCodecs(StreamCodec<T> codec) {
		return ofStreamCodecs(codec, codec);
	}

}

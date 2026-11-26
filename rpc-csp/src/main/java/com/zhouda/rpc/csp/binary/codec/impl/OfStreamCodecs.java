package com.zhouda.rpc.csp.binary.codec.impl;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufs;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.exception.MalformedDataException;
import com.zhouda.rpc.csp.binary.codec.ByteBufsCodec;
import com.zhouda.rpc.serializer.BinaryInput;
import com.zhouda.rpc.serializer.stream.EOSException;
import com.zhouda.rpc.serializer.stream.StreamDecoder;
import com.zhouda.rpc.serializer.stream.StreamEncoder;
import com.zhouda.rpc.serializer.stream.StreamInput;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

@ExposedInternals
public final class OfStreamCodecs<I, O> implements ByteBufsCodec<I, O> {
	public final StreamDecoder<I> input;
	public final StreamEncoder<O> output;

	public OfStreamCodecs(StreamDecoder<I> input, StreamEncoder<O> output) {
		this.input = input;
		this.output = output;
	}

	@Override
	public ByteBuf encode(O item) {
		byte[] bytes = output.toByteArray(item);
		return ByteBuf.wrapForReading(bytes);
	}

	@Override
	public @Nullable I tryDecode(ByteBufs bufs) throws MalformedDataException {
		ByteBuf buf = bufs.takeRemaining();

		BinaryInput binaryInput = new BinaryInput(buf.getArray());
		try (StreamInput streamInput = StreamInput.create(binaryInput)) {
			I decode;
			try {
				decode = input.decode(streamInput);
			} catch (EOSException e) {
				bufs.add(buf);
				return null;
			}
			buf.moveHead(binaryInput.pos());
			bufs.add(buf);
			return decode;
		} catch (IOException e) {
			throw new MalformedDataException(e);
		}
	}
}

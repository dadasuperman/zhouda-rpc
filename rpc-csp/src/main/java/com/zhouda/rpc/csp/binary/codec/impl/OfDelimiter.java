package com.zhouda.rpc.csp.binary.codec.impl;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufs;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.exception.MalformedDataException;
import com.zhouda.rpc.csp.binary.codec.ByteBufsCodec;
import com.zhouda.rpc.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

import java.util.function.UnaryOperator;

@ExposedInternals
public final class OfDelimiter implements ByteBufsCodec<ByteBuf, ByteBuf> {
	public final UnaryOperator<ByteBuf> delimiterOut;
	public final ByteBufsDecoder<ByteBuf> delimiterIn;

	public OfDelimiter(UnaryOperator<ByteBuf> delimiterOut, ByteBufsDecoder<ByteBuf> delimiterIn) {
		this.delimiterOut = delimiterOut;
		this.delimiterIn = delimiterIn;
	}

	@Override
	public ByteBuf encode(ByteBuf buf) {
		return delimiterOut.apply(buf);
	}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		return delimiterIn.tryDecode(bufs);
	}
}

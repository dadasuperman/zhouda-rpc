package com.zhouda.rpc.csp.binary.decoder.impl;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufs;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.exception.InvalidSizeException;
import com.zhouda.rpc.common.exception.MalformedDataException;
import com.zhouda.rpc.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public class OfShortSizePrefixed implements ByteBufsDecoder<ByteBuf> {
	public final int maxSize;

	public OfShortSizePrefixed(int maxSize) {this.maxSize = maxSize;}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		if (!bufs.hasRemainingBytes(2)) return null;
		int size =
			(bufs.peekByte(0) & 0xFF) << 8 |
			(bufs.peekByte(1) & 0xFF);
		if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");
		if (!bufs.hasRemainingBytes(2 + size)) return null;
		bufs.skip(2);
		return bufs.takeExactSize(size);
	}
}

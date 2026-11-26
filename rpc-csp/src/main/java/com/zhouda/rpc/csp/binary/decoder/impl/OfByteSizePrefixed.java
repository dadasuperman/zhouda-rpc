package com.zhouda.rpc.csp.binary.decoder.impl;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufs;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.exception.InvalidSizeException;
import com.zhouda.rpc.common.exception.MalformedDataException;
import com.zhouda.rpc.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public class OfByteSizePrefixed implements ByteBufsDecoder<ByteBuf> {
	public final int maxSize;

	public OfByteSizePrefixed(int maxSize) {this.maxSize = maxSize;}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		if (!bufs.hasRemaining()) return null;
		int size = bufs.peekByte() & 0xFF;
		if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");
		if (!bufs.hasRemainingBytes(1 + size)) return null;
		bufs.skip(1);
		return bufs.takeExactSize(size);
	}
}

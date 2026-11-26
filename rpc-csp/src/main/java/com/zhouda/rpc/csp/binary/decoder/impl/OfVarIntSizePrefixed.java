package com.zhouda.rpc.csp.binary.decoder.impl;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufs;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.exception.InvalidSizeException;
import com.zhouda.rpc.common.exception.MalformedDataException;
import com.zhouda.rpc.csp.binary.Utils;
import com.zhouda.rpc.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class OfVarIntSizePrefixed implements ByteBufsDecoder<ByteBuf> {
	public final Utils.VarIntByteScanner scanner;
	public final int maxSize;

	public OfVarIntSizePrefixed(int maxSize) {
		this.maxSize = maxSize;
		scanner = new Utils.VarIntByteScanner();
	}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		int bytes = bufs.scanBytes(scanner);

		if (bytes == 0) return null;

		int size = scanner.getResult();

		if (size < 0)
			throw new InvalidSizeException("Invalid size of bytes to be read, should be greater than 0");
		if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");

		if (!bufs.hasRemainingBytes(bytes + size)) return null;
		bufs.skip(bytes);
		return bufs.takeExactSize(size);
	}
}

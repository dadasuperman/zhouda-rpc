package com.zhouda.rpc.csp.binary.decoder.impl;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufs;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.exception.MalformedDataException;
import com.zhouda.rpc.csp.binary.decoder.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public class OfByteTerminated implements ByteBufsDecoder<ByteBuf> {
	public final byte terminator;
	public final int maxSize;

	public OfByteTerminated(byte terminator, int maxSize) {
		this.terminator = terminator;
		this.maxSize = maxSize;
	}

	@Override
	public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
		int bytes = bufs.scanBytes((index, nextByte) -> {
			if (nextByte == terminator) {
				return true;
			}
			if (index == maxSize - 1) {
				throw new MalformedDataException("No terminator byte is found in " + maxSize + " bytes");
			}
			return false;
		});

		if (bytes == 0) return null;

		ByteBuf buf = bufs.takeExactSize(bytes);
		buf.moveTail(-1);
		return buf;
	}
}

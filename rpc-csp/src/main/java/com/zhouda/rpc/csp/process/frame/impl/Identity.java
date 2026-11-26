package com.zhouda.rpc.csp.process.frame.impl;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufs;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.process.frame.BlockDecoder;
import com.zhouda.rpc.csp.process.frame.BlockEncoder;
import com.zhouda.rpc.csp.process.frame.FrameFormat;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class Identity implements FrameFormat {
	@Override
	public BlockEncoder createEncoder() {
		return new BlockEncoder() {
			@Override
			public ByteBuf encode(ByteBuf inputBuf) {
				return inputBuf.slice();
			}

			@Override
			public void reset() {
			}

			@Override
			public ByteBuf encodeEndOfStreamBlock() {
				return ByteBuf.empty();
			}
		};
	}

	@Override
	public BlockDecoder createDecoder() {
		return new BlockDecoder() {

			@Override
			public @Nullable ByteBuf decode(ByteBufs bufs) {
				return bufs.hasRemaining() ? bufs.takeRemaining() : null;
			}

			@Override
			public void reset() {
			}

			@Override
			public boolean ignoreMissingEndOfStreamBlock() {
				return true;
			}
		};
	}
}

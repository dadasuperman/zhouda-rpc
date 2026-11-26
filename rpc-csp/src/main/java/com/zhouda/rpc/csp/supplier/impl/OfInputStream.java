package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufPool;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.promise.Promise;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executor;

@ExposedInternals
public final class OfInputStream extends AbstractChannelSupplier<ByteBuf> {
	public final Executor executor;
	public final int bufSize;
	public final InputStream inputStream;

	public OfInputStream(Executor executor, int bufSize, InputStream inputStream) {
		this.executor = executor;
		this.bufSize = bufSize;
		this.inputStream = inputStream;
	}

	@Override
	protected Promise<ByteBuf> doGet() {
		return Promise.ofBlocking(executor, () -> {
			ByteBuf buf = ByteBufPool.allocate(bufSize);
			int readBytes;
			try {
				readBytes = inputStream.read(buf.array(), 0, bufSize);
			} catch (IOException e) {
				buf.recycle();
				throw e;
			}
			if (readBytes != -1) {
				buf.moveTail(readBytes);
				return buf;
			} else {
				buf.recycle();
				return null;
			}
		});
	}

	@Override
	protected void onClosed(Exception e) {
		executor.execute(() -> {
			try {
				inputStream.close();
			} catch (IOException ignored) {
			}
		});
	}
}

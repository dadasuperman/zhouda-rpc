package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.async.function.AsyncSupplier;
import com.zhouda.rpc.async.process.AsyncCloseable;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.promise.Promise;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class OfAsyncSupplier<T> extends AbstractChannelSupplier<T> {
	public final AsyncSupplier<T> supplier;

	public OfAsyncSupplier(AsyncSupplier<T> supplier, @Nullable AsyncCloseable closeable) {
		super(closeable);
		this.supplier = supplier;
	}

	@Override
	protected Promise<T> doGet() {
		return supplier.get();
	}
}

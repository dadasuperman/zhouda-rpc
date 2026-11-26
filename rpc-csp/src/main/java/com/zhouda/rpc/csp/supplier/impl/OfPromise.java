package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.csp.supplier.ChannelSupplier;
import com.zhouda.rpc.promise.Promise;

@ExposedInternals
public final class OfPromise<T> extends AbstractChannelSupplier<T> {
	public final Promise<? extends ChannelSupplier<T>> promise;

	public ChannelSupplier<T> supplier;

	public OfPromise(Promise<? extends ChannelSupplier<T>> promise) {
		this.promise = promise;
	}

	@Override
	protected Promise<T> doGet() {
		if (supplier != null) return supplier.get();
		return promise.then(supplier -> {
			this.supplier = supplier;
			return supplier.get();
		});
	}

	@Override
	protected void onClosed(Exception e) {
		promise.whenResult(supplier -> supplier.closeEx(e));
	}
}

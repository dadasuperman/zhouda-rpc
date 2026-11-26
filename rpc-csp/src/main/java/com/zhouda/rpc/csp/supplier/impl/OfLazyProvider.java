package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.csp.supplier.ChannelSupplier;
import com.zhouda.rpc.promise.Promise;

import java.util.function.Supplier;

@ExposedInternals
public final class OfLazyProvider<T> extends AbstractChannelSupplier<T> {
	public final Supplier<? extends ChannelSupplier<T>> provider;

	public ChannelSupplier<T> supplier;

	public OfLazyProvider(Supplier<? extends ChannelSupplier<T>> provider) {
		this.provider = provider;
	}

	@Override
	protected Promise<T> doGet() {
		if (supplier == null) supplier = provider.get();
		return supplier.get();
	}

	@Override
	protected void onClosed(Exception e) {
		if (supplier != null) {
			supplier.closeEx(e);
		}
	}
}

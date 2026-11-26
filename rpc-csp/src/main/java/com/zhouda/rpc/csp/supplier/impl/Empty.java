package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.promise.Promise;

@ExposedInternals
public final class Empty<T> extends AbstractChannelSupplier<T> {
	@Override
	protected Promise<T> doGet() {
		return Promise.of(null);
	}
}

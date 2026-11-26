package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.promise.Promise;

@ExposedInternals
public final class OfException<T> extends AbstractChannelSupplier<T> {
	public final Exception exception;

	public OfException(Exception exception) {
		this.exception = exception;
	}

	@Override
	protected Promise<T> doGet() {
		return Promise.ofException(exception);
	}
}

package com.zhouda.rpc.datastream.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.datastream.supplier.AbstractStreamSupplier;

import static com.zhouda.rpc.common.Utils.nullify;

@ExposedInternals
public final class ClosingWithError<T> extends AbstractStreamSupplier<T> {
	public Exception error;

	public ClosingWithError(Exception e) {
		this.error = e;
	}

	@Override
	protected void onInit() {
		error = nullify(error, this::closeEx);
	}
}

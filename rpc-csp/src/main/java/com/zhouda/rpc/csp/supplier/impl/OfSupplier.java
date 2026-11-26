package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.async.process.AsyncCloseable;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.function.SupplierEx;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.promise.Promise;
import org.jetbrains.annotations.Nullable;

import static com.zhouda.rpc.common.exception.FatalErrorHandler.handleError;

@ExposedInternals
public final class OfSupplier<T> extends AbstractChannelSupplier<T> {
	public final SupplierEx<T> supplier;

	public OfSupplier(SupplierEx<T> supplier, @Nullable AsyncCloseable closeable) {
		super(closeable);
		this.supplier = supplier;
	}

	@Override
	protected Promise<T> doGet() {
		try {
			return Promise.of(supplier.get());
		} catch (Exception e) {
			handleError(e, supplier);
			return Promise.ofException(e);
		}
	}
}

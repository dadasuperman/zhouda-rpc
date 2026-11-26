package com.zhouda.rpc.datastream.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.function.SupplierEx;
import com.zhouda.rpc.datastream.supplier.AbstractStreamSupplier;

import static com.zhouda.rpc.common.exception.FatalErrorHandler.handleError;

@ExposedInternals
public class OfSupplier<T> extends AbstractStreamSupplier<T> {
	public final SupplierEx<T> supplier;

	public OfSupplier(SupplierEx<T> supplier) {this.supplier = supplier;}

	@Override
	protected void onResumed() {
		while (isReady()) {
			T t;
			try {
				t = supplier.get();
			} catch (Exception ex) {
				handleError(ex, supplier);
				closeEx(ex);
				break;
			}
			if (t != null) {
				send(t);
			} else {
				sendEndOfStream();
				break;
			}
		}
	}
}

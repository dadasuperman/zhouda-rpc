package com.zhouda.rpc.datastream.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.datastream.supplier.AbstractStreamSupplier;

@ExposedInternals
public final class Empty<T> extends AbstractStreamSupplier<T> {
	@Override
	protected void onInit() {
		sendEndOfStream();
	}
}

package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.recycle.Recyclers;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.promise.Promise;

import static com.zhouda.rpc.common.Utils.nullify;

@ExposedInternals
public final class OfValue<T> extends AbstractChannelSupplier<T> {
	public T item;

	public OfValue(T item) {
		this.item = item;
	}

	public T getValue() {
		return item;
	}

	public T takeValue() {
		T item = this.item;
		this.item = null;
		return item;
	}

	@Override
	protected Promise<T> doGet() {
		T item = takeValue();
		return Promise.of(item);
	}

	@Override
	protected void onCleanup() {
		item = nullify(item, Recyclers::recycle);
	}
}

package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.recycle.Recyclers;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.promise.Promise;

import java.util.Iterator;

@ExposedInternals
public final class OfIterator<T> extends AbstractChannelSupplier<T> {
	public final Iterator<? extends T> iterator;
	public final boolean ownership;

	public OfIterator(Iterator<? extends T> iterator, boolean ownership) {
		this.iterator = iterator;
		this.ownership = ownership;
	}

	@Override
	protected Promise<T> doGet() {
		return Promise.of(iterator.hasNext() ? iterator.next() : null);
	}

	@Override
	protected void onCleanup() {
		if (ownership) {
			iterator.forEachRemaining(Recyclers::recycle);
		} else {
			Recyclers.recycle(iterator);
		}
	}
}

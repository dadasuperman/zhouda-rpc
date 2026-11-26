package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.recycle.Recyclers;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.csp.supplier.ChannelSupplier;
import com.zhouda.rpc.csp.supplier.ChannelSuppliers;
import com.zhouda.rpc.promise.Promise;

import java.util.Iterator;

@ExposedInternals
public final class Concat<T> extends AbstractChannelSupplier<T> {
	public final Iterator<? extends ChannelSupplier<? extends T>> iterator;
	public final boolean ownership;

	public ChannelSupplier<? extends T> current = ChannelSuppliers.empty();

	public Concat(Iterator<? extends ChannelSupplier<? extends T>> iterator, boolean ownership) {
		this.iterator = iterator;
		this.ownership = ownership;
	}

	@Override
	protected Promise<T> doGet() {
		return current.get()
			.then((value, e) -> {
				if (e == null) {
					if (value != null) {
						return Promise.of(value);
					} else {
						if (iterator.hasNext()) {
							current = iterator.next();
							return get();
						} else {
							return Promise.of(null);
						}
					}
				} else {
					closeEx(e);
					return Promise.ofException(e);
				}
			});
	}

	@Override
	protected void onClosed(Exception e) {
		current.closeEx(e);
		if (ownership) {
			iterator.forEachRemaining(Recyclers::recycle);
		} else {
			Recyclers.recycle(iterator);
		}
	}
}

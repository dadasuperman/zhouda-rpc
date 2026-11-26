package com.zhouda.rpc.datastream.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.supplier.ChannelSupplier;
import com.zhouda.rpc.datastream.supplier.AbstractStreamSupplier;

@ExposedInternals
public final class OfChannelSupplier<T> extends AbstractStreamSupplier<T> {
	public final ChannelSupplier<T> supplier;

	public OfChannelSupplier(ChannelSupplier<T> supplier) {
		this.supplier = supplier;
	}

	@Override
	protected void onResumed() {
		asyncBegin();
		supplier.get()
			.subscribe((item, e) -> {
				if (e == null) {
					if (item != null) {
						send(item);
						asyncResume();
					} else {
						sendEndOfStream();
					}
				} else {
					closeEx(e);
				}
			});
	}

	@Override
	protected void onError(Exception e) {
		supplier.closeEx(e);
	}
}

package com.zhouda.rpc.csp.consumer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.recycle.Recyclers;
import com.zhouda.rpc.csp.consumer.AbstractChannelConsumer;
import com.zhouda.rpc.promise.Promise;

@ExposedInternals
public final class OfException<T> extends AbstractChannelConsumer<T> {
	public final Exception exception;

	public OfException(Exception exception) {
		this.exception = exception;
	}

	@Override
	protected Promise<Void> doAccept(T value) {
		Recyclers.recycle(value);
		return Promise.ofException(exception);
	}
}

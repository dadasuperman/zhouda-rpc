package com.zhouda.rpc.datastream.consumer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.function.ConsumerEx;
import com.zhouda.rpc.datastream.consumer.AbstractStreamConsumer;

import static com.zhouda.rpc.common.exception.FatalErrorHandler.handleError;

@ExposedInternals
public final class OfConsumer<T> extends AbstractStreamConsumer<T> {
	public final ConsumerEx<T> consumer;

	public OfConsumer(ConsumerEx<T> consumer) {
		this.consumer = consumer;
	}

	@Override
	protected void onStarted() {
		resume(item -> {
			try {
				consumer.accept(item);
			} catch (Exception ex) {
				handleError(ex, this);
				closeEx(ex);
			}
		});
	}

	@Override
	protected void onEndOfStream() {
		acknowledge();
	}
}

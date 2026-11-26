package com.zhouda.rpc.csp.consumer.impl;

import com.zhouda.rpc.async.function.AsyncConsumer;
import com.zhouda.rpc.async.process.AsyncCloseable;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.consumer.AbstractChannelConsumer;
import com.zhouda.rpc.promise.Promise;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class OfAsyncConsumer<T> extends AbstractChannelConsumer<T> {
	public final AsyncConsumer<T> consumer;

	public OfAsyncConsumer(AsyncConsumer<T> consumer, @Nullable AsyncCloseable closeable) {
		super(closeable);
		this.consumer = consumer;
	}

	@Override
	protected Promise<Void> doAccept(T value) {
		if (value != null) {
			return this.consumer.accept(value);
		}
		return Promise.complete();
	}
}

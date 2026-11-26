package com.zhouda.rpc.csp.consumer.impl;

import com.zhouda.rpc.async.process.AsyncCloseable;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.function.ConsumerEx;
import com.zhouda.rpc.csp.consumer.AbstractChannelConsumer;
import com.zhouda.rpc.promise.Promise;
import org.jetbrains.annotations.Nullable;

import static com.zhouda.rpc.common.exception.FatalErrorHandler.handleError;

@ExposedInternals
public final class OfConsumer<T> extends AbstractChannelConsumer<T> {
	public final ConsumerEx<T> consumer;

	public OfConsumer(ConsumerEx<T> consumer, @Nullable AsyncCloseable closeable) {
		super(closeable);
		this.consumer = consumer;
	}

	@Override
	protected Promise<Void> doAccept(T value) {
		if (value != null) {
			try {
				consumer.accept(value);
			} catch (Exception e) {
				handleError(e, consumer);
				return Promise.ofException(e);
			}
			return Promise.complete();
		}
		return Promise.complete();
	}
}

package com.zhouda.rpc.csp.consumer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.common.recycle.Recyclers;
import com.zhouda.rpc.csp.consumer.AbstractChannelConsumer;
import com.zhouda.rpc.csp.consumer.ChannelConsumer;
import com.zhouda.rpc.promise.Promise;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class OfPromise<T> extends AbstractChannelConsumer<T> {
	public final Promise<? extends ChannelConsumer<T>> promise;

	public @Nullable ChannelConsumer<T> consumer;

	public OfPromise(Promise<? extends ChannelConsumer<T>> promise) {
		this.promise = promise;
	}

	@Override
	protected Promise<Void> doAccept(T value) {
		if (consumer != null) return consumer.accept(value);
		return promise.then(
			consumer -> {
				this.consumer = consumer;
				return consumer.accept(value);
			},
			e -> {
				Recyclers.recycle(value);
				return Promise.ofException(e);
			});
	}

	@Override
	protected void onClosed(Exception e) {
		promise.whenResult(supplier -> supplier.closeEx(e));
	}
}

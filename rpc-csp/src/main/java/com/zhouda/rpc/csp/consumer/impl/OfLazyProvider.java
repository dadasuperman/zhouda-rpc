package com.zhouda.rpc.csp.consumer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.consumer.AbstractChannelConsumer;
import com.zhouda.rpc.csp.consumer.ChannelConsumer;
import com.zhouda.rpc.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

@ExposedInternals
public final class OfLazyProvider<T> extends AbstractChannelConsumer<T> {
	public final Supplier<? extends ChannelConsumer<T>> provider;

	public @Nullable ChannelConsumer<T> consumer;

	public OfLazyProvider(Supplier<? extends ChannelConsumer<T>> provider) {
		this.provider = provider;
	}

	@Override
	protected Promise<Void> doAccept(@Nullable T value) {
		if (consumer == null) consumer = provider.get();
		return consumer.accept(value);
	}

	@Override
	protected void onClosed(Exception e) {
		if (consumer != null) {
			consumer.closeEx(e);
		}
	}
}

package com.zhouda.rpc.csp.consumer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.consumer.AbstractChannelConsumer;
import com.zhouda.rpc.csp.consumer.ChannelConsumer;
import com.zhouda.rpc.promise.Promise;
import com.zhouda.rpc.promise.SettablePromise;
import com.zhouda.rpc.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

@ExposedInternals
public final class OfAnotherReactor<T> extends AbstractChannelConsumer<T> {
	public final Reactor anotherReactor;
	public final ChannelConsumer<T> anotherReactorConsumer;

	public OfAnotherReactor(Reactor anotherReactor, ChannelConsumer<T> anotherReactorConsumer) {
		this.anotherReactor = anotherReactor;
		this.anotherReactorConsumer = anotherReactorConsumer;
	}

	@Override
	protected Promise<Void> doAccept(@Nullable T value) {
		SettablePromise<Void> promise = new SettablePromise<>();
		reactor.startExternalTask();
		anotherReactor.execute(() ->
			anotherReactorConsumer.accept(value)
				.subscribe((v, e) -> {
					reactor.execute(() -> promise.set(v, e));
					reactor.completeExternalTask();
				}));
		return promise;
	}

	@Override
	protected void onClosed(Exception e) {
		reactor.startExternalTask();
		anotherReactor.execute(() -> {
			anotherReactorConsumer.closeEx(e);
			reactor.completeExternalTask();
		});
	}
}

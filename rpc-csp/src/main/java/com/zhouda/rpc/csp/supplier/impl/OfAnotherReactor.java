package com.zhouda.rpc.csp.supplier.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.supplier.AbstractChannelSupplier;
import com.zhouda.rpc.csp.supplier.ChannelSupplier;
import com.zhouda.rpc.promise.Promise;
import com.zhouda.rpc.promise.SettablePromise;
import com.zhouda.rpc.reactor.Reactor;

@ExposedInternals
public final class OfAnotherReactor<T> extends AbstractChannelSupplier<T> {
	public final Reactor anotherReactor;
	public final ChannelSupplier<T> anotherReactorSupplier;

	public OfAnotherReactor(Reactor anotherReactor, ChannelSupplier<T> anotherReactorSupplier) {
		this.anotherReactor = anotherReactor;
		this.anotherReactorSupplier = anotherReactorSupplier;
	}

	@Override
	protected Promise<T> doGet() {
		SettablePromise<T> promise = new SettablePromise<>();
		reactor.startExternalTask();
		anotherReactor.execute(() ->
			anotherReactorSupplier.get()
				.subscribe((item, e) -> {
					reactor.execute(() -> promise.set(item, e));
					reactor.completeExternalTask();
				}));
		return promise;
	}

	@Override
	protected void onClosed(Exception e) {
		reactor.startExternalTask();
		anotherReactor.execute(() -> {
			anotherReactorSupplier.closeEx(e);
			reactor.completeExternalTask();
		});
	}
}

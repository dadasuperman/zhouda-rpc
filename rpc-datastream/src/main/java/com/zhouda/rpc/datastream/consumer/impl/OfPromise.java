package com.zhouda.rpc.datastream.consumer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.datastream.consumer.AbstractStreamConsumer;
import com.zhouda.rpc.datastream.consumer.StreamConsumer;
import com.zhouda.rpc.datastream.supplier.AbstractStreamSupplier;
import com.zhouda.rpc.promise.Promise;

@ExposedInternals
public final class OfPromise<T> extends AbstractStreamConsumer<T> {
	public final InternalSupplier internalSupplier = new InternalSupplier();

	public Promise<? extends StreamConsumer<T>> promise;

	public class InternalSupplier extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed() {
			OfPromise.this.resume(getDataAcceptor());
		}

		@Override
		protected void onSuspended() {
			OfPromise.this.suspend();
		}
	}

	public OfPromise(Promise<? extends StreamConsumer<T>> promise) {
		this.promise = promise;
	}

	@Override
	protected void onInit() {
		promise
			.whenResult(consumer -> {
				consumer.getAcknowledgement()
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
				this.getAcknowledgement()
					.whenException(consumer::closeEx);
				internalSupplier.streamTo(consumer);
			})
			.whenException(this::closeEx);
	}

	@Override
	protected void onEndOfStream() {
		internalSupplier.sendEndOfStream();
	}

	@Override
	protected void onCleanup() {
		promise = null;
	}
}

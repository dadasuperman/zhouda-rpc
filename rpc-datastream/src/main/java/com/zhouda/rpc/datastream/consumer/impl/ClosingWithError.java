package com.zhouda.rpc.datastream.consumer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.datastream.consumer.AbstractStreamConsumer;

import static com.zhouda.rpc.common.Utils.nullify;

@ExposedInternals
public final class ClosingWithError<T> extends AbstractStreamConsumer<T> {
	public Exception error;

	public ClosingWithError(Exception e) {
		this.error = e;
	}

	@Override
	protected void onInit() {
		error = nullify(error, this::closeEx);
	}
}

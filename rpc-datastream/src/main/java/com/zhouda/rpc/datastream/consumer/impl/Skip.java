package com.zhouda.rpc.datastream.consumer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.datastream.consumer.AbstractStreamConsumer;

@ExposedInternals
public final class Skip<T> extends AbstractStreamConsumer<T> {
	@Override
	protected void onStarted() {
		resume(item -> {});
	}

	@Override
	protected void onEndOfStream() {
		acknowledge();
	}
}

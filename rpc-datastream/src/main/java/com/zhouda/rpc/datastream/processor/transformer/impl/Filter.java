package com.zhouda.rpc.datastream.processor.transformer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.datastream.processor.transformer.AbstractStreamTransformer;
import com.zhouda.rpc.datastream.supplier.StreamDataAcceptor;

import java.util.function.Predicate;

@ExposedInternals
public final class Filter<T> extends AbstractStreamTransformer<T, T> {
	public final Predicate<T> predicate;

	public Filter(Predicate<T> predicate) {this.predicate = predicate;}

	@Override
	protected StreamDataAcceptor<T> onResumed(StreamDataAcceptor<T> output) {
		return item -> {if (predicate.test(item)) output.accept(item);};
	}
}

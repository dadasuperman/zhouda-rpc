package com.zhouda.rpc.datastream.processor.transformer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.datastream.processor.transformer.AbstractStreamTransformer;
import com.zhouda.rpc.datastream.supplier.StreamDataAcceptor;

import java.util.function.Function;

@ExposedInternals
public final class Mapper<I, O> extends AbstractStreamTransformer<I, O> {
	public final Function<I, O> mapFn;

	public Mapper(Function<I, O> mapFn) {this.mapFn = mapFn;}

	@Override
	protected StreamDataAcceptor<I> onResumed(StreamDataAcceptor<O> output) {
		return item -> output.accept(mapFn.apply(item));
	}
}

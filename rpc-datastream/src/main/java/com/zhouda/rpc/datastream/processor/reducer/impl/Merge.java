package com.zhouda.rpc.datastream.processor.reducer.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.datastream.processor.reducer.Reducer;
import com.zhouda.rpc.datastream.supplier.StreamDataAcceptor;

/**
 * Represent a reducer which streams received items sorted by keys
 *
 * @param <K> type of keys
 * @param <T> type of input and output data
 */
@ExposedInternals
public final class Merge<K, T> implements Reducer<K, T, T, Void> {
	@Override
	public Void onFirstItem(StreamDataAcceptor<T> stream, K key, T firstValue) {
		stream.accept(firstValue);
		return null;
	}

	@Override
	public Void onNextItem(StreamDataAcceptor<T> stream, K key, T nextValue, Void accumulator) {
		stream.accept(nextValue);
		return null;
	}

	@Override
	public void onComplete(StreamDataAcceptor<T> stream, K key, Void accumulator) {
	}
}

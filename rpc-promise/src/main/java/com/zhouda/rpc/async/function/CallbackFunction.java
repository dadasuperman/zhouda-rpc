package com.zhouda.rpc.async.function;

import com.zhouda.rpc.promise.SettableCallback;

@FunctionalInterface
public interface CallbackFunction<T, R> extends CallbackFunctionEx<T, R> {
	@Override
	void apply(T t, SettableCallback<R> cb);
}

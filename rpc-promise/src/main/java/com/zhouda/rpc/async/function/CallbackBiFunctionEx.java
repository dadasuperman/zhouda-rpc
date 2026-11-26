package com.zhouda.rpc.async.function;

import com.zhouda.rpc.promise.SettableCallback;

@FunctionalInterface
public interface CallbackBiFunctionEx<T, U, R> {
	void apply(T t, U u, SettableCallback<R> cb) throws Exception;
}

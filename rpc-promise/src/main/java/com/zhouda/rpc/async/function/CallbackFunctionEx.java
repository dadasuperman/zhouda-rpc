package com.zhouda.rpc.async.function;

import com.zhouda.rpc.promise.SettableCallback;

@FunctionalInterface
public interface CallbackFunctionEx<T, R> {
	void apply(T t, SettableCallback<R> cb) throws Exception;
}

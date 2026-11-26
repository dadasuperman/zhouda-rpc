package com.zhouda.rpc.async.function;

import com.zhouda.rpc.promise.SettableCallback;

@FunctionalInterface
public interface CallbackSupplierEx<R> {
	void get(SettableCallback<R> cb) throws Exception;
}

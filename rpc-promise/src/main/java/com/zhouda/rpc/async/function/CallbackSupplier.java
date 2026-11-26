package com.zhouda.rpc.async.function;

import com.zhouda.rpc.promise.SettableCallback;

@FunctionalInterface
public interface CallbackSupplier<R> extends CallbackSupplierEx<R> {
	@Override
	void get(SettableCallback<R> cb);
}

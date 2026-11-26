package com.zhouda.rpc.common.builder;

public interface Rebuildable<T, B extends Builder<T>> {
	B rebuild();
}

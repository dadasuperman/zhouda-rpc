package com.zhouda.rpc.reactor;

import com.zhouda.rpc.reactor.nio.NioReactor;

public interface NioReactive extends Reactive {
	@Override
	NioReactor getReactor();
}

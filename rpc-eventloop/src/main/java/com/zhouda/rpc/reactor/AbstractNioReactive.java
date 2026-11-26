package com.zhouda.rpc.reactor;

import com.zhouda.rpc.reactor.nio.NioReactor;

public abstract class AbstractNioReactive implements NioReactive {
	protected final NioReactor reactor;

	public AbstractNioReactive(NioReactor reactor) {this.reactor = reactor;}

	@Override
	public final NioReactor getReactor() {
		return reactor;
	}
}

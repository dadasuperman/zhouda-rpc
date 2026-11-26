package com.zhouda.rpc.reactor;

import static com.zhouda.rpc.reactor.Reactor.getCurrentReactor;

public abstract class ImplicitlyReactive extends AbstractReactive {
	public ImplicitlyReactive() {
		super(getCurrentReactor());
	}
}

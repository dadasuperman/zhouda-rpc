package com.zhouda.rpc.client.sender.strategy.impl;

import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.client.RpcClient;
import com.zhouda.rpc.client.RpcClientConnectionPool;
import com.zhouda.rpc.client.sender.RpcSender;
import com.zhouda.rpc.client.sender.strategy.RpcStrategy;

import java.net.InetSocketAddress;
import java.util.Set;

@ExposedInternals
public final class Forced implements RpcStrategy {
	public final RpcStrategy strategy;

	public Forced(RpcStrategy strategy) {this.strategy = strategy;}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return strategy.getAddresses();
	}

	@Override
	public RpcSender createSender(RpcClientConnectionPool pool) {
		RpcSender sender = strategy.createSender(pool);
		return sender == null ?
			new RpcClient.NoSenderAvailable() :
			sender;
	}
}

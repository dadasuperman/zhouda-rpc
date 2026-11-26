package com.zhouda.rpc.client.sender.strategy;

import com.zhouda.rpc.async.callback.Callback;
import com.zhouda.rpc.client.RpcClientConnectionPool;
import com.zhouda.rpc.client.sender.RpcSender;
import com.zhouda.rpc.client.sender.strategy.impl.FirstAvailable;
import com.zhouda.rpc.client.sender.strategy.impl.FirstValidResult;
import com.zhouda.rpc.client.sender.strategy.impl.Forced;
import com.zhouda.rpc.client.sender.strategy.impl.RandomSampling;
import com.zhouda.rpc.client.sender.strategy.impl.RendezvousHashing;
import com.zhouda.rpc.client.sender.strategy.impl.RoundRobin;
import com.zhouda.rpc.client.sender.strategy.impl.Sharding;
import com.zhouda.rpc.client.sender.strategy.impl.TypeDispatching;
import com.zhouda.rpc.protocol.RpcException;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.*;

public class RpcStrategyTest {
	private static final RpcClientConnectionPool EMPTY_POOL = address -> null;

	// 特性：Server 策略只暴露一个地址，并原样返回对应连接 sender。
	@Test
	public void serverStrategyDelegatesToOneConnection() {
		InetSocketAddress address = address(1);
		RpcStrategy strategy = RpcStrategies.server(address);
		CountingSender sender = new CountingSender(request -> request);
		RpcClientConnectionPool pool = candidate -> address.equals(candidate) ? sender : null;

		assertEquals(Set.of(address), strategy.getAddresses());
		assertSame(sender, strategy.createSender(pool));
		assertNull(strategy.createSender(EMPTY_POOL));
	}

	// 特性：FirstAvailable 按顺序跳过未连接节点，全部不可用时返回 null。
	@Test
	public void firstAvailableSkipsMissingConnections() {
		RpcStrategy first = fixedStrategy(address(1), null);
		CountingSender secondSender = new CountingSender(request -> request);
		RpcStrategy second = fixedStrategy(address(2), secondSender);
		FirstAvailable strategy = new FirstAvailable(List.of(first, second));

		assertEquals(Set.of(address(1), address(2)), strategy.getAddresses());
		assertSame(secondSender, strategy.createSender(EMPTY_POOL));
		assertNull(new FirstAvailable(List.of(first)).createSender(EMPTY_POOL));
		assertNull(new FirstAvailable(List.of()).createSender(EMPTY_POOL));
	}

	// 特性：RoundRobin 仅在活跃节点间轮转，并可要求最低活跃节点数。
	@Test
	public void roundRobinBalancesOnlyActiveSenders() {
		CountingSender first = new CountingSender(request -> request);
		CountingSender second = new CountingSender(request -> request);
		CountingSender third = new CountingSender(request -> request);
		RoundRobin strategy = RoundRobin.builder(
			fixedStrategy(address(1), first),
			fixedStrategy(address(2), null),
			fixedStrategy(address(3), second),
			fixedStrategy(address(4), third)
		).build();
		RpcSender sender = strategy.createSender(EMPTY_POOL);
		for (int i = 0; i < 5; i++) send(sender, i);

		assertEquals(2, first.calls);
		assertEquals(2, second.calls);
		assertEquals(1, third.calls);
		assertNull(RoundRobin.builder(fixedStrategy(address(1), first))
			.withMinActiveSubStrategies(2).build().createSender(EMPTY_POOL));
	}

	// 特性：Sharding 保留节点槽位，按请求哈希精确路由，缺失槽位返回业务异常。
	@Test
	public void shardingRoutesByKeyAndReportsMissingShard() {
		CountingSender first = new CountingSender(request -> request);
		CountingSender third = new CountingSender(request -> request);
		Sharding strategy = Sharding.builder((Integer request) -> request % 3,
			fixedStrategy(address(1), first),
			fixedStrategy(address(2), null),
			fixedStrategy(address(3), third)
		).build();
		RpcSender sender = strategy.createSender(EMPTY_POOL);

		for (int i : new int[]{0, 3, 6, 2, 5}) send(sender, i);
		assertEquals(3, first.calls);
		assertEquals(2, third.calls);
		ResultCallback<Object> callback = new ResultCallback<>();
		sender.sendRequest(1, 100, callback);
		assertTrue(callback.exception instanceof RpcException);
		assertEquals("No senders available", callback.exception.getMessage());
		assertNull(Sharding.builder((Integer request) -> 0, fixedStrategy(address(1), null))
			.withMinActiveSubStrategies(1).build().createSender(EMPTY_POOL));
	}

	// 特性：FirstValidResult 并发探测多个节点，只回调首个有效结果一次。
	@Test
	public void firstValidResultAcceptsFirstValidResponseOnly() {
		CountingSender invalid = new CountingSender(request -> null);
		CountingSender valid = new CountingSender(request -> "valid");
		RpcSender sender = FirstValidResult.builder(
			fixedStrategy(address(1), invalid), fixedStrategy(address(2), valid)
		).build().createSender(EMPTY_POOL);
		ResultCallback<Object> callback = new ResultCallback<>();

		sender.sendRequest("request", 100, callback);
		assertEquals(1, invalid.calls);
		assertEquals(1, valid.calls);
		assertEquals(1, callback.calls);
		assertEquals("valid", callback.result);
		assertNull(callback.exception);
	}

	// 特性：所有结果无效时返回配置的异常，节点异常也可作为最终失败原因。
	@Test
	public void firstValidResultReturnsConfiguredFailure() {
		RpcException configured = new RpcException("no valid replica");
		RpcSender sender = FirstValidResult.builder(fixedStrategy(address(1),
			new CountingSender(request -> null)))
			.withNoValidResultException(configured)
			.build().createSender(EMPTY_POOL);
		ResultCallback<Object> callback = new ResultCallback<>();
		sender.sendRequest("request", 100, callback);
		assertSame(configured, callback.exception);

		IllegalArgumentException nodeFailure = new IllegalArgumentException("replica failed");
		sender = FirstValidResult.builder(fixedStrategy(address(1),
			new CountingSender(request -> null, nodeFailure))).build().createSender(EMPTY_POOL);
		callback = new ResultCallback<>();
		sender.sendRequest("request", 100, callback);
		assertSame(nodeFailure, callback.exception);
	}

	// 特性：Forced 策略原样委托可用节点，节点缺失时提供明确的失败 sender。
	@Test
	public void forcedStrategyNeverReturnsNullSender() {
		CountingSender delegate = new CountingSender(request -> request);
		Forced available = new Forced(fixedStrategy(address(1), delegate));
		assertSame(delegate, available.createSender(EMPTY_POOL));
		assertEquals(Set.of(address(1)), available.getAddresses());

		Forced unavailable = new Forced(fixedStrategy(address(1), null));
		RpcSender sender = unavailable.createSender(EMPTY_POOL);
		assertNotNull(sender);
		ResultCallback<Object> callback = new ResultCallback<>();
		sender.sendRequest("request", 100, callback);
		assertTrue(callback.exception instanceof RpcException);
	}

	// 特性：RandomSampling 按权重选择节点，零权重节点不会被调度。
	@Test
	public void randomSamplingHonorsWeightsAndSkipsZeroWeight() {
		CountingSender first = new CountingSender(request -> request);
		CountingSender second = new CountingSender(request -> request);
		CountingSender zero = new CountingSender(request -> request);
		RpcSender sender = RandomSampling.builder()
			.withRandom(new Random(42))
			.with(2, fixedStrategy(address(1), first))
			.with(3, fixedStrategy(address(2), second))
			.with(0, fixedStrategy(address(3), zero))
			.build().createSender(EMPTY_POOL);
		for (int i = 0; i < 10_000; i++) send(sender, i);

		assertTrue(first.calls > 1_500);
		assertTrue(second.calls > 2_500);
		assertEquals(10_000, first.calls + second.calls + zero.calls);
		assertEquals(0, zero.calls);
	}

	// 特性：RandomSampling 拒绝负权重和重复策略。
	@Test
	public void randomSamplingRejectsInvalidWeights() {
		RpcStrategy strategy = fixedStrategy(address(1), new CountingSender(request -> request));
		assertThrows(IllegalArgumentException.class, () -> RandomSampling.builder().with(-1, strategy));
		assertThrows(IllegalArgumentException.class, () -> RandomSampling.builder().with(1, strategy).with(2, strategy));
	}

	// 特性：RendezvousHashing 按桶选择最高优先级活跃分片，节点移除后自动重映射。
	@Test
	public void rendezvousHashingRemapsMissingShards() {
		CountingSender first = new CountingSender(request -> request);
		CountingSender second = new CountingSender(request -> request);
		RendezvousHashing strategy = RendezvousHashing.builder((Integer request) -> request)
			.withBuckets(4)
			.withHashBucketFunction((shard, bucket) -> shard.equals("first") ? bucket + 1 : -bucket)
			.withShard("first", RpcStrategies.server(address(1)))
			.withShard("second", RpcStrategies.server(address(2)))
			.build();
		RpcClientConnectionPool onlySecond = candidate -> address(2).equals(candidate) ? second : null;
		RpcSender sender = strategy.createSender(address ->
			address(1).equals(address) ? first : address(2).equals(address) ? second : null);
		for (int i = 0; i < 8; i++) send(sender, i);
		assertEquals(8, first.calls);
		assertEquals(0, second.calls);

		sender = strategy.createSender(onlySecond);
		for (int i = 0; i < 8; i++) send(sender, i);
		assertEquals(8, second.calls);
		assertNull(RendezvousHashing.builder((Integer request) -> request)
			.withBuckets(4).withMinActiveShards(2)
			.withShard("first", fixedStrategy(address(1), first)).build()
			.createSender(onlySecond));
	}

	// 特性：RendezvousHashing 的桶数必须是大于零的 2 的幂。
	@Test
	public void rendezvousHashingRejectsInvalidBucketCounts() {
		assertThrows(IllegalArgumentException.class, () -> RendezvousHashing.builder((Integer request) -> request).withBuckets(3));
		assertThrows(IllegalArgumentException.class, () -> RendezvousHashing.builder((Integer request) -> request).withBuckets(0));
	}

	// 特性：TypeDispatching 按请求具体类型路由，未知类型使用默认策略。
	@Test
	public void typeDispatchingUsesExactTypeAndDefault() {
		CountingSender strings = new CountingSender(request -> request);
		CountingSender integers = new CountingSender(request -> request);
		CountingSender fallback = new CountingSender(request -> request);
		TypeDispatching strategy = TypeDispatching.builder()
			.with(String.class, fixedStrategy(address(1), strings))
			.with(Integer.class, fixedStrategy(address(2), integers))
			.withDefault(fixedStrategy(address(3), fallback))
			.build();
		RpcSender sender = strategy.createSender(EMPTY_POOL);
		send(sender, "text");
		send(sender, 7);
		send(sender, 1.5);
		assertEquals(1, strings.calls);
		assertEquals(1, integers.calls);
		assertEquals(1, fallback.calls);

		assertThrows(IllegalStateException.class, () -> TypeDispatching.builder()
			.with(String.class, fixedStrategy(address(1), strings))
			.with(String.class, fixedStrategy(address(2), integers)));
	}

	// 特性：TypeDispatching 在无默认策略时拒绝未知类型，配置节点缺失时拒绝创建 sender。
	@Test
	public void typeDispatchingReportsUnavailableTypes() {
		CountingSender strings = new CountingSender(request -> request);
		RpcSender sender = TypeDispatching.builder()
			.with(String.class, fixedStrategy(address(1), strings)).build().createSender(EMPTY_POOL);
		ResultCallback<Object> callback = new ResultCallback<>();
		sender.sendRequest(1, 100, callback);
		assertTrue(callback.exception instanceof RpcException);
		assertEquals("No senders available", callback.exception.getMessage());
		assertNull(TypeDispatching.builder().with(String.class, fixedStrategy(address(1), null))
			.build().createSender(EMPTY_POOL));
	}

	private static RpcStrategy fixedStrategy(InetSocketAddress address, @Nullable RpcSender sender) {
		return new RpcStrategy() {
			@Override
			public Set<InetSocketAddress> getAddresses() {
				return Set.of(address);
			}

			@Override
			public @Nullable RpcSender createSender(RpcClientConnectionPool pool) {
				return sender;
			}
		};
	}

	private static void send(RpcSender sender, Object request) {
		sender.sendRequest(request, 100, (result, error) -> assertNull(error));
	}

	private static InetSocketAddress address(int port) {
		return new InetSocketAddress("127.0.0.1", port);
	}

	private static final class CountingSender implements RpcSender {
		private final Function<Object, Object> response;
		private final @Nullable Exception exception;
		private int calls;

		private CountingSender(Function<Object, Object> response) {
			this(response, null);
		}

		private CountingSender(Function<Object, Object> response, @Nullable Exception exception) {
			this.response = response;
			this.exception = exception;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <I, O> void sendRequest(I request, int timeout, Callback<O> callback) {
			calls++;
			if (exception != null) {
				((Callback<Object>) callback).accept(null, exception);
			} else {
				((Callback<Object>) callback).accept(response.apply(request), null);
			}
		}
	}

	private static final class ResultCallback<T> implements Callback<T> {
		private int calls;
		private @Nullable T result;
		private @Nullable Exception exception;

		@Override
		public void accept(@Nullable T result, @Nullable Exception e) {
			calls++;
			this.result = result;
			exception = e;
		}
	}
}

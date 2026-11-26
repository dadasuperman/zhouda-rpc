package com.zhouda.rpc;

import com.zhouda.rpc.eventloop.Eventloop;
import com.zhouda.rpc.reactor.Reactor;
import com.zhouda.rpc.reactor.jmx.ReactorJmxBeanAdapter;
import com.zhouda.rpc.support.RpcTestEnvironment;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public final class ReactorJmxBeanAdapterTest {

	// 特性：已注册 JMX bean 的操作分发到其 reactor，未注册 bean 被拒绝。
	@Test
	public void dispatchesOperationsToRegisteredReactor() throws Exception {
		try (RpcTestEnvironment env = RpcTestEnvironment.start()) {
			ReactorJmxBeanAdapter adapter = new ReactorJmxBeanAdapter();
			adapter.setRefreshParameters(Duration.ofSeconds(1), 10);
			TestBean bean = new TestBean(env.getEventloop());
			adapter.registerRefreshableBean(bean, List.of(timestamp -> {}));

			CompletableFuture<String> executed = env.call(() -> {
				CompletableFuture<String> future = new CompletableFuture<>();
				adapter.execute(bean, () -> future.complete(Thread.currentThread().getName()));
				return future;
			}).thenCompose(future -> future);
			assertEquals("rpc-test-eventloop", env.await(executed));
			assertThrows(NullPointerException.class, () -> adapter.execute(new Object(), () -> {}));
		}
	}

	public static final class TestBean {
		private final Eventloop eventloop;

		private TestBean(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		public Eventloop getReactor() {
			return eventloop;
		}
	}
}

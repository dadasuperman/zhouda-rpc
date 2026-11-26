/*
 * Copyright (C) 2020 ZhoudaRpc LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zhouda.rpc.csp.consumer;

import com.zhouda.rpc.async.process.AbstractAsyncCloseable;
import com.zhouda.rpc.async.process.AsyncCloseable;
import com.zhouda.rpc.common.Checks;
import com.zhouda.rpc.common.recycle.Recyclers;
import com.zhouda.rpc.promise.Promise;
import org.jetbrains.annotations.Nullable;

import static com.zhouda.rpc.reactor.Reactive.checkInReactorThread;

public abstract class AbstractChannelConsumer<T> extends AbstractAsyncCloseable implements ChannelConsumer<T> {
	private static final boolean CHECKS = Checks.isEnabled(AbstractChannelConsumer.class);

	protected AbstractChannelConsumer() {
		setCloseable(null);
	}

	protected AbstractChannelConsumer(@Nullable AsyncCloseable closeable) {
		setCloseable(closeable);
	}

	protected abstract Promise<Void> doAccept(@Nullable T value);

	@Override
	public final Promise<Void> accept(@Nullable T value) {
		if (CHECKS) checkInReactorThread(this);
		if (isClosed()) {
			Recyclers.recycle(value);
			return Promise.ofException(getException());
		}
		return doAccept(value);
	}
}

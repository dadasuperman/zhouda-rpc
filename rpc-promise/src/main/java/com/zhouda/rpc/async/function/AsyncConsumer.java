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

package com.zhouda.rpc.async.function;

import com.zhouda.rpc.common.function.ConsumerEx;
import com.zhouda.rpc.promise.Promise;

import static com.zhouda.rpc.common.exception.FatalErrorHandler.handleError;

/**
 * Represents an asynchronous consumer that consumes data items.
 */
@FunctionalInterface
public interface AsyncConsumer<T> extends AsyncConsumerEx<T> {
	/**
	 * Consumes some data asynchronously.
	 *
	 * @param value value to be consumed
	 * @return {@link Promise} of {@link Void} that represents successful consumption of data
	 */
	@Override
	Promise<Void> accept(T value);

	/**
	 * Wraps a {@link ConsumerEx} interface.
	 *
	 * @param consumer a {@link ConsumerEx}
	 * @return {@link AsyncConsumer} that works on top of {@link ConsumerEx} interface
	 */
	static <T> AsyncConsumer<T> of(ConsumerEx<? super T> consumer) {
		return value -> {
			try {
				consumer.accept(value);
			} catch (Exception e) {
				handleError(e, consumer);
				return Promise.ofException(e);
			}
			return Promise.complete();
		};
	}

	static <T> AsyncConsumer<T> sanitize(AsyncConsumerEx<? super T> consumer) {
		return value -> {
			try {
				return consumer.accept(value);
			} catch (Exception e) {
				handleError(e, consumer);
				return Promise.ofException(e);
			}
		};
	}
}

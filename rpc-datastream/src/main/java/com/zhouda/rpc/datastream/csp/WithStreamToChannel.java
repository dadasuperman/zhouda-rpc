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

package com.zhouda.rpc.datastream.csp;

import com.zhouda.rpc.csp.dsl.WithChannelOutput;
import com.zhouda.rpc.csp.supplier.ChannelSupplier;
import com.zhouda.rpc.datastream.consumer.StreamConsumer;
import com.zhouda.rpc.datastream.processor.transformer.StreamSupplierTransformer;
import com.zhouda.rpc.datastream.supplier.StreamSupplier;

/**
 * This interface is a shortcut for implementing transformers that convert
 * {@link StreamSupplier stream suppliers} to {@link ChannelSupplier channel suppliers}
 * and are useful through both DSLs.
 */
public interface WithStreamToChannel<Self, I, O> extends
	StreamConsumer<I>,
	WithChannelOutput<Self, O>,
	StreamSupplierTransformer<I, ChannelSupplier<O>> {

	@Override
	default ChannelSupplier<O> transform(StreamSupplier<I> streamSupplier) {
		streamSupplier.streamTo(this);
		return getOutput().getSupplier();
	}
}

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

package com.zhouda.rpc.csp.process.transformer.impl;

import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufs;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.csp.process.transformer.AbstractChannelTransformer;
import com.zhouda.rpc.promise.Promise;
import com.zhouda.rpc.promise.Promises;

import static com.zhouda.rpc.common.Checks.checkArgument;
import static java.lang.Math.min;

@ExposedInternals
public final class ByteChunker extends AbstractChannelTransformer<ByteChunker, ByteBuf, ByteBuf> {
	public final ByteBufs bufs = new ByteBufs();

	public final int minChunkSize;
	public final int maxChunkSize;

	public ByteChunker(int minChunkSize, int maxChunkSize) {
		this.minChunkSize = checkArgument(minChunkSize, minSize -> minSize > 0, "Minimal chunk size should be greater than 0");
		this.maxChunkSize = checkArgument(maxChunkSize, maxSize -> maxSize >= minChunkSize, "Maximal chunk size cannot be less than minimal chunk size");
	}

	@Override
	protected Promise<Void> onItem(ByteBuf item) {
		bufs.add(item);
		return Promises.repeat(
			() -> {
				if (!bufs.hasRemainingBytes(minChunkSize)) return Promise.of(false);
				int exactSize = 0;
				for (int i = 0; i != bufs.remainingBufs(); i++) {
					exactSize += bufs.peekBuf(i).readRemaining();
					if (exactSize >= minChunkSize) {
						break;
					}
				}
				return send(bufs.takeExactSize(min(exactSize, maxChunkSize)))
					.map($ -> true);
			});
	}

	@Override
	protected Promise<Void> onProcessFinish() {
		return (bufs.hasRemaining() ? send(bufs.takeRemaining()) : Promise.complete())
			.then(this::sendEndOfStream);
	}

	@Override
	protected void onCleanup() {
		bufs.recycle();
	}
}

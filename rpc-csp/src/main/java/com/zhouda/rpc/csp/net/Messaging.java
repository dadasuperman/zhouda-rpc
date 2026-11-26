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

package com.zhouda.rpc.csp.net;

import com.zhouda.rpc.async.process.AbstractAsyncCloseable;
import com.zhouda.rpc.bytebuf.ByteBuf;
import com.zhouda.rpc.bytebuf.ByteBufs;
import com.zhouda.rpc.common.Checks;
import com.zhouda.rpc.common.exception.TruncatedDataException;
import com.zhouda.rpc.csp.binary.BinaryChannelSupplier;
import com.zhouda.rpc.csp.binary.codec.ByteBufsCodec;
import com.zhouda.rpc.csp.consumer.ChannelConsumer;
import com.zhouda.rpc.csp.consumer.ChannelConsumers;
import com.zhouda.rpc.csp.supplier.ChannelSupplier;
import com.zhouda.rpc.csp.supplier.ChannelSuppliers;
import com.zhouda.rpc.net.socket.tcp.ITcpSocket;
import com.zhouda.rpc.promise.Promise;

import static com.zhouda.rpc.reactor.Reactive.checkInReactorThread;

/**
 * Represents a simple binary protocol over for communication a TCP connection.
 */
public final class Messaging<I, O> extends AbstractAsyncCloseable implements IMessaging<I, O> {
	private static final boolean CHECKS = Checks.isEnabled(Messaging.class);

	private final ITcpSocket socket;

	private final ByteBufsCodec<I, O> codec;

	private final ByteBufs bufs = new ByteBufs();
	private final BinaryChannelSupplier bufsSupplier;

	private boolean readDone;
	private boolean writeDone;

	private Messaging(ITcpSocket socket, ByteBufsCodec<I, O> codec) {
		this.socket = socket;
		this.codec = codec;
		this.bufsSupplier = BinaryChannelSupplier.ofProvidedBufs(bufs,
			() -> this.socket.read()
				.whenResult(buf -> {
					if (buf != null) {
						bufs.add(buf);
					} else {
						throw new TruncatedDataException();
					}
				})
				.toVoid()
				.whenException(this::closeEx),
			Promise::complete,
			this);
	}

	public static <I, O> Messaging<I, O> create(ITcpSocket socket, ByteBufsCodec<I, O> serializer) {
		Messaging<I, O> messaging = new Messaging<>(socket, serializer);
		messaging.prefetch();
		return messaging;
	}

	private void prefetch() {
		if (bufs.isEmpty()) {
			socket.read()
				.whenResult(buf -> {
					if (buf != null) {
						bufs.add(buf);
					} else {
						readDone = true;
						closeIfDone();
					}
				})
				.whenException(this::closeEx);
		}
	}

	@Override
	public Promise<I> receive() {
		if (CHECKS) checkInReactorThread(this);
		return bufsSupplier.decode(codec::tryDecode)
			.whenResult(this::prefetch)
			.whenException(this::closeEx);
	}

	@Override
	public Promise<Void> send(O msg) {
		if (CHECKS) checkInReactorThread(this);
		return socket.write(codec.encode(msg))
			.whenException(this::closeEx);
	}

	@Override
	public Promise<Void> sendEndOfStream() {
		if (CHECKS) checkInReactorThread(this);
		return socket.write(null)
			.whenResult(() -> {
				writeDone = true;
				closeIfDone();
			})
			.whenException(this::closeEx);
	}

	@Override
	public ChannelConsumer<ByteBuf> sendBinaryStream() {
		if (CHECKS) checkInReactorThread(this);
		return ChannelConsumers.ofSocket(socket)
			.withAcknowledgement(ack -> ack
				.whenResult(() -> {
					writeDone = true;
					closeIfDone();
				})
				.whenException(this::closeEx));
	}

	@Override
	public ChannelSupplier<ByteBuf> receiveBinaryStream() {
		if (CHECKS) checkInReactorThread(this);
		return ChannelSuppliers.concat(ChannelSuppliers.ofIterator(bufs.asIterator()), ChannelSuppliers.ofSocket(socket))
			.withEndOfStream(eos -> eos
				.whenResult(() -> {
					readDone = true;
					closeIfDone();
				})
				.whenException(this::closeEx));
	}

	@Override
	protected void onClosed(Exception e) {
		socket.closeEx(e);
		bufs.recycle();
	}

	private void closeIfDone() {
		if (readDone && writeDone) {
			close();
		}
	}

	@Override
	public String toString() {
		return "Messaging{socket=" + socket + "}";
	}
}

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

package com.zhouda.rpc.serializer.def.impl;

import com.zhouda.rpc.codegen.expression.Expression;
import com.zhouda.rpc.codegen.expression.Variable;
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.serializer.CompatibilityLevel;
import com.zhouda.rpc.serializer.def.PrimitiveSerializerDef;
import com.zhouda.rpc.serializer.def.SerializerDef;

import static com.zhouda.rpc.serializer.def.SerializerExpressions.readByte;
import static com.zhouda.rpc.serializer.def.SerializerExpressions.writeByte;

@ExposedInternals
public final class ByteSerializerDef extends PrimitiveSerializerDef {

	@SuppressWarnings("unused") // used via reflection
	public ByteSerializerDef() {
		this(true);
	}

	public ByteSerializerDef(boolean wrapped) {
		super(byte.class, wrapped);
	}

	@Override
	public SerializerDef ensureWrapped() {
		return new ByteSerializerDef(true);
	}

	@Override
	protected Expression doSerialize(Expression byteArray, Variable off, Expression value, CompatibilityLevel compatibilityLevel) {
		return writeByte(byteArray, off, value);
	}

	@Override
	protected Expression doDeserialize(Expression in, CompatibilityLevel compatibilityLevel) {
		return readByte(in);
	}
}

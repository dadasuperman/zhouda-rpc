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
import com.zhouda.rpc.serializer.def.SerializerDefWithVarLength;

import static com.zhouda.rpc.codegen.expression.Expressions.cast;
import static com.zhouda.rpc.serializer.def.SerializerExpressions.*;

@ExposedInternals
public final class IntSerializerDef extends PrimitiveSerializerDef implements SerializerDefWithVarLength {
	public final boolean varLength;

	@SuppressWarnings("unused") // used via reflection
	public IntSerializerDef() {
		this(true, false);
	}

	public IntSerializerDef(boolean wrapped, boolean varLength) {
		super(int.class, wrapped);
		this.varLength = varLength;
	}

	@Override
	public SerializerDef ensureWrapped() {
		return new IntSerializerDef(true, varLength);
	}

	@Override
	protected Expression doSerialize(Expression byteArray, Variable off, Expression value, CompatibilityLevel compatibilityLevel) {
		return varLength ?
			writeVarInt(byteArray, off, cast(value, int.class)) :
			writeInt(byteArray, off, cast(value, int.class), !compatibilityLevel.isLittleEndian());
	}

	@Override
	protected Expression doDeserialize(Expression in, CompatibilityLevel compatibilityLevel) {
		return varLength ?
			readVarInt(in) :
			readInt(in, !compatibilityLevel.isLittleEndian());
	}

	@Override
	public SerializerDef ensureVarLength() {
		return new IntSerializerDef(wrapped, true);
	}
}

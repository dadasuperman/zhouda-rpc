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
import com.zhouda.rpc.common.annotation.ExposedInternals;
import com.zhouda.rpc.serializer.CompatibilityLevel;
import com.zhouda.rpc.serializer.def.AbstractMapSerializerDef;
import com.zhouda.rpc.serializer.def.SerializerDef;

import java.util.function.BinaryOperator;

import static com.zhouda.rpc.codegen.expression.Expressions.*;

@ExposedInternals
public class RegularMapSerializerDef extends AbstractMapSerializerDef {
	public RegularMapSerializerDef(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> encodeType, Class<?> decodeType, Class<?> keyType, Class<?> valueType, boolean nullable) {
		super(keySerializer, valueSerializer, encodeType, decodeType, keyType, valueType, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new RegularMapSerializerDef(keySerializer, valueSerializer, encodeType, decodeType, keyType, valueType, true);
	}

	@Override
	protected Expression doIterateMap(Expression collection, BinaryOperator<Expression> keyValueAction) {
		return iterateMap(collection, keyValueAction);
	}

	@Override
	protected Expression createBuilder(Expression length) {
		return constructor(decodeType, length);
	}

	@Override
	protected Expression putToBuilder(Expression builder, Expression index, Expression key, Expression value) {
		return call(builder, "put", key, value);
	}

	@Override
	protected Expression build(Expression builder) {
		return builder;
	}
}

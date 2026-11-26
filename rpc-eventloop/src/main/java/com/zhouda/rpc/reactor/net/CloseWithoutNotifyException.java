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

package com.zhouda.rpc.reactor.net;

import com.zhouda.rpc.common.ApplicationSettings;

public class CloseWithoutNotifyException extends Exception {
	public static final boolean WITH_STACK_TRACE = ApplicationSettings.getBoolean(CloseWithoutNotifyException.class, "withStackTrace", false);

	public CloseWithoutNotifyException(String message, Throwable cause) {
		super(message, cause);
	}

	@Override
	public final Throwable fillInStackTrace() {
		return WITH_STACK_TRACE ? super.fillInStackTrace() : this;
	}
}

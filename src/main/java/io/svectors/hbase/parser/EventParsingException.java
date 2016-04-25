/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.svectors.hbase.parser;

/**
 * @author ravi.magham
 */
public class EventParsingException extends RuntimeException {

    /**
	 *
	 */
	private static final long serialVersionUID = -5861884289109519422L;

	public EventParsingException() {
        super();
    }

    public EventParsingException(String message) {
        super(message);
    }

    public EventParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventParsingException(Throwable cause) {
        super(cause);
    }

    protected EventParsingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

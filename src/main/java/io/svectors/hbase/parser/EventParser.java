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

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * @author ravi.magham
 */
public interface EventParser {

    /**
     * Parses the key value based on the key schema .
     * @param sr
     * @return
     */
    Map<String, byte[]> parseKey(SinkRecord sr) throws EventParsingException;

    /**
     * Parses the values based on the value schema.
     * @param sr
     * @return
     */
    Map<String, byte[]> parseValue(SinkRecord sr) throws EventParsingException;
}

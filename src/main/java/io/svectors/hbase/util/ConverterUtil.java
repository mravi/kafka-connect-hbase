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
package io.svectors.hbase.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author ravi.magham
 */
public final class ConverterUtil {

    private static final ConverterUtil INSTANCE = new ConverterUtil();
    private final Map<String, byte[]> EMPTY_MAP = Collections.emptyMap();
    private final StringConverter keyConverter;
    private final AvroData avroData = new AvroData(100);

    private ConverterUtil() {
        Map<String, String> props = new HashMap<>();
        props.put("schemas.enable", Boolean.TRUE.toString());
        keyConverter = new StringConverter();
        keyConverter.configure(props, true);
    }

    public static ConverterUtil getInstance() {
        return INSTANCE;
    }

    /**
     * If the key holds the data, we use that as the row key for hbase. if not, we get it from the configuration.
     * @param sinkRecord
     * @return
     */
    public Optional<byte[]> toRowKey(final SinkRecord sinkRecord) {
        byte[] rowKey = keyConverter.fromConnectData(sinkRecord.topic(), sinkRecord.keySchema(), sinkRecord.key());
        return Optional.ofNullable(rowKey);
    }

    /**
     * Returns the values as a map. The assumption here is the value schema is a struct.
     * @param sinkRecord
     * @return Map<String, byte[]> : the key is the column name and the value is the Byte representation of the value.
     */
    public Map<String, byte[]> toColumnValues(final SinkRecord sinkRecord) {
        Object data = avroData.fromConnectData(sinkRecord.valueSchema(), sinkRecord.value());
        if(!(data instanceof GenericRecord)) {
            return EMPTY_MAP;
        }
        final GenericRecord record = (GenericRecord)data;
        final List<Field> fields = sinkRecord.valueSchema().fields();
        final Map<String, byte[]> valueMap = Maps.newHashMapWithExpectedSize(fields.size());
        for(Field field : fields) {
            final byte[] fieldValue = toColumnValue(record, field);
            if(fieldValue == null) {
                continue;
            }
            valueMap.put(field.name(), fieldValue);
        }
        return valueMap;
    }

    private byte[] toColumnValue(final GenericRecord record, final Field field) {
        Preconditions.checkNotNull(field);
        final Schema.Type type = field.schema().type();
        final String fieldName = field.name();
        final Object fieldValue = record.get(fieldName);
        switch (type) {
            case STRING:
                return Bytes.toBytes((String)fieldValue);
            case BOOLEAN:
                return Bytes.toBytes((Boolean)fieldValue);
            case BYTES:
                return Bytes.toBytes((ByteBuffer) fieldValue);
            case FLOAT32:
                return Bytes.toBytes((Float)fieldValue);
            case FLOAT64:
                return Bytes.toBytes((Double)fieldValue);
            case INT8:
                return Bytes.toBytes((Byte)fieldValue);
            case INT16:
                return Bytes.toBytes((Short)fieldValue);
            case INT32:
                return Bytes.toBytes((Integer)fieldValue);
            case INT64:
                return Bytes.toBytes((Long)fieldValue);
            default:
                return null;
        }
    }
}

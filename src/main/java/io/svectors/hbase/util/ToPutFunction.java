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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import java.util.Map;
import java.util.Optional;
import io.svectors.hbase.config.HBaseSinkConfig;

/**
 * @author ravi.magham
 */
public class ToPutFunction implements Function<SinkRecord, Put> {

    private final ConverterUtil converterUtil = ConverterUtil.getInstance();
    private final AbstractConfig sinkConfig;

    public ToPutFunction(AbstractConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    @Override
    public Put apply(final SinkRecord sinkRecord) {
        Preconditions.checkNotNull(sinkRecord);

        final Optional<byte[]> rowkeyOptional = converterUtil.toRowKey(sinkRecord);
        final Map<String, byte[]> valuesMap  = converterUtil.toColumnValues(sinkRecord);
        byte[] rowkey = null;
        if(!rowkeyOptional.isPresent()) {
            rowkey = toRowKey(valuesMap);
        } else {
            rowkey = rowkeyOptional.get();
        }
        if(rowkey == null) {

        }
        final Put put = new Put(rowkey);
        valuesMap.entrySet().stream().forEach(entry -> {
            final String qualifier = entry.getKey();
            final byte[] value = entry.getValue();
            put.addColumn(Bytes.toBytes("d"), Bytes.toBytes(qualifier), value);
        });
        return put;
    }

    /**
     * constructs the row key.
     * @param valuesMap
     * @return
     */
    private byte[] toRowKey(Map<String, byte[]> valuesMap) {
        final String rowKeyCols = sinkConfig.getString(HBaseSinkConfig.HBASE_ROWKEY_DEFAULT_CONFIG);
        final String delimiter = sinkConfig.getString(HBaseSinkConfig.HBASE_ROWKEY_DELIMITER_CONFIG);
        final byte[] delimiterBytes = Bytes.toBytes(delimiter);
        final String[] columns = rowKeyCols.split(",");
        byte[] rowkey = null;
        for(String column : columns) {
            byte[] columnValue = valuesMap.get(column);
            if(rowkey == null) {
                rowkey = Bytes.add(columnValue, delimiterBytes);
            } else {
                rowkey = Bytes.add(rowkey, delimiterBytes, columnValue);
            }
        }
        return rowkey;
    }
}

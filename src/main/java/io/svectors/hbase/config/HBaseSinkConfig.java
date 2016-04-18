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
package io.svectors.hbase.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.Map;

/**
 * @author ravi.magham
 */
public class HBaseSinkConfig extends AbstractConfig {

    public static String ZOOKEEPER_QUORUM_CONFIG = "zookeeper.quorum";
    public static String HBASE_ROWKEY_DEFAULT_CONFIG = "hbase.rowkey.columns";
    public static String HBASE_ROWKEY_DELIMITER_CONFIG = "hbase.rowkey.delimiter";
    public static String HBASE_ROWKEY_DELIMITER = ",";

    private static ConfigDef CONFIG = new ConfigDef();

    static {

        CONFIG.define(ZOOKEEPER_QUORUM_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Zookeeper quorum " +
          "for the hbase cluster");

        CONFIG.define(HBASE_ROWKEY_DEFAULT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "Rowkey columns " +
          "for the table when kafka key is null. ");

        CONFIG.define(HBASE_ROWKEY_DELIMITER_CONFIG, ConfigDef.Type.STRING, HBASE_ROWKEY_DELIMITER,
           ConfigDef.Importance.MEDIUM, "Rowkey columns delimiter for the table when kafka key is null. ");
    }

    public HBaseSinkConfig(Map<String, String> originals) {
        this(CONFIG, originals);
    }

    public HBaseSinkConfig(ConfigDef definition, Map<String, String> originals) {
        super(definition, originals);
    }
}

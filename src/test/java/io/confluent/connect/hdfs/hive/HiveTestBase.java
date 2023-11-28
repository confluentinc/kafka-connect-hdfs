/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.hive;

import io.confluent.connect.hdfs.FileUtils;
import org.junit.After;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.storage.hive.HiveConfig;

public class HiveTestBase extends TestWithMiniDFSCluster {

  protected String hiveDatabase;
  protected HiveMetaStore hiveMetaStore;
  protected HiveExec hiveExec;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HiveConfig.HIVE_CONF_DIR_CONFIG, "src/test/resources/conf");
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    hiveDatabase = connectorConfig.getString(HiveConfig.HIVE_DATABASE_CONFIG);
    hiveMetaStore = new HiveMetaStore(conf, connectorConfig);
    hiveExec = new HiveExec(connectorConfig);
    cleanHive();
  }

  @After
  public void tearDown() throws Exception {
    cleanHive();
    super.tearDown();
  }

  private void cleanHive() {
    // ensures all tables are removed
    for (String database : hiveMetaStore.getAllDatabases()) {
      for (String table : hiveMetaStore.getAllTables(database)) {
        hiveMetaStore.dropTable(database, table);
      }
      if (!"default".equals(database)) {
        hiveMetaStore.dropDatabase(database, false);
      }
    }
  }

  protected String partitionLocation(String topic, int partition) {
    return partitionLocation(topic, partition, "partition");
  }

  protected String partitionLocation(String topic,
                                     int partition,
                                     String partitionField) {
    String directory = topic + "/" + partitionField + "=" + partition;
    String topicsDir = connectorConfig.getTopicsDirFromTopic(topic);
    return FileUtils.directoryName(url, topicsDir, directory);
  }

}

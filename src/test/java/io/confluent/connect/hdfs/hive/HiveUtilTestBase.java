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

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.storage.hive.HiveUtil;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public abstract class HiveUtilTestBase extends HiveTestBase {

  private final Map<String, String> localProps = new HashMap<>();
  private final String hiveTableNameConfig;
  private HiveUtil hive;

  public HiveUtilTestBase(String hiveTableNameConfig) {
    this.hiveTableNameConfig = hiveTableNameConfig;
  }

  @Parameterized.Parameters(name = "{index}: hiveTableNameConfig={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
            { "${topic}" },
            { "a-${topic}-table" }
    });
  }

  @Before
  public void beforeTest() {
    localProps.put(HdfsSinkConnectorConfig.HIVE_TABLE_NAME_CONFIG, hiveTableNameConfig);
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    hive = createHiveUtil();
  }

  protected abstract HiveUtil createHiveUtil();

  @Test
  public void testCreateTable() throws Exception {
    setUp();
    prepareData(TOPIC, PARTITION);
    Partitioner partitioner = HiveTestUtils.getPartitioner(parsedConfig);

    String hiveTableName = connectorConfig.getHiveTableName(TOPIC);
    Schema schema = createSchema();
    hive.createTable(hiveDatabase, hiveTableName, schema, partitioner, TOPIC);
    String location = "partition=" + PARTITION;
    hiveMetaStore.addPartition(hiveDatabase, hiveTableName, location);

    Struct expectedRecord = createRecord(schema);
    List<String> expectedResult = new ArrayList<>();
    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
      expectedResult.add(String.valueOf(expectedRecord.get(field.name())));
    }

    Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);
    List<String> actualColumnNames = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      actualColumnNames.add(column.getName());
    }

    assertEquals(expectedColumnNames, actualColumnNames);
    List<FieldSchema> partitionCols = table.getPartitionKeys();
    assertEquals(1, partitionCols.size());
    assertEquals("partition", partitionCols.get(0).getName());

    String result = HiveTestUtils.runHive(
            hiveExec,
            "SELECT * FROM " + hiveMetaStore.tableNameConverter(hiveTableName)
    );
    String[] rows = result.split("\n");
    // Only 6 of the 7 records should have been delivered due to flush_size = 3
    assertEquals(6, rows.length);
    for (String row : rows) {
      String[] parts = HiveTestUtils.parseOutput(row);
      int j = 0;
      for (String expectedValue : expectedResult) {
        assertEquals(expectedValue, parts[j++]);
      }
    }
  }

  @Test
  public void testAlterSchema() throws Exception {
    setUp();
    prepareData(TOPIC, PARTITION);
    Partitioner partitioner = HiveTestUtils.getPartitioner(parsedConfig);

    String hiveTableName = connectorConfig.getHiveTableName(TOPIC);
    Schema schema = createSchema();
    hive.createTable(hiveDatabase, hiveTableName, schema, partitioner, TOPIC);

    String location = "partition=" + PARTITION;
    hiveMetaStore.addPartition(hiveDatabase, hiveTableName, location);

    Struct expectedRecord = createRecord(schema);
    List<String> expectedResult = new ArrayList<>();
    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
      expectedResult.add(String.valueOf(expectedRecord.get(field.name())));
    }

    Table table = hiveMetaStore.getTable(hiveDatabase, hiveTableName);
    List<String> actualColumnNames = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      actualColumnNames.add(column.getName());
    }

    assertEquals(expectedColumnNames, actualColumnNames);

    Schema newSchema = createNewSchema();

    hive.alterSchema(hiveDatabase, hiveTableName, newSchema);

    String result = HiveTestUtils.runHive(
            hiveExec,
            "SELECT * from " + hiveMetaStore.tableNameConverter(hiveTableName)
    );
    String[] rows = result.split("\n");
    // Only 6 of the 7 records should have been delivered due to flush_size = 3
    assertEquals(6, rows.length);
    for (String row : rows) {
      String[] parts = HiveTestUtils.parseOutput(row);
      int j = 0;
      for (String expectedValue : expectedResult) {
        assertEquals(expectedValue, parts[j++]);
      }
    }
  }

  private void prepareData(String topic, int partition) {
    TopicPartition tp = new TopicPartition(topic, partition);
    DataWriter hdfsWriter = createWriter(context, avroData);
    hdfsWriter.recover(tp);

    List<SinkRecord> sinkRecords = createSinkRecords(7);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();
  }

  private DataWriter createWriter(SinkTaskContext context, AvroData avroData){
    return new DataWriter(connectorConfig, context, avroData);
  }

}

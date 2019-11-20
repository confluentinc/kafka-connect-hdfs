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

package io.confluent.connect.hdfs.avro;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.hive.HiveTestBase;
import io.confluent.connect.hdfs.hive.HiveTestUtils;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class AvroHiveUtilTest extends HiveTestBase {
  private HiveUtil hive;
  private Map<String, String> localProps = new HashMap<>();
  private static final Logger log = LoggerFactory.getLogger(AvroHiveUtilTest.class);

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    hive = new AvroHiveUtil(connectorConfig, avroData, hiveMetaStore);
  }

  public void setUp(boolean isLogicalEnabled) throws Exception {
    super.setUp();
    Map<String, String> originals = connectorConfig.originalsStrings();
    hive = new AvroHiveUtil(new HdfsSinkConnectorConfig(originals), avroData, hiveMetaStore);
  }

  @Test
  public void testCreateTable() throws Exception {
    setUp();
    prepareData(TOPIC, PARTITION);
    Partitioner partitioner = HiveTestUtils.getPartitioner(parsedConfig);

    Schema schema = createSchema();
    hive.createTable(hiveDatabase, TOPIC, schema, partitioner);
    String location = "partition=" + String.valueOf(PARTITION);
    hiveMetaStore.addPartition(hiveDatabase, TOPIC, location);

    Struct expectedRecord = createRecord(schema);
    List<String> expectedResult = new ArrayList<>();
    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
      expectedResult.add(String.valueOf(expectedRecord.get(field.name())));
    }

    Table table = hiveMetaStore.getTable(hiveDatabase, TOPIC);
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
        "SELECT * FROM " + hiveMetaStore.tableNameConverter(TOPIC)
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
    Schema schema = createSchema();
    hive.createTable(hiveDatabase, TOPIC, schema, partitioner);

    String location = "partition=" + String.valueOf(PARTITION);
    hiveMetaStore.addPartition(hiveDatabase, TOPIC, location);

    Struct expectedRecord = createRecord(schema);
    List<String> expectedResult = new ArrayList<>();
    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
      expectedResult.add(String.valueOf(expectedRecord.get(field.name())));
    }

    Table table = hiveMetaStore.getTable(hiveDatabase, TOPIC);
    List<String> actualColumnNames = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      actualColumnNames.add(column.getName());
    }

    assertEquals(expectedColumnNames, actualColumnNames);

    Schema newSchema = createNewSchema();

    hive.alterSchema(hiveDatabase, TOPIC, newSchema);

    String result = HiveTestUtils.runHive(
        hiveExec,
        "SELECT * from " + hiveMetaStore.tableNameConverter(TOPIC)
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

//  @Test
//  public void testStripOffLogicalTypes() throws Exception {
//    setUp(true);
//    prepareLogicalData(TOPIC, PARTITION);
//    Partitioner partitioner = HiveTestUtils.getPartitioner(parsedConfig);
//
//    Schema schema = createLogicalSchema();
//    hive.createTable(hiveDatabase, TOPIC, schema, partitioner);
//    String location = "partition=" + String.valueOf(PARTITION);
//    hiveMetaStore.addPartition(hiveDatabase, TOPIC, location);
//
//    Struct expectedRecord = createLogicalRecord(schema);
//    List<String> expectedResult = new ArrayList<>();
//    List<String> expectedColumnNames = new ArrayList<>();
//    for (Field field : schema.fields()) {
//      expectedColumnNames.add(field.name());
//      expectedResult.add(String.valueOf(expectedRecord.get(field.name())));
//    }
//
//    Table table = hiveMetaStore.getTable(hiveDatabase, TOPIC);
//    List<String> actualColumnNames = new ArrayList<>();
//    for (FieldSchema column : table.getSd().getCols()) {
//      actualColumnNames.add(column.getName().toUpperCase());
//    }
//
//    assertEquals(expectedColumnNames, actualColumnNames);
//
//    for (FieldSchema column : table.getSd().getCols()) {
//      log.error(column.getType());
//    }
//    setUp();
//
//    Schema schema = AvroHiveUtil.stripOffLogicalType(createLogicalSchema());
//    for (Field f : schema.fields()) {
//      assertNull(f.schema().name());
//      Map<String, String> props = f.schema().parameters();
//      if (props != null)
//      assertFalse(props.containsKey(AvroHiveUtil.AVRO_LOGICAL_TYPE_PROP));
//    }

//    String result = HiveTestUtils.runHive(
//        hiveExec,
//        "SELECT * FROM " + hiveMetaStore.tableNameConverter(TOPIC)
//    );
//    String[] rows = result.split("\n");
//    // Only 6 of the 7 records should have been delivered due to flush_size = 3
//    assertEquals(6, rows.length);
//    for (String row : rows) {
//      String[] parts = HiveTestUtils.parseOutput(row);
//      int j = 0;
//      for (String expectedValue : expectedResult) {
//        assertEquals(expectedValue, parts[j++]);
//      }
//    }
  //}

  private void prepareData(String topic, int partition) throws Exception {
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
//
//  private Schema createLogicalSchema() {
//    return SchemaBuilder.struct()
//        .field("DECIMAL_COLUMN", Decimal.schema(10))
//        .field("TIMESTAMP_COLUMN", Timestamp.SCHEMA)
//        .field("DATE_COLUMN", Date.SCHEMA)
//        .field("TIME_COLUMN", Time.SCHEMA)
//        .build();
//  }
//
//  private Struct createLogicalRecord(Schema schema) {
//    Struct struct = new Struct(schema);
//    for (Field field : schema.fields()) {
//      String name = field.name();
//      switch (name) {
//        case "DECIMAL_COLUMN":
//          struct = struct.put(name, new BigDecimal(new BigInteger("1000000"), 10));
//          break;
//        case "DATE_COLUMN":
//          struct = struct.put(
//              name,
//              java.util.Date.from(
//                  Instant.EPOCH
//                      .plusMillis(TimeUnit.DAYS.toMillis(1))
//              )
//          );
//          break;
//        case "TIME_COLUMN":
//        case "TIMESTAMP_COLUMN":
//          struct = struct.put(
//              name,
//              java.util.Date.from(
//                  Instant.EPOCH
//                      .plusSeconds(10)
//              )
//          );
//      }
//    }
//    return struct;
//  }
//
//  private List<SinkRecord> logicalRecordsProvider(int num) {
//    Schema schema = createLogicalSchema();
//    List<Struct> same = new ArrayList<>();
//    for (int i = 0; i < num; ++i) {
//      same.add(createLogicalRecord(schema));
//    }
//    return createSinkRecords(same, schema, 0, Collections
//        .singleton(new TopicPartition(TOPIC, PARTITION)));
//  }
//
//  private void prepareLogicalData(String topic, int partition) throws Exception {
//    TopicPartition tp = new TopicPartition(topic, partition);
//    DataWriter hdfsWriter = createWriter(context, avroData);
//    hdfsWriter.recover(tp);
//
//    List<SinkRecord> sinkRecords = logicalRecordsProvider(7);
//
//    hdfsWriter.write(sinkRecords);
//    hdfsWriter.close();
//    hdfsWriter.stop();
//  }
}

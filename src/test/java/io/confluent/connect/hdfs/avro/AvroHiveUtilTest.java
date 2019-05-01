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

import io.confluent.connect.storage.hive.HiveSchemaConverter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.hive.HiveTestBase;
import io.confluent.connect.hdfs.hive.HiveTestUtils;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;

import static org.junit.Assert.assertEquals;

public class AvroHiveUtilTest extends HiveTestBase {
  private HiveUtil hive;
  private Map<String, String> localProps = new HashMap<>();

  private static int SCALE = 3;
  private static String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";
  private static String CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT = "38";

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

  @Test
  public void testLogicalType() throws Exception {
    setUp();
    prepareData(TOPIC, PARTITION);
    Partitioner partitioner = HiveTestUtils.getPartitioner(parsedConfig);

    Schema schema = createSchemaAllLogical();
    hive.createTable(hiveDatabase, TOPIC, schema, partitioner);
    String location = "partition=" + String.valueOf(PARTITION);
    hiveMetaStore.addPartition(hiveDatabase, TOPIC, location);

    String precision = schema
        .field("decimal")
        .schema()
        .parameters()
        .getOrDefault(CONNECT_AVRO_DECIMAL_PRECISION_PROP, CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT);
    String scale = SCALE + "" ;

    List<String> expectedColumnNames = new ArrayList<>();
    for (Field field : schema.fields()) {
      expectedColumnNames.add(field.name());
    }
    List<String> expectedColumnTypes = new ArrayList<>();
    expectedColumnTypes.add(serdeConstants.BINARY_TYPE_NAME);
    expectedColumnTypes.add(serdeConstants.INT_TYPE_NAME);
    expectedColumnTypes.add(serdeConstants.BIGINT_TYPE_NAME);
    expectedColumnTypes.add(serdeConstants.DECIMAL_TYPE_NAME + "(" + precision + "," + scale + ")");
    expectedColumnTypes.add(serdeConstants.DATE_TYPE_NAME);
    expectedColumnTypes.add(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
    expectedColumnTypes.add(serdeConstants.TIMESTAMP_TYPE_NAME);

    Table table = hiveMetaStore.getTable(hiveDatabase, TOPIC);
    List<String> actualColumnNames = new ArrayList<>();
    List<String> actualColumnTypes = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      actualColumnNames.add(column.getName());
      actualColumnTypes.add(column.getType());
    }

    assertEquals(expectedColumnNames, actualColumnNames);
    assertEquals(expectedColumnTypes, actualColumnTypes);
  }


  //move this later to StorageSinkTestBase
  public static Schema createSchemaAllLogical() {
    return SchemaBuilder.struct().name("record").version(1)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("decimal", Decimal.schema(SCALE))
        .field("date", Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .build();
  }

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
}

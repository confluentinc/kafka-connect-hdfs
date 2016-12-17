/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.After;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.StorageSinkTestBase;

public class HdfsSinkConnectorTestBase extends StorageSinkTestBase {

  protected Configuration conf;
  protected HdfsSinkConnectorConfig connectorConfig;
  protected String topicsDir;
  protected String logsDir;
  protected AvroData avroData;

  protected static final String TOPIC_WITH_DOTS = "topic.with.dots";
  protected static final TopicPartition TOPIC_WITH_DOTS_PARTITION = new TopicPartition(TOPIC_WITH_DOTS, PARTITION);

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();
    props.put(HdfsSinkConnectorConfig.HDFS_URL_CONFIG, url);
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "3");
    return props;
  }

  protected Struct createRecord(Schema schema, int ibase, float fbase) {
    return new Struct(schema)
        .put("boolean", true)
        .put("int", ibase)
        .put("long", (long) ibase)
        .put("float", fbase)
        .put("double", (double) fbase);
  }

  // Create a batch of records with incremental numeric field values. Total number of records is
  // given by 'size'.
  protected List<Struct> createRecordBatch(Schema schema, int size) {
    ArrayList<Struct> records = new ArrayList<>(size);
    int ibase = 16;
    float fbase = 12.2f;

    for (int i = 0; i < size; ++i) {
      records.add(createRecord(schema, ibase + i, fbase + i));
    }
    return records;
  }

  // Create a list of records by repeating the same record batch. Total number of records: 'batchesNum' x 'batchSize'
  protected List<Struct> createRecordBatches(Schema schema, int batchSize, int batchesNum) {
    ArrayList<Struct> records = new ArrayList<>();
    for (int i = 0; i < batchesNum; ++i) {
      records.addAll(createRecordBatch(schema, batchSize));
    }
    return records;
  }

  //@Before
  @Override
  public void setUp() throws Exception {
    conf = new Configuration();
    url = "memory://";
    super.setUp();
    // Configure immediately in setup for common case of just using this default. Subclasses can
    // re-call this safely.
    configureConnector();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  protected void configureConnector() {
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPICS_DIR_CONFIG);
    logsDir = connectorConfig.getString(HdfsSinkConnectorConfig.LOGS_DIR_CONFIG);
    int schemaCacheSize = connectorConfig.getInt(HdfsSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG);
    avroData = new AvroData(schemaCacheSize);
  }
}

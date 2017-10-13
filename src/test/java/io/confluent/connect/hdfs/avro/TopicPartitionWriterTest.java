/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.hdfs.avro;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.MockTime;
import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.TopicPartitionWriter;
import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.partitioner.DefaultPartitioner;
import io.confluent.connect.hdfs.partitioner.FieldPartitioner;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner;
import io.confluent.connect.hdfs.partitioner.TimeUtils;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TopicPartitionWriterTest extends TestWithMiniDFSCluster {
  private RecordWriterProvider writerProvider = null;
  private io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig>
      newWriterProvider;
  private HdfsStorage storage;
  private Map<String, String> localProps = new HashMap<>();
  private MockTime time;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    time = new MockTime();

    @SuppressWarnings("unchecked")
    Class<? extends HdfsStorage> storageClass = (Class<? extends HdfsStorage>)
        connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
    storage = StorageFactory.createStorage(
        storageClass,
        HdfsSinkConnectorConfig.class,
        connectorConfig,
        url
    );
    @SuppressWarnings("unchecked")
    Class<io.confluent.connect.storage.format.Format> formatClass
        = (Class<io.confluent.connect.storage.format.Format>) connectorConfig.getClass(
        HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG
    );
    io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> format
        = formatClass.getConstructor(HdfsStorage.class).newInstance(storage);
    writerProvider = null;
    newWriterProvider = format.getRecordWriterProvider();
    dataFileReader = new AvroDataFileReader();
    extension = newWriterProvider.getExtension();
    createTopicDir(url, topicsDir, TOPIC);
    createLogsDir(url, logsDir);
  }

  @Test
  public void testWriteRecordDefaultWithPadding() throws Exception {
    localProps.put(HdfsSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
    setUp();
    Partitioner partitioner = new DefaultPartitioner();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    // Add a single records at the end of the batches sequence. Total records: 10
    records.add(createRecord(schema));
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    Set<Path> expectedFiles = new HashSet<>();
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
                               "/" + TOPIC + "+" + PARTITION + "+00+02" + extension));
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
                               "/" + TOPIC + "+" + PARTITION + "+03+05" + extension));
    expectedFiles.add(new Path(url + "/" + topicsDir + "/" + TOPIC + "/partition=" + PARTITION +
                               "/" + TOPIC + "+" + PARTITION + "+06+08" + extension));
    int expectedBatchSize = 3;
    verify(expectedFiles, expectedBatchSize, records, schema);
  }

  @Test
  public void testWriteRecordFieldPartitioner() throws Exception {
    setUp();
    Partitioner partitioner = new FieldPartitioner();
    partitioner.configure(parsedConfig);

    String partitionField = (String) parsedConfig.get(
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG
    );

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchema();
    List<Struct> records = new ArrayList<>();
    for (int i = 16; i < 19; ++i) {
      for (int j = 0; j < 3; ++j) {
        records.add(createRecord(schema, i, 12.2f));

      }
    }
    // Add a single records at the end of the batches sequence. Total records: 10
    records.add(createRecord(schema));
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    String directory1 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(16));
    String directory2 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(17));
    String directory3 = partitioner.generatePartitionedPath(TOPIC, partitionField + "=" + String.valueOf(18));

    Set<Path> expectedFiles = new HashSet<>();
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory1, TOPIC_PARTITION, 0, 2, extension, zeroPadFormat)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory2, TOPIC_PARTITION, 3, 5, extension, zeroPadFormat)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory3, TOPIC_PARTITION, 6, 8, extension, zeroPadFormat)));

    int expectedBatchSize = 3;
    verify(expectedFiles, expectedBatchSize, records, schema);
  }

  @Test
  public void testWriteRecordTimeBasedPartition() throws Exception {
    setUp();
    Partitioner partitioner = new TimeBasedPartitioner();
    partitioner.configure(parsedConfig);

    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData,
        time
    );

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    // Add a single records at the end of the batches sequence. Total records: 10
    records.add(createRecord(schema));
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    long partitionDurationMs = (Long) parsedConfig.get(
        PartitionerConfig.PARTITION_DURATION_MS_CONFIG
    );
    String pathFormat = (String) parsedConfig.get(PartitionerConfig.PATH_FORMAT_CONFIG);
    String timeZoneString = (String) parsedConfig.get(PartitionerConfig.TIMEZONE_CONFIG);
    long timestamp = System.currentTimeMillis();

    String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat, timeZoneString, timestamp);

    String directory = partitioner.generatePartitionedPath(TOPIC, encodedPartition);

    Set<Path> expectedFiles = new HashSet<>();
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, 0, 2, extension, zeroPadFormat)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, 3, 5, extension, zeroPadFormat)));
    expectedFiles.add(new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, 6, 8, extension, zeroPadFormat)));

    int expectedBatchSize = 3;
    verify(expectedFiles, expectedBatchSize, records, schema);
  }

  private void createTopicDir(String url, String topicsDir, String topic) throws IOException {
    Path path = new Path(FileUtils.topicDirectory(url, topicsDir, topic));
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  private void createLogsDir(String url, String logsDir) throws IOException {
    Path path = new Path(url + "/" + logsDir);
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  private void verify(Set<Path> expectedFiles, int expectedSize, List<Struct> records, Schema schema) throws IOException {
    Path path = new Path(FileUtils.topicDirectory(url, topicsDir, TOPIC));
    FileStatus[] statuses = FileUtils.traverse(storage, path, new CommittedFileFilter());
    assertEquals(expectedFiles.size(), statuses.length);
    int index = 0;
    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      assertTrue(expectedFiles.contains(status.getPath()));
      Collection<Object> avroRecords = dataFileReader.readData(connectorConfig.getHadoopConfiguration(), filePath);
      assertEquals(expectedSize, avroRecords.size());
      for (Object avroRecord : avroRecords) {
        assertEquals(avroData.fromConnectData(schema, records.get(index++)), avroRecord);
      }
    }
  }

}

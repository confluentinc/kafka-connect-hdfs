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

import io.confluent.connect.hdfs.wal.FSWAL;
import io.confluent.connect.hdfs.wal.WALFile.Writer;
import io.confluent.connect.hdfs.wal.WALFileTest;
import io.confluent.connect.hdfs.wal.WALFileTest.CorruptWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.Time;
import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.wal.WAL;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataWriterAvroTest extends TestWithMiniDFSCluster {

  private static final Logger log = LoggerFactory.getLogger(DataWriterAvroTest.class);

  @Before
  public void setUp() throws Exception {
    super.setUp();
    dataFileReader = new AvroDataFileReader();
    extension = ".avro";
  }

  @Test
  public void testWriteRecord() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecords(7);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testRecovery() throws Exception {
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    fs.delete(new Path(FileUtils.directoryName(url, topicsDir, TOPIC_PARTITION)), true);

    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();

    WAL wal = storage.wal(logsDir, TOPIC_PARTITION);

    wal.append(WAL.beginMarker, "");

    for (int i = 0; i < 5; ++i) {
      long startOffset = i * 10;
      long endOffset = (i + 1) * 10 - 1;
      String tempfile = FileUtils.tempFileName(url, topicsDir, getDirectory(), extension);
      fs.createNewFile(new Path(tempfile));
      String committedFile = FileUtils.committedFileName(url, topicsDir, getDirectory(), TOPIC_PARTITION, startOffset,
                                                         endOffset, extension, zeroPadFormat);
      wal.append(tempfile, committedFile);
    }
    wal.append(WAL.endMarker, "");
    wal.close();

    hdfsWriter.recover(TOPIC_PARTITION);
    Map<TopicPartition, Long> offsets = context.offsets();
    assertTrue(offsets.containsKey(TOPIC_PARTITION));
    assertEquals(50L, (long) offsets.get(TOPIC_PARTITION));

    List<SinkRecord> sinkRecords = createSinkRecords(3, 50);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    long[] validOffsets = {0, 10, 20, 30, 40, 50, 53};
    verifyFileListing(validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
  }

  @Test
  public void testCorruptRecovery() throws Exception {
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    fs.delete(new Path(FileUtils.directoryName(url, topicsDir, TOPIC_PARTITION)), true);

    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();

    WAL wal = new FSWAL(logsDir, TOPIC_PARTITION, storage) {
      public void acquireLease() throws ConnectException {
        super.acquireLease();
        // initialize a new writer if the writer is not a CorruptWriter
        if (writer.getClass() != WALFileTest.CorruptWriter.class) {
          try {
            writer = new CorruptWriter(storage.conf(), Writer.file(new Path(this.getLogFile())),
                Writer.appendIfExists(true));
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    };
    wal.append(WAL.beginMarker, "");

    // Write enough bytes to trigger a sync
    for (int i = 0; i < 20; ++i) {
      long startOffset = i * 10;
      long endOffset = (i + 1) * 10 - 1;
      String tempfile = FileUtils.tempFileName(url, topicsDir, getDirectory(), extension);
      fs.createNewFile(new Path(tempfile));
      String committedFile = FileUtils.committedFileName(url, topicsDir, getDirectory(), TOPIC_PARTITION, startOffset,
          endOffset, extension, zeroPadFormat);
      wal.append(tempfile, committedFile);
    }

    wal.append(WAL.endMarker, "");
    wal.close();

    hdfsWriter.recover(TOPIC_PARTITION);
    Map<TopicPartition, Long> offsets = context.offsets();
    // Offsets shouldn't exist since corrupt WAL file entries should not not be committed
    assertFalse(offsets.containsKey(TOPIC_PARTITION));
  }

  @Test
  public void testWriteRecordMultiplePartitions() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();

    for (TopicPartition tp : context.assignment()) {
      hdfsWriter.recover(tp);
    }

    List<SinkRecord> sinkRecords = createSinkRecords(7, 0, context.assignment());

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testWriteInterleavedRecordsInMultiplePartitions() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();

    for (TopicPartition tp : context.assignment()) {
      hdfsWriter.recover(tp);
    }

    List<SinkRecord> sinkRecords = createSinkRecordsInterleaved(7 * context.assignment().size(), 0,
        context.assignment());

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testWriteInterleavedRecordsInMultiplePartitionsNonZeroInitialOffset() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();

    List<SinkRecord> sinkRecords = createSinkRecordsInterleaved(7 * context.assignment().size(), 9,
        context.assignment());

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    long[] validOffsets = {9, 12, 15};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testGetNextOffsets() throws Exception {
    String directory = TOPIC + "/" + "partition=" + String.valueOf(PARTITION);
    String topicsDir = this.topicsDir.get(TOPIC);
    long[] startOffsets = {0, 3};
    long[] endOffsets = {2, 5};

    for (int i = 0; i < startOffsets.length; ++i) {
      Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory, TOPIC_PARTITION, startOffsets[i],
                                                       endOffsets[i], extension, zeroPadFormat));
      fs.createNewFile(path);
    }
    Path path = new Path(FileUtils.tempFileName(url, topicsDir, directory, extension));
    fs.createNewFile(path);

    path = new Path(FileUtils.fileName(url, topicsDir, directory, "abcd"));
    fs.createNewFile(path);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    hdfsWriter.recover(TOPIC_PARTITION);

    Map<TopicPartition, Long> committedOffsets = hdfsWriter.getCommittedOffsets();

    assertTrue(committedOffsets.containsKey(TOPIC_PARTITION));
    long nextOffset = committedOffsets.get(TOPIC_PARTITION);
    assertEquals(6L, nextOffset);

    hdfsWriter.close();
    hdfsWriter.stop();
  }

  @Test
  public void testWriteRecordNonZeroInitialOffset() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecords(7, 3);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    // Last file (offset 9) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {3, 6, 9};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testRebalance() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();

    Set<TopicPartition> originalAssignment = new HashSet<>(context.assignment());
    // Starts with TOPIC_PARTITION and TOPIC_PARTITION2
    for (TopicPartition tp : originalAssignment) {
      hdfsWriter.recover(tp);
    }

    Set<TopicPartition> nextAssignment = new HashSet<>();
    nextAssignment.add(TOPIC_PARTITION);
    nextAssignment.add(TOPIC_PARTITION3);

    List<SinkRecord> sinkRecords = createSinkRecords(7, 0, originalAssignment);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    // Set the new assignment to the context
    context.setAssignment(nextAssignment);
    hdfsWriter.open(nextAssignment);

    assertEquals(null, hdfsWriter.getBucketWriter(TOPIC_PARTITION2));
    assertNotNull(hdfsWriter.getBucketWriter(TOPIC_PARTITION));
    assertNotNull(hdfsWriter.getBucketWriter(TOPIC_PARTITION3));

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsetsTopicPartition2 = {0, 3, 6};
    verify(sinkRecords, validOffsetsTopicPartition2, Collections.singleton(TOPIC_PARTITION2), true);

    // Message offsets start at 6 because we discarded the in-progress temp file on re-balance
    sinkRecords = createSinkRecords(3, 6, context.assignment());

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    // Last file (offset 9) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsetsTopicPartition1 = {6, 9};
    verify(sinkRecords, validOffsetsTopicPartition1, Collections.singleton(TOPIC_PARTITION), true);

    long[] validOffsetsTopicPartition3 = {6, 9};
    verify(sinkRecords, validOffsetsTopicPartition3, Collections.singleton(TOPIC_PARTITION3), true);
  }

  @Test
  public void testProjectBackWard() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecordsWithAlternatingSchemas(7, 0);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();
    long[] validOffsets = {0, 1, 3, 5, 7};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testProjectNone() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecordsWithAlternatingSchemas(7, 0);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    long[] validOffsets = {0, 1, 2, 3, 4, 5, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testProjectForward() throws Exception {
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
    props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "FORWARD");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    // By excluding the first element we get a list starting with record having the new schema.
    List<SinkRecord> sinkRecords = createSinkRecordsWithAlternatingSchemas(8, 0).subList(1, 8);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    long[] validOffsets = {1, 2, 4, 6, 8};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testProjectNoVersion() throws Exception {
    Map<String, String> props = createProps();
    props.put(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecordsNoVersion(1, 0);
    sinkRecords.addAll(createSinkRecordsWithAlternatingSchemas(7, 0));

    try {
      hdfsWriter.write(sinkRecords);
      fail("Version is required for Backward compatibility.");
    } catch (RuntimeException e) {
      // expected
    } finally {
      hdfsWriter.close();
      hdfsWriter.stop();
      long[] validOffsets = {};
      verify(Collections.<SinkRecord>emptyList(), validOffsets);
    }
  }

  @Test
  public void testFlushPartialFile() throws Exception {
    String ROTATE_INTERVAL_MS_CONFIG = "1000";
    // wait for 2 * ROTATE_INTERVAL_MS_CONFIG
    long WAIT_TIME = Long.valueOf(ROTATE_INTERVAL_MS_CONFIG) * 2;

    String FLUSH_SIZE_CONFIG = "10";
    // send 1.5 * FLUSH_SIZE_CONFIG records
    int NUMBER_OF_RECORDS = Integer.valueOf(FLUSH_SIZE_CONFIG) + Integer.valueOf(FLUSH_SIZE_CONFIG) / 2;

    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, FLUSH_SIZE_CONFIG);
    props.put(HdfsSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG, ROTATE_INTERVAL_MS_CONFIG);
    props.put(
        PartitionerConfig.PARTITION_DURATION_MS_CONFIG,
        String.valueOf(TimeUnit.DAYS.toMillis(1))
    );
    props.put(
        PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        TopicPartitionWriterTest.MockedWallclockTimestampExtractor.class.getName()
    );
    props.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        TimeBasedPartitioner.class.getName()
    );
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
    context.assignment().add(TOPIC_PARTITION);

    Time time = TopicPartitionWriterTest.MockedWallclockTimestampExtractor.TIME;
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData, time);
    partitioner = hdfsWriter.getPartitioner();

    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecords(NUMBER_OF_RECORDS);
    hdfsWriter.write(sinkRecords);

    // Wait so everything is committed
    time.sleep(WAIT_TIME);
    hdfsWriter.write(new ArrayList<SinkRecord>());

    Map<TopicPartition, Long> committedOffsets = hdfsWriter.getCommittedOffsets();
    assertTrue(committedOffsets.containsKey(TOPIC_PARTITION));
    long nextOffset = committedOffsets.get(TOPIC_PARTITION);
    assertEquals(NUMBER_OF_RECORDS, nextOffset);

    hdfsWriter.close();
    hdfsWriter.stop();
  }

  @Test
  public void testAvroCompression() throws Exception {
    //set compression codec to Snappy
    Map<String, String> props = createProps();
    props.put(HdfsSinkConnectorConfig.AVRO_CODEC_CONFIG, "snappy");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);

    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecords(7);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);

    // check if the raw bytes have a "avro.codec" entry followed by "snappy"
    List<String> filenames = getExpectedFiles(validOffsets, TOPIC_PARTITION);
    for (String filename : filenames) {
      Path p = new Path(filename);
      try (FSDataInputStream stream = fs.open(p)) {
        int size = (int) fs.getFileStatus(p).getLen();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        if (stream.read(buffer) <= 0) {
          log.error("Could not read file {}", filename);
        }

        String fileContents = new String(buffer.array());
        int index;
        assertTrue((index = fileContents.indexOf("avro.codec")) > 0
            && fileContents.indexOf("snappy", index) > 0);
      }
    }
  }
}


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

package io.confluent.connect.hdfs.wal;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.io.OutputStream;
import java.io.IOException;

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.storage.HdfsStorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FSWALTest extends TestWithMiniDFSCluster {

  @Test
  public void testTruncate() throws Exception {
    setUp();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    TopicPartition tp = new TopicPartition("mytopic", 123);
    FSWAL wal = new FSWAL("/logs", tp, storage);
    wal.append("a", "b");
    assertTrue("WAL file should exist after append",
            storage.exists("/logs/mytopic/123/log"));
    wal.truncate();
    assertFalse("WAL file should not exist after truncate",
            storage.exists("/logs/mytopic/123/log"));
    assertTrue("Rotated WAL file should exist after truncate",
            storage.exists("/logs/mytopic/123/log.1"));
    wal.append("c", "d");
    assertTrue("WAL file should be recreated after truncate + append",
            storage.exists("/logs/mytopic/123/log"));
    assertTrue("Rotated WAL file should exist after truncate + append",
            storage.exists("/logs/mytopic/123/log.1"));
  }
  
  @Test
  public void testEmptyWalFileRecovery() throws Exception {
    setUp();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    TopicPartition tp = new TopicPartition("mytopic", 123);
    fs.create(new Path(FileUtils.logFileName(url, logsDir, tp)), true);
    FSWAL wal = new FSWAL("/logs", tp, storage);
    wal.acquireLease();
  }
  
  @Test
  public void testTruncatedVersionWalFileRecovery() throws Exception {
    setUp();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    TopicPartition tp = new TopicPartition("mytopic", 123);
    OutputStream o = fs.create(new Path(FileUtils.logFileName(url, logsDir, tp)), true);
    o.write(47);
    o.write(61);
    FSWAL wal = new FSWAL("/logs", tp, storage);
    wal.acquireLease();
  }

  @Test
  public void testExtractOffsetsFromPath() {
    List<String> filepaths = Arrays.asList(
        "hdfs://namenode:8020/topics/test_hdfs/f1=value1/test_hdfs+0+0000000000+0000000000.avro",
        "hdfs://namenode:8020/topics/test_hdfs/f1=value6/test_hdfs+0+0000000005+0000000005.avro",
        "hdfs://namenode:8020/topics/test_hdfs/f1=value1/test_hdfs+0+0000000006+0000000009.avro",
        "hdfs://namenode:8020/topics/test_hdfs/f1=value1/test_hdfs+0+0000001034+0000001333.avro",
        "hdfs://namenode:8020/topics/test_hdfs/f1=value1/test_hdfs+0+0123132133+0213314343.avro"
    );
    long[] expectedOffsets = {0, 5, 9, 1333, 213314343};

    int index = 0;
    for (String path : filepaths){
      long extractedOffset = FSWAL.extractOffsetsFromFilePath(path);
      assertEquals(expectedOffsets[index], extractedOffset);
      index++;
    }
  }

  @Test
  public void testOffsetsExtractedFromWALWithEmptyBlocks() throws Exception {
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);

    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);
    //create a few empty blocks
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");

    assertNull(wal.extractLatestOffset());

    addSampleEntriesToWAL(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");

    long latestOffset = wal.extractLatestOffset().getOffset();
    assertEquals(49, latestOffset);
  }

  @Test
  public void testNoOffsetsFromWALWithMissingEndMarkerFirstBlock() throws Exception {
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    // test missing end marker on middle block
    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);

    wal.append(WAL.beginMarker, "");
    addSampleEntriesToWALNoMarkers(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);
    // missing end marker here

    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.endMarker, "");
    wal.close();

    assertNull(wal.extractLatestOffset());
  }

  @Test
  public void testNoOffsetsFromWALWithMissingEndMarkerMiddleBlock() throws Exception {
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    // test missing end marker on middle block
    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);
    //create a few empty blocks
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");

    wal.append(WAL.beginMarker, "");
    addSampleEntriesToWALNoMarkers(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);
    // missing end marker here

    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.endMarker, "");
    wal.close();

    assertNull(wal.extractLatestOffset());
  }

  @Test
  public void testOffsetsFromWALWithMissingEndMarkerLastBlockAndValidPreviousBlock() throws Exception {
    long expectedOffset = 108L;
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    // test missing end marker on middle block
    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);
    wal.append(WAL.beginMarker, "");

    String tempfile = FileUtils.tempFileName(url, topicsDir.get(TOPIC_PARTITION.topic()), getDirectory(), extension);
    fs.createNewFile(new Path(tempfile));
    String committedFile = FileUtils.committedFileName(url, topicsDir.get(TOPIC_PARTITION.topic()), getDirectory(), TOPIC_PARTITION, 9,
        expectedOffset, extension, zeroPadFormat);
    wal.append(tempfile, committedFile);

    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    addSampleEntriesToWALNoMarkers(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);
    wal.close();
    //missing end marker here
    assertEquals(expectedOffset, wal.extractLatestOffset().getOffset());
  }

  @Test
  public void testNoOffsetsFromWALWithMissingEndMarkerLastBlock() throws Exception {
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    // test missing end marker on middle block
    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    addSampleEntriesToWALNoMarkers(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);
    //missing end marker here
    assertNull(wal.extractLatestOffset());
  }

  @Test
  public void testNoOffsetsFromWALWithMissingBeginMarkerFirstBlock() throws Exception {
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);

    //test missing begin marker
    addSampleEntriesToWALNoMarkers(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);
    wal.append(WAL.endMarker, "");
    wal.close();
    assertNull(wal.extractLatestOffset());
  }

  @Test
  public void testNoOffsetsFromWALWithMissingBeginMarkerMiddleBlock() throws Exception {
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);

    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    //test missing begin marker
    addSampleEntriesToWALNoMarkers(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.close();
    assertNull(wal.extractLatestOffset());
  }

  @Test
  public void testNoOffsetsFromWALWithMissingBeginMarkerLastBlock() throws Exception {
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);

    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    wal.append(WAL.beginMarker, "");
    wal.append(WAL.endMarker, "");
    //test missing begin marker
    addSampleEntriesToWALNoMarkers(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);
    wal.append(WAL.endMarker, "");
    wal.close();
    assertNull(wal.extractLatestOffset());
  }

  @Test
  public void testOffsetsExtractedFromWAL() throws Exception {
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);
    addSampleEntriesToWAL(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);

    long latestOffset = wal.extractLatestOffset().getOffset();
    assertEquals(49, latestOffset);
  }

  @Test
  public void testOffsetsExtractedFromOldWAL() throws Exception {
    setupWalTest();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    FSWAL wal = (FSWAL) storage.wal(logsDir, TOPIC_PARTITION);
    addSampleEntriesToWAL(topicsDir.get(TOPIC_PARTITION.topic()), wal, 5);
    //creates old WAL and empties new one
    wal.truncate();

    long latestOffset = wal.extractLatestOffset().getOffset();
    assertEquals(49, latestOffset);
  }

  private void setupWalTest() throws Exception {
    setUp();
    String topicsDir = this.topicsDir.get(TOPIC_PARTITION.topic());
    fs.delete(new Path(FileUtils.directoryName(url, topicsDir, TOPIC_PARTITION)), true);
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
  }

  private void addSampleEntriesToWAL(String topicsDir, WAL wal, int numEntries) throws IOException {
    wal.append(WAL.beginMarker, "");
    addSampleEntriesToWALNoMarkers(topicsDir, wal, numEntries);
    wal.append(WAL.endMarker, "");
    wal.close();
  }

  private void addSampleEntriesToWALNoMarkers(String topicsDir, WAL wal, int numEntries) throws IOException {
    for (int i = 0; i < numEntries; ++i) {
      long startOffset = i * 10;
      long endOffset = (i + 1) * 10 - 1;
      String tempfile = FileUtils.tempFileName(url, topicsDir, getDirectory(), extension);
      fs.createNewFile(new Path(tempfile));
      String committedFile = FileUtils.committedFileName(url, topicsDir, getDirectory(), TOPIC_PARTITION, startOffset,
          endOffset, extension, zeroPadFormat);
      wal.append(tempfile, committedFile);
    }
  }

}

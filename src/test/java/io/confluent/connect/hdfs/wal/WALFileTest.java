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

import java.rmi.server.UID;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.storage.common.StorageCommonConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WALFileTest extends TestWithMiniDFSCluster {

  @Test
  public void testAppend() throws Exception {
    setUp();
    properties.put(HdfsSinkConnectorConfig.TOPIC_CAPTURE_GROUPS_REGEX_CONFIG, "(.*)");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(properties);

    String topic = "topic";
    String topicsDir = connectorConfig.getTopicsDirFromTopic(topic);

    int partition = 0;
    TopicPartition topicPart = new TopicPartition(topic, partition);

    Path file = new Path(FileUtils.logFileName(url, topicsDir, topicPart));

    WALFile.Writer writer = WALFile.createWriter(connectorConfig, WALFile.Writer.file(file));

    WALEntry key1 = new WALEntry("key1");
    WALEntry val1 = new WALEntry("val1");

    WALEntry key2 = new WALEntry("key2");
    WALEntry val2 = new WALEntry("val2");

    writer.append(key1, val1);
    writer.append(key2, val2);
    writer.close();

    verify2Values(file);

    writer = WALFile.createWriter(
        connectorConfig,
        WALFile.Writer.file(file),
        WALFile.Writer.appendIfExists(true)
    );

    WALEntry key3 = new WALEntry("key3");
    WALEntry val3 = new WALEntry("val3");

    WALEntry key4 = new WALEntry("key4");
    WALEntry val4 = new WALEntry("val4");

    writer.append(key3, val3);
    writer.append(key4, val4);
    writer.hsync();
    writer.close();

    verifyAll4Values(file);

    fs.deleteOnExit(file);
  }

  private void verify2Values(Path file) throws IOException {
    WALEntry key1 = new WALEntry("key1");
    WALEntry val1 = new WALEntry("val1");

    WALEntry key2 = new WALEntry("key2");
    WALEntry val2 = new WALEntry("val2");

    WALFile.Reader reader = new WALFile.Reader(conf, WALFile.Reader.file(file));

    assertEquals(key1.getName(), reader.next((WALEntry) null).getName());
    assertEquals(val1.getName(), reader.getCurrentValue(null).getName());
    assertEquals(key2.getName(), reader.next((WALEntry) null).getName());
    assertEquals(val2.getName(), reader.getCurrentValue(null).getName());
    assertNull(reader.next((WALEntry) null));
    reader.close();
  }

  private void verifyAll4Values(Path file) throws IOException {
    WALEntry key1 = new WALEntry("key1");
    WALEntry val1 = new WALEntry("val1");

    WALEntry key2 = new WALEntry("key2");
    WALEntry val2 = new WALEntry("val2");

    WALEntry key3 = new WALEntry("key3");
    WALEntry val3 = new WALEntry("val3");

    WALEntry key4 = new WALEntry("key4");
    WALEntry val4 = new WALEntry("val4");

    WALFile.Reader reader = new WALFile.Reader(conf, WALFile.Reader.file(file));
    assertEquals(key1.getName(), reader.next((WALEntry) null).getName());
    assertEquals(val1.getName(), reader.getCurrentValue(null).getName());
    assertEquals(key2.getName(), reader.next((WALEntry) null).getName());
    assertEquals(val2.getName(), reader.getCurrentValue(null).getName());

    assertEquals(key3.getName(), reader.next((WALEntry) null).getName());
    assertEquals(val3.getName(), reader.getCurrentValue(null).getName());
    assertEquals(key4.getName(), reader.next((WALEntry) null).getName());
    assertEquals(val4.getName(), reader.getCurrentValue(null).getName());
    assertNull(reader.next((WALEntry) null));
    reader.close();
  }

  @Test
  public void testCorruptReadDoesThrowException() throws Exception {
    setUp();
    properties.put(HdfsSinkConnectorConfig.TOPIC_CAPTURE_GROUPS_REGEX_CONFIG, "(.*)");
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(properties);

    String topic = "topic";
    String topicsDir = connectorConfig.getTopicsDirFromTopic(topic);

    int partition = 0;
    TopicPartition topicPart = new TopicPartition(topic, partition);

    Path file = new Path(FileUtils.logFileName(url, topicsDir, topicPart));

    CorruptWriter writer = new CorruptWriter(connectorConfig, WALFile.Writer.file(file));
    // Write enough bytes to trigger a sync
    for (int i = 0; i < 350; i++) {
      writer.append(new WALEntry("key"), new WALEntry("val"));
    }
    writer.close();

    try {
      readAllValues(file);
    } catch (CorruptWalFileException e) {
      fs.deleteOnExit(file);
      return;
    }

    fs.deleteOnExit(file);
    throw new Exception("should have thrown CorruptWALFileException");
  }

  private void readAllValues(Path file) throws IOException {
    WALFile.Reader reader = new WALFile.Reader(conf, WALFile.Reader.file(file));
    WALEntry key = new WALEntry();
    WALEntry value = new WALEntry();
    while (reader.next(key, value)) {
      // do nothing
    }
  }

  /**
   * Class used for tests that require a corrupted WAL file.
   */
  public static class CorruptWriter extends WALFile.Writer {

    public CorruptWriter(HdfsSinkConnectorConfig connectorConfig, Option... opts) throws IOException {
      super(connectorConfig, opts);
    }

    public void changeSync() {
      MessageDigest digester;
      try {
        digester = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException ex) {
        return;
      }
      long time = Time.now();
      digester.update((new UID() + "@" + time).getBytes(Charsets.UTF_8));
      sync = digester.digest();
    }

    public synchronized void append(WALEntry key, WALEntry val) throws IOException {
      super.append(key, val);
      changeSync();
    }
  }

  @Test
  public void testHdfsIsDown() throws Exception {
    setUp();
    HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(properties);
    String topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    String topic = "topic";
    int partition = 0;
    TopicPartition topicPart = new TopicPartition(topic, partition);
    Path file = new Path(FileUtils.logFileName(url, topicsDir, topicPart));
    cluster.shutdown();
    int fileSystemCacheSizeBefore = getFileSystemCacheSize();
    try {
      WALFile.createWriter(connectorConfig, WALFile.Writer.file(file));
    } catch (Exception e) {
      //expected
    }
    assertEquals(fileSystemCacheSizeBefore, getFileSystemCacheSize());
  }
}

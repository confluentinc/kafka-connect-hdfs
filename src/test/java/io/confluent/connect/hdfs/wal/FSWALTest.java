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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Map;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.storage.HdfsStorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
  public void testAcquireLeaseThrowsException() throws Exception {
    setUp();
    HdfsStorage storage = new HdfsStorage(connectorConfig, url);
    TopicPartition tp = new TopicPartition("mytopic", 123);
    //stop HDFS cluster so that WALFile.createWriter() throws an exception
    cluster.shutdown();
    FSWAL wal = new FSWAL("/logs", tp, storage);
    int fileSystemCacheSizeBefore = getFileSystemCacheSize();
    for (int i = 0; i < 10; i++) {
      try {
        wal.acquireLease();
      } catch (Exception e) {
        //expected
      }
    }
    // make sure org.apache.hadoop.fs.FileSystem.CACHE
    // doesn't grow when acquireLease re-throws an exception
    assertEquals(fileSystemCacheSizeBefore, getFileSystemCacheSize());
  }

  private int getFileSystemCacheSize() throws Exception {
    Field cacheField = FileSystem.class.getDeclaredField("CACHE");
    cacheField.setAccessible(true);
    Object cache = cacheField.get(Object.class);
    Field cacheMapField = cache.getClass().getDeclaredField("map");
    cacheMapField.setAccessible(true);
    //suppressing the warning since org.apache.hadoop.fs.FileSystem.Cache.Key has package-level visibility
    @SuppressWarnings("rawtypes") Map cacheMap = (Map) cacheMapField.get(cache);
    cacheField.setAccessible(false);
    cacheMapField.setAccessible(false);
    return cacheMap.size();
  }
}

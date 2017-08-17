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

package io.confluent.connect.hdfs.wal;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.wal.WAL;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WALTest extends TestWithMiniDFSCluster {
  private static final String ZERO_PAD_FMT = "%010d";
  private HdfsStorage storage;

  private boolean closed;
  private static final String extension = ".avro";

  @Test
  public void testMultiWALFromOneDFSClient() throws Exception {
    setUp();
    fs.delete(new Path(FileUtils.directoryName(url, topicsDir, TOPIC_PARTITION)), true);

    @SuppressWarnings("unchecked")
    Class<? extends HdfsStorage> storageClass = (Class<? extends HdfsStorage>) connectorConfig
        .getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
    storage = io.confluent.connect.storage.StorageFactory.createStorage(
        storageClass,
        HdfsSinkConnectorConfig.class,
        connectorConfig,
        url
    );
    final WAL wal1 = storage.wal(topicsDir, TOPIC_PARTITION);
    final FSWAL wal2 = (FSWAL) storage.wal(topicsDir, TOPIC_PARTITION);

    String directory = TOPIC + "/" + String.valueOf(PARTITION);
    final String tempfile = FileUtils.tempFileName(url, topicsDir, directory, extension);
    final String committedFile = FileUtils.committedFileName(
        url,
        topicsDir,
        directory,
        TOPIC_PARTITION,
        0,
        10,
        extension,
        ZERO_PAD_FMT
    );
    fs.createNewFile(new Path(tempfile));

    wal1.acquireLease();
    wal1.append(WAL.beginMarker, "");
    wal1.append(tempfile, committedFile);
    wal1.append(WAL.endMarker, "");

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // holding the lease for time that is less than wal2's initial retry interval, which is 1000 ms.
          Thread.sleep(WALConstants.INITIAL_SLEEP_INTERVAL_MS - 100);
          closed = true;
          wal1.close();
        } catch (ConnectException | InterruptedException e) {
          // Ignored
        }
      }
    });
    thread.start();

    // acquireLease() will try to acquire the lease that wal1 is holding and fail. It will retry after 1000 ms.
    wal2.acquireLease();
    assertTrue(closed);
    wal2.apply();
    wal2.close();

    assertTrue(fs.exists(new Path(committedFile)));
    assertFalse(fs.exists(new Path(tempfile)));
    storage.close();
  }
}

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

import java.nio.file.Paths;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.wal.WALFile.Reader;
import io.confluent.connect.hdfs.wal.WALFile.Writer;

public class FSWAL implements WAL {

  private static final Logger log = LoggerFactory.getLogger(FSWAL.class);
  private static final String OLD_LOG_EXTENSION = ".1";

  private final HdfsSinkConnectorConfig conf;
  private final HdfsStorage storage;
  private final String logFile;

  protected WALFile.Writer writer = null;
  private WALFile.Reader reader = null;

  public FSWAL(String logsDir, TopicPartition topicPart, HdfsStorage storage)
      throws ConnectException {
    this.storage = storage;
    this.conf = storage.conf();
    String url = storage.url();
    logFile = FileUtils.logFileName(url, logsDir, topicPart);
  }

  @Override
  public void append(String tempFile, String committedFile) throws ConnectException {
    try {
      acquireLease();
      WALEntry key = new WALEntry(tempFile);
      WALEntry value = new WALEntry(committedFile);
      writer.append(key, value);
      writer.hsync();
    } catch (IOException e) {
      log.error("Error appending WAL file: {}, {}", logFile, e);
      close();
      throw new DataException(e);
    }
  }

  public void acquireLease() throws ConnectException {
    log.debug("Attempting to acquire lease for WAL file: {}", logFile);
    long sleepIntervalMs = WALConstants.INITIAL_SLEEP_INTERVAL_MS;
    while (sleepIntervalMs < WALConstants.MAX_SLEEP_INTERVAL_MS) {
      try {
        if (writer == null) {
          writer = WALFile.createWriter(conf, Writer.file(new Path(logFile)),
                                        Writer.appendIfExists(true));
          log.debug(
              "Successfully acquired lease, {}-{}, file {}",
              conf.name(),
              conf.getTaskId(),
              logFile
          );
        }
        break;
      } catch (RemoteException e) {
        if (e.getClassName().equals(WALConstants.LEASE_EXCEPTION_CLASS_NAME)) {
          log.warn(
              "Cannot acquire lease on WAL, {}-{}, file {}",
              conf.name(),
              conf.getTaskId(),
              logFile
          );
          try {
            Thread.sleep(sleepIntervalMs);
          } catch (InterruptedException ie) {
            throw new ConnectException(ie);
          }
          sleepIntervalMs = sleepIntervalMs * 2;
        } else {
          throw new ConnectException(e);
        }
      } catch (IOException e) {
        throw new DataException(
            String.format(
                "Error creating writer for log file, %s-%s, file %s",
                conf.name(),
                conf.getTaskId(),
                logFile
            ),
            e
        );
      }
    }
    if (sleepIntervalMs >= WALConstants.MAX_SLEEP_INTERVAL_MS) {
      throw new ConnectException("Cannot acquire lease after timeout, will retry.");
    }
  }

  @Override
  public void apply() throws ConnectException {
    log.debug("Starting to apply WAL: {}", logFile);
    if (!storage.exists(logFile)) {
      log.debug("WAL file does not exist: {}", logFile);
      return;
    }
    acquireLease();
    log.debug("Lease acquired");

    try {
      if (reader == null) {
        reader = new WALFile.Reader(conf.getHadoopConfiguration(), Reader.file(new Path(logFile)));
      }
      commitWalEntriesToStorage();
    } catch (CorruptWalFileException e) {
      log.error("Error applying WAL file '{}' because it is corrupted: {}", logFile, e);
      log.warn("Truncating and skipping corrupt WAL file '{}'.", logFile);
      close();
    } catch (IOException e) {
      log.error("Error applying WAL file: {}, {}", logFile, e);
      close();
      throw new DataException(e);
    }
    log.debug("Finished applying WAL: {}", logFile);
  }

  /**
   * Read all the filepath entries in the WAL file, commit the pending ones to HdfsStorage
   *
   * @throws IOException when the WAL reader is unable to get the next entry
   */
  private void commitWalEntriesToStorage() throws IOException {
    Map<WALEntry, WALEntry> entries = new HashMap<>();
    WALEntry key = new WALEntry();
    WALEntry value = new WALEntry();
    while (reader.next(key, value)) {
      String keyName = key.getName();
      if (keyName.equals(beginMarker)) {
        entries.clear();
      } else if (keyName.equals(endMarker)) {
        commitEntriesToStorage(entries);
      } else {
        WALEntry mapKey = new WALEntry(key.getName());
        WALEntry mapValue = new WALEntry(value.getName());
        entries.put(mapKey, mapValue);
      }
    }
  }

  /**
   * Commit the given WAL file entries to HDFS storage,
   * typically a batch between BEGIN and END markers in the WAL file.
   *
   * @param entries a map of filepath entries containing temp and committed paths
   */
  private void commitEntriesToStorage(Map<WALEntry, WALEntry> entries) {
    for (Map.Entry<WALEntry, WALEntry> entry: entries.entrySet()) {
      String tempFile = entry.getKey().getName();
      String committedFile = entry.getValue().getName();
      if (!storage.exists(committedFile)) {
        storage.commit(tempFile, committedFile);
      }
    }
  }


  /**
   * Extract the latest offset from the WAL file. Attempt with the most recent WAL file,
   * and fall back to the old file if it's not applicable.
   *
   * <p> The old WAL is used when the most recent WAL file has already been truncated
   * and is not existent, which may happen when the connector has flushed all records,
   * and is restarted. </p>
   *
   * @return the latest offset from the WAL file or -1
   */
  public long extractLatestOffsetFromWAL() {
    String oldWALFile = logFile + OLD_LOG_EXTENSION;
    try {
      long latestOffset = -1;
      if (storage.exists(logFile)) {
        log.trace("Restoring offset from WAL file: {}", logFile);
        if (reader == null) {
          reader = new WALFile.Reader(conf.getHadoopConfiguration(),
              Reader.file(new Path(logFile)));
        } else {
          // reset read position after apply()
          reader.seekToFirstRecord();
        }
        ArrayList<String> committedFileBatch = getLastFilledBlockFromWAL(reader);
        // At this point the committedFilenames list will contain the
        // filenames in the last BEGIN-END block of the file. Find the latest offsets among these.
        latestOffset = getLatestOffsetFromList(committedFileBatch);
      }

      // attempt to use old log file if recent WAL is empty or non-existent
      if (latestOffset == -1 && storage.exists(oldWALFile)) {
        log.trace("Could not find offset in log file {}, using {}", logFile, oldWALFile);
        Reader oldFileReader =
            new WALFile.Reader(conf.getHadoopConfiguration(), Reader.file(new Path(oldWALFile)));
        ArrayList<String> committedFileBatch = getLastFilledBlockFromWAL(oldFileReader);
        latestOffset = getLatestOffsetFromList(committedFileBatch);;
        oldFileReader.close();
      }
      return latestOffset;
    } catch (IOException e) {
      log.warn("Error restoring offsets from WAL files {} and {}", logFile, oldWALFile);
      return -1;
    }
  }

  /**
   * Extract the last filled BEGIN-END block of entries from the WAL.
   *
   * @param reader the WAL file reader
   * @return the last batch of entries, may be empty if the WAL
   *         is empty or only contains empty BEGIN-END blocks
   * @throws IOException error on reading the WAL file
   */
  private ArrayList<String> getLastFilledBlockFromWAL(Reader reader) throws IOException {
    //temp filenames don't contain offset info, use committed only (WAL entry values)
    ArrayList<String> committedFilenames = new ArrayList<>();

    WALEntry key = new WALEntry();
    WALEntry value = new WALEntry();
    boolean wasBeginMarker = false;

    // The entry with the latest offsets will be in the last BEGIN-END block of the file.
    // There may be empty BEGIN and END blocks as well.
    while (reader.next(key, value)) {
      String keyName = key.getName();
      if (keyName.equals(beginMarker)) {
        wasBeginMarker = true;
      } else if (!keyName.equals(endMarker)) {
        if (wasBeginMarker) {
          // if we hit a filename entry and the previous token was a BEGIN marker
          // we start a new BEGIN-END block
          committedFilenames.clear();
        }
        committedFilenames.add(value.getName());
        wasBeginMarker = false;
      }
    }
    return committedFilenames;
  }

  /**
   * Extract the offsets from the given filenames and find the latest.
   *
   * @param committedFileNames a list of committed filenames
   * @return the latest offset committed
   */
  private long getLatestOffsetFromList(ArrayList<String> committedFileNames) {
    // Entries in the BEGIN-END block are currently not guaranteed any ordering.
    long latestOffset = -1;
    for (String fileName : committedFileNames) {
      long currentOffset = extractOffsetsFromFilePath(fileName);
      if (currentOffset > latestOffset) {
        latestOffset = currentOffset;
      }
    }
    return latestOffset;
  }

  /**
   * Extract the file offset from the full file path.
   *
   * @param fullPath the full HDFS file path
   * @return the offset or -1 if not present
   */
  static long extractOffsetsFromFilePath(String fullPath) {
    try {
      if (fullPath != null) {
        String latestFileName = Paths.get(fullPath).getFileName().toString();
        return FileUtils.extractOffset(latestFileName);
      }
    } catch (IllegalArgumentException e) {
      log.warn("Could not extract offsets from file path: {}", fullPath);
    }
    return -1;
  }

  @Override
  public void truncate() throws ConnectException {
    log.debug("Truncating WAL file: {}", logFile);
    try {
      String oldLogFile = logFile + OLD_LOG_EXTENSION;
      storage.delete(oldLogFile);
      storage.commit(logFile, oldLogFile);
    } finally {
      close();
    }
  }

  @Override
  public void close() throws ConnectException {
    log.debug(
        "Closing WAL, {}-{}, file: {}",
        conf.name(),
        conf.getTaskId(),
        logFile
    );
    try {
      if (writer != null) {
        writer.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new DataException("Error closing " + logFile, e);
    } finally {
      writer = null;
      reader = null;
    }
  }

  @Override
  public String getLogFile() {
    return logFile;
  }
}

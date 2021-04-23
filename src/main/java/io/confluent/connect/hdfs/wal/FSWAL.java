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
import org.apache.hadoop.ipc.RemoteException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.wal.WALFile.Reader;
import io.confluent.connect.hdfs.wal.WALFile.Writer;
import io.confluent.connect.storage.wal.FilePathOffset;

public class FSWAL implements WAL {

  private static final Logger log = LoggerFactory.getLogger(FSWAL.class);
  private static final String TRUNCATED_LOG_EXTENSION = ".1";

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
        reader = newWalFileReader(logFile);
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
   * Extract the latest offset and file path from the WAL file.
   * Attempt with the most recent WAL file and fall back to the old file if it's not applicable.
   *
   * <p> The old WAL is used when the most recent WAL file has already been truncated
   * and does not exist, which may happen when the connector is restarted after having flushed
   * all the records. </p>
   *
   * <p> If the recent WAL exists but is corrupted, using the old WAL is not applicable. The old
   * WAL would contain older offsets than the files in HDFS. In this case null is returned. </p>
   *
   * @return the latest offset and filepath from the WAL file or null
   */
  @Override
  public FilePathOffset extractLatestOffset() {
    String oldWALFile = logFile + TRUNCATED_LOG_EXTENSION;
    try {
      FilePathOffset latestOffset = null;
      if (storage.exists(logFile)) {
        log.trace("Restoring offset from WAL file: {}", logFile);
        if (reader == null) {
          reader = newWalFileReader(logFile);
        } else {
          // reset read position after apply()
          reader.seekToFirstRecord();
        }
        List<String> committedFileBatch = getLastFilledBlockFromWAL(reader);
        // At this point the committedFilenames list will contain the
        // filenames in the last BEGIN-END block of the file. Find the latest offsets among these.
        latestOffset = getLatestOffsetFromList(committedFileBatch);
      }

      // attempt to use old log file if recent WAL is empty or non-existent
      if (latestOffset == null && storage.exists(oldWALFile)) {
        log.trace("Could not find offset in log file {}. Using {} instead", logFile, oldWALFile);
        try (Reader oldFileReader = newWalFileReader(oldWALFile)) {
          List<String> committedFileBatch = getLastFilledBlockFromWAL(oldFileReader);
          latestOffset = getLatestOffsetFromList(committedFileBatch);
        }
      }
      return latestOffset;
    } catch (IOException e) {
      log.warn(
          "Error restoring offsets from either {} or {} WAL files: {}",
          logFile,
          oldWALFile,
          e.getMessage()
      );
      return null;
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
  private List<String> getLastFilledBlockFromWAL(Reader reader) throws IOException {
    // In a WAL entry the temp filenames (keys) don't contain offset info,
    // so we only need to track the committed filename (values).
    List<String> committedFilenames = Collections.emptyList();
    // enables skipping corrupted blocks that are missing an END marker,
    // only valid blocks will be moved to committedFilenames
    List<String> tempFilenames = new ArrayList<>();
    WALEntry key = new WALEntry();
    WALEntry value = new WALEntry();

    // whether a BEGIN-END block was started
    boolean entryBlockStarted = false;

    // The entry with the latest offsets will be in the last BEGIN-END block of the file.
    // There may be empty BEGIN and END blocks as well, skip over these and only keep the
    // entries of the last filled block.
    while (reader.next(key, value)) {
      String keyName = key.getName();
      if (keyName.equals(beginMarker)) {
        tempFilenames.clear();
        entryBlockStarted = true;
      } else if (keyName.equals(endMarker)) {
        if (entryBlockStarted && !tempFilenames.isEmpty()) {
          // only save non-empty blocks
          committedFilenames = new ArrayList<>(tempFilenames);
        }
        tempFilenames.clear();
        entryBlockStarted = false;
      } else {
        // file path entry
        if (entryBlockStarted) {
          tempFilenames.add(value.getName());
        }
      }
    }

    if (entryBlockStarted && !tempFilenames.isEmpty()) {
      // the last filled BEGIN-END block was missing an END,
      // these entries would be skipped by apply() so they
      // shouldn't be used to infer latest offset information
      log.warn("The last file block in the WAL is missing an END token");
    }
    return committedFilenames;
  }

  /**
   * Extract the offsets from the given filenames and find the latest offset and filepath.
   *
   * @param committedFileNames a list of committed filenames
   * @return the latest offset committed along with the filepath
   */
  private FilePathOffset getLatestOffsetFromList(List<String> committedFileNames) {
    FilePathOffset latestOffsetEntry = null;
    long latestOffset = -1;
    // Entries in the BEGIN-END block are currently not guaranteed any ordering.
    for (String fileName : committedFileNames) {
      long currentOffset = extractOffsetsFromFilePath(fileName);
      if (currentOffset > latestOffset) {
        latestOffset = currentOffset;
        latestOffsetEntry = new FilePathOffset(latestOffset, fileName);
      }
    }
    return latestOffsetEntry;
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
      log.warn("Could not extract offsets from file path {}: {}", fullPath, e.getMessage());
    }
    return -1;
  }

  private Reader newWalFileReader(String logFile) throws IOException {
    return new Reader(conf.getHadoopConfiguration(), Reader.file(new Path(logFile)));
  }

  @Override
  public void truncate() throws ConnectException {
    try {
      if (storage.exists(logFile)) {
        log.debug("Truncating WAL file: {}", logFile);
        // The old WAL file should only be deleted if there is a new one to replace it.
        // Otherwise the old log file will be lost on 2+ restarts with an empty buffer.
        String oldLogFile = logFile + TRUNCATED_LOG_EXTENSION;
        storage.delete(oldLogFile);
        storage.commit(logFile, oldLogFile);
      }
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

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

package io.confluent.connect.hdfs.utils;

import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.wal.WAL;

public class MemoryStorage extends HdfsStorage {

  private static final Map<String, List<Object>> data = Data.getData();
  private HdfsSinkConnectorConfig conf;
  private String url;
  private Failure failure = Failure.noFailure;

  public enum Failure {
    noFailure,
    listStatusFailure,
    appendFailure,
    mkdirsFailure,
    existsFailure,
    deleteFailure,
    commitFailure,
    closeFailure
  }

  public MemoryStorage(HdfsSinkConnectorConfig conf, String url) {
    super(conf, url, null);
    this.conf = conf;
    this.url = url;
  }

  @Override
  public List<FileStatus> list(String path) {
    List<FileStatus> result = new ArrayList<>();
    for (String key: data.keySet()) {
      if (key.startsWith(path)) {
        FileStatus status = new FileStatus(data.get(key).size(), false, 1, 0, 0, 0, null, null, null, new Path(key));
        result.add(status);
      }
    }
    return result;
  }

  public List<FileStatus> list(String path, PathFilter filter) {
    if (failure == Failure.listStatusFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("listStatus failed.");
    }
    List<FileStatus> result = new ArrayList<>();
    for (String key: data.keySet()) {
      if (key.startsWith(path) && filter.accept(new Path(key))) {
          FileStatus status = new FileStatus(data.get(key).size(), false, 1, 0, 0, 0, null, null, null, new Path(key));
          result.add(status);
      }
    }
    return result;
  }

  public void append(String filename, Object object) {
    if (failure == Failure.appendFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("append failed.");
    }
    if (!data.containsKey(filename)) {
      data.put(filename, new LinkedList<>());
    }
    data.get(filename).add(object);
  }

  @Override
  public boolean create(String filename) {
    if (failure == Failure.mkdirsFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("mkdirs failed.");
    }
    return true;
  }

  @Override
  public boolean exists(String filename) {
    if (failure == Failure.existsFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("exists failed.");
    }
    return data.containsKey(filename);
  }

  @Override
  public void delete(String filename) {
    if (failure == Failure.deleteFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("delete failed.");
    }
    if (data.containsKey(filename)) {
      data.get(filename).clear();
      data.remove(filename);
    }
  }

  @Override
  public void commit(String tempFile, String committedFile) {
    if (failure == Failure.commitFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("commit failed.");
    }
    if (!data.containsKey(committedFile)) {
      List<Object> entryList = data.get(tempFile);
      data.put(committedFile, entryList);
      data.remove(tempFile);
    }
  }

  @Override
  public void close() {
    if (failure == Failure.closeFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("close failed.");
    }
    data.clear();
  }

  @Override
  public WAL wal(String topicsDir, TopicPartition topicPart) {
    return new MemoryWAL(topicsDir, topicPart, this);
  }

  @Override
  public HdfsSinkConnectorConfig conf() {
    return conf;
  }

  @Override
  public String url() {
    return url;
  }

  public void setFailure(Failure failure) {
    this.failure = failure;
  }

  @Override
  public SeekableInput open(String filename, HdfsSinkConnectorConfig conf) {
    return null;
  }

  @Override
  public OutputStream create(String filename, HdfsSinkConnectorConfig conf, boolean overwrite) {
    return null;
  }
}

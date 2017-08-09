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

package io.confluent.connect.hdfs.storage;

import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.wal.FSWAL;
import io.confluent.connect.hdfs.wal.WAL;

public class HdfsStorage
    implements io.confluent.connect.storage.Storage<HdfsSinkConnectorConfig, List<FileStatus>>,
    Storage {

  private final FileSystem fs;
  private final HdfsSinkConnectorConfig conf;
  private final String url;

  // Visible for testing.
  protected HdfsStorage(HdfsSinkConnectorConfig conf,  String url, FileSystem fs) {
    this.conf = conf;
    this.url = url;
    this.fs = fs;
  }

  public HdfsStorage(HdfsSinkConnectorConfig conf,  String url) throws IOException {
    this.conf = conf;
    this.url = url;
    fs = FileSystem.newInstance(URI.create(url), conf.getHadoopConfiguration());
  }

  public List<FileStatus> list(String path, PathFilter filter) {
    try {
      return Arrays.asList(fs.listStatus(new Path(path), filter));
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public List<FileStatus> list(String path) {
    try {
      return Arrays.asList(fs.listStatus(new Path(path)));
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public OutputStream append(String filename) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean create(String filename) {
    try {
      return fs.mkdirs(new Path(filename));
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public boolean exists(String filename) {
    try {
      return fs.exists(new Path(filename));
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public void commit(String tempFile, String committedFile) {
    renameFile(tempFile, committedFile);
  }

  @Override
  public void delete(String filename) {
    try {
      fs.delete(new Path(filename), true);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public void close() {
    if (fs != null) {
      try {
        fs.close();
      } catch (IOException e) {
        throw new ConnectException(e);
      }
    }
  }

  public WAL wal(String topicsDir, TopicPartition topicPart) {
    return new FSWAL(topicsDir, topicPart, this);
  }

  @Override
  public HdfsSinkConnectorConfig conf() {
    return conf;
  }

  @Override
  public String url() {
    return url;
  }

  private void renameFile(String sourcePath, String targetPath) {
    if (sourcePath.equals(targetPath)) {
      return;
    }
    try {
      final Path srcPath = new Path(sourcePath);
      final Path dstPath = new Path(targetPath);
      if (fs.exists(srcPath)) {
        fs.rename(srcPath, dstPath);
      }
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public SeekableInput open(String filename, HdfsSinkConnectorConfig conf) {
    try {
      return new FsInput(new Path(filename), conf.getHadoopConfiguration());
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  public OutputStream create(String filename, boolean overwrite) {
    return create(filename, this.conf, overwrite);
  }

  @Override
  public OutputStream create(String filename, HdfsSinkConnectorConfig conf, boolean overwrite) {
    try {
      Path path = new Path(filename);
      return path.getFileSystem(conf.getHadoopConfiguration()).create(path);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}

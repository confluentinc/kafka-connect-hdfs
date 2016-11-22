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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;

import io.confluent.connect.hdfs.wal.WAL;

@Deprecated
public interface Storage extends io.confluent.connect.storage.Storage {
  @Override
  boolean exists(String filename) throws IOException;

  @Override
  boolean mkdirs(String filename) throws IOException;

  @Override
  void append(String filename, Object object) throws IOException;

  @Override
  void delete(String filename) throws IOException;

  @Override
  void commit(String tempFile, String committedFile) throws IOException;

  @Override
  void close() throws IOException;

  @Override
  WAL wal(String topicsDir, TopicPartition topicPart);

  @Override
  FileStatus[] listStatus(String path, PathFilter filter) throws IOException;

  @Override
  FileStatus[] listStatus(String path) throws IOException;

  @Override
  String url();

  @Override
  Configuration conf();
}

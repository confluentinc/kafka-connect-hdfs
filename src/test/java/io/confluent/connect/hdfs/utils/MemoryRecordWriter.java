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

package io.confluent.connect.hdfs.utils;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.RecordWriter;

public class MemoryRecordWriter implements io.confluent.connect.storage.format.RecordWriter {
  private String filename;
  private static final Map<String, List<Object>> data = Data.getData();
  private Failure failure = Failure.noFailure;

  public enum Failure {
    noFailure,
    writeFailure,
    closeFailure
  }

  public MemoryRecordWriter(String filename) {
    this.filename = filename;
  }

  @Override
  public void write(SinkRecord record) {
    if (failure == Failure.writeFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("write failed.");
    }
    data.get(filename).add(record);

  }

  @Override
  public void commit() {}

  @Override
  public void close() {
    if (failure == Failure.closeFailure) {
      failure = Failure.noFailure;
      throw new ConnectException("close failed.");
    }
  }

  public void setFailure(Failure failure) {
    this.failure = failure;
  }
}

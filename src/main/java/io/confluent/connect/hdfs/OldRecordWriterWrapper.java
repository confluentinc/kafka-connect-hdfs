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

package io.confluent.connect.hdfs;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

/**
 * Wrapper for old-style RecordWriters that implements the new common RecordWriter interface and
 * delegates to the old implementation.
 */
public class OldRecordWriterWrapper implements io.confluent.connect.storage.format.RecordWriter {

  // Strictly speaking RecordWriter was generic, but in practice the implementation was always
  // using the SinkRecord type despite the type not being specified everywhere.
  private final RecordWriter<SinkRecord> oldWriter;

  public OldRecordWriterWrapper(RecordWriter<SinkRecord> oldWriter) {
    this.oldWriter = oldWriter;
  }

  @Override
  public void write(SinkRecord sinkRecord) {
    try {
      oldWriter.write(sinkRecord);
    } catch (IOException e) {
      throw new ConnectException("Failed to write a record to " + oldWriter, e);
    }
  }

  @Override
  public void close() {
    try {
      oldWriter.close();
    } catch (IOException e) {
      throw new ConnectException("Failed to close " + oldWriter, e);
    }
  }

  @Override
  public void commit() {
    // Old interface doesn't have commit
  }
}

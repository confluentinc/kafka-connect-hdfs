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

package io.confluent.connect.hdfs;

import org.apache.kafka.connect.sink.SinkRecord;

import io.confluent.connect.avro.AvroData;

@Deprecated
public interface RecordWriterProvider
    extends io.confluent.connect.storage.format.RecordWriterProvider
    <HdfsSinkConnectorConfig> {
  RecordWriter getRecordWriter(
      HdfsSinkConnectorConfig conf,
      String fileName,
      SinkRecord record,
      final AvroData avroData
  );
}

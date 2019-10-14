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

package io.confluent.connect.hdfs.avro;

import org.apache.hadoop.fs.Path;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;

public class AvroFormat
    implements io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> {
  private final HdfsStorage storage;
  private final AvroData avroData;

  // DO NOT change this signature, it is required for instantiation via reflection
  public AvroFormat(HdfsStorage storage) {
    this.storage = storage;
    this.avroData = new AvroData(
        storage.conf().getInt(HdfsSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG)
    );
  }

  @Override
  public RecordWriterProvider<HdfsSinkConnectorConfig> getRecordWriterProvider() {
    return new AvroRecordWriterProvider(storage, avroData);
  }

  @Override
  public SchemaFileReader<HdfsSinkConnectorConfig, Path> getSchemaFileReader() {
    return new AvroFileReader(avroData);
  }

  @Override
  public HiveFactory getHiveFactory() {
    return new AvroHiveFactory(avroData);
  }
}

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
 */

package io.confluent.connect.hdfs.avro;

import org.apache.hadoop.fs.Path;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.hive.HiveFactory;

public class AvroFormat implements Format,
    io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> {
  private final AvroData avroData;

  public AvroFormat(HdfsStorage storage) {
    this.avroData = new AvroData(
        storage.conf().getInt(HdfsSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG)
    );
  }

  @Override
  public RecordWriterProvider getRecordWriterProvider() {
    return new AvroRecordWriterProvider(avroData);
  }

  @Override
  public SchemaFileReader getSchemaFileReader() {
    return new AvroFileReader(avroData);
  }

  @Deprecated
  @Override
  public HiveUtil getHiveUtil(
      HdfsSinkConnectorConfig config,
      HiveMetaStore hiveMetaStore
  ) {
    return (HiveUtil) getHiveFactory().createHiveUtil(config, hiveMetaStore);
  }

  @Override
  public HiveFactory getHiveFactory() {
    return new AvroHiveFactory(avroData);
  }
}

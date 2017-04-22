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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.AbstractConfig;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.storage.hive.HiveFactory;

@Deprecated
public interface Format
    extends io.confluent.connect.storage.format.Format<Configuration, AvroData, Path> {
  @Override
  RecordWriterProvider getRecordWriterProvider();

  @Override
  SchemaFileReader getSchemaFileReader(AvroData avroData);

  HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, AvroData avroData, HiveMetaStore hiveMetaStore);

  @Override
  HiveFactory<? extends AbstractConfig, AvroData> getHiveFactory();
}

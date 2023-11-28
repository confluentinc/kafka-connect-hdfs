/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.connect.hdfs.orc;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import org.apache.hadoop.fs.Path;

public class OrcFormat implements Format<HdfsSinkConnectorConfig, Path> {

  // DO NOT change this signature, it is required for instantiation via reflection
  public OrcFormat(HdfsStorage storage) { }

  @Override
  public RecordWriterProvider<HdfsSinkConnectorConfig> getRecordWriterProvider() {
    return new OrcRecordWriterProvider();
  }

  @Override
  public SchemaFileReader<HdfsSinkConnectorConfig, Path> getSchemaFileReader() {
    return new OrcFileReader();
  }

  @Override
  public HiveFactory getHiveFactory() {
    return new OrcHiveFactory();
  }
}

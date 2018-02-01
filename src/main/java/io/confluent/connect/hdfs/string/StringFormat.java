/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.connect.hdfs.string;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import org.apache.hadoop.fs.Path;


/**
 * A storage format implementation that exports JSON records to text files with a '.json'
 * extension. In these files, records are separated by the system's line separator,
 * and therefore store one record per line.
 */
public class StringFormat implements Format<HdfsSinkConnectorConfig, Path> {
  private final HdfsStorage storage;

  /**
   * Constructor.
   *
   * @param storage the underlying storage implementation.
   */
  public StringFormat(HdfsStorage storage) {
    this.storage = storage;
  }

  @Override
  public RecordWriterProvider<HdfsSinkConnectorConfig> getRecordWriterProvider() {
    return new StringRecordWriterProvider(storage);
  }

  @Override
  public SchemaFileReader<HdfsSinkConnectorConfig, Path> getSchemaFileReader() {
    return new StringFileReader();
  }

  @Override
  public HiveFactory getHiveFactory() {
    throw new UnsupportedOperationException("Hive integration is not currently supported with "
        + "JSON format");
  }

}

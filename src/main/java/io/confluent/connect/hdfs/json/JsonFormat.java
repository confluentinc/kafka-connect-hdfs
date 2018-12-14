/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.json;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;

/**
 * A storage format implementation that exports JSON records to text files with a '.json'
 * extension. In these files, records are separated by the system's line separator,
 * and therefore store one record per line.
 */
public class JsonFormat implements Format<HdfsSinkConnectorConfig, Path> {
  private final HdfsStorage storage;
  private final JsonConverter converter;

  /**
   * Constructor.
   *
   * @param storage the underlying storage implementation.
   */
  public JsonFormat(HdfsStorage storage) {
    this.storage = storage;
    this.converter = new JsonConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converterConfig.put(
        "schemas.cache.size",
        String.valueOf(storage.conf().get(HdfsSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG))
    );
    this.converter.configure(converterConfig, false);
  }

  @Override
  public RecordWriterProvider<HdfsSinkConnectorConfig> getRecordWriterProvider() {
    return new JsonRecordWriterProvider(storage, converter);
  }

  @Override
  public SchemaFileReader<HdfsSinkConnectorConfig, Path> getSchemaFileReader() {
    return new JsonFileReader();
  }

  @Override
  public HiveFactory getHiveFactory() {
    throw new UnsupportedOperationException("Hive integration is not currently supported with "
        + "JSON format");
  }

}

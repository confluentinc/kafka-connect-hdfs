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

package io.confluent.connect.hdfs.schema;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;

import java.util.Optional;

public class SingleSchemaContainer extends SchemaResolutionAbstract
        implements SchemaResolutionStrategy {
  private final CommittedFileFilter committedFileFilter;
  private boolean readSchemaFromFile;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<Schema> currentSchema = Optional.empty();

  public SingleSchemaContainer(Storage storage, HdfsSinkConnectorConfig connectorConfig,
                               String topic,
                               SchemaFileReader<HdfsSinkConnectorConfig, Path> schemaFileReader,
                               CommittedFileFilter committedFileFilter,
                               boolean readSchemaFromFile) {
    super(storage, connectorConfig, topic, schemaFileReader);
    this.committedFileFilter = committedFileFilter;
    this.readSchemaFromFile = readSchemaFromFile;
  }

  @Override
  public Optional<Schema> getOrLoadCurrentSchema(String recordSchemaName, long offset) {
    if (!currentSchema.isPresent() && readSchemaFromFile && offset != -1) {
      currentSchema = readSchemaFromFile(committedFileFilter);
    }
    return currentSchema;
  }

  @Override
  public void update(Schema schema) {
    this.currentSchema = Optional.of(schema);
  }
}

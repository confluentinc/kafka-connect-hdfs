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
import io.confluent.connect.hdfs.filter.TopicCommittedFileFilter;
import io.confluent.connect.hdfs.filter.TopicPartitionCommittedFileFilter;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;

import static io.confluent.connect.hdfs.HdfsSinkConnectorConfig.MULTI_SCHEMA_SUPPORT_CONFIG;
import static io.confluent.connect.storage.hive.HiveConfig.SCHEMA_COMPATIBILITY_CONFIG;
import static io.confluent.connect.storage.schema.StorageSchemaCompatibility.NONE;
import static io.confluent.connect.storage.schema.StorageSchemaCompatibility.getCompatibility;

public class SchemaResolutionStrategyProvider {
  private HdfsStorage storage;
  private HdfsSinkConnectorConfig connectorConfig;
  private SchemaFileReader<HdfsSinkConnectorConfig, Path> schemaFileReader;

  public SchemaResolutionStrategyProvider(HdfsStorage storage,
                                          HdfsSinkConnectorConfig connectorConfig,
                                          SchemaFileReader<HdfsSinkConnectorConfig, Path>
                                                  schemaFileReader) {
    this.storage = storage;
    this.connectorConfig = connectorConfig;
    this.schemaFileReader = schemaFileReader;
  }

  public SchemaResolutionStrategy get(TopicPartition topicPartition) {
    boolean readSchemaFromFile = getCompatibility(
            connectorConfig.getString(SCHEMA_COMPATIBILITY_CONFIG)) != NONE;
    CommittedFileFilter filter = new TopicPartitionCommittedFileFilter(topicPartition);
    if (connectorConfig.getBoolean(MULTI_SCHEMA_SUPPORT_CONFIG)) {
      return new MultipleSchemasContainer(storage, connectorConfig, topicPartition.topic(),
              schemaFileReader, filter, readSchemaFromFile);
    } else {
      return new SingleSchemaContainer(storage, connectorConfig, topicPartition.topic(),
              schemaFileReader, filter, readSchemaFromFile);
    }
  }

  public SchemaResolutionStrategy get(String topic, boolean readSchemaFromFile) {
    if (connectorConfig.getBoolean(MULTI_SCHEMA_SUPPORT_CONFIG)) {
      return new MultipleSchemasContainer(storage, connectorConfig, topic, schemaFileReader,
              new TopicCommittedFileFilter(topic), readSchemaFromFile);
    } else {
      return new SingleSchemaContainer(storage, connectorConfig, topic, schemaFileReader,
              new TopicCommittedFileFilter(topic), readSchemaFromFile);
    }
  }
}

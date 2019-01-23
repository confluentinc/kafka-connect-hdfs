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

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;

import java.util.Optional;

import static io.confluent.connect.hdfs.FileUtils.fileStatusWithMaxOffset;
import static io.confluent.connect.storage.common.StorageCommonConfig.TOPICS_DIR_CONFIG;

public abstract class SchemaResolutionAbstract implements SchemaResolutionStrategy {
  protected final String topic;
  private final Storage storage;
  private final SchemaFileReader<HdfsSinkConnectorConfig, Path> schemaFileReader;
  private final String topicsDir;
  private final HdfsSinkConnectorConfig connectorConfig;

  protected SchemaResolutionAbstract(Storage storage, HdfsSinkConnectorConfig connectorConfig,
                                     String topic, SchemaFileReader<HdfsSinkConnectorConfig,
                                     Path> schemaFileReader) {
    this.storage = storage;
    this.connectorConfig = connectorConfig;
    this.topicsDir = connectorConfig.getString(TOPICS_DIR_CONFIG);
    this.topic = topic;
    this.schemaFileReader = schemaFileReader;
  }

  protected Optional<Schema> readSchemaFromFile(CommittedFileFilter filter) {
    String topicDir = FileUtils.topicDirectory(storage.url(), topicsDir, topic);
    FileStatus fileStatusWithMaxOffset = fileStatusWithMaxOffset(
            storage, new Path(topicDir), filter
    );
    if (fileStatusWithMaxOffset != null) {
      return Optional.of(
              schemaFileReader.getSchema(connectorConfig, fileStatusWithMaxOffset.getPath())
      );
    }
    return Optional.empty();
  }
}

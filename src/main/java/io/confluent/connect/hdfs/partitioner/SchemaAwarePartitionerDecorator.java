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

package io.confluent.connect.hdfs.partitioner;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

public class SchemaAwarePartitionerDecorator implements Partitioner {
  private Partitioner partitioner;

  public SchemaAwarePartitionerDecorator(Partitioner partitioner) {
    this.partitioner = partitioner;
  }

  @Override
  public void configure(Map<String, Object> config) {
    partitioner.configure(config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    return sinkRecord.valueSchema().name() + "/" + partitioner.encodePartition(sinkRecord);
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    return partitioner.generatePartitionedPath(topic, encodedPartition);
  }

  @Override
  public List<FieldSchema> partitionFields() {
    return partitioner.partitionFields();
  }
}

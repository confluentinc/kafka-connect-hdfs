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

package io.confluent.connect.hdfs.partitioner;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

/**
 * Partition incoming records, and generates directories and file names in which to store the
 * incoming records.
 */
@Deprecated
public interface Partitioner
    extends io.confluent.connect.storage.partitioner.Partitioner<FieldSchema> {
  @Override
  void configure(Map<String, Object> config);

  @Override
  String encodePartition(SinkRecord sinkRecord);

  @Override
  String generatePartitionedPath(String topic, String encodedPartition);

  @Override
  List<FieldSchema> partitionFields();
}

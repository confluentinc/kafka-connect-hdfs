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


package io.confluent.connect.hdfs.hive;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.storage.hive.HiveConfig;
import org.apache.kafka.common.TopicPartition;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class HiveServiceProvider {
  private final Partitioner hivePartitioner;
  private final HiveTableNamingStrategy hiveTableNamingStrategy;
  private final String hiveDatabase;
  private final HiveMetaStore hiveMetaStore;
  private final HiveUtil hive;
  private final ExecutorService executorService;
  private final Queue<Future<Void>> hiveUpdateFutures;

  public HiveServiceProvider(Partitioner hivePartitioner,
                             HiveTableNamingStrategy hiveTableNamingStrategy,
                             HiveMetaStore hiveMetaStore, HiveUtil hive,
                             ExecutorService executorService,
                             Queue<Future<Void>> hiveUpdateFutures,
                             HdfsSinkConnectorConfig connectorConfig) {
    this.hivePartitioner = hivePartitioner;
    this.hiveTableNamingStrategy = hiveTableNamingStrategy;
    if (connectorConfig.getBoolean(HiveConfig.HIVE_INTEGRATION_CONFIG)) {
      this.hiveDatabase = connectorConfig.getString(HiveConfig.HIVE_DATABASE_CONFIG);
    } else {
      this.hiveDatabase = null;
    }
    this.hiveMetaStore = hiveMetaStore;
    this.hive = hive;
    this.executorService = executorService;
    this.hiveUpdateFutures = hiveUpdateFutures;
  }

  public HiveService get(TopicPartition tp) {
    return new HiveService(tp, hivePartitioner, hiveTableNamingStrategy, hiveMetaStore, hive,
            executorService, hiveUpdateFutures, hiveDatabase);
  }
}

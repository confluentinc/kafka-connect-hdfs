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

import io.confluent.connect.hdfs.partitioner.Partitioner;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class HiveService {
  private static final Logger log = LoggerFactory.getLogger(HiveService.class);
  private final TopicPartition tp;
  private final Partitioner hivePartitioner;
  private final HiveTableNamingStrategy hiveTableNamingStrategy;
  private final String hiveDatabase;
  private final HiveMetaStore hiveMetaStore;
  private final HiveUtil hive;
  private final ExecutorService executorService;
  private final Queue<Future<Void>> hiveUpdateFutures;

  public HiveService(TopicPartition tp, Partitioner hivePartitioner,
                     HiveTableNamingStrategy hiveTableNamingStrategy, HiveMetaStore hiveMetaStore,
                     HiveUtil hive, ExecutorService executorService,
                     Queue<Future<Void>> hiveUpdateFutures,
                     String hiveDatabase) {
    this.tp = tp;
    this.hivePartitioner = hivePartitioner;
    this.hiveTableNamingStrategy = hiveTableNamingStrategy;
    this.hiveDatabase = hiveDatabase;
    this.hiveMetaStore = hiveMetaStore;
    this.hive = hive;
    this.executorService = executorService;
    this.hiveUpdateFutures = hiveUpdateFutures;
  }

  public void createHiveTable(Schema schema) {
    Future<Void> future = executorService.submit(() -> {
      try {
        hive.createTable(hiveDatabase, tp.topic(), schema, hivePartitioner);
      } catch (Throwable e) {
        log.error("Creating Hive table threw unexpected error", e);
      }
      return null;
    });
    hiveUpdateFutures.add(future);
  }

  public void alterHiveSchema(Schema schema) {
    Future<Void> future = executorService.submit(() -> {
      try {
        String tableName = hiveTableNamingStrategy.createName(
                new HiveTableNamingParameters(tp.topic(), schema)
        );
        hive.alterSchema(hiveDatabase, tableName, schema);
      } catch (Throwable e) {
        log.error("Altering Hive schema threw unexpected error", e);
      }
      return null;
    });
    hiveUpdateFutures.add(future);
  }

  public void addHivePartition(final SinkRecord record, Schema schema) {
    String location = hivePartitioner.encodePartition(record);
    Future<Void> future = executorService.submit(() -> {
      try {
        String tableName = hiveTableNamingStrategy.createName(
                new HiveTableNamingParameters(tp.topic(), schema)
        );
        hiveMetaStore.addPartition(hiveDatabase, tableName, location);
      } catch (Throwable e) {
        log.error("Adding Hive partition threw unexpected error", e);
      }
      return null;
    });
    hiveUpdateFutures.add(future);
  }
}

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

package io.confluent.connect.hdfs.jdbc;

import io.confluent.connect.hdfs.HdfsSinkTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class JdbcHdfsSinkTask extends HdfsSinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcHdfsSinkTask.class);

  private final JdbcHashCache jdbcHashCache = new JdbcHashCache();
  private ConfiguredTables configuredTables;
  private RetrySpec retrySpec;
  private JdbcConnection jdbcConnection;

  public JdbcHdfsSinkTask() {
  }

  @Override
  public void start(Map<String, String> props) {
    log.info(
        "{} Loading {}",
        getClass().getSimpleName(),
        JdbcHdfsSinkConnectorConfig.class.getSimpleName()
    );

    try {
      JdbcHdfsSinkConnectorConfig connectorConfig = new JdbcHdfsSinkConnectorConfig(props);

      configuredTables = new ConfiguredTables(props);

      retrySpec = new RetrySpec(
          connectorConfig.getConnectionAttempts() - 1,
          connectorConfig.getConnectionBackoff()
      );

      jdbcConnection = new JdbcConnection(
          connectorConfig.getConnectionUrl(),
          connectorConfig.getConnectionProperties()
      );
    } catch (ConfigException ex) {
      log.error(
          "{} Couldn't start due to configuration error: {}",
          getClass().getSimpleName(),
          ex.getMessage(),
          ex
      );
      throw new ConnectException(
          getClass().getSimpleName() + " Couldn't start due to configuration error.",
          ex
      );
    } catch (RuntimeException ex) {
      log.error(
          "{} Couldn't start due to: {}",
          getClass().getSimpleName(),
          ex.getMessage(),
          ex
      );
      throw ex;
    }

    log.info(
        "{} Loaded {} successfully",
        getClass().getSimpleName(),
        JdbcHdfsSinkConnectorConfig.class.getSimpleName()
    );

    super.start(props);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("Read {} records from Kafka; retrieving Large columns from JDBC", records.size());
    // TODO: Keep track of schema changes
    // TODO: Verify db and schema match the connection string.
    // TODO: groupBy?

    // NOTE: Do not pre-allocate JdbcRecordTransformer,
    //       as it needs to have a fresh SqlCache every iteration.
    // TODO: Determine if it is safe to long-term cache Table Schemas,
    //       or do they change often enough to warrant a refresh every iteration?
    JdbcRecordTransformer recordTransformer = new JdbcRecordTransformer(
        jdbcConnection,
        retrySpec,
        configuredTables,
        jdbcHashCache
    );

    // Iterate over each record, and put() each individually
    records
        .stream()
        .map(recordTransformer::transformRecord)
        .filter(Objects::nonNull)
        .peek(newRecord -> log.debug(
            "Created new SinkRecord from old Sink Record: PK [{}] Columns [{}]",
            newRecord.key(),
            newRecord
                .valueSchema()
                .fields()
                .stream()
                .map(Field::name)
                .collect(Collectors.joining(","))
        ))
        .map(Collections::singletonList)
        .forEach(super::put);

    // Trigger a sync() to HDFS, even if no records were written.
    // This updates all accounting and delayed writes, etc...
    super.put(Collections.emptyList());
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    //log.info("Opening {}", getClass().getSimpleName());
    super.open(partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    //log.info("Closing {}", getClass().getSimpleName());
    super.close(partitions);
  }

  @Override
  public void stop() {
    log.info("Stopping {}", getClass().getSimpleName());
    if (jdbcConnection != null) {
      try {
        jdbcConnection.close();
      } catch (Exception ex) {
        log.warn("Failed to close JdbcConnection: {}", ex.getMessage(), ex);
      }
    }
    super.stop();
  }
}

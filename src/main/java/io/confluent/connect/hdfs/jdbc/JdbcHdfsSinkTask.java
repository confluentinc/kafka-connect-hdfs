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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.confluent.connect.hdfs.HdfsSinkTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcHdfsSinkTask extends HdfsSinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcHdfsSinkTask.class);

  private HashCache hashCache;
  private HikariConfig hikariConfig;
  private Map<JdbcTableInfo, Set<String>> includedFieldsLowerMap;
  private HikariDataSource dataSource;
  private JdbcRecordTransformer recordTransformer;

  @Override
  public void start(Map<String, String> props) {
    try {
      log.info("Loading JDBC configs");

      JdbcHdfsSinkConnectorConfig connectorConfig = new JdbcHdfsSinkConnectorConfig(props);

      hikariConfig = new HikariConfig();
      hikariConfig.setJdbcUrl(connectorConfig.getConnectionUrl());
      hikariConfig.setUsername(connectorConfig.getConnectionUser());
      hikariConfig.setPassword(connectorConfig.getConnectionPassword().value());

      includedFieldsLowerMap = connectorConfig.getIncludedFieldsLower();
      includedFieldsLowerMap.forEach((table, columns) -> log.info(
          "Configured to include {} columns {}",
          table,
          columns
      ));

      if (connectorConfig.isHashCacheEnabled()) {
        hashCache = new HashCache(
            connectorConfig.getHashCacheSize(),
            // TODO: Un-hardcode this
            MessageDigest.getInstance("MD5")
        );
      }

      log.info("Successfully loaded JDBC configs");
    } catch (ConnectException ex) {
      log.error("JDBC configuration error: {}", ex.getMessage(), ex);
      throw ex;
    } catch (Exception ex) {
      log.error("JDBC configuration error: {}", ex.getMessage(), ex);
      throw new ConnectException("JDBC configuration error: " + ex.getMessage(), ex);
    }

    super.start(props);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("put(large-columns): Processing {} records from Kafka", records.size());
    // TODO: Keep track of schema changes
    // TODO: Verify db and schema match the connection string.
    // TODO: groupBy?

    // NOTE: We need to have a fresh SqlCache every iteration.
    // TODO: Determine if it would be safe to cache Table Schemas long-term,
    //       or do they change often enough to warrant a refresh every iteration?
    SqlMetadataCache sqlMetadataCache = new SqlMetadataCache(dataSource);

    // Iterate over each record, and put() each individually
    for (SinkRecord record : records) {
      try {
        Optional
            .ofNullable(recordTransformer.transformRecord(sqlMetadataCache, record))
            .map(Stream::of)
            .orElseGet(Stream::empty)
            .peek(transformedRecord -> log.debug(
                "Created new SinkRecord from old Sink Record: PK [{}] Columns [{}]",
                transformedRecord.key(),
                transformedRecord
                    .valueSchema()
                    .fields()
                    .stream()
                    .map(Field::name)
                    .collect(Collectors.joining(","))
            ))
            .map(Collections::singletonList)
            .forEach(super::put);
      } catch (SQLException ex) {
        log.error("Failed to transform Record: {}", ex.getMessage(), ex);
        throw new DataException("Failed to transform Record: " + ex.getMessage(), ex);
      }
    }

    // Trigger a sync() to HDFS, even if no records were written.
    // This updates all accounting and delayed writes, etc...
    super.put(Collections.emptyList());
    log.debug("put(-large-files): Finished");
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    log.info("Opening JDBC DataSource: {}", hikariConfig.getJdbcUrl());

    dataSource = new HikariDataSource(hikariConfig);

    recordTransformer = new JdbcRecordTransformer(dataSource,
                                                  includedFieldsLowerMap,
                                                  hashCache);

    log.info("Successfully opened JDBC DataSource: {}", dataSource.getJdbcUrl());

    super.open(partitions);
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    super.close(partitions);

    if (dataSource != null) {
      try {
        log.info("Closing JDBC DataSource {}", hikariConfig.getJdbcUrl());
        dataSource.close();
      } catch (Exception ex) {
        log.warn("Failed to close JDBC DataSource: {}", ex.getMessage(), ex);
      }
    }
  }
}

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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JdbcHdfsSinkTask extends HdfsSinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcHdfsSinkTask.class);

  private final JdbcRetrySpec retrySpec = JdbcRetrySpec.NoRetries; // TODO: Make Configurable
  private ConfiguredTables configuredTables;
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
    } catch (ConnectException ex) {
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
  public void put(Collection<SinkRecord> records) throws ConnectException {
    log.debug("Read {} records from Kafka; retrieving Large columns from JDBC", records.size());
    try {
      // TODO: Keep track of schema changes
      // TODO: Un-hardcode this
      //this.maxRetries = Math.max(0, jdbcConfig.getConnectionAttempts() - 1);
      // TODO: Verify db and schema match the connection string.
      // TODO: groupBy
      // TODO: MD5 Optimization/pruning

      if (records.isEmpty()) {
        super.put(records);
      } else {
        SqlCache sqlCache = new SqlCache(jdbcConnection, retrySpec);

        // Iterate over each record, and put() each individually
        for (SinkRecord record : records) {
          Optional
              .ofNullable(transformRecord(sqlCache, record))
              .ifPresent(newRecord -> {
                log.debug(
                    "Created new SinkRecord from old Sink Record: PK [{}] Columns [{}]",
                    newRecord.key(),
                    newRecord
                        .valueSchema()
                        .fields()
                        .stream()
                        .map(Field::name)
                        .collect(Collectors.joining(","))
                );
                super.put(Collections.singletonList(newRecord));
              });
        }
        // Trigger a sync() to HDFS, even if no records were written.
        super.put(Collections.emptyList());
      }
    } catch (SQLException ex) {
      throw new ConnectException(ex);
    }
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
  public void stop() throws ConnectException {
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

  private SinkRecord transformRecord(SqlCache sqlCache,
                                     SinkRecord oldRecord) throws SQLException {
    JdbcTableInfo tableInfo = new JdbcTableInfo(oldRecord);

    Set<String> configuredFieldNamesLower = configuredTables.getColumnNamesLower(tableInfo);

    // No columns to Query? No need to write anything at all to HDFS

    if (configuredFieldNamesLower.isEmpty()) {
      return null;
    }

    // Calculate the list of Columns to query from the DB

    Schema oldValueSchema = oldRecord.valueSchema();

    Map<String, Field> oldFieldsMap =
        oldValueSchema
            .fields()
            .stream()
            .collect(Collectors.toMap(
                field -> Optional
                    .ofNullable(field.name())
                    .map(String::trim)
                    .filter(((Predicate<String>) String::isEmpty).negate())
                    // NOTE: Should be impossible to reach here!
                    .orElseThrow(() -> new ConnectException(
                        "Old Field ["
                            + field.name()
                            + "] is null or empty for Table ["
                            + tableInfo
                            + "]"
                    )),
                Function.identity()
            ));

    Set<String> oldFieldNamesLower =
        oldFieldsMap
            .keySet()
            .stream()
            .map(String::toLowerCase)
            .collect(Collectors.toSet());

    Set<String> columnNamesLowerToQuery =
        configuredFieldNamesLower
            .stream()
            .filter(((Predicate<String>) oldFieldNamesLower::contains).negate())
            .collect(Collectors.toSet());

    // NOTE: No actual columns to Query? No need to write anything at all to HDFS

    if (columnNamesLowerToQuery.isEmpty()) {
      return null;
    }

    // Gather Column Metadata from the DB

    List<JdbcColumn> allColumns = sqlCache.fetchAllColumns(tableInfo);

    Map<String, JdbcColumn> allColumnsLowerMap =
        allColumns
            .stream()
            .collect(Collectors.toMap(
                column -> column.getName().toLowerCase(),
                Function.identity()
            ));

    List<JdbcColumn> primaryKeyColumns = sqlCache.fetchPrimaryKeyColumns(tableInfo);

    Set<String> primaryKeyColumnNamesLower =
        primaryKeyColumns
            .stream()
            .map(JdbcColumn::getName)
            .map(String::toLowerCase)
            .collect(Collectors.toSet());

    List<JdbcColumn> columnsToQuery =
        columnNamesLowerToQuery
            .stream()
            .filter(((Predicate<String>) primaryKeyColumnNamesLower::contains).negate())
            .map(columnNameLower -> Optional
                .ofNullable(allColumnsLowerMap.get(columnNameLower))
                .orElseThrow(() -> new ConnectException(
                    "Configured Column ["
                        + columnNameLower
                        + "] does not exist in Table ["
                        + tableInfo
                        + "]"
                ))
            )
            .sorted(JdbcColumn.byOrdinal)
            .collect(Collectors.toList());

    // Create the Schema and new value Struct

    Schema newValueSchema = JdbcSchema.createSchema(
        configuredFieldNamesLower,
        oldValueSchema,
        primaryKeyColumns,
        columnsToQuery
    );

    Struct newValueStruct = new Struct(newValueSchema);

    // Populate the newValueStruct with existing values from oldValueStruct

    Struct oldValueStruct = (Struct) oldRecord.value();

    newValueSchema
        .fields()
        .forEach(newField -> Optional
            .ofNullable(oldFieldsMap.get(newField.name()))
            .flatMap(oldField -> Optional.ofNullable(oldValueStruct.get(oldField)))
            .ifPresent(oldValue -> newValueStruct.put(newField, oldValue))
        );

    // Execute the query

    JdbcReader jdbcReader = new JdbcReader(jdbcConnection, retrySpec);

    jdbcReader.executeQuery(
        tableInfo,
        primaryKeyColumns,
        columnsToQuery,
        JdbcReader.structToJdbcValueMapper(oldValueStruct),
        JdbcReader.columnToStructVisitor(newValueStruct),
        () -> Optional
            .ofNullable(oldRecord.key())
            .map(Object::toString)
            .orElse("")
    );

    // Make sure the newValueStruct is fully populated
    newValueStruct.validate();

    // Create the newly transformed SourceRecord
    return oldRecord.newRecord(
        oldRecord.topic(),
        oldRecord.kafkaPartition(),
        oldRecord.keySchema(),
        oldRecord.key(),
        newValueSchema,
        newValueStruct,
        oldRecord.timestamp()
    );
  }
}

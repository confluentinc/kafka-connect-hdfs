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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcHdfsSinkTask extends HdfsSinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcHdfsSinkTask.class);

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
        //JdbcHdfsCache jdbcHdfsCache = new JdbcHdfsCache();
        SimpleSqlCache sqlCache = new SimpleSqlCache();

        // Iterate over each record, and put() each individually
        for (SinkRecord record : records) {
          Optional
              .ofNullable(transformRecord(sqlCache, record))
              .map(Collections::singletonList)
              .ifPresent(super::put);
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

  private <T, K> Predicate<T> distinctBy(Function<T, K> distinctFn) {
    Map<K, Boolean> seen = new ConcurrentHashMap<>();
    return t -> seen.putIfAbsent(distinctFn.apply(t), Boolean.TRUE) == null;
  }

  private SinkRecord transformRecord(SimpleSqlCache sqlCache,
                                     SinkRecord oldRecord) throws SQLException {
    JdbcTableInfo tableInfo = new JdbcTableInfo(oldRecord);
    Struct oldValueStruct = (Struct) oldRecord.value();
    Schema oldValueSchema = oldRecord.valueSchema();

    Set<String> configuredFieldNamesLower = configuredTables.getColumnNamesLower(tableInfo);

    // No columns to Query? No need to write anything at all to HDFS

    if (configuredFieldNamesLower.isEmpty())
      return null;

    // Calculate the list of Columns to query from the DB

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

    if (columnNamesLowerToQuery.isEmpty())
      return null;

    // Gather Column Metadata from the DB

    Map<String, JdbcColumn> allColumnsLowerMap =
        sqlCache.computeIfAbsent(
            "allColumnLowerMap",
            tableInfo,
            __ -> jdbcConnection
                .fetchAllColumns(tableInfo)
                .stream()
                .collect(Collectors.toMap(
                    jdbcColumn -> jdbcColumn.getName().toLowerCase(),
                    Function.identity()
                ))
        );

    List<JdbcColumn> columnsToQuery =
        columnNamesLowerToQuery
            .stream()
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

    List<JdbcColumn> primaryKeyColumns =
        sqlCache.computeIfAbsent(
            "primaryKeyColumns",
            tableInfo,
            __ -> jdbcConnection
                .fetchPrimaryKeyNames(tableInfo)
                .stream()
                .map(String::toLowerCase)
                .map(primaryKeyName -> Optional
                    .ofNullable(allColumnsLowerMap.get(primaryKeyName))
                    .orElseThrow(() -> new ConnectException(
                        "Primary Key ["
                            + primaryKeyName
                            + "] does not exist in Table ["
                            + tableInfo
                            + "]"
                    ))
                )
                .sorted(JdbcColumn.byOrdinal)
                .collect(Collectors.toList())
        );

    // Create the Schema

    SchemaBuilder newSchemaBuilder = SchemaBuilder
        .struct();

    Set<String> newColumnNames =
        Stream
            .concat(
                primaryKeyColumns.stream(),
                columnsToQuery.stream()
            )
            .filter(distinctBy(JdbcColumn::getName))
            .sorted(JdbcColumn.byOrdinal)
            .peek(column -> {
              String columnName = column.getName();
              Schema fieldSchema =
                  Optional
                      .ofNullable(oldValueSchema.field(columnName))
                      .map(Field::schema)
                      .orElseGet(() -> toSchema(column));
              newSchemaBuilder.field(columnName, fieldSchema);
            })
            .map(JdbcColumn::getName)
            .collect(Collectors.toSet());

    oldValueSchema
        .fields()
        .forEach(field -> {
          String fieldName = field.name().trim();
          if (!newColumnNames.contains(fieldName) && configuredFieldNamesLower.contains(fieldName.toLowerCase())) {
            newSchemaBuilder.field(fieldName, field.schema());
          }
        });

    Schema newValueSchema = newSchemaBuilder.build();
    Struct newValueStruct = new Struct(newValueSchema);

    // Populate the newValueStruct with existing values from oldValueStruct

    newValueSchema
        .fields()
        .forEach(newField -> Optional
            .ofNullable(oldFieldsMap.get(newField.name()))
            .flatMap(oldField -> Optional.ofNullable(oldValueStruct.get(oldField)))
            .ifPresent(oldValue -> newValueStruct.put(newField, oldValue))
        );

    // Create the query

    String whereClause =
        primaryKeyColumns
            .stream()
            .map(JdbcColumn::getName)
            .map(primaryKeyName -> primaryKeyName + "=?")
            .collect(Collectors.joining(" AND "));

    String sqlQuery =
        "SELECT "
            + columnsToQuery.stream().map(JdbcColumn::getName).collect(Collectors.joining(","))
            + " FROM "
            + tableInfo.qualifiedName()
            + " WHERE "
            + whereClause
            + ";";

    JdbcValueMapper<String> jdbcValueMapper =
        new StructToJdbcValueMapper(oldValueStruct);

    // Execute the Query

    jdbcConnection.withPreparedStatement(sqlQuery, preparedStatement -> {
      int index = 0;
      for (JdbcColumn primaryKeyColumn : primaryKeyColumns) {
        JdbcQueryUtil.prepareWhereColumn(
            preparedStatement,
            primaryKeyColumn,
            ++index,
            jdbcValueMapper
        );
      }

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (!resultSet.next()) {
          // TODO: How do we detect if incoming record is a DELETE?
          log.warn(
              "Cannot find Row for PK [{}] in Table [{}]",
              oldRecord.key(),
              tableInfo
          );
        } else {

          // Read values from the DB into newValueStruct
          JdbcQueryUtil.visitColumns(resultSet, columnVisitor(newValueStruct));

          // NOTE: We should only have a single result!
          if (resultSet.next()) {
            throw new ConnectException(
                "Got more than 1 row for query ["
                    + oldRecord.key()
                    + "] in Table ["
                    + tableInfo
                    + "]"
            );
          }
        }
      }
      return null;
    });

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

  private Schema toSchema(JdbcColumn column) {
    switch (column.getJdbcType()) {
      case BLOB:
        return column.isNullable() ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
      case CLOB:
      case SQLXML:
        return column.isNullable() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
      default:
        throw new ConnectException(
            "Cannot convert Column ["
                + column.getName()
                + "] type ["
                + column.getJdbcType()
                + "] into a Value Schema"
        );
    }
  }

  private JdbcColumnVisitor columnVisitor(Struct struct) {
    return new JdbcColumnVisitor() {
      @Override
      public void visit(String columnName, Blob blob) throws SQLException {
        if (blob != null) {
          // TODO: Would be so much better if we could stream this data, instead of loading it into a byte[]
          byte[] bytes = blob.getBytes(1, (int) blob.length());
          struct.put(columnName, bytes);
        }
      }

      @Override
      public void visit(String columnName, Clob clob) throws SQLException {
        if (clob != null) {
          // TODO: Would be so much better if we could stream this data, instead of loading it into a String
          String text = clob.getSubString(1, (int) clob.length());
          struct.put(columnName, text);
        }
      }

      @Override
      public void visit(String columnName, SQLXML sqlxml) throws SQLException {
        if (sqlxml != null) {
          // TODO: Would be so much better if we could stream this data, instead of loading it into a String
          String text = sqlxml.getString();
          struct.put(columnName, text);
        }
      }
    };
  }
}

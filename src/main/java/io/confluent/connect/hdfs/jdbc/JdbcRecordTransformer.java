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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JdbcRecordTransformer {
  private final JdbcConnection jdbcConnection;
  private final RetrySpec retrySpec;
  private final ConfiguredTables configuredTables;
  private final JdbcHashCache jdbcHashCache;
  private final SqlCache sqlCache;

  public JdbcRecordTransformer(JdbcConnection jdbcConnection,
                               RetrySpec retrySpec,
                               ConfiguredTables configuredTables,
                               JdbcHashCache jdbcHashCache) {
    this.jdbcConnection = jdbcConnection;
    this.retrySpec = retrySpec;
    this.configuredTables = configuredTables;
    this.jdbcHashCache = jdbcHashCache;
    this.sqlCache = new SqlCache(jdbcConnection, retrySpec);
  }

  public SinkRecord transformRecord(SinkRecord oldRecord) {
    JdbcTableInfo tableInfo = new JdbcTableInfo(oldRecord.headers());

    Set<String> configuredFieldNamesLower = configuredTables.getColumnNamesLower(tableInfo);

    // No columns to Query? No need to write anything at all to HDFS

    if (configuredFieldNamesLower.isEmpty()) {
      return null;
    }

    // Calculate the list of Columns to query

    Schema oldValueSchema = oldRecord.valueSchema();

    Map<String, Field> oldFieldsMap = toFieldsMap(oldValueSchema);

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

    // No actual columns to Query? No need to write anything at all to HDFS

    if (columnNamesLowerToQuery.isEmpty()) {
      return null;
    }

    // Gather Column Metadata from the DB

    Map<String, JdbcColumn> allColumnsLowerMap =
        sqlCache
            .fetchAllColumns(tableInfo)
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
                .orElseThrow(() -> new DataException(
                    "Configured Column ["
                    + columnNameLower
                    + "] does not exist in Table ["
                    + tableInfo
                    + "]"
                ))
            )
            .sorted(JdbcColumn.byOrdinal)
            .collect(Collectors.toList());

    // Create the mew Schema and new value Struct

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

    boolean hasChangedColumns =
        JdbcQueryUtil.executeQuery(
            jdbcHashCache,
            jdbcConnection,
            retrySpec,
            tableInfo,
            primaryKeyColumns,
            columnsToQuery,
            oldValueStruct,
            newValueStruct,
            oldRecord.key()
        );

    // Only write a record if there are changes in the LOB(s).
    // This is an optimization for when LOBs already exist in the cache.
    // TODO: Make this optimization configurable, so it can be disabled from the config

    if (!hasChangedColumns) {
      return null;
    }

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

  private Map<String, Field> toFieldsMap(Schema schema) {
    return schema
        .fields()
        .stream()
        .collect(Collectors.toMap(
            field -> Optional
                .ofNullable(field.name())
                .map(String::trim)
                .filter(((Predicate<String>) String::isEmpty).negate())
                // NOTE: Should be impossible to reach here!
                .orElseThrow(() -> new DataException(
                    "Field ["
                    + field.name()
                    + "] is null or empty for Schema ["
                    + schema.name()
                    + "]"
                )),
            Function.identity()
        ));
  }
}

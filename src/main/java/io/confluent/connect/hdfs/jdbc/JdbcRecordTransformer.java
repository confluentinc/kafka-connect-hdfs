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

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JdbcRecordTransformer {
  private final DataSource dataSource;
  private final Map<JdbcTableInfo, Set<String>> configuredTableColumnsMap;
  private final JdbcHashCache jdbcHashCache;

  public JdbcRecordTransformer(
      DataSource dataSource,
      Map<JdbcTableInfo, Set<String>> configuredTableColumnsMap,
      JdbcHashCache jdbcHashCache
  ) {
    this.dataSource = dataSource;
    this.configuredTableColumnsMap = new HashMap<>(configuredTableColumnsMap);
    this.jdbcHashCache = jdbcHashCache;
  }

  /**
   * NOTE: Not threadsafe, as several components update things like basic Collections
   */
  public SinkRecord transformRecord(
      SqlMetadataCache sqlMetadataCache,
      SinkRecord oldRecord
  ) throws SQLException {
    JdbcTableInfo tableInfo = new JdbcTableInfo(oldRecord.headers());

    Set<String> configuredFieldNamesLower =
        configuredTableColumnsMap.computeIfAbsent(
            tableInfo,
            __ -> Collections.emptySet()
        );

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

    Map<String, JdbcColumnInfo> allColumnsLowerMap =
        sqlMetadataCache
            .fetchAllColumns(tableInfo)
            .stream()
            .collect(Collectors.toMap(
                column -> column.getName().toLowerCase(),
                Function.identity()
            ));

    List<JdbcColumnInfo> primaryKeyColumns =
        sqlMetadataCache.fetchPrimaryKeyColumns(tableInfo);

    Set<String> primaryKeyColumnNamesLower =
        primaryKeyColumns
            .stream()
            .map(JdbcColumnInfo::getName)
            .map(String::toLowerCase)
            .collect(Collectors.toSet());

    List<JdbcColumnInfo> columnsToQuery =
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
            .sorted(JdbcColumnInfo.byOrdinal)
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

    String primaryKeyStr = Optional
        .ofNullable(oldRecord.key())
        .map(Object::toString)
        .map(String::trim)
        .orElse("");

    FilteredColumnToStructVisitor columnVisitor =
        new FilteredColumnToStructVisitor(
            jdbcHashCache,
            newValueStruct,
            tableInfo,
            primaryKeyStr
        );

    JdbcQueryUtil.executeSingletonQuery(
        dataSource,
        tableInfo,
        primaryKeyColumns,
        columnsToQuery,
        new StructToJdbcValueMapper(oldValueStruct),
        columnVisitor,
        primaryKeyStr
    );

    // Only write a record if there are changes in the LOB(s).
    // This is an optimization for when LOBs already exist in the cache.
    // TODO: Make this optimization configurable, so it can be disabled from the config

    if (!columnVisitor.hasChangedColumns()) {
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

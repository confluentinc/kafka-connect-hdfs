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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcSchema {
  public static Schema createSchema(Set<String> fieldsLower,
                                    Schema oldSchema,
                                    Collection<JdbcColumnInfo> primaryKeyColumns,
                                    Collection<JdbcColumnInfo> columnsToQuery) {
    SchemaBuilder newSchemaBuilder = SchemaBuilder.struct();

    Set<String> newColumnNames =
        Stream
            .concat(
                primaryKeyColumns.stream(),
                columnsToQuery.stream()
            )
            // Unnecessary, as columnsToQuery already filtered out all primary keys
            //.filter(distinctBy(JdbcColumn::getName))
            .sorted(JdbcColumnInfo.byOrdinal)
            .peek(column -> {
              String columnName = column.getName();
              Schema fieldSchema = Optional
                  .ofNullable(oldSchema.field(columnName))
                  .map(Field::schema)
                  .orElseGet(() -> toSchema(column));
              newSchemaBuilder.field(columnName, fieldSchema);
            })
            .map(JdbcColumnInfo::getName)
            .collect(Collectors.toSet());

    oldSchema
        .fields()
        .forEach(field -> {
          String fieldName = field.name().trim();
          if (!newColumnNames.contains(fieldName)
              && fieldsLower.contains(fieldName.toLowerCase())) {
            newSchemaBuilder.field(fieldName, field.schema());
          }
        });

    return newSchemaBuilder.build();
  }

  private static Schema toSchema(JdbcColumnInfo column) {
    switch (column.getJdbcType()) {
      case BLOB:
        return column.isNullable() ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
      case CLOB:
      case LONGVARCHAR:
      case SQLXML:
      case VARCHAR:
        return column.isNullable() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
      default:
        throw new DataException(
            "Cannot convert Column ["
            + column.getName()
            + "] type ["
            + column.getJdbcType()
            + "] into a Value Schema"
        );
    }
  }
}

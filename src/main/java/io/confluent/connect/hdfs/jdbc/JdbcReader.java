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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class JdbcReader {
  private static final Logger log = LoggerFactory.getLogger(JdbcReader.class);

  private final JdbcConnection jdbcConnection;
  private final JdbcRetrySpec retrySpec;

  public JdbcReader(JdbcConnection jdbcConnection,
                    JdbcRetrySpec retrySpec) {
    this.jdbcConnection = jdbcConnection;
    this.retrySpec = retrySpec;
  }

  public void executeQuery(
      JdbcTableInfo tableInfo,
      Collection<JdbcColumn> primaryKeyColumns,
      Collection<JdbcColumn> columnsToQuery,
      JdbcValueMapper valueMapper,
      JdbcColumnVisitor columnVisitor,
      Supplier<String> primaryKeyForLogging
  ) throws SQLException {
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

    // Execute the Query
    log.debug(
        "Executing SQL Query for PK [{}] in Table [{}]",
        primaryKeyForLogging.get(),
        tableInfo
    );

    jdbcConnection.withPreparedStatement(retrySpec, sqlQuery, jdbcPreparedStatement -> {
      int index = 0;
      for (JdbcColumn primaryKeyColumn : primaryKeyColumns) {
        jdbcPreparedStatement.setColumn(primaryKeyColumn, ++index, valueMapper);
      }

      try (ResultSet resultSet = jdbcPreparedStatement.executeQuery()) {
        if (!resultSet.next()) {
          // TODO: How do we detect if incoming record is a DELETE?
          log.warn(
              "Cannot find Row for PK [{}] in Table [{}]",
              primaryKeyForLogging.get(),
              tableInfo
          );
        } else {
          log.debug(
              "Processing ResultSet from Query with PK [{}] in Table [{}]",
              primaryKeyForLogging.get(),
              tableInfo
          );

          // Read values from the DB into newValueStruct
          jdbcPreparedStatement.visitColumns(resultSet, columnVisitor);

          // NOTE: We should only have a single result!
          if (resultSet.next()) {
            throw new ConnectException(
                "Got more than 1 row for query ["
                    + primaryKeyForLogging.get()
                    + "] in Table ["
                    + tableInfo
                    + "]"
            );
          }
        }
      }
    });
  }

  public static JdbcColumnVisitor columnToStructVisitor(Struct struct) {
    return new JdbcColumnVisitor() {
      @Override
      public void visit(String columnName, Blob blob) throws SQLException {
        if (blob != null) {
          // TODO: Would be so much better if we could stream this data
          byte[] bytes = blob.getBytes(1, (int) blob.length());
          struct.put(columnName, bytes);
        }
      }

      @Override
      public void visit(String columnName, Clob clob) throws SQLException {
        if (clob != null) {
          // TODO: Would be so much better if we could stream this data
          String text = clob.getSubString(1, (int) clob.length());
          struct.put(columnName, text);
        }
      }

      @Override
      public void visit(String columnName, SQLXML sqlxml) throws SQLException {
        if (sqlxml != null) {
          // TODO: Would be so much better if we could stream this data
          String text = sqlxml.getString();
          struct.put(columnName, text);
        }
      }
    };
  }

  public static JdbcValueMapper structToJdbcValueMapper(Struct struct) {
    return new JdbcValueMapper() {
      @Override
      public Boolean getBoolean(String columnName) {
        return struct.getBoolean(columnName);
      }

      @Override
      public Byte getByte(String value) {
        return struct.getInt8(value);
      }

      @Override
      public Integer getInteger(String value) {
        return struct.getInt32(value);
      }

      @Override
      public Long getLong(String value) {
        return struct.getInt64(value);
      }

      @Override
      public Short getShort(String value) {
        return struct.getInt16(value);
      }

      @Override
      public String getString(String value) {
        return struct.getString(value);
      }
    };
  }
}

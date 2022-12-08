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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class JdbcQueryUtil {
  private static final Logger log = LoggerFactory.getLogger(JdbcQueryUtil.class);

  @FunctionalInterface
  private interface PrepareConsumer {
    void accept(PreparedStatement preparedStatement,
                int index,
                JDBCType jdbcType,
                Struct struct,
                String fieldName) throws SQLException;
  }

  @FunctionalInterface
  private interface ResultSetConsumer {
    void accept(ResultSet resultSet,
                String columnName,
                JdbcColumnVisitor columnVisitor) throws SQLException;
  }

  private static final Map<JDBCType, PrepareConsumer> jdbcTypePrepareMap = new HashMap<>();
  private static final Map<JDBCType, ResultSetConsumer> jdbcTypeResultSetMap = new HashMap<>();

  static {
    jdbcTypePrepareMap.put(JDBCType.BIGINT, JdbcQueryUtil::setLong);
    jdbcTypePrepareMap.put(JDBCType.BOOLEAN, JdbcQueryUtil::setBoolean);
    jdbcTypePrepareMap.put(JDBCType.CHAR, JdbcQueryUtil::setString);
    jdbcTypePrepareMap.put(JDBCType.INTEGER, JdbcQueryUtil::setInt);
    jdbcTypePrepareMap.put(JDBCType.LONGVARCHAR, JdbcQueryUtil::setString);
    jdbcTypePrepareMap.put(JDBCType.SMALLINT, JdbcQueryUtil::setShort);
    jdbcTypePrepareMap.put(JDBCType.TINYINT, JdbcQueryUtil::setByte);
    jdbcTypePrepareMap.put(JDBCType.VARCHAR, JdbcQueryUtil::setString);

    jdbcTypeResultSetMap.put(JDBCType.BLOB, JdbcQueryUtil::visitBlob);
    jdbcTypeResultSetMap.put(JDBCType.CLOB, JdbcQueryUtil::visitClob);
    jdbcTypeResultSetMap.put(JDBCType.SQLXML, JdbcQueryUtil::visitSqlXml);
  }

  public static boolean executeQuery(
      JdbcHashCache jdbcHashCache,
      JdbcConnection jdbcConnection,
      RetrySpec retrySpec,
      JdbcTableInfo tableInfo,
      List<JdbcColumn> primaryKeyColumns,
      Collection<JdbcColumn> columnsToQuery,
      Struct oldStruct,
      Struct newStruct,
      String primaryKeyStr
  ) {
    FilteredColumnToStructVisitor columnVisitor =
        new FilteredColumnToStructVisitor(
            jdbcHashCache,
            newStruct,
            tableInfo,
            primaryKeyStr
        );

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
        primaryKeyStr,
        tableInfo
    );

    return jdbcConnection.withPreparedStatement(retrySpec, sqlQuery, preparedStatement -> {
      int index = 1;
      for (JdbcColumn primaryKeyColumn : primaryKeyColumns) {
        JdbcQueryUtil.setPreparedValue(
            preparedStatement,
            index++,
            primaryKeyColumn.getJdbcType(),
            oldStruct,
            primaryKeyColumn.getName()
        );
      }

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (!resultSet.next()) {
          // TODO: How do we detect if incoming record is a DELETE?
          log.warn(
              "Cannot find Row {} in Table [{}]",
              primaryKeyStr,
              tableInfo
          );
        } else {
          log.debug(
              "Processing ResultSet from Query PK [{}] in Table [{}]",
              primaryKeyStr,
              tableInfo
          );

          // Read values from the DB into newValueStruct
          JdbcQueryUtil.visitResultSetColumns(resultSet, columnVisitor);

          // NOTE: We should only have a single result!
          if (resultSet.next()) {
            throw new DataException(
                "Got more than 1 row for Query PK ["
                + primaryKeyStr
                + "] in Table ["
                + tableInfo
                + "]"
            );
          }
        }
      } catch (SQLException ex) {
        throw new RetriableException(ex);
      }
      // TODO: Rollback?

      return columnVisitor.hasChangedColumns();
    });
  }

  public static void setPreparedValue(PreparedStatement preparedStatement,
                                      int index,
                                      JDBCType jdbcType,
                                      Struct struct,
                                      String fieldName
  ) {
    try {
      if (JDBCType.NULL == jdbcType) {
        preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
        return;
      }
      Optional
          .ofNullable(jdbcTypePrepareMap.get(jdbcType))
          .orElseThrow(
              () -> new DataException(
                  "Unsupported Query Column ["
                  + fieldName
                  + "] type ["
                  + jdbcType
                  + "] in PreparedStatement"
              )
          )
          .accept(preparedStatement, index, jdbcType, struct, fieldName);
    } catch (SQLException ex) {
      throw new RetriableException(ex);
    }
  }

  public static void visitResultSetColumns(ResultSet resultSet,
                                           JdbcColumnVisitor columnVisitor) throws SQLException {
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      String columnName = resultSetMetaData.getColumnName(i);
      JDBCType jdbcType = JDBCType.valueOf(resultSetMetaData.getColumnType(i));
      String tableName = resultSetMetaData.getTableName(i);

      // TODO: For now, we only support LOB types. Will we ever need any other types?
      Optional
          .ofNullable(jdbcTypeResultSetMap.get(jdbcType))
          .orElseThrow(() -> new ConnectException(
              "Unsupported ResultSet Column ["
              + columnName
              + "] type ["
              + jdbcType
              + "] in Table ["
              + tableName
              + "]"
          ))
          .accept(resultSet, columnName, columnVisitor);
    }
  }

  private static void setBoolean(PreparedStatement preparedStatement,
                                 int index,
                                 JDBCType jdbcType,
                                 Struct struct,
                                 String fieldName) throws SQLException {
    Boolean value = struct.getBoolean(fieldName);
    if (value != null) {
      preparedStatement.setBoolean(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setByte(PreparedStatement preparedStatement,
                              int index,
                              JDBCType jdbcType,
                              Struct struct,
                              String fieldName) throws SQLException {
    Byte value = struct.getInt8(fieldName);
    if (value != null) {
      preparedStatement.setByte(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setInt(PreparedStatement preparedStatement,
                             int index,
                             JDBCType jdbcType,
                             Struct struct,
                             String fieldName) throws SQLException {
    Integer value = struct.getInt32(fieldName);
    if (value != null) {
      preparedStatement.setInt(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setLong(PreparedStatement preparedStatement,
                              int index,
                              JDBCType jdbcType,
                              Struct struct,
                              String fieldName) throws SQLException {
    Long value = struct.getInt64(fieldName);
    if (value != null) {
      preparedStatement.setLong(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setShort(PreparedStatement preparedStatement,
                               int index,
                               JDBCType jdbcType,
                               Struct struct,
                               String fieldName) throws SQLException {
    Short value = struct.getInt16(fieldName);
    if (value != null) {
      preparedStatement.setShort(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setString(PreparedStatement preparedStatement,
                                int index,
                                JDBCType jdbcType,
                                Struct struct,
                                String fieldName) throws SQLException {
    String value = struct.getString(fieldName);
    if (value != null) {
      preparedStatement.setString(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void visitBlob(ResultSet resultSet,
                                String columnName,
                                JdbcColumnVisitor columnVisitor) throws SQLException {
    Blob blob = resultSet.getBlob(columnName);
    if (blob != null) {
      log.debug("Visit Column [{}] type [{}] = length [{}]",
                columnName,
                JDBCType.BLOB,
                blob.length()
      );
    } else {
      log.info(
          "Visit Column [{}] type [{}] = null",
          columnName,
          JDBCType.BLOB
      );
    }
    columnVisitor.visit(columnName, blob);
  }

  private static void visitClob(ResultSet resultSet,
                                String columnName,
                                JdbcColumnVisitor columnVisitor) throws SQLException {
    Clob clob = resultSet.getClob(columnName);
    if (clob != null) {
      log.debug("Visit Column [{}] type [{}] = length [{}]",
                columnName,
                JDBCType.CLOB,
                clob.length()
      );
    } else {
      log.info(
          "Visit Column [{}] type [{}] = null",
          columnName,
          JDBCType.CLOB
      );
    }
    columnVisitor.visit(columnName, clob);
  }

  private static void visitSqlXml(ResultSet resultSet,
                                  String columnName,
                                  JdbcColumnVisitor columnVisitor) throws SQLException {
    SQLXML sqlxml = resultSet.getSQLXML(columnName);
    log.debug("Visit Column [{}] type [{}] = isNull [{}]",
              columnName,
              JDBCType.SQLXML,
              sqlxml == null
    );
    columnVisitor.visit(columnName, sqlxml);
  }
}

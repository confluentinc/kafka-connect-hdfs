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

import org.apache.kafka.connect.errors.ConnectException;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JdbcPreparedStatement {
  private static final Logger log = LoggerFactory.getLogger(JdbcPreparedStatement.class);

  @FunctionalInterface
  private interface PrepareConsumer {
    void accept(PreparedStatement preparedStatement,
                JdbcColumn column,
                int index,
                JdbcValueMapper valueMapper) throws SQLException;
  }

  @FunctionalInterface
  private interface ResultSetConsumer {
    void accept(ResultSet resultSet,
                String columnName,
                JdbcColumnVisitor columnVisitor) throws SQLException;
  }

  private static final Map<JDBCType, PrepareConsumer> columnTypePrepareMap = new HashMap<>();
  private static final Map<JDBCType, ResultSetConsumer> columnTypeResultMap = new HashMap<>();

  static {
    columnTypePrepareMap.put(JDBCType.BIGINT, JdbcPreparedStatement::setLong);
    columnTypePrepareMap.put(JDBCType.BOOLEAN, JdbcPreparedStatement::setBoolean);
    columnTypePrepareMap.put(JDBCType.CHAR, JdbcPreparedStatement::setString);
    columnTypePrepareMap.put(JDBCType.INTEGER, JdbcPreparedStatement::setInteger);
    columnTypePrepareMap.put(JDBCType.LONGVARCHAR, JdbcPreparedStatement::setString);
    columnTypePrepareMap.put(JDBCType.NULL, JdbcPreparedStatement::setNull);
    columnTypePrepareMap.put(JDBCType.SMALLINT, JdbcPreparedStatement::setShort);
    columnTypePrepareMap.put(JDBCType.TINYINT, JdbcPreparedStatement::setByte);
    columnTypePrepareMap.put(JDBCType.VARCHAR, JdbcPreparedStatement::setString);
    columnTypeResultMap.put(JDBCType.BLOB, JdbcPreparedStatement::visitBlob);
    columnTypeResultMap.put(JDBCType.CLOB, JdbcPreparedStatement::visitClob);
    columnTypeResultMap.put(JDBCType.SQLXML, JdbcPreparedStatement::visitSqlXml);
  }

  private final PreparedStatement preparedStatement;

  public JdbcPreparedStatement(PreparedStatement preparedStatement) {
    this.preparedStatement = preparedStatement;
  }

  public void setColumn(JdbcColumn column,
                        int index,
                        JdbcValueMapper valueMapper) throws SQLException {
    Optional
        .ofNullable(columnTypePrepareMap.get(column.getJdbcType()))
        .orElseThrow(() -> new ConnectException(
            "Unsupported Where Column ["
                + column.getName()
                + "] type ["
                + column.getJdbcType()
                + "] in PreparedStatement"
        ))
        .accept(preparedStatement, column, index, valueMapper);
  }

  public ResultSet executeQuery() throws SQLException {
    return preparedStatement.executeQuery();
  }

  public void visitColumns(ResultSet resultSet,
                           JdbcColumnVisitor columnVisitor) throws SQLException {
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      String columnName = resultSetMetaData.getColumnName(i);
      JDBCType jdbcType = JDBCType.valueOf(resultSetMetaData.getColumnType(i));
      String tableName = resultSetMetaData.getTableName(i);

      // TODO: For now, we only support LOB types. Will we ever need any other types?
      Optional
          .ofNullable(columnTypeResultMap.get(jdbcType))
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
                                 JdbcColumn column,
                                 int index,
                                 JdbcValueMapper valueMapper) throws SQLException {
    Boolean value = valueMapper.getBoolean(column.getName());
    if (value != null) {
      preparedStatement.setBoolean(index, value);
    } else {
      setNull(preparedStatement, column, index, valueMapper);
    }
  }

  private static void setString(PreparedStatement preparedStatement,
                                JdbcColumn column,
                                int index,
                                JdbcValueMapper valueMapper) throws SQLException {

    String value = valueMapper.getString(column.getName());
    if (value != null) {
      preparedStatement.setString(index, value);
    } else {
      setNull(preparedStatement, column, index, valueMapper);
    }
  }

  private static void setLong(PreparedStatement preparedStatement,
                              JdbcColumn column,
                              int index,
                              JdbcValueMapper valueMapper) throws SQLException {

    Long value = valueMapper.getLong(column.getName());
    if (value != null) {
      preparedStatement.setLong(index, value);
    } else {
      setNull(preparedStatement, column, index, valueMapper);
    }
  }

  private static void setInteger(PreparedStatement preparedStatement,
                                 JdbcColumn column,
                                 int index,
                                 JdbcValueMapper valueMapper) throws SQLException {

    Integer value = valueMapper.getInteger(column.getName());
    if (value != null) {
      preparedStatement.setInt(index, value);
    } else {
      setNull(preparedStatement, column, index, valueMapper);
    }
  }

  private static void setShort(PreparedStatement preparedStatement,
                               JdbcColumn column,
                               int index,
                               JdbcValueMapper valueMapper) throws SQLException {
    Short value = valueMapper.getShort(column.getName());
    if (value != null) {
      preparedStatement.setShort(index, value);
    } else {
      setNull(preparedStatement, column, index, valueMapper);
    }
  }

  private static void setByte(PreparedStatement preparedStatement,
                              JdbcColumn column,
                              int index,
                              JdbcValueMapper valueMapper) throws SQLException {

    Byte value = valueMapper.getByte(column.getName());
    if (value != null) {
      preparedStatement.setByte(index, value);
    } else {
      setNull(preparedStatement, column, index, valueMapper);
    }
  }

  private static void setNull(PreparedStatement preparedStatement,
                              JdbcColumn column,
                              int index,
                              JdbcValueMapper valueMapper) throws SQLException {
    preparedStatement.setNull(
        index,
        column.getJdbcType().getVendorTypeNumber()
    );
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

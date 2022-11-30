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

public class JdbcQueryUtil {
  private static final Logger log = LoggerFactory.getLogger(JdbcQueryUtil.class);

  public static void prepareWhereColumn(PreparedStatement preparedStatement,
                                        JdbcColumn column,
                                        int index,
                                        JdbcValueMapper<String> jdbcValueMapper) throws SQLException {
    switch (column.getJdbcType()) {
      case BOOLEAN: {
        Boolean value = jdbcValueMapper.getBoolean(column.getName());
        if (value != null)
          preparedStatement.setBoolean(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case CHAR:
      case LONGVARCHAR:
      case VARCHAR: {
        String value = jdbcValueMapper.getString(column.getName());
        if (value != null)
          preparedStatement.setString(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case BIGINT: {
        Long value = jdbcValueMapper.getLong(column.getName());
        if (value != null)
          preparedStatement.setLong(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case INTEGER: {
        Integer value = jdbcValueMapper.getInteger(column.getName());
        if (value != null)
          preparedStatement.setInt(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case SMALLINT: {
        Short value = jdbcValueMapper.getShort(column.getName());
        if (value != null)
          preparedStatement.setShort(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case TINYINT: {
        Byte value = jdbcValueMapper.getByte(column.getName());
        if (value != null)
          preparedStatement.setByte(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case NULL: {
        preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      default:
        throw new ConnectException(
            "Unsupported Where Column ["
                + column.getName()
                + "] Type ["
                + column.getJdbcType()
                + "] in PreparedStatement"
        );
    }
  }

  public static void visitColumns(ResultSet resultSet, JdbcColumnVisitor visitor) throws SQLException {
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      String columnName = resultSetMetaData.getColumnName(i);
      JDBCType jdbcType = JDBCType.valueOf(resultSetMetaData.getColumnType(i));
      switch (jdbcType) {
        // TODO: For now, we only support LOB types. Will we ever need any other types?
        //case BOOLEAN: {
        //  Boolean value = resultSet.getBoolean(columnName);
        //  log.info("COLUMN [{}] TYPE [{}] NULL [{}]", columnName, jdbcType, value == null);
        //  visitor.visit(columnName, value);
        //  break;
        //}
        //case CHAR:
        //case LONGVARCHAR:
        //case VARCHAR: {
        //  String value = resultSet.getString(columnName);
        //  log.info("COLUMN [{}] TYPE [{}] NULL [{}]", columnName, jdbcType, value == null);
        //  visitor.visit(columnName, value);
        //  break;
        //}
        //case BIGINT: {
        //  Long value = resultSet.getLong(columnName);
        //  log.info("COLUMN [{}] TYPE [{}] NULL [{}]", columnName, jdbcType, value == null);
        //  visitor.visit(columnName, value);
        //  break;
        //}
        //case INTEGER: {
        //  Integer value = resultSet.getInt(columnName);
        //  log.info("COLUMN [{}] TYPE [{}] NULL [{}]", columnName, jdbcType, value == null);
        //  visitor.visit(columnName, value);
        //  break;
        //}
        //case SMALLINT: {
        //  Short value = resultSet.getShort(columnName);
        //  log.info("COLUMN [{}] TYPE [{}] NULL [{}]", columnName, jdbcType, value == null);
        //  visitor.visit(columnName, value);
        //  break;
        //}
        //case TINYINT: {
        //  Byte value = resultSet.getByte(columnName);
        //  log.info("COLUMN [{}] TYPE [{}] NULL [{}]", columnName, jdbcType, value == null);
        //  visitor.visit(columnName, value);
        //  break;
        //}
        case BLOB: {
          Blob blob = resultSet.getBlob(columnName);
          if (blob != null) {
            log.info("COLUMN [{}] [{}] = [length {}]",
                columnName,
                jdbcType,
                blob.length()
            );
          } else {
            log.info("COLUMN [{}] [{}] = null", columnName, jdbcType);
          }
          visitor.visit(columnName, blob);
          break;
        }
        case CLOB: {
          Clob clob = resultSet.getClob(columnName);
          if (clob != null) {
            log.info("COLUMN [{}] [{}] = [length {}]",
                columnName,
                jdbcType,
                clob.length()
            );
          } else {
            log.info("COLUMN [{}] [{}] = null", columnName, jdbcType);
          }
          visitor.visit(columnName, clob);
          break;
        }
        case SQLXML: {
          SQLXML sqlxml = resultSet.getSQLXML(columnName);
          log.info("COLUMN [{}] TYPE [{}] NULL [{}]", columnName, jdbcType, sqlxml == null);
          visitor.visit(columnName, sqlxml);
          break;
        }
        default:
          throw new ConnectException(
              "Unsupported Result Column [" + columnName + "] type [" + jdbcType + "] in Table [" + resultSet.getMetaData().getTableName(i) + "]"
          );
      }
    }
  }
}
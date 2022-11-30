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

import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcSchema {
  private static final Logger log = LoggerFactory.getLogger(JdbcSchema.class);

  public interface ColumnVisitor {
    void visit(String columnName, Boolean value);

    void visit(String columnName, Byte value);

    void visit(String columnName, byte[] value); // TODO

    void visit(String columnName, Integer value);

    void visit(String columnName, Long value);

    void visit(String columnName, Short value);

    void visit(String columnName, String value);
  }

  public static class StructColumnVisitor implements ColumnVisitor {
    public final Struct struct;

    public StructColumnVisitor(Struct struct) {
      this.struct = struct;
    }

    @Override
    public void visit(String columnName, Boolean value) {
      if (value != null) {
        struct.put(columnName, value);
      }
    }

    @Override
    public void visit(String columnName, Byte value) {
      if (value != null) {
        struct.put(columnName, value);
      }
    }

    @Override
    public void visit(String columnName, byte[] value) {
      if (value != null) {
        struct.put(columnName, value);
      }
    }

    @Override
    public void visit(String columnName, Integer value) {
      if (value != null) {
        struct.put(columnName, value);
      }
    }

    @Override
    public void visit(String columnName, Long value) {
      if (value != null) {
        struct.put(columnName, value);
      }
    }

    @Override
    public void visit(String columnName, Short value) {
      if (value != null) {
        struct.put(columnName, value);
      }
    }

    @Override
    public void visit(String columnName, String value) {
      if (value != null) {
        struct.put(columnName, value);
      }
    }
  }

  public interface JdbcTransformer<T> {
    Boolean getBoolean(T value);

    Byte getByte(T value);

    Integer getInteger(T value);

    Long getLong(T value);

    Short getShort(T value);

    String getString(T value);
  }

  public static class StructToJdbcTransformer implements JdbcTransformer<String> {
    public final Struct struct;

    public StructToJdbcTransformer(Struct struct) {
      this.struct = struct;
    }

    @Override
    public Boolean getBoolean(String value) {
      return struct.getBoolean(value);
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
  }

  public static void prepareWhereColumn(PreparedStatement preparedStatement,
                                         JdbcColumn column,
                                         int index,
                                         JdbcTransformer<String> jdbcTransformer) throws SQLException {
    switch (column.getJdbcType()) {
      case BOOLEAN: {
        Boolean value = jdbcTransformer.getBoolean(column.getName());
        if (value != null)
          preparedStatement.setBoolean(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case CHAR:
      case LONGVARCHAR:
      case VARCHAR: {
        String value = jdbcTransformer.getString(column.getName());
        if (value != null)
          preparedStatement.setString(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case BIGINT: {
        Long value = jdbcTransformer.getLong(column.getName());
        if (value != null)
          preparedStatement.setLong(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case INTEGER: {
        Integer value = jdbcTransformer.getInteger(column.getName());
        if (value != null)
          preparedStatement.setInt(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case SMALLINT: {
        Short value = jdbcTransformer.getShort(column.getName());
        if (value != null)
          preparedStatement.setShort(index, value);
        else
          preparedStatement.setNull(index, column.getJdbcType().getVendorTypeNumber());
        break;
      }
      case TINYINT: {
        Byte value = jdbcTransformer.getByte(column.getName());
        if (value != null)
          preparedStatement.setByte(index, value);
        else
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

  public static void visitColumns(ResultSet resultSet, ColumnVisitor visitor) throws SQLException {
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
      String columnName = resultSetMetaData.getColumnName(i);
      JDBCType jdbcType = JDBCType.valueOf(resultSetMetaData.getColumnType(i));
      switch (jdbcType) {
        case BOOLEAN: {
          Boolean value = resultSet.getBoolean(columnName);
          log.info("COLUMN [{}] [{}] = [{}]", columnName, jdbcType, value);
          visitor.visit(columnName, value);
          break;
        }
        case CHAR:
        case LONGVARCHAR:
        case VARCHAR: {
          String value = resultSet.getString(columnName);
          log.info("COLUMN [{}] [{}] = [{}]", columnName, jdbcType, value);
          visitor.visit(columnName, value);
          break;
        }
        case BIGINT: {
          Long value = resultSet.getLong(columnName);
          log.info("COLUMN [{}] [{}] = [{}]", columnName, jdbcType, value);
          visitor.visit(columnName, value);
          break;
        }
        case INTEGER: {
          Integer value = resultSet.getInt(columnName);
          log.info("COLUMN [{}] [{}] = [{}]", columnName, jdbcType, value);
          visitor.visit(columnName, value);
          break;
        }
        case SMALLINT: {
          Short value = resultSet.getShort(columnName);
          log.info("COLUMN [{}] [{}] = [{}]", columnName, jdbcType, value);
          visitor.visit(columnName, value);
          break;
        }
        case TINYINT: {
          Byte value = resultSet.getByte(columnName);
          log.info("COLUMN [{}] [{}] = [{}]", columnName, jdbcType, value);
          visitor.visit(columnName, value);
          break;
        }
        case CLOB: {
          Clob clob = resultSet.getClob(columnName);
          final byte[] value;
          if (clob != null) {
            log.info("COLUMN [{}] [{}] = [length {}]",
                columnName,
                jdbcType,
                clob.length()
            );
            // TODO: Would be so much better if we could stream this data, instead of loading it into a byte[]
            try (Reader clobReader = clob.getCharacterStream()) {
              value = IOUtils.toByteArray(clobReader, StandardCharsets.UTF_8);
            } catch (IOException ex) {
              throw new ConnectException(ex.getMessage(), ex);
            }
          } else {
            log.info("COLUMN [{}] [{}] = null", columnName, jdbcType);
            value = null;
          }
          visitor.visit(columnName, value);
          break;
        }
        // TODO: Time/date/timestamp/CLOB/BLOB/XML
        default:
          throw new ConnectException(
              "Unsupported Result Column [" + columnName + "] type [" + jdbcType + "] in Table [" + resultSet.getMetaData().getTableName(i) + "]"
          );
      }
    }
  }
}
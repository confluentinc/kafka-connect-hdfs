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

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class JdbcQueryUtil {
  private static final Logger log = LoggerFactory.getLogger(JdbcQueryUtil.class);

  @FunctionalInterface
  private interface PrepareConsumer {
    void accept(PreparedStatement preparedStatement,
                int index,
                JDBCType jdbcType,
                JdbcValueMapper valueMapper,
                String fieldName) throws SQLException;
  }

  @FunctionalInterface
  private interface ResultSetConsumer {
    void accept(ResultSet resultSet,
                JDBCType jdbcType,
                String columnName,
                JdbcColumnVisitor columnVisitor) throws SQLException;
  }

  private static final Map<JDBCType, PrepareConsumer> jdbcTypePrepareMap = new HashMap<>();
  private static final Map<JDBCType, ResultSetConsumer> jdbcTypeResultSetMap = new HashMap<>();

  static {
    // TODO: Add more as needed, if Primary Keys are other data types
    jdbcTypePrepareMap.put(JDBCType.BIGINT, JdbcQueryUtil::setLong);
    jdbcTypePrepareMap.put(JDBCType.BIT, JdbcQueryUtil::setBoolean);
    jdbcTypePrepareMap.put(JDBCType.BOOLEAN, JdbcQueryUtil::setBoolean);
    jdbcTypePrepareMap.put(JDBCType.CHAR, JdbcQueryUtil::setString);
    jdbcTypePrepareMap.put(JDBCType.DOUBLE, JdbcQueryUtil::setDouble);
    jdbcTypePrepareMap.put(JDBCType.FLOAT, JdbcQueryUtil::setFloat);
    jdbcTypePrepareMap.put(JDBCType.INTEGER, JdbcQueryUtil::setInt);
    jdbcTypePrepareMap.put(JDBCType.LONGVARCHAR, JdbcQueryUtil::setString);
    jdbcTypePrepareMap.put(JDBCType.REAL, JdbcQueryUtil::setFloat);
    jdbcTypePrepareMap.put(JDBCType.SMALLINT, JdbcQueryUtil::setShort);
    jdbcTypePrepareMap.put(JDBCType.TINYINT, JdbcQueryUtil::setByte);
    jdbcTypePrepareMap.put(JDBCType.VARCHAR, JdbcQueryUtil::setString);
    // TODO: Add more as needed, if Query results are not all LOBs or VARCHARS
    jdbcTypeResultSetMap.put(JDBCType.BLOB, JdbcQueryUtil::visitBlob);
    jdbcTypeResultSetMap.put(JDBCType.CLOB, JdbcQueryUtil::visitClob);
    jdbcTypeResultSetMap.put(JDBCType.LONGVARCHAR, JdbcQueryUtil::visitString);
    jdbcTypeResultSetMap.put(JDBCType.SQLXML, JdbcQueryUtil::visitSqlXml);
    jdbcTypeResultSetMap.put(JDBCType.VARCHAR, JdbcQueryUtil::visitString);
  }

  public static List<JdbcColumnInfo> fetchAllColumns(
      DataSource dataSource,
      JdbcTableInfo tableInfo
  ) throws SQLException {
    try (Connection connection = dataSource.getConnection();
         // We uppercase the schema and table because otherwise DB2 won't recognize them...
         ResultSet columns = connection.getMetaData().getColumns(
             null,
             JdbcUtil.toUpperCase(tableInfo.getSchema()),
             JdbcUtil.toUpperCase(tableInfo.getTable()),
             null
         )
    ) {
      List<JdbcColumnInfo> columnList = new LinkedList<>();
      while (columns.next()) {
        String columnName = columns.getString("COLUMN_NAME").trim();
        // WARNING: This returns the wrong value in some cases (2009/XML becomes 1111)
        int dataTypeNum = columns.getInt("DATA_TYPE");
        String dataTypeStr = columns.getString("DATA_TYPE");
        String typeName = columns.getString("TYPE_NAME");
        // TODO: Validate dataType against typeName
        JDBCType jdbcType = JDBCType.valueOf(Integer.parseInt(dataTypeStr));
        boolean nullable = columns.getBoolean("NULLABLE");
        //String isAutoIncrement = columns.getString("IS_AUTOINCREMENT");
        //int radix = columns.getInt("NUM_PREC_RADIX");
        int ordinal = columns.getInt("ORDINAL_POSITION");
        JdbcColumnInfo jdbcColumn = new JdbcColumnInfo(columnName, jdbcType, ordinal, nullable);
        log.debug(
            "Loaded Column for Table [{}] TypeName [{}] DataType [{} ==? {}] = {}",
            tableInfo,
            typeName,
            dataTypeStr,
            dataTypeNum,
            jdbcColumn
        );
        columnList.add(jdbcColumn);
      }

      return columnList
          .stream()
          .sorted(JdbcColumnInfo.byOrdinal)
          .collect(Collectors.toList());
    }
  }

  public static Set<String> fetchPrimaryKeyNames(
      DataSource dataSource,
      JdbcTableInfo tableInfo
  ) throws SQLException {
    try (Connection connection = dataSource.getConnection();
         // We uppercase the schema and table because otherwise DB2 won't recognize them...
         ResultSet columns = connection.getMetaData().getPrimaryKeys(
             null,
             JdbcUtil.toUpperCase(tableInfo.getSchema()),
             JdbcUtil.toUpperCase(tableInfo.getTable())
         )
    ) {
      Set<String> primaryKeyNames = new HashSet<>();
      while (columns.next()) {
        //String schem = columns.getString("TABLE_SCHEM");
        //String tn = columns.getString("TABLE_NAME");
        String columnName = columns.getString("COLUMN_NAME").trim();
        //String pkName = columns.getString("PK_NAME");
        //short kseq = columns.getShort("KEY_SEQ");
        primaryKeyNames.add(columnName);
      }
      log.debug("Table [{}] PrimaryKeys: {}", tableInfo, primaryKeyNames);
      return primaryKeyNames;
    }
  }

  public static void executeSingletonQuery(
      DataSource dataSource,
      JdbcTableInfo tableInfo,
      List<JdbcColumnInfo> primaryKeyColumns,
      Collection<JdbcColumnInfo> columnsToQuery,
      JdbcValueMapper valueMapper,
      JdbcColumnVisitor columnVisitor,
      String primaryKeyStr
  ) throws SQLException {
    String selectClause =
        columnsToQuery
            .stream()
            .map(JdbcColumnInfo::getName)
            .collect(Collectors.joining(","));

    String whereClause =
        primaryKeyColumns
            .stream()
            .map(JdbcColumnInfo::getName)
            .map(primaryKeyName -> primaryKeyName + "=?")
            .collect(Collectors.joining(" AND "));

    String sqlQuery =
        "SELECT "
        + selectClause
        + " FROM "
        + tableInfo.qualifiedName()
        + " WHERE "
        + whereClause
        + ";";

    // Execute the Query
    log.debug(
        "Executing SQL Query for PK [{}] in Table [{}]: {}",
        primaryKeyStr,
        tableInfo,
        sqlQuery
    );

    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
      int index = 1;
      for (JdbcColumnInfo primaryKeyColumn : primaryKeyColumns) {
        JdbcQueryUtil.setPreparedValue(
            preparedStatement,
            index++,
            primaryKeyColumn.getJdbcType(),
            valueMapper,
            primaryKeyColumn.getName()
        );
      }

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (!resultSet.next()) {
          // TODO: How do we detect if incoming record is a DELETE?
          //       So we don't end up writing large values for no reason.
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
      }
      // TODO: Rollback?
    }
  }

  public static void setPreparedValue(PreparedStatement preparedStatement,
                                      int index,
                                      JDBCType jdbcType,
                                      JdbcValueMapper valueMapper,
                                      String fieldName) throws SQLException {
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
        .accept(preparedStatement, index, jdbcType, valueMapper, fieldName);
  }

  public static void visitResultSetColumns(ResultSet resultSet,
                                           JdbcColumnVisitor columnVisitor) throws SQLException {
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    int columnCount = resultSetMetaData.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      String columnName = resultSetMetaData.getColumnName(i);
      JDBCType jdbcType = JDBCType.valueOf(resultSetMetaData.getColumnType(i));
      String tableName = resultSetMetaData.getTableName(i);

      Optional
          .ofNullable(jdbcTypeResultSetMap.get(jdbcType))
          .orElseThrow(() -> new DataException(
              "Unsupported ResultSet Column ["
              + columnName
              + "] type ["
              + jdbcType
              + "] in Table ["
              + tableName
              + "]"
          ))
          .accept(resultSet, jdbcType, columnName, columnVisitor);
    }
  }

  private static void setBoolean(PreparedStatement preparedStatement,
                                 int index,
                                 JDBCType jdbcType,
                                 JdbcValueMapper valueMapper,
                                 String fieldName) throws SQLException {
    Boolean value = valueMapper.getBoolean(fieldName);
    if (value != null) {
      preparedStatement.setBoolean(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setByte(PreparedStatement preparedStatement,
                              int index,
                              JDBCType jdbcType,
                              JdbcValueMapper valueMapper,
                              String fieldName) throws SQLException {
    Byte value = valueMapper.getByte(fieldName);
    if (value != null) {
      preparedStatement.setByte(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setDouble(PreparedStatement preparedStatement,
                                int index,
                                JDBCType jdbcType,
                                JdbcValueMapper valueMapper,
                                String fieldName) throws SQLException {
    Double value = valueMapper.getDouble(fieldName);
    if (value != null) {
      preparedStatement.setDouble(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setFloat(PreparedStatement preparedStatement,
                               int index,
                               JDBCType jdbcType,
                               JdbcValueMapper valueMapper,
                               String fieldName) throws SQLException {
    Float value = valueMapper.getFloat(fieldName);
    if (value != null) {
      preparedStatement.setFloat(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setInt(PreparedStatement preparedStatement,
                             int index,
                             JDBCType jdbcType,
                             JdbcValueMapper valueMapper,
                             String fieldName) throws SQLException {
    Integer value = valueMapper.getInteger(fieldName);
    if (value != null) {
      preparedStatement.setInt(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setLong(PreparedStatement preparedStatement,
                              int index,
                              JDBCType jdbcType,
                              JdbcValueMapper valueMapper,
                              String fieldName) throws SQLException {
    Long value = valueMapper.getLong(fieldName);
    if (value != null) {
      preparedStatement.setLong(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setShort(PreparedStatement preparedStatement,
                               int index,
                               JDBCType jdbcType,
                               JdbcValueMapper valueMapper,
                               String fieldName) throws SQLException {
    Short value = valueMapper.getShort(fieldName);
    if (value != null) {
      preparedStatement.setShort(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void setString(PreparedStatement preparedStatement,
                                int index,
                                JDBCType jdbcType,
                                JdbcValueMapper valueMapper,
                                String fieldName) throws SQLException {
    String value = valueMapper.getString(fieldName);
    if (value != null) {
      preparedStatement.setString(index, value);
    } else {
      preparedStatement.setNull(index, jdbcType.getVendorTypeNumber());
    }
  }

  private static void visitBlob(ResultSet resultSet,
                                JDBCType jdbcType,
                                String columnName,
                                JdbcColumnVisitor columnVisitor) throws SQLException {
    Blob value = resultSet.getBlob(columnName);
    log.debug(
        "Visit Column [{}] type [{}] length [{}]",
        columnName,
        jdbcType,
        (value != null) ? value.length() : null
    );
    columnVisitor.visit(columnName, value);
  }

  private static void visitClob(ResultSet resultSet,
                                JDBCType jdbcType,
                                String columnName,
                                JdbcColumnVisitor columnVisitor) throws SQLException {
    Clob value = resultSet.getClob(columnName);
    log.debug(
        "Visit Column [{}] type [{}] length [{}]",
        columnName,
        jdbcType,
        (value != null) ? value.length() : null
    );
    columnVisitor.visit(columnName, value);
  }

  private static void visitString(ResultSet resultSet,
                                  JDBCType jdbcType,
                                  String columnName,
                                  JdbcColumnVisitor columnVisitor) throws SQLException {
    String value = resultSet.getString(columnName);
    log.debug(
        "Visit Column [{}] type [{}] length [{}]",
        columnName,
        jdbcType,
        (value != null) ? value.length() : null
    );
    columnVisitor.visit(columnName, value);
  }

  private static void visitSqlXml(ResultSet resultSet,
                                  JDBCType jdbcType,
                                  String columnName,
                                  JdbcColumnVisitor columnVisitor) throws SQLException {
    SQLXML value = resultSet.getSQLXML(columnName);
    log.debug(
        "Visit Column [{}] type [{}] isNull? [{}]",
        columnName,
        jdbcType,
        (value == null)
    );
    columnVisitor.visit(columnName, value);
  }
}

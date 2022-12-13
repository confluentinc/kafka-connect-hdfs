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
                Struct struct,
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
    jdbcTypePrepareMap.put(JDBCType.BOOLEAN, JdbcQueryUtil::setBoolean);
    jdbcTypePrepareMap.put(JDBCType.CHAR, JdbcQueryUtil::setString);
    jdbcTypePrepareMap.put(JDBCType.INTEGER, JdbcQueryUtil::setInt);
    jdbcTypePrepareMap.put(JDBCType.LONGVARCHAR, JdbcQueryUtil::setString);
    jdbcTypePrepareMap.put(JDBCType.SMALLINT, JdbcQueryUtil::setShort);
    jdbcTypePrepareMap.put(JDBCType.TINYINT, JdbcQueryUtil::setByte);
    jdbcTypePrepareMap.put(JDBCType.VARCHAR, JdbcQueryUtil::setString);
    // TODO: Add more as needed, if Query results are not all LOBs
    jdbcTypeResultSetMap.put(JDBCType.BLOB, JdbcQueryUtil::visitBlob);
    jdbcTypeResultSetMap.put(JDBCType.CLOB, JdbcQueryUtil::visitClob);
    jdbcTypeResultSetMap.put(JDBCType.SQLXML, JdbcQueryUtil::visitSqlXml);
  }

  public static List<JdbcColumn> fetchAllColumns(
      DataSource dataSource,
      JdbcTableInfo tableInfo
  ) throws SQLException {
    try (Connection connection = dataSource.getConnection();
         // We uppercase the schema and table because otherwise DB2 won't recognize them...
         ResultSet columns = connection.getMetaData().getColumns(
             null,
             toUpperCase(tableInfo.getSchema()),
             toUpperCase(tableInfo.getTable()),
             null
         )
    ) {
      List<JdbcColumn> columnList = new LinkedList<>();
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
        JdbcColumn jdbcColumn = new JdbcColumn(columnName, jdbcType, ordinal, nullable);
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
          .sorted(JdbcColumn.byOrdinal)
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
             toUpperCase(tableInfo.getSchema()),
             toUpperCase(tableInfo.getTable())
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

  public static boolean executeQuery(
      JdbcHashCache jdbcHashCache,
      DataSource dataSource,
      JdbcTableInfo tableInfo,
      List<JdbcColumn> primaryKeyColumns,
      Collection<JdbcColumn> columnsToQuery,
      Struct oldValueStruct,
      Struct newValueStruct,
      Object oldKey
  ) throws SQLException {
    String primaryKeyStr = Optional
        .ofNullable(oldKey)
        .map(Object::toString)
        .map(String::trim)
        .orElse("");

    String selectClause =
        columnsToQuery
            .stream()
            .map(JdbcColumn::getName)
            .collect(Collectors.joining(","));

    String whereClause =
        primaryKeyColumns
            .stream()
            .map(JdbcColumn::getName)
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
      for (JdbcColumn primaryKeyColumn : primaryKeyColumns) {
        JdbcQueryUtil.setPreparedValue(
            preparedStatement,
            index++,
            primaryKeyColumn.getJdbcType(),
            oldValueStruct,
            primaryKeyColumn.getName()
        );
      }

      FilteredColumnToStructVisitor columnVisitor =
          new FilteredColumnToStructVisitor(
              jdbcHashCache,
              newValueStruct,
              tableInfo,
              primaryKeyStr
          );

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
      }
      // TODO: Rollback?

      return columnVisitor.hasChangedColumns();
    }
  }

  public static void setPreparedValue(PreparedStatement preparedStatement,
                                      int index,
                                      JDBCType jdbcType,
                                      Struct struct,
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
        .accept(preparedStatement, index, jdbcType, struct, fieldName);
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

  private static void visitSqlXml(ResultSet resultSet,
                                  JDBCType jdbcType,
                                  String columnName,
                                  JdbcColumnVisitor columnVisitor) throws SQLException {
    SQLXML value = resultSet.getSQLXML(columnName);
    log.debug(
        "Visit Column [{}] type [{}] state [{}]",
        columnName,
        jdbcType,
        (value != null) ? "not-null" : null
    );
    columnVisitor.visit(columnName, value);
  }

  private static String toUpperCase(String value) {
    return Optional
        .ofNullable(value)
        .map(String::toUpperCase)
        .orElse(null);
  }
}

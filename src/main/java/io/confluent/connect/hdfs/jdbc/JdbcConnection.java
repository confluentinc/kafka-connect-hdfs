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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * HdfsSinkConnector is a Kafka Connect Connector implementation that ingest data from Kafka to
 * HDFS.
 */
public class JdbcConnection implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(JdbcConnection.class);

  private final String jdbcUrl;
  private final Properties jdbcProperties;
  private Connection connection;

  public JdbcConnection(String jdbcUrl, Properties jdbcProperties) {
    this.jdbcUrl = jdbcUrl;
    this.jdbcProperties = jdbcProperties;
  }

  /**
   * TODO: Close and reconnect if the connection fails or times out
   */
  public synchronized Connection getConnection() throws SQLException {
    // These config names are the same for both source and sink configs ...
    // Timeout is 40 seconds to be as long as possible for customer to have a long connection
    // handshake, while still giving enough time to validate once in the follower worker,
    // and again in the leader worker and still be under 90s REST serving timeout
    DriverManager.setLoginTimeout(40);
    if (connection == null) {
      log.info("Creating JDBC connection to {}", jdbcUrl);
      connection = DriverManager.getConnection(jdbcUrl, jdbcProperties);
      log.info("Created JDBC connection [{}] to {}", connection.hashCode(), jdbcUrl);
    }
    return connection;
  }

  @Override
  public synchronized void close() {
    Connection oldConnection = connection;
    connection = null;
    if (oldConnection != null) {
      log.info("Closing JDBC Connection [{}} to {}", oldConnection.hashCode(), jdbcUrl);
      try {
        oldConnection.close();
      } catch (Exception ex) {
        log.warn("Failed to close connection [{}] to {}", oldConnection.hashCode(), jdbcUrl);
      }
    }
  }

  public List<JdbcColumn> fetchAllColumns(JdbcTableInfo tableInfo) throws SQLException {
    return withConnection(connection -> {
      // We uppercase the schema and table because otherwise DB2 won't recognize them...
      try (
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
          // NOTE: This returns the wrong value in some cases (2009/XML becomes 1111)
          //int dataType = columns.getInt("DATA_TYPE");
          String dataTypeAsString = columns.getString("DATA_TYPE");
          String typeName = columns.getString("TYPE_NAME");
          log.debug(
              "Table [{}] Column [{}] Type [{}] TypeNum [{}]",
              tableInfo,
              columnName,
              typeName,
              dataTypeAsString
          );
          // TODO: Validate dataType against typeName
          JDBCType dataType = JDBCType.valueOf(Integer.parseInt(dataTypeAsString));
          boolean nullable = columns.getBoolean("NULLABLE");
          //String isAutoIncrement = columns.getString("IS_AUTOINCREMENT");
          //int radix = columns.getInt("NUM_PREC_RADIX");
          int ordinal = columns.getInt("ORDINAL_POSITION");
          columnList.add(new JdbcColumn(columnName, dataType, ordinal, nullable));
        }

        return columnList
            .stream()
            .sorted(JdbcColumn.byOrdinal)
            .collect(Collectors.toList());
      }
    });
  }

  public Set<String> fetchPrimaryKeyNames(JdbcTableInfo tableInfo) throws SQLException {
    return withConnection(connection -> {
      // We uppercase the schema and table because otherwise DB2 won't recognize them...
      try (
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
    });
  }

  public <R> R withStatement(SqlMethod.Function<Statement, R> fn) throws SQLException {
    return withConnection(connection -> {
      try (Statement stmt = getConnection().createStatement()) {
        return fn.apply(stmt);
      }
    });
  }

  public <R> R withPreparedStatement(
      String query,
      SqlMethod.Function<PreparedStatement, R> fn
  ) throws SQLException {
    return withConnection(connection -> {
      try (PreparedStatement pstmt = connection.prepareStatement(query)) {
        return fn.apply(pstmt);
      }
    });
  }

  public <T> T withConnection(SqlMethod.Function<Connection, T> fn) throws SQLException {
    try {
      return fn.apply(getConnection());
    } catch (SQLException ex) {
      log.error("Caught SQLException: {}", ex.getMessage(), ex);
      throw ex;
    }
  }

  public <T> T withConnectionAndRetries(
      int attempt,
      int maxRetries,
      SqlMethod.Function<Connection, T> fn
  ) throws SQLException {
    try {
      return fn.apply(getConnection());
    } catch (SQLException ex) {
      if (attempt < maxRetries) {
        log.warn(
            "Caught SQLException; will reconnect and retry: [attempt {}] {}",
            attempt,
            ex.getMessage(),
            ex
        );
        close();
        return withConnectionAndRetries(attempt + 1, maxRetries, fn);
      }
      log.error("Caught SQLException; no more retries: {}", ex.getMessage(), ex);
      throw ex;
    }
  }

  private static String toUpperCase(String value) {
    return Optional
        .ofNullable(value)
        .map(String::toUpperCase)
        .orElse(null);
  }
}

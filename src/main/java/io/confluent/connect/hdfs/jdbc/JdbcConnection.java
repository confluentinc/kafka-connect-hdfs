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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Wrapper of a singleton JDBC Connection, managing retries and reconnects.
 * NOTE: This class is not meant for parallel calls on the same Connection.
 */
public class JdbcConnection implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(JdbcConnection.class);

  private final String jdbcUrl;
  private final Properties jdbcProperties;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private volatile Connection currentConnection;

  public JdbcConnection(String jdbcUrl, Properties jdbcProperties) {
    this.jdbcUrl = jdbcUrl;
    this.jdbcProperties = jdbcProperties;

    // These config names are the same for both source and sink configs ...
    // Timeout is 40 seconds to be as long as possible for customer to have a long connection
    // handshake, while still giving enough time to validate once in the follower worker,
    // and again in the leader worker and still be under 90s REST serving timeout
    DriverManager.setLoginTimeout(40);
  }

  @Override
  public void close() {
    isClosed.set(true);
    // NOTE: setCurrentConnection() may block while a new Connection is being constructed
    Connection oldConnection = setCurrentConnection(null);
    closeConnection(oldConnection);
  }

  public List<JdbcColumn> fetchAllColumns(JdbcRetrySpec retrySpec,
                                          JdbcTableInfo tableInfo) throws SQLException {
    return withConnection(retrySpec, connection -> {
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

  public Set<String> fetchPrimaryKeyNames(JdbcRetrySpec retrySpec,
                                          JdbcTableInfo tableInfo) throws SQLException {
    return withConnection(retrySpec, connection -> {
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

  public <R> R withPreparedStatement(
      JdbcRetrySpec retrySpec,
      String sqlQuery,
      SqlFunction<JdbcPreparedStatement, R> sqlFn
  ) throws SQLException {
    return withConnection(retrySpec, connection -> {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
        return sqlFn.apply(new JdbcPreparedStatement(preparedStatement));
      }
    });
  }

  public void withPreparedStatement(
      JdbcRetrySpec retrySpec,
      String sqlQuery,
      SqlConsumer<JdbcPreparedStatement> sqlConsumer
  ) throws SQLException {
    withConnection(retrySpec, connection -> {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
        sqlConsumer.accept(new JdbcPreparedStatement(preparedStatement));
      }
      return null;
    });
  }

  @Override
  public String toString() {
    int connectionHash =
        Optional
            .ofNullable(currentConnection)
            .map(Object::hashCode)
            .orElse(0);
    return "JdbcConnection{"
        + "isClosed=" + isClosed
        + ", jdbcUrl='" + jdbcUrl
        + "', currentConnectionHash=" + connectionHash
        + "}";
  }

  private <T> T withConnection(JdbcRetrySpec retrySpec,
                               SqlFunction<Connection, T> sqlFn) throws SQLException {
    int timesRetried = 0;
    int maxRetries = Math.max(0, retrySpec.getMaxRetries());

    while (true) {
      try {
        Connection connection = getOrCreateConnection();
        return sqlFn.apply(connection);
      } catch (SQLException ex) {

        if (timesRetried++ >= maxRetries) {
          log.error("Caught SQLException; no more retries: {}", ex.getMessage(), ex);
          throw ex;
        }

        log.warn(
            "Caught SQLException; will reconnect and retry [{}/{}] in [{} ms]: {}",
            timesRetried,
            maxRetries,
            retrySpec.getBackoff(),
            ex.getMessage(),
            ex
        );

        closeConnection(setCurrentConnection(null));

        sleep(retrySpec.getBackoff());
      }
    }
  }

  private static String toUpperCase(String value) {
    return Optional
        .ofNullable(value)
        .map(String::toUpperCase)
        .orElse(null);
  }

  private static void sleep(Duration duration) {
    long durationMillis =
        Optional
            .ofNullable(duration)
            .filter(((Predicate<Duration>) Duration::isNegative).negate())
            .map(Duration::toMillis)
            .orElse(0L);

    if (durationMillis > 0) {
      try {
        Thread.sleep(durationMillis);
      } catch (InterruptedException e) {
        // this is okay, we just wake up early
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * TODO: This Connection NOT Threadsafe, so do not access across threads
   */
  private Connection getOrCreateConnection() throws SQLException {
    if (isClosed.get()) {
      throw new ConnectException("Cannot access closed Connection: " + this);
    }

    Connection tmpConnection = currentConnection;
    return (tmpConnection != null)
        ? tmpConnection
        // NOTE: Connection might have been closed by JdbcConnection.close(), but do we care?
        : getOrCreateConnectionSync();
  }

  private synchronized Connection getOrCreateConnectionSync() throws SQLException {
    if (isClosed.get()) {
      throw new ConnectException("Cannot access closed Connection: " + this);
    }

    Connection tmpConnection = currentConnection;
    if (tmpConnection == null) {
      // Double-check the connection _after_ creation,
      // as this outer class may have been close()d in the interim
      try {
        log.info("Creating JDBC connection to {}", jdbcUrl);
        tmpConnection = DriverManager.getConnection(jdbcUrl, jdbcProperties);
        log.info(
            "Created JDBC connection [{}] to {}",
            tmpConnection.hashCode(),
            jdbcUrl
        );
      } catch (SQLException | RuntimeException ex) {
        closeConnection(tmpConnection);
        throw ex;
      }

      // Did the outer/wrapping(this) JdbcConnection get closed in the interim?
      if (isClosed.get()) {
        closeConnection(tmpConnection);
        throw new ConnectException("Cannot access closed Connection: " + this);
      }
      this.setCurrentConnection(tmpConnection);
    }

    return tmpConnection;
  }

  private void closeConnection(Connection connection) {
    if (connection != null) {
      log.info("Closing JDBC Connection [{}] to {}", connection.hashCode(), jdbcUrl);
      try {
        connection.close();
      } catch (Exception ex) {
        log.warn("Failed to close connection [{}] to {}", connection.hashCode(), jdbcUrl);
      }
    }
  }

  private synchronized Connection setCurrentConnection(Connection newConnection) {
    Connection oldConnection = currentConnection;
    currentConnection = newConnection;
    return oldConnection;
  }
}

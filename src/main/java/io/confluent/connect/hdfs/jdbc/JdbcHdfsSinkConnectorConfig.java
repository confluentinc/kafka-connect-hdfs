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

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcHdfsSinkConnectorConfig extends HdfsSinkConnectorConfig {
  public static final String DATABASE_GROUP = "Database";
  public static final String HASH_CACHE_GROUP = "HashCache";

  public static final String CONNECTION_URL_CONFIG = "connection.url";
  public static final String CONNECTION_URL_DOC = "JDBC connection URL.";
  public static final String CONNECTION_URL_DISPLAY = "JDBC URL";

  public static final String CONNECTION_USER_CONFIG = "connection.user";
  public static final String CONNECTION_USER_DOC = "JDBC connection user.";
  public static final String CONNECTION_USER_DISPLAY = "JDBC User";

  public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
  public static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
  public static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

  public static final String HASH_CACHE_SIZE_CONFIG = "hash.cache.size";
  public static final String HASH_CACHE_SIZE_DOC =
      "Maximum size of the Hash Cache. LRU entries are evicted from the cache.";
  public static final String HASH_CACHE_SIZE_DISPLAY = "Hash Cache size";
  public static final int HASH_CACHE_SIZE_DEFAULT = 10000;

  public static final String JDBC_FILTERS_CONFIG = "jdbc.filters";
  public static final String JDBC_FILTERS_DOC =
      "List of names to filter. Each name denotes a distinct Table";
  public static final String JDBC_FILTERS_DISPLAY = "Hash Cache size";

  // Example:
  //   jdbc.filters=dvtest,tableX
  //   jdbc.filters.dvtest.db=testdb
  //   jdbc.filters.dvtest.schema=DB2INST1
  //   jdbc.filters.dvtest.table=DV_TEST
  //   jdbc.filters.dvtest.columns=db_action,VALUE_CLOB,VALUE_XML,Value_Blob
  //   jdbc.filters.tableX.db=dbX
  //   jdbc.filters.tableX.schema=schemaX
  //   jdbc.filters.tableX.table=tableX
  //   jdbc.filters.tableX.columns=c1,c2,c3

  public static final String JDBC_FILTER_DB = "db";
  public static final String JDBC_FILTER_SCHEMA = "schema";
  public static final String JDBC_FILTER_TABLE = "table";
  public static final String JDBC_FILTER_COLUMNS = "columns";

  public static ConfigDef newConfigDef() {
    int orderInDatabaseGroup = 0;
    int orderInHashCacheGroup = 0;
    return HdfsSinkConnectorConfig
        .newConfigDef()
        // Define Database configuration group
        .define(
            CONNECTION_URL_CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            CONNECTION_URL_DOC,
            DATABASE_GROUP,
            ++orderInDatabaseGroup,
            Width.MEDIUM,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_USER_CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            CONNECTION_USER_DOC,
            DATABASE_GROUP,
            ++orderInDatabaseGroup,
            Width.MEDIUM,
            CONNECTION_USER_DISPLAY
        )
        .define(
            CONNECTION_PASSWORD_CONFIG,
            Type.PASSWORD,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            CONNECTION_PASSWORD_DOC,
            DATABASE_GROUP,
            ++orderInDatabaseGroup,
            Width.SHORT,
            CONNECTION_PASSWORD_DISPLAY
        )
        .define(
            JDBC_FILTERS_CONFIG,
            Type.LIST,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            JDBC_FILTERS_DOC,
            DATABASE_GROUP,
            ++orderInDatabaseGroup,
            Width.LONG,
            JDBC_FILTERS_DISPLAY
        )
        .define(
            HASH_CACHE_SIZE_CONFIG,
            Type.INT,
            HASH_CACHE_SIZE_DEFAULT,
            Importance.LOW,
            HASH_CACHE_SIZE_DOC,
            HASH_CACHE_GROUP,
            ++orderInHashCacheGroup,
            Width.MEDIUM,
            HASH_CACHE_SIZE_DISPLAY
        );
  }

  public JdbcHdfsSinkConnectorConfig(Map<String, String> props) {
    this(newConfigDef(), addDefaults(props));
  }

  protected JdbcHdfsSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
  }

  public String name() {
    return originalsStrings().getOrDefault("name", "JDBC-HDFS-sink");
  }

  public static ConfigDef getConfig() {
    return HdfsSinkConnectorConfig.getConfig();
  }

  public String getConnectionUrl() {
    return JdbcUtil
        .trimToNone(getString(CONNECTION_URL_CONFIG))
        .orElseThrow(() -> new ConfigException(
            "Missing or empty String value for required [" + CONNECTION_URL_CONFIG + "]"
        ));
  }

  public String getConnectionUser() {
    return JdbcUtil
        .trimToNone(getString(CONNECTION_USER_CONFIG))
        .orElseThrow(() -> new ConfigException(
            "Missing or empty String value for required [" + CONNECTION_USER_CONFIG + "]"
        ));
  }

  public Password getConnectionPassword() {
    return Optional
        .ofNullable(getPassword(CONNECTION_PASSWORD_CONFIG))
        .orElseThrow(() -> new ConfigException(
            "Missing Password value for required [" + CONNECTION_PASSWORD_CONFIG + "]"
        ));
  }

  public int getHashCacheSize() {
    return getInt(HASH_CACHE_SIZE_CONFIG);
  }

  public Map<JdbcTableInfo, Set<String>> getJdbcFilterMap() {
    Map<JdbcTableInfo, Set<String>> filterMap =
        getList(JDBC_FILTERS_CONFIG)
            .stream()
            .map(JdbcUtil::trimToNull)
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(
                this::jdbcFilterToTable,
                this::jdbcFilterToColumns
            ));

    if (filterMap.isEmpty()) {
      throw new ConfigException(
          "Empty list of filter names for ["
          + JDBC_FILTERS_CONFIG
          + "]. Must be a comma-separated list."
      );
    }

    return filterMap;
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }

  private String prefixOf(String filterName) {
    return JDBC_FILTERS_CONFIG
           + "."
           + filterName
           + ".";
  }

  // Refactor to use a ConfigDef for parsing each JDBC Filter
  private JdbcTableInfo jdbcFilterToTable(String filterName) {
    String prefix = prefixOf(filterName);

    Map<String, Object> filterConfig = originalsWithPrefix(prefix);

    String db = Optional
        .ofNullable(filterConfig.get(JDBC_FILTER_DB))
        .map(String.class::cast)
        .flatMap(JdbcUtil::trimToNone)
        .orElseThrow(() -> new ConfigException(
            "Missing or empty String value for required ["
            + prefix
            + JDBC_FILTER_DB
            + "]"
        ));

    String schema = Optional
        .ofNullable(filterConfig.get(JDBC_FILTER_SCHEMA))
        .map(String.class::cast)
        .flatMap(JdbcUtil::trimToNone)
        .orElseThrow(() -> new ConfigException(
            "Missing or empty String value for required ["
            + prefix
            + JDBC_FILTER_SCHEMA
            + "]"
        ));

    String table = Optional
        .ofNullable(filterConfig.get(JDBC_FILTER_TABLE))
        .map(String.class::cast)
        .flatMap(JdbcUtil::trimToNone)
        .orElseThrow(() -> new ConfigException(
            "Missing or empty String value for required ["
            + prefix
            + JDBC_FILTER_TABLE
            + "]"
        ));

    return new JdbcTableInfo(db, schema, table);
  }

  private Set<String> jdbcFilterToColumns(String filterName) {
    String prefix = prefixOf(filterName);

    Map<String, Object> filterConfig = originalsWithPrefix(prefix);

    // Columns are required, and must not be empty
    Set<String> columns = Optional
        .ofNullable(filterConfig.get(JDBC_FILTER_COLUMNS))
        .map(String.class::cast)
        .map(csvColumns -> csvColumns.split(","))
        .map(Arrays::stream)
        .orElseGet(Stream::empty)
        .map(JdbcUtil::trimToNull)
        .filter(Objects::nonNull)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());

    if (columns.isEmpty()) {
      throw new ConfigException(
          "Missing or empty list of columns for ["
          + prefix
          + JDBC_FILTER_COLUMNS
          + "]. Must be a comma-separated list."
      );
    }

    return columns;
  }
}

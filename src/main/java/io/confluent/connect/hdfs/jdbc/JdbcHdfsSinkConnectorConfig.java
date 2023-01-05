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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

  public static final String COLUMN_INCLUDE_LIST_CONFIG = "column.include.list";
  public static final String COLUMN_INCLUDE_FORMAT = "<schema>.<table>.<column>";
  public static final String COLUMN_INCLUDE_LIST_DOC =
      "List of fully-qualified columns to include. IE: " + COLUMN_INCLUDE_FORMAT;
  public static final String COLUMN_INCLUDE_LIST_DISPLAY = "Columns to include";

  public static final String HASH_CACHE_ENABLED_CONFIG = "hash.cache.enabled";
  public static final String HASH_CACHE_ENABLED_DOC =
      "Enable support for Caching Hashed values for large columns,"
      + " to prevent writing duplicate values.";
  public static final String HASH_CACHE_ENABLED_DISPLAY = "Hash Cache size";
  public static final boolean HASH_CACHE_ENABLED_DEFAULT = true;

  public static final String HASH_CACHE_SIZE_CONFIG = "hash.cache.size";
  public static final String HASH_CACHE_SIZE_DOC =
      "Maximum size of the Hash Cache. LRU entries are evicted from the cache.";
  public static final String HASH_CACHE_SIZE_DISPLAY = "Hash Cache size";
  public static final int HASH_CACHE_SIZE_DEFAULT = 10000;

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
            COLUMN_INCLUDE_LIST_CONFIG,
            Type.LIST,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            COLUMN_INCLUDE_LIST_DOC,
            DATABASE_GROUP,
            ++orderInDatabaseGroup,
            Width.LONG,
            COLUMN_INCLUDE_LIST_DISPLAY
        )
        .define(
            HASH_CACHE_ENABLED_CONFIG,
            Type.BOOLEAN,
            HASH_CACHE_ENABLED_DEFAULT,
            Importance.LOW,
            HASH_CACHE_ENABLED_DOC,
            HASH_CACHE_GROUP,
            ++orderInHashCacheGroup,
            Width.SHORT,
            HASH_CACHE_ENABLED_DISPLAY
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

  public boolean isHashCacheEnabled() {
    return getBoolean(HASH_CACHE_ENABLED_CONFIG);
  }

  public int getHashCacheSize() {
    return getInt(HASH_CACHE_SIZE_CONFIG);
  }

  public Map<JdbcTableInfo, Set<String>> getIncludedFieldsLower() {
    Map<JdbcTableInfo, Set<String>> includedFieldsLowerMap =
        getList(COLUMN_INCLUDE_LIST_CONFIG)
            .stream()
            .map(JdbcUtil::trimToNull)
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(
                this::toTable,
                Collectors.mapping(this::toColumnLower, Collectors.toSet())
            ));

    if (includedFieldsLowerMap.isEmpty()) {
      throw new ConfigException(
          "Empty list of columns/fields to include for key ["
          + COLUMN_INCLUDE_LIST_CONFIG
          + "]. Must be a comma-separated list. IE: "
          + COLUMN_INCLUDE_FORMAT
      );
    }

    return includedFieldsLowerMap;
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }

  private List<String> columnStringToList(String fullyQualifiedColumn) {
    List<String> splitList = Arrays.asList(fullyQualifiedColumn.split("\\."));

    if (splitList.size() != 3) {
      throw new ConfigException(
          "Incomplete value for "
          + COLUMN_INCLUDE_LIST_CONFIG
          + "] value ["
          + fullyQualifiedColumn
          + "] expected "
          + COLUMN_INCLUDE_FORMAT
      );
    }

    return splitList;
  }

  private JdbcTableInfo toTable(String fullyQualifiedColumn) {
    List<String> schemaTableColumn = columnStringToList(fullyQualifiedColumn);

    String schema = JdbcUtil
        .trimToNone(schemaTableColumn.get(0))
        .map(String::toLowerCase)
        .orElseThrow(() -> new ConfigException(
            "Empty <schema> value for key ["
            + COLUMN_INCLUDE_LIST_CONFIG
            + "] value ["
            + fullyQualifiedColumn
            + "] expected "
            + COLUMN_INCLUDE_FORMAT
        ));

    String table = JdbcUtil
        .trimToNone(schemaTableColumn.get(1))
        .map(String::toLowerCase)
        .orElseThrow(() -> new ConfigException(
            "Empty <table> value for key ["
            + COLUMN_INCLUDE_LIST_CONFIG
            + "] value ["
            + fullyQualifiedColumn
            + "] expected "
            + COLUMN_INCLUDE_FORMAT
        ));

    return new JdbcTableInfo(schema, table);
  }

  private String toColumnLower(String fullyQualifiedColumn) {
    List<String> schemaTableColumn = columnStringToList(fullyQualifiedColumn);

    return JdbcUtil
        .trimToNone(schemaTableColumn.get(2))
        .map(String::toLowerCase)
        .orElseThrow(() -> new ConfigException(
            "Empty <column> value for key ["
            + COLUMN_INCLUDE_LIST_CONFIG
            + "] value ["
            + fullyQualifiedColumn
            + "] expected "
            + COLUMN_INCLUDE_FORMAT
        ));
  }
}

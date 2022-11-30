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

import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfiguredTables {
  private static final String CONFIG_ERROR_MSG =
      "must match 'jdbc.<db>.<schema>.<table>=<column1>,<column2>,...'";
  private static final String CONFIG_JDBC_PREFIX = "jdbc.";

  private final Map<JdbcTableInfo, Set<String>> tableFieldNamesLowerMap;

  public ConfiguredTables(Map<String, String> props) {
    // TODO: Move this parsing into JdbcHdfsSinkConnectorConfig
    tableFieldNamesLowerMap = props
        .entrySet()
        .stream()
        .filter(entry -> entry.getKey().trim().toLowerCase().startsWith(CONFIG_JDBC_PREFIX))
        .collect(Collectors.toMap(
            entry -> configKeyTableInfo(entry.getKey()),
            ConfiguredTables::valueToColumns
        ));
  }

  /**
   * NOTE: Not threadsafe
   */
  public Set<String> getColumnNamesLower(JdbcTableInfo tableInfo) {
    return tableFieldNamesLowerMap.computeIfAbsent(
        tableInfo,
        __ -> Collections.emptySet()
    );
  }

  private static Optional<String> trimToNone(String value) {
    return Optional
        .ofNullable(value)
        .map(String::trim)
        .filter(((Predicate<String>) String::isEmpty).negate());
  }

  private static JdbcTableInfo configKeyTableInfo(String key) {
    List<String> keyPath = Arrays.asList(key.trim().toLowerCase().split("\\."));
    // First value in the array should be ~ CONFIG_JDBC_PREFIX (minus the trailing dot)
    if (keyPath.size() < 4) {
      throw new ConfigException("Invalid config parameter [" + key + "] " + CONFIG_ERROR_MSG);
    }

    String db =
        trimToNone(keyPath.get(1))
            .orElseThrow(() -> new ConfigException(
                "Invalid config parameter [" + key + "] with empty [db] " + CONFIG_ERROR_MSG
            ));

    String schema =
        trimToNone(keyPath.get(2))
            .orElseThrow(() -> new ConfigException(
                "Invalid config parameter [" + key + "] with empty [schema] " + CONFIG_ERROR_MSG
            ));

    String table =
        trimToNone(keyPath.get(3))
            .orElseThrow(() -> new ConfigException(
                "Invalid config parameter [" + key + "] with empty [table] " + CONFIG_ERROR_MSG
            ));

    return new JdbcTableInfo(db, schema, table);
  }

  private static Set<String> valueToColumns(Map.Entry<?, String> entry) {
    Set<String> fieldNames = Arrays
        .stream(entry.getValue().split(","))
        .flatMap(fieldName -> trimToNone(fieldName).map(Stream::of).orElseGet(Stream::empty))
        .map(String::toLowerCase)
        .collect(Collectors.toSet());

    if (fieldNames.isEmpty()) {
      throw new ConfigException(
          "Empty list of fields for ["
              + entry.getKey()
              + "]. Must be a comma-separated list."
      );
    }

    return Collections.unmodifiableSet(fieldNames);
  }

}

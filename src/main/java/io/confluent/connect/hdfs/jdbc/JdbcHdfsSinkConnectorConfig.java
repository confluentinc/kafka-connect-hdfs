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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;

public class JdbcHdfsSinkConnectorConfig extends HdfsSinkConnectorConfig {
  public static final String CONNECTION_PREFIX = "connection";
  public static final String DATABASE_GROUP = "Database";

  public interface ConnectionUrl {
    String CONFIG = String.join(".", CONNECTION_PREFIX, "url");
    String DOC = "JDBC connection URL.";
    String DISPLAY = "JDBC URL";
  }

  public interface ConnectionUser {
    String CONFIG = String.join(".", CONNECTION_PREFIX, "user");
    String DOC = "JDBC connection user.";
    String DISPLAY = "JDBC User";
  }

  public interface ConnectionPassword {
    String CONFIG = String.join(".", CONNECTION_PREFIX, "password");
    String DOC = "JDBC connection password.";
    String DISPLAY = "JDBC Password";
  }

  public interface ConnectionAttempts {
    String CONFIG = String.join(".", CONNECTION_PREFIX, "attempts");
    String DOC = "Maximum number of attempts to retrieve a valid JDBC connection."
        + " Must be a positive integer.";
    String DISPLAY = "JDBC connection attempts";
    short DEFAULT = 3;
  }

  public interface ConnectionBackoffMs {
    String CONFIG = String.join(".", CONNECTION_PREFIX, "backoff_ms");
    String DOC = "Backoff time in milliseconds between connection attempts.";
    String DISPLAY = "JDBC connection backoff in milliseconds";
    int DEFAULT = 10000;
  }

  public static ConfigDef newConfigDef() {
    int orderInGroup = 0;
    return HdfsSinkConnectorConfig
        .newConfigDef()
        // Define Database configuration group
        .define(
            ConnectionUrl.CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            ConnectionUrl.DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            ConnectionUrl.DISPLAY
        )
        .define(
            ConnectionUser.CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            ConnectionUser.DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.MEDIUM,
            ConnectionUser.DISPLAY
        )
        .define(
            ConnectionPassword.CONFIG,
            Type.PASSWORD,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            ConnectionPassword.DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.SHORT,
            ConnectionPassword.DISPLAY
        )
        .define(
            ConnectionAttempts.CONFIG,
            Type.SHORT,
            ConnectionAttempts.DEFAULT,
            ConfigDef.Range.atLeast(1),
            Importance.LOW,
            ConnectionAttempts.DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.SHORT,
            ConnectionAttempts.DISPLAY
        )
        .define(
            ConnectionBackoffMs.CONFIG,
            Type.LONG,
            ConnectionBackoffMs.DEFAULT,
            ConfigDef.Range.atLeast(100),
            Importance.LOW,
            ConnectionBackoffMs.DOC,
            DATABASE_GROUP,
            ++orderInGroup,
            Width.LONG,
            ConnectionBackoffMs.DISPLAY
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
    return getStringRequired(ConnectionUrl.CONFIG, String::trim);
  }

  public String getConnectionUser() {
    return getStringRequired(ConnectionUser.CONFIG, String::trim);
  }

  public Password getConnectionPassword() {
    return Optional
        .ofNullable(getPassword(ConnectionPassword.CONFIG))
        .orElseThrow(() -> new ConfigException(
            "Missing Password value for [" + ConnectionPassword.CONFIG + "]"
        ));
  }

  public short getConnectionAttempts() {
    return getShort(ConnectionAttempts.CONFIG);
  }

  public Duration getConnectionBackoff() {
    return Duration.ofMillis(getLong(ConnectionBackoffMs.CONFIG));
  }

  public Properties getConnectionProperties() {
    Properties properties = new Properties();
    properties.setProperty("user", getConnectionUser());
    properties.setProperty("password", getConnectionPassword().value());
    values()
        .entrySet()
        .stream()
        .filter(entry -> entry.getValue() != null)
        .filter(entry -> entry.getKey().startsWith(CONNECTION_PREFIX + "."))
        .filter(entry -> ConnectionUrl.CONFIG.equals(entry.getKey()))
        .filter(entry -> ConnectionUser.CONFIG.equals(entry.getKey()))
        .filter(entry -> ConnectionPassword.CONFIG.equals(entry.getKey()))
        .filter(entry -> ConnectionAttempts.CONFIG.equals(entry.getKey()))
        .forEach(entry -> properties.setProperty(
            entry.getKey().substring(CONNECTION_PREFIX.length() + 1),
            entry.getValue().toString()
        ));
    return properties;
  }

  private String getStringRequired(String key, Function<String, String> transformFn) {
    return Optional
        .ofNullable(getString(key))
        .map(transformFn)
        .filter(((Predicate<String>) String::isEmpty).negate())
        .orElseThrow(() -> new ConfigException("Missing or empty String value for [" + key + "]"));
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }
}

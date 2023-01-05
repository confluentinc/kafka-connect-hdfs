package io.confluent.connect.hdfs.jdbc;

import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class JdbcHdfsSinkConnectorConfigTest {
  @Test
  public void testGetIncludedFieldsLower_Trim() {
    Map<String, String> configMap = makeConfigMap();
    configMap.put(
        JdbcHdfsSinkConnectorConfig.COLUMN_INCLUDE_LIST_CONFIG,
        "fOo 1 A. Bar\n\t. 0baz\n _A_\r ,A. B.c \r,1-.\r\n\t2.3\r 4,\n,\r ,"
    );

    JdbcHdfsSinkConnectorConfig connectorConfig = new JdbcHdfsSinkConnectorConfig(configMap);
    Map<JdbcTableInfo, Set<String>> includedFieldsLowerMap = connectorConfig.getIncludedFieldsLower();

    assertEquals(3, includedFieldsLowerMap.size());

    assertEquals(
        Collections.singleton("0baz\n _a_"),
        includedFieldsLowerMap.get(new JdbcTableInfo("foo 1 a", "bar")));

    assertEquals(
        Collections.singleton("c"),
        includedFieldsLowerMap.get(new JdbcTableInfo("a", "b")));

    assertEquals(
        Collections.singleton("3\r 4"),
        includedFieldsLowerMap.get(new JdbcTableInfo("1-", "2")));
  }

  @Test
  public void testGetIncludedFieldsLower_Duplicates() {
    Map<String, String> configMap = makeConfigMap();
    configMap.put(
        JdbcHdfsSinkConnectorConfig.COLUMN_INCLUDE_LIST_CONFIG,
        "a.b.c,a.b.d,a.d.a,a.b.d,a.b.a,a.d.a"
    );

    JdbcHdfsSinkConnectorConfig connectorConfig = new JdbcHdfsSinkConnectorConfig(configMap);
    Map<JdbcTableInfo, Set<String>> includedFieldsLowerMap = connectorConfig.getIncludedFieldsLower();

    assertEquals(2, includedFieldsLowerMap.size());

    assertEquals(
        new HashSet<>(Arrays.asList("a", "c", "d")),
        includedFieldsLowerMap.get(new JdbcTableInfo("a", "b")));

    assertEquals(
        Collections.singleton("a"),
        includedFieldsLowerMap.get(new JdbcTableInfo("a", "d")));
  }

  @Test(expected = ConfigException.class)
  public void testGetIncludedFieldsLower_NullSchema() {
    Map<String, String> configMap = makeConfigMap();
    configMap.put(
        JdbcHdfsSinkConnectorConfig.COLUMN_INCLUDE_LIST_CONFIG,
        "\n \r.b.c"
    );

    JdbcHdfsSinkConnectorConfig connectorConfig = new JdbcHdfsSinkConnectorConfig(configMap);
    connectorConfig.getIncludedFieldsLower();
  }

  @Test(expected = ConfigException.class)
  public void testGetIncludedFieldsLower_NullTable() {
    Map<String, String> configMap = makeConfigMap();
    configMap.put(
        JdbcHdfsSinkConnectorConfig.COLUMN_INCLUDE_LIST_CONFIG,
        "a..c"
    );

    JdbcHdfsSinkConnectorConfig connectorConfig = new JdbcHdfsSinkConnectorConfig(configMap);
    connectorConfig.getIncludedFieldsLower();
  }

  @Test(expected = ConfigException.class)
  public void testGetIncludedFieldsLower_NullColumn() {
    Map<String, String> configMap = makeConfigMap();
    configMap.put(
        JdbcHdfsSinkConnectorConfig.COLUMN_INCLUDE_LIST_CONFIG,
        "a.b."
    );

    JdbcHdfsSinkConnectorConfig connectorConfig = new JdbcHdfsSinkConnectorConfig(configMap);
    connectorConfig.getIncludedFieldsLower();
  }

  @Test(expected = ConfigException.class)
  public void testGetIncludedFieldsLower_Empty() {
    Map<String, String> configMap = makeConfigMap();
    configMap.put(
        JdbcHdfsSinkConnectorConfig.COLUMN_INCLUDE_LIST_CONFIG,
        ""
    );

    JdbcHdfsSinkConnectorConfig connectorConfig = new JdbcHdfsSinkConnectorConfig(configMap);
    connectorConfig.getIncludedFieldsLower();
  }

  private Map<String, String> makeConfigMap() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        StorageCommonConfig.STORE_URL_CONFIG,
        "url"
    );
    configMap.put(
        StorageSinkConnectorConfig.FLUSH_SIZE_CONFIG,
        "100"
    );
    configMap.put(
        JdbcHdfsSinkConnectorConfig.CONNECTION_URL_CONFIG,
        "url"
    );
    configMap.put(
        JdbcHdfsSinkConnectorConfig.CONNECTION_USER_CONFIG,
        "user"
    );
    configMap.put(
        JdbcHdfsSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG,
        "pass"
    );
    return configMap;
  }
}

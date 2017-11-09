/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.confluent.connect.hdfs.avro.AvroFormat;
import io.confluent.connect.hdfs.json.JsonFormat;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

import static io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_CONFIG;
import static io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_DISPLAY;
import static io.confluent.connect.storage.common.StorageCommonConfig.STORAGE_CLASS_DOC;

public class HdfsSinkConnectorConfig extends StorageSinkConnectorConfig {

  // HDFS Group
  // This config is deprecated and will be removed in future releases. Use store.url instead.
  public static final String HDFS_URL_CONFIG = "hdfs.url";
  public static final String HDFS_URL_DOC =
      "The HDFS connection URL. This configuration has the format of hdfs:://hostname:port and "
          + "specifies the HDFS to export data to. This property is deprecated and will be "
          + "removed in future releases. Use ``store.url`` instead.";
  public static final String HDFS_URL_DEFAULT = null;
  public static final String HDFS_URL_DISPLAY = "HDFS URL";

  public static final String HADOOP_CONF_DIR_CONFIG = "hadoop.conf.dir";
  public static final String HADOOP_CONF_DIR_DEFAULT = "";
  private static final String HADOOP_CONF_DIR_DOC = "The Hadoop configuration directory.";
  private static final String HADOOP_CONF_DIR_DISPLAY = "Hadoop Configuration Directory";

  public static final String HADOOP_HOME_CONFIG = "hadoop.home";
  public static final String HADOOP_HOME_DEFAULT = "";
  private static final String HADOOP_HOME_DOC = "The Hadoop home directory.";
  private static final String HADOOP_HOME_DISPLAY = "Hadoop home directory";

  public static final String LOGS_DIR_CONFIG = "logs.dir";
  public static final String LOGS_DIR_DOC =
      "Top level directory to store the write ahead logs.";
  public static final String LOGS_DIR_DEFAULT = "logs";
  public static final String LOGS_DIR_DISPLAY = "Logs directory";

  public static final String AVRO_CODEC_CONFIG = "avro.codec";
  public static final String AVRO_CODEC_DEFAULT = "null";
  public static final String AVRO_CODEC_DISPLAY = "Avro Compression Codec";
  public static final String AVRO_CODEC_DOC = "The Avro compression codec to be used for "
      + "output files. Available values: null, deflate, "
      + "snappy and bzip2 (CodecSource is org.apache.avro.file.CodecFactory)";

  // Security group
  public static final String HDFS_AUTHENTICATION_KERBEROS_CONFIG = "hdfs.authentication.kerberos";
  private static final String HDFS_AUTHENTICATION_KERBEROS_DOC =
      "Configuration indicating whether HDFS is using Kerberos for authentication.";
  private static final boolean HDFS_AUTHENTICATION_KERBEROS_DEFAULT = false;
  private static final String HDFS_AUTHENTICATION_KERBEROS_DISPLAY = "HDFS Authentication Kerberos";

  public static final String CONNECT_HDFS_PRINCIPAL_CONFIG = "connect.hdfs.principal";
  public static final String CONNECT_HDFS_PRINCIPAL_DEFAULT = "";
  private static final String CONNECT_HDFS_PRINCIPAL_DOC =
      "The principal to use when HDFS is using Kerberos to for authentication.";
  private static final String CONNECT_HDFS_PRINCIPAL_DISPLAY = "Connect Kerberos Principal";

  public static final String CONNECT_HDFS_KEYTAB_CONFIG = "connect.hdfs.keytab";
  public static final String CONNECT_HDFS_KEYTAB_DEFAULT = "";
  private static final String CONNECT_HDFS_KEYTAB_DOC =
      "The path to the keytab file for the HDFS connector principal. "
          + "This keytab file should only be readable by the connector user.";
  private static final String CONNECT_HDFS_KEYTAB_DISPLAY = "Connect Kerberos Keytab";

  public static final String HDFS_NAMENODE_PRINCIPAL_CONFIG = "hdfs.namenode.principal";
  public static final String HDFS_NAMENODE_PRINCIPAL_DEFAULT = "";
  private static final String HDFS_NAMENODE_PRINCIPAL_DOC = "The principal for HDFS Namenode.";
  private static final String HDFS_NAMENODE_PRINCIPAL_DISPLAY = "HDFS NameNode Kerberos Principal";

  public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG =
      "kerberos.ticket.renew.period.ms";
  public static final long KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT = 60000 * 60;
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DOC =
      "The period in milliseconds to renew the Kerberos ticket.";
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY = "Kerberos Ticket Renew "
      + "Period (ms)";

  private static final ConfigDef.Recommender hdfsAuthenticationKerberosDependentsRecommender =
      new BooleanParentRecommender(
          HDFS_AUTHENTICATION_KERBEROS_CONFIG);

  private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender FORMAT_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();

  static {
    STORAGE_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(HdfsStorage.class)
    );

    FORMAT_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(AvroFormat.class, JsonFormat.class)
    );

    PARTITIONER_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            DefaultPartitioner.class,
            HourlyPartitioner.class,
            DailyPartitioner.class,
            TimeBasedPartitioner.class,
            FieldPartitioner.class
        )
    );
  }

  public static ConfigDef newConfigDef() {
    ConfigDef configDef = new ConfigDef();
    // Define HDFS configuration group
    {
      final String group = "HDFS";
      int orderInGroup = 0;

      // HDFS_URL_CONFIG property is retained for backwards compatibility with HDFS connector and
      // will be removed in future versions.
      configDef.define(
          HDFS_URL_CONFIG,
          Type.STRING,
          HDFS_URL_DEFAULT,
          Importance.HIGH,
          HDFS_URL_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          HDFS_URL_DISPLAY
      );

      configDef.define(
          HADOOP_CONF_DIR_CONFIG,
          Type.STRING,
          HADOOP_CONF_DIR_DEFAULT,
          Importance.HIGH,
          HADOOP_CONF_DIR_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          HADOOP_CONF_DIR_DISPLAY
      );

      configDef.define(
          HADOOP_HOME_CONFIG,
          Type.STRING,
          HADOOP_HOME_DEFAULT,
          Importance.HIGH,
          HADOOP_HOME_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          HADOOP_HOME_DISPLAY
      );

      configDef.define(
          LOGS_DIR_CONFIG,
          Type.STRING,
          LOGS_DIR_DEFAULT,
          Importance.HIGH,
          LOGS_DIR_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          LOGS_DIR_DISPLAY
      );

      // define avro codec
      configDef.define(AVRO_CODEC_CONFIG,
          Type.STRING,
          AVRO_CODEC_DEFAULT,
          Importance.LOW,
          AVRO_CODEC_DOC,
          group, ++orderInGroup, Width.LONG,
          AVRO_CODEC_DISPLAY
      );
    }

    {
      final String group = "Security";
      int orderInGroup = 0;
      // Define Security configuration group
      configDef.define(
          HDFS_AUTHENTICATION_KERBEROS_CONFIG,
          Type.BOOLEAN,
          HDFS_AUTHENTICATION_KERBEROS_DEFAULT,
          Importance.HIGH,
          HDFS_AUTHENTICATION_KERBEROS_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          HDFS_AUTHENTICATION_KERBEROS_DISPLAY,
          Arrays.asList(
              CONNECT_HDFS_PRINCIPAL_CONFIG,
              CONNECT_HDFS_KEYTAB_CONFIG,
              HDFS_NAMENODE_PRINCIPAL_CONFIG,
              KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG
          )
      );

      configDef.define(
          CONNECT_HDFS_PRINCIPAL_CONFIG,
          Type.STRING,
          CONNECT_HDFS_PRINCIPAL_DEFAULT,
          Importance.HIGH,
          CONNECT_HDFS_PRINCIPAL_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          CONNECT_HDFS_PRINCIPAL_DISPLAY,
          hdfsAuthenticationKerberosDependentsRecommender
      );

      configDef.define(
          CONNECT_HDFS_KEYTAB_CONFIG,
          Type.STRING,
          CONNECT_HDFS_KEYTAB_DEFAULT,
          Importance.HIGH,
          CONNECT_HDFS_KEYTAB_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          CONNECT_HDFS_KEYTAB_DISPLAY,
          hdfsAuthenticationKerberosDependentsRecommender
      );

      configDef.define(
          HDFS_NAMENODE_PRINCIPAL_CONFIG,
          Type.STRING,
          HDFS_NAMENODE_PRINCIPAL_DEFAULT,
          Importance.HIGH,
          HDFS_NAMENODE_PRINCIPAL_DOC,
          group,
          ++orderInGroup,
          Width.MEDIUM,
          HDFS_NAMENODE_PRINCIPAL_DISPLAY,
          hdfsAuthenticationKerberosDependentsRecommender
      );

      configDef.define(
          KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG,
          Type.LONG,
          KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT,
          Importance.LOW,
          KERBEROS_TICKET_RENEW_PERIOD_MS_DOC,
          group,
          ++orderInGroup,
          Width.SHORT,
          KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY,
          hdfsAuthenticationKerberosDependentsRecommender
      );
    }
    // Put the storage group(s) last ...
    ConfigDef storageConfigDef = StorageSinkConnectorConfig.newConfigDef(FORMAT_CLASS_RECOMMENDER);
    for (ConfigDef.ConfigKey key : storageConfigDef.configKeys().values()) {
      configDef.define(key);
    }
    return configDef;
  }

  private final String name;
  private final StorageCommonConfig commonConfig;
  private final HiveConfig hiveConfig;
  private final PartitionerConfig partitionerConfig;
  private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();
  private Configuration hadoopConfig;

  public HdfsSinkConnectorConfig(Map<String, String> props) {
    this(newConfigDef() , addDefaults(props));
  }

  protected HdfsSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
    ConfigDef storageCommonConfigDef = StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER);
    commonConfig = new StorageCommonConfig(storageCommonConfigDef, originalsStrings());
    hiveConfig = new HiveConfig(originalsStrings());
    ConfigDef partitionerConfigDef = PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER);
    partitionerConfig = new PartitionerConfig(partitionerConfigDef, originalsStrings());
    this.name = parseName(originalsStrings());
    this.hadoopConfig = new Configuration();
    addToGlobal(hiveConfig);
    addToGlobal(partitionerConfig);
    addToGlobal(commonConfig);
    addToGlobal(this);
  }

  public static Map<String, String> addDefaults(Map<String, String> props) {
    ConcurrentMap<String, String> propsCopy = new ConcurrentHashMap<>(props);
    propsCopy.putIfAbsent(STORAGE_CLASS_CONFIG, HdfsStorage.class.getName());
    propsCopy.putIfAbsent(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    return propsCopy;
  }

  protected static String parseName(Map<String, String> props) {
    String nameProp = props.get("name");
    return nameProp != null ? nameProp : "HDFS-sink";
  }

  private void addToGlobal(AbstractConfig config) {
    allConfigs.add(config);
    addConfig(config.values(), (ComposableConfig) config);
  }

  private void addConfig(Map<String, ?> parsedProps, ComposableConfig config) {
    for (String key : parsedProps.keySet()) {
      propertyToConfig.put(key, config);
    }
  }

  public String getName() {
    return name;
  }

  @Override
  public Object get(String key) {
    ComposableConfig config = propertyToConfig.get(key);
    if (config == null) {
      throw new ConfigException(String.format("Unknown configuration '%s'", key));
    }
    return config == this ? super.get(key) : config.get(key);
  }

  public Configuration getHadoopConfiguration() {
    return hadoopConfig;
  }

  public Map<String, ?> plainValues() {
    Map<String, Object> map = new HashMap<>();
    for (AbstractConfig config : allConfigs) {
      map.putAll(config.values());
    }
    return map;
  }

  public String getAvroCodec() {
    return getString(AVRO_CODEC_CONFIG);
  }

  private static class BooleanParentRecommender implements ConfigDef.Recommender {

    protected String parentConfigName;

    public BooleanParentRecommender(String parentConfigName) {
      this.parentConfigName = parentConfigName;
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return (Boolean) connectorConfigs.get(parentConfigName);
    }
  }

  public static ConfigDef getConfig() {
    // Define the names of the configurations we're going to override
    Set<String> skip = new HashSet<>();
    skip.add(STORAGE_CLASS_CONFIG);
    skip.add(FORMAT_CLASS_CONFIG);

    // Order added is important, so that group order is maintained
    ConfigDef visible = new ConfigDef();
    addAllConfigKeys(visible, newConfigDef(), skip);
    addAllConfigKeys(visible, StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER), skip);
    addAllConfigKeys(visible, PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER), skip);
    addAllConfigKeys(visible, HiveConfig.getConfig(), skip);

    // Add the overridden configurations
    visible.define(
        STORAGE_CLASS_CONFIG,
        Type.CLASS,
        HdfsStorage.class.getName(),
        Importance.HIGH,
        STORAGE_CLASS_DOC,
        "Storage",
        1,
        Width.NONE,
        STORAGE_CLASS_DISPLAY,
        STORAGE_CLASS_RECOMMENDER
    );

    visible.define(
        FORMAT_CLASS_CONFIG,
        Type.CLASS,
        AvroFormat.class.getName(),
        Importance.HIGH,
        FORMAT_CLASS_DOC,
        "Connector",
        1,
        Width.NONE,
        FORMAT_CLASS_DISPLAY,
        FORMAT_CLASS_RECOMMENDER
    );

    return visible;
  }

  private static void addAllConfigKeys(ConfigDef container, ConfigDef other, Set<String> skip) {
    for (ConfigDef.ConfigKey key : other.configKeys().values()) {
      if (skip != null && !skip.contains(key.name)) {
        container.define(key);
      }
    }
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }

}

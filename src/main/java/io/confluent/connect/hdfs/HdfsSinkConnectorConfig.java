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

package io.confluent.connect.hdfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.commons.lang.StringUtils;
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
import io.confluent.connect.storage.common.ParentValueRecommender;
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
import static io.confluent.connect.storage.common.StorageCommonConfig.TOPICS_DIR_CONFIG;
import static io.confluent.connect.storage.common.StorageCommonConfig.TOPICS_DIR_DEFAULT;
import static io.confluent.connect.storage.common.StorageCommonConfig.TOPICS_DIR_DISPLAY;
import static io.confluent.connect.storage.common.StorageCommonConfig.TOPICS_DIR_DOC;

public class HdfsSinkConnectorConfig extends StorageSinkConnectorConfig {

  // HDFS Group
  // This config is deprecated and will be removed in future releases. Use store.url instead.
  public static final String HDFS_URL_CONFIG = "hdfs.url";
  public static final String HDFS_URL_DOC =
      "The HDFS connection URL. This configuration has the format of hdfs://hostname:port and "
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

  // Storage group
  public static final String TOPIC_REGEX_CAPTURE_GROUP_CONFIG = "topic.regex.capture.group";
  public static final String TOPIC_REGEX_CAPTURE_GROUP_DISPLAY = "Topic Regex Capture Group";
  public static final String TOPIC_REGEX_CAPTURE_GROUP_DOC = "A regex that matches the entire "
      + "topic and  specifies what groups to capture in the topic, so when specifying `${1}` in "
      + "`topics.dir`, `${1}` will refer to the first captured group. Example config value of "
      + "`([a-zA-Z]*)_([a-zA-Z]*)` match a topic that is two words delimited by a - and will "
      + "capture both words as separate groupds, so for `topic.dir = ${1}/${2}` and "
      + "`topic = topic_name` the corresponding `topic.dir` will be `topic/name/`. By default, "
      + "this functionality is not enabled.";
  public static final String TOPIC_REGEX_CAPTURE_GROUP_DEFAULT = null;
  
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
  private static final ParentValueRecommender AVRO_COMPRESSION_RECOMMENDER
      = new ParentValueRecommender(FORMAT_CLASS_CONFIG, AvroFormat.class, AVRO_SUPPORTED_CODECS);

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
    ConfigDef storageConfigDef = StorageSinkConnectorConfig.newConfigDef(
        FORMAT_CLASS_RECOMMENDER,
        AVRO_COMPRESSION_RECOMMENDER);

    int lastOrder = 0;
    String group = "Storage";
    for (ConfigDef.ConfigKey key : storageConfigDef.configKeys().values()) {
      configDef.define(key);
      group = key.group;
      lastOrder = key.orderInGroup;
    }

    // add the topic.regex.capture.group config to storage
    configDef
        .define(
            TOPIC_REGEX_CAPTURE_GROUP_CONFIG,
            Type.STRING,
            TOPIC_REGEX_CAPTURE_GROUP_DEFAULT,
            Importance.LOW,
            TOPIC_REGEX_CAPTURE_GROUP_DOC,
            group,
            ++lastOrder,
            Width.LONG,
            TOPIC_REGEX_CAPTURE_GROUP_DISPLAY
    );

    return configDef;
  }

  private final String name;
  private final String url;
  private final StorageCommonConfig commonConfig;
  private final HiveConfig hiveConfig;
  private final PartitionerConfig partitionerConfig;
  private final Pattern topixRegexCaptureGroup;
  private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();
  private Configuration hadoopConfig;
  private List<Integer> partsToReplace;

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
    this.url = extractUrl();
    try {
      this.topixRegexCaptureGroup = getString(TOPIC_REGEX_CAPTURE_GROUP_CONFIG) != null
          ? Pattern.compile(getString(TOPIC_REGEX_CAPTURE_GROUP_CONFIG))
          : null;
    } catch (PatternSyntaxException e) {
      throw new ConfigException(
          TOPIC_REGEX_CAPTURE_GROUP_CONFIG + " is an invalid regex pattern: ",
          e
      );
    }

    partsToReplace = getPartsToReplace();
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

  /**
   * Returns the url property. Preference is given to property <code>store.url</code> over
   * <code>hdfs.url</code> because <code>hdfs.url</code> is deprecated.
   *
   * @return String url for HDFS
   */
  private String extractUrl() {
    String storageUrl = getString(StorageCommonConfig.STORE_URL_CONFIG);
    if (StringUtils.isNotBlank(storageUrl)) {
      return storageUrl;
    }

    String hdfsUrl = getString(HDFS_URL_CONFIG);
    if (StringUtils.isNotBlank(hdfsUrl)) {
      return hdfsUrl;
    }

    throw new ConfigException(
        String.format("Configuration %s cannot be empty.", StorageCommonConfig.STORE_URL_CONFIG)
    );
  }

  public String getName() {
    return name;
  }

  public String getUrl() {
    return url;
  }

  /**
   * Performs all substitutions and calculates the final topic directory for a topic
   *
   * @param topic - String - the topic whose directory to find
   * @return String - the directory name and path
   */
  public String getTopicDirFromTopic(String topic) {
    String topicsDir = getString(TOPICS_DIR_CONFIG);
    topicsDir = topicsDir.replace("${topic}", topic);

    // only if configured
    if (topixRegexCaptureGroup != null) {

      // find all of the captured groups by the regex
      Matcher matcher = topixRegexCaptureGroup.matcher(topic);
      if (!matcher.matches()) {
        throw new ConfigException(
            String.format(
                "Configuration %s with value %s does not fully match the specified regex %s in %s",
                TOPICS_DIR_CONFIG,
                topicsDir,
                topixRegexCaptureGroup.pattern(),
                TOPIC_REGEX_CAPTURE_GROUP_CONFIG
            )
        );
      }

      // make sure that all references to captured groups actually exist
      if (!partsToReplace.isEmpty()) {
        int largestVar = partsToReplace.get(partsToReplace.size() - 1);
        if (largestVar > matcher.groupCount()) {
          throw new ConfigException(
              String.format(
                  "Topic %s must have at least %d capture groups using regex pattern %s, "
                      + "but actually had %d capture groups.",
                  topic,
                  largestVar,
                  topixRegexCaptureGroup.pattern(),
                  matcher.groupCount()
              )
          );
        }
      }

      for (int i = 1; i < matcher.groupCount() + 1; i++) {
        topicsDir = topicsDir.replace("${" + i + "}", matcher.group(i));
      }
    }

    return topicsDir;
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
    // Include any additional properties not defined by the ConfigDef
    // that custom partitioners might need
    Map<String, ?> originals = originals();
    for (String originalKey : originals.keySet()) {
      if (!map.containsKey(originalKey)) {
        map.put(originalKey, originals.get(originalKey));
      }
    }

    return map;
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
    skip.add(TOPICS_DIR_CONFIG);

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

    visible.define(
        TOPICS_DIR_CONFIG,
        Type.STRING,
        TOPICS_DIR_DEFAULT,
        Importance.HIGH,
        TOPICS_DIR_DOC + " Supports ``${topic}`` in the value, which will be "
            + "replaced by the actual topic name. Supports ``${1}``, ..., ``${n}`` in "
            + "conjunction with " + TOPIC_REGEX_CAPTURE_GROUP_CONFIG + ". See "
            + TOPIC_REGEX_CAPTURE_GROUP_CONFIG + " configuration documentation for details.",
        "Storage",
        2,
        Width.NONE,
        TOPICS_DIR_DISPLAY
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

  /**
   * Finds all instances of `${[0-9]+}` in `topics.dir` and returns a list.
   * @return list of all numbers found in the regex pattern
   */
  private List<Integer> getPartsToReplace() {

    List<Integer> toReplace = new ArrayList<>();
    Pattern pattern = Pattern.compile("\\$\\{\\d+}");
    Matcher partsMatcher = pattern.matcher(getString(TOPICS_DIR_CONFIG));

    while (partsMatcher.find()) {
      String part = partsMatcher.group();
      if (!part.isEmpty()) {
        part = part.substring(2, part.length() - 1); // trim "${" and "}"
        toReplace.add(Integer.valueOf(part));
      }
    }

    Collections.sort(toReplace);

    return toReplace;
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }

}

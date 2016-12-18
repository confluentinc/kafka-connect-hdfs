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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.confluent.connect.storage.StorageSinkConnectorConfig;

public class HdfsSinkConnectorConfig extends StorageSinkConnectorConfig {

  // HDFS Group
  public static final String HADOOP_CONF_DIR_CONFIG = "hadoop.conf.dir";
  private static final String HADOOP_CONF_DIR_DOC =
      "The Hadoop configuration directory.";
  public static final String HADOOP_CONF_DIR_DEFAULT = "";
  private static final String HADOOP_CONF_DIR_DISPLAY = "Hadoop Configuration Directory";

  public static final String HADOOP_HOME_CONFIG = "hadoop.home";
  private static final String HADOOP_HOME_DOC =
      "The Hadoop home directory.";
  public static final String HADOOP_HOME_DEFAULT = "";
  private static final String HADOOP_HOME_DISPLAY = "Hadoop home directory";

  // Security group
  public static final String HDFS_AUTHENTICATION_KERBEROS_CONFIG = "hdfs.authentication.kerberos";
  private static final String HDFS_AUTHENTICATION_KERBEROS_DOC =
      "Configuration indicating whether HDFS is using Kerberos for authentication.";
  private static final boolean HDFS_AUTHENTICATION_KERBEROS_DEFAULT = false;
  private static final String HDFS_AUTHENTICATION_KERBEROS_DISPLAY = "HDFS Authentication Kerberos";

  public static final String CONNECT_HDFS_PRINCIPAL_CONFIG = "connect.hdfs.principal";
  private static final String CONNECT_HDFS_PRINCIPAL_DOC =
      "The principal to use when HDFS is using Kerberos to for authentication.";
  public static final String CONNECT_HDFS_PRINCIPAL_DEFAULT = "";
  private static final String CONNECT_HDFS_PRINCIPAL_DISPLAY = "Connect Kerberos Principal";

  public static final String CONNECT_HDFS_KEYTAB_CONFIG = "connect.hdfs.keytab";
  private static final String CONNECT_HDFS_KEYTAB_DOC =
      "The path to the keytab file for the HDFS connector principal. "
      + "This keytab file should only be readable by the connector user.";
  public static final String CONNECT_HDFS_KEYTAB_DEFAULT = "";
  private static final String CONNECT_HDFS_KEYTAB_DISPLAY = "Connect Kerberos Keytab";

  public static final String HDFS_NAMENODE_PRINCIPAL_CONFIG = "hdfs.namenode.principal";
  private static final String HDFS_NAMENODE_PRINCIPAL_DOC = "The principal for HDFS Namenode.";
  public static final String HDFS_NAMENODE_PRINCIPAL_DEFAULT = "";
  private static final String HDFS_NAMENODE_PRINCIPAL_DISPLAY = "HDFS NameNode Kerberos Principal";

  public static final String KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG = "kerberos.ticket.renew.period.ms";
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DOC =
      "The period in milliseconds to renew the Kerberos ticket.";
  public static final long KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT = 60000 * 60;
  private static final String KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY = "Kerberos Ticket Renew Period (ms)";

  public static final String HDFS_GROUP = "HDFS";
  public static final String SECURITY_GROUP = "Security";

  private static final ConfigDef.Recommender hdfsAuthenticationKerberosDependentsRecommender = new BooleanParentRecommender(HDFS_AUTHENTICATION_KERBEROS_CONFIG);

  static {
    // Define HDFS configuration group
    CONFIG_DEF.define(HADOOP_CONF_DIR_CONFIG, Type.STRING, HADOOP_CONF_DIR_DEFAULT, Importance.HIGH, HADOOP_CONF_DIR_DOC, HDFS_GROUP, 2, Width.MEDIUM, HADOOP_CONF_DIR_DISPLAY)
        .define(HADOOP_HOME_CONFIG, Type.STRING, HADOOP_HOME_DEFAULT, Importance.HIGH, HADOOP_HOME_DOC, HDFS_GROUP, 3, Width.SHORT, HADOOP_HOME_DISPLAY);

    // Define Security configuration group
    CONFIG_DEF.define(HDFS_AUTHENTICATION_KERBEROS_CONFIG, Type.BOOLEAN, HDFS_AUTHENTICATION_KERBEROS_DEFAULT, Importance.HIGH, HDFS_AUTHENTICATION_KERBEROS_DOC,
                  SECURITY_GROUP, 1, Width.SHORT, HDFS_AUTHENTICATION_KERBEROS_DISPLAY,
                  Arrays.asList(CONNECT_HDFS_PRINCIPAL_CONFIG, CONNECT_HDFS_KEYTAB_CONFIG, HDFS_NAMENODE_PRINCIPAL_CONFIG, KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG))
        .define(CONNECT_HDFS_PRINCIPAL_CONFIG, Type.STRING, CONNECT_HDFS_PRINCIPAL_DEFAULT, Importance.HIGH, CONNECT_HDFS_PRINCIPAL_DOC,
                SECURITY_GROUP, 2, Width.MEDIUM, CONNECT_HDFS_PRINCIPAL_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender)
        .define(CONNECT_HDFS_KEYTAB_CONFIG, Type.STRING, CONNECT_HDFS_KEYTAB_DEFAULT, Importance.HIGH, CONNECT_HDFS_KEYTAB_DOC,
                SECURITY_GROUP, 3, Width.MEDIUM, CONNECT_HDFS_KEYTAB_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender)
        .define(HDFS_NAMENODE_PRINCIPAL_CONFIG, Type.STRING, HDFS_NAMENODE_PRINCIPAL_DEFAULT, Importance.HIGH, HDFS_NAMENODE_PRINCIPAL_DOC,
                SECURITY_GROUP, 4, Width.MEDIUM, HDFS_NAMENODE_PRINCIPAL_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender)
        .define(KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG, Type.LONG, KERBEROS_TICKET_RENEW_PERIOD_MS_DEFAULT, Importance.LOW, KERBEROS_TICKET_RENEW_PERIOD_MS_DOC,
                SECURITY_GROUP, 5, Width.SHORT, KERBEROS_TICKET_RENEW_PERIOD_MS_DISPLAY, hdfsAuthenticationKerberosDependentsRecommender);
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
    return CONFIG_DEF;
  }

  public HdfsSinkConnectorConfig(Map<String, String> props) {
    super(props);
  }
}

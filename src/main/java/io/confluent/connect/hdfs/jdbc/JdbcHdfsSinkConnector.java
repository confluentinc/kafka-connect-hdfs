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

import io.confluent.connect.hdfs.HdfsSinkConnector;
import io.confluent.connect.hdfs.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JdbcHdfsSinkConnector extends SinkConnector {
  private Map<String, String> configProperties;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    configProperties = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return JdbcHdfsSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return IntStream
        .range(0, maxTasks)
        .mapToObj(i -> {
          Map<String, String> taskProps = new HashMap<>(configProperties);
          taskProps.put(HdfsSinkConnector.TASK_ID_CONFIG_NAME, Integer.toString(i));
          return taskProps;
        })
        .collect(Collectors.toList());
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return JdbcHdfsSinkConnectorConfig.getConfig();
  }
}

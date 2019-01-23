/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.hive;

import org.apache.kafka.connect.data.Schema;

public class HiveTableNamingParameters {
  private String topicName;
  private Schema schema;

  public HiveTableNamingParameters(String topicName, Schema schema) {
    this.topicName = topicName;
    this.schema = schema;
  }

  public String getTopicName() {
    return topicName;
  }

  public Schema getSchema() {
    return schema;
  }
}

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

package io.confluent.connect.hdfs.avro;

import io.confluent.connect.hdfs.hive.HiveUtilTestBase;
import io.confluent.connect.hdfs.hive.HiveUtil;

public class AvroHiveUtilTest extends HiveUtilTestBase {

  public AvroHiveUtilTest(String hiveTableNameConfig) {
    super(hiveTableNameConfig);
  }

  @Override
  protected HiveUtil createHiveUtil() {
    return new AvroHiveUtil(connectorConfig, avroData, hiveMetaStore);
  }
}

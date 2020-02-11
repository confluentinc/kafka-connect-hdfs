/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.connect.hdfs.orc;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveUtil;
import org.apache.kafka.common.config.AbstractConfig;

public class OrcHiveFactory implements HiveFactory {

  @Override
  public HiveUtil createHiveUtil(AbstractConfig conf, HiveMetaStore hiveMetaStore) {
    return new OrcHiveUtil((HdfsSinkConnectorConfig) conf, hiveMetaStore);
  }
}

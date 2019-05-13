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

package io.confluent.connect.hdfs.hive;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.HiveMetaStoreException;

import org.apache.hadoop.conf.Configuration;

@Deprecated
@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_SUPERCLASS")
public class HiveMetaStore extends io.confluent.connect.storage.hive.HiveMetaStore {

  public HiveMetaStore(
      Configuration conf,
      HdfsSinkConnectorConfig connectorConfig
  ) throws HiveMetaStoreException {
    super(conf, connectorConfig);
  }

}

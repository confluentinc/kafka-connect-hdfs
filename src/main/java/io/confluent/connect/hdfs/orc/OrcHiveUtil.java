/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.hdfs.orc;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.HiveMetaStoreException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

public class OrcHiveUtil extends HiveUtil {
  private final String topicsDir;

  public OrcHiveUtil(HdfsSinkConnectorConfig conf, HiveMetaStore hiveMetaStore) {
    super(conf, hiveMetaStore);
    this.topicsDir = conf.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
  }

  @Override
  public void alterSchema(String dbName, String tableName, Schema schema) {

  }

  @Override
  public void createTable(String database,
                          String tableName,
                          Schema schema,
                          Partitioner partitioner) throws HiveMetaStoreException {
    Table table = constructOrcTable(database, tableName, schema, partitioner);
    hiveMetaStore.createTable(table);
  }

  private Table constructOrcTable(String database,
                                  String tableName,
                                  Schema schema,
                                  Partitioner partitioner) throws HiveMetaStoreException {
    Table table = newTable(database, tableName);
    table.setTableType(TableType.EXTERNAL_TABLE);
    table.getParameters().put("EXTERNAL", "TRUE");
    String tablePath = hiveDirectoryName(url, topicsDir, tableName);
    table.setDataLocation(new Path(tablePath));
    table.setSerializationLib(getHiveOrcSerde());
    try {
      table.setInputFormatClass(getHiveOrcInputFormat());
      table.setOutputFormatClass(getHiveOrcOutputFormat());
    } catch (HiveException e) {
      throw new HiveMetaStoreException("Cannot find input/output format:", e);
    }
    // convert copycat schema schema to Hive columns
    List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
    table.setFields(columns);
    table.setPartCols(partitioner.partitionFields());
    return table;
  }

  private String getHiveOrcInputFormat() {
    return OrcInputFormat.class.getName();
  }

  private String getHiveOrcOutputFormat() {
    return OrcOutputFormat.class.getName();
  }

  private String getHiveOrcSerde() {
    return OrcSerde.class.getName();
  }

}

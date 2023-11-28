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
import io.confluent.connect.storage.errors.HiveMetaStoreException;
import io.confluent.connect.storage.hive.HiveMetaStore;
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import io.confluent.connect.storage.hive.HiveUtil;
import io.confluent.connect.storage.partitioner.Partitioner;
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
  private final HdfsSinkConnectorConfig config;

  public OrcHiveUtil(HdfsSinkConnectorConfig config, HiveMetaStore hiveMetaStore) {
    super(config, hiveMetaStore);
    this.config = config;
  }

  @Override
  public void alterSchema(String database, String tableName, Schema schema) {
    Table table = hiveMetaStore.getTable(database, tableName);
    List<FieldSchema> columns = HiveSchemaConverter.convertSchemaMaybeLogical(schema);
    table.setFields(columns);
    hiveMetaStore.alterTable(table);
  }

  @Override
  public void createTable(
      String database,
      String tableName,
      Schema schema,
      Partitioner<FieldSchema> partitioner,
      String topic
  ) throws HiveMetaStoreException {
    Table table = constructOrcTable(database, tableName, schema, partitioner, topic);
    hiveMetaStore.createTable(table);
  }

  private Table constructOrcTable(
      String database,
      String tableName,
      Schema schema,
      Partitioner<FieldSchema> partitioner,
      String topic
  ) throws HiveMetaStoreException {

    Table table = newTable(database, tableName);
    table.setTableType(TableType.EXTERNAL_TABLE);
    table.getParameters().put("EXTERNAL", "TRUE");

    String tablePath = hiveDirectoryName(url, config.getTopicsDirFromTopic(topic), topic);
    table.setDataLocation(new Path(tablePath));
    table.setSerializationLib(getHiveOrcSerde());

    try {
      table.setInputFormatClass(getHiveOrcInputFormat());
      table.setOutputFormatClass(getHiveOrcOutputFormat());
    } catch (HiveException e) {
      throw new HiveMetaStoreException("Cannot find input/output format:", e);
    }

    // convert Connect schema schema to Hive columns
    List<FieldSchema> columns = HiveSchemaConverter.convertSchemaMaybeLogical(schema);
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

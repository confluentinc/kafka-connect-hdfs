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

package io.confluent.connect.hdfs.avro;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.HiveMetaStoreException;
import io.confluent.connect.storage.hive.HiveSchemaConverter;

public class AvroHiveUtil extends HiveUtil {

  private static final String AVRO_SERDE = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
  private static final String AVRO_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro"
      + ".AvroContainerInputFormat";
  private static final String AVRO_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro"
      + ".AvroContainerOutputFormat";
  private static final String AVRO_SCHEMA_LITERAL = "avro.schema.literal";
  private final AvroData avroData;
  private final String topicsDir;

  public AvroHiveUtil(
      HdfsSinkConnectorConfig conf, AvroData avroData, HiveMetaStore
      hiveMetaStore
  ) {
    super(conf, hiveMetaStore);
    this.avroData = avroData;
    this.topicsDir = conf.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
  }

  @Override
  public void createTable(String database, String tableName, Schema schema, Partitioner partitioner)
      throws HiveMetaStoreException {
    Table table = constructAvroTable(database, tableName, schema, partitioner);
    hiveMetaStore.createTable(table);
  }

  @Override
  public void alterSchema(
      String database,
      String tableName,
      Schema schema
  ) throws HiveMetaStoreException {
    Table table = hiveMetaStore.getTable(database, tableName);
    table.getParameters().put(AVRO_SCHEMA_LITERAL, avroData.fromConnectSchema(schema).toString());
    hiveMetaStore.alterTable(table);
  }

  private Table constructAvroTable(
      String database,
      String tableName,
      Schema schema,
      Partitioner partitioner
  )
      throws HiveMetaStoreException {
    Table table = newTable(database, tableName);
    table.setTableType(TableType.EXTERNAL_TABLE);
    table.getParameters().put("EXTERNAL", "TRUE");
    String tablePath = hiveDirectoryName(url, topicsDir, tableName);
    table.setDataLocation(new Path(tablePath));
    table.setSerializationLib(AVRO_SERDE);
    try {
      table.setInputFormatClass(AVRO_INPUT_FORMAT);
      table.setOutputFormatClass(AVRO_OUTPUT_FORMAT);
    } catch (HiveException e) {
      throw new HiveMetaStoreException("Cannot find input/output format:", e);
    }
    List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
    table.setFields(columns);
    table.setPartCols(partitioner.partitionFields());
    table.getParameters().put(AVRO_SCHEMA_LITERAL, avroData.fromConnectSchema(schema).toString());
    return table;
  }
}

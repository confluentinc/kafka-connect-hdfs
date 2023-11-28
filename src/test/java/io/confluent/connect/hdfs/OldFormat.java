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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;

// Be careful if you need to edit anything in here. The Format, RecordWriterProvider,
// RecordWriter, SchemaFileReader, and HiveUtil classes need to remain compatible.
public class OldFormat implements Format {

  // DO NOT add any other constructors. No-arg constructor must be accessible
  public OldFormat() {

  }

  @Override
  public RecordWriterProvider getRecordWriterProvider() {
    return new RecordWriterProvider() {
      @Override
      public String getExtension() {
        return ".fake";
      }

      @Override
      public RecordWriter<SinkRecord> getRecordWriter(
          Configuration conf,
          String fileName,
          SinkRecord record,
          AvroData avroData
      ) throws IOException {
        return new RecordWriter<SinkRecord>() {
          @Override
          public void write(SinkRecord value) throws IOException {
            // Intentionally empty
          }

          @Override
          public void close() throws IOException {
            // Intentionally empty
          }
        };
      }
    };
  }

  @Override
  public SchemaFileReader getSchemaFileReader(AvroData avroData) {
    return new SchemaFileReader() {
      @Override
      public Schema getSchema(Configuration conf, Path path) throws IOException {
        return Schema.INT32_SCHEMA;
      }

      @Override
      public Collection<Object> readData(Configuration conf, Path path) throws IOException {
        return Arrays.asList((Object) 1, 2, 3);
      }
    };
  }

  @Override
  public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, HiveMetaStore hiveMetaStore) {
    return new HiveUtil(config, hiveMetaStore) {
      @Override
      public void createTable(
          String database, String tableName, Schema schema,
          Partitioner partitioner, String topic
      ) {
        // Intentionally empty
      }

      @Override
      public void alterSchema(String s, String s1, Schema schema) {
        // Intentionally empty
      }
    };
  }
}

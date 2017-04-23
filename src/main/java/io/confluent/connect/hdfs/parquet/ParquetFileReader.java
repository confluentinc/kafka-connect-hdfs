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
 */

package io.confluent.connect.hdfs.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.SchemaFileReader;

public class ParquetFileReader implements SchemaFileReader,
    io.confluent.connect.storage.format.SchemaFileReader<HdfsSinkConnectorConfig, Path> {
  private AvroData avroData;

  public ParquetFileReader(AvroData avroData) {
    this.avroData = avroData;
  }

  @Override
  public Schema getSchema(HdfsSinkConnectorConfig conf, Path path) {
    AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
    ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, path);
    try {
      ParquetReader<GenericRecord> parquetReader = builder.withConf(conf.getHadoopConfiguration())
          .build();
      GenericRecord record;
      Schema schema = null;
      while ((record = parquetReader.read()) != null) {
        schema = avroData.toConnectSchema(record.getSchema());
      }
      parquetReader.close();
      return schema;
    } catch (IOException e) {
      throw new DataException(e);
    }
  }

  @Override
  public Collection<Object> readData(HdfsSinkConnectorConfig conf, Path path) {
    Collection<Object> result = new ArrayList<>();
    AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
    ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, path);
    try {
      ParquetReader<GenericRecord> parquetReader = builder.withConf(conf.getHadoopConfiguration())
          .build();
      GenericRecord record;
      while ((record = parquetReader.read()) != null) {
        result.add(record);
      }
      parquetReader.close();
    } catch (IOException e) {
      throw new DataException(e);
    }
    return result;
  }

  public boolean hasNext() {
    throw new UnsupportedOperationException();
  }

  public Object next() {
    throw new UnsupportedOperationException();
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  public Iterator<Object> iterator() {
    throw new UnsupportedOperationException();
  }

  public void close() {}
}

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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.io.IOException;
import java.util.Iterator;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

public class AvroFileReader
    implements io.confluent.connect.storage.format.SchemaFileReader<HdfsSinkConnectorConfig, Path> {
  private AvroData avroData;

  public AvroFileReader(AvroData avroData) {
    this.avroData = avroData;
  }

  @Override
  public Schema getSchema(HdfsSinkConnectorConfig conf, Path path) {
    try {
      SeekableInput input = new FsInput(path, conf.getHadoopConfiguration());
      DatumReader<Object> reader = new GenericDatumReader<>();
      FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
      org.apache.avro.Schema schema = fileReader.getSchema();
      fileReader.close();
      return avroData.toConnectSchema(schema);
    } catch (IOException e) {
      throw new DataException(e);
    }
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

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

package io.confluent.connect.hdfs.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.SchemaFileReader;

public class AvroFileReader implements SchemaFileReader,
    io.confluent.connect.storage.format.SchemaFileReader<Configuration, Path> {
  private AvroData avroData;

  public AvroFileReader(AvroData avroData) {
    this.avroData = avroData;
  }

  @Override
  public Schema getSchema(Configuration conf, Path path) {
    try {
      SeekableInput input = new FsInput(path, conf);
      DatumReader<Object> reader = new GenericDatumReader<>();
      FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
      org.apache.avro.Schema schema = fileReader.getSchema();
      fileReader.close();
      return avroData.toConnectSchema(schema);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public Collection<Object> readData(Configuration conf, Path path) {
    ArrayList<Object> collection = new ArrayList<>();
    try {
      SeekableInput input = new FsInput(path, conf);
      DatumReader<Object> reader = new GenericDatumReader<>();
      FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
      for (Object object : fileReader) {
        collection.add(object);
      }
      fileReader.close();
    } catch (IOException e) {
      throw new ConnectException(e);
    }
    return collection;
  }
}

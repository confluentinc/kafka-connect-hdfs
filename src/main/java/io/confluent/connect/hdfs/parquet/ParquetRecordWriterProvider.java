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
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;

public class ParquetRecordWriterProvider implements RecordWriterProvider,
    io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private final static String EXTENSION = ".parquet";
  private final AvroData avroData;

  ParquetRecordWriterProvider(AvroData avroData) {
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(
      HdfsSinkConnectorConfig conf,
      final String filename,
      SinkRecord record,
      final AvroData avroData
  ) {
    return getRecordWriter(conf, filename);
  }

  @Override
  public RecordWriter getRecordWriter(
      final HdfsSinkConnectorConfig conf,
      final String filename
  ) {
    return new RecordWriter() {
      final CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
      final int blockSize = 256 * 1024 * 1024;
      final int pageSize = 64 * 1024;
      final Path path = new Path(filename);
      Schema schema = null;
      ParquetWriter<GenericRecord> writer = null;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          try {
            log.info("Opening record writer for: {}", filename);
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
            writer = new AvroParquetWriter<>(
                path,
                avroSchema,
                compressionCodecName,
                blockSize,
                pageSize,
                true,
                conf.getHadoopConfiguration()
            );
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        log.trace("Sink record: {}", record.toString());
        Object value = avroData.fromConnectData(record.valueSchema(), record.value());
        try {
          writer.write((GenericRecord) value);
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {}

      @Override
      public void close() {
        if (writer != null) {
          try {
            writer.close();
          } catch(IOException e){
            throw new ConnectException(e);
          }
        }
      }
    };
  }
}

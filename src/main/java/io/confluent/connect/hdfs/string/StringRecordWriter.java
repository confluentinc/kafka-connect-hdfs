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

package io.confluent.connect.hdfs.string;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

public class StringRecordWriter implements RecordWriter {
  private static final int WRITER_BUFFER_SIZE = 128 * 1024;
  private static final Logger log = LoggerFactory.getLogger(StringRecordWriterProvider.class);
  private BufferedWriter writer;
  private Compressor compressor;

  @SuppressWarnings("WeakerAccess")
  public StringRecordWriter(HdfsSinkConnectorConfig conf,
                            String filename,
                            CompressionCodec compressionCodec) throws IOException {
    final Path path = new Path(filename);
    OutputStream out = path.getFileSystem(conf.getHadoopConfiguration()).create(path);
    if (compressionCodec != null) {
      compressor = CodecPool.getCompressor(compressionCodec);
      try {
        out = compressionCodec.createOutputStream(out, compressor);
      } catch (IOException e) {
        CodecPool.returnCompressor(compressor);
        IOUtils.closeStream(out);
        throw e;
      }
    }
    final OutputStreamWriter streamWriter = new OutputStreamWriter(out, Charset.defaultCharset());
    writer = new BufferedWriter(streamWriter, WRITER_BUFFER_SIZE);
  }

  @Override
  public void write(SinkRecord record) {
    log.trace("Sink record: {}", record.toString());
    try {
      String value = (String) record.value();
      writer.write(value);
      writer.newLine();
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public void commit() {}

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      throw new ConnectException(e);
    } finally {
      if (compressor != null) {
        CodecPool.returnCompressor(compressor);
        compressor = null;
      }
    }
  }
}

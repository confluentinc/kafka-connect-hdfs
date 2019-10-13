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
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;

import static io.confluent.connect.hdfs.HdfsSinkConnectorConfig.STRING_FORMAT_COMPRESSION_CONFIG;
import static io.confluent.connect.hdfs.HdfsSinkConnectorConfig.STRING_FORMAT_COMPRESSION_NONE;

/**
 * Provider of a text record writer.
 */
public class StringRecordWriterProvider implements RecordWriterProvider<HdfsSinkConnectorConfig> {

  private static final String PLAINTEXT_EXTENSION = ".txt";
  private final String extension;
  private final CompressionCodec compressionCodec;

  /**
   * Constructor.
   *
   * @param storage the underlying storage implementation.
   */
  StringRecordWriterProvider(HdfsStorage storage) {
    final HdfsSinkConnectorConfig config = storage.conf();
    final String compression = config.getString(STRING_FORMAT_COMPRESSION_CONFIG);
    if (compression == null || compression.equalsIgnoreCase(STRING_FORMAT_COMPRESSION_NONE)) {
      compressionCodec = null;
      extension = PLAINTEXT_EXTENSION;
    } else {
      final CompressionCodecFactory compressionCodecFactory =
          new CompressionCodecFactory(config.getHadoopConfiguration());
      compressionCodec = compressionCodecFactory.getCodecByName(compression);
      if (compressionCodec == null) {
        throw new ConnectException("Compression codec '" + compression + "' not found");
      }
      extension = compressionCodec.getDefaultExtension();
    }
  }

  @Override
  public String getExtension() {
    return extension;
  }

  @Override
  public RecordWriter getRecordWriter(final HdfsSinkConnectorConfig conf, final String filename) {
    try {
      return new StringRecordWriter(conf, filename, compressionCodec);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}

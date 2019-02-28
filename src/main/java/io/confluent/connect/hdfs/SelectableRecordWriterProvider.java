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

package io.confluent.connect.hdfs;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

public class SelectableRecordWriterProvider {
  private final HdfsSinkConnectorConfig connectorConfig;
  private final AvroData avroData;

  private final io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig>
          newWriterProvider;
  // This is one case where we cannot simply wrap the old or new RecordWriterProvider with the
  // other because they have incompatible requirements for some methods -- one requires the Hadoop
  // config + extra parameters, the other requires the ConnectorConfig and doesn't get the other
  // extra parameters. Instead, we have to (optionally) store one of each and use whichever one is
  // non-null.
  private final RecordWriterProvider writerProvider;

  public SelectableRecordWriterProvider(HdfsSinkConnectorConfig connectorConfig, AvroData avroData,
         io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig>
                 newWriterProvider, RecordWriterProvider writerProvider) {
    this.connectorConfig = connectorConfig;
    this.avroData = avroData;
    this.newWriterProvider = newWriterProvider;
    this.writerProvider = writerProvider;
  }

  public String getExtension() {
    if (writerProvider != null) {
      return writerProvider.getExtension();
    } else if (newWriterProvider != null) {
      return newWriterProvider.getExtension();
    } else {
      throw new ConnectException(
              "Invalid state: either old or new RecordWriterProvider must be provided"
      );
    }
  }

  io.confluent.connect.storage.format.RecordWriter getRecordWriter(String fileName,
                                                                   SinkRecord record)
          throws IOException {
    if (writerProvider != null) {
      return new OldRecordWriterWrapper(
              writerProvider.getRecordWriter(
                      connectorConfig.getHadoopConfiguration(),
                      fileName,
                      record,
                      avroData
              )
      );
    } else if (newWriterProvider != null) {
      return newWriterProvider.getRecordWriter(connectorConfig, fileName);
    } else {
      throw new ConnectException(
              "Invalid state: either old or new RecordWriterProvider must be provided"
      );
    }
  }
}

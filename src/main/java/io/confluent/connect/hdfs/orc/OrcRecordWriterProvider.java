/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.hdfs.orc;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrcRecordWriterProvider
    implements io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig> {

  private static Logger log = LoggerFactory.getLogger(OrcRecordWriterProvider.class);
  private static final String EXTENSION = ".orc";

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final HdfsSinkConnectorConfig conf, final String filename) {
    final Path path = new Path(filename);
    return new io.confluent.connect.storage.format.RecordWriter() {
      Writer writer = null;
      TypeInfo typeInfo = null;
      Schema schema = null;

      @Override
      public void write(SinkRecord record) {
        try {
          if (schema == null) {
            log.info("Opening record writer for: {}", filename);
            schema = record.valueSchema();
            if (schema.type() == Schema.Type.STRUCT) {

              OrcFile.WriterCallback writerCallback = new OrcFile.WriterCallback() {
                @Override
                public void preStripeWrite(OrcFile.WriterContext writerContext) throws IOException {
                }

                @Override
                public void preFooterWrite(OrcFile.WriterContext writerContext) throws IOException {
                }
              };

              typeInfo = HiveSchemaConverter.convert(schema);
              ObjectInspector objectInspector = OrcStruct.createObjectInspector(typeInfo);

              writer = OrcFile.createWriter(path,
                      OrcFile.writerOptions(conf.getHadoopConfiguration())
                              .inspector(objectInspector)
                              .callback(writerCallback));
            }
          }

          if (schema.type() == Schema.Type.STRUCT) {
            log.trace("Sink record: {}", record.toString());
            Struct struct = (Struct) record.value();
            OrcStruct row = OrcUtil.createOrcStruct(typeInfo, OrcUtil.convertStruct(struct));
            writer.addRow(row);
          } else {
            throw new DataException("Top level type must be STRUCT");
          }
        } catch (IOException e) {
          e.printStackTrace();
          throw new DataException(e);
        }
      }

      @Override
      public void close() {
        try {
          if (writer != null) {
            writer.close();
          }
        } catch (IOException e) {
          throw new DataException(e);
        }
      }

      @Override
      public void commit() {
      }
    };
  }
}

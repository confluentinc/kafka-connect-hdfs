/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.*;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OrcRecordWriterProvider implements io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig> {
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
              typeInfo = HiveSchemaConverter.convert(schema);
              ObjectInspector objectInspector = OrcStruct.createObjectInspector(typeInfo);
              OrcFile.WriterOptions writerOptions = OrcFile
                .writerOptions(conf.getHadoopConfiguration())
                .inspector(objectInspector);
              writer = OrcFile.createWriter(path, writerOptions);
            }
          }

          if (schema.type() == Schema.Type.STRUCT) {
            log.trace("Sink record: {}", record.toString());
            Struct struct = (Struct) record.value();
            OrcStruct row = OrcUtils.createOrcStruct(typeInfo, objs(struct));
            writer.addRow(row);
          } else {
            throw new DataException("Top level type must be STRUCT");
          }
        } catch (IOException e) {
          e.printStackTrace();
          throw new DataException(e);
        }
      }

      private Object[] objs(Struct struct) {
        List<Object> data = new LinkedList<>();
        for (Field f : struct.schema().fields()) {
          if (struct.get(f) == null) {
            data.add((Writable) null);
          } else {
            Schema.Type schemaType = f.schema().type();
            switch (schemaType) {
              case BOOLEAN:
                data.add(new BooleanWritable(struct.getBoolean(f.name())));
                break;
              case STRING:
                data.add(new Text(struct.getString(f.name())));
                break;
              case BYTES:
                data.add(new BytesWritable(struct.getBytes(f.name())));
                break;
              case INT8:
                data.add(new ByteWritable(struct.getInt8(f.name())));
                break;
              case INT16:
                data.add(new ShortWritable(struct.getInt16(f.name())));
                break;
              case INT32:
                if (Date.LOGICAL_NAME.equals(f.schema().name())) {
                  java.util.Date date = (java.util.Date) struct.get(f);
                  data.add(new DateWritable(new java.sql.Date(date.getTime())));
                } else if (Time.LOGICAL_NAME.equals(f.schema().name())) {
                  java.util.Date date = (java.util.Date) struct.get(f);
                  data.add(new TimestampWritable(new java.sql.Timestamp(date.getTime())));
                } else {
                  data.add(new IntWritable(struct.getInt32(f.name())));
                }
                break;
              case INT64:
                if (Timestamp.LOGICAL_NAME.equals(f.schema().name())) {
                  java.util.Date date = (java.util.Date) struct.get(f);
                  data.add(new TimestampWritable(new java.sql.Timestamp(date.getTime())));
                } else {
                  data.add(new LongWritable(struct.getInt64(f.name())));
                }
                break;
              case FLOAT32:
                data.add(new FloatWritable(struct.getFloat32(f.name())));
                break;
              case FLOAT64:
                data.add(new DoubleWritable(struct.getFloat64(f.name())));
                break;
              case ARRAY:
                data.add(new ArrayPrimitiveWritable(struct.getArray(f.name()).toArray()));
                break;
              case MAP:
                MapWritable mapWritable = new MapWritable();
                Map<Object, Object> map = struct.getMap(f.name());
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                  mapWritable.put(new ObjectWritable(entry.getKey()), new ObjectWritable(entry.getValue()));
                }
                data.add(mapWritable);
                break;
              case STRUCT:
                data.add(objs(struct.getStruct(f.name())));
                break;
            }
          }
        }
        return data.toArray(new Object[0]);
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

/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.connect.hdfs.orc;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.storage.format.SchemaFileReader;
import javax.annotation.Nonnull;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class OrcFileReader implements SchemaFileReader<HdfsSinkConnectorConfig, Path> {

  private static final Logger log = LoggerFactory.getLogger(OrcFileReader.class);

  @Override
  public Schema getSchema(HdfsSinkConnectorConfig conf, Path path) {
    try {
      log.debug("Opening ORC record reader for: {}", path);

      ReaderOptions readerOptions = new ReaderOptions(conf.getHadoopConfiguration());
      Reader reader = OrcFile.createReader(path, readerOptions);

      if (reader.getObjectInspector().getCategory() == ObjectInspector.Category.STRUCT) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name("record").version(1);
        StructObjectInspector objectInspector = (StructObjectInspector) reader.getObjectInspector();

        for (StructField schema : objectInspector.getAllStructFieldRefs()) {
          ObjectInspector fieldObjectInspector = schema.getFieldObjectInspector();
          String typeName = fieldObjectInspector.getTypeName();
          Schema.Type schemaType;

          switch (fieldObjectInspector.getCategory()) {
            case PRIMITIVE:
              PrimitiveTypeEntry typeEntry = PrimitiveObjectInspectorUtils
                  .getTypeEntryFromTypeName(typeName);
              if (java.sql.Date.class.isAssignableFrom(typeEntry.primitiveJavaClass)) {
                schemaType = Date.SCHEMA.type();
              } else if (java.sql.Timestamp.class.isAssignableFrom(typeEntry.primitiveJavaClass)) {
                schemaType = Timestamp.SCHEMA.type();
              } else {
                schemaType = ConnectSchema.schemaType(typeEntry.primitiveJavaClass);
              }
              break;
            case LIST:
              schemaType = Schema.Type.ARRAY;
              break;
            case MAP:
              schemaType = Schema.Type.MAP;
              break;
            default:
              throw new DataException("Unknown type " + fieldObjectInspector.getCategory().name());
          }

          schemaBuilder.field(schema.getFieldName(), SchemaBuilder.type(schemaType).build());
        }

        return schemaBuilder.build();
      } else {
        throw new ConnectException(
            "Top level type must be of type STRUCT, but was "
                + reader.getObjectInspector().getCategory().name()
        );
      }
    } catch (IOException e) {
      throw new ConnectException("Failed to get schema for file " + path, e);
    }
  }

  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException();
  }

  public Object next() {
    throw new UnsupportedOperationException();
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Nonnull
  public Iterator<Object> iterator() {
    throw new UnsupportedOperationException();
  }

  public void close() {
  }
}

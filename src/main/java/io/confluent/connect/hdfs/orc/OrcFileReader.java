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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
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
        StructObjectInspector objectInspector = (StructObjectInspector) reader.getObjectInspector();
        SchemaBuilder schemaBuilder = deriveStruct(objectInspector);
        schemaBuilder.name("record").version(1);
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

  private SchemaBuilder derivePrimitive(PrimitiveObjectInspector inspector) {
    Class<?> klass = inspector.getTypeInfo().getPrimitiveJavaClass();
    Schema.Type schemaType;
    if (java.sql.Date.class.isAssignableFrom(klass)) {
      schemaType = Date.SCHEMA.type();
    } else if (java.sql.Timestamp.class.isAssignableFrom(klass)) {
      schemaType = Timestamp.SCHEMA.type();
    } else if (org.apache.hadoop.hive.common.type.HiveDecimal.class.isAssignableFrom(klass)) {
      schemaType = Decimal.schema(inspector.scale()).type();
    } else {
      schemaType = ConnectSchema.schemaType(klass);
    }
    return SchemaBuilder.type(schemaType);
  }


  private SchemaBuilder deriveStruct(StructObjectInspector inspector) {

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (StructField field: inspector.getAllStructFieldRefs()) {

      ObjectInspector fieldInspector = field.getFieldObjectInspector();
      switch (field.getFieldObjectInspector().getCategory()) {
        case PRIMITIVE:
          schemaBuilder.field(field.getFieldName(), derivePrimitive((PrimitiveObjectInspector)
              fieldInspector).build());
          break;
        case LIST:
          schemaBuilder.field(field.getFieldName(), SchemaBuilder.type(Type.ARRAY).build());
          break;
        case MAP:
          schemaBuilder.field(field.getFieldName(), SchemaBuilder.type(Type.MAP).build());
          break;
        case STRUCT:
          SchemaBuilder b = deriveStruct((StructObjectInspector) fieldInspector);
          b.name(field.getFieldName());
          schemaBuilder.field(field.getFieldName(), b.build());
          break;
        default:
          throw new DataException("Unknown type " + field.getFieldObjectInspector().getCategory()
              .name());
      }
    }

    return schemaBuilder;
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

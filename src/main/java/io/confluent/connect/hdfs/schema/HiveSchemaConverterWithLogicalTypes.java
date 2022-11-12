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

package io.confluent.connect.hdfs.schema;

import io.confluent.connect.storage.hive.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.List;

public class HiveSchemaConverterWithLogicalTypes {

  public static TypeInfo convert(Schema schema) {
    // TODO: throw an error on recursive types
    switch (schema.type()) {
      case STRUCT:
        return convertStruct(schema);
      case ARRAY:
        return convertArray(schema);
      case MAP:
        return convertMap(schema);
      default:
        return convertPrimitive(schema);
    }
  }

  public static TypeInfo convertStruct(Schema schema) {
    final List<Field> fields = schema.fields();
    final List<String> names = new ArrayList<>(fields.size());
    final List<TypeInfo> types = new ArrayList<>(fields.size());
    for (Field field : fields) {
      names.add(field.name());
      types.add(convert(field.schema()));
    }
    return TypeInfoFactory.getStructTypeInfo(names, types);
  }

  public static TypeInfo convertArray(Schema schema) {
    return TypeInfoFactory.getListTypeInfo(convert(schema.valueSchema()));
  }

  public static TypeInfo convertMap(Schema schema) {
    return TypeInfoFactory.getMapTypeInfo(
        convert(schema.keySchema()),
        convert(schema.valueSchema())
    );
  }

  public static TypeInfo convertPrimitive(Schema schema) {
    if (schema.name() != null) {
      switch (schema.name()) {
        case Date.LOGICAL_NAME:
          return TypeInfoFactory.dateTypeInfo;
        case Timestamp.LOGICAL_NAME:
          return TypeInfoFactory.timestampTypeInfo;
        // NOTE: We currently leave TIME values as INT32 (the default).
        //       Converting to a STRING would be ok too.
        //       Sadly, writing as INTERVAL is unsupported in the kafka-connect library.
        //       See: org.apache.hadoop.hive.ql.io.orc.WriterImpl - INTERVAL is missing
        //case Time.LOGICAL_NAME:
        //  return TypeInfoFactory.intervalDayTimeTypeInfo;
        default:
          break;
      }
    }

    // HiveSchemaConverter converts primitives just fine, just not all logical-types.
    return HiveSchemaConverter.convertPrimitiveMaybeLogical(schema);
  }
}
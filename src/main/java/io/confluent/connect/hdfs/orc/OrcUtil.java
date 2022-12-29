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

import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import static org.apache.kafka.connect.data.Schema.Type.BOOLEAN;
import static org.apache.kafka.connect.data.Schema.Type.BYTES;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT32;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT64;
import static org.apache.kafka.connect.data.Schema.Type.INT16;
import static org.apache.kafka.connect.data.Schema.Type.INT32;
import static org.apache.kafka.connect.data.Schema.Type.INT64;
import static org.apache.kafka.connect.data.Schema.Type.INT8;
import static org.apache.kafka.connect.data.Schema.Type.MAP;
import static org.apache.kafka.connect.data.Schema.Type.STRING;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;

public final class OrcUtil {

  private static final Map<Type, BiFunction<Struct, Field, Object>> CONVERSION_MAP =
      new HashMap<>();

  static {
    CONVERSION_MAP.put(ARRAY, OrcUtil::convertArray);
    CONVERSION_MAP.put(BOOLEAN, OrcUtil::convertBoolean);
    CONVERSION_MAP.put(BYTES, OrcUtil::convertBytes);
    CONVERSION_MAP.put(FLOAT32, OrcUtil::convertFloat32);
    CONVERSION_MAP.put(FLOAT64, OrcUtil::convertFloat64);
    CONVERSION_MAP.put(INT8, OrcUtil::convertInt8);
    CONVERSION_MAP.put(INT16, OrcUtil::convertInt16);
    CONVERSION_MAP.put(INT32, OrcUtil::convertInt32);
    CONVERSION_MAP.put(INT64, OrcUtil::convertInt64);
    CONVERSION_MAP.put(MAP, OrcUtil::convertMap);
    CONVERSION_MAP.put(STRING, OrcUtil::convertString);
  }

  /**
   * Create an object of OrcStruct given a type and a list of objects
   *
   * @param typeInfo the type info
   * @param objs the objects corresponding to the struct fields
   * @return the struct object
   */
  @SuppressWarnings("unchecked")
  public static OrcStruct createOrcStruct(TypeInfo typeInfo, Object[] objs) {
    SettableStructObjectInspector oi = (SettableStructObjectInspector)
            OrcStruct.createObjectInspector(typeInfo);

    List<StructField> fields = (List<StructField>) oi.getAllStructFieldRefs();
    OrcStruct result = (OrcStruct) oi.create();
    result.setNumFields(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      oi.setStructFieldData(result, fields.get(i), objs[i]);
    }

    return result;
  }

  /**
   * Convert a Struct into a Writable array
   *
   * @param struct the struct to convert
   * @return the struct as a writable array
   */
  public static Object[] convertStruct(TypeInfo typeInfo, Struct struct) {
    List<Object> data = new LinkedList<>();
    for (Field field : struct.schema().fields()) {
      if (struct.get(field) == null) {
        data.add(null);
      } else {
        Schema.Type schemaType = field.schema().type();
        if (STRUCT.equals(schemaType)) {
          data.add(convertStruct(typeInfo, struct, field));
        } else {
          data.add(CONVERSION_MAP.get(schemaType).apply(struct, field));
        }
      }
    }

    return data.toArray();
  }

  private static Object convertStruct(TypeInfo typeInfo, Struct struct, Field field) {
    TypeInfo fieldTypeInfo = ((StructTypeInfo) typeInfo).getStructFieldTypeInfo(field.name());

    return createOrcStruct(
        fieldTypeInfo,
        convertStruct(fieldTypeInfo, struct.getStruct(field.name()))
    );
  }

  private static Object convertArray(Struct struct, Field field) {
    return new ArrayPrimitiveWritable(struct.getArray(field.name()).toArray());
  }

  private static Object convertBoolean(Struct struct, Field field) {
    return new BooleanWritable(struct.getBoolean(field.name()));
  }

  private static Object convertBytes(Struct struct, Field field) {

    if (Decimal.LOGICAL_NAME.equals(field.schema().name())) {
      BigDecimal bigDecimal = (BigDecimal) struct.get(field.name());
      return new HiveDecimalWritable(HiveDecimal.create(bigDecimal));
    }

    return new BytesWritable(struct.getBytes(field.name()));
  }

  private static Object convertFloat32(Struct struct, Field field) {
    return new FloatWritable(struct.getFloat32(field.name()));
  }

  private static Object convertFloat64(Struct struct, Field field) {
    return new DoubleWritable(struct.getFloat64(field.name()));
  }

  private static Object convertInt8(Struct struct, Field field) {
    return new ByteWritable(struct.getInt8(field.name()));
  }

  private static Object convertInt16(Struct struct, Field field) {
    return new ShortWritable(struct.getInt16(field.name()));
  }

  private static Object convertInt32(Struct struct, Field field) {

    if (Date.LOGICAL_NAME.equals(field.schema().name())) {
      java.util.Date date = (java.util.Date) struct.get(field);
      return new DateWritable(new java.sql.Date(date.getTime()));
    }

    if (Time.LOGICAL_NAME.equals(field.schema().name())) {
      java.util.Date date = (java.util.Date) struct.get(field);
      return new IntWritable((int) date.getTime());
    }

    return new IntWritable(struct.getInt32(field.name()));
  }

  private static Object convertInt64(Struct struct, Field field) {

    if (Timestamp.LOGICAL_NAME.equals(field.schema().name())) {
      java.util.Date date = (java.util.Date) struct.get(field);
      return new TimestampWritable(new java.sql.Timestamp(date.getTime()));
    }

    return new LongWritable(struct.getInt64(field.name()));
  }

  private static Object convertMap(Struct struct, Field field) {
    MapWritable mapWritable = new MapWritable();
    struct.getMap(field.name()).forEach(
        (key, value) -> mapWritable.put(new ObjectWritable(key), new ObjectWritable(value))
    );

    return mapWritable;
  }

  private static Object convertString(Struct struct, Field field) {
    return new Text(struct.getString(field.name()));
  }
}

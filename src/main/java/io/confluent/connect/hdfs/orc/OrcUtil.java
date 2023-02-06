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

import static org.apache.kafka.connect.data.Schema.Type.BOOLEAN;
import static org.apache.kafka.connect.data.Schema.Type.BYTES;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT32;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT64;
import static org.apache.kafka.connect.data.Schema.Type.INT16;
import static org.apache.kafka.connect.data.Schema.Type.INT32;
import static org.apache.kafka.connect.data.Schema.Type.INT64;
import static org.apache.kafka.connect.data.Schema.Type.INT8;
import static org.apache.kafka.connect.data.Schema.Type.STRING;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;

import java.util.stream.Collectors;
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
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

  private static final Map<Type, BiFunction<Schema, Object, Object>> PRIMITIVE_CONVERSION_MAP =
      new HashMap<>();

  static {
    PRIMITIVE_CONVERSION_MAP.put(BOOLEAN, OrcUtil::convertBoolean);
    PRIMITIVE_CONVERSION_MAP.put(BYTES, OrcUtil::convertBytes);
    PRIMITIVE_CONVERSION_MAP.put(FLOAT32, OrcUtil::convertFloat32);
    PRIMITIVE_CONVERSION_MAP.put(FLOAT64, OrcUtil::convertFloat64);
    PRIMITIVE_CONVERSION_MAP.put(INT8, OrcUtil::convertInt8);
    PRIMITIVE_CONVERSION_MAP.put(INT16, OrcUtil::convertInt16);
    PRIMITIVE_CONVERSION_MAP.put(INT32, OrcUtil::convertInt32);
    PRIMITIVE_CONVERSION_MAP.put(INT64, OrcUtil::convertInt64);
    PRIMITIVE_CONVERSION_MAP.put(STRING, OrcUtil::convertString);
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
        TypeInfo fieldTypeInfo = ((StructTypeInfo) typeInfo).getStructFieldTypeInfo(field.name());
        data.add(convert(fieldTypeInfo, field.schema(), struct.get(field)));
      }
    }

    return data.toArray();
  }

  public static Object convert(TypeInfo typeInfo, Schema schema, Object obj) {

    switch (schema.type()) {
      case STRUCT:
        return createOrcStruct(typeInfo, convertStruct(typeInfo, (Struct) obj));
      case ARRAY:
        return convertArray(typeInfo, schema, (List<?>) obj);
      case MAP:
        return convertMap(typeInfo, schema, (Map<?, ?>) obj);
      default:
        return PRIMITIVE_CONVERSION_MAP.get(schema.type()).apply(schema, obj);
    }
  }

  private static Object convertArray(TypeInfo typeInfo, Schema schema, List<?> objects) {

    TypeInfo elementTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
    Schema valueSchema = schema.valueSchema();
    return objects.stream().map(o -> convert(elementTypeInfo, valueSchema, o))
        .collect(Collectors.toList());
  }

  private static Object convertBoolean(Schema schema, Object obj) {
    return new BooleanWritable((Boolean) obj);
  }

  private static Object convertBytes(Schema schema, Object obj) {

    if (Decimal.LOGICAL_NAME.equals(schema.name())) {
      BigDecimal bigDecimal = (BigDecimal) obj;
      return new HiveDecimalWritable(HiveDecimal.create(bigDecimal));
    }

    // taken from Struct.getBytes()
    byte[] bytes = obj instanceof ByteBuffer ? ((ByteBuffer)obj).array() : (byte[])((byte[])obj);
    return new BytesWritable(bytes);
  }

  private static Object convertFloat32(Schema schema, Object obj) {
    return new FloatWritable((Float) obj);
  }

  private static Object convertFloat64(Schema schema, Object obj) {
    return new DoubleWritable((Double) obj);
  }

  private static Object convertInt8(Schema schema, Object obj) {
    return new ByteWritable((Byte) obj);
  }

  private static Object convertInt16(Schema schema, Object obj) {
    return new ShortWritable((Short) obj);
  }

  private static Object convertInt32(Schema schema, Object obj) {

    if (Date.LOGICAL_NAME.equals(schema.name())) {
      java.util.Date date = (java.util.Date) obj;
      return new DateWritable(new java.sql.Date(date.getTime()));
    }

    if (Time.LOGICAL_NAME.equals(schema.name())) {
      java.util.Date date = (java.util.Date) obj;
      return new IntWritable((int) date.getTime());
    }

    return new IntWritable((Integer) obj);
  }

  private static Object convertInt64(Schema schema, Object obj) {

    if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
      java.util.Date date = (java.util.Date) obj;
      return new TimestampWritable(new java.sql.Timestamp(date.getTime()));
    }

    if (Time.LOGICAL_NAME.equals(schema.name())) {
      java.util.Date date = (java.util.Date) obj;
      return new LongWritable(date.getTime());
    }

    return new LongWritable((Long) obj);
  }

  private static Object convertMap(TypeInfo typeInfo, Schema schema, Map<?, ?> obj) {

    MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
    return obj.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<>(
        convert(mapTypeInfo.getMapKeyTypeInfo(), schema.keySchema(), e.getKey()),
        convert(mapTypeInfo.getMapValueTypeInfo(), schema.valueSchema(), e.getValue()))
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
  }

  private static Object convertString(Schema schema, Object obj) {
    return new Text((String) obj);
  }
}

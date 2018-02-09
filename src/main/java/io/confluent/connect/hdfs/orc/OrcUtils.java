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

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Properties;

public class OrcUtils {

  /**
   * Generate TypeInfo for a given java class based on reflection
   *
   * @param typeClass
   * @return
   */
  public static TypeInfo getTypeInfo(Class<?> typeClass) {
    ObjectInspector oi = ObjectInspectorFactory
      .getReflectionObjectInspector(typeClass, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    return TypeInfoUtils.getTypeInfoFromObjectInspector(oi);
  }

  /**
   * Create an object of OrcStruct given a type string and a list of objects
   *
   * @param typeInfo
   * @param objs
   * @return
   */
  public static OrcStruct createOrcStruct(TypeInfo typeInfo, Object... objs) {
    SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
    List<StructField> fields = (List<StructField>) oi.getAllStructFieldRefs();
    OrcStruct result = (OrcStruct) oi.create();
    result.setNumFields(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      oi.setStructFieldData(result, fields.get(i), objs[i]);
    }
    return result;
  }

  /**
   * Create a binary serde for OrcStruct serialization/deserialization
   *
   * @param typeInfo
   * @return
   */
  public static BinarySortableSerDe createBinarySerde(TypeInfo typeInfo) {
    BinarySortableSerDe serde = new BinarySortableSerDe();

    StringBuffer nameSb = new StringBuffer();
    StringBuffer typeSb = new StringBuffer();

    StructTypeInfo sti = (StructTypeInfo) typeInfo;
    for (String name : sti.getAllStructFieldNames()) {
      nameSb.append(name);
      nameSb.append(',');
    }
    for (TypeInfo info : sti.getAllStructFieldTypeInfos()) {
      typeSb.append(info.toString());
      typeSb.append(',');
    }

    Properties tbl = new Properties();
    String names = nameSb.length() > 0 ? nameSb.substring(0,
      nameSb.length() - 1) : "";
    String types = typeSb.length() > 0 ? typeSb.substring(0,
      typeSb.length() - 1) : "";
    tbl.setProperty(serdeConstants.LIST_COLUMNS, names);
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);

    try {
      serde.initialize(null, tbl);
    } catch (SerDeException e) {
      throw new DataException("Unable to initialize binary serde");
    }

    return serde;
  }

}

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

package io.confluent.connect.hdfs.jdbc;

import org.apache.kafka.connect.data.Struct;

public class StructToJdbcValueMapper implements JdbcValueMapper {
  public final Struct struct;

  public StructToJdbcValueMapper(Struct struct) {
    this.struct = struct;
  }

  @Override
  public Boolean getBoolean(String key) {
    return struct.getBoolean(key);
  }

  @Override
  public Byte getByte(String key) {
    return struct.getInt8(key);
  }

  @Override
  public Double getDouble(String key) {
    return struct.getFloat64(key);
  }

  @Override
  public Float getFloat(String key) {
    return struct.getFloat32(key);
  }

  @Override
  public Integer getInteger(String key) {
    return struct.getInt32(key);
  }

  @Override
  public Long getLong(String key) {
    return struct.getInt64(key);
  }

  @Override
  public Short getShort(String key) {
    return struct.getInt16(key);
  }

  @Override
  public String getString(String key) {
    return struct.getString(key);
  }
}

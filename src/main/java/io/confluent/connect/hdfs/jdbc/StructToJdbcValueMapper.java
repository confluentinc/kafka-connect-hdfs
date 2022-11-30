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

public class StructToJdbcValueMapper implements JdbcValueMapper<String> {
  public final Struct struct;

  public StructToJdbcValueMapper(Struct struct) {
    this.struct = struct;
  }

  @Override
  public Boolean getBoolean(String value) {
    return struct.getBoolean(value);
  }

  @Override
  public Byte getByte(String value) {
    return struct.getInt8(value);
  }

  @Override
  public Integer getInteger(String value) {
    return struct.getInt32(value);
  }

  @Override
  public Long getLong(String value) {
    return struct.getInt64(value);
  }

  @Override
  public Short getShort(String value) {
    return struct.getInt16(value);
  }

  @Override
  public String getString(String value) {
    return struct.getString(value);
  }
}

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

import java.sql.JDBCType;
import java.util.Comparator;

public class JdbcColumn {
  private final String name;
  private final JDBCType jdbcType;
  private final int ordinal;
  private final boolean nullable;

  public static Comparator<JdbcColumn> byOrdinal = Comparator.comparingInt(JdbcColumn::getOrdinal);

  public JdbcColumn(String name,
                    JDBCType jdbcType,
                    int ordinal,
                    boolean nullable) {
    this.name = name;
    this.jdbcType = jdbcType;
    this.ordinal = ordinal;
    this.nullable = nullable;
  }

  public String getName() {
    return name;
  }

  public JDBCType getJdbcType() {
    return jdbcType;
  }

  public int getOrdinal() {
    return ordinal;
  }

  public boolean isNullable() {
    return nullable;
  }

  @Override
  public String toString() {
    return "JdbcColumn{" +
           "name='" + name + '\'' +
           ", jdbcType=" + jdbcType +
           ", ordinal=" + ordinal +
           ", nullable=" + nullable +
           '}';
  }
}

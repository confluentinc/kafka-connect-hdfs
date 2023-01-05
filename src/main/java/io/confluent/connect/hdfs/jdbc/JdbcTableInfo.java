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

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcTableInfo implements Comparable<JdbcTableInfo> {
  //private final String db;
  private final String schema;
  private final String table;

  public static Comparator<JdbcTableInfo> comparator =
      Comparator
          .comparing(
              //JdbcTableInfo::getDb,
              //Comparator.nullsFirst(Comparator.naturalOrder())
              //)
              //.thenComparing(
              JdbcTableInfo::getSchema,
              Comparator.nullsFirst(Comparator.naturalOrder())
          )
          .thenComparing(
              JdbcTableInfo::getTable,
              Comparator.nullsFirst(Comparator.naturalOrder())
          );

  public JdbcTableInfo(/*String db, */String schema, String table) {
    //this.db = db;
    this.schema = schema;
    this.table = table;
  }

  //public String getDb() {
  //  return db;
  //}

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  public String qualifiedName() {
    // Same as toString, without being prefixed by the db
    return JdbcUtil.trimToNull(
        Stream
            .of(schema, table)
            .filter(Objects::nonNull)
            .collect(Collectors.joining("."))
    );
  }

  @Override
  public int compareTo(@Nonnull JdbcTableInfo tableInfo) {
    return comparator.compare(this, tableInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JdbcTableInfo)) {
      return false;
    }
    return compareTo((JdbcTableInfo) o) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(/*getDb(), */getSchema(), getTable());
  }

  @Override
  public String toString() {
    return Stream
        .of(/*db, */schema, table)
        .filter(Objects::nonNull)
        .collect(Collectors.joining(".", "JdbcTableInfo{", "}"));
  }
}

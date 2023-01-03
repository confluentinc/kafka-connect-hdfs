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

import java.util.Optional;

public abstract class FilteredJdbcColumnVisitor implements JdbcColumnVisitor {
  private final HashCache hashCache;
  protected final JdbcTableInfo tableInfo;
  protected final String primaryKey;
  private int columnsChanged = 0;

  public FilteredJdbcColumnVisitor(HashCache hashCache,
                                   JdbcTableInfo tableInfo,
                                   String primaryKey) {
    this.hashCache = hashCache;
    this.tableInfo = tableInfo;
    this.primaryKey = primaryKey;
  }

  public boolean hasChangedColumns() {
    return columnsChanged > 0;
  }

  protected void updateCache(String columnName, byte[] value) {
    boolean columnChanged = Optional
        .ofNullable(hashCache)
        .map(hashCache_ -> hashCache_.updateCache(tableInfo, primaryKey, columnName, value))
        .orElse(true);

    // If it has changed, indicate that we should write the new value to HDFS
    if (columnChanged) {
      columnsChanged++;
    }
  }

  protected void updateCache(String columnName, String value) {
    boolean columnChanged = Optional
        .ofNullable(hashCache)
        .map(hashCache_ -> hashCache_.updateCache(tableInfo, primaryKey, columnName, value))
        .orElse(true);

    // If it has changed, indicate that we should write the new value to HDFS
    if (columnChanged) {
      columnsChanged++;
    }
  }
}

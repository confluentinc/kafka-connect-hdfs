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

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;

public class FilteredColumnToStructVisitor extends FilteredJdbcColumnVisitor {
  private final Struct struct;

  public FilteredColumnToStructVisitor(HashCache hashCache,
                                       JdbcTableInfo tableInfo,
                                       String primaryKey,
                                       Struct struct) {
    super(hashCache, tableInfo, primaryKey);
    this.struct = struct;
  }

  @Override
  public void visit(String columnName, Blob value) throws SQLException {
    // TODO: Would be so much better if we could stream this data
    // TODO: Write to a disk-buffer first, and then digest()? RocksDB?
    byte[] bytes = value != null
        ? value.getBytes(1L, (int) value.length())
        : null;

    updateCache(columnName, bytes);

    struct.put(columnName, bytes);
  }

  @Override
  public void visit(String columnName, Clob value) throws SQLException {
    // TODO: Would be so much better if we could stream this data
    // TODO: Write to a disk-buffer first, and then digest()? RocksDB?
    String valueStr = value != null
        ? value.getSubString(1L, (int) value.length())
        : null;

    updateCache(columnName, valueStr);

    struct.put(columnName, valueStr);
  }

  @Override
  public void visit(String columnName, SQLXML value) throws SQLException {
    // TODO: Would be so much better if we could stream this data
    // TODO: Write to a disk-buffer first, and then digest()? RocksDB?
    String valueStr = value != null
        ? value.getString()
        : null;

    updateCache(columnName, valueStr);

    struct.put(columnName, valueStr);
  }

  @Override
  public void visit(String columnName, String value) {
    updateCache(columnName, value);

    struct.put(columnName, value);
  }
}

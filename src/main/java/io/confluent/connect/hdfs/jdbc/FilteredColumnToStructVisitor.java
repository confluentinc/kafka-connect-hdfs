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

import org.apache.hadoop.io.MD5Hash;
import org.apache.kafka.connect.data.Struct;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;

class FilteredColumnToStructVisitor implements JdbcColumnVisitor {

  private final JdbcTableHashCache jdbcTableHashCache;
  private final Struct struct;
  private final JdbcTableInfo tableInfo;
  private final String primaryKey;
  private boolean columnsChanged = false;

  public FilteredColumnToStructVisitor(JdbcTableHashCache jdbcTableHashCache,
                                       Struct struct,
                                       JdbcTableInfo tableInfo,
                                       String primaryKey) {
    this.jdbcTableHashCache = jdbcTableHashCache;
    this.struct = struct;
    this.tableInfo = tableInfo;
    this.primaryKey = primaryKey;
  }

  public boolean hasChangedColumns() {
    return columnsChanged;
  }

  @Override
  public void visit(String columnName, Blob value) throws SQLException {
    // TODO: Would be so much better if we could stream this data
    // TODO: Write to a disk-buffer first, and then run MD5? RocksDB?
    byte[] bytes = value != null
        ? value.getBytes(1L, (int) value.length())
        : null;

    // NOTE: MD5 Hashing takes about 1-2 seconds per gig, at least locally
    boolean columnChanged = jdbcTableHashCache.updateCache(
        tableInfo,
        primaryKey,
        columnName,
        bytes,
        MD5Hash::digest
    );
    columnsChanged |= columnChanged;

    // If it has changed, write the new value to HDFS
    if (bytes != null) {
      struct.put(columnName, bytes);
    }
  }

  @Override
  public void visit(String columnName, Clob value) throws SQLException {
    // TODO: Would be so much better if we could stream this data
    // TODO: Write to a disk-buffer first, and then run MD5? RocksDB?
    String valueStr = value != null
        ? value.getSubString(1L, (int) value.length())
        : null;

    // NOTE: MD5 Hashing takes about 1-2 seconds per gig, at least locally
    boolean columnChanged = jdbcTableHashCache.updateCache(
        tableInfo,
        primaryKey,
        columnName,
        valueStr,
        MD5Hash::digest
    );
    columnsChanged |= columnChanged;

    // If it has changed, write the new value to HDFS
    if (valueStr != null) {
      struct.put(columnName, valueStr);
    }
  }

  @Override
  public void visit(String columnName, SQLXML value) throws SQLException {
    // TODO: Would be so much better if we could stream this data
    // TODO: Write to a disk-buffer first, and then run MD5? RocksDB?
    String valueStr = value != null
        ? value.getString()
        : null;

    // NOTE: MD5 Hashing takes about 1-2 seconds per gig, at least locally
    boolean columnChanged = jdbcTableHashCache.updateCache(
        tableInfo,
        primaryKey,
        columnName,
        valueStr,
        MD5Hash::digest
    );
    columnsChanged |= columnChanged;

    // If it has changed, write the new value to HDFS
    if (valueStr != null) {
      struct.put(columnName, valueStr);
    }
  }
}

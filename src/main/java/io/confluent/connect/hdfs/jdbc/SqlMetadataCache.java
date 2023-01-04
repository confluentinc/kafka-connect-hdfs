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

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlMetadataCache {
  private final Map<JdbcTableInfo, List<JdbcColumnInfo>> allColumnsMap =
      new HashMap<>();
  private final Map<JdbcTableInfo, List<JdbcColumnInfo>> primaryKeyColumnsMap =
      new HashMap<>();
  private final DataSource dataSource;

  public SqlMetadataCache(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public synchronized List<JdbcColumnInfo> fetchAllColumns(
      JdbcTableInfo tableInfo
  ) throws SQLException {
    List<JdbcColumnInfo> allColumns = allColumnsMap.get(tableInfo);

    if (allColumns == null) {
      allColumns = JdbcQueryUtil.fetchAllColumns(dataSource, tableInfo);
      allColumnsMap.put(tableInfo, allColumns);
    }
    return allColumns;
  }

  public synchronized List<JdbcColumnInfo> fetchPrimaryKeyColumns(
      JdbcTableInfo tableInfo
  ) throws SQLException {
    List<JdbcColumnInfo> primaryKeyColumns = primaryKeyColumnsMap.get(tableInfo);

    if (primaryKeyColumns == null) {
      Collection<String> primaryKeyNames =
          JdbcQueryUtil.fetchPrimaryKeyNames(dataSource, tableInfo);

      // TODO: Do we need to verify the PK exists in the list of columns?
      primaryKeyColumns =
          fetchAllColumns(tableInfo)
              .stream()
              .filter(column -> primaryKeyNames.contains(column.getName()))
              .collect(Collectors.toList());

      primaryKeyColumnsMap.put(tableInfo, primaryKeyColumns);
    }
    return primaryKeyColumns;
  }
}

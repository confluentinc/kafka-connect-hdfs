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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

class SimpleSqlCache {
  private final Map<String, Map<?, ?>> cacheMap = new HashMap<>();

  @SuppressWarnings("unchecked")
  public <K, V> V computeIfAbsent(String category,
                                  K key,
                                  SqlMethod.Function<K, V> sqlFn) throws SQLException {
    final Map<K, V> jdbcMap;
    synchronized (cacheMap) {
      jdbcMap = (Map<K, V>) cacheMap.computeIfAbsent(category, __ -> new HashMap<>());
    }

    // TODO: Have a single level cache
    synchronized (jdbcMap) {
      if (jdbcMap.containsKey(key)) {
        return jdbcMap.get(key);
      }

      V value = sqlFn.apply(key);
      jdbcMap.put(key, value);

      return value;
    }
  }
}

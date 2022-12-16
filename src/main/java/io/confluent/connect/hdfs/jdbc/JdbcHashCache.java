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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Cache of LOB hashes, for pruning unnecessary HDFS writes
 */
public class JdbcHashCache {

  private final Map<JdbcTableInfo, LinkedHashMap<String, Map<String, MD5Hash>>>
      tablePkColumnCache = new HashMap<>();

  private final int maxHashSize;

  public JdbcHashCache(int maxHashSize) {
    this.maxHashSize = maxHashSize;
  }

  /**
   * Update cache; and increment change-counter if hash has changed
   */
  public <T> boolean updateCache(JdbcTableInfo tableInfo,
                                 String primaryKey,
                                 String columnName,
                                 T value,
                                 Function<T, MD5Hash> hashFn
  ) {
    // Get the Cache for the given Table + PK
    Map<String, Map<String, MD5Hash>> pkColumnMd5Cache = getPkColumnCache(tableInfo);
    Map<String, MD5Hash> columnMd5Cache = pkColumnMd5Cache.computeIfAbsent(
        primaryKey,
        __ -> new HashMap<>()
    );

    MD5Hash newHash = (value != null) ? hashFn.apply(value) : new MD5Hash();

    // Check the hash against the cache
    MD5Hash oldHash = columnMd5Cache.put(columnName, newHash);
    return !Objects.equals(newHash, oldHash);
  }

  private LinkedHashMap<String, Map<String, MD5Hash>> getPkColumnCache(
      JdbcTableInfo tableInfo
  ) {
    // The entire tablePkColumnMd5Cache map structure is accessed serially,
    // so no need to make it threadsafe
    return tablePkColumnCache.computeIfAbsent(
        tableInfo,
        __ -> new LinkedHashMap<String, Map<String, MD5Hash>>(
            100,
            .75f,
            true
        ) {
          // NOTE: Turns LinkedHashMap into an LRU Cache
          @Override
          protected boolean removeEldestEntry(
              Map.Entry<String, Map<String, MD5Hash>> eldest
          ) {
            return this.size() > maxHashSize;
          }
        }
    );
  }
}

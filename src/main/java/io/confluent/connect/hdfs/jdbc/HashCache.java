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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Cache of LOB hashes, for pruning unnecessary HDFS writes;
 * NOTE: None of this is synchronized, as it expects to be called from non-parallel code.
 * NOTE: MD5 Hashing takes about 1-2 seconds per gig, at least locally
 */
public class HashCache {
  private static final byte[] EMPTY_HASH = {};

  private final Map<JdbcTableInfo, LinkedHashMap<String, Map<String, byte[]>>>
      tablePkColumnCache = new HashMap<>();

  private final int maxHashSize;
  private final MessageDigest messageDigest;

  public HashCache(int maxHashSize, MessageDigest messageDigest) {
    this.maxHashSize = maxHashSize;
    this.messageDigest = messageDigest;
  }

  public boolean updateCache(JdbcTableInfo tableInfo,
                             String primaryKey,
                             String columnName,
                             String value
  ) {
    byte[] bytes = Optional
        .ofNullable(value)
        // TODO: Should we hardcode UTF_8 here?
        .map(value_ -> value_.getBytes(StandardCharsets.UTF_8))
        .orElse(null);

    return updateCache(tableInfo, primaryKey, columnName, bytes);
  }

  /**
   * Update cache; and return true if hash has changed
   */
  public boolean updateCache(JdbcTableInfo tableInfo,
                             String primaryKey,
                             String columnName,
                             byte[] value
  ) {
    byte[] hash = Optional
        .ofNullable(value)
        .map(bytes -> {
          messageDigest.reset();
          return messageDigest.digest(bytes);
        })
        .orElse(EMPTY_HASH);

    // Get the Cache for the given Table + PK
    Map<String, Map<String, byte[]>> pkColumnHashCache = getPkColumnCache(tableInfo);
    Map<String, byte[]> columnHashCache = pkColumnHashCache.computeIfAbsent(
        primaryKey,
        __ -> new HashMap<>()
    );

    // Check the hash against the cache
    byte[] oldHash = columnHashCache.put(columnName, hash);
    return !Arrays.equals(hash, oldHash);
  }

  private LinkedHashMap<String, Map<String, byte[]>> getPkColumnCache(
      JdbcTableInfo tableInfo
  ) {
    // The entire tablePkColumnCache map structure is accessed serially,
    // so no need to make it threadsafe
    return tablePkColumnCache.computeIfAbsent(
        tableInfo,
        __ -> new LinkedHashMap<String, Map<String, byte[]>>(
            100,
            .75f,
            true
        ) {
          // NOTE: Turns LinkedHashMap into an LRU Cache
          @Override
          protected boolean removeEldestEntry(
              Map.Entry<String, Map<String, byte[]>> eldest
          ) {
            return this.size() > maxHashSize;
          }
        }
    );
  }
}

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
import java.util.function.Predicate;

public class JdbcUtil {
  public static Optional<String> trimToNone(String value) {
    return Optional
        .ofNullable(value)
        .map(String::trim)
        .filter(((Predicate<String>) String::isEmpty).negate());
  }

  public static String trimToNull(String value) {
    return trimToNone(value)
        .orElse(null);
  }

  public static String toUpperCase(String value) {
    return Optional
        .ofNullable(value)
        .map(String::toUpperCase)
        .orElse(null);
  }
}

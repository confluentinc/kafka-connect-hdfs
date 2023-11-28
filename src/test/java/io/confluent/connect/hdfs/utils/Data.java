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

package io.confluent.connect.hdfs.utils;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Data {
  private static final Map<String, List<Object>> data = new HashMap<>();

  private static final Logger log = LoggerFactory.getLogger(Data.class);

  public static Map<String, List<Object>> getData() {
    return data;
  }

  public static void logContents(String message) {
    if (log.isDebugEnabled()) {
      log.debug("{}: {}",
          message,
          data.entrySet()
              .stream()
              .map(e -> e.getKey()
                      + "="
                      + (
                      e.getValue() != null
                          ? (e.getValue()
                          .stream()
                          .map(Object::toString)
                          .collect(Collectors.joining(",\n\t", "[\n\t", "]")))
                          : "null"
                  )
              )
              .collect(Collectors.joining(",\n")));
    }
  }
}

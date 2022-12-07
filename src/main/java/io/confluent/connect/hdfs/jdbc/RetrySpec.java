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

import java.time.Duration;

public class RetrySpec {
  public static final RetrySpec NoRetries =
      new RetrySpec(0, null);

  private final int maxRetries;
  private final Duration backoff;

  public RetrySpec(int maxRetries, Duration backoff) {
    this.maxRetries = Math.max(0, maxRetries);
    this.backoff = backoff;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public Duration getBackoff() {
    return backoff;
  }
}

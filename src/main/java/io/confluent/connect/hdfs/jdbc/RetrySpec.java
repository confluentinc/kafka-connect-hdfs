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

import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class RetrySpec {
  private static final Logger log = LoggerFactory.getLogger(RetrySpec.class);

  public static final RetrySpec NoRetries =
      new RetrySpec(1, null);

  private final int maxAttempts;
  private final Duration backoff;

  public RetrySpec(int maxAttempts, Duration backoff) {
    this.maxAttempts = Math.max(1, maxAttempts);
    this.backoff = backoff;
  }

  public <T> T executeWithRetries(Supplier<T> supplier) {
    int timesAttempted = 0;

    while (true) {
      try {
        return supplier.get();
      } catch (RetriableException | org.apache.kafka.common.errors.RetriableException ex) {
        if (++timesAttempted >= maxAttempts) {
          log.error("Caught RetriableException but no more retries: {}", ex.getMessage(), ex);
          throw ex;
        }

        log.warn(
            "Caught RetriableException [attempts {}/{}]; will retry in [{}]: {}",
            timesAttempted,
            maxAttempts,
            backoff,
            ex.getMessage(),
            ex
        );

        sleep(backoff);
      } catch (RuntimeException ex) {
        log.error("Caught non-retriable SQLException: {}", ex.getMessage(), ex);
        throw ex;
      }
    }
  }

  @Override
  public String toString() {
    return "RetrySpec{"
           + "maxAttempts="
           + maxAttempts
           + ", backoff="
           + backoff
           + "}";
  }

  private static void sleep(Duration duration) {
    long durationMillis =
        Optional
            .ofNullable(duration)
            .filter(((Predicate<Duration>) Duration::isNegative).negate())
            .map(Duration::toMillis)
            .orElse(0L);

    if (durationMillis > 0) {
      try {
        Thread.sleep(durationMillis);
      } catch (InterruptedException e) {
        // this is okay, we just wake up early
        Thread.currentThread().interrupt();
      }
    }
  }
}

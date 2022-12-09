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
    int attempt = 0;

    while (true) {
      attempt++;
      try {
        log.trace(
            "Starting execution for [{}] attempt [{}/{}]",
            supplier.hashCode(),
            attempt,
            maxAttempts
        );
        T t = supplier.get();
        log.trace(
            "Successfully completed execution for [{}] attempt [{}/{}]",
            supplier.hashCode(),
            attempt,
            maxAttempts
        );
        return t;
      } catch (RetriableException | org.apache.kafka.common.errors.RetriableException ex) {
        if (attempt >= maxAttempts) {
          log.error(
              "Caught RetriableException for [{}] attempt [{}/{}]; no more retries: {}",
              supplier.hashCode(),
              attempt,
              maxAttempts,
              ex.getMessage(),
              ex
          );
          throw ex;
        }

        log.warn(
            "Caught RetriableException for [{}] attempt [{}/{}]; will retry in [{}]: {}",
            supplier.hashCode(),
            attempt,
            maxAttempts,
            backoff,
            ex.getMessage(),
            ex
        );

        sleep(backoff);
      } catch (RuntimeException ex) {
        log.error(
            "Caught non-retriable Exception for [{}] attempt [{}/{}]: {}",
            supplier.hashCode(),
            attempt,
            maxAttempts,
            ex.getMessage(),
            ex
        );
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

/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.hdfs;

import org.joda.time.DateTimeZone;

public class DateTimeUtils {

  private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;

  /**
   * Calculates next period of periodMs after currentTimeMs starting from midnight in given
   * timeZone.
   * If the next period is in next day then 12am of next day will be returned
   *
   * @param currentTimeMs time to calculate at
   * @param periodMs period in ms
   * @param timeZone timezone to get midnight time
   * @return timestamp in ms
   */
  public static long getNextTimeAdjustedByDay(
      long currentTimeMs,
      long periodMs,
      DateTimeZone timeZone
  ) {
    long startOfDay =
        timeZone.convertLocalToUTC(
            timeZone.convertUTCToLocal(currentTimeMs) / DAY_IN_MS * DAY_IN_MS,
            true
        );
    long nextPeriodOffset = ((currentTimeMs - startOfDay) / periodMs + 1) * periodMs;
    long offset = Math.min(nextPeriodOffset, DAY_IN_MS);
    return startOfDay + offset;
  }
}

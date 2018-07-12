/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.hdfs.partitioner;

import static org.junit.Assert.assertEquals;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import io.confluent.connect.hdfs.HdfsSinkConnectorTestBase;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class TimeBasedPartitionerTest extends HdfsSinkConnectorTestBase {
  private static final String TIME_ZONE_STRING = "America/Los_Angeles";
  private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(TIME_ZONE_STRING);

  @Test
  public void testGeneratePartitionedPath() throws Exception {
    setUp();
    BiHourlyPartitioner partitioner = new BiHourlyPartitioner();
    partitioner.configure(propsWithDefaults);
    String pathFormat = partitioner.getPathFormat();
    long partitionDurationMs = TimeUnit.HOURS.toMillis(2);
    long timestamp = new DateTime(2015, 1, 1, 3, 0, 0, 0, DATE_TIME_ZONE).getMillis();
    String encodedPartition = TimeUtils.encodeTimestamp(partitionDurationMs, pathFormat,
        TIME_ZONE_STRING, timestamp);
    String path = partitioner.generatePartitionedPath("topic", encodedPartition);
    assertEquals("topic/year=2015/month=January/day=01/hour=2/", path);
  }

  @Test
  public void testDaylightSavingTime() {
    DateTime time = new DateTime(2015, 11, 1, 2, 1, DATE_TIME_ZONE);
    String pathFormat = "'year='YYYY/'month='MMMM/'day='dd/'hour='H/";
    DateTimeFormatter formatter = DateTimeFormat.forPattern(pathFormat).withZone(DATE_TIME_ZONE);
    long utc1 = DATE_TIME_ZONE.convertLocalToUTC(time.getMillis() - TimeUnit.MINUTES.toMillis(60), false);
    long utc2 = DATE_TIME_ZONE.convertLocalToUTC(time.getMillis() - TimeUnit.MINUTES.toMillis(120), false);
    DateTime time1 = new DateTime(DATE_TIME_ZONE.convertUTCToLocal(utc1));
    DateTime time2 = new DateTime(DATE_TIME_ZONE.convertUTCToLocal(utc2));
    assertEquals(time1.toString(formatter), time2.toString(formatter));
  }

  private static class BiHourlyPartitioner extends TimeBasedPartitioner {
    private static String partitionDurationMs = String.valueOf(TimeUnit.HOURS.toMillis(2));
    private static String pathFormat = "'year'=YYYY/'month'=MMMM/'day'=dd/'hour'=H/";

    @Override
    public void configure(Map<String, String> props) {
      // Do not assume configure()'s superclasses implementations
      props.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, partitionDurationMs);
      props.put(PartitionerConfig.PATH_FORMAT_CONFIG, pathFormat);
      props.put(PartitionerConfig.LOCALE_CONFIG, Locale.FRENCH.toString());
      props.put(PartitionerConfig.TIMEZONE_CONFIG, TIME_ZONE_STRING);
      super.configure(props);
    }

    @Override
    public String getPathFormat() {
      return pathFormat;
    }
  }
}

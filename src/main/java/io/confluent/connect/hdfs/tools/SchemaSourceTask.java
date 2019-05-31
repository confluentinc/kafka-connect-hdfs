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

package io.confluent.connect.hdfs.tools;


import java.math.BigInteger;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SchemaSourceTask extends SourceTask {
  public static final String NAME_CONFIG = "name";
  public static final String ID_CONFIG = "id";
  public static final String TOPIC_CONFIG = "topic";
  public static final String NUM_MSGS_CONFIG = "num.messages";
  public static final String THROUGHPUT_CONFIG = "throughput";
  public static final String MULTIPLE_SCHEMA_CONFIG = "multiple.schema";
  public static final String PARTITION_COUNT_CONFIG = "partition.count";
  private static final Logger log = LoggerFactory.getLogger(SchemaSourceTask.class);
  private static final String ID_FIELD = "id";
  private static final String SEQNO_FIELD = "seqno";
  private static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";
  private static Schema valueSchema = SchemaBuilder.struct().version(1).name("record")
      .field("date", Date.builder().build())
      .field("decimalwithprecision", Decimal.builder(2)
          .parameter(CONNECT_AVRO_DECIMAL_PRECISION_PROP, "64").build())
      .field("decimalwithoutprecision", Decimal.builder(2).build())
      .field("time", Time.builder().build())
      .field("timestamp", Timestamp.builder().build())
      .build();
  private static Schema valueSchema2 = SchemaBuilder.struct().version(2).name("record")
      .field("date", Date.builder().build())
      .field("decimalwithprecision", Decimal.builder(2)
          .parameter(CONNECT_AVRO_DECIMAL_PRECISION_PROP, "64").build())
      .field("decimalwithoutprecision", Decimal.builder(2).build())
      .field("time", Time.builder().build())
      .field("timestamp", Timestamp.builder().build())
      .field("string", SchemaBuilder.string().defaultValue("abc").build())
      .build();
  private String name; // Connector name
  private int id; // Task ID
  private String topic;
  private Map<String, Integer> partition;
  private long startingSeqno;
  private long seqno;
  private long count;
  private long maxNumMsgs;
  private boolean multipleSchema;
  private int partitionCount;
  // Until we can use ThroughputThrottler from Kafka, use a fixed sleep interval. This isn't
  // perfect, but close enough
  // for system testing purposes
  private long intervalMs;
  private int intervalNanos;

  public String version() {
    return new SchemaSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    try {
      name = props.get(NAME_CONFIG);
      id = Integer.parseInt(props.get(ID_CONFIG));
      topic = props.get(TOPIC_CONFIG);
      maxNumMsgs = Long.parseLong(props.get(NUM_MSGS_CONFIG));
      multipleSchema = Boolean.parseBoolean(props.get(MULTIPLE_SCHEMA_CONFIG));
      partitionCount = Integer.parseInt(
          props.containsKey(PARTITION_COUNT_CONFIG) ? props.get(PARTITION_COUNT_CONFIG) : "1"
      );
      String throughputStr = props.get(THROUGHPUT_CONFIG);
      if (throughputStr != null) {
        long throughput = Long.parseLong(throughputStr);
        long intervalTotalNanos = 1_000_000_000L / throughput;
        intervalMs = intervalTotalNanos / 1_000_000L;
        intervalNanos = (int) (intervalTotalNanos % 1_000_000L);
      } else {
        intervalMs = 0;
        intervalNanos = 0;
      }
    } catch (NumberFormatException e) {
      throw new ConnectException("Invalid SchemaSourceTask configuration", e);
    }

    partition = Collections.singletonMap(ID_FIELD, id);
    Map<String, Object> previousOffset = this.context.offsetStorageReader().offset(partition);
    if (previousOffset != null) {
      seqno = (Long) previousOffset.get(SEQNO_FIELD) + 1;
    } else {
      seqno = 0;
    }
    startingSeqno = seqno;
    count = 0;
    log.info(
        "Started SchemaSourceTask {}-{} producing to topic {} resuming from seqno {}",
        name,
        id,
        topic,
        startingSeqno
    );
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (count < maxNumMsgs) {
      if (intervalMs > 0 || intervalNanos > 0) {
        synchronized (this) {
          this.wait(intervalMs, intervalNanos);
        }
      }

      Map<String, Long> ccOffset = Collections.singletonMap(SEQNO_FIELD, seqno);
      int partitionVal = (int) (seqno % partitionCount);
      final Struct data;
      final SourceRecord srcRecord;
      byte[] bytes = new BigInteger("1220").toByteArray();
      if (!multipleSchema || count % 2 == 0) {
        data = new Struct(valueSchema)
            .put("date", Date.toLogical(valueSchema.field("date").schema(), 12))
            .put("decimalwithprecision", Decimal.toLogical(
                valueSchema.field("decimalwithprecision").schema(), bytes))
            .put("decimalwithoutprecision", Decimal.toLogical(
                valueSchema.field("decimalwithoutprecision").schema(), bytes))
            .put("time", Time.toLogical(valueSchema.field("time").schema(), 12))
            .put("timestamp", Timestamp.toLogical(valueSchema.field("timestamp").schema(), 12L));

        srcRecord = new SourceRecord(
            partition,
            ccOffset,
            topic,
            id,
            Schema.STRING_SCHEMA,
            "key",
            valueSchema,
            data
        );
      } else {
        data = new Struct(valueSchema2)
            .put("date", Date.toLogical(valueSchema2.field("date").schema(), 12))
            .put("decimalwithprecision", Decimal.toLogical(
                valueSchema2.field("decimalwithprecision").schema(),bytes))
            .put("decimalwithoutprecision", Decimal.toLogical(
                valueSchema2.field("decimalwithoutprecision").schema(), bytes))
            .put("time", Time.toLogical(valueSchema2.field("time").schema(), 12))
            .put("timestamp", Timestamp.toLogical(valueSchema2.field("timestamp").schema(), 12L))
            .put("string", "def");

        srcRecord = new SourceRecord(
            partition,
            ccOffset,
            topic,
            id,
            Schema.STRING_SCHEMA,
            "key",
            valueSchema2,
            data
        );
      }

      System.out.println("{\"task\": " + id + ", \"seqno\": " + seqno + "}");
      List<SourceRecord> result = Arrays.asList(srcRecord);
      seqno++;
      count++;
      return result;
    } else {
      synchronized (this) {
        this.wait();
      }
      return new ArrayList<>();
    }
  }

  @Override
  public void stop() {
    synchronized (this) {
      this.notifyAll();
    }
  }
}

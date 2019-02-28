/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.partitioner;

import io.confluent.connect.hdfs.HdfsSinkConnectorTestBase;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.Assert.assertEquals;

public class SchemaAwarePartitionerDecoratorTest extends HdfsSinkConnectorTestBase {

  @Test
  public void testGeneratePartitionedPath() throws Exception {
    setUp();
    SchemaAwarePartitionerDecorator partitioner = new SchemaAwarePartitionerDecorator(new InternalPartitioner());
    partitioner.configure(parsedConfig);
    Schema schema = createSchema();
    SinkRecord record = new SinkRecord(TOPIC, 0, STRING_SCHEMA, "key", schema, createRecord(schema, 16, 12), 0);
    String paritionEncoded = partitioner.encodePartition(record);
    assertEquals(schema.name() + "/time=TEST", paritionEncoded);
  }

  private static class InternalPartitioner extends TimeBasedPartitioner {

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
      return "time=TEST";
    }
  }
}

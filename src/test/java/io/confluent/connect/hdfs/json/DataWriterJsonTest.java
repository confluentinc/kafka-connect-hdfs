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

package io.confluent.connect.hdfs.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;

import static org.junit.Assert.assertEquals;

public class DataWriterJsonTest extends TestWithMiniDFSCluster {

  private JsonConverter converter;
  protected final ObjectMapper mapper = new ObjectMapper();

  @Before
  public void setUp() throws Exception {
    super.setUp();
    converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    dataFileReader = new JsonDataFileReader();
    extension = ".json";
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    return props;
  }

  @Test
  public void testWithSchema() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecords(7, 0, context.assignment());

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  @Test
  public void testNoSchema() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createJsonRecordsWithoutSchema(
        7 * context.assignment().size(),
        0,
        context.assignment()
    );

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  protected List<SinkRecord> createJsonRecordsWithoutSchema(
      int size,
      long startOffset,
      Set<TopicPartition> partitions
  ) {
    String key = "key";
    int ibase = 12;

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition tp : partitions) {
        String record = "{\"schema\":{\"type\":\"struct\",\"fields\":[ "
            + "{\"type\":\"boolean\",\"optional\":true,\"field\":\"booleanField\"},"
            + "{\"type\":\"int32\",\"optional\":true,\"field\":\"intField\"},"
            + "{\"type\":\"int64\",\"optional\":true,\"field\":\"longField\"},"
            + "{\"type\":\"string\",\"optional\":false,\"field\":\"stringField\"}],"
            + "\"payload\":"
            + "{\"booleanField\":\"true\","
            + "\"intField\":" + String.valueOf(ibase) + ","
            + "\"longField\":" + String.valueOf((long) ibase) + ","
            + "\"stringField\":str" + String.valueOf(ibase)
            + "}}";
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), null, key, null, record, offset));
        if (++total >= size) {
          break;
        }
      }
    }
    return sinkRecords;
  }

  @Override
  protected void verifyContents(
      List<SinkRecord> expectedRecords,
      int startIndex,
      Collection<Object> records
  ) {
    for (Object jsonRecord : records) {
      SinkRecord expectedRecord = expectedRecords.get(startIndex++);
      Object expectedValue = expectedRecord.value();
      try {
        if (expectedValue instanceof Struct) {
          byte[] expectedBytes = converter.fromConnectData(
              TOPIC,
              expectedRecord.valueSchema(),
              expectedRecord.value()
          );
          expectedValue = mapper.readValue(expectedBytes, Object.class);
        }
        assertEquals(expectedValue, jsonRecord);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.connect.hdfs.string;

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class DataWriterStringTest extends TestWithMiniDFSCluster {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    dataFileReader = new StringDataFileReader();
    extension = ".txt";
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, StringFormat.class.getName());
    return props;
  }

  @Test
  public void testReadString() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createStringRecords(
        7 * context.assignment().size(),
        context.assignment()
    );

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment());
  }

  protected List<SinkRecord> createStringRecords(
      int size,
      Set<TopicPartition> partitions
  ) {
    String key = "key";

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = 0, total = 0; total < size; ++offset) {
      for (TopicPartition tp : partitions) {
        String record = "Some random text...";
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
    for (Object record : records) {
      SinkRecord expectedRecord = expectedRecords.get(startIndex++);
      Object expectedValue = expectedRecord.value();
      assertEquals(expectedValue, record);
    }
  }
}

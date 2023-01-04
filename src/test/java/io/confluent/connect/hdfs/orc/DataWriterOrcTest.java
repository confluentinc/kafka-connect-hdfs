/*
 * Copyright 2020 Confluent Inc.
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
 */

package io.confluent.connect.hdfs.orc;

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.hdfs.schema.HiveSchemaConverterWithLogicalTypes;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataWriterOrcTest extends TestWithMiniDFSCluster {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    dataFileReader = new OrcDataFileReader();
    extension = ".orc";
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, OrcFormat.class.getName());
    return props;
  }

  @Test
  public void testWriteRecord() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecords(7);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Override
  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
    Schema expectedSchema = null;
    for (Object orcRecord : records) {
      if (expectedSchema == null) {
        expectedSchema = expectedRecords.get(startIndex).valueSchema();
      }
      Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
        expectedRecords.get(startIndex++).value(),
        expectedSchema);

      TypeInfo typeInfo = HiveSchemaConverterWithLogicalTypes.convert(expectedSchema);

      ArrayList<Object> objs = new ArrayList<>();
      for (Field field : expectedSchema.fields()) {
        objs.add(((Struct) expectedValue).get(field));
      }

      expectedValue = OrcUtil.createOrcStruct(typeInfo, objs.toArray(new Object[0]));

      assertEquals(expectedValue.toString(), orcRecord.toString());
    }
  }

}

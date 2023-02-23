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
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.kafka.common.TopicPartition;
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
    writeAndVerify(createSinkRecords(7));
  }

  @Test
  public void testWriteNestedRecord() throws Exception {
    Struct struct = createNestedStruct();
    writeAndVerify(createSinkRecords(Collections.nCopies(7, struct), struct.schema()));
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

      TypeInfo typeInfo = HiveSchemaConverter.convertMaybeLogical(expectedSchema);

      OrcStruct orcStruct = (OrcStruct) OrcUtil.convert(typeInfo, expectedSchema, expectedValue);

      assertEquals(orcStruct.toString(), orcRecord.toString());
    }
  }

}

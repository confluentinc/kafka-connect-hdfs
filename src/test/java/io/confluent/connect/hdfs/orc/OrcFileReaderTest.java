package io.confluent.connect.hdfs.orc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

public class OrcFileReaderTest extends TestWithMiniDFSCluster {

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
  public void testSchemaRead() throws Exception {
    writeAndVerify(createSinkRecords(7));
  }

  @Test
  public void testSchemaReadWithNestedStruct() throws Exception {
    Struct struct = createNestedStruct();
    writeAndVerify(createSinkRecords(Collections.nCopies(7, struct), struct.schema()));
  }

  @Override
  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets,
      Set<TopicPartition> partitions, boolean skipFileListing)
      throws IOException {

    if (!skipFileListing) {
      verifyFileListing(validOffsets, partitions);
    }

    for (TopicPartition tp : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        long endOffset = validOffsets[i] - 1;

        String topicsDir = this.topicsDir.get(tp.topic());
        String filename = FileUtils.committedFileName(url, topicsDir,
            getDirectory(tp.topic(), tp.partition()), tp,
            startOffset, endOffset, extension, zeroPadFormat);
        Path path = new Path(filename);

        Schema fileSchema = new OrcFileReader().getSchema(connectorConfig, path);
        String hiveSchema = HiveSchemaConverter.convertMaybeLogical(fileSchema).toString();
        String connectSchema = HiveSchemaConverter
            .convertMaybeLogical(sinkRecords.get(0).valueSchema()).toString();
        assertEquals(hiveSchema, connectSchema);
      }
    }

  }
}

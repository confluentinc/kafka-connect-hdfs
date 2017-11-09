package io.confluent.connect.hdfs.avro;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.TestWithMiniDFSCluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AvroCompressionTest extends TestWithMiniDFSCluster {

  private static final Logger log = LoggerFactory.getLogger(AvroCompressionTest.class);

  @Before
  public void setUp() throws Exception {
    //set compression codec to Snappy
    this.localProps.put(HdfsSinkConnectorConfig.AVRO_CODEC_CONFIG, "snappy");

    super.setUp();
    dataFileReader = new AvroDataFileReader();
    extension = ".avro";
  }

  @Test
  public void testAvroCompression() throws Exception {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    partitioner = hdfsWriter.getPartitioner();
    hdfsWriter.recover(TOPIC_PARTITION);

    List<SinkRecord> sinkRecords = createSinkRecords(7);

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);

    // check if the raw bytes have a "avro.codec" entry followed by "snappy"
    List<String> filename = getExpectedFiles(validOffsets, TOPIC_PARTITION);
    for (String s : filename) {
      Path p = new Path(s);
      try (FSDataInputStream stream = fs.open(p)) {
        int size = (int) fs.getFileStatus(p).getLen();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        if (stream.read(buffer) <= 0) {
          log.error("Could not read file {}", s);
        }

        String deserialized = new String(buffer.array());
        int ix;
        assertTrue((ix = deserialized.indexOf("avro.codec")) > 0
            && deserialized.indexOf("snappy", ix) > 0);
      }
    }
  }

}

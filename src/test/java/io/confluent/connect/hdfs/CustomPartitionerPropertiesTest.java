package io.confluent.connect.hdfs;

import java.util.Map;

import io.confluent.connect.hdfs.avro.AvroDataFileReader;
import io.confluent.connect.hdfs.hive.HiveTestBase;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test to ensure we can still instantiate & use the old-style HDFS-only interfaces instead of
 * those from storage-common and use them with DataWriter
 */
public class CustomPartitionerPropertiesTest extends HiveTestBase {

  /**
   * Extend the deprecated {@link io.confluent.connect.hdfs.partitioner.DefaultPartitioner}
   * so that the {@link DataWriter} doesn't wrap it.
   */
  public static final class CustomPartitioner
      extends io.confluent.connect.hdfs.partitioner.DefaultPartitioner {

    public static final String CUSTOM_PROPERTY = "custom.property";
    public static final String EXPECTED_VALUE = "expectThis";

    String customValue;

    @Override
    public void configure(Map<String, Object> config) {
      super.configure(config);
      this.customValue = (String) config.get(CUSTOM_PROPERTY);
    }

    public String customValue() {
      return this.customValue;
    }
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
    props.put(CustomPartitioner.CUSTOM_PROPERTY, CustomPartitioner.EXPECTED_VALUE);
    return props;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    dataFileReader = new AvroDataFileReader();
    extension = ".avro";
  }

  @Test
  public void createDataWriterWithCustomPartitioner() {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
    Partitioner<?> partitioner = hdfsWriter.getPartitioner();
    assertEquals(CustomPartitioner.class.getName(), partitioner.getClass().getName());
    CustomPartitioner customPartitioner = (CustomPartitioner) partitioner;
    assertEquals(CustomPartitioner.EXPECTED_VALUE, customPartitioner.customValue());

    hdfsWriter.close();
    hdfsWriter.stop();
  }
}

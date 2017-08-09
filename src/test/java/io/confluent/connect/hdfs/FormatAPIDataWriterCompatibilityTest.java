package io.confluent.connect.hdfs;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import io.confluent.connect.hdfs.avro.AvroDataFileReader;
import io.confluent.connect.hdfs.hive.HiveTestBase;
import io.confluent.connect.storage.hive.HiveConfig;

/**
 * Test to ensure we can still instantiate & use the old-style HDFS-only interfaces instead of
 * those from storage-common and use them with DataWriter
 */
public class FormatAPIDataWriterCompatibilityTest extends HiveTestBase {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    dataFileReader = new AvroDataFileReader();
    extension = ".avro";
  }


  @Test
  public void dataWriterNewFormatAPICompatibilityTest() {
    DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);

    hdfsWriter.recover(TOPIC_PARTITION);

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);

    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = 0; offset < 7; offset++) {
      SinkRecord sinkRecord =
          new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);
      sinkRecords.add(sinkRecord);
    }

    hdfsWriter.write(sinkRecords);
    hdfsWriter.close();
    hdfsWriter.stop();

    Map<String, String> props = createProps();
    props.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
    HdfsSinkConnectorConfig config = new HdfsSinkConnectorConfig(props);

    hdfsWriter = new DataWriter(config, context, avroData);
    hdfsWriter.syncWithHive();

    // Since we're not using a real format, we won't validate the output. However, this should at
    // least exercise the code paths for the old Format class

    hdfsWriter.close();
    hdfsWriter.stop();
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, OldFormat.class.getName());
    // Enable Hive integration to make sure we exercise the paths that get HiveUtils
    props.put(HiveConfig.HIVE_INTEGRATION_CONFIG, "true");
    return props;
  }

}

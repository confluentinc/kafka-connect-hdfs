package io.confluent.connect.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.hdfs.avro.AvroDataFileReader;
import io.confluent.connect.hdfs.filter.CommittedFileFilter;
import io.confluent.connect.hdfs.partitioner.DefaultPartitioner;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test to ensure we can still instantiate & use the old-style HDFS-only interfaces instead of
 * those from storage-common and use them with TopicPartitionWriter
 */
public class FormatAPITopicPartitionWriterCompatibilityTest extends TestWithMiniDFSCluster {
  private RecordWriterProvider writerProvider = null;
  private io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig>
      newWriterProvider;
  private HdfsStorage storage;

  @Override
  protected Map<String, String> createProps() {
    return super.createProps();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    @SuppressWarnings("unchecked")
    Class<? extends HdfsStorage> storageClass = (Class<? extends HdfsStorage>)
        connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
    storage = StorageFactory.createStorage(
        storageClass,
        HdfsSinkConnectorConfig.class,
        connectorConfig,
        url
    );

    Format format = new OldFormat();
    writerProvider = format.getRecordWriterProvider();
    newWriterProvider = null;
    dataFileReader = new AvroDataFileReader();
    extension = writerProvider.getExtension();
    createTopicDir(url, topicsDir, TOPIC);
    createLogsDir(url, logsDir);
  }

  @Test
  public void testWriteRecordDefaultWithPadding() throws Exception {
    Partitioner partitioner = new DefaultPartitioner();
    partitioner.configure(parsedConfig);
    TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
        TOPIC_PARTITION,
        storage,
        writerProvider,
        newWriterProvider,
        partitioner,
        connectorConfig,
        context,
        avroData
    );

    Schema schema = createSchema();
    List<Struct> records = createRecordBatches(schema, 3, 3);
    // Add a single records at the end of the batches sequence. Total records: 10
    records.add(createRecord(schema));
    List<SinkRecord> sinkRecords = createSinkRecords(records, schema);

    for (SinkRecord record : sinkRecords) {
      topicPartitionWriter.buffer(record);
    }

    topicPartitionWriter.recover();
    topicPartitionWriter.write();
    topicPartitionWriter.close();

    // No verification since the format is a dummy format. We're really just trying to exercise
    // the old APIs and any paths that should hit them (and not NPE due to the variables for
    // new-style formats being null)
  }

  private void createTopicDir(String url, String topicsDir, String topic) throws IOException {
    Path path = new Path(FileUtils.topicDirectory(url, topicsDir, topic));
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  private void createLogsDir(String url, String logsDir) throws IOException {
    Path path = new Path(url + "/" + logsDir);
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

}

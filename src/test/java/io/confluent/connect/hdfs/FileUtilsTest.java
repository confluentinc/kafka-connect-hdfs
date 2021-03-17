package io.confluent.connect.hdfs;

import static org.junit.Assert.assertEquals;

import io.confluent.connect.hdfs.partitioner.DefaultPartitioner;
import org.junit.Test;

public class FileUtilsTest {
  @Test
  public void testExtractOffset() {
    assertEquals(1001, FileUtils.extractOffset("namespace.topic+1+1000+1001.avro"));
    assertEquals(1001, FileUtils.extractOffset("namespace.topic+1+1000+1001"));
    assertEquals(1001, FileUtils.extractOffset("namespace-topic_stuff.foo+1+1000+1001.avro"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractOffsetInvalid() {
    assertEquals(1001, FileUtils.extractOffset("namespace+topic+1+1000+1001.avro"));
  }

  @Test
  public void testTopicDataRootDirectory() {

    String url = "hdfs://localhost";
    String topicsDir = "kafka_data";
    String topic = "topic-a";

    assertEquals(
        url + "/" + topicsDir + "/" + topic,
        FileUtils.topicDataRootDirectory(url, topicsDir, topic, new DefaultPartitioner()));

    assertEquals(
        url + "/" + topicsDir,
        FileUtils.topicDataRootDirectory(url, topicsDir, topic, new CustomPathPartitioner()));

  }

  private static class CustomPathPartitioner extends DefaultPartitioner {

    static final String DIR = "custom_all_topics_dir";

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
      return DIR + this.delim + encodedPartition;
    }
  }
}

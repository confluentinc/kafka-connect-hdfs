package io.confluent.connect.hdfs.partitioner;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

import java.util.Map;

public abstract class BasePartitioner implements Partitioner {

  private boolean partitionIncludesTopic = true;

  /**
   * Returns true if the topic name should be part of the created folder in HDFS; false otherwise
   *
   * @return
   */
  protected boolean getPartitionIncludesTopic() {
    return partitionIncludesTopic;
  }

  @Override
  public void configure(Map<String, Object> config) {
    configureBase(config);
  }

  /**
   * This is because Hourly and Daily partitioner don't call the base class configure so we miss this(not ideal)
   * @param config
   */
  protected void configureBase(Map<String, Object> config) {
    partitionIncludesTopic = (boolean) config.get(HdfsSinkConnectorConfig.PARTITION_INCLUDE_TOPIC_NAME_CONFIG);
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
    if (partitionIncludesTopic) {
      return topic + "/" + encodedPartition;
    } else {
      return encodedPartition;
    }
  }
}

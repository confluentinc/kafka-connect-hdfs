package io.confluent.connect.hdfs.utils;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.AbstractConfig;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.hive.HiveFactory;

public class MemoryFormat
    implements io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> {

  // DO NOT change this signature, it is required for instantiation via reflection
  public MemoryFormat(HdfsStorage storage) {
  }

  @Override
  public io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig> getRecordWriterProvider() {
    return new MemoryRecordWriterProvider();
  }

  @Override
  public io.confluent.connect.storage.format.SchemaFileReader<HdfsSinkConnectorConfig, Path>
  getSchemaFileReader() {
    return null;
  }

  @Override
  public HiveFactory getHiveFactory() {
    return new HiveFactory() {
      @Override
      public io.confluent.connect.storage.hive.HiveUtil createHiveUtil(
          AbstractConfig abstractConfig,
          io.confluent.connect.storage.hive.HiveMetaStore hiveMetaStore
      ) {
        return null;
      }
    };
  }
}

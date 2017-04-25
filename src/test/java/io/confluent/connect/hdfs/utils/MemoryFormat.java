package io.confluent.connect.hdfs.utils;

import org.apache.kafka.common.config.AbstractConfig;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.storage.hive.HiveFactory;

public class MemoryFormat implements Format {

  @Override
  public RecordWriterProvider getRecordWriterProvider() {
    return new MemoryRecordWriterProvider();
  }

  @Override
  public SchemaFileReader getSchemaFileReader(AvroData avroData) {
    return null;
  }

  @Override
  public SchemaFileReader getSchemaFileReader() {
    return null;
  }

  @Override
  public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, HiveMetaStore hiveMetaStore) {
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

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
  public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, AvroData avroData, HiveMetaStore hiveMetaStore) {
    return null;
  }

  @Override
  public HiveFactory<? extends AbstractConfig, AvroData> getHiveFactory() {
    return new HiveFactory<HdfsSinkConnectorConfig, AvroData>() {
      @Override
      public io.confluent.connect.storage.hive.HiveUtil createHiveUtil(HdfsSinkConnectorConfig abstractConfig,
                                                                       AvroData avroData,
                                                                       io.confluent.connect.storage.hive.HiveMetaStore hiveMetaStore) {
        return null;
      }
    };
  }
}

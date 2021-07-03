package io.confluent.connect.hdfs.parquet;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.hive.HiveUtilTestBase;
import io.confluent.connect.storage.hive.HiveUtil;

import java.util.Map;

public class ParquetHiveUtilTest extends HiveUtilTestBase {

  public ParquetHiveUtilTest(String hiveTableNameConfig) {
    super(hiveTableNameConfig);
  }

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, ParquetFormat.class.getName());
    return props;
  }

  @Override
  protected HiveUtil createHiveUtil() {
    return new ParquetHiveUtil(connectorConfig, hiveMetaStore);
  }

}

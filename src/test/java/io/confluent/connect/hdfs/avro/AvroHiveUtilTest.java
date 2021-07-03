package io.confluent.connect.hdfs.avro;

import io.confluent.connect.hdfs.hive.HiveUtilTestBase;
import io.confluent.connect.hdfs.hive.HiveUtil;

public class AvroHiveUtilTest extends HiveUtilTestBase {

  public AvroHiveUtilTest(String hiveTableNameConfig) {
    super(hiveTableNameConfig);
  }

  @Override
  protected HiveUtil createHiveUtil() {
    return new AvroHiveUtil(connectorConfig, avroData, hiveMetaStore);
  }
}

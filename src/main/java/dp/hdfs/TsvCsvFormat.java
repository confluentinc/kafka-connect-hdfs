package dp.hdfs;


import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.avro.AvroHiveUtil;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;

public class TsvCsvFormat implements Format {
  public RecordWriterProvider getRecordWriterProvider() {
    return new LineWriterProvider();
  }

  public SchemaFileReader getSchemaFileReader(AvroData avroData) {
    return new LineFileReader(avroData);
  }

  public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, AvroData avroData, HiveMetaStore hiveMetaStore) {
    return new AvroHiveUtil(config, avroData, hiveMetaStore);
  }
}

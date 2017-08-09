package io.confluent.connect.hdfs.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import io.confluent.connect.hdfs.DataFileReader;

public class ParquetDataFileReader implements DataFileReader {
  @Override
  public Collection<Object> readData(Configuration conf, Path path) throws IOException {
    Collection<Object> result = new ArrayList<>();
    AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
    ParquetReader.Builder<GenericRecord> builder = ParquetReader.builder(readSupport, path);
    ParquetReader<GenericRecord> parquetReader = builder.withConf(conf).build();
    GenericRecord record;
    while ((record = parquetReader.read()) != null) {
      result.add(record);
    }
    parquetReader.close();
    return result;
  }
}

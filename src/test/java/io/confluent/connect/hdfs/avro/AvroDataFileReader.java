package io.confluent.connect.hdfs.avro;

import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import io.confluent.connect.hdfs.DataFileReader;

public class AvroDataFileReader implements DataFileReader {
  @Override
  public Collection<Object> readData(Configuration conf, Path path) throws IOException {
    ArrayList<Object> collection = new ArrayList<>();
    SeekableInput input = new FsInput(path, conf);
    DatumReader<Object> reader = new GenericDatumReader<>();
    FileReader<Object> fileReader = org.apache.avro.file.DataFileReader.openReader(input, reader);
    for (Object object: fileReader) {
      collection.add(object);
    }
    fileReader.close();
    return collection;
  }
}

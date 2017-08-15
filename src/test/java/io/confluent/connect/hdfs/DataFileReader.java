package io.confluent.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface that corresponds to SchemaFileReader but reads data objects. Only used to validate
 * output during tests.
 */
public interface DataFileReader {
  Collection<Object> readData(Configuration conf, Path path) throws IOException;
}

/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.connect.hdfs.string;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;

import java.util.Collection;
import java.util.Iterator;

/**
 * A no-op implementation of a schema reader for text files.
 */
public class StringFileReader implements SchemaFileReader<HdfsSinkConnectorConfig, Path> {
  @Override
  public Schema getSchema(HdfsSinkConnectorConfig conf, Path path) {
    return null;
  }

  public Collection<Object> readData(HdfsSinkConnectorConfig conf, Path path) {
    throw new UnsupportedOperationException();
  }

  public boolean hasNext() {
    throw new UnsupportedOperationException();
  }

  public Object next() {
    throw new UnsupportedOperationException();
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  public Iterator<Object> iterator() {
    throw new UnsupportedOperationException();
  }

  public void close() {}
}

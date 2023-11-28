/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.connect.hdfs.orc;

import io.confluent.connect.hdfs.DataFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class OrcDataFileReader implements DataFileReader {

  @Override
  public Collection<Object> readData(Configuration conf, Path path) throws IOException {
    ArrayList<Object> collection = new ArrayList<>();
    OrcFile.ReaderOptions readerOptions = new OrcFile.ReaderOptions(conf);
    Reader reader = OrcFile.createReader(path, readerOptions);

    RecordReader rows = reader.rows();

    Object row = null;
    while (rows.hasNext()) {
      row = rows.next(row);
      collection.add(row);
    }

    rows.close();

    return collection;
  }
}

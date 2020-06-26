/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.hdfs.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;

public class MemoryRecordWriterProvider
    implements io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig> {

  @Override
  public String getExtension() {
    return "";
  }

  @Override
  public io.confluent.connect.storage.format.RecordWriter getRecordWriter(
      HdfsSinkConnectorConfig conf,
      final String filename
  ) {
    final Map<String, List<Object>> data = Data.getData();

    // Overwrite the existing file in storage like every other writer provider
    data.put(filename, new LinkedList<>());

    return new MemoryRecordWriter(filename);
  }

}

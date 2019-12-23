/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs;

import io.confluent.connect.hdfs.storage.HdfsStorage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;

public class RecoveryHelper {

  public static final String RECOVERY_RECORD_KEY = "latestFilename";

  static RecoveryHelper instance = new RecoveryHelper();

  public static RecoveryHelper getInstance() {
    return instance;
  }

  private final ConcurrentHashMap<TopicPartition, List<String>> files = new ConcurrentHashMap<>();

  public List<String> getCommittedFiles(TopicPartition tp) {
    return files.get(tp);
  }

  public void addFile(TopicPartition tp, String name) {
    List<String> newList = new ArrayList<>();
    newList.add(name);
    files.merge(tp, newList, (a, b) -> {
      a.addAll(b);
      return a;
    });
  }

  public void addFile(String name) {
    TopicPartition tp = FileUtils.extractTopicPartition(new Path(name).getName());
    addFile(tp, name);
  }

  public static class RecoveryPoint {
    public final TopicPartition tp;
    public final long offset;
    public final String filename;

    public RecoveryPoint(TopicPartition tp, long offset, String filename) {
      this.tp = tp;
      this.offset = offset;
      this.filename = filename;
    }
  }

  public static RecoveryPoint getRecoveryPoint(TopicPartition tp, HdfsStorage storage) {
    if (getInstance().getCommittedFiles(tp) != null) {
      List<String> files = getInstance().getCommittedFiles(tp);
      files.sort(Comparator.comparing(a -> FileUtils.extractOffset(new Path(a).getName())));
      long maxOffset = -1;
      String latestFilename = null;

      // go backward from the latest file until we find a file that exists.
      for (int i = files.size() - 1; i >= 0; i--) {
        String filename = files.get(i);
        if (!storage.exists(filename)) {
          continue;
        }
        long endOffset = FileUtils.extractOffset(new Path(filename).getName());
        if (maxOffset < endOffset) {
          maxOffset = endOffset;
          latestFilename = filename;
        }
      }
      if (maxOffset > 0) {
        return new RecoveryPoint(tp, maxOffset, latestFilename);
      }
    }

    return null;
  }
}

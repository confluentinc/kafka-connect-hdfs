/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class SchemaTypeFilterDecorator extends CommittedFileFilter {
  private PathFilter pathFilter;
  private String schemaName;

  public SchemaTypeFilterDecorator(PathFilter pathFilter, String schemaName) {
    this.schemaName = schemaName;
    this.pathFilter = pathFilter;
  }

  @Override
  public boolean accept(Path path) {
    if (!pathFilter.accept(path)) {
      return false;
    }
    return path.getName().contains(schemaName);
  }
}

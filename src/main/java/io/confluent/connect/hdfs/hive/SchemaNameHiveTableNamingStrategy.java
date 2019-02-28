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

package io.confluent.connect.hdfs.hive;

public class SchemaNameHiveTableNamingStrategy implements HiveTableNamingStrategy {
  @Override
  public String createName(HiveTableNamingParameters parameters) {
    String schemaFullName = parameters.getSchema().name();
    int startIndex = schemaFullName.lastIndexOf('.');
    return schemaFullName.substring(startIndex == -1 ? 0 : startIndex).toLowerCase();
  }
}

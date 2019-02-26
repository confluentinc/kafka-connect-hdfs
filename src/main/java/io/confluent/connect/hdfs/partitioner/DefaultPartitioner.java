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

package io.confluent.connect.hdfs.partitioner;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Deprecated
@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_SUPERCLASS")
public class DefaultPartitioner
    extends io.confluent.connect.storage.partitioner.DefaultPartitioner<FieldSchema>
    implements Partitioner {
}

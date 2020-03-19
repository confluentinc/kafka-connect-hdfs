/*
 * Copyright 2017 Confluent Inc.
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

package io.confluent.connect.hdfs;

import io.confluent.connect.hdfs.parquet.ParquetFormat;
import io.confluent.connect.hdfs.string.StringFormat;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.hdfs.avro.AvroFormat;
import io.confluent.connect.hdfs.json.JsonFormat;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HdfsSinkConnectorConfigTest extends TestWithMiniDFSCluster {
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test(expected = ConfigException.class)
  public void testUrlConfigMustBeNonEmpty() {
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    properties.remove(HdfsSinkConnectorConfig.HDFS_URL_CONFIG);
    connectorConfig = new HdfsSinkConnectorConfig(properties);
  }

  @Test
  public void testStorageCommonUrlPreferred() {
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    assertEquals(url, connectorConfig.getUrl());
  }

  @Test
  public void testHdfsUrlIsValid() {
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    assertEquals(url, connectorConfig.getUrl());
  }

  @Test
  public void testStorageClass() throws Exception {
    // No real test case yet
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    assertEquals(
        HdfsStorage.class,
        connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG)
    );
  }

  @Test
  public void testUndefinedURL() throws Exception {
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    assertNull(connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG));
  }

  @Test
  public void testAvroCompressionSettings() {
    for (String codec : HdfsSinkConnectorConfig.AVRO_SUPPORTED_CODECS) {
      Map<String, String> props = new HashMap<>(this.properties);
      props.put(HdfsSinkConnectorConfig.AVRO_CODEC_CONFIG, codec);
      HdfsSinkConnectorConfig config = new HdfsSinkConnectorConfig(props);
      Assert.assertNotNull(config.getAvroCodec());
    }
  }

  @Test(expected = ConfigException.class)
  public void testUnsupportedAvroCompressionSettings() {
    // test for an unsupported codec.
    this.properties.put(HdfsSinkConnectorConfig.AVRO_CODEC_CONFIG, "abc");

    new HdfsSinkConnectorConfig(properties);
    Assert.assertTrue("Expected the constructor to throw an exception", false);
  }

  @Test
  public void testRecommendedValues() throws Exception {
    List<Object> expectedStorageClasses = Arrays.<Object>asList(HdfsStorage.class);

    List<Object> expectedFormatClasses = Arrays.<Object>asList(
        AvroFormat.class,
        JsonFormat.class,
        ParquetFormat.class,
        StringFormat.class
    );

    List<Object> expectedPartitionerClasses = Arrays.<Object>asList(
        DefaultPartitioner.class,
        HourlyPartitioner.class,
        DailyPartitioner.class,
        TimeBasedPartitioner.class,
        FieldPartitioner.class
    );

    List<ConfigValue> values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      if (val.value() instanceof Class) {
        switch (val.name()) {
          case StorageCommonConfig.STORAGE_CLASS_CONFIG:
            assertEquals(expectedStorageClasses, val.recommendedValues());
            break;
          case HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG:
            assertEquals(expectedFormatClasses, val.recommendedValues());
            break;
          case PartitionerConfig.PARTITIONER_CLASS_CONFIG:
            assertEquals(expectedPartitionerClasses, val.recommendedValues());
            break;
        }
      }
    }
  }

  @Test
  public void testVisibilityForPartitionerClassDependentConfigs() throws Exception {
    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
    List<ConfigValue> values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getName());
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
          assertTrue(val.visible());
          break;
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DailyPartitioner.class.getName());
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, HourlyPartitioner.class.getName());
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        TimeBasedPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    Partitioner<?> klass = new Partitioner<FieldSchema>() {
      @Override
      public void configure(Map<String, Object> config) {}

      @Override
      public String encodePartition(SinkRecord sinkRecord) {
        return null;
      }

      @Override
      public String generatePartitionedPath(String topic, String encodedPartition) {
        return null;
      }

      @Override
      public List<FieldSchema> partitionFields() {
        return null;
      }
    };

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        klass.getClass().getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }

  @Test
  public void testVisibilityForDeprecatedPartitionerClassDependentConfigs() throws Exception {
    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.DefaultPartitioner.class.getName()
    );
    List<ConfigValue> values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.FieldPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
          assertTrue(val.visible());
          break;
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.DailyPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.HourlyPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner.class.getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }

    io.confluent.connect.hdfs.partitioner.Partitioner klass =
        new io.confluent.connect.hdfs.partitioner.Partitioner() {
      @Override
      public void configure(Map<String, Object> config) {}

      @Override
      public String encodePartition(SinkRecord sinkRecord) {
        return null;
      }

      @Override
      public String generatePartitionedPath(String topic, String encodedPartition) {
        return null;
      }

      @Override
      public List<FieldSchema> partitionFields() {
        return null;
      }
    };

    properties.put(
        PartitionerConfig.PARTITIONER_CLASS_CONFIG,
        klass.getClass().getName()
    );
    values = HdfsSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }
}


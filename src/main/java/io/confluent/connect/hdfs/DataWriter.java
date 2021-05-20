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

import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveFactory;
import io.confluent.connect.storage.partitioner.PartitionerConfig;

public class DataWriter {
  private static final Logger log = LoggerFactory.getLogger(DataWriter.class);
  private static final Time SYSTEM_TIME = new SystemTime();
  private final Time time;

  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private HdfsStorage storage;
  private Format format;
  private RecordWriterProvider writerProvider;
  private io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig>
      newWriterProvider;
  private io.confluent.connect.storage.format.SchemaFileReader<HdfsSinkConnectorConfig, Path>
      schemaFileReader;
  private io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> newFormat;
  private Partitioner partitioner;
  private HdfsSinkConnectorConfig connectorConfig;
  private AvroData avroData;
  private SinkTaskContext context;
  private ExecutorService executorService;
  private String hiveDatabase;
  private HiveMetaStore hiveMetaStore;
  private HiveUtil hive;
  private Queue<Future<Void>> hiveUpdateFutures;
  private Thread ticketRenewThread;
  private volatile boolean isRunning;

  public DataWriter(
      HdfsSinkConnectorConfig connectorConfig,
      SinkTaskContext context,
      AvroData avroData
  ) {
    this(connectorConfig, context, avroData, SYSTEM_TIME);

  }

  @SuppressWarnings("unchecked")
  public DataWriter(
      HdfsSinkConnectorConfig config,
      SinkTaskContext context,
      AvroData avroData,
      Time time
  ) {
    this.time = time;
    this.avroData = avroData;
    this.context = context;
    connectorConfig = config;
    topicPartitionWriters = new HashMap<>();

    try {
      partitioner = newPartitioner(config);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new ConnectException(
          String.format("Unable to initialize partitioner: %s", e.getMessage()),
          e
      );
    }

    System.setProperty("hadoop.home.dir", config.hadoopHome());
    log.info("Hadoop configuration directory {}", config.hadoopConfDir());
    Configuration hadoopConfiguration = config.getHadoopConfiguration();
    if (!config.hadoopConfDir().equals("")) {
      hadoopConfiguration.addResource(new Path(config.hadoopConfDir() + "/core-site.xml"));
      hadoopConfiguration.addResource(new Path(config.hadoopConfDir() + "/hdfs-site.xml"));
    }

    if (config.kerberosAuthentication()) {
      configureKerberosAuthentication(hadoopConfiguration);
    }

    Class<? extends HdfsStorage> storageClass = config.storageClass();
    storage = io.confluent.connect.storage.StorageFactory.createStorage(
        storageClass,
        HdfsSinkConnectorConfig.class,
        config,
        connectorConfig.url()
    );

    try {
      // Try to instantiate as a new-style storage-common type class, then fall back to old-style
      // with no parameters
      try {
        Class<io.confluent.connect.storage.format.Format> formatClass =
            (Class<io.confluent.connect.storage.format.Format>)
                config.getClass(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG);
        newFormat = formatClass.getConstructor(HdfsStorage.class).newInstance(storage);
        newWriterProvider = newFormat.getRecordWriterProvider();
        schemaFileReader = newFormat.getSchemaFileReader();
      } catch (NoSuchMethodException e) {
        Class<Format> formatClass =
            (Class<Format>) config.getClass(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG);
        format = formatClass.getConstructor().newInstance();
        writerProvider = format.getRecordWriterProvider();
        final io.confluent.connect.hdfs.SchemaFileReader oldReader
            = format.getSchemaFileReader(avroData);
        schemaFileReader = new SchemaFileReader<HdfsSinkConnectorConfig, Path>() {
          @Override
          public Schema getSchema(HdfsSinkConnectorConfig hdfsSinkConnectorConfig, Path path) {
            try {
              return oldReader.getSchema(hdfsSinkConnectorConfig.getHadoopConfiguration(), path);
            } catch (IOException e) {
              throw new ConnectException("Failed to get schema", e);
            }
          }

          @Override
          public Iterator<Object> iterator() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean hasNext() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object next() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void close() throws IOException {

          }
        };
      }

    } catch (IllegalAccessException
              | InstantiationException
              | InvocationTargetException
              | NoSuchMethodException e
    ) {
      throw new ConnectException("Reflection exception: ", e);
    }

    if (connectorConfig.hiveIntegrationEnabled()) {
      initializeHiveServices(hadoopConfiguration);
    }
  }

  private void configureKerberosAuthentication(Configuration hadoopConfiguration) {
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS,
        hadoopConfiguration
    );

    if (connectorConfig.connectHdfsPrincipal() == null
        || connectorConfig.connectHdfsKeytab() == null) {
      throw new ConfigException(
          "Hadoop is using Kerberos for authentication, you need to provide both a connect "
              + "principal and the path to the keytab of the principal.");
    }

    hadoopConfiguration.set("hadoop.security.authentication", "kerberos");
    hadoopConfiguration.set("hadoop.security.authorization", "true");

    try {
      String hostname = InetAddress.getLocalHost().getCanonicalHostName();

      String namenodePrincipal = SecurityUtil.getServerPrincipal(
          connectorConfig.hdfsNamenodePrincipal(),
          hostname
      );

      // namenode principal is needed for multi-node hadoop cluster
      if (hadoopConfiguration.get("dfs.namenode.kerberos.principal") == null) {
        hadoopConfiguration.set("dfs.namenode.kerberos.principal", namenodePrincipal);
      }
      log.info("Hadoop namenode principal: {}",
          hadoopConfiguration.get("dfs.namenode.kerberos.principal"));

      UserGroupInformation.setConfiguration(hadoopConfiguration);
      // replace the _HOST specified in the principal config to the actual host
      String principal = SecurityUtil.getServerPrincipal(
          connectorConfig.connectHdfsPrincipal(),
          hostname
      );
      UserGroupInformation.loginUserFromKeytab(principal, connectorConfig.connectHdfsKeytab());
      final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      log.info("Login as: " + ugi.getUserName());

      isRunning = true;
      ticketRenewThread = new Thread(() -> renewKerberosTicket(ugi));
    } catch (UnknownHostException e) {
      throw new ConnectException(
          String.format(
              "Could not resolve local hostname for Kerberos authentication: %s",
              e.getMessage()
          ),
          e
      );
    } catch (IOException e) {
      throw new ConnectException(
          String.format("Could not authenticate with Kerberos: %s", e.getMessage()),
          e
      );
    }

    log.info(
        "Starting the Kerberos ticket renew thread with period {} ms.",
        connectorConfig.kerberosTicketRenewPeriodMs()
    );
    ticketRenewThread.start();
  }

  private void initializeHiveServices(Configuration hadoopConfiguration) {
    hiveDatabase = connectorConfig.hiveDatabase();
    hiveMetaStore = new HiveMetaStore(hadoopConfiguration, connectorConfig);
    if (format != null) {
      hive = format.getHiveUtil(connectorConfig, hiveMetaStore);
    } else if (newFormat != null) {
      final io.confluent.connect.storage.hive.HiveUtil newHiveUtil
          = ((HiveFactory) newFormat.getHiveFactory())
          .createHiveUtil(connectorConfig, hiveMetaStore);
      hive = new HiveUtil(connectorConfig, hiveMetaStore) {
        @Override
        public void createTable(
            String database, String tableName, Schema schema,
            Partitioner partitioner
        ) {
          newHiveUtil.createTable(database, tableName, schema, partitioner);
          log.debug("Created Hive table {}", tableName);
        }

        @Override
        public void alterSchema(String database, String tableName, Schema schema) {
          newHiveUtil.alterSchema(database, tableName, schema);
          log.debug("Altered Hive table {}", tableName);
        }
      };
    } else {
      throw new ConnectException("One of old or new format classes must be provided");
    }
    executorService = Executors.newSingleThreadExecutor();
    hiveUpdateFutures = new LinkedList<>();
  }

  public void write(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);
      topicPartitionWriters.get(tp).buffer(record);
    }

    if (connectorConfig.hiveIntegrationEnabled()) {
      Iterator<Future<Void>> iterator = hiveUpdateFutures.iterator();
      while (iterator.hasNext()) {
        try {
          Future<Void> future = iterator.next();
          if (future.isDone()) {
            future.get();
            iterator.remove();
          } else {
            break;
          }
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }

    for (TopicPartition tp : topicPartitionWriters.keySet()) {
      topicPartitionWriters.get(tp).write();
    }
  }

  public void recover(TopicPartition tp) {
    topicPartitionWriters.get(tp).recover();
  }

  /**
   * For each partition, get the schema from the latest file and attempt
   * to create a Hive table from it.
   */
  public void syncWithHive() throws ConnectException {
    // ensure each topic is only synced once
    Set<String> topics = topicPartitionWriters.keySet()
        .stream().map(TopicPartition::topic).collect(Collectors.toSet());

    for (String topic : topics) {
      Path recoveredFileWithMaxOffsets = getLatestFilePathForTopic(topic);

      if (recoveredFileWithMaxOffsets != null) {
        Schema latestSchema = schemaFileReader
            .getSchema(connectorConfig, recoveredFileWithMaxOffsets);

        String topicDir = FileUtils.topicDirectory(
            connectorConfig.url(),
            connectorConfig.getTopicsDirFromTopic(topic),
            topic
        );
        createHiveTable(topic, topicDir, latestSchema);
      }
    }
  }

  public void open(Collection<TopicPartition> partitions) {
    log.debug("Opening DataWriter with partitions: {}", partitions);
    for (TopicPartition tp : partitions) {
      TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
          tp,
          storage,
          writerProvider,
          newWriterProvider,
          partitioner,
          connectorConfig,
          context,
          avroData,
          hiveMetaStore,
          hive,
          schemaFileReader,
          executorService,
          hiveUpdateFutures,
          time
      );
      topicPartitionWriters.put(tp, topicPartitionWriter);
      // We need to immediately start recovery to ensure we pause consumption of messages for the
      // assigned topics while we try to recover offsets and rewind.
      log.debug("Recovering offsets for partition {}", tp);
      topicPartitionWriter.recover();
    }

    if (connectorConfig.hiveIntegrationEnabled()) {
      syncWithHive();
    }
  }

  public void close() {
    // Close any writers we have. We may get assigned the same partitions and end up duplicating
    // some effort since we'll have to reprocess those messages. It may be possible to hold on to
    // the TopicPartitionWriter and continue to use the temp file, but this can get significantly
    // more complex due to potential failures and network partitions. For example, we may get
    // this close, then miss a few generations of group membership, during which
    // data may have continued to be processed and we'd have to restart from the recovery stage,
    // make sure we apply the WAL, and only reuse the temp file if the starting offset is still
    // valid. For now, we prefer the simpler solution that may result in a bit of wasted effort.
    for (TopicPartitionWriter writer : topicPartitionWriters.values()) {
      try {
        if (writer != null) {
          // In some failure modes, the writer might not have been created for all assignments
          writer.close();
        }
      } catch (ConnectException e) {
        log.warn("Unable to close writer for topic partition {}: ", writer.topicPartition(), e);
      }
    }
    topicPartitionWriters.clear();
  }

  public void stop() {
    if (executorService != null) {
      boolean terminated = false;
      try {
        log.info("Shutting down Hive executor service.");
        executorService.shutdown();
        long shutDownTimeout = connectorConfig.getLong(
            HdfsSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG
        );
        log.info("Awaiting termination.");
        terminated = executorService.awaitTermination(shutDownTimeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignored
      }

      if (!terminated) {
        log.warn(
            "Unclean Hive executor service shutdown, you probably need to sync with Hive next "
                + "time you start the connector"
        );
        executorService.shutdownNow();
      }
    }

    storage.close();

    if (ticketRenewThread != null) {
      synchronized (this) {
        isRunning = false;
        this.notifyAll();
      }
    }
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  /**
   * By convention, the consumer stores the offset that corresponds to the next record to consume.
   * To follow this convention, this methods returns each offset that is one more than the last
   * offset committed to HDFS.
   *
   * @return Map from TopicPartition to next offset after the most recently committed offset to HDFS
   */
  public Map<TopicPartition, Long> getCommittedOffsets() {
    Map<TopicPartition, Long> offsets = new HashMap<>();
    log.debug("Writer looking for last offsets for topic partitions {}",
        topicPartitionWriters.keySet()
    );
    for (TopicPartition tp : topicPartitionWriters.keySet()) {
      long committedOffset = topicPartitionWriters.get(tp).offset();
      log.debug("Writer found last offset {} for topic partition {}", committedOffset, tp);
      if (committedOffset >= 0) {
        offsets.put(tp, committedOffset);
      }
    }
    return offsets;
  }

  public TopicPartitionWriter getBucketWriter(TopicPartition tp) {
    return topicPartitionWriters.get(tp);
  }

  public Storage getStorage() {
    return storage;
  }

  Map<String, io.confluent.connect.storage.format.RecordWriter> getWriters(TopicPartition tp) {
    return topicPartitionWriters.get(tp).getWriters();
  }

  public Map<String, String> getTempFileNames(TopicPartition tp) {
    TopicPartitionWriter topicPartitionWriter = topicPartitionWriters.get(tp);
    return topicPartitionWriter.getTempFiles();
  }

  private Partitioner newPartitioner(HdfsSinkConnectorConfig config)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    Partitioner partitioner;
    try {
      @SuppressWarnings("unchecked")
      Class<? extends Partitioner> partitionerClass =
          (Class<? extends Partitioner>)
              config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);
      partitioner = partitionerClass.newInstance();
    } catch (ClassCastException e) {
      @SuppressWarnings("unchecked")
      Class<? extends io.confluent.connect.storage.partitioner.Partitioner<FieldSchema>>
          partitionerClass =
          (Class<? extends io.confluent.connect.storage.partitioner.Partitioner<FieldSchema>>)
              config.getClass(PartitionerConfig.PARTITIONER_CLASS_CONFIG);
      partitioner = new PartitionerWrapper(partitionerClass.newInstance());
    }

    partitioner.configure(new HashMap<>(config.plainValues()));
    return partitioner;
  }

  public static class PartitionerWrapper implements Partitioner {
    public final io.confluent.connect.storage.partitioner.Partitioner<FieldSchema>  partitioner;

    public PartitionerWrapper(
        io.confluent.connect.storage.partitioner.Partitioner<FieldSchema> partitioner
    ) {
      this.partitioner = partitioner;
    }

    @Override
    public void configure(Map<String, Object> config) {
      partitioner.configure(config);
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
      return partitioner.encodePartition(sinkRecord);
    }

    @Override
    public String generatePartitionedPath(String topic, String encodedPartition) {
      return partitioner.generatePartitionedPath(topic, encodedPartition);
    }

    @Override
    public List<FieldSchema> partitionFields() {
      return partitioner.partitionFields();
    }
  }

  private String getPartitionValue(String path) {
    String[] parts = path.split("/");
    StringBuilder sb = new StringBuilder();
    sb.append("/");
    for (int i = 3; i < parts.length; ++i) {
      sb.append(parts[i]);
      sb.append("/");
    }
    return sb.toString();
  }

  private Partitioner createPartitioner(HdfsSinkConnectorConfig config)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    @SuppressWarnings("unchecked")
    Class<? extends Partitioner> partitionerClasss = (Class<? extends Partitioner>)
        Class.forName(config.getString(PartitionerConfig.PARTITIONER_CLASS_CONFIG));

    Map<String, Object> map = copyConfig(config);
    Partitioner partitioner = partitionerClasss.newInstance();
    partitioner.configure(map);
    return partitioner;
  }

  private Map<String, Object> copyConfig(HdfsSinkConnectorConfig config) {
    Map<String, Object> map = new HashMap<>();
    map.put(
        PartitionerConfig.PARTITION_FIELD_NAME_CONFIG,
        config.getString(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG)
    );
    map.put(
        PartitionerConfig.PARTITION_DURATION_MS_CONFIG,
        config.getLong(PartitionerConfig.PARTITION_DURATION_MS_CONFIG)
    );
    map.put(
        PartitionerConfig.PATH_FORMAT_CONFIG,
        config.getString(PartitionerConfig.PATH_FORMAT_CONFIG)
    );
    map.put(PartitionerConfig.LOCALE_CONFIG, config.getString(PartitionerConfig.LOCALE_CONFIG));
    map.put(PartitionerConfig.TIMEZONE_CONFIG, config.getString(PartitionerConfig.TIMEZONE_CONFIG));
    return map;
  }

  private void renewKerberosTicket(UserGroupInformation ugi) {
    synchronized (DataWriter.this) {
      while (isRunning) {
        try {
          DataWriter.this.wait(connectorConfig.kerberosTicketRenewPeriodMs());
          if (isRunning) {
            log.debug("Attempting re-login from keytab for user: {}", ugi.getUserName());
            ugi.reloginFromKeytab();
          }
        } catch (IOException e) {
          // We ignore this exception during relogin as each successful relogin gives
          // additional 24 hours of authentication in the default config. In normal
          // situations, the probability of failing relogin 24 times is low and if
          // that happens, the task will fail eventually.
          log.error("Error renewing the ticket", e);
        } catch (InterruptedException e) {
          // ignored
        }
      }
    }
  }

  /**
   * Attempt to create a Hive table based on a file's schema from the topic.
   *
   * @param topic the name of the topic
   * @param topicDir the directory of the topic
   * @param latestSchema the schema of the latest file
   */
  private void createHiveTable(String topic, String topicDir, Schema latestSchema) {
    try {
      hive.createTable(hiveDatabase, topic, latestSchema, partitioner);
      List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, topic, (short) -1);
      FileStatus[] statuses = FileUtils.getDirectories(storage, new Path(topicDir));
      for (FileStatus status : statuses) {
        String location = status.getPath().toString();
        if (!partitions.contains(location)) {
          String partitionValue = getPartitionValue(location);
          hiveMetaStore.addPartition(hiveDatabase, topic, partitionValue);
        }
      }
    } catch (IOException e) {
      throw new ConnectException(
          String.format("Error creating Hive table: %s", e.getMessage()),
          e
      );
    }
  }

  /**
   * Get the latest file written for a topic. Go through the topic partition writers
   * for a topic and make use of the already computed latest file in recover.
   *
   * @param topic the topic to get the most recently written file for
   *
   * @return the path of the latest file in the topic
   */
  private Path getLatestFilePathForTopic(String topic) {
    long greatestOffset = -1;
    Path latestFilePath = null;
    for (TopicPartitionWriter tp : topicPartitionWriters.values()) {
      if (tp.topicPartition().topic().equals(topic)) {
        if (tp.offset() > greatestOffset) {
          greatestOffset = tp.offset();
          latestFilePath = tp.getRecoveredFileWithMaxOffsets();
        }
      }
    }
    return latestFilePath;
  }
}

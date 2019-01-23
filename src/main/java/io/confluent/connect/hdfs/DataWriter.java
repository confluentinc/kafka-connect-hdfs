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

package io.confluent.connect.hdfs;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveServiceProvider;
import io.confluent.connect.hdfs.hive.HiveTableNamingParameters;
import io.confluent.connect.hdfs.hive.HiveTableNamingStrategy;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.hive.SchemaNameHiveTableNamingStrategy;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.schema.SchemaResolutionStrategyProvider;
import io.confluent.connect.hdfs.schema.SchemaResolutionStrategy;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.hdfs.storage.Storage;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.SchemaFileReader;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.confluent.connect.hdfs.FileUtils.topicDirectory;
import static io.confluent.connect.hdfs.HdfsSinkConnectorConfig.MULTI_SCHEMA_SUPPORT_CONFIG;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class DataWriter {
  private static final Logger log = LoggerFactory.getLogger(DataWriter.class);
  private static final Time SYSTEM_TIME = new SystemTime();
  private final Time time;

  private Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private String url;
  private HdfsStorage storage;
  private String topicsDir;
  private Format format;
  private SchemaResolutionStrategyProvider schemaResolutionStrategyProvider;
  private io.confluent.connect.storage.format.Format<HdfsSinkConnectorConfig, Path> newFormat;
  private Set<TopicPartition> assignment;
  private Partitioner partitioner;
  private Map<TopicPartition, Long> offsets;
  private HdfsSinkConnectorConfig connectorConfig;
  private SinkTaskContext context;
  private ExecutorService executorService;
  private String hiveDatabase;
  private HiveMetaStore hiveMetaStore;
  private HiveUtil hive;
  private Queue<Future<Void>> hiveUpdateFutures;
  private HiveTableNamingStrategy hiveTableNamingStrategy;
  private boolean hiveIntegration;
  private Thread ticketRenewThread;
  private volatile boolean isRunning;
  private HiveServiceProvider hiveServiceProvider;
  private SelectableRecordWriterProvider recoerdWriterProvider;

  public DataWriter(
      HdfsSinkConnectorConfig connectorConfig,
      SinkTaskContext context,
      AvroData avroData
  ) {
    this(connectorConfig, context, avroData, SYSTEM_TIME);

  }

  @SuppressWarnings("unchecked")
  public DataWriter(
      HdfsSinkConnectorConfig connectorConfig,
      SinkTaskContext context,
      AvroData avroData,
      Time time
  ) {
    this.time = time;
    try {
      String hadoopHome = connectorConfig.getString(HdfsSinkConnectorConfig.HADOOP_HOME_CONFIG);
      System.setProperty("hadoop.home.dir", hadoopHome);

      this.connectorConfig = connectorConfig;
      this.context = context;

      String hadoopConfDir = connectorConfig.getString(
          HdfsSinkConnectorConfig.HADOOP_CONF_DIR_CONFIG
      );
      log.info("Hadoop configuration directory {}", hadoopConfDir);
      Configuration conf = connectorConfig.getHadoopConfiguration();
      if (!hadoopConfDir.equals("")) {
        conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
        conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
      }

      boolean secureHadoop = connectorConfig.getBoolean(
          HdfsSinkConnectorConfig.HDFS_AUTHENTICATION_KERBEROS_CONFIG
      );
      if (secureHadoop) {
        SecurityUtil.setAuthenticationMethod(
            UserGroupInformation.AuthenticationMethod.KERBEROS,
            conf
        );
        String principalConfig = connectorConfig.getString(
            HdfsSinkConnectorConfig.CONNECT_HDFS_PRINCIPAL_CONFIG
        );
        String keytab = connectorConfig.getString(
            HdfsSinkConnectorConfig.CONNECT_HDFS_KEYTAB_CONFIG
        );

        if (principalConfig == null || keytab == null) {
          throw new ConfigException(
              "Hadoop is using Kerberos for authentication, you need to provide both a connect "
                  + "principal and the path to the keytab of the principal.");
        }

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        String namenodePrincipalConfig = connectorConfig.getString(
            HdfsSinkConnectorConfig.HDFS_NAMENODE_PRINCIPAL_CONFIG
        );

        String namenodePrincipal = SecurityUtil.getServerPrincipal(
            namenodePrincipalConfig,
            hostname
        );
        // namenode principal is needed for multi-node hadoop cluster
        if (conf.get("dfs.namenode.kerberos.principal") == null) {
          conf.set("dfs.namenode.kerberos.principal", namenodePrincipal);
        }
        log.info("Hadoop namenode principal: " + conf.get("dfs.namenode.kerberos.principal"));

        UserGroupInformation.setConfiguration(conf);
        // replace the _HOST specified in the principal config to the actual host
        String principal = SecurityUtil.getServerPrincipal(principalConfig, hostname);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        log.info("Login as: " + ugi.getUserName());

        final long renewPeriod = connectorConfig.getLong(
            HdfsSinkConnectorConfig.KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG
        );

        isRunning = true;
        ticketRenewThread = new Thread(new Runnable() {
          @Override
          public void run() {
            synchronized (DataWriter.this) {
              while (isRunning) {
                try {
                  DataWriter.this.wait(renewPeriod);
                  if (isRunning) {
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
        });
        log.info("Starting the Kerberos ticket renew thread with period {}ms.", renewPeriod);
        ticketRenewThread.start();
      }

      url = connectorConfig.getString(HdfsSinkConnectorConfig.HDFS_URL_CONFIG);
      topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);

      @SuppressWarnings("unchecked")
      Class<? extends HdfsStorage> storageClass = (Class<? extends HdfsStorage>) connectorConfig
          .getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
      storage = io.confluent.connect.storage.StorageFactory.createStorage(
          storageClass,
          HdfsSinkConnectorConfig.class,
          connectorConfig,
          url
      );

      createDir(topicsDir);
      createDir(topicsDir + HdfsSinkConnectorConstants.TEMPFILE_DIRECTORY);
      String logsDir = connectorConfig.getString(HdfsSinkConnectorConfig.LOGS_DIR_CONFIG);
      createDir(logsDir);
      setupStorageFormat(connectorConfig, avroData);

      partitioner = newPartitioner(connectorConfig);

      assignment = new HashSet<>(context.assignment());
      offsets = new HashMap<>();

      hiveIntegration = connectorConfig.getBoolean(HiveConfig.HIVE_INTEGRATION_CONFIG);
      if (hiveIntegration) {
        setupHiveIntegration(connectorConfig, conf);
      }

      topicPartitionWriters = new HashMap<>();
      for (TopicPartition tp : assignment) {
        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
            tp,
            storage,
            recoerdWriterProvider,
            partitioner,
            connectorConfig,
            context,
            schemaResolutionStrategyProvider.get(tp),
            hiveServiceProvider != null ? hiveServiceProvider.get(tp) : null,
            time
        );
        topicPartitionWriters.put(tp, topicPartitionWriter);
      }
    } catch (ClassNotFoundException
            | IllegalAccessException
            | InstantiationException
            | InvocationTargetException
            | NoSuchMethodException e
    ) {
      throw new ConnectException("Reflection exception: ", e);
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  private void setupHiveIntegration(HdfsSinkConnectorConfig connectorConfig, Configuration conf) {
    hiveDatabase = connectorConfig.getString(HiveConfig.HIVE_DATABASE_CONFIG);
    hiveMetaStore = new HiveMetaStore(conf, connectorConfig);
    if (format != null) {
      hive = format.getHiveUtil(connectorConfig, hiveMetaStore);
    } else if (newFormat != null) {
      final io.confluent.connect.storage.hive.HiveUtil newHiveUtil
              = newFormat.getHiveFactory().createHiveUtil(connectorConfig, hiveMetaStore);
      hive = new HiveUtil(connectorConfig, hiveMetaStore) {
        @Override
        public void createTable(
                String database, String tableName, Schema schema,
                Partitioner partitioner
        ) {
          newHiveUtil.createTable(database, tableName, schema, partitioner);
        }

        @Override
        public void alterSchema(String database, String tableName, Schema schema) {
          newHiveUtil.alterSchema(database, tableName, schema);
        }
      };
    } else {
      throw new ConnectException("One of old or new format classes must be provided");
    }
    executorService = Executors.newSingleThreadExecutor();
    hiveUpdateFutures = new LinkedList<>();
    if (connectorConfig.getBoolean(MULTI_SCHEMA_SUPPORT_CONFIG)) {
      this.hiveTableNamingStrategy = new SchemaNameHiveTableNamingStrategy();
    } else {
      this.hiveTableNamingStrategy = new HiveTableNamingStrategy() {};
    }
    this.hiveServiceProvider = new HiveServiceProvider(partitioner, hiveTableNamingStrategy,
            hiveMetaStore, hive, executorService, hiveUpdateFutures, connectorConfig);
  }

  @SuppressWarnings("unchecked")
  private void setupStorageFormat(HdfsSinkConnectorConfig connectorConfig, AvroData avroData)
          throws InstantiationException, IllegalAccessException, InvocationTargetException,
          NoSuchMethodException {
    // Try to instantiate as a new-style storage-common type class, then fall back to old-style
    // with no parameters
    io.confluent.connect.storage.format.SchemaFileReader<HdfsSinkConnectorConfig, Path>
            schemaFileReader;
    RecordWriterProvider writerProvider = null;
    io.confluent.connect.storage.format.RecordWriterProvider<HdfsSinkConnectorConfig>
            newWriterProvider = null;
    try {
      Class<io.confluent.connect.storage.format.Format> formatClass =
              (Class<io.confluent.connect.storage.format.Format>)
                      connectorConfig.getClass(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG);

      newFormat = formatClass.getConstructor(HdfsStorage.class).newInstance(storage);
      newWriterProvider = newFormat.getRecordWriterProvider();
      schemaFileReader = newFormat.getSchemaFileReader();
    } catch (NoSuchMethodException e) {
      Class<Format> formatClass =
              (Class<Format>) connectorConfig.getClass(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG);
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
        public void close() throws IOException {}
      };
    }
    this.recoerdWriterProvider = new SelectableRecordWriterProvider(this.storage.conf(), avroData,
            newWriterProvider, writerProvider);
    this.schemaResolutionStrategyProvider = new SchemaResolutionStrategyProvider(storage,
            connectorConfig, schemaFileReader);
  }

  public void write(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
      String topic = record.topic();
      int partition = record.kafkaPartition();
      TopicPartition tp = new TopicPartition(topic, partition);
      topicPartitionWriters.get(tp).buffer(record);
    }

    if (hiveIntegration) {
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

    for (TopicPartition tp : assignment) {
      topicPartitionWriters.get(tp).write();
    }
  }

  public void recover(TopicPartition tp) {
    topicPartitionWriters.get(tp).recover();
  }

  public void syncWithHive() throws ConnectException {
    Set<String> topics = new HashSet<>();
    for (TopicPartition tp : assignment) {
      topics.add(tp.topic());
    }

    try {
      for (String topic : topics) {
        for (Schema latestSchema : readSchemas(topic)) {
          hive.createTable(hiveDatabase, topic, latestSchema, partitioner);
          String hiveTableName = hiveTableNamingStrategy.createName(
                  new HiveTableNamingParameters(topic, latestSchema)
          );
          List<String> partitions = hiveMetaStore.listPartitions(hiveDatabase, hiveTableName,
                  (short) -1
          );
          String topicDirPath = createTopicDirectoryName(topic, latestSchema);
          FileStatus[] statuses = FileUtils.getDirectories(storage, new Path(topicDirPath));
          for (FileStatus status : statuses) {
            String location = status.getPath().toString();
            if (!partitions.contains(location)) {
              String partitionValue = getPartitionValue(location);
              hiveMetaStore.addPartition(hiveDatabase, hiveTableName, partitionValue);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  private List<Schema> readSchemas(String topic) {
    SchemaResolutionStrategy schemaResolutionStrategy =
            schemaResolutionStrategyProvider.get(topic, true);
    if (connectorConfig.getBoolean(MULTI_SCHEMA_SUPPORT_CONFIG)) {
      return findStoredSchemaNames(topic).stream()
              .map(schemaName -> schemaResolutionStrategy.getOrLoadCurrentSchema(schemaName, 0))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(toList());
    } else {
      return schemaResolutionStrategy.getOrLoadCurrentSchema(null, 0)
              .map(Collections::singletonList)
              .orElse(emptyList());
    }
  }

  private List<String> findStoredSchemaNames(String topic) {
    String topicDir = topicDirectory(url, topicsDir, topic);
    List<FileStatus> files = storage.list(topicDir);
    List<String> schemas = new ArrayList<>();
    for (FileStatus typeDirectory : files) {
      if (!typeDirectory.isDirectory()) {
        throw new ConnectException("Path " + typeDirectory.getPath().toString()
                + " is not supported for multi schema integration."
                + " Path should consist of type name.");
      }
      schemas.add(typeDirectory.getPath().getName());
    }
    return schemas;
  }

  private String createTopicDirectoryName(String topic, Schema latestSchema) {
    String topicDir = topicDirectory(url, topicsDir, topic);
    if (connectorConfig.getBoolean(MULTI_SCHEMA_SUPPORT_CONFIG)) {
      return topicDir + "/" + latestSchema.name();
    }
    return topicDir;
  }

  public void open(Collection<TopicPartition> partitions) {
    assignment = new HashSet<>(partitions);
    for (TopicPartition tp : assignment) {
      TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
          tp,
          storage,
          recoerdWriterProvider,
          partitioner,
          connectorConfig,
          context,
          schemaResolutionStrategyProvider.get(tp),
          hiveServiceProvider != null ? hiveServiceProvider.get(tp) : null,
          time
      );
      topicPartitionWriters.put(tp, topicPartitionWriter);
      // We need to immediately start recovery to ensure we pause consumption of messages for the
      // assigned topics while we try to recover offsets and rewind.
      recover(tp);
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
    for (TopicPartition tp : assignment) {
      try {
        topicPartitionWriters.get(tp).close();
      } catch (ConnectException e) {
        log.error("Error closing writer for {}. Error: {}", tp, e.getMessage());
      } finally {
        topicPartitionWriters.remove(tp);
      }
    }
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

  public Map<TopicPartition, Long> getCommittedOffsets() {
    for (TopicPartition tp : assignment) {
      offsets.put(tp, topicPartitionWriters.get(tp).offset());
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

  private void createDir(String dir) {
    String path = url + "/" + dir;
    if (!storage.exists(path)) {
      storage.create(path);
    }
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
}

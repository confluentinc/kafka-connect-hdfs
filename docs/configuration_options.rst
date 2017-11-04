Configuration Options
---------------------

HDFS
^^^^

``hdfs.url``
  The HDFS connection URL. This configuration has the format of hdfs:://hostname:port and specifies the HDFS to export data to. This property is deprecated and will be removed in future releases. Use ``store.url`` instead.

  * Type: string
  * Default: null
  * Importance: high

``hadoop.conf.dir``
  The Hadoop configuration directory.

  * Type: string
  * Default: ""
  * Importance: high

``hadoop.home``
  The Hadoop home directory.

  * Type: string
  * Default: ""
  * Importance: high

``logs.dir``
  Top level directory to store the write ahead logs.

  * Type: string
  * Default: logs
  * Importance: high

Security
^^^^^^^^

``hdfs.authentication.kerberos``
  Configuration indicating whether HDFS is using Kerberos for authentication.

  * Type: boolean
  * Default: false
  * Importance: high
  * Dependents: ``connect.hdfs.principal``, ``connect.hdfs.keytab``, ``hdfs.namenode.principal``, ``kerberos.ticket.renew.period.ms``

``connect.hdfs.principal``
  The principal to use when HDFS is using Kerberos to for authentication.

  * Type: string
  * Default: ""
  * Importance: high

``connect.hdfs.keytab``
  The path to the keytab file for the HDFS connector principal. This keytab file should only be readable by the connector user.

  * Type: string
  * Default: ""
  * Importance: high

``hdfs.namenode.principal``
  The principal for HDFS Namenode.

  * Type: string
  * Default: ""
  * Importance: high

``kerberos.ticket.renew.period.ms``
  The period in milliseconds to renew the Kerberos ticket.

  * Type: long
  * Default: 3600000
  * Importance: low

Connector
^^^^^^^^^

``format.class``
  The format class to use when writing data to the store.

  * Type: class
  * Default: io.confluent.connect.hdfs.avro.AvroFormat
  * Importance: high

``flush.size``
  Number of records written to store before invoking file commits.

  * Type: int
  * Importance: high

``rotate.interval.ms``
  The time interval in milliseconds to invoke file commits. This configuration ensures that file commits are invoked every configured interval. This configuration is useful when data ingestion rate is low and the connector didn't write enough messages to commit files. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: high

``rotate.schedule.interval.ms``
  The time interval in milliseconds to periodically invoke file commits. This configuration ensures that file commits are invoked every configured interval. Time of commit will be adjusted to 00:00 of selected timezone. Commit will be performed at scheduled time regardless previous commit time or number of messages. This configuration is useful when you have to commit your data based on current server time, like at the beginning of every hour. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: medium

``schema.cache.size``
  The size of the schema cache used in the Avro converter.

  * Type: int
  * Default: 1000
  * Importance: low

``retry.backoff.ms``
  The retry backoff in milliseconds. This config is used to notify Kafka connect to retry delivering a message batch or performing recovery in case of transient exceptions.

  * Type: long
  * Default: 5000
  * Importance: low

``shutdown.timeout.ms``
  Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are completed during connector shutdown.

  * Type: long
  * Default: 3000
  * Importance: medium

``filename.offset.zero.pad.width``
  Width to zero pad offsets in store's filenames if offsets are too short in order to provide fixed width filenames that can be ordered by simple lexicographic sorting.

  * Type: int
  * Default: 10
  * Valid Values: [0,...]
  * Importance: low

Storage
^^^^^^^

``storage.class``
  The underlying storage layer.

  * Type: class
  * Default: io.confluent.connect.hdfs.storage.HdfsStorage
  * Importance: high

``topics.dir``
  Top level directory to store the data ingested from Kafka.

  * Type: string
  * Default: topics
  * Importance: high

``store.url``
  Store's connection URL, if applicable.

  * Type: string
  * Default: null
  * Importance: high

``directory.delim``
  Directory delimiter pattern

  * Type: string
  * Default: /
  * Importance: medium

``file.delim``
  File delimiter pattern

  * Type: string
  * Default: +
  * Importance: medium

Partitioner
^^^^^^^^^^^

``partitioner.class``
  The partitioner to use when writing data to the store. You can use ``DefaultPartitioner``, which preserves the Kafka partitions; ``FieldPartitioner``, which partitions the data to different directories according to the value of the partitioning field specified in ``partition.field.name``; ``TimeBasedPartitioner``, which partitions data according to ingestion time.

  * Type: class
  * Default: io.confluent.connect.storage.partitioner.DefaultPartitioner
  * Importance: high
  * Dependents: ``partition.field.name``, ``partition.duration.ms``, ``path.format``, ``locale``, ``timezone``

``partition.field.name``
  The name of the partitioning field when FieldPartitioner is used.

  * Type: string
  * Default: ""
  * Importance: medium

``partition.duration.ms``
  The duration of a partition milliseconds used by ``TimeBasedPartitioner``. The default value -1 means that we are not using ``TimeBasedPartitioner``.

  * Type: long
  * Default: -1
  * Importance: medium

``path.format``
  This configuration is used to set the format of the data directories when partitioning with ``TimeBasedPartitioner``. The format set in this configuration converts the Unix timestamp to proper directories strings. For example, if you set ``path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH``, the data directories will have the format ``/year=2015/month=12/day=07/hour=15/``.

  * Type: string
  * Default: ""
  * Importance: medium

``locale``
  The locale to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium

``timezone``
  The timezone to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium

``timestamp.extractor``
  The extractor that gets the timestamp for records when partitioning with ``TimeBasedPartitioner``. It can be set to ``Wallclock``, ``Record`` or ``RecordField`` in order to use one of the built-in timestamp extractors or be given the fully-qualified class name of a user-defined class that extends the ``TimestampExtractor`` interface.

  * Type: string
  * Default: Wallclock
  * Importance: medium

``timestamp.field``
  The record field to be used as timestamp by the timestamp extractor.

  * Type: string
  * Default: timestamp
  * Importance: medium

Hive
^^^^

``hive.integration``
  Configuration indicating whether to integrate with Hive when running the connector.

  * Type: boolean
  * Default: false
  * Importance: high
  * Dependents: ``hive.metastore.uris``, ``hive.conf.dir``, ``hive.home``, ``hive.database``, ``schema.compatibility``

``hive.metastore.uris``
  The Hive metastore URIs, can be IP address or fully-qualified domain name and port of the metastore host.

  * Type: string
  * Default: ""
  * Importance: high

``hive.conf.dir``
  Hive configuration directory

  * Type: string
  * Default: ""
  * Importance: high

``hive.home``
  Hive home directory.

  * Type: string
  * Default: ""
  * Importance: high

``hive.database``
  The database to use when the connector creates tables in Hive.

  * Type: string
  * Default: default
  * Importance: high

Schema
^^^^^^

``schema.compatibility``
  The schema compatibility rule to use when the connector is observing schema changes. The supported configurations are NONE, BACKWARD, FORWARD and FULL.

  * Type: string
  * Default: NONE
  * Importance: high
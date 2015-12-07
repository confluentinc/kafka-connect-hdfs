.. Kafka Connect HDFS documentation master file

HDFS Connector
==============

The HDFS connector allows you to export data from Kafka topics to HDFS files in a variety of formats
and integrates with Hive to make data immediately available for querying with HiveQL.

The connector periodically polls data from Kafka and writes them to HDFS. The data from each Kafka
topic is partitioned by the provided partitioner and divided into chucks. Each chunk of data is
represented as an HDFS file with topic, kafka partition, start and end offset of this data chuck
in the filename. If no partitioner is specified in the configuration, the default partitioner which
preserves the Kafka partitioning will be used. The size of each data chunk is determined by the
number of records written to HDFS and the time written to HDFS.

The HDFS connector integrates with Hive and when it is enabled, the connector automatically creates
an external Hive partitioned table for each Kafka topic and updates the table according to the data
available in HDFS.

Quickstart
----------
In this Quickstart, we use the HDFS connector to export data produced by the Avro console producer
to HDFS.

Start Zookeeper, Kafka and SchemaRegistry is you haven't done so. The instructions on how to start
these services is available at the Confluent Platform QuickStart. You also need to have Hadoop
running locally or remotely and makes sure that you know the HDFS url. For Hive integration, you
need to have Hive installed. This Quickstart assumes that you started the required services with
the default config and you should make necessary changes according to the actual configurations used.

First, start the Avro console producer::

  $ ./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic test_hdfs \
  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

Then in the console producer, type in::

  {"f1": "value1"}
  {"f1": "value2"}
  {"f1": "value3"}

The three records entered will be published to the Kafka topic ``test_hdfs`` in Avro format.

Then run the following command to start Kafka connect with the HDFS connector::

  $ ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties \
  etc/kafka-connect-hdfs/quickstart-hdfs.properties

You should see that the process starts up and logs some messages, and then it will export data from
Kafka to HDFS. Once the connector finishes ingesting data to HDFS, check that the data is available
in HDFS::

  $ hadoop fs -ls /topics/test_hdfs/partitions=0

You should see a file with name ``/topics/t1/partition=0/test_hdfs+0+0000000000+0000000002.avro``
The file name is encoded as ``topic+kafkaPartition+startOffset+endOoffset.format``.

You can use ``avro-tools-1.7.7.jar``
(`<http://mirror.metrocast.net/apache/avro/avro-1.7.7/java/avro-tools-1.7.7.jar>`_)
to extract the content of the file::

  $ hadoop jar avro-tools-1.7.7.jar tojson \
  /topics/test_hdfs/partition=0/test_hdfs+0+0000000000+0000000002.avro

You should see the following output::

  {"f1":"value1"}
  {"f1":"value2"}
  {"f1":"value3"}


.. note:: If you want to run the Quickstart with Hive integration, before starting the connector,
   you need to add the following configurations to
   ``etc/kafka-connect-hdfs/quickstart-hdfs.properties``::

      hive.integration=true
      hive.metastore.uris=uri to your hive metastore
      schema.compatibility=BACKWARD
   After the connector finishes ingesting data to HDFS, you can use Hive to check the data::

      $hive>SELECT * FROM test_hdfs;

   Note that if you leave ``hive.metastore.uris`` empty, an embedded Hive metastore will be created
   in the directory the connector is started. You need to start Hive in that specific directory
   in order to query the data.

Features
--------
The HDFS connector offers a bunch of features as follows:

* **Exactly Once Delivery**: The connector uses a write ahead log to make sure each record exports
  to HDFS exactly once. Also, the connector manages offset commit by encoding the Kafka offset
  information into the file so that the we can start from the last committed offset in case of
  failures and task restarts.

* **Extensible Data Format**: Out of the box, the connector supports writing data to HDFS in Avro
  and Parquet format. Also, you can write other formats to HDFS by extending the ``Format`` class.

* **Hive Integration**: The connector supports Hive integration out of the box, and when it is
  enabled, the connector automatically creates a Hive external partitioned table for each topic
  exported to HDFS.

* **Schema Evolution**: The connector supports schema evolution and different schema compatibility.
  When the connector observes a schema change, it will project to the proper schema according to the
  schema compatibility configuration specified in the connector config. In case of schema
  compatibility is specified ``BACKWARD``, ``FORWARD`` and ``FULL``, Hive integration is also
  supported. The Hive table contains the proper table schema that can be used to query the
  data of the topic ingested with different schema.

* **Secure HDFS and Hive Metastore**: The connector supports Kerberos authentication and thus
  works with secure HDFS and Hive metastore.

* **Pluggable Partitioner**: The connector supports default partitioner, field partitioner, and
  time based partitioner including daily and hourly partitioner out of the box. You can implement
  your own partitioner by extending the ``Partitioner`` class. Plus, you can customize time based
  partitioner by extending the ``TimebasedPartitioner`` class.

Configurations
--------------
This section gives example configuration files that cover common scenarios, then provides an
exhaustive description of the available configuration options.

Example
~~~~~~~
Here is the content of ``etc/kafka-connect-hdfs/quickstart-hdfs.properties``::

  name=hdfs-sink
  connector.class=io.confluent.connect.hdfs.HdfsSinkConnector
  tasks.max=1
  topics=test_hdfs
  hdfs.url=hdfs://localhost:9000
  flush.size=3

The first few settings are common settings you'll specify for all connectors. The ``topics``
specifies the topics we want to export data from, in this case ``test_hdfs``. The ``hdfs.url``
specifies the HDFS we are writing to and you should set this according to your configuration. The
``flush.size`` specifies the number of records we write before we commit the file to HDFS.

Format and Partitioner
~~~~~~~~~~~~~~~~~~~~~~
You need to specify the following configurations to write other format to HDFS or use other
partitioners: ``format.class`` and ``partitioner.class``. The following configuration file
shows that if you want to write Parquet and use hourly partitioner::

  format.class=io.confluent.connect.hdfs.parquet.ParquetFormat
  partitioner.class=io.confluent.connect.hdfs.partitioner.HourlyPartitioner

If you want ot use the field partitioner, you need to specify the ``partition.field.name`` configuration as
well to specify the field name of the record.

Hive Integration
~~~~~~~~~~~~~~~~
At minimum, you need to specify ``hive.integration``, ``hive.metastore.uris`` and
``schema.compatibility`` when integrating Hive. Here is an example configuration::

  hive.integration=true
  hive.metastore.uris=thrift://localhost:9083
  schema.compatibility=BACKWARD

You should adjust the ``hive.metastore.uris`` according to your Hive configurations. If you don't
specify the ``hive.metastore.uris``, the connector will use a local metastore with Derby in the
directory running the connector. You need to run hive in this directory in order to see the
Hive metadata change. Also, to support schema evolution, we need to specify the
``schema.compatibility`` to be ``BACKWARD``, ``FORWARD`` and ``FULL``. This ensures that Hive can
query the data ingested with different schemas using the latest Hive table schema. Please find
more information on schema compatibility in the `Schema Evolution`_ section.

Secure HDFS and Hive Metastore
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To work with secure HDFS and Hive metastore, you need to specify the following configurations:
``hdfs.authentication.kerberos``, ``connect.hdfs.principal``, ``connect.keytab``,
``hdfs.namenode.principal`` in the connector config::

  hdfs.authentication.kerberos=true
  connect.hdfs.principal=connect-hdfs/_HOST@YOUR-REALM.COM
  connect.hdfs.keytab=path to the key tab
  hdfs.namenode.principal=namenode principal

You need to create the Kafka connect principals and keytab files via Kerboros. Also you need
to distribute the keytab file to all nodes that running the connector and ensures that only the
connector user has read access to the keytab file. Also, you need to make sure the connector user
have write access to the directories specified in ``topics.dir`` and ``logs.dir``.

.. note:: Currently, the connector requires that the principal and the keytab path to be the same
   on all the hosts running the connector.

.. _schema_evolution:
Schema Evolution
----------------
The HDFS connector supports schema evolution and will take actions when observing schema changes
according to the ``schema.compatibility`` configuration. In this section, we will explain how the
connector reacts to schema evolution under different values of ``schema.compatibility``. The
``schema.compatibility`` can be set to ``NONE``, ``BACKWARD``, ``FORWARD`` and ``FULL``, which means
NO compatibility, BACKWARD compatibility, FORWARD compatibility and FULL compatibility.

* **NO Compatibility**: By default, this configuration is set to ``NONE``. in this case, the
  connector only ensures that each file written to HDFS has the proper schema. When the connector
  observes a schema change, it will commit the current files for the affected topic partitions and
  writing the new record with its schema.

* **BACKWARD Compatibility**: If a schema is evolved in a backward compatible way, we can always
  use the latest schema to query all the data uniformly. For example, removing fields is backward
  compatible change to a schema, since when we encounter records written with the old schema that
  contain these fields we can just ignore them. Adding a field with a default value is also backward
  compatible, When a record arrives, we check the version of the schema of the record. if
  ``BACKWARD`` is specified in the configuration, the connector keeps track of the latest schema
  used in writing data to HDFS, and if the connector observes a record with a schema version larger
  than current latest schema, the connector will commit the files with the newer schema. For records
  arriving at a later time with an earlier schema version, the connector project the record to the
  latest schema before writing to HDFS.

* **FORWARD Compatibility**: If a schema is evolved in a forward compatible way, we can always
  use the oldest schema to query all the data uniformly. Removing a field that had a default value
  is forward compatible, since the old schema will use the default value when the field is missing.
  If ``FORWARD`` is specified in the configuration, the connector will project the data to
  the oldest schema before writing to HDFS.

* **Full Compatibility**: Full compatibility means that old data can be read with the new schema
  and new data can also be read with the old schema. If ``FULL`` is specified in the configuration,
  the connector performs the same action as ``BACKWARD``.

If Hive integration is enabled, we need to specify the ``schema.compatibility`` to be ``BACKWARD``,
``FORWARD`` or ``FULL``. This ensures that the Hive table schema is able to query all the data under
a topic written with different schemas.

Configuration Options
~~~~~~~~~~~~~~~~~~~~~
``flush.size``
  Number of records written to HDFS before invoking file commits.

  * Type: int
  * Default:
  * Importance: high

``hdfs.url``
  The HDFS connection URL. This configuration has the format of hdfs:://hostname:port and specifies
  the HDFS to export data to.

  * Type: string
  * Default: ""
  * Importance: high

``connect.hdfs.keytab``
  The path to the keytab file for the HDFS connector principal. This keytab file should only be
  readable by the connector user.

  * Type: string
  * Default: ""
  * Importance: high

``connect.hdfs.principal``
  The principal to use when HDFS is using Kerberos to for authentication.

  * Type: string
  * Default: ""
  * Importance: high

``format.class``
  The format class to use when writing data to HDFS.

  * Type: string
  * Default: "io.confluent.connect.hdfs.avro.AvroFormat"
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

``hdfs.authentication.kerberos``
  Configuration indicating whether HDFS is using Kerberos for authentication.

  * Type: boolean
  * Default: false
  * Importance: high

``hdfs.namenode.principal``
  The principal for HDFS Namenode.

  * Type: string
  * Default: ""
  * Importance: high

``hive.conf.dir``
  Hive configuration directory

  * Type: string
  * Default: ""
  * Importance: high

``hive.database``
  The database to use when the connector creates tables in Hive.

  * Type: string
  * Default: "default"
  * Importance: high

``hive.home``
  Hive home directory

  * Type: string
  * Default: ""
  * Importance: high

``hive.integration``
  Configuration indicating whether to integrate with Hive when running the connector.

  * Type: boolean
  * Default: false
  * Importance: high

``hive.metastore.uris``
  The Hive metastore URIs, can be IP address or fully-qualified domain name and port of the
  metastore host.

  * Type: string
  * Default: ""
  * Importance: high

``logs.dir``
  Top level HDFS directory to store the write ahead logs.

  * Type: string
  * Default: "logs"
  * Importance: high

``partitioner.class``
  The partitioner to use when writing data to HDFS. You can use ``DefaultPartitioner``, which
  preserves the Kafka partitions; ``FieldPartitioner``, which partitions the data to different
  directories according to the value of the partitioning field specified in
  ``partition.field.name``; ``TimebasedPartitioner``, which partitions data according to the time
  ingested to HDFS.

  * Type: string
  * Default: "io.confluent.connect.hdfs.partitioner.DefaultPartitioner"
  * Importance: high

``rotate.interval.ms``
  The time interval in milliseconds to invoke file commits. This configuration ensures that file
  commits are invoked every configured interval. This configuration is useful when data ingestion
  rate is low and the connector didn't write enough messages to commit files.The default value -1
  means that this feature is disabled.

  * Type: long
  * Default: -1
  * Importance: high

``schema.compatibility``
  The schema compatibility rule to use when the connector is observing schema changes. The supported
  configurations are NONE, BACKWARD, FORWARD and FULL.

  * Type: string
  * Default: "NONE"
  * Importance: high

``topics.dir``
  Top level HDFS directory to store the data ingested from Kafka.

  * Type: string
  * Default: "topics"
  * Importance: high

``locale``
  The locale to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium

``partition.duration.ms``
  The duration of a partition milliseconds used by ``TimeBasedPartitioner``. The default value -1
  means that we are not using ``TimebasedPartitioner``.

  * Type: long
  * Default: -1
  * Importance: medium

``partition.field.name``
  The name of the partitioning field when FieldPartitioner is used.

  * Type: string
  * Default: ""
  * Importance: medium

``path.format``
  This configuration is used to set the format of the data directories when partitioning with
  ``TimeBasedPartitioner``. The format set in this configuration converts the Unix timestamp to
  proper directories strings. For example, if you set
  ``path.format='year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/``, the data directories will have
  the format ``/year=2015/month=12/day=07/hour=15``

  * Type: string
  * Default: ""
  * Importance: medium

``shutdown.timeout.ms``
  Clean shutdown timeout. This makes sure that asynchronous Hive metastore updates are completed
  during connector shutdown.

  * Type: long
  * Default: 3000
  * Importance: medium

``timezone``
  The timezone to use when partitioning with ``TimeBasedPartitioner``.

  * Type: string
  * Default: ""
  * Importance: medium

``filename.offset.zero.pad.width``
  Width to zero pad offsets in HDFS filenames to if the offsets is too short in order to provide
  fixed width filenames that can be ordered by simple lexicographic sorting.

  * Type: int
  * Default: 10
  * Importance: low

``kerberos.ticket.renew.period.ms``
  The period in milliseconds to renew the Kerberos ticket.

  * Type: long
  * Default: 3600000
  * Importance: low

``retry.backoff.ms``
  The retry backoff in milliseconds. This config is used to notify Kafka connect to retry delivering
  a message batch or performing recovery in case of transient exceptions.

  * Type: long
  * Default: 5000
  * Importance: low

``schema.cache.size``
  The size of the schema cache used in the Avro converter.

  * Type: int
  * Default: 1000
  * Importance: low

``storage.class``
  The underlying storage layer. The default is HDFS

  * Type: string
  * Default: "io.confluent.connect.hdfs.storage.HdfsStorage"
  * Importance: low


.. _hdfs_connector_changelog:

Changelog
=========

Version 4.1.2
-------------

* `PR-335 <https://github.com/confluentinc/kafka-connect-hdfs/pull/335>`_ - MINOR: Fix the secure HDFS test

Version 4.1.1
-------------

* `PR-307 <https://github.com/confluentinc/kafka-connect-hdfs/pull/307>`_ - CC-918 Add Kafka Connect Maven plugin to build

Version 4.1.0
-------------

* `PR-299 <https://github.com/confluentinc/kafka-connect-hdfs/pull/299>`_ - CC-1669: Set no offset when no offset was found for topic partition
* `PR-293 <https://github.com/confluentinc/kafka-connect-hdfs/pull/293>`_ - Explicitly specify Jackson version instead of relying on transitive dependency version.
* `PR-283 <https://github.com/confluentinc/kafka-connect-hdfs/pull/283>`_ - Adding StringFormat to available formatters
* `PR-286 <https://github.com/confluentinc/kafka-connect-hdfs/pull/286>`_ - Support for multiple fields in field partitioner
* `PR-269 <https://github.com/confluentinc/kafka-connect-hdfs/pull/269>`_ - Fix or suppress findbugs errors
* `PR-255 <https://github.com/confluentinc/kafka-connect-hdfs/pull/255>`_ - CC-1299: Add configuration setting for Avro compression codec
* `PR-48 <https://github.com/confluentinc/kafka-connect-storage-common/pull/48>`_ - RecordField nested timestamp extraction
* `PR-57 <https://github.com/confluentinc/kafka-connect-storage-common/pull/57>`_ - Issue-53. Multiple field support for partitioning.
* `PR-55 <https://github.com/confluentinc/kafka-connect-storage-common/pull/55>`_ - CC-1489: NPE on records with null timestamp.
* `PR-45 <https://github.com/confluentinc/kafka-connect-storage-common/pull/45>`_ - HOTFIX: Consider all the properties in AvroDataConfig.
* `PR-52 <https://github.com/confluentinc/kafka-connect-storage-common/pull/52>`_ - CC-1333: Remove dependency on hive-exec and instead use hive-exec:core
* `PR-51 <https://github.com/confluentinc/kafka-connect-storage-common/pull/51>`_ - CC-1333: Remove dependency on hive-exec and instead use hive-exec:core
* `PR-50 <https://github.com/confluentinc/kafka-connect-storage-common/pull/50>`_ - CC-1333: Remove dependency on hive-exec and instead use hive-exec:core
* `PR-46 <https://github.com/confluentinc/kafka-connect-storage-common/pull/46>`_ - CC-1299: Create AVRO_CODEC ConfigKey

Version 4.0.1
-------------

* `PR-305 <https://github.com/confluentinc/kafka-connect-hdfs/pull/305>`_ - CC-1669: Disable commit of consumer offsets
* `PR-299 <https://github.com/confluentinc/kafka-connect-hdfs/pull/299>`_ - CC-1669: Set no offset when no offset was found for topic partition
* `PR-293 <https://github.com/confluentinc/kafka-connect-hdfs/pull/293>`_ - Explicitly specify Jackson version instead of relying on transitive dependency version.
* `PR-55 <https://github.com/confluentinc/kafka-connect-storage-common/pull/55>`_ - CC-1489: NPE on records with null timestamp.
* `PR-45 <https://github.com/confluentinc/kafka-connect-storage-common/pull/45>`_ - HOTFIX: Consider all the properties in AvroDataConfig.

Version 4.0.0
-------------

* `PR-251 <https://github.com/confluentinc/kafka-connect-hdfs/pull/251>`_ - CC-1270: Reinstantiate HiveConfig properties in getConfig.
* `PR-247 <https://github.com/confluentinc/kafka-connect-hdfs/pull/247>`_ - MINOR: Exclude storage common dependencies.
* `PR-246 <https://github.com/confluentinc/kafka-connect-hdfs/pull/246>`_ - CC-1213: Enable record based timebased partitioners in HDFS connector
* `PR-245 <https://github.com/confluentinc/kafka-connect-hdfs/pull/245>`_ - CC-1212: Accept partitioners implementing the new partitioner interface
* `PR-243 <https://github.com/confluentinc/kafka-connect-hdfs/pull/243>`_ - CC-1168: Recommenders for HDFS connector's class type properties
* `PR-237 <https://github.com/confluentinc/kafka-connect-hdfs/pull/237>`_ - Remove unused imports
* `PR-196 <https://github.com/confluentinc/kafka-connect-hdfs/pull/196>`_ - CC-492: Add JsonFormat to HDFS Connector
* `PR-142 <https://github.com/confluentinc/kafka-connect-hdfs/pull/142>`_ - not trying to hold leases on WAL files if we are holding them already.
* `PR-220 <https://github.com/confluentinc/kafka-connect-hdfs/pull/220>`_ - HOTFIX: Enable default for format and storage classes in hdfs connector
* `PR-219 <https://github.com/confluentinc/kafka-connect-hdfs/pull/219>`_ - Move to common pom
* `PR-188 <https://github.com/confluentinc/kafka-connect-hdfs/pull/188>`_ - CC-397: Refactoring on top of storage-common
* `PR-214 <https://github.com/confluentinc/kafka-connect-hdfs/pull/214>`_ - fix TimeBasedPartitionerTest by defining the expected locale
* `PR-212 <https://github.com/confluentinc/kafka-connect-hdfs/pull/212>`_ - Remove invalid offset check.
* `PR-44 <https://github.com/confluentinc/kafka-connect-storage-common/pull/44>`_ - HOTFIX: Remove unused parameter in newConfigDef for PartitionerConfig.
* `PR-41 <https://github.com/confluentinc/kafka-connect-storage-common/pull/41>`_ - HOTFIX: Update dependencies
* `PR-40 <https://github.com/confluentinc/kafka-connect-storage-common/pull/40>`_ - HOTFIX: Make specific dependencies explicit.
* `PR-37 <https://github.com/confluentinc/kafka-connect-storage-common/pull/37>`_ - Remove unused imports
* `PR-35 <https://github.com/confluentinc/kafka-connect-storage-common/pull/35>`_ - Add missing modules to the dependencyManagement pom section so downstream projects will inherit the right version automatically.
* `PR-31 <https://github.com/confluentinc/kafka-connect-storage-common/pull/31>`_ - Remove schema.generator.class config and have Formats specify their own SchemaGenerator internally

Version 3.3.1
-------------

* `PR-235 <https://github.com/confluentinc/kafka-connect-hdfs/pull/235>`_ - CC-1172: Fix memory leak in TopicPartitionerWriter
* `PR-227 <https://github.com/confluentinc/kafka-connect-hdfs/pull/227>`_ - Specify a nodeLabel for Jenkins that doesn't die during the HDFS connector tests.
* `PR-226 <https://github.com/confluentinc/kafka-connect-hdfs/pull/226>`_ - Add upstream project so builds will be triggered automatically
* `PR-217 <https://github.com/confluentinc/kafka-connect-hdfs/pull/217>`_ - Update quickstart to use Confluent CLI

Version 3.3.0
-------------

* `PR-187 <https://github.com/confluentinc/kafka-connect-hdfs/pull/187>`_ - CC-491: Consolidate and simplify unit tests of HDFS connector.
* `PR-205 <https://github.com/confluentinc/kafka-connect-hdfs/pull/205>`_ - Upgrade avro to 1.8.2.

Version 3.2.2
-------------

* `PR-194 <https://github.com/confluentinc/kafka-connect-hdfs/pull/194>`_ - Fix HdfsSinkConnector to extend from SinkConnector instead of Connector.
* `PR-200 <https://github.com/confluentinc/kafka-connect-hdfs/pull/200>`_ - Fix incorrect licensing and webpage info.

Version 3.2.1
-------------
No changes

Version 3.2.0
-------------

* `PR-135 <https://github.com/confluentinc/kafka-connect-hdfs/pull/135>`_ - Fix typos
* `PR-164 <https://github.com/confluentinc/kafka-connect-hdfs/pull/164>`_ - Issue 136 - Support topic with dots in hive.
* `PR-170 <https://github.com/confluentinc/kafka-connect-hdfs/pull/170>`_ - MINOR: Upgrade Hadoop version to 2.7.3 and joda-time to 2.9.7

Version 3.1.1
-------------
No changes

Version 3.1.0
-------------

* `PR-134 <https://github.com/confluentinc/kafka-connect-hdfs/pull/134>`_ - Flush the last partial file when incoming stream is paused.
* `PR-133 <https://github.com/confluentinc/kafka-connect-hdfs/pull/133>`_ - CC-331: Update config options docs
* `PR-126 <https://github.com/confluentinc/kafka-connect-hdfs/pull/126>`_ - Fix TimeBasedPartitioner config validation
* `PR-112 <https://github.com/confluentinc/kafka-connect-hdfs/pull/112>`_ - Lint change to avoid compiler error in Oracle JDK 1.7 using jenv.
* `PR-94 <https://github.com/confluentinc/kafka-connect-hdfs/pull/94>`_ - Fix lint annoyances
* `PR-108 <https://github.com/confluentinc/kafka-connect-hdfs/pull/108>`_ - Revert "support multi partition fields."
* `PR-105 <https://github.com/confluentinc/kafka-connect-hdfs/pull/105>`_ - support multi partition fields.
* `PR-101 <https://github.com/confluentinc/kafka-connect-hdfs/pull/101>`_ - Added link to Confluent documentation for the connector.
* `PR-92 <https://github.com/confluentinc/kafka-connect-hdfs/pull/92>`_ - Start a new WAL file after `truncate` instead of appending to log.1
* `PR-87 <https://github.com/confluentinc/kafka-connect-hdfs/pull/87>`_ - Scheduled rotation implementation
* `PR-90 <https://github.com/confluentinc/kafka-connect-hdfs/pull/90>`_ - Use configured Hadoop configuration object for Parquet writer
* `PR-91 <https://github.com/confluentinc/kafka-connect-hdfs/pull/91>`_ - Upgrade to Hadoop 2.6.1
* `PR-70 <https://github.com/confluentinc/kafka-connect-hdfs/pull/70>`_ - Fix handling of topics with periods
* `PR-68 <https://github.com/confluentinc/kafka-connect-hdfs/pull/68>`_ - prints details of HDFS exceptions
* `PR-67 <https://github.com/confluentinc/kafka-connect-hdfs/pull/67>`_ - clean up hive metastore artifacts from testing
* `PR-64 <https://github.com/confluentinc/kafka-connect-hdfs/pull/64>`_ - cleaned up .gitignore.  Now ignores Eclipse files

Version 3.0.1
-------------

HDFS Connector
~~~~~~~~~~~~~~
* `PR-82 <https://github.com/confluentinc/kafka-connect-hdfs/pull/82>`_ - add version.txt to share/doc

Version 3.0.0
-------------

HDFS Connector
~~~~~~~~~~~~~~
* `PR-62 <https://github.com/confluentinc/kafka-connect-hdfs/pull/62>`_ - Update doc for CP 3.0.
* `PR-60 <https://github.com/confluentinc/kafka-connect-hdfs/pull/60>`_ - Remove HDFS connectivity check.
* `PR-55 <https://github.com/confluentinc/kafka-connect-hdfs/pull/55>`_ - Removing retry logic from HiveMetaStore to fix the metastore connection bloat.
* `PR-50 <https://github.com/confluentinc/kafka-connect-hdfs/pull/50>`_ - Remove close of topic partition writers in DataWriter close.
* `PR-42 <https://github.com/confluentinc/kafka-connect-hdfs/pull/42>`_ - Using new config validation.
* `PR-41 <https://github.com/confluentinc/kafka-connect-hdfs/pull/41>`_ - Bump version to 3.0.0-SNAPSHOT and Kafka dependency to 0.10.0.0-SNAPSHOT.
* `PR-35 <https://github.com/confluentinc/kafka-connect-hdfs/pull/35>`_ - Minor doc typo fix TimeBasedPartitioner.
* `PR-33 <https://github.com/confluentinc/kafka-connect-hdfs/pull/33>`_ - Minor doc fix.

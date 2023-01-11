# Kafka Connect Large Column HDFS Connector

```
io.confluent.connect.hdfs.jdbc.JdbcHdfsSinkConnector
```

## Description

Custom Kafka Connector that receives Kafka messages, originally sent from an upstream CDC Connector for a DB.
For each Message, this Connector queries Columns missing from that message, usually Columns too large to fit inside a Kafka message, like LOBs.
It then writes those Columns to HDFS.

## Basis

This Connector is strongly based on the base __HdfsSinkConnector__ Connector, and uses many of the same configuration parameters and functionality.
```
io.confluent.connect.hdfs.HdfsSinkConnector
```

## Behavior

1. Create a JDBC Connection (pool) to the source Database, to be used later.
2. Receive Kafka messages, just like the __HdfsSinkConnector__.
3. Determines which columns are missing from the message, based on (new/custom) configuration values.
4. If no columns need to be queried, ignore the message entirely.
   * we assume an instance of the __HdfsSinkConnector__ will have written the message/row separately from this connector.
5. If any columns need to be queried, extract the DB, Table and Primary Keys from the message.
6. Query the Database to get the Data Types for the missing Columns.
   * Using a short-lived Cache, to prevent multiple DB queries in a short amount of time.
7. Generate a Kafka Connect Schema containing the Primary Keys and the missing Columns.
8. Create a Kafka Connect Record based on that Schema, populated with the Primary Key data.
9. Create a SQL SELECT Query based on the Primary Keys, returning the missing Columns.
10. Execute the SQL Query using the JDBC Connection (pool) created earlier.
11. Populate the Kafka Connect Record with the returned Columns.
12. Write that Record to HDFS, using the exact same mechanism as the __HdfsSinkConnector__
13. Flush _each_ Kafka Connect Record to HDFS.

## Configuration

This connector supports almost all the same configuration parameters as the __HdfsSinkConnector__, except for the list of topics, which must be defined explicitly.

### consumer.max.poll.records (old, global)

This value limits the number of messages read from Kafka every poll interval.
If this value is too large, and the DB Queries done for each message take a long time, and too many are polled at once, Kafka might think the Connector is dead due to a Timeout (__5__ minutes by default).

__Sadly, this value must be set globally in Kafka Connect itself, and cannot be set on a per-connector basis.__

```consumer.max.poll.records=20``` (should be safe, and still decently performant for other Connectors)

### connector.class (old)

Must be explicitly set to this Connector.
```
connector.class=io.confluent.connect.hdfs.jdbc.JdbcHdfsSinkConnector
```

### name (old)

Must be different from the names of the __HdfsSinkConnector__ connectors, if any.
That is the only difference for this parameter.
```
name=large_hdfs_connector_1
```

### Tasks (old)

Exactly the same as __HdfsSinkConnector__, but should be changed due to potential RAM memory constraints.
This is because the entire LOB must be loaded into RAM before being written to HDFS, and cannot be streamed from the DB directly.
This it is important to limit the number of LOBs in memory at the same time, based on the number of Tasks.
But with too few Tasks, the time it takes to process all rows will be slower, especially for the initial Snapshot.
The easy solution would be to make sure the Kafka Connect nodes have a lot of RAM, and then increase the Tasks accordingly.

A Safe value would be __1__ or __2__; aggressive would be setting it equal to __partition-count__

Default: __1__

```
tasks.max=1
```

### logs.dir (old)

Must be different from the names of the __HdfsSinkConnector__ connectors, if any, so choose something different.

Default: __logs__ (_choose something different_)
```
logs.dir=logs_large
```

### topics.dir (old)

Must be different from the names of the __HdfsSinkConnector__ connectors, if any, so choose something different.

Default: __topics__ (_choose something different_)
```
topics.dir=topics_large
```

### flush.size (old)

Can be left as the default, but means it's possible that the created ORC file could be many GB in size, depending on the number of Records written to each file.
It really depends on how big you want each HDFS file to be, vs the number of files created.
```
flush.size=20
```

### connection.url (new)

The JDBC Connection to use when querying the large Columns from the upstream DB.
This should probably use the same values as the upstream CDC Connector generating the Kafka Records.
This configuration property is identical to the one in the JDBC Kafka Connector:
* https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html#jdbc-source-connector-configuration-properties
```
connection.url=jdbc:db2://my.db2.host:50000/my_db
```

### connection.user (new)

The JDBC Username to use when querying the large Columns from the upstream DB.
This should probably use the same values as the upstream CDC Connector generating the Kafka Records.
This configuration property is identical to the one in the JDBC Kafka Connector:
* https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html#jdbc-source-connector-configuration-properties
```
connection.user=my_user
```

### connection.password (new)

The JDBC Password to use when querying the large Columns from the upstream DB.
This should probably use the same values as the upstream CDC Connector generating the Kafka Records.
This configuration property is identical to the one in the JDBC Kafka Connector:
* https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/source_config_options.html#jdbc-source-connector-configuration-properties
```
connection.password=my_pass
```

### hash.cache.enabled (new)

If enabled, will cache ```hash.cache.size``` hashes, based on LRU, to prevent writing the same large-column values over and over.

Default: __true__
```
hash.cache.enabled=true
```

### hash.cache.size (new)

Number of (MD5) hash values to keep in the LRU Cache before evicting the least-recently-used (LRU) value(s).
Only used if ```hash.cache.enabled``` is enabled.
Making this larger could improve HDFS write performance, as fewer redundant large-columns would be written, but with an in-memory size tradeoff.

Default: __10000__
```
hash.cache.size=10000
```

### column.include.list (new)

Comma separated list of fully qualified Columns to query from the upstream Database.
Any Kafka messages for unmatched Tables/Columns will be ignored by this Connector.

Fully qualified, meaning: ```<schema>.<table>.<column>```
```
column.include.list=SCHEMA1.TABLE1.COLUMN4,SCHEMA1.TABLE1.COLUMN5,SCHEMA1.TABLE2.COLUMN4,...
```

## Notes

* Designed to work only with a single DB2 Database.
    * Other JDBC Databases might work, but some minor code adjustments will probably be needed.
    * Connections to multiple DB2 Databases will require 1 instance of this Connector per.
* The Connector does a flush to HDFS after _every_ record, as batch-writes might cause OutOfMemory errors.
    * This is because the entire LOB must be loaded into RAM before being written to HDFS, and cannot be streamed from the DB directly.
* Limit the number of messages read from Kafka every iteration.
    * The DB Queries done for each message can take a long time, and too many are polled at once, Kafka might think the Connector is dead due to a Timeout.
    * Value to set: ```consumer.max.poll.records=10``` (should be safe)
    * __Sadly, this value must be set globally in Kafka Connect itself, and cannot be set on a per-connector basis.__
* This Connector uses an LRU Cache to keep track of what large Columns it has written, and if the hash (for all columns for the given row) are in the Cache, the Kafka message is ignored, and nothing is written to HDFS.
    * This is used to prevent writing the same large-column values over and over when they haven't changed, but other columns have.
    * The hash is the MD5 Hash of the Column. _This can possibly be an expensive operation._
    * The LRU Key is based on the DB, Table and Primary Keys, as well as the MD5 Hash. Thus, values for different rows or tables will never be compared.
    * The default size is fairly large (10000) but could be larger, depending on how much JVM memory you want to use for the Cache.
    * This functionality can be disabled entirely, in the configuration, if desired.
* The list of Tables, and Columns for each Table, must be explicitly defined in the configuration.
    * Any messages not explicitly matched will be ignored by this Connector.

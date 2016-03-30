package io.confluent.connect.hdfs;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class NotificationTests extends TestWithMiniDFSCluster {
    @Test
    public void testCommitNotification() throws Exception {
        DataWriter hdfsWriter = new DataWriter(connectorConfig, context, avroData);
        hdfsWriter.recover(TOPIC_PARTITION);

        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = 0; offset < 7; offset++) {
            SinkRecord sinkRecord =
                    new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset);

            sinkRecords.add(sinkRecord);
        }
        hdfsWriter.write(sinkRecords);
        hdfsWriter.close();
    }
}

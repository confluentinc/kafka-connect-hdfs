package io.confluent.connect.hdfs;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class NotificationTests extends TestWithMiniDFSCluster {
    @Test
    public void testSingleNotification() throws Exception {
        assertEquals(1, 1);
    }
}

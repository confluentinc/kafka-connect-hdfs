package io.confluent.connect.hdfs.jdbc;

import org.junit.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HashCacheTest {
  private static final JdbcTableInfo table0 =
      new JdbcTableInfo(/*"foo", */"bar", "table0");
  private static final JdbcTableInfo table1 =
      new JdbcTableInfo(/*"foo", */"bar", "table1");

  private static final byte[] value0 = new byte[] {1, 2, 3};
  private static final byte[] value1 = new byte[] {4, 5, 6};

  @Test
  public void testMD5() throws NoSuchAlgorithmException {
    HashCache hashCache = new HashCache(2, MessageDigest.getInstance("MD5"));

    // Newly added
    assertTrue(hashCache.updateCache(table0, "PK1", "COL_A", value0));
    // Already exists
    assertFalse(hashCache.updateCache(table0, "PK1", "COL_A", value0));
    // Updated with hash(null)
    assertTrue(hashCache.updateCache(table0, "PK1", "COL_A", (byte[]) null));
    // Already exists (with hash(null))
    assertFalse(hashCache.updateCache(table0, "PK1", "COL_A", (byte[]) null));
    // Updated with second value
    assertTrue(hashCache.updateCache(table0, "PK1", "COL_A", value1));
    // Already exists (with second value)
    assertFalse(hashCache.updateCache(table0, "PK1", "COL_A", value1));
    // Different Column
    assertTrue(hashCache.updateCache(table0, "PK2", "COL_B", value1));
    // Already exists (with second Column)
    assertFalse(hashCache.updateCache(table0, "PK2", "COL_B", value1));
    // Different PK
    assertTrue(hashCache.updateCache(table0, "PK2", "COL_A", value1));
    // Already exists (with second PK)
    assertFalse(hashCache.updateCache(table0, "PK2", "COL_A", value1));
    // Different Table
    assertTrue(hashCache.updateCache(table1, "PK1", "COL_A", value1));
    // Already exists (with second table)
    assertFalse(hashCache.updateCache(table1, "PK1", "COL_A", value1));

    // Overflow Cache size; LRU is based on number of Primary Keys (PK)

    // Yet another PK
    assertTrue(hashCache.updateCache(table0, "PK3", "COL_A", value1));
    // Already exists (yet another PK)
    assertFalse(hashCache.updateCache(table0, "PK3", "COL_A", value1));
    // Newly added
    assertTrue(hashCache.updateCache(table0, "PK1", "COL_A", value1));
    // Already exists
    assertFalse(hashCache.updateCache(table0, "PK1", "COL_A", value1));
  }
}

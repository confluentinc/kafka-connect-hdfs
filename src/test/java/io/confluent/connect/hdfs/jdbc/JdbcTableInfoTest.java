package io.confluent.connect.hdfs.jdbc;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

public class JdbcTableInfoTest {
  @Test
  public void testGetters() {
    JdbcTableInfo table = new JdbcTableInfo("ScHeMa  ", "__ Table \n");

    assertEquals("ScHeMa  ", table.getSchema());
    assertEquals("__ Table \n", table.getTable());
  }

  @Test
  public void testEquality() {
    JdbcTableInfo tableA = new JdbcTableInfo("ScHeMa", "__tablE ");
    JdbcTableInfo tableB = new JdbcTableInfo("ScHeMa", "__tablE ");

    assertEquals(tableA, tableA);
    assertEquals(tableB, tableB);
    assertEquals(tableA, tableB);
    assertNotSame(tableA, tableB);
    assertEquals(tableA.hashCode(), tableB.hashCode());
  }

  @Test
  public void testInequality() {
    assertNotEquals(
        new JdbcTableInfo("ScHeMa", "table"),
        new JdbcTableInfo("SCHEMA", "table")
    );

    assertNotEquals(
        new JdbcTableInfo("SCHEMA", "TABLE"),
        new JdbcTableInfo("SCHEMA", "TABLE ")
    );
  }

  @Test
  public void testSort() {
    JdbcTableInfo tableA = new JdbcTableInfo("A", "C");
    JdbcTableInfo tableB = new JdbcTableInfo("B", "A");
    JdbcTableInfo tableC = new JdbcTableInfo("A", "A");
    JdbcTableInfo tableD = new JdbcTableInfo("A", "B");

    List<JdbcTableInfo> expected = Arrays.asList(tableC, tableD, tableA, tableB);
    List<JdbcTableInfo> actual = Arrays.asList(tableA, tableB, tableC, tableD);

    actual.sort(JdbcTableInfo.comparator);

    assertEquals(expected, actual);
    assertNotSame(expected, actual);
  }
}

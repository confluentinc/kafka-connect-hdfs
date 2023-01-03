package io.confluent.connect.hdfs.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JdbcTableInfoTest {
  @Test
  public void testGetters() {
    JdbcTableInfo tableA = new JdbcTableInfo("fOo 1 A", "  Bar\n\t", "\0baz _A_\r");

    assertEquals("foo 1 a", tableA.getDb());
    assertEquals("bar", tableA.getSchema());
    assertEquals("baz _a_", tableA.getTable());
    assertEquals("bar.baz _a_", tableA.qualifiedName());

    JdbcTableInfo tableB = new JdbcTableInfo("", "", "");

    assertNull(tableB.getDb());
    assertNull(tableB.getSchema());
    assertNull(tableB.getTable());
    assertNull(tableB.qualifiedName());

    JdbcTableInfo tableC = new JdbcTableInfo(null, null, null);

    assertNull(tableC.getDb());
    assertNull(tableC.getSchema());
    assertNull(tableC.getTable());
    assertNull(tableC.qualifiedName());

    JdbcTableInfo tableD = new JdbcTableInfo("foo", "", null);

    assertEquals("foo", tableD.getDb());
    assertNull(tableD.getSchema());
    assertNull(tableD.getTable());
    assertNull(tableD.qualifiedName());

    JdbcTableInfo tableE = new JdbcTableInfo(null, "bar", "");

    assertNull(tableE.getDb());
    assertEquals("bar", tableE.getSchema());
    assertNull(tableE.getTable());
    assertEquals("bar", tableE.qualifiedName());

    JdbcTableInfo tableF = new JdbcTableInfo(null, "", "baz");

    assertNull(tableF.getDb());
    assertNull(tableF.getSchema());
    assertEquals("baz", tableF.getTable());
    assertEquals("baz", tableF.qualifiedName());

    JdbcTableInfo tableG = new JdbcTableInfo("Ace ", "", " \rbAs sZ");

    assertEquals("ace", tableG.getDb());
    assertNull(tableG.getSchema());
    assertEquals("bas sz", tableG.getTable());
    assertEquals("bas sz", tableG.qualifiedName());
  }
}

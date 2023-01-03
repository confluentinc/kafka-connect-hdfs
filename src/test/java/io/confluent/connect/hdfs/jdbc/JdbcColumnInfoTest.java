package io.confluent.connect.hdfs.jdbc;

import org.junit.Test;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

public class JdbcColumnInfoTest {
  @Test
  public void testGetters() {
    JdbcColumnInfo column = new JdbcColumnInfo("NaMe123", JDBCType.INTEGER, 33, false);

    assertEquals("NaMe123", column.getName());
    assertEquals(JDBCType.INTEGER, column.getJdbcType());
    assertEquals(33, column.getOrdinal());
    assertFalse(column.isNullable());
  }

  @Test
  public void testEquality() {
    JdbcColumnInfo columnA = new JdbcColumnInfo("NamE", JDBCType.INTEGER, 1, false);
    JdbcColumnInfo columnB = new JdbcColumnInfo("NamE", JDBCType.INTEGER, 1, false);

    assertEquals(columnA, columnA);
    assertEquals(columnB, columnB);
    assertEquals(columnA, columnB);
    assertNotSame(columnA, columnB);
    assertEquals(columnA.hashCode(), columnB.hashCode());
  }

  @Test
  public void testInequality() {
    assertNotEquals(
        new JdbcColumnInfo("NaMe", JDBCType.INTEGER, 1, false),
        new JdbcColumnInfo("NAME", JDBCType.INTEGER, 1, false)
    );

    assertNotEquals(
        new JdbcColumnInfo("NaMe", JDBCType.INTEGER, 1, false),
        new JdbcColumnInfo("NaMe", JDBCType.BLOB, 1, false)
    );

    assertNotEquals(
        new JdbcColumnInfo("NaMe", JDBCType.BLOB, 1, false),
        new JdbcColumnInfo("NaMe", JDBCType.BLOB, 2, false)
    );

    assertNotEquals(
        new JdbcColumnInfo("NaMe", JDBCType.BLOB, 1, false),
        new JdbcColumnInfo("NaMe", JDBCType.BLOB, 1, true)
    );
  }

  @Test
  public void testSortByOrdinal() {
    JdbcColumnInfo columnA = new JdbcColumnInfo("A", JDBCType.BLOB, 2, false);
    JdbcColumnInfo columnB = new JdbcColumnInfo("B", JDBCType.INTEGER, 3, true);
    JdbcColumnInfo columnC = new JdbcColumnInfo("C", JDBCType.VARCHAR, 1, false);
    JdbcColumnInfo columnD = new JdbcColumnInfo("D", JDBCType.NULL, 4, true);

    List<JdbcColumnInfo> expected = Arrays.asList(columnC, columnA, columnB, columnD);
    List<JdbcColumnInfo> actual = Arrays.asList(columnA, columnB, columnC, columnD);

    actual.sort(JdbcColumnInfo.byOrdinal);

    assertEquals(expected, actual);
    assertNotSame(expected, actual);
  }
}

package io.confluent.connect.hdfs.jdbc;

import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JdbcQueryUtilTest extends AbstractJdbcTestCase {
  @Mock
  PreparedStatement preparedStatementMock;
  @Mock
  JdbcValueMapper valueMapperMock;
  @Mock
  JdbcColumnVisitor columnVisitorMock;
  @Mock
  ResultSet resultSetMock;
  @Mock
  ResultSetMetaData resultSetMetaDataMock;
  @Mock
  Blob blobMock;
  @Mock
  Clob clobMock;
  @Mock
  SQLXML sqlXmlMock0;
  @Mock
  SQLXML sqlXmlMock1;

  final MappedCaptor<Blob, byte[]> blobMappedCaptor = MappedCaptor.blobCaptor();
  final MappedCaptor<Clob, String> clobMappedCaptor = MappedCaptor.clobCaptor();
  final MappedCaptor<Clob, String> otherClobMappedCaptor = MappedCaptor.clobCaptor();

  public JdbcQueryUtilTest() {
    super(InMemoryDatabase.HSQL);
  }

  @Override
  public void tearDown() throws Exception {
    verifyNoMoreInteractions(
        preparedStatementMock,
        valueMapperMock,
        columnVisitorMock,
        resultSetMock,
        resultSetMetaDataMock,
        blobMock,
        clobMock,
        sqlXmlMock0,
        sqlXmlMock1
    );

    super.tearDown();
  }

  @Test
  public void testFetchAllColumns() throws SQLException {
    List<JdbcColumnInfo> columns = JdbcQueryUtil.fetchAllColumns(getDataSource(), tableInfo);
    assertEquals(6, columns.size());
    assertEquals(idColumn, columns.get(0));
    assertEquals(dateColumn, columns.get(1));
    assertEquals(blobColumn, columns.get(2));
    assertEquals(clobColumn, columns.get(3));
    assertEquals(otherClobColumn, columns.get(4));
    assertEquals(varcharColumn, columns.get(5));
  }

  @Test
  public void testFetchPrimaryKeyNames() throws SQLException {
    Set<String> primaryKeyNames = JdbcQueryUtil.fetchPrimaryKeyNames(getDataSource(), tableInfo);
    assertEquals(
        Collections.singleton("ID"),
        primaryKeyNames
    );
  }

  @Test
  public void testSetPreparedValue() throws SQLException {
    when(valueMapperMock.getInteger(anyString())).thenReturn(13, null, 777, 44);
    when(valueMapperMock.getString(anyString())).thenReturn("", "foo", "X", null);
    when(valueMapperMock.getDouble(anyString())).thenReturn(3.45d);
    when(valueMapperMock.getFloat(anyString())).thenReturn(4.56f);
    when(valueMapperMock.getByte(anyString())).thenReturn(null, (byte) 98);
    when(valueMapperMock.getShort(anyString())).thenReturn((short) 654);
    when(valueMapperMock.getLong(anyString())).thenReturn(12345678910L);
    when(valueMapperMock.getBoolean(anyString())).thenReturn(true);

    InOrder verifier = inOrder(valueMapperMock, preparedStatementMock);

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   3,
                                   JDBCType.INTEGER,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getInteger("VAL_A");
    verifier.verify(preparedStatementMock).setInt(3, 13);

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   3,
                                   JDBCType.VARCHAR,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getString("VAL_A");
    verifier.verify(preparedStatementMock).setString(3, "");

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   1,
                                   JDBCType.TINYINT,
                                   valueMapperMock,
                                   "VAL_B");

    verifier.verify(valueMapperMock).getByte("VAL_B");
    verifier.verify(preparedStatementMock).setNull(1, JDBCType.TINYINT.getVendorTypeNumber());

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   3,
                                   JDBCType.INTEGER,
                                   valueMapperMock,
                                   "VAL_B");

    verifier.verify(valueMapperMock).getInteger("VAL_B");
    verifier.verify(preparedStatementMock).setNull(3, JDBCType.INTEGER.getVendorTypeNumber());

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   3,
                                   JDBCType.LONGVARCHAR,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getString("VAL_A");
    verifier.verify(preparedStatementMock).setString(3, "foo");

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   2,
                                   JDBCType.BIGINT,
                                   valueMapperMock,
                                   "VAL_C");

    verifier.verify(valueMapperMock).getLong("VAL_C");
    verifier.verify(preparedStatementMock).setLong(2, 12345678910L);

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   1,
                                   JDBCType.CHAR,
                                   valueMapperMock,
                                   "VAL_B");

    verifier.verify(valueMapperMock).getString("VAL_B");
    verifier.verify(preparedStatementMock).setString(1, "X");

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   1,
                                   JDBCType.TINYINT,
                                   valueMapperMock,
                                   "VAL_B");

    verifier.verify(valueMapperMock).getByte("VAL_B");
    verifier.verify(preparedStatementMock).setByte(1, (byte) 98);

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   3,
                                   JDBCType.VARCHAR,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getString("VAL_A");
    verifier.verify(preparedStatementMock).setNull(3, JDBCType.VARCHAR.getVendorTypeNumber());

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   2,
                                   JDBCType.SMALLINT,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getShort("VAL_A");
    verifier.verify(preparedStatementMock).setShort(2, (short) 654);

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   1,
                                   JDBCType.INTEGER,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getInteger("VAL_A");
    verifier.verify(preparedStatementMock).setInt(1, 777);

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   2,
                                   JDBCType.DOUBLE,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getDouble("VAL_A");
    verifier.verify(preparedStatementMock).setDouble(2, 3.45d);

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   1,
                                   JDBCType.BOOLEAN,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getBoolean("VAL_A");
    verifier.verify(preparedStatementMock).setBoolean(1, true);

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   1,
                                   JDBCType.FLOAT,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getFloat("VAL_A");
    verifier.verify(preparedStatementMock).setFloat(1, 4.56f);

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   1,
                                   JDBCType.NULL,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(preparedStatementMock).setNull(1, JDBCType.NULL.getVendorTypeNumber());

    JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                   1,
                                   JDBCType.INTEGER,
                                   valueMapperMock,
                                   "VAL_A");

    verifier.verify(valueMapperMock).getInteger("VAL_A");
    verifier.verify(preparedStatementMock).setInt(1, 44);
  }

  @Test
  public void testSetPreparedValueWithErrors() throws SQLException {
    when(valueMapperMock.getInteger(anyString())).thenThrow(new DataException("test exception message"));

    try {
      JdbcQueryUtil.setPreparedValue(preparedStatementMock,
                                     3,
                                     JDBCType.INTEGER,
                                     valueMapperMock,
                                     "VAL_A");
      fail("Should have failed");
    } catch (DataException ex) {
      assertEquals("test exception message", ex.getMessage());
    }

    verify(valueMapperMock).getInteger("VAL_A");
  }

  @Test
  public void testVisitResultSetColumns() throws SQLException {
    when(resultSetMock.getMetaData()).thenReturn(resultSetMetaDataMock);

    when(resultSetMetaDataMock.getColumnCount()).thenReturn(5);
    when(resultSetMetaDataMock.getTableName(anyInt())).thenReturn("IGNORED_TABLE_NAME");

    int columnId = 0;

    String columnName1 = mockMetaDataColumn(++columnId, JDBCType.SQLXML);
    when(resultSetMock.getSQLXML(columnName1)).thenReturn(sqlXmlMock0);

    String columnName2 = mockMetaDataColumn(++columnId, JDBCType.CLOB);
    when(resultSetMock.getClob(columnName2)).thenReturn(clobMock);
    when(clobMock.length()).thenReturn(10L);

    String columnName3 = mockMetaDataColumn(++columnId, JDBCType.SQLXML);
    when(resultSetMock.getSQLXML(columnName3)).thenReturn(sqlXmlMock1);

    String columnName4 = mockMetaDataColumn(++columnId, JDBCType.CLOB);
    when(resultSetMock.getClob(columnName4)).thenReturn(null);

    String columnName5 = mockMetaDataColumn(++columnId, JDBCType.BLOB);
    when(resultSetMock.getBlob(columnName5)).thenReturn(blobMock);
    when(blobMock.length()).thenReturn(9999L);

    JdbcQueryUtil.visitResultSetColumns(resultSetMock, columnVisitorMock);

    InOrder verifier = inOrder(columnVisitorMock);

    verifier.verify(columnVisitorMock).visit(columnName1, sqlXmlMock0);
    verifier.verify(columnVisitorMock).visit(columnName2, clobMock);
    verifier.verify(columnVisitorMock).visit(columnName3, sqlXmlMock1);
    verifier.verify(columnVisitorMock).visit(columnName4, (Clob) null);
    verifier.verify(columnVisitorMock).visit(columnName5, blobMock);

    verify(resultSetMetaDataMock).getColumnCount();
    verify(resultSetMetaDataMock, times(5)).getTableName(anyInt());
    verify(resultSetMetaDataMock, times(5)).getColumnName(anyInt());
    verify(resultSetMetaDataMock, times(5)).getColumnType(anyInt());

    verify(resultSetMock).getMetaData();
    verify(resultSetMock, times(2)).getSQLXML(anyString());
    verify(resultSetMock, times(2)).getClob(anyString());
    verify(resultSetMock).getBlob(anyString());

    verify(clobMock).length();
    verify(blobMock).length();
  }

  @Test
  public void testExecuteSingletonQueryId0() throws SQLException {
    executeSingletonQuery(0);

    assertEquals("This is my text.", new String(blobMappedCaptor.getOnlyValue()));
    assertEquals("test clob text", clobMappedCaptor.getOnlyValue());
    assertEquals("other clob text", otherClobMappedCaptor.getOnlyValue());

    verify(valueMapperMock).getInteger("ID");
    verify(columnVisitorMock).visit(anyString(), any(Blob.class));
    verify(columnVisitorMock, times(2)).visit(anyString(), any(Clob.class));
    verify(columnVisitorMock).visit("MY_VARCHAR", "vc1");
  }

  @Test
  public void testExecuteSingletonQueryId1() throws SQLException {
    executeSingletonQuery(1);

    assertNull(blobMappedCaptor.getOnlyValue());
    assertNull(clobMappedCaptor.getOnlyValue());
    assertEquals("another clob text for id 1", otherClobMappedCaptor.getOnlyValue());

    verify(valueMapperMock).getInteger("ID");
    verify(columnVisitorMock).visit(anyString(), (Blob) isNull());
    verify(columnVisitorMock).visit(anyString(), (Clob) isNull());
    verify(columnVisitorMock).visit(anyString(), any(Clob.class));
    verify(columnVisitorMock).visit("MY_VARCHAR", (String) null);
  }

  private String mockMetaDataColumn(int columnId, JDBCType jdbcType) throws SQLException {
    String columnName = "COL_" + columnId;
    when(resultSetMetaDataMock.getColumnName(columnId)).thenReturn(columnName);
    when(resultSetMetaDataMock.getColumnType(columnId)).thenReturn(jdbcType.getVendorTypeNumber());
    return columnName;
  }

  /**
   * TODO: We cannot test SQLXML directly because neither H2 nor HSQL support it
   */
  private void executeSingletonQuery(int id) throws SQLException {
    when(valueMapperMock.getInteger(anyString())).thenReturn(id);

    doNothing()
        .when(columnVisitorMock)
        .visit(eq("MY_BLOB"), argThat(blobMappedCaptor));

    doNothing()
        .when(columnVisitorMock)
        .visit(eq("MY_CLOB"), argThat(clobMappedCaptor));

    doNothing()
        .when(columnVisitorMock)
        .visit(eq("MY_OTHER_CLOB"), argThat(otherClobMappedCaptor));

    JdbcQueryUtil.executeSingletonQuery(
        getDataSource(),
        tableInfo,
        Collections.singletonList(idColumn),
        Arrays.asList(clobColumn, blobColumn, otherClobColumn, varcharColumn),
        valueMapperMock,
        columnVisitorMock,
        "My PK is 0"
    );
  }
}

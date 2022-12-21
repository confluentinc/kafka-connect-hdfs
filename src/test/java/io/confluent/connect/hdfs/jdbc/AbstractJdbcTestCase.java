package io.confluent.connect.hdfs.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.dbunit.JdbcBasedDBTestCase;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.JDBCType;

/**
 * TODO: No in-memory database supports SQLXML,
 *       so we can only columns of that type that with mocks
 */
public abstract class AbstractJdbcTestCase extends JdbcBasedDBTestCase {
  private static final Logger log = LoggerFactory.getLogger(AbstractJdbcTestCase.class);

  protected final JdbcTableInfo tableInfo =
      new JdbcTableInfo(null, null, "TESTTABLE");
  protected final JdbcColumnInfo idColumn =
      new JdbcColumnInfo("ID", JDBCType.INTEGER, 1, false);
  protected final JdbcColumnInfo dateColumn =
      new JdbcColumnInfo("MY_DATE", JDBCType.DATE, 2, true);
  protected final JdbcColumnInfo blobColumn =
      new JdbcColumnInfo("MY_BLOB", JDBCType.BLOB, 3, true);
  protected final JdbcColumnInfo clobColumn =
      new JdbcColumnInfo("MY_CLOB", JDBCType.CLOB, 4, true);
  protected final JdbcColumnInfo otherClobColumn =
      new JdbcColumnInfo("MY_OTHER_CLOB", JDBCType.CLOB, 5, false);

  protected HikariDataSource dataSource;

  @Override
  protected String getDriverClass() {
    return org.h2.Driver.class.getName();
  }

  @Override
  protected String getConnectionUrl() {
    return "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;init=runscript from 'classpath:schema.sql'";
  }

  @Override
  protected String getUsername() {
    return "sa";
  }

  @Override
  protected String getPassword() {
    return "sa";
  }

  @Override
  protected IDataSet getDataSet() throws Exception {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream("dataset.xml")) {
      return new FlatXmlDataSetBuilder().build(is);
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(getConnectionUrl());
    hikariConfig.setUsername(getUsername());
    hikariConfig.setPassword(getPassword());
    dataSource = new HikariDataSource(hikariConfig);

    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    try {
      dataSource.close();
    } catch (Exception ex) {
      log.warn("Failed to close DataSource: {}", ex.getMessage(), ex);
    }

    super.tearDown();
  }
}

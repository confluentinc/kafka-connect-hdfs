package io.confluent.connect.hdfs.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.dbunit.DataSourceBasedDBTestCase;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * TODO: No in-memory database supports SQLXML,
 *       so we can only columns of that type that with mocks
 */
public abstract class AbstractJdbcTestCase extends DataSourceBasedDBTestCase {
  public enum InMemoryDatabase {
    H2, HSQL
  }

  private static final Logger log = LoggerFactory.getLogger(AbstractJdbcTestCase.class);

  protected final JdbcTableInfo tableInfo =
      new JdbcTableInfo(/*null, */null, "TESTTABLE");
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
  protected final JdbcColumnInfo varcharColumn =
      new JdbcColumnInfo("MY_VARCHAR", JDBCType.VARCHAR, 6, true);

  private final InMemoryDatabase inMemoryDatabase;
  private HikariDataSource dataSource;

  protected AbstractJdbcTestCase(InMemoryDatabase inMemoryDatabase) {
    this.inMemoryDatabase = inMemoryDatabase;
  }

  @Override
  protected HikariDataSource getDataSource() {
    return dataSource;
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
    switch (inMemoryDatabase) {
      case H2:
        dataSource = createH2Datasource();
        break;
      case HSQL:
        dataSource = createHsqlDataSource();
        break;
      default:
        throw new RuntimeException("Invalid InMemoryDatabase: " + inMemoryDatabase);
    }
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    try {
      dataSource.close();
    } catch (Exception ex) {
      log.warn("Failed to close DataSource: {}", ex.getMessage(), ex);
    }
  }

  private HikariDataSource createH2Datasource() {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;init=runscript from 'classpath:schema.sql'");
    hikariConfig.setUsername("sa");
    hikariConfig.setPassword("sa");
    return new HikariDataSource(hikariConfig);
  }

  private HikariDataSource createHsqlDataSource() {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl("jdbc:hsqldb:mem:testdb");
    hikariConfig.setUsername("SA");
    hikariConfig.setPassword("");
    HikariDataSource dataSource = new HikariDataSource(hikariConfig);

    // Only for HSQL; H2 uses the schema.sql file to define the table
    String createTableSql =
        "CREATE MEMORY TABLE IF NOT EXISTS TESTTABLE(\n"
        + "    ID INT NOT NULL,\n"
        + "    MY_DATE DATE,\n"
        + "    MY_BLOB BLOB,\n"
        + "    MY_CLOB CLOB,\n"
        //+ "    MY_XML SQL_SQLXML,\n"
        + "    MY_OTHER_CLOB CLOB NOT NULL,\n"
        + "    MY_VARCHAR VARCHAR(10),\n"
        + "    PRIMARY KEY (ID)\n"
        + ");";

    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(createTableSql)) {
      assertFalse(preparedStatement.execute());
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    return dataSource;
  }
}

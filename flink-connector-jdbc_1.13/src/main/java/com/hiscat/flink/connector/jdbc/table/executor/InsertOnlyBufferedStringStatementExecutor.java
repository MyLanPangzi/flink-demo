package com.hiscat.flink.connector.jdbc.table.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

@Slf4j
public class InsertOnlyBufferedStringStatementExecutor
    implements JdbcBatchStatementExecutor<String> {

  private final String sql;
  private transient Statement statement;
  private final List<String> buffer = new ArrayList<>();

  public InsertOnlyBufferedStringStatementExecutor(String sql) {
    this.sql = sql;
  }

  @Override
  public void prepareStatements(Connection connection) throws SQLException {
    statement = connection.createStatement();
  }

  @Override
  public void addToBatch(String record) {
    buffer.add(record);
  }

  @Override
  public void executeBatch() throws SQLException {
    if (buffer.isEmpty()) {
      return;
    }
    String sql = this.sql + String.join(",", buffer);
    this.statement.execute(sql);
    buffer.clear();
  }

  @Override
  public void closeStatements() throws SQLException {
    statement.close();
  }
}

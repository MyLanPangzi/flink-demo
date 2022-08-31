package com.hiscat.flink.connector.jdbc.table.executor;

import static java.util.stream.Collectors.toMap;

import com.hiscat.flink.connector.jdbc.table.PartialUpdateOptions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;

public class PartialUpdateJdbcBatchStatementExecutor
    implements JdbcBatchStatementExecutor<RowData> {

  private final Map<String, PartialUpdateOptions> options;

  private final Function<RowData, String> tagExtractor;

  private final JdbcDmlOptions dmlOptions;

  public PartialUpdateJdbcBatchStatementExecutor(
      JdbcDmlOptions dmlOptions,
      List<PartialUpdateOptions> partialUpdateOptions,
      Function<RowData, String> tagExtractor) {
    this.tagExtractor = tagExtractor;
    this.dmlOptions = dmlOptions;
    this.options =
        partialUpdateOptions.stream().collect(toMap(PartialUpdateOptions::getTag, o -> o));
  }

  @Override
  public void prepareStatements(Connection connection) {
    this.options
        .values()
        .forEach(
            o -> {
              String[] fields = o.getFields().toArray(new String[0]);
              String upsertSql =
                  dmlOptions
                      .getDialect()
                      .getUpsertStatement(
                          dmlOptions.getTableName(),
                          fields,
                          dmlOptions
                              .getKeyFields()
                              .orElseThrow(() -> new NoSuchElementException("pk not exists")))
                      .orElseThrow(() -> new NoSuchElementException("upsert sql not provided"));
              try {
                FieldNamedPreparedStatement pst =
                    FieldNamedPreparedStatement.prepareStatement(connection, upsertSql, fields);
                o.setPreparedStatement(pst);
                o.setBuffer(new ArrayList<>());
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  public void addToBatch(RowData record) {
    options.get(tagExtractor.apply(record)).getBuffer().add(record);
  }

  @Override
  public void executeBatch() {
    options.values().forEach(PartialUpdateOptions::flush);
  }

  @Override
  public void closeStatements() {
    options.values().forEach(PartialUpdateOptions::close);
  }
}

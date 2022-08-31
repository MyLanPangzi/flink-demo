package com.hiscat.flink.connector.jdbc.table;

import com.hiscat.flink.connector.jdbc.table.executor.InsertOnlyBufferedStringStatementExecutor;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

public class RedshiftDynamicTableSink implements DynamicTableSink {
  private final JdbcOptions jdbcOptions;
  private final JdbcExecutionOptions executionOptions;
  private final JdbcDmlOptions dmlOptions;
  private final ResolvedSchema tableSchema;

  public RedshiftDynamicTableSink(
      JdbcOptions jdbcOptions,
      JdbcExecutionOptions executionOptions,
      JdbcDmlOptions dmlOptions,
      ResolvedSchema tableSchema) {
    this.jdbcOptions = jdbcOptions;
    this.executionOptions = executionOptions;
    this.dmlOptions = dmlOptions;
    this.tableSchema = tableSchema;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.upsert();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

    List<FieldStringGetter> fieldStringGetters = getFieldStringGetters();
    String sql = getInsertIntoStatement();
    return SinkFunctionProvider.of(
        new GenericJdbcSinkFunction<>(
            new JdbcBatchingOutputFormat<>(
                new SimpleJdbcConnectionProvider(jdbcOptions),
                this.executionOptions,
                ctx -> new InsertOnlyBufferedStringStatementExecutor(sql),
                rowData ->
                    fieldStringGetters.stream()
                        .map(f -> Objects.requireNonNull(f.getFieldOrNull(rowData)).toString())
                        .collect(Collectors.joining(",", "(", ")")))),
        jdbcOptions.getParallelism());
  }

  private String getInsertIntoStatement() {
    JdbcDialect dialect = dmlOptions.getDialect();
    String escapedCols =
        Arrays.stream(dmlOptions.getFieldNames())
            .map(dialect::quoteIdentifier)
            .collect(Collectors.joining(","));
    return String.format(
        "insert into %s (%s) values",
        dmlOptions.getTableName(), escapedCols);
  }

  private List<FieldStringGetter> getFieldStringGetters() {
    List<DataType> columnDataTypes = this.tableSchema.getColumnDataTypes();
    List<String> columnNames = this.tableSchema.getColumnNames();
    return columnNames.stream()
        .map(
            col -> {
              int filedPos = columnNames.indexOf(col);
              LogicalType logicalType = columnDataTypes.get(filedPos).getLogicalType();
              return new FieldStringGetter(
                  logicalType, RowData.createFieldGetter(logicalType, filedPos));
            })
        .collect(Collectors.toList());
  }

  @Override
  public DynamicTableSink copy() {
    return new RedshiftDynamicTableSink(jdbcOptions, executionOptions, dmlOptions, tableSchema);
  }

  @Override
  public String asSummaryString() {
    return "redshift dynamic table sink";
  }
}

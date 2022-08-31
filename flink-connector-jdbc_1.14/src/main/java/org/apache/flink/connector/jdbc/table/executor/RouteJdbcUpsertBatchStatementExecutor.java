package org.apache.flink.connector.jdbc.table.executor;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

public class RouteJdbcUpsertBatchStatementExecutor extends AbstractRouteJdbcBatchStatementExecutor
        implements JdbcBatchStatementExecutor<RowData> {

    public RouteJdbcUpsertBatchStatementExecutor(
            JdbcDmlOptions dmlOptions,
            Function<RowData, String> tableExtractor,
            JdbcRowConverter rowConverter) {
        super(dmlOptions, tableExtractor, rowConverter);
    }

    @Override
    protected FieldNamedPreparedStatement getFieldNamedPreparedStatement(String table) throws SQLException {
        String[] uniqueKeyFields = dmlOptions.getKeyFields()
                .orElseThrow(() -> new RuntimeException("pk not exists"));
        String[] fieldNames = Arrays.stream(dmlOptions.getFieldNames())
                .filter(f -> !f.equals(TABLE)).toArray(String[]::new);
        String sql = dmlOptions.getDialect().getUpsertStatement(table, fieldNames, uniqueKeyFields)
                .orElseThrow(() -> new RuntimeException("upsert sql not provided"));
        return FieldNamedPreparedStatement
                .prepareStatement(connection, sql, fieldNames);
    }

}

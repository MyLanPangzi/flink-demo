package org.apache.flink.connector.jdbc.table.executor;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;

import java.sql.SQLException;
import java.util.function.Function;

public class RouteJdbcDeleteBatchStatementExecutor extends AbstractRouteJdbcBatchStatementExecutor
        implements JdbcBatchStatementExecutor<RowData> {


    public RouteJdbcDeleteBatchStatementExecutor(
            JdbcDmlOptions dmlOptions,
            Function<RowData, String> tableExtractor,
            JdbcRowConverter rowConverter) {
        super(dmlOptions, tableExtractor, rowConverter);
    }

    @Override
    protected FieldNamedPreparedStatement getFieldNamedPreparedStatement(String table) throws SQLException {
        String[] uniqueKeyFields = dmlOptions.getKeyFields()
                .orElseThrow(() -> new RuntimeException("pk not exists"));
        String sql = dmlOptions.getDialect().getDeleteStatement(table, uniqueKeyFields);
        return FieldNamedPreparedStatement
                .prepareStatement(connection, sql, uniqueKeyFields);
    }

}

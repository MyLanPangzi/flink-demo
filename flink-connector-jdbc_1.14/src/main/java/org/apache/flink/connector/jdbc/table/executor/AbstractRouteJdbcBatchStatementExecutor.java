package org.apache.flink.connector.jdbc.table.executor;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

public abstract class AbstractRouteJdbcBatchStatementExecutor
        implements JdbcBatchStatementExecutor<RowData> {

    public static final String TABLE = "table";
    private final Function<RowData, String> tableExtractor;

    protected final JdbcDmlOptions dmlOptions;
    private final JdbcRowConverter rowConverter;

    protected transient Connection connection;

    private final Map<String, List<RowData>> buffer;

    public AbstractRouteJdbcBatchStatementExecutor(
            JdbcDmlOptions dmlOptions,
            Function<RowData, String> tableExtractor,
            JdbcRowConverter rowConverter) {
        this.tableExtractor = tableExtractor;
        this.dmlOptions = dmlOptions;
        this.rowConverter = rowConverter;
        this.buffer = new HashMap<>();
    }

    @Override
    public void prepareStatements(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void addToBatch(RowData record) {
        String table = tableExtractor.apply(record);
        buffer.computeIfAbsent(table, k -> new ArrayList<>()).add(record);
    }

    @Override
    public void executeBatch() {
        buffer.forEach((table, buffer) -> {
            if (buffer.isEmpty()) {
                return;
            }
            try {
                FieldNamedPreparedStatement prepareStatement = getFieldNamedPreparedStatement(table);
                buffer.forEach(e -> {
                    try {
                        rowConverter.toExternal(e, prepareStatement);
                        prepareStatement.addBatch();
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }
                });
                prepareStatement.executeBatch();
                prepareStatement.close();
                buffer.clear();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        buffer.clear();
    }

    @Override
    public void closeStatements() {
        buffer.clear();
    }

    protected abstract FieldNamedPreparedStatement getFieldNamedPreparedStatement(String table) throws SQLException;
}

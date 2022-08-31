/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.PartialUpdateMySQLRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.table.executor.RouteTableBufferReducedStatementExecutorFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link DynamicTableSink} for JDBC.
 */
@Internal
public class RouteJdbcDynamicTableSink
        implements DynamicTableSink, SupportsWritingMetadata {

    private final JdbcConnectorOptions jdbcOptions;
    private final JdbcExecutionOptions executionOptions;
    private final JdbcDmlOptions dmlOptions;
    private final ResolvedSchema tableSchema;
    private final String dialectName;
    private List<String> metadataKeys;

    public RouteJdbcDynamicTableSink(
            JdbcConnectorOptions jdbcOptions,
            JdbcExecutionOptions executionOptions,
            JdbcDmlOptions dmlOptions,
            ResolvedSchema tableSchema) {
        this.jdbcOptions = jdbcOptions;
        this.executionOptions = executionOptions;
        this.dmlOptions = dmlOptions;
        this.tableSchema = tableSchema;
        this.dialectName = dmlOptions.getDialect().dialectName();
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode) || dmlOptions.getKeyFields().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<RowData> rowDataTypeInformation =
                context.createTypeInformation(tableSchema.toSinkRowDataType());
        JdbcRowConverter upsertRowConverter = getUpsertRowConverter();
        JdbcRowConverter deleteRowConverter = getDeleteRowConverter();

        return SinkFunctionProvider.of(
                new GenericJdbcSinkFunction<>(
                        new JdbcOutputFormat<>(
                                new SimpleJdbcConnectionProvider(jdbcOptions),
                                executionOptions,
                                new RouteTableBufferReducedStatementExecutorFactory(
                                        rowDataTypeInformation,
                                        upsertRowConverter,
                                        deleteRowConverter,
                                        tableSchema.toPhysicalRowDataType().getChildren(),
                                        new TableExtractor(extractTablePos()),
                                        dmlOptions
                                ),
                                JdbcOutputFormat.RecordExtractor.identity())),
                jdbcOptions.getParallelism());
    }

    private PartialUpdateMySQLRowConverter getUpsertRowConverter() {
        List<String> cols = tableSchema.getColumnNames()
                .stream()
                .filter(c -> !TableMetadata.TABLE.key.equals(c))
                .collect(toList());
        List<Integer> fieldIndexes = getFieldIndexes(cols);
        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        return new PartialUpdateMySQLRowConverter(rowType, fieldIndexes);
    }

    private PartialUpdateMySQLRowConverter getDeleteRowConverter() {
        List<String> pks = tableSchema.getPrimaryKey()
                .orElseThrow(() -> new NoSuchElementException("pk not exists"))
                .getColumns();
        List<Integer> fieldIndexes = getFieldIndexes(pks);
        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        return new PartialUpdateMySQLRowConverter(rowType, fieldIndexes);
    }

    private List<Integer> getFieldIndexes(List<String> pks) {
        return pks.stream()
                .filter(c -> !TableMetadata.TABLE.key.equals(c))
                .map(c -> tableSchema.getColumnNames().indexOf(c))
                .collect(toList());
    }

    private int extractTablePos() {
        return this.tableSchema.toPhysicalRowDataType().getChildren().size()
                + this.metadataKeys.indexOf(TableMetadata.TABLE.key);
    }

    @Override
    public DynamicTableSink copy() {
        return new RouteJdbcDynamicTableSink(
                jdbcOptions, executionOptions, dmlOptions, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RouteJdbcDynamicTableSink)) {
            return false;
        }
        RouteJdbcDynamicTableSink that = (RouteJdbcDynamicTableSink) o;
        return Objects.equals(jdbcOptions, that.jdbcOptions)
                && Objects.equals(executionOptions, that.executionOptions)
                && Objects.equals(dmlOptions, that.dmlOptions)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcOptions, executionOptions, dmlOptions, tableSchema, dialectName);
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        HashMap<String, DataType> metadata = new HashMap<>();
        metadata.put(TableMetadata.TABLE.key, TableMetadata.TABLE.dataType);
        return metadata;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
    }

    enum TableMetadata {
        TABLE(
                "table",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.STRING(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }

                        return row.getString(pos);
                    }
                });
        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        TableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

    interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }
}

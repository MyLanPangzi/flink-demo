package org.apache.flink.connector.jdbc.table.executor;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferReducedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.table.TableExtractor;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.data.RowData.createFieldGetter;

public class RouteTableBufferReducedStatementExecutorFactory
        implements JdbcOutputFormat.StatementExecutorFactory<TableBufferReducedStatementExecutor> {

    private final TypeInformation<RowData> rowDataTypeInformation;
    private final List<DataType> columnDataTypes;
    private final TableExtractor tableExtractor;
    private final JdbcDmlOptions dmlOptions;
    private final JdbcRowConverter upsertRowConverter;
    private final JdbcRowConverter deleteRowConverter;

    public RouteTableBufferReducedStatementExecutorFactory(
            TypeInformation<RowData> rowDataTypeInformation,
            JdbcRowConverter upsertRowConverter,
            JdbcRowConverter deleteRowConverter,
            List<DataType> columnDataTypes,
            TableExtractor tableExtractor,
            JdbcDmlOptions dmlOptions) {
        this.rowDataTypeInformation = rowDataTypeInformation;
        this.columnDataTypes = columnDataTypes;
        this.tableExtractor = tableExtractor;
        this.dmlOptions = dmlOptions;
        this.upsertRowConverter = upsertRowConverter;
        this.deleteRowConverter = deleteRowConverter;
    }

    @Override
    public TableBufferReducedStatementExecutor apply(RuntimeContext ctx) {

        final Function<RowData, RowData> valueTransform =
                getValueTransform(
                        ctx, this.rowDataTypeInformation.createSerializer(ctx.getExecutionConfig()));

        List<String> fieldNames = Arrays.asList(dmlOptions.getFieldNames());
        return new TableBufferReducedStatementExecutor(
                new RouteJdbcUpsertBatchStatementExecutor(dmlOptions, tableExtractor, upsertRowConverter),
                new RouteJdbcDeleteBatchStatementExecutor(dmlOptions, tableExtractor, deleteRowConverter),
                createRowKeyExtractor(getRowKeyExtractorParameters(fieldNames), tableExtractor),
                valueTransform);
    }

    private List<RowKeyExtractorParameters> getRowKeyExtractorParameters(List<String> fieldNames) {
        return Arrays.stream(dmlOptions.getKeyFields().orElseThrow(() -> new RuntimeException("pk absent!")))
                .map(pk -> {
                    int index = fieldNames.indexOf(pk);
                    LogicalType logicalType = this.columnDataTypes.get(index).getLogicalType();
                    FieldGetter fieldGetter = createFieldGetter(logicalType, index);
                    return new RowKeyExtractorParameters(index, fieldGetter);
                }).collect(toList());
    }

    private Function<RowData, RowData> getValueTransform(
            RuntimeContext ctx, TypeSerializer<RowData> typeSerializer) {
        return ctx.getExecutionConfig().isObjectReuseEnabled()
                ? typeSerializer::copy
                : Function.identity();
    }

    private static Function<RowData, RowData> createRowKeyExtractor(
            List<RowKeyExtractorParameters> rowKeyExtractorParameters,
            TableExtractor tagExtractor) {

        return row -> getPrimaryKey(row, rowKeyExtractorParameters, tagExtractor);
    }

    private static RowData getPrimaryKey(
            RowData row,
            List<RowKeyExtractorParameters> rowKeyExtractorParameters,
            TableExtractor tagExtractor) {
        GenericRowData pkRow = new GenericRowData(row.getArity());
        for (RowKeyExtractorParameters primaryKeyParameter : rowKeyExtractorParameters) {
            pkRow.setField(primaryKeyParameter.index, primaryKeyParameter.fieldGetter.getFieldOrNull(row));
        }
        pkRow.setField(tagExtractor.getPos(), StringData.fromString(tagExtractor.apply(row)));
        return pkRow;
    }


    static class RowKeyExtractorParameters implements Serializable {
        private int index;
        private FieldGetter fieldGetter;

        public RowKeyExtractorParameters() {
        }

        public RowKeyExtractorParameters(int index, FieldGetter fieldGetter) {
            this.index = index;
            this.fieldGetter = fieldGetter;
        }

        @Override
        public String toString() {
            return "PrimaryKeyParameters{" +
                    "index=" + index +
                    ", fieldGetter=" + fieldGetter +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RowKeyExtractorParameters that = (RowKeyExtractorParameters) o;
            return index == that.index && Objects.equals(fieldGetter, that.fieldGetter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, fieldGetter);
        }
    }
}

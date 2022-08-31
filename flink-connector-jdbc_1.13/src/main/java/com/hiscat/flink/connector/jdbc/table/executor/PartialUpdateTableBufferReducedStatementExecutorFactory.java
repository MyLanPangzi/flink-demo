package com.hiscat.flink.connector.jdbc.table.executor;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.data.RowData.createFieldGetter;

import com.hiscat.flink.connector.jdbc.table.PartialUpdateOptions;
import com.hiscat.flink.connector.jdbc.table.PrepareStatementTagExtractor;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat.StatementExecutorFactory;
import org.apache.flink.connector.jdbc.internal.executor.TableBufferReducedStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

public class PartialUpdateTableBufferReducedStatementExecutorFactory
    implements StatementExecutorFactory<TableBufferReducedStatementExecutor> {

  private final TypeInformation<RowData> rowDataTypeInformation;
  private final List<DataType> columnDataTypes;
  private final PrepareStatementTagExtractor tagExtractor;
  private final JdbcDmlOptions dmlOptions;
  private final List<PartialUpdateOptions> partialUpdateOptions;

  public PartialUpdateTableBufferReducedStatementExecutorFactory(
      TypeInformation<RowData> rowDataTypeInformation,
      List<DataType> columnDataTypes,
      PrepareStatementTagExtractor tagExtractor,
      JdbcDmlOptions dmlOptions,
      List<PartialUpdateOptions> partialUpdateOptions) {
    this.rowDataTypeInformation = rowDataTypeInformation;
    this.columnDataTypes = columnDataTypes;
    this.tagExtractor = tagExtractor;
    this.dmlOptions = dmlOptions;
    this.partialUpdateOptions = partialUpdateOptions;
  }

  @Override
  public TableBufferReducedStatementExecutor apply(RuntimeContext ctx) {

    final Function<RowData, RowData> valueTransform =
        getValueTransform(
            ctx, this.rowDataTypeInformation.createSerializer(ctx.getExecutionConfig()));
    List<Integer> fields =
        IntStream.range(0, dmlOptions.getFieldNames().length).boxed().collect(toList());
    int[] pkFields =
        Arrays.stream(
                dmlOptions.getKeyFields().orElseThrow(() -> new RuntimeException("pk absent!")))
            .mapToInt(Arrays.asList(dmlOptions.getFieldNames())::indexOf)
            .toArray();
    LogicalType[] pkTypes =
        Arrays.stream(pkFields)
            .mapToObj(this.columnDataTypes::get)
            .map(DataType::getLogicalType)
            .toArray(LogicalType[]::new);

    return new TableBufferReducedStatementExecutor(
        new PartialUpdateJdbcBatchStatementExecutor(dmlOptions, partialUpdateOptions, tagExtractor),
        new PartialUpdateJdbcBatchStatementExecutor(dmlOptions, partialUpdateOptions, tagExtractor),
        createRowKeyExtractor(fields, pkTypes, pkFields, tagExtractor),
        valueTransform);
  }

  private Function<RowData, RowData> getValueTransform(
      RuntimeContext ctx, TypeSerializer<RowData> typeSerializer) {
    return ctx.getExecutionConfig().isObjectReuseEnabled()
        ? typeSerializer::copy
        : Function.identity();
  }

  private static Function<RowData, RowData> createRowKeyExtractor(
      List<Integer> fields,
      LogicalType[] logicalTypes,
      int[] pkFields,
      PrepareStatementTagExtractor tagExtractor) {
    final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fields.size()];
    for (int i = 0; i < pkFields.length; i++) {
      fieldGetters[i] = createFieldGetter(logicalTypes[pkFields[i]], pkFields[i]);
    }
    List<Integer> pkList = Arrays.stream(pkFields).boxed().collect(toList());
    return row -> getPrimaryKey(fields, pkList, row, fieldGetters, tagExtractor);
  }

  private static RowData getPrimaryKey(
      List<Integer> fields,
      List<Integer> pkList,
      RowData row,
      FieldGetter[] fieldGetters,
      PrepareStatementTagExtractor tagExtractor) {
    GenericRowData pkRow = new GenericRowData(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      if (pkList.contains(i)) {
        pkRow.setField(i, fieldGetters[i].getFieldOrNull(row));
      } else {
        pkRow.setField(i, null);
      }
    }
    pkRow.setField(tagExtractor.getPos(), StringData.fromString(tagExtractor.apply(row)));
    return pkRow;
  }
}

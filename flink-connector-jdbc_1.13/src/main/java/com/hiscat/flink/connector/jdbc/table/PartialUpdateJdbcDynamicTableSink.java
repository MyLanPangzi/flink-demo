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

package com.hiscat.flink.connector.jdbc.table;

import static com.hiscat.flink.connector.jdbc.table.PartialUpdateJdbcDynamicTableSink.PrepareStatementTagMetadata.PREPARE_STATEMENT_TAG;
import static org.apache.flink.util.Preconditions.checkState;

import com.hiscat.flink.connector.jdbc.table.executor.PartialUpdateTableBufferReducedStatementExecutorFactory;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat.RecordExtractor;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/** A {@link DynamicTableSink} for JDBC. */
@Internal
public class PartialUpdateJdbcDynamicTableSink
    implements DynamicTableSink, SupportsWritingMetadata {

  private final List<PartialUpdateOptions> partialUpdateOptions;
  private final JdbcOptions jdbcOptions;
  private final JdbcExecutionOptions executionOptions;
  private final JdbcDmlOptions dmlOptions;
  private final ResolvedSchema tableSchema;
  private final String dialectName;
  private List<String> metadataKeys;

  public PartialUpdateJdbcDynamicTableSink(
      List<PartialUpdateOptions> options,
      JdbcOptions jdbcOptions,
      JdbcExecutionOptions executionOptions,
      JdbcDmlOptions dmlOptions,
      ResolvedSchema tableSchema) {
    this.partialUpdateOptions = options;
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

    return SinkFunctionProvider.of(
        new GenericJdbcSinkFunction<>(
            new JdbcBatchingOutputFormat<>(
                new SimpleJdbcConnectionProvider(jdbcOptions),
                executionOptions,
                new PartialUpdateTableBufferReducedStatementExecutorFactory(
                    rowDataTypeInformation,
                    tableSchema.getColumnDataTypes(),
                    new PrepareStatementTagExtractor(extractTagPos()),
                    dmlOptions,
                    partialUpdateOptions),
                RecordExtractor.identity())),
        jdbcOptions.getParallelism());
  }

  private int extractTagPos() {
    return this.tableSchema.toPhysicalRowDataType().getChildren().size()
        + this.metadataKeys.indexOf(PREPARE_STATEMENT_TAG.key);
  }

  @Override
  public DynamicTableSink copy() {
    return new PartialUpdateJdbcDynamicTableSink(
        partialUpdateOptions, jdbcOptions, executionOptions, dmlOptions, tableSchema);
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
    if (!(o instanceof PartialUpdateJdbcDynamicTableSink)) {
      return false;
    }
    PartialUpdateJdbcDynamicTableSink that = (PartialUpdateJdbcDynamicTableSink) o;
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
    metadata.put(PREPARE_STATEMENT_TAG.key, PREPARE_STATEMENT_TAG.dataType);
    return metadata;
  }

  @Override
  public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
    this.metadataKeys = metadataKeys;
  }

  enum PrepareStatementTagMetadata {
    PREPARE_STATEMENT_TAG(
        "prepareStatementTag",
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

    PrepareStatementTagMetadata(String key, DataType dataType, MetadataConverter converter) {
      this.key = key;
      this.dataType = dataType;
      this.converter = converter;
    }
  }

  interface MetadataConverter extends Serializable {
    Object read(RowData consumedRow, int pos);
  }
}

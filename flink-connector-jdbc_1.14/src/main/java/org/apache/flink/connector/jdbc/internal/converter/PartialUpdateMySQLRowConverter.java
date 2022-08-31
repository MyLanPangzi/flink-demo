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

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.Serializable;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * MySQL.
 */
public class PartialUpdateMySQLRowConverter extends AbstractJdbcRowConverter {

  private static final long serialVersionUID = 1L;
  private final List<Integer> fieldIndexes;

  protected final PartialUpdateJdbcSerializationConverter[] toExternalConverters;

  @Override
  public String converterName() {
    return "PartialUpdateMySQL";
  }

  public PartialUpdateMySQLRowConverter(RowType rowType, List<Integer> fieldIndexes) {
    super(rowType);
    this.fieldIndexes = fieldIndexes;
    toExternalConverters = new PartialUpdateJdbcSerializationConverter[rowType.getFieldCount()];
    for (int i = 0; i < toExternalConverters.length; i++) {
      toExternalConverters[i] = createNullablePartialUpdateExternalConverter(fieldTypes[i]);
    }
  }

  @Override
  public FieldNamedPreparedStatement toExternal(
      RowData rowData, FieldNamedPreparedStatement statement) throws SQLException {
    for (int i = 0; i < fieldIndexes.size(); i++) {
      toExternalConverters[fieldIndexes.get(i)].serialize(
          rowData, fieldIndexes.get(i), statement, i);
    }
    return statement;
  }

  @FunctionalInterface
  interface PartialUpdateJdbcSerializationConverter extends Serializable {
    void serialize(
        RowData rowData,
        int rowDataIndex,
        FieldNamedPreparedStatement statement,
        int parameterIndex)
        throws SQLException;
  }

  PartialUpdateJdbcSerializationConverter createNullablePartialUpdateExternalConverter(
      LogicalType type) {
    return wrapIntoNullablePartialUpdateExternalConverter(
        createPartialUpdateExternalConverter(type), type);
  }

  PartialUpdateJdbcSerializationConverter wrapIntoNullablePartialUpdateExternalConverter(
      PartialUpdateJdbcSerializationConverter jdbcSerializationConverter, LogicalType type) {
    final int sqlType =
        JdbcTypeUtil.typeInformationToSqlType(
            TypeConversions.fromDataTypeToLegacyInfo(TypeConversions.fromLogicalToDataType(type)));
    return (val, rowDataIndex, statement, parameterIndex) -> {
      if (val == null
          || val.isNullAt(rowDataIndex)
          || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
        statement.setNull(parameterIndex, sqlType);
      } else {
        jdbcSerializationConverter.serialize(val, rowDataIndex, statement, parameterIndex);
      }
    };
  }

  PartialUpdateJdbcSerializationConverter createPartialUpdateExternalConverter(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
        return (val, index, statement, parameterIndex) ->
            statement.setBoolean(parameterIndex, val.getBoolean(index));
      case TINYINT:
        return (val, index, statement, parameterIndex) ->
            statement.setByte(parameterIndex, val.getByte(index));
      case SMALLINT:
        return (val, index, statement, parameterIndex) ->
            statement.setShort(parameterIndex, val.getShort(index));
      case INTEGER:
      case INTERVAL_YEAR_MONTH:
        return (val, index, statement, parameterIndex) ->
            statement.setInt(parameterIndex, val.getInt(index));
      case BIGINT:
      case INTERVAL_DAY_TIME:
        return (val, index, statement, parameterIndex) ->
            statement.setLong(parameterIndex, val.getLong(index));
      case FLOAT:
        return (val, index, statement, parameterIndex) ->
            statement.setFloat(parameterIndex, val.getFloat(index));
      case DOUBLE:
        return (val, index, statement, parameterIndex) ->
            statement.setDouble(parameterIndex, val.getDouble(index));
      case CHAR:
      case VARCHAR:
        // value is BinaryString
        return (val, index, statement, parameterIndex) ->
            statement.setString(parameterIndex, val.getString(index).toString());
      case BINARY:
      case VARBINARY:
        return (val, index, statement, parameterIndex) ->
            statement.setBytes(parameterIndex, val.getBinary(index));
      case DATE:
        return (val, index, statement, parameterIndex) ->
            statement.setDate(parameterIndex, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
      case TIME_WITHOUT_TIME_ZONE:
        return (val, index, statement, parameterIndex) ->
            statement.setTime(
                index, Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        final int timestampPrecision = ((TimestampType) type).getPrecision();
        return (val, index, statement, parameterIndex) ->
            statement.setTimestamp(
                index, val.getTimestamp(parameterIndex, timestampPrecision).toTimestamp());
      case DECIMAL:
        final int decimalPrecision = ((DecimalType) type).getPrecision();
        final int decimalScale = ((DecimalType) type).getScale();
        return (val, index, statement, parameterIndex) ->
            statement.setBigDecimal(
                index, val.getDecimal(parameterIndex, decimalPrecision, decimalScale).toBigDecimal());
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      case RAW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }
}
